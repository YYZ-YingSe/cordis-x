//! Explicit plugin factories and staged Value provisions over native Context.
use crate::{
    Error, Payload, Result, Value,
    context::{Router, next_id},
    ffi::Reply,
    json,
};
use cordis_contract::ContractId;
use cordis_runtime::WeakPluginHandle;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;

use cordis_host::Body;
#[derive(Clone, Debug, Default)]
pub struct CompositionLimits {
    /// Includes disposed child handles; IDs are never reused in an instance.
    pub children: Option<usize>,
    pub active_installs: Option<usize>,
}
impl crate::Capabilities {
    pub(crate) fn validate_composition(
        &self,
        manifest: Option<&Value>,
        primary: &ContractId,
    ) -> Result<()> {
        if self
            .composition
            .active_installs
            .is_some_and(|n| n > Semaphore::MAX_PERMITS)
        {
            return Err(Error::InvalidInput("invalid Composition capacity".into()));
        }
        let mut factories = BTreeSet::new();
        for binding in &self.bindings.factories {
            if !valid_alias(&binding.alias) || !factories.insert(binding.alias.clone()) {
                return Err(Error::Contract("invalid or duplicate factory alias".into()));
            }
        }
        let mut expected = json!({"factories": factories, "provisions": {}});
        let mut slots = BTreeSet::new();
        for binding in &self.bindings.provisions {
            let contract = binding.service.contract();
            if !valid_alias(&binding.alias) || contract.name.is_empty() || contract.revision.is_empty()
                || contract == primary || !slots.insert(contract.clone())
                || expected["provisions"].as_object_mut().unwrap().insert(binding.alias.clone(),
                    json!({"name":contract.name.as_ref(),"revision":contract.revision.as_ref()})).is_some() {
                return Err(Error::Contract("invalid, duplicate or primary ABI provision".into()));
            }
        }
        // Factory order is not authority; duplicates are rejected at load time.
        let mut actual = manifest
            .cloned()
            .unwrap_or_else(|| json!({"factories":[],"provisions":{}}));
        if let Some(names) = actual["factories"].as_array_mut() {
            names.sort_by(|a, b| a.as_str().cmp(&b.as_str()));
        }
        if actual != expected {
            return Err(Error::Contract(
                "Composition bindings must exactly match the guest manifest".into(),
            ));
        }
        Ok(())
    }
}
fn valid_alias(alias: &str) -> bool {
    !alias.is_empty() && alias.len() <= 1024
}

pub(crate) struct Composition {
    sweep: cordis_wire::retention::Sweep,
    children: Mutex<BTreeMap<u64, Option<WeakPluginHandle>>>,
    installs: Semaphore,
    waits: Semaphore,
    disposals: Semaphore,
}
impl Composition {
    pub fn new(caps: &crate::Capabilities) -> Self {
        Self {
            sweep: Default::default(),
            children: Mutex::default(),
            installs: Semaphore::new(
                caps.composition
                    .active_installs
                    .unwrap_or(Semaphore::MAX_PERMITS),
            ),
            waits: Semaphore::new(caps.max_callbacks.unwrap_or(Semaphore::MAX_PERMITS)),
            disposals: Semaphore::new(caps.max_callbacks.unwrap_or(Semaphore::MAX_PERMITS)),
        }
    }
}
struct Reservation {
    router: Arc<Router>,
    id: u64,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        let mut children = self.router.composition.children.lock().unwrap();
        if matches!(children.get(&self.id), Some(None)) {
            children.remove(&self.id);
        }
    }
}
impl Router {
    fn child_handle(&self, target: u64) -> Result<cordis_runtime::PluginHandle> {
        self.composition
            .children
            .lock()
            .unwrap()
            .get(&target)
            .and_then(Option::as_ref)
            .and_then(WeakPluginHandle::upgrade)
            .ok_or(Error::Closed)
    }
    /// Child observation borrows its existing handle. Cancellation does not
    /// dispose the plugin or roll back its initialization contributions.
    pub(crate) async fn observe_child(&self, kind: u32, target: u64) -> Result<Reply> {
        let instance = self.instance.upgrade().ok_or(Error::Closed)?;
        if instance.module.composition_manifest().is_none() {
            return Err(Error::Closed);
        }
        if crate::context::in_cleanup() && kind != 11 {
            return Err(Error::Refused {
                code: "cleanup_wait".into(),
                message: "blocking child observations during cleanup are unsupported".into(),
            });
        }
        let child = self.child_handle(target)?;
        let snapshot = if kind == 11 {
            child.snapshot()
        } else {
            let _permit = self
                .composition
                .waits
                .try_acquire()
                .map_err(|_| Error::Overloaded)?;
            if kind == 5 {
                child.ready().await?
            } else {
                child.settled().await?
            }
        };
        let state = match snapshot.state {
            cordis_kernel::Lifecycle::Inactive => "inactive",
            cordis_kernel::Lifecycle::Loading => "loading",
            cordis_kernel::Lifecycle::Active => "active",
            cordis_kernel::Lifecycle::Unloading => "unloading",
        };
        let value = json!({"state":state,"retired":snapshot.retired,"outcome":snapshot.outcome.map(|o| format!("{o:?}"))});
        Ok(Reply {
            value: value.into(),
            handle: 0,
        })
    }

    pub async fn compose(
        self: &Arc<Self>,
        grant: u64,
        operation: u32,
        target: u64,
        alias: String,
        args: Payload,
    ) -> Result<Reply> {
        let instance = self.instance.upgrade().ok_or(Error::Closed)?;
        if instance.module.composition_manifest().is_none() {
            return Err(Error::Closed);
        }
        if crate::context::in_cleanup() {
            return Err(Error::Refused {
                code: "cleanup_wait".into(),
                message: "Composition operations during cleanup are unsupported".into(),
            });
        }
        if alias.len() > 1024 {
            return Err(Error::InvalidInput("Composition alias too long".into()));
        }
        if !matches!(operation, 1 | 6) {
            args.clone()
                .control_only()
                .map_err(|e| Error::InvalidInput(e.to_string()))?;
        }
        let value = match operation {
            1 => {
                let args = args.one().map_err(|e| Error::InvalidInput(e.to_string()))?;
                let ctx = self.context(grant)?;
                let factory = self
                    .capabilities
                    .bindings
                    .factories
                    .iter()
                    .find(|f| f.alias == alias)
                    .ok_or_else(|| Error::Refused {
                        code: "factory".into(),
                        message: "undeclared factory".into(),
                    })?
                    .factory
                    .clone();
                let router = self.clone();
                let id = ctx
                    .operation(move |ctx| async move {
                        Ok(async move {
                            let _instance = instance;
                            let _permit = router
                                .composition
                                .installs
                                .try_acquire()
                                .map_err(|_| Error::Overloaded)?;
                            let id = next_id()?;
                            {
                                let mut children = router.composition.children.lock().unwrap();
                                if router.composition.sweep.due(children.len())
                                    || router
                                        .capabilities
                                        .composition
                                        .children
                                        .is_some_and(|limit| children.len() >= limit)
                                {
                                    children.retain(|_, e| {
                                        !e.as_ref().is_some_and(WeakPluginHandle::is_retired)
                                    });
                                    router.composition.sweep.retained(children.len());
                                }
                                if router
                                    .capabilities
                                    .composition
                                    .children
                                    .is_some_and(|limit| children.len() >= limit)
                                {
                                    return Err(Error::Overloaded);
                                }
                                children.insert(id, None);
                            }
                            let _reservation = Reservation {
                                router: router.clone(),
                                id,
                            };
                            // No registry lock or Tokio executor thread spans host factory code.
                            let child = tokio::task::spawn_blocking(move || {
                                ctx.install_with(|| factory(Body::from_bytes(args)))
                            })
                            .await
                            .map_err(|e| Error::Runtime(format!("factory failed: {e}")))??;
                            router
                                .composition
                                .children
                                .lock()
                                .unwrap()
                                .insert(id, Some(child.downgrade()));
                            Ok(id)
                        }
                        .await)
                    })
                    .await??;
                return Ok(Reply {
                    handle: id,
                    value: Value::Null.into(),
                });
            }
            4 => {
                let child = self.child_handle(target)?;
                let _permit = self
                    .composition
                    .disposals
                    .try_acquire()
                    .map_err(|_| Error::Overloaded)?;
                child.dispose().await?;
                Value::Null
            }
            6 => {
                let ctx = self.context(grant)?;
                let binding = self
                    .capabilities
                    .bindings
                    .provisions
                    .iter()
                    .find(|p| p.alias == alias)
                    .ok_or_else(|| Error::Refused {
                        code: "provision".into(),
                        message: "undeclared provision".into(),
                    })?;
                ctx.provide(
                    &binding.service,
                    Arc::new(Body::from_bytes(
                        args.one().map_err(|e| Error::InvalidInput(e.to_string()))?,
                    )),
                )?;
                Value::Null
            }
            _ => return Err(Error::InvalidInput("unknown Composition operation".into())),
        };
        Ok(Reply {
            handle: 0,
            value: value.into(),
        })
    }
}
