//! Explicit plugin factories and staged Value provisions over native Context.
use crate::{
    Bytes, Error, Payload, Result, Value,
    context::{Router, next_id},
    ffi::Reply,
    json,
};
use cordis_contract::ContractId;
use cordis_runtime::{Plugin, Service, WeakPluginHandle};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;

type Factory = dyn Fn(Bytes) -> cordis_runtime::Result<Plugin> + Send + Sync;
/// A named, trusted definition factory. It must not acquire external resources:
/// acquisition belongs in the returned plugin's initializer and effects.
#[derive(Clone)]
pub struct FactoryBinding {
    alias: String,
    factory: Arc<Factory>,
}
impl FactoryBinding {
    pub fn new<F>(alias: impl Into<String>, factory: F) -> Self
    where
        F: Fn(Bytes) -> cordis_runtime::Result<Plugin> + Send + Sync + 'static,
    {
        Self {
            alias: alias.into(),
            factory: Arc::new(factory),
        }
    }
}
impl fmt::Debug for FactoryBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("FactoryBinding").field(&self.alias).finish()
    }
}
#[derive(Clone)]
pub struct ProvisionBinding {
    alias: String,
    pub(crate) service: Service<Bytes>,
}
impl fmt::Debug for ProvisionBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ProvisionBinding")
            .field(&self.alias)
            .field(self.service.contract())
            .finish()
    }
}
impl ProvisionBinding {
    pub fn new(alias: impl Into<String>, service: &Service<Bytes>) -> Self {
        Self {
            alias: alias.into(),
            service: service.clone(),
        }
    }
}
#[derive(Clone, Debug)]
pub struct CompositionLimits {
    /// Includes disposed child handles; IDs are never reused in an instance.
    pub children: usize,
    pub active_installs: usize,
}
impl Default for CompositionLimits {
    fn default() -> Self {
        Self {
            children: 1024,
            active_installs: 16,
        }
    }
}
impl crate::Capabilities {
    pub(crate) fn validate_composition(
        &self,
        manifest: Option<&Value>,
        primary: &ContractId,
    ) -> Result<()> {
        if self.composition.active_installs > Semaphore::MAX_PERMITS {
            return Err(Error::InvalidInput("invalid Composition capacity".into()));
        }
        let mut factories = BTreeSet::new();
        for binding in &self.factories {
            if !valid_alias(&binding.alias) || !factories.insert(binding.alias.clone()) {
                return Err(Error::Contract("invalid or duplicate factory alias".into()));
            }
        }
        let mut expected = json!({"factories": factories, "provisions": {}});
        let mut slots = BTreeSet::new();
        for binding in &self.provisions {
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
    children: Mutex<BTreeMap<u64, Option<WeakPluginHandle>>>,
    installs: Semaphore,
    waits: Semaphore,
    disposals: Semaphore,
}
impl Composition {
    pub fn new(caps: &crate::Capabilities) -> Self {
        Self {
            children: Mutex::default(),
            installs: Semaphore::new(caps.composition.active_installs),
            waits: Semaphore::new(caps.max_callbacks),
            disposals: Semaphore::new(caps.max_callbacks),
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
        if crate::context::in_cleanup() && operation != 5 {
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
                                if children.len() >= router.capabilities.composition.children {
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
                                ctx.install_with(|| factory(args))
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
            2..=5 => {
                let child = self
                    .composition
                    .children
                    .lock()
                    .unwrap()
                    .get(&target)
                    .and_then(Option::as_ref)
                    .and_then(WeakPluginHandle::upgrade)
                    .ok_or(Error::Closed)?;
                if operation == 4 {
                    let _permit = self
                        .composition
                        .disposals
                        .try_acquire()
                        .map_err(|_| Error::Overloaded)?;
                    child.dispose().await?;
                    Value::Null
                } else {
                    let snapshot = if operation == 5 {
                        child.snapshot()
                    } else {
                        let _permit = self
                            .composition
                            .waits
                            .try_acquire()
                            .map_err(|_| Error::Overloaded)?;
                        if operation == 2 {
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
                    json!({"state":state,"retired":snapshot.retired,"outcome":snapshot.outcome.map(|o| format!("{o:?}"))})
                }
            }
            6 => {
                let ctx = self.context(grant)?;
                let binding = self
                    .capabilities
                    .provisions
                    .iter()
                    .find(|p| p.alias == alias)
                    .ok_or_else(|| Error::Refused {
                        code: "provision".into(),
                        message: "undeclared provision".into(),
                    })?;
                ctx.provide(
                    &binding.service,
                    Arc::new(args.one().map_err(|e| Error::InvalidInput(e.to_string()))?),
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
