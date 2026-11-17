use crate::runtime::ActivationData;
use crate::{Context, EffectSnapshot, Error, Plugin, PluginHandle, Result, View};
use cordis_kernel::{Activation, FiberId, Snapshot};
use std::{
    future::Future,
    sync::{Arc, Mutex, OnceLock, Weak},
};

struct Definition {
    source: Weak<ActivationData>,
    activation: OnceLock<Activation>,
    view: View,
    caller: Option<Activation>,
}

/// A single-generation child owner. Each run receives fresh callback authority;
/// dependencies and metadata still come from the creating definition/use site.
#[derive(Clone)]
pub struct ScopeHandle {
    plugin: PluginHandle,
    definition: Arc<Definition>,
}

#[derive(Clone)]
pub struct WeakScopeHandle {
    plugin: crate::WeakPluginHandle,
    definition: Arc<Definition>,
}

impl Context {
    pub fn scope(&self, label: impl Into<String>) -> Result<ScopeHandle> {
        // A scope installs only the fixed owner initializer below, not an
        // arbitrary plugin factory. Retain dynamic implementations before the
        // invoking callback ends, and release them after child recovery. This
        // lets reference and middleware implementations group resources without
        // losing their original implementation or bypassing recovery cycles.
        let (_, pins) = self.retain_dynamic(true)?;
        let pins = Arc::new(Mutex::new(Some(pins)));
        let definition = Arc::new(Definition {
            source: Arc::downgrade(&self.definition),
            activation: OnceLock::new(),
            view: self.view().snapshot(),
            caller: self.caller(),
        });
        let start = definition.clone();
        // A scope inherits the exact defining view, independently of use-side
        // configuration. Its dependency edges use the existing child algorithm.
        let mut parent = self.derive(self.definition.view.clone());
        // The durable pins above replace the temporary call pins for this
        // fixed initializer. General child-plugin installation stays guarded.
        parent.dynamic.clear();
        let plugin = parent.install(Plugin::new(label, move |ctx| {
            let start = start.clone();
            let pins = pins.lock().unwrap().take().ok_or(Error::Closed);
            async move {
                let pins = pins?;
                if !pins.0.is_empty() {
                    ctx.defer("scope implementation lifetime", move |_| async move {
                        drop(pins);
                        Ok(())
                    })?;
                }
                start
                    .activation
                    .set(ctx.owner())
                    .map_err(|_| Error::Closed)?;
                Ok(())
            }
        }))?;
        Ok(ScopeHandle { plugin, definition })
    }
}

impl ScopeHandle {
    pub fn id(&self) -> FiberId {
        self.plugin.id()
    }
    pub fn snapshot(&self) -> Snapshot {
        self.plugin.snapshot()
    }
    pub fn effects(&self) -> Vec<EffectSnapshot> {
        self.plugin.effects()
    }
    pub async fn ready(&self) -> Result<Snapshot> {
        self.plugin.ready().await
    }
    pub async fn dispose(&self) -> Result<()> {
        self.plugin.dispose().await
    }
    pub fn downgrade(&self) -> WeakScopeHandle {
        WeakScopeHandle {
            plugin: self.plugin.downgrade(),
            definition: self.definition.clone(),
        }
    }

    /// Cancellation drops only the wait. Admission and all callback work belong
    /// to the child; closing the parent first withdraws and drains this child.
    pub async fn run<F, Fut, T>(&self, callback: F) -> Result<T>
    where
        F: FnOnce(Context) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        let snapshot = self.ready().await?;
        let activation = self
            .definition
            .activation
            .get()
            .copied()
            .ok_or(Error::Closed)?;
        if snapshot.activation != Some(activation) {
            return Err(Error::Closed);
        }
        let owner = self
            .plugin
            .inner
            .state
            .lock()
            .unwrap()
            .activations
            .get(&activation)
            .cloned()
            .ok_or(Error::Closed)?;
        let mut ctx = Context::base(&self.plugin.inner, owner, None);
        ctx.view = self.definition.view.clone();
        ctx.caller = self.definition.caller;
        // Ownership belongs to the child; executable code and dependency
        // identity still belong to the definition that created the scope.
        // This distinction matters when a service creates a scope beneath a
        // different caller: the caller's directory envelope must not change the
        // service's installed identity. No stale callback authority is reused.
        let source = self.definition.source.upgrade().ok_or(Error::Closed)?;
        let implementation = source.gate.enter(true, false)?;
        ctx.definition = source;
        ctx.operation(move |ctx| async move {
            let _implementation = implementation;
            callback(ctx).await
        })
        .await
    }
}
impl WeakScopeHandle {
    pub fn is_retired(&self) -> bool {
        self.plugin.is_retired()
    }
    pub fn upgrade(&self) -> Option<ScopeHandle> {
        Some(ScopeHandle {
            plugin: self.plugin.upgrade()?,
            definition: self.definition.clone(),
        })
    }
}
