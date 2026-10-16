use crate::runtime::{ActivationData, Inner};
use crate::{Context, EffectSnapshot, Error, Plugin, PluginHandle, Result, View};
use cordis_kernel::{Activation, FiberId, Snapshot};
use std::{
    future::Future,
    sync::{Arc, OnceLock, Weak},
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
    inner: Weak<Inner>,
    id: FiberId,
    definition: Arc<Definition>,
}

impl Context {
    pub fn scope(&self, label: impl Into<String>) -> Result<ScopeHandle> {
        let definition = Arc::new(Definition {
            source: Arc::downgrade(&self.definition),
            activation: OnceLock::new(),
            view: self.view().snapshot(),
            caller: self.caller(),
        });
        let start = definition.clone();
        // A scope inherits the exact defining view, independently of use-side
        // configuration. Its dependency edges use the existing child algorithm.
        let plugin = self
            .derive(self.definition.view.clone())
            .install(Plugin::new(label, move |ctx| {
                let start = start.clone();
                async move {
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
            inner: Arc::downgrade(&self.plugin.inner),
            id: self.id(),
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
        // The child has its own definition and inherited committed bindings,
        // just like Context::install. Also pin the originating implementation,
        // including while it initializes, without granting it a stale Context.
        let source = self.definition.source.upgrade().ok_or(Error::Closed)?;
        let implementation = source.gate.enter(true, false)?;
        ctx.operation(move |ctx| async move {
            let _implementation = implementation;
            callback(ctx).await
        })
        .await
    }
}
impl WeakScopeHandle {
    pub fn upgrade(&self) -> Option<ScopeHandle> {
        Some(ScopeHandle {
            plugin: PluginHandle {
                id: self.id,
                inner: self.inner.upgrade()?,
                waiter: None,
            },
            definition: self.definition.clone(),
        })
    }
}
