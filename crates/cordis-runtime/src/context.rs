use crate::{
    EffectHandle, Error, Plugin, PluginHandle, Resource, Result, Service, ServiceHandle, View,
    effects::Effect,
    gate::{Gate, Lease, Phase},
    protected,
    runtime::{ActivationData, Authority, Binding, Inner},
};
use cordis_kernel::{Activation, Event};
use std::{
    any::Any,
    future::Future,
    sync::{Arc, Weak, atomic::Ordering},
};
use tokio::{
    runtime::Handle,
    sync::{oneshot, watch},
};

/// Definition-side dependencies and use-side effects are deliberately separate.
#[derive(Clone)]
pub struct Context {
    pub(crate) inner: Weak<Inner>,
    pub(crate) definition: Arc<ActivationData>,
    pub(crate) owner: Arc<ActivationData>,
    pub(crate) view: View,
    pub(crate) caller: Option<Activation>,
    pub(crate) authority: Option<Arc<Authority>>,
    pub(crate) cleanup: bool,
    pub(crate) dynamic: Vec<Arc<crate::retention::Pin>>,
    cleanup_barriers: Vec<Arc<Gate>>,
    execution_barriers: Vec<Arc<Gate>>,
    executor: Handle,
}

impl Context {
    pub(crate) fn base(
        inner: &Arc<Inner>,
        data: Arc<ActivationData>,
        authority: Option<Arc<Authority>>,
    ) -> Self {
        Self {
            inner: Arc::downgrade(inner),
            definition: data.clone(),
            owner: data.clone(),
            view: data.view.clone(),
            caller: None,
            authority,
            cleanup: false,
            dynamic: Vec::new(),
            cleanup_barriers: Vec::new(),
            execution_barriers: Vec::new(),
            executor: inner.executor.clone(),
        }
    }

    pub fn activation(&self) -> Activation {
        self.definition.activation
    }
    pub fn owner(&self) -> Activation {
        self.owner.activation
    }
    pub fn caller(&self) -> Option<Activation> {
        self.caller
    }
    /// Whether this context carries authority for an inverse/cleanup callback.
    pub fn is_cleanup(&self) -> bool {
        self.cleanup
    }
    pub fn view(&self) -> View {
        self.view.clone()
    }

    /// Pin use configuration for a callback or resource without changing authority.
    pub fn snapshot(&self) -> Self {
        Self {
            view: self.view.snapshot(),
            ..self.clone()
        }
    }

    pub fn metadata_revision(&self) -> Option<u64> {
        self.view.metadata_revision()
    }

    pub fn derive(&self, view: View) -> Self {
        Self {
            view,
            ..self.clone()
        }
    }

    pub fn metadata<T: ?Sized, M: Any + Send + Sync>(
        &self,
        service: &Service<T>,
    ) -> Result<Vec<Arc<M>>> {
        self.check_authority()?;
        self.view.metadata(service)
    }

    pub(crate) fn executor(&self) -> &Handle {
        &self.executor
    }

    pub(crate) fn continuing(&self) -> bool {
        self.authority.as_ref().is_some_and(|a| a.valid())
    }

    pub(crate) fn check_authority(&self) -> Result<()> {
        if self.dynamic.iter().any(|pin| !pin.live())
            || self
                .authority
                .as_ref()
                .is_some_and(|a| a.strict && !a.valid())
        {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }

    pub(crate) fn enter_owner(&self) -> Result<Lease> {
        self.check_authority()?;
        self.owner.gate.enter(true, self.continuing())
    }

    pub(crate) fn cleanup_context(&self) -> (Self, Arc<Authority>, Arc<Gate>) {
        let authority = Authority::new(true);
        let barrier = Gate::new();
        barrier.activate();
        let mut cleanup_barriers = self.cleanup_barriers.clone();
        cleanup_barriers.push(barrier.clone());
        (
            Self {
                authority: Some(authority.clone()),
                cleanup: true,
                cleanup_barriers,
                execution_barriers: Vec::new(),
                ..self.clone()
            },
            authority,
            barrier,
        )
    }

    pub(crate) fn for_service(
        &self,
        definition: Arc<ActivationData>,
        authority: Arc<Authority>,
    ) -> Result<(Self, crate::retention::Pins)> {
        let (retained, pins) = self.retain_dynamic(false)?;
        Ok((
            Self {
                caller: Some(self.definition.activation),
                definition,
                authority: Some(authority),
                view: retained.view.snapshot(),
                ..retained
            },
            pins,
        ))
    }

    pub(crate) fn with_barrier(mut self, gate: Arc<Gate>) -> Self {
        self.execution_barriers.push(gate);
        self
    }

    pub(crate) fn operation_context(
        &self,
    ) -> Result<(Self, Arc<Authority>, crate::retention::Pins)> {
        let (retained, pins) = self.retain_dynamic(false)?;
        let authority = Authority::new(true);
        Ok((
            Self {
                authority: Some(authority.clone()),
                view: retained.view.snapshot(),
                ..retained
            },
            authority,
            pins,
        ))
    }

    // Returned resources and disposal tokens belong to the use scope. Their ordinary
    // use outlives the creating call, but does not inherit its closing continuation.
    pub(crate) fn owned_context(&self) -> Self {
        Self {
            authority: None,
            execution_barriers: Vec::new(),
            ..self.clone()
        }
    }

    pub fn dependency<T: ?Sized + Send + Sync + 'static>(
        &self,
        service: &Service<T>,
    ) -> Result<ServiceHandle<T>> {
        self.check_authority()?;
        if self.definition.gate.phase() == Phase::Closed {
            return Err(Error::Closed);
        }
        let binding = self
            .definition
            .bindings
            .get(service.contract())
            .ok_or_else(|| Error::UndeclaredDependency(service.contract().clone()))?
            .clone();
        match (service.descriptor(), binding.descriptor.as_ref()) {
            (Some(expected), Some(actual)) => expected
                .compare(actual)
                .map_err(|e| Error::Message(format!("contract {}: {e}", service.contract())))?,
            (None, None) => {}
            _ => {
                return Err(Error::Message(format!(
                    "contract {}: typed and described bindings differ",
                    service.contract()
                )));
            }
        }
        let value = binding
            .value
            .downcast_ref::<Arc<T>>()
            .ok_or_else(|| Error::TypeMismatch(service.contract().clone()))?
            .clone();
        Ok(ServiceHandle {
            binding,
            value,
            context: self.clone(),
        })
    }

    /// Provision is staged until this initializer succeeds; T may be a trait object.
    pub fn provide<T: ?Sized + Send + Sync + 'static>(
        &self,
        service: &Service<T>,
        value: Arc<T>,
    ) -> Result<()> {
        if self.cleanup || self.definition.activation != self.owner.activation {
            return Err(Error::Closed);
        }
        let _lease = self.enter_owner()?;
        let inner = self.inner.upgrade().ok_or(Error::RuntimeStopped)?;
        let slot = self.definition.provision_view.slot(service.contract());
        let mut state = inner.state.lock().unwrap();
        let actions = state.kernel.step(Event::Provided {
            activation: self.definition.activation,
            slot: slot.clone(),
        })?;
        debug_assert!(actions.is_empty());
        let binding = Arc::new(Binding {
            descriptor: service.descriptor().cloned(),
            value: Arc::new(value),
            definition: Arc::downgrade(&self.definition),
            gate: self.definition.gate.clone(),
        });
        state
            .services
            .insert((self.definition.activation, slot.contract), binding);
        drop(state);
        inner.notify();
        Ok(())
    }

    pub fn defer<F, Fut>(&self, label: impl Into<String>, cleanup: F) -> Result<EffectHandle>
    where
        F: FnOnce(Context) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        if self.cleanup {
            return Err(Error::Closed);
        }
        let _lease = self.enter_owner()?;
        let effect = Effect::new(
            self.owner.next_effect.fetch_add(1, Ordering::Relaxed),
            label.into(),
            self.clone(),
            Box::new(move |ctx| Box::pin(cleanup(ctx))),
        )?;
        self.owner.effects.lock().unwrap().push(effect.clone());
        Ok(EffectHandle {
            context: effect.context().owned_context(),
            effect,
        })
    }

    /// Acquisition and registration belong to the executor even when this future is dropped.
    /// On Err, the acquisition itself must have left no resource requiring recovery.
    pub async fn acquire<T, A, D, Fut>(
        &self,
        label: impl Into<String>,
        acquire: A,
        release: D,
    ) -> Result<Resource<T>>
    where
        T: Send + Sync + 'static,
        A: Future<Output = Result<T>> + Send + 'static,
        D: FnOnce(Arc<T>, Context) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        if self.cleanup {
            return Err(Error::Closed);
        }
        let lease = self.enter_owner()?;
        // Reject recovery cycles before executing acquisition, when no external
        // resource responsibility has yet been incurred.
        let (retained, reservation) = self.retain_dynamic(true)?;
        let (context, authority, pins) = retained.operation_context()?;
        let label = label.into();
        let (tx, rx) = oneshot::channel();
        self.executor.spawn(async move {
            let result = protected(async {
                let value = Arc::new(acquire.await?);
                let cleanup_value = value.clone();
                let gate = crate::gate::Gate::new();
                gate.activate();
                let cleanup_gate = gate.clone();
                let effect = context.defer(label, move |ctx| async move {
                    cleanup_gate.close();
                    cleanup_gate.drained().await;
                    cleanup_gate.recover();
                    release(cleanup_value, ctx).await?;
                    cleanup_gate.finish();
                    Ok(())
                })?;
                Ok((value, gate, effect))
            })
            .await;
            authority.expire();
            drop(pins);
            drop(reservation);
            drop(lease);
            let _ = tx.send(result);
        });
        let (value, gate, effect) = rx.await.map_err(|_| Error::RuntimeStopped)??;
        Ok(Resource {
            value,
            gate,
            context: effect.context.clone(),
            effect,
        })
    }

    /// Children inherit definition-side declared dependencies while their realms
    /// agree. Their own declarations shadow inherited bindings.
    pub fn install(&self, plugin: Plugin) -> Result<PluginHandle> {
        self.install_with(|| Ok(plugin))
    }

    /// Validate installation authority before running a synchronous definition
    /// factory. The factory must not acquire resources requiring recovery.
    /// Blocking factories should be called from an adapter's blocking worker.
    pub fn install_with<F>(&self, factory: F) -> Result<PluginHandle>
    where
        F: FnOnce() -> Result<Plugin>,
    {
        if !self.dynamic.is_empty() {
            return Err(cordis_contract::refused("broker_install", "child installation through a Broker call is not supported; install from the caller scope").into());
        }
        if self.cleanup {
            return Err(Error::Closed);
        }
        let _lease = self.enter_owner()?;
        // A definition factory runs before child registration. A returned
        // native error or panic here has not installed a child. Explicit
        // portable outcomes retain their certainty and business category.
        let plugin = std::panic::catch_unwind(std::panic::AssertUnwindSafe(factory))
            .map_err(|_| {
                Error::from(cordis_contract::refused(
                    "factory_panicked",
                    "definition factory panicked before child registration",
                ))
            })?
            .map_err(|error| match error {
                Error::Portable(_) => error,
                other => cordis_contract::refused("factory_rejected", other).into(),
            })?;
        self.inner.upgrade().ok_or(Error::RuntimeStopped)?.install(
            self.view.clone(),
            plugin,
            Some(self.owner.activation),
            Some(self.definition.activation),
        )
    }

    pub async fn cancelled(&self) {
        tokio::select! {
            _ = self.owner.gate.cancelled() => {},
            _ = self.definition.gate.cancelled() => {},
        }
    }

    /// Execute an admitted adapter operation with owner/definition leases retained
    /// even if its waiter is dropped (including a timeout inside a service method).
    pub async fn operation<F, Fut, T>(&self, operation: F) -> Result<T>
    where
        F: FnOnce(Context) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        let mut leases = self.call_leases()?;
        if self.definition.activation != self.owner.activation {
            leases.push(self.definition_lease()?);
        }
        let (context, authority, pins) = self.operation_context()?;
        let (tx, rx) = oneshot::channel();
        self.executor.spawn(async move {
            let result = protected(async move { operation(context).await }).await;
            authority.expire();
            drop(pins);
            drop(leases);
            let _ = tx.send(result);
        });
        rx.await.map_err(|_| Error::RuntimeStopped)?
    }

    /// Report loss of this activation's execution backend. Withdrawal cascades to
    /// consumers; recovery still observes the usual resource and call barriers.
    pub fn fail(&self, message: impl Into<String>) -> Result<()> {
        self.check_authority()?;
        self.inner
            .upgrade()
            .ok_or(Error::RuntimeStopped)?
            .event(Event::Fault {
                activation: self.definition.activation,
                message: message.into(),
            })
    }

    /// Managed tasks must cooperate with `cancelled()`. Their errors remain in effect diagnostics.
    pub fn spawn<F, Fut>(&self, label: impl Into<String>, task: F) -> Result<TaskHandle>
    where
        F: FnOnce(Context) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        if self.cleanup {
            return Err(Error::Closed);
        }
        let mut leases = vec![self.enter_owner()?];
        if self.definition.activation != self.owner.activation {
            leases.push(self.definition_lease()?);
        }
        let (result, rx) = watch::channel(None);
        let handle = TaskHandle { result: rx };
        let cleanup_handle = handle.clone();
        self.defer(label, move |_| async move { cleanup_handle.join().await })?;
        let (mut context, authority, pins) = self.operation_context()?;
        context.execution_barriers.clear();
        self.executor.spawn(async move {
            let outcome = protected(async move { task(context).await }).await;
            authority.expire();
            drop(pins);
            result.send_replace(Some(outcome));
            drop(leases);
        });
        Ok(handle)
    }

    pub(crate) fn call_leases(&self) -> Result<Vec<Lease>> {
        self.check_authority()?;
        let mut leases = if self.cleanup {
            if !self.continuing()
                || !matches!(
                    self.owner.gate.phase(),
                    Phase::Loading | Phase::Active | Phase::Closing | Phase::Recovering
                )
            {
                return Err(Error::Closed);
            }
            // Count cleanup continuations too: dropping their wait must not allow earlier inverses to run.
            let lease = self.owner.gate.enter_cleanup()?;
            // Recheck after admission: expiration cannot race with a zero-lease
            // observation and let a stale cleanup start after the next inverse.
            self.check_authority()?;
            vec![lease]
        } else {
            vec![self.enter_owner()?]
        };
        for barrier in self.cleanup_barriers.iter().chain(&self.execution_barriers) {
            leases.push(barrier.enter(false, self.continuing())?);
        }
        for pin in &self.dynamic {
            leases.push(pin.enter()?);
        }
        // An authority may expire between evaluating continuation and entering a
        // barrier. Recheck after admission so no late caller starts user code
        // after that inverse has observed its barrier drained.
        self.check_authority()?;
        Ok(leases)
    }
}

#[derive(Clone)]
pub struct TaskHandle {
    result: watch::Receiver<Option<Result<()>>>,
}
impl TaskHandle {
    /// Observe a completed task without waiting or consuming its cached result.
    pub fn outcome(&self) -> Option<Result<()>> {
        self.result.borrow().clone()
    }
    pub async fn join(&self) -> Result<()> {
        let mut result = self.result.clone();
        loop {
            if let Some(outcome) = result.borrow().clone() {
                return outcome;
            }
            result.changed().await.map_err(|_| Error::RuntimeStopped)?;
        }
    }
}
