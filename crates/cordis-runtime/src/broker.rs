use crate::{
    BoxFuture, Context, EffectSnapshot, Error, Result, Service, ServiceHandle, WeakEffectHandle,
    gate::Gate, runtime::ActivationData,
};
use cordis_kernel::Activation;
use std::sync::{Arc, Mutex, Weak};

struct Endpoint<T: ?Sized> {
    value: Arc<T>,
    definition: Weak<ActivationData>,
    gate: Arc<Gate>,
}

/// Stable, typed service routing. Install with `Context::provide_broker`.
/// Implementations require this service and register themselves; the Broker
/// never requires an implementation back.
pub struct Broker<T: ?Sized> {
    route: Mutex<Option<Arc<Endpoint<T>>>>,
}

/// A candidate is invisible until explicitly promoted after its activation is
/// Active. Clones share the same revocation and cleanup result.
pub struct BrokerRegistration<T: ?Sized> {
    broker: Weak<Broker<T>>,
    endpoint: Arc<Endpoint<T>>,
    effect: WeakEffectHandle,
}
/// An observation token that does not keep candidate code or its value alive.
pub struct WeakBrokerRegistration<T: ?Sized> {
    broker: Weak<Broker<T>>,
    endpoint: Weak<Endpoint<T>>,
    effect: WeakEffectHandle,
}
impl<T: ?Sized> Clone for WeakBrokerRegistration<T> {
    fn clone(&self) -> Self {
        Self {
            broker: self.broker.clone(),
            endpoint: self.endpoint.clone(),
            effect: self.effect.clone(),
        }
    }
}
impl<T: ?Sized> WeakBrokerRegistration<T> {
    pub fn upgrade(&self) -> Option<BrokerRegistration<T>> {
        self.effect.upgrade()?;
        Some(BrokerRegistration {
            broker: self.broker.clone(),
            endpoint: self.endpoint.upgrade()?,
            effect: self.effect.clone(),
        })
    }
}
impl<T: ?Sized> Clone for BrokerRegistration<T> {
    fn clone(&self) -> Self {
        Self {
            broker: self.broker.clone(),
            endpoint: self.endpoint.clone(),
            effect: self.effect.clone(),
        }
    }
}

impl Context {
    pub fn provide_broker<T: ?Sized + Send + Sync + 'static>(
        &self,
        service: &Service<Broker<T>>,
    ) -> Result<()> {
        self.provide(
            service,
            Arc::new(Broker {
                route: Mutex::new(None),
            }),
        )
    }
}

impl<T: ?Sized + Send + Sync + 'static> ServiceHandle<Broker<T>> {
    /// Register from the implementing activation, not a forwarded use-site
    /// Context. The committed service handle determines runtime and realm.
    pub fn register(&self, value: Arc<T>) -> Result<BrokerRegistration<T>> {
        let context = &self.context;
        if context.cleanup || context.owner() != context.activation() || !context.dynamic.is_empty()
        {
            return Err(Error::Closed);
        }
        let _leases = context.call_leases()?;
        let _broker = self.binding.gate.enter(false, context.continuing())?;
        let endpoint = Arc::new(Endpoint {
            value,
            definition: Arc::downgrade(&context.definition),
            gate: Gate::new(),
        });
        endpoint.gate.activate();
        let broker = Arc::downgrade(&self.value);
        let cleanup_broker = broker.clone();
        let cleanup_endpoint = endpoint.clone();
        let effect = context.defer("Broker endpoint", move |_| async move {
            withdraw(&cleanup_broker, &cleanup_endpoint);
            cleanup_endpoint.gate.drained().await;
            cleanup_endpoint.gate.finish();
            Ok(())
        })?;
        Ok(BrokerRegistration {
            broker,
            endpoint,
            effect: effect.downgrade(),
        })
    }

    /// Select once per admitted request. Cancellation drops only the wait.
    /// Effects/resources created by the callback retain the selected provider
    /// until their inverses succeed; they do not follow subsequent promotions.
    pub async fn route<R, F>(&self, call: F) -> Result<R>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a T, Context) -> BoxFuture<'a, Result<R>> + Send + 'static,
    {
        self.invoke(None, call).await
    }

    /// Call this unpublished candidate through the same admission and resource
    /// ownership rules as routing. The candidate must belong to this exact Broker
    /// and be Active. Dropping the wait does not cancel admitted work.
    pub async fn probe<R, F>(&self, candidate: &BrokerRegistration<T>, call: F) -> Result<R>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a T, Context) -> BoxFuture<'a, Result<R>> + Send + 'static,
    {
        let broker = candidate.broker.upgrade().ok_or(Error::Closed)?;
        if !Arc::ptr_eq(&broker, &self.value) {
            return Err(Error::BrokerMismatch);
        }
        self.invoke(Some(candidate.endpoint.clone()), call).await
    }

    async fn invoke<R, F>(&self, candidate: Option<Arc<Endpoint<T>>>, call: F) -> Result<R>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a T, Context) -> BoxFuture<'a, Result<R>> + Send + 'static,
    {
        self.call(move |broker, context| {
            Box::pin(async move {
                // Selection and old-endpoint closure are serialized by this lock.
                let (endpoint, definition, lease, admission) = {
                    let route = broker.route.lock().unwrap();
                    let endpoint = candidate
                        .or_else(|| route.clone())
                        .ok_or(Error::BrokerUnavailable)?;
                    let admission = endpoint.gate.enter(false, false)?;
                    let definition = endpoint.definition.upgrade().ok_or(Error::Closed)?;
                    let lease = definition.gate.enter(false, false)?;
                    (endpoint, definition, lease, admission)
                };
                let authority = crate::runtime::Authority::new(true);
                let (mut context, mut pins) =
                    context.for_service(definition.clone(), authority.clone())?;
                context.retain_provider(&definition, lease, &mut pins)?;
                let result =
                    crate::protected(async move { call(&endpoint.value, context).await }).await;
                authority.expire();
                drop(pins);
                drop(admission);
                result
            })
        })
        .await
    }
}

impl<T: ?Sized + Send + Sync + 'static> BrokerRegistration<T> {
    pub fn downgrade(&self) -> WeakBrokerRegistration<T> {
        WeakBrokerRegistration {
            broker: self.broker.clone(),
            endpoint: Arc::downgrade(&self.endpoint),
            effect: self.effect.clone(),
        }
    }
    pub fn activation(&self) -> Option<Activation> {
        self.endpoint
            .definition
            .upgrade()
            .map(|data| data.activation)
    }

    /// Atomically select this Active candidate and close admission to the old
    /// endpoint. Withdrawal is irreversible; register a new candidate to reuse it.
    pub fn promote(&self) -> Result<()> {
        self.promote_when(|_| Ok(()))
    }

    /// Compare and promote under the route lock. `None` requires an empty route.
    /// A stale deployment decision cannot overwrite another controller's route.
    pub fn promote_if_current(&self, previous: Option<&Self>) -> Result<()> {
        self.promote_when(|route| match (route.as_ref(), previous) {
            (None, None) => Ok(()),
            (Some(actual), Some(expected)) if Arc::ptr_eq(actual, &expected.endpoint) => Ok(()),
            _ => Err(Error::BrokerChanged),
        })
    }

    fn promote_when(
        &self,
        check: impl FnOnce(&Option<Arc<Endpoint<T>>>) -> Result<()>,
    ) -> Result<()> {
        let effect = self.effect.upgrade().ok_or(Error::Closed)?;
        let inner = effect
            .context
            .inner
            .upgrade()
            .ok_or(Error::RuntimeStopped)?;
        // Publication/withdrawal and this Active check share the coordinator.
        let _state = inner.state.lock().unwrap();
        let broker = self.broker.upgrade().ok_or(Error::Closed)?;
        let mut route = broker.route.lock().unwrap();
        check(&route)?;
        let _endpoint = self.endpoint.gate.enter(false, false)?;
        let definition = self.endpoint.definition.upgrade().ok_or(Error::Closed)?;
        let _provider = definition.gate.enter(false, false)?;
        if route
            .as_ref()
            .is_some_and(|old| Arc::ptr_eq(old, &self.endpoint))
        {
            return Ok(());
        }
        let previous = route.replace(self.endpoint.clone());
        if let Some(old) = &previous {
            old.gate.close();
        }
        drop(route);
        drop(_state);
        drop(previous);
        Ok(())
    }

    /// Stop new requests immediately. Existing requests and resources keep their
    /// selected implementation. Retire its PluginHandle to await full recovery.
    pub fn withdraw(&self) {
        withdraw(&self.broker, &self.endpoint);
    }

    pub async fn dispose(&self) -> Result<()> {
        self.withdraw();
        match self.effect.upgrade() {
            Some(effect) => effect.dispose().await,
            None => Ok(()),
        }
    }

    pub fn snapshot(&self) -> Option<EffectSnapshot> {
        self.effect.upgrade().map(|e| e.snapshot())
    }
}

fn withdraw<T: ?Sized>(broker: &Weak<Broker<T>>, endpoint: &Arc<Endpoint<T>>) {
    let removed = if let Some(broker) = broker.upgrade() {
        let mut route = broker.route.lock().unwrap();
        if endpoint.gate.phase() == crate::gate::Phase::Active {
            endpoint.gate.close();
        }
        if route
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, endpoint))
        {
            route.take()
        } else {
            None
        }
    } else {
        if endpoint.gate.phase() == crate::gate::Phase::Active {
            endpoint.gate.close();
        }
        None
    };
    drop(removed);
}
