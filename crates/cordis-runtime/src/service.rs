use crate::{
    BoxFuture, Context, Error, Result, protected,
    runtime::{Authority, Binding},
};
use cordis_contract::ContractId;
use std::{
    marker::PhantomData,
    sync::{Arc, Weak},
};
use tokio::sync::oneshot;

/// A typed native view of an encoding-independent contract.
pub struct Service<T: ?Sized> {
    contract: ContractId,
    descriptor: Option<Arc<cordis_contract::Descriptor>>,
    marker: PhantomData<fn() -> T>,
}

impl<T: ?Sized> Clone for Service<T> {
    fn clone(&self) -> Self {
        Self {
            contract: self.contract.clone(),
            descriptor: self.descriptor.clone(),
            marker: PhantomData,
        }
    }
}

impl<T: ?Sized> Service<T> {
    pub fn new(name: impl Into<Arc<str>>, revision: impl Into<Arc<str>>) -> Self {
        Self {
            contract: ContractId::new(name, revision),
            descriptor: None,
            marker: PhantomData,
        }
    }
    pub fn contract(&self) -> &ContractId {
        &self.contract
    }
    /// Attach an explicit portable interface while retaining typed native access.
    pub fn with_descriptor(mut self, descriptor: cordis_contract::Descriptor) -> Self {
        self.descriptor = Some(Arc::new(descriptor));
        self
    }
    pub fn descriptor(&self) -> Option<&cordis_contract::Descriptor> {
        self.descriptor.as_deref()
    }
}

/// No Deref or raw Arc extraction: every call participates in admission and draining.
pub struct ServiceHandle<T: ?Sized> {
    pub(crate) binding: Arc<Binding>,
    pub(crate) value: Arc<T>,
    pub(crate) context: Context,
}

impl<T: ?Sized> Clone for ServiceHandle<T> {
    fn clone(&self) -> Self {
        Self {
            binding: self.binding.clone(),
            value: self.value.clone(),
            context: self.context.clone(),
        }
    }
}

impl<T: ?Sized + Send + Sync + 'static> ServiceHandle<T> {
    pub fn with<R>(&self, call: impl FnOnce(&T, Context) -> Result<R>) -> Result<R> {
        let mut leases = self.context.call_leases()?;
        leases.push(self.binding.gate.enter(false, self.context.continuing())?);
        let definition = self.binding.definition.upgrade().ok_or(Error::Closed)?;
        let authority = Authority::new(true);
        let (context, pins) = self.context.for_service(definition, authority.clone())?;
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| call(&self.value, context)));
        authority.expire();
        drop(pins);
        drop(leases);
        result.unwrap_or_else(|_| Err(Error::Panic("native service panicked".into())))
    }

    /// The executor owns the call after admission, so dropping the wait does not release its lease.
    pub async fn call<R, F>(&self, call: F) -> Result<R>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a T, Context) -> BoxFuture<'a, Result<R>> + Send + 'static,
    {
        let mut leases = self.context.call_leases()?;
        leases.push(self.binding.gate.enter(false, self.context.continuing())?);
        let definition = self.binding.definition.upgrade().ok_or(Error::Closed)?;
        let authority = Authority::new(true);
        let (context, pins) = self.context.for_service(definition, authority.clone())?;
        let value = self.value.clone();
        let (tx, rx) = oneshot::channel();
        self.context.executor().spawn(async move {
            let result = protected(async move { call(&value, context).await }).await;
            authority.expire();
            drop(pins);
            drop(leases);
            let _ = tx.send(result);
        });
        rx.await.map_err(|_| Error::RuntimeStopped)?
    }
}

/// A scope-owned resource; its value may only be borrowed under a live scope lease.
pub struct Resource<T> {
    pub(crate) value: Arc<T>,
    pub(crate) context: Context,
    pub(crate) gate: Arc<crate::gate::Gate>,
    pub(crate) effect: crate::EffectHandle,
}
impl<T> Clone for Resource<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            context: self.context.clone(),
            gate: self.gate.clone(),
            effect: self.effect.clone(),
        }
    }
}
impl<T: Send + Sync + 'static> Resource<T> {
    /// Adapter registries can address a resource without retaining its owner.
    pub fn downgrade(&self) -> WeakResource<T> {
        WeakResource {
            value: Arc::downgrade(&self.value),
            gate: Arc::downgrade(&self.gate),
            effect: self.effect.downgrade(),
        }
    }
    pub fn with<R>(&self, use_resource: impl FnOnce(&T) -> R) -> Result<R> {
        let _leases = self.context.call_leases()?;
        let _resource = self.gate.enter(false, false)?;
        Ok(use_resource(&self.value))
    }

    /// Borrow under both scope and resource leases. Execution survives cancellation
    /// of the waiting future; manual disposal waits for admitted resource calls.
    pub async fn call<R, F>(&self, call: F) -> Result<R>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a T, Context) -> BoxFuture<'a, Result<R>> + Send + 'static,
    {
        let lease = self.gate.enter(false, false)?;
        let value = self.value.clone();
        self.context
            .operation(move |context| async move {
                let _lease = lease;
                call(&value, context).await
            })
            .await
    }

    pub async fn dispose(&self) -> Result<()> {
        self.effect.dispose().await
    }
    pub fn snapshot(&self) -> crate::EffectSnapshot {
        self.effect.snapshot()
    }
}

/// A resource reference that does not retain its value or owning scope.
/// Upgrading preserves the original Context and never reopens a closed resource.
pub struct WeakResource<T> {
    value: Weak<T>,
    gate: Weak<crate::gate::Gate>,
    effect: crate::WeakEffectHandle,
}
impl<T> Clone for WeakResource<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            gate: self.gate.clone(),
            effect: self.effect.clone(),
        }
    }
}
impl<T> WeakResource<T> {
    /// Observe/dispose the original effect even after the business value drops.
    pub fn effect(&self) -> Option<crate::EffectHandle> {
        self.effect.upgrade()
    }
    pub fn upgrade(&self) -> Option<Resource<T>> {
        let effect = self.effect.upgrade()?;
        Some(Resource {
            value: self.value.upgrade()?,
            gate: self.gate.upgrade()?,
            context: effect.context.clone(),
            effect,
        })
    }
}
