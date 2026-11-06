use crate::{Body, Result};
use cordis_runtime::BoxFuture;
use std::sync::Arc;

/// Implemented by an adapter around an existing Runtime resource capability.
/// The adapter must retain the original owner's admission and cleanup semantics.
pub trait ResourceProvider: Send + Sync + 'static {
    fn invoke(&self, method: String, args: Body) -> BoxFuture<'_, Result<Body>>;
    fn dispose(&self) -> BoxFuture<'_, Result<()>>;
    fn downgrade(&self) -> WeakResource;
}
/// A weak Runtime capability, not a weak pointer to this facade. It must remain
/// usable while the original Runtime owns the ticket, without pinning its owner.
pub trait WeakResourceProvider: Send + Sync + 'static {
    /// Confirmed successful release, including the Runtime cleanup barrier.
    fn is_released(&self) -> bool;
    fn invoke(&self, method: String, args: Body) -> BoxFuture<'_, Result<Body>>;
    fn dispose(&self) -> BoxFuture<'_, Result<()>>;
}
#[derive(Clone)]
pub struct Resource(Arc<dyn ResourceProvider>);
#[derive(Clone)]
pub struct WeakResource(Arc<dyn WeakResourceProvider>);
impl Resource {
    pub fn new(value: impl ResourceProvider) -> Self {
        Self(Arc::new(value))
    }
    pub async fn invoke(&self, method: &str, args: Body) -> Result<Body> {
        self.0.invoke(method.into(), args).await
    }
    pub async fn dispose(&self) -> Result<()> {
        self.0.dispose().await
    }
    pub fn downgrade(&self) -> WeakResource {
        self.0.downgrade()
    }
}
impl WeakResource {
    pub fn is_released(&self) -> bool {
        self.0.is_released()
    }
    pub fn new(value: impl WeakResourceProvider) -> Self {
        Self(Arc::new(value))
    }
    pub async fn invoke(&self, method: &str, args: Body) -> Result<Body> {
        self.0.invoke(method.into(), args).await
    }
    pub async fn dispose(&self) -> Result<()> {
        self.0.dispose().await
    }
}

impl WeakResource {
    pub async fn invoke_bytes(
        &self,
        method: &str,
        args: cordis_wire::Bytes,
    ) -> Result<cordis_wire::Bytes> {
        let input = cordis_wire::local::Envelope::from(cordis_wire::Payload::message(args));
        self.invoke(method, input.into())
            .await?
            .into_envelope()
            .map_err(crate::unknown)?
            .one()
            .map_err(crate::unknown)
    }
}
