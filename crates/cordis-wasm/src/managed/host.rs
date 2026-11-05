//! Provider adaptation is independent of the consumer's execution backend.
use super::{ContextResource, ContextService};
use cordis_host::{Body, Provider, ResourceProvider, WeakResourceProvider};
use cordis_runtime::{BoxFuture, Context};
impl Provider for ContextService {
    const SUPPORTS_RESOURCES: bool = true;
    fn invoke(
        &self,
        ctx: Context,
        method: String,
        args: Body,
    ) -> BoxFuture<'_, cordis_host::Result<Body>> {
        Box::pin(async move {
            let args = args
                .into_envelope()
                .map_err(|e| cordis_host::refused("invalid_argument", e))?;
            self.invoke_envelope(&ctx, &method, args)
                .await
                .map(Into::into)
                .map_err(Into::into)
        })
    }
    fn acquire(
        &self,
        ctx: Context,
        kind: String,
        args: Body,
    ) -> BoxFuture<'_, cordis_host::Result<cordis_host::Resource>> {
        Box::pin(async move {
            let args = args
                .into_envelope()
                .map_err(|e| cordis_host::refused("invalid_argument", e))?;
            self.acquire_envelope(&ctx, &kind, args)
                .await
                .map(cordis_host::Resource::new)
                .map_err(Into::into)
        })
    }
}
impl ResourceProvider for ContextResource {
    fn invoke(&self, method: String, args: Body) -> BoxFuture<'_, cordis_host::Result<Body>> {
        Box::pin(async move {
            let args = args
                .into_envelope()
                .map_err(|e| cordis_host::refused("invalid_argument", e))?;
            self.invoke_envelope(&method, args)
                .await
                .map(Into::into)
                .map_err(crate::host_failure::resource_call)
        })
    }
    fn dispose(&self) -> BoxFuture<'_, cordis_host::Result<()>> {
        Box::pin(async move { self.dispose().await.map_err(Into::into) })
    }
    fn downgrade(&self) -> cordis_host::WeakResource {
        cordis_host::WeakResource::new(self.downgrade())
    }
}
impl WeakResourceProvider for super::resources::WeakContextResource {
    fn invoke(&self, method: String, args: Body) -> BoxFuture<'_, cordis_host::Result<Body>> {
        Box::pin(async move {
            let args = args
                .into_envelope()
                .map_err(|e| cordis_host::refused("invalid_argument", e))?;
            self.invoke_envelope(&method, args)
                .await
                .map(Into::into)
                .map_err(crate::host_failure::resource_call)
        })
    }
    fn dispose(&self) -> BoxFuture<'_, cordis_host::Result<()>> {
        Box::pin(async move { self.dispose().await.map_err(Into::into) })
    }
}
