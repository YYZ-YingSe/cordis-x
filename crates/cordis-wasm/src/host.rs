//! Provider adaptation is independent of the consumer's execution backend.
use crate::{WasmResource, WasmService};
use cordis_host::{Body, Provider, ResourceProvider, WeakResourceProvider};
use cordis_runtime::{BoxFuture, Context};
impl Provider for WasmService {
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
            self.invoke(
                &ctx,
                &method,
                args.one()
                    .map_err(|e| cordis_host::refused("invalid_argument", e))?,
            )
            .await
            .map(Body::from_bytes)
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
            self.acquire(
                &ctx,
                &kind,
                args.one()
                    .map_err(|e| cordis_host::refused("invalid_argument", e))?,
            )
            .await
            .map(cordis_host::Resource::new)
            .map_err(Into::into)
        })
    }
}
impl ResourceProvider for WasmResource {
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
impl WeakResourceProvider for crate::WeakWasmResource {
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
