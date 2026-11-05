//! Adapt the local executor without serializing native message objects.
use super::*;
use cordis_host::{Body, Provider, ResourceProvider, WeakResourceProvider};
impl Provider for Service {
    const SUPPORTS_RESOURCES: bool = true;
    fn invoke(
        &self,
        ctx: rt::Context,
        method: String,
        args: Body,
    ) -> rt::BoxFuture<'_, Result<Body>> {
        Box::pin(async move {
            let signature =
                cordis_contract::portable::method(&self.descriptor.interface().methods, &method)
                    .map_err(|e| refused("unknown_method", e))?;
            let args = args
                .into_message(&signature.args)
                .map_err(|e| refused("invalid_argument", e))?;
            self.invoke(&ctx, &method, args).await.map(Into::into)
        })
    }
    fn acquire(
        &self,
        ctx: rt::Context,
        kind: String,
        args: Body,
    ) -> rt::BoxFuture<'_, Result<cordis_host::Resource>> {
        Box::pin(async move {
            let signature = cordis_contract::portable::resource(&self.descriptor, &kind)
                .map_err(|e| refused("unknown_resource_kind", e))?;
            let args = args
                .into_message(&signature.args)
                .map_err(|e| refused("invalid_argument", e))?;
            self.acquire(&ctx, &kind, args)
                .await
                .map(cordis_host::Resource::new)
        })
    }
}
impl ResourceProvider for Resource {
    fn invoke(&self, method: String, args: Body) -> rt::BoxFuture<'_, Result<Body>> {
        Box::pin(async move {
            let signature = cordis_contract::portable::method(&self.contract.methods, &method)
                .map_err(|e| refused("unknown_method", e))?;
            let args = args
                .into_message(&signature.args)
                .map_err(|e| refused("invalid_argument", e))?;
            self.invoke(&method, args).await.map(Into::into)
        })
    }
    fn dispose(&self) -> rt::BoxFuture<'_, Result<()>> {
        Box::pin(self.dispose())
    }
    fn downgrade(&self) -> cordis_host::WeakResource {
        cordis_host::WeakResource::new(self.downgrade())
    }
}
impl WeakResourceProvider for resource::WeakResource {
    fn invoke(&self, method: String, args: Body) -> rt::BoxFuture<'_, Result<Body>> {
        Box::pin(async move {
            let resource = self.upgrade()?;
            ResourceProvider::invoke(&resource, method, args).await
        })
    }
    fn dispose(&self) -> rt::BoxFuture<'_, Result<()>> {
        Box::pin(self.dispose())
    }
}
