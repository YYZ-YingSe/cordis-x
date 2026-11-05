use crate::{Body, Resource, Result, refused, service_failure};
use cordis_contract::Descriptor;
use cordis_runtime::{BoxFuture, Context, Plugin, Service};
use std::sync::Arc;

type Invoke = dyn Fn(Context, String, Body) -> BoxFuture<'static, Result<Body>> + Send + Sync;
type Acquire = dyn Fn(Context, String, Body) -> BoxFuture<'static, Result<Resource>> + Send + Sync;

/// A concrete execution adapter implements this once, independently of consumers.
pub trait Provider: Send + Sync + 'static {
    /// Adapter support, independent of a particular service's declared resource kinds.
    const SUPPORTS_RESOURCES: bool = false;
    fn invoke(&self, ctx: Context, method: String, args: Body) -> BoxFuture<'_, Result<Body>>;
    fn acquire(
        &self,
        _ctx: Context,
        _kind: String,
        _args: Body,
    ) -> BoxFuture<'_, Result<Resource>> {
        Box::pin(async {
            Err(refused(
                "unsupported_resource",
                "selected provider has no resource adapter",
            ))
        })
    }
}
/// A fixed selection of a Runtime service. Every use resolves the service through
/// its calling Context; the binding never captures an active implementation.
#[derive(Clone)]
pub struct Dependency {
    declaration: Service<()>,
    descriptor: Descriptor,
    invoke: Arc<Invoke>,
    acquire: Option<Arc<Acquire>>,
}
impl std::fmt::Debug for Dependency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Dependency")
            .field(self.declaration.contract())
            .finish()
    }
}
impl Dependency {
    pub fn new<T: Provider>(service: &Service<T>) -> Result<Self> {
        let descriptor = service.descriptor().cloned().ok_or_else(|| {
            refused(
                "contract_mismatch",
                "service endpoint requires a descriptor",
            )
        })?;
        let mut binding = Self::local(service, descriptor, |value, ctx, method, args| {
            value.invoke(ctx, method, args)
        });
        let service = service.clone();
        if T::SUPPORTS_RESOURCES {
            binding.acquire = Some(Arc::new(move |ctx, kind, args| {
                let service = service.clone();
                Box::pin(async move {
                    ctx.dependency(&service)
                        .map_err(|e| refused("runtime", e))?
                        .call(move |value, ctx| {
                            Box::pin(async move { Ok(value.acquire(ctx, kind, args).await) })
                        })
                        .await
                        .map_err(service_failure)?
                })
            }));
        }
        Ok(binding)
    }
    /// Explicit adaptation of a host-owned service with a portable contract.
    pub fn local<T, F>(service: &Service<T>, descriptor: Descriptor, invoke: F) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
        F: for<'a> Fn(&'a T, Context, String, Body) -> BoxFuture<'a, Result<Body>>
            + Send
            + Sync
            + 'static,
    {
        let declaration = Service::new(
            service.contract().name.clone(),
            service.contract().revision.clone(),
        );
        let service = service.clone();
        let invoke = Arc::new(invoke);
        Self {
            declaration,
            descriptor,
            acquire: None,
            invoke: Arc::new(move |ctx, method, args| {
                let service = service.clone();
                let invoke = invoke.clone();
                Box::pin(async move {
                    ctx.dependency(&service)
                        .map_err(|e| refused("runtime", e))?
                        .call(move |value, ctx| {
                            Box::pin(async move { Ok(invoke(value, ctx, method, args).await) })
                        })
                        .await
                        .map_err(service_failure)?
                })
            }),
        }
    }
    pub fn declaration(&self) -> &Service<()> {
        &self.declaration
    }
    pub fn contract(&self) -> &cordis_contract::ContractId {
        self.declaration.contract()
    }
    pub fn descriptor(&self) -> &Descriptor {
        &self.descriptor
    }
    pub fn with_validation(mut self, validation: cordis_contract::Validation) -> Self {
        self.descriptor = self.descriptor.with_validation(validation);
        self
    }
    pub fn declare(&self, plugin: Plugin) -> Plugin {
        plugin.requires(&self.declaration)
    }
    pub async fn invoke(&self, ctx: Context, method: String, args: Body) -> Result<Body> {
        let signature =
            cordis_contract::portable::method(&self.descriptor.interface().methods, &method)
                .map_err(|e| refused("unknown_method", e))?;
        // Bind identity and host policy without adding a reflected predecode in
        // front of generated dispatch. Raw gateways validate on byte extraction.
        let args = args
            .into_message(&signature.args)
            .map_err(|e| refused("invalid_argument", e))?;
        let value = (self.invoke)(ctx, method, args.into())
            .await
            .map_err(|e| e.check_declared(&signature.errors, "dependency result"))?;
        value
            .into_message(&signature.result)
            .map(Into::into)
            .map_err(crate::unknown)
    }
    pub async fn acquire(&self, ctx: Context, kind: String, args: Body) -> Result<Resource> {
        let acquire = self.acquire.as_ref().ok_or_else(|| {
            refused(
                "unsupported_resource",
                "selected endpoint has no resource acquisition adapter",
            )
        })?;
        let schema = cordis_contract::portable::resource(&self.descriptor, &kind)
            .map_err(|e| refused("unknown_resource_kind", e))?;
        let args = args
            .into_message(&schema.args)
            .map_err(|e| refused("invalid_argument", e))?;
        acquire(ctx, kind, args.into())
            .await
            .map_err(|e| e.check_declared(&schema.errors, "dependency resource"))
    }
    /// Resolve a factory contract only after confirming the selected adapter
    /// actually supports acquisition. No handle slot is reserved on refusal.
    pub fn resource_contract(&self, kind: &str) -> Result<&cordis_contract::Resource> {
        if !self.supports_resources() {
            return Err(refused(
                "unsupported_resource",
                "selected endpoint has no resource acquisition adapter",
            ));
        }
        cordis_contract::portable::resource(&self.descriptor, kind)
            .map_err(|e| refused("invalid_argument", e))
    }
    pub fn supports_resources(&self) -> bool {
        self.acquire.is_some()
    }
}

impl Dependency {
    /// Encoded consumer adapter; local validation receipts stay host-local.
    pub async fn invoke_wire(
        &self,
        ctx: Context,
        method: String,
        args: cordis_wire::local::Envelope,
    ) -> Result<cordis_wire::local::Envelope> {
        self.invoke(ctx, method, args.into())
            .await?
            .into_envelope()
            .map_err(crate::unknown)
    }
    pub async fn acquire_wire(
        &self,
        ctx: Context,
        kind: String,
        args: cordis_wire::local::Envelope,
    ) -> Result<Resource> {
        self.acquire(ctx, kind, args.into()).await
    }
}

impl Dependency {
    /// An explicit gateway for an application service implemented with JSON.
    pub fn json<T, F, E>(service: &Service<T>, descriptor: Descriptor, invoke: F) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
        F: for<'a> Fn(
                &'a T,
                Context,
                String,
                cordis_wire::Value,
            ) -> BoxFuture<'a, std::result::Result<cordis_wire::Value, E>>
            + Send
            + Sync
            + 'static,
        E: Into<crate::Failure> + 'static,
    {
        let schema = descriptor.clone();
        let invoke = Arc::new(invoke);
        Self::local(service, descriptor, move |value, ctx, method, args| {
            let schema = schema.clone();
            let invoke = invoke.clone();
            Box::pin(async move {
                let signature =
                    cordis_contract::portable::method(&schema.interface().methods, &method)
                        .map_err(|e| refused("invalid_argument", e))?;
                let input = args
                    .into_message(&signature.args)
                    .and_then(|m| m.to_json())
                    .map_err(|e| refused("invalid_argument", e))?;
                let output = invoke(value, ctx, method, input)
                    .await
                    .map_err(Into::into)?;
                cordis_sdk_core::Message::from_json(signature.result.clone(), &output)
                    .map(Into::into)
                    .map_err(crate::unknown)
            })
        })
    }
    /// A raw-byte gateway returns a fresh snapshot with no validation receipt.
    pub fn bytes<T, F, E>(service: &Service<T>, descriptor: Descriptor, invoke: F) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
        F: for<'a> Fn(
                &'a T,
                Context,
                String,
                cordis_wire::Bytes,
            ) -> BoxFuture<'a, std::result::Result<cordis_wire::Bytes, E>>
            + Send
            + Sync
            + 'static,
        E: Into<crate::Failure> + 'static,
    {
        let invoke = Arc::new(invoke);
        Self::local(service, descriptor, move |value, ctx, method, args| {
            let invoke = invoke.clone();
            Box::pin(async move {
                let args = args
                    .into_envelope()
                    .map_err(|e| refused("invalid_argument", e))?;
                let output = invoke(
                    value,
                    ctx,
                    method,
                    args.one().map_err(|e| refused("invalid_argument", e))?,
                )
                .await
                .map_err(Into::into)?;
                Ok(
                    cordis_wire::local::Envelope::from(cordis_wire::Payload::message(output))
                        .into(),
                )
            })
        })
    }
}

impl Dependency {
    /// Host-owned typed service adaptation. Encoding is deferred to a real wire edge.
    pub fn messages<T, F>(service: &Service<T>, descriptor: Descriptor, invoke: F) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
        F: for<'a> Fn(
                &'a T,
                Context,
                String,
                cordis_sdk_core::Message,
            ) -> BoxFuture<'a, Result<cordis_sdk_core::Message>>
            + Send
            + Sync
            + 'static,
    {
        let schema = descriptor.clone();
        let invoke = Arc::new(invoke);
        Self::local(service, descriptor, move |value, ctx, method, args| {
            let schema = schema.clone();
            let invoke = invoke.clone();
            Box::pin(async move {
                let signature =
                    cordis_contract::portable::method(&schema.interface().methods, &method)
                        .map_err(|e| refused("invalid_argument", e))?;
                let input = args
                    .into_message(&signature.args)
                    .map_err(|e| refused("invalid_argument", e))?;
                invoke(value, ctx, method, input).await.map(Into::into)
            })
        })
    }
}
