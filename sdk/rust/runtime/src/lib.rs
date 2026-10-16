//! Run portable, locally asynchronous Rust plugins on the native Runtime.
//! Typed `cordis-sdk` calls are unaffected by this optional adapter.
mod backend;
mod resource;
mod worker;

use cordis_runtime as rt;
use cordis_sdk_core::{Failure, Message as Payload, Plugin, Result, Value, json, refused};
pub use resource::Resource;
use std::{collections::BTreeMap, sync::Arc};
pub use worker::Service;

type Invoke =
    dyn Fn(rt::Context, String, Payload) -> rt::BoxFuture<'static, Result<Payload>> + Send + Sync;
type Acquire =
    dyn Fn(rt::Context, String, Payload) -> rt::BoxFuture<'static, Result<Resource>> + Send + Sync;

/// Explicit native service binding. The Runtime still checks the declaration
/// and resolves through the definition site of the actual callback.
#[derive(Clone)]
pub struct Dependency {
    declaration: rt::Service<()>,
    descriptor: cordis_contract::Descriptor,
    invoke: Arc<Invoke>,
    acquire: Option<Arc<Acquire>>,
}
impl Dependency {
    pub fn new<T, F>(
        service: &rt::Service<T>,
        descriptor: cordis_contract::Descriptor,
        invoke: F,
    ) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
        F: for<'a> Fn(&'a T, rt::Context, String, Payload) -> rt::BoxFuture<'a, Result<Payload>>
            + Send
            + Sync
            + 'static,
    {
        let declaration = rt::Service::new(
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
                        .map_err(rejected)?
                        .call(move |value, ctx| {
                            Box::pin(async move { Ok(invoke(value, ctx, method, args).await) })
                        })
                        .await
                        .map_err(unknown)?
                })
            }),
        }
    }
    pub fn portable(service: &rt::Service<Service>) -> Result<Self> {
        let descriptor = service.descriptor().cloned().ok_or_else(|| {
            refused(
                "contract_mismatch",
                "portable dependency requires an explicit descriptor",
            )
        })?;
        let mut dependency = Self::new(service, descriptor, |service, ctx, method, args| {
            Box::pin(async move { service.invoke(&ctx, &method, args).await })
        });
        let service = service.clone();
        dependency.acquire = Some(Arc::new(move |ctx, kind, args| {
            let service = service.clone();
            Box::pin(async move {
                ctx.dependency(&service)
                    .map_err(rejected)?
                    .call(move |value, ctx| {
                        Box::pin(async move { Ok(value.acquire(&ctx, &kind, args).await) })
                    })
                    .await
                    .map_err(unknown)?
            })
        }));
        Ok(dependency)
    }
}
pub type Factory = Arc<dyn Fn(Payload) -> rt::Result<rt::Plugin> + Send + Sync>;

/// Host-selected capabilities. Merely knowing an alias does not grant access.
#[derive(Clone)]
pub struct Capabilities {
    pub validation: cordis_sdk_core::Validation,
    pub dependencies: Vec<Dependency>,
    pub events: BTreeMap<String, rt::Event<Payload, Payload>>,
    pub middleware: BTreeMap<String, rt::Middleware<Payload, Payload>>,
    pub factories: BTreeMap<String, Factory>,
    pub provisions: BTreeMap<String, rt::Service<Payload>>,
}
impl Default for Capabilities {
    fn default() -> Self {
        Self {
            validation: cordis_sdk_core::Validation::Checked,
            dependencies: vec![],
            events: BTreeMap::new(),
            middleware: BTreeMap::new(),
            factories: BTreeMap::new(),
            provisions: BTreeMap::new(),
        }
    }
}

/// Install a portable plugin using a dedicated local executor. Callback futures
/// may suspend and reenter the same instance without making business state Send.
pub fn plugin<P: Plugin>(
    name: impl Into<String>,
    service: &rt::Service<Service>,
    config: Value,
    mut capabilities: Capabilities,
) -> Result<rt::Plugin> {
    for dependency in &mut capabilities.dependencies {
        dependency.descriptor = dependency
            .descriptor
            .clone()
            .with_validation(capabilities.validation);
    }
    let manifest = P::manifest();
    if manifest.provides.name != service.contract().name.as_ref()
        || manifest.provides.revision != service.contract().revision.as_ref()
    {
        return Err(refused(
            "contract_mismatch",
            "plugin contract differs from binding",
        ));
    }
    if let Some(expected) = service.descriptor() {
        let actual = cordis_contract::Descriptor::new(P::interface())
            .map_err(|e| refused("contract_mismatch", e.to_string()))?;
        expected
            .compare(&actual)
            .map_err(|e| refused("contract_mismatch", e.to_string()))?;
    }
    let expected: BTreeMap<_, _> = manifest
        .requires
        .iter()
        .map(|c| (c.name.as_str(), c.revision.as_str()))
        .collect();
    let actual: BTreeMap<_, _> = capabilities
        .dependencies
        .iter()
        .map(|d| {
            (
                d.declaration.contract().name.as_ref(),
                d.declaration.contract().revision.as_ref(),
            )
        })
        .collect();
    if expected != actual
        || expected.len() != manifest.requires.len()
        || actual.len() != capabilities.dependencies.len()
    {
        return Err(refused(
            "dependency_mismatch",
            "exact dependency bindings required",
        ));
    }
    let declared_interfaces = P::dependency_interfaces();
    if declared_interfaces
        .keys()
        .map(String::as_str)
        .ne(expected.keys().copied())
    {
        return Err(refused(
            "contract_mismatch",
            "one interface is required for every dependency",
        ));
    }
    for dependency in &capabilities.dependencies {
        let name = dependency.declaration.contract().name.as_ref();
        let expected = cordis_contract::Descriptor::new(declared_interfaces[name].clone())
            .map_err(|e| refused("contract_mismatch", e))?;
        expected
            .compare(&dependency.descriptor)
            .map_err(|e| refused("contract_mismatch", format!("dependencies.{name}: {e}")))?;
    }
    let bindings = P::bindings();
    let contract = |c: &cordis_contract::ContractId| cordis_sdk_core::Contract {
        name: c.name.to_string(),
        revision: c.revision.to_string(),
    };
    let events = capabilities
        .events
        .iter()
        .map(|(alias, e)| (alias.clone(), contract(e.contract())))
        .collect::<BTreeMap<_, _>>();
    let middleware = capabilities
        .middleware
        .iter()
        .map(|(alias, m)| (alias.clone(), contract(m.contract())))
        .collect::<BTreeMap<_, _>>();
    let provisions = capabilities
        .provisions
        .iter()
        .map(|(alias, p)| (alias.clone(), contract(p.contract())))
        .collect::<BTreeMap<_, _>>();
    let mut factories = bindings.factories.clone();
    factories.sort();
    if bindings.events != events
        || bindings.middleware != middleware
        || bindings.provisions != provisions
        || factories != capabilities.factories.keys().cloned().collect::<Vec<_>>()
    {
        return Err(refused(
            "binding_mismatch",
            "exact event, middleware and composition bindings required",
        ));
    }
    let descriptor = Arc::new(
        cordis_contract::Descriptor::new(P::interface())
            .map_err(|e| refused("contract_mismatch", e))?
            .with_validation(capabilities.validation),
    );
    let declaration = service.clone();
    let provided = service.clone();
    let caps = Arc::new(capabilities.clone());
    let mut plugin = rt::Plugin::new(name, move |ctx| {
        let config = config.clone();
        let caps = caps.clone();
        let provided = provided.clone();
        let descriptor = descriptor.clone();
        async move {
            let worker = worker::Client::start::<P>(config, caps)
                .await
                .map_err(native_error)?;
            let cleanup = worker.clone();
            ctx.defer("portable Rust instance", move |ctx| async move {
                cleanup.lifecycle(&ctx, false).await.map_err(native_error)?;
                cleanup.close().await.map_err(native_error)
            })?;
            worker.lifecycle(&ctx, true).await.map_err(native_error)?;
            ctx.provide(
                &provided,
                Arc::new(Service {
                    client: worker,
                    descriptor,
                }),
            )
        }
    })
    .provides(&declaration);
    for dependency in capabilities.dependencies {
        plugin = plugin.requires(&dependency.declaration);
    }
    for provision in capabilities.provisions.values() {
        plugin = plugin.provides(provision);
    }
    Ok(plugin)
}
fn rejected(value: impl std::fmt::Display) -> Failure {
    refused("runtime", value)
}
fn unknown(value: impl std::fmt::Display) -> Failure {
    Failure::Unknown(cordis_sdk_core::ErrorInfo {
        code: "runtime".into(),
        message: value.to_string(),
    })
}
fn native_error(value: Failure) -> rt::Error {
    value.into()
}
fn runtime_failure(value: rt::Error) -> Failure {
    match value {
        rt::Error::Portable(failure) => failure,
        rt::Error::DuplicateNext => refused("duplicate_next", "next already admitted"),
        rt::Error::ExpiredContinuation => refused("expired_next", "middleware frame ended"),
        other => unknown(other),
    }
}
