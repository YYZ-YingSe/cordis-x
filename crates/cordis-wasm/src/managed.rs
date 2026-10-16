//! Context adapter with native Component Model asynchronous execution.
mod execution;
#[cfg(test)]
#[path = "managed/resource_tests.rs"]
mod resource_tests;
mod resources;
mod router;
use crate::{
    Compiler, Error, Limits, Result, Value, Wasi,
    compiler::{CompiledCode, Profile},
    engine::unknown,
};
use cordis_component::{Bytes, Payload};
use cordis_contract::{ContractId, Descriptor, portable as contract};
use cordis_runtime::{
    BoxFuture, Broker, BrokerRegistration, Context, Event, Middleware, Plugin, Service,
};
use cordis_wire::local::Envelope;
pub use resources::ContextResource;
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::sync::Semaphore;
use wasmtime::component::{Accessor, HasData, Linker};
use wasmtime::{Store, StoreLimits, StoreLimitsBuilder};
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/managed_bindings.rs"));
}
use bindings::cordis::plugin::types as wit;
fn input(value: &Payload) -> Result<wit::Payload> {
    cordis_component::encode(value).map_err(|e| Error::InvalidInput(e.to_string()))
}
fn output(value: wit::Payload, validation: cordis_contract::Validation) -> Result<Payload> {
    cordis_component::decode_with(value, validation).map_err(unknown)
}

fn refused(code: &str, message: impl ToString) -> Error {
    Error::Refused {
        code: code.into(),
        message: message.to_string(),
    }
}
fn business<T>(result: std::result::Result<T, wit::Failure>) -> Result<T> {
    result.map_err(|e| match e {
        wit::Failure::Rejected(e) => Error::Rejected {
            code: e.code,
            message: e.message,
        },
        wit::Failure::Refused(e) => refused(&e.code, e.message),
        wit::Failure::Unknown(e) => unknown(format!("{}: {}", e.code, e.message)),
    })
}
fn failure(e: Error) -> wit::Failure {
    match e {
        Error::Rejected { code, message } => {
            wit::Failure::Rejected(wit::ErrorInfo { code, message })
        }
        Error::Refused { code, message } => wit::Failure::Refused(wit::ErrorInfo { code, message }),
        Error::Closed => wit::Failure::Refused(wit::ErrorInfo {
            code: "closed".into(),
            message: "callback authority expired".into(),
        }),
        Error::InvalidInput(message) => wit::Failure::Refused(wit::ErrorInfo {
            code: "invalid_argument".into(),
            message,
        }),
        Error::Overloaded => wit::Failure::Refused(wit::ErrorInfo {
            code: "overloaded".into(),
            message: "capacity exhausted".into(),
        }),
        e => wit::Failure::Unknown(wit::ErrorInfo {
            code: "host_failed".into(),
            message: e.to_string(),
        }),
    }
}
type Invoke =
    dyn Fn(Context, String, Envelope) -> BoxFuture<'static, Result<Envelope>> + Send + Sync;
type Acquire =
    dyn Fn(Context, String, Envelope) -> BoxFuture<'static, Result<ContextResource>> + Send + Sync;
#[derive(Clone)]
pub struct Dependency {
    declaration: Service<()>,
    descriptor: Option<Descriptor>,
    invoke: Arc<Invoke>,
    acquire: Option<Arc<Acquire>>,
}
impl Dependency {
    pub fn new<T, F>(service: &Service<T>, descriptor: Descriptor, invoke: F) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
        F: for<'a> Fn(&'a T, Context, String, Bytes) -> BoxFuture<'a, Result<Bytes>>
            + Send
            + Sync
            + 'static,
    {
        let invoke = Arc::new(invoke);
        Self::bind(
            service,
            Some(descriptor),
            move |service, ctx, method, args| {
                let invoke = invoke.clone();
                Box::pin(async move {
                    let bytes = invoke(
                        service,
                        ctx,
                        method,
                        args.one().map_err(|e| Error::InvalidInput(e.to_string()))?,
                    )
                    .await?;
                    Ok(Payload::message(bytes).into())
                })
            },
        )
    }
    /// Explicit JSON gateway for a host application whose service API is JSON.
    pub fn new_json<T, F>(
        service: &Service<T>,
        descriptor: cordis_contract::Descriptor,
        invoke: F,
    ) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
        F: for<'a> Fn(&'a T, Context, String, Value) -> BoxFuture<'a, Result<Value>>
            + Send
            + Sync
            + 'static,
    {
        let schema = descriptor.clone();
        let invoke = Arc::new(invoke);
        Self::new(service, descriptor, move |value, ctx, method, args| {
            let schema = schema.clone();
            let invoke = invoke.clone();
            Box::pin(async move {
                let signature =
                    cordis_contract::portable::method(&schema.interface().methods, &method)
                        .map_err(Error::InvalidInput)?;
                let input = crate::body_json(&signature.args, &args)
                    .map_err(|e| Error::InvalidInput(e.to_string()))?;
                let result = invoke(value, ctx, method, input).await?;
                crate::json_body(&signature.result, &result)
                    .map_err(|e| Error::Unknown(e.to_string()))
            })
        })
    }
    fn bind<T, F>(service: &Service<T>, descriptor: Option<Descriptor>, invoke: F) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
        F: for<'a> Fn(&'a T, Context, String, Envelope) -> BoxFuture<'a, Result<Envelope>>
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
                    ctx.dependency(&service)?
                        .call(move |s, c| {
                            Box::pin(async move { Ok(invoke(s, c, method, args).await) })
                        })
                        .await?
                })
            }),
        }
    }
    pub fn wasm(service: &Service<ContextService>) -> Self {
        let mut binding = Self::bind(service, service.descriptor().cloned(), |s, c, m, a| {
            Box::pin(async move { s.invoke_envelope(&c, &m, a).await })
        });
        let service = service.clone();
        binding.acquire = Some(Arc::new(move |ctx, kind, args| {
            let service = service.clone();
            Box::pin(async move {
                ctx.dependency(&service)?
                    .call(move |s, ctx| {
                        Box::pin(async move { Ok(s.acquire_envelope(&ctx, &kind, args).await) })
                    })
                    .await?
            })
        }));
        binding
    }
}
/// Explicit bindings and bounded Context work. WASI is disabled unless provided.
#[derive(Clone)]
pub struct Capabilities {
    pub validation: cordis_contract::Validation,
    pub dependencies: Vec<Dependency>,
    pub events: BTreeMap<String, Event<Bytes, Bytes>>,
    pub middleware: BTreeMap<String, Middleware<Bytes, Bytes>>,
    /// Named host factories are the only plugins a guest may install.
    pub factories: BTreeMap<String, Arc<dyn Fn(Bytes) -> Result<Plugin> + Send + Sync>>,
    /// Explicit value services the guest is allowed to publish during initialization.
    pub provisions: BTreeMap<String, Service<Bytes>>,
    pub wasi: Option<Wasi>,
    pub max_handles: usize,
    pub max_requests: usize,
    pub max_calls: usize,
    pub call_timeout: Duration,
}
impl Default for Capabilities {
    fn default() -> Self {
        Self {
            validation: cordis_contract::Validation::Checked,
            dependencies: Vec::new(),
            events: BTreeMap::new(),
            middleware: BTreeMap::new(),
            factories: BTreeMap::new(),
            provisions: BTreeMap::new(),
            wasi: None,
            max_handles: 1024,
            max_requests: 256,
            max_calls: 128,
            call_timeout: Duration::from_secs(30),
        }
    }
}
struct Module {
    code: Arc<CompiledCode>,
    limits: Limits,
    capabilities: Capabilities,
    contract: ContractId,
    descriptor: Option<Descriptor>,
    requires: BTreeMap<String, (String, Descriptor)>,
}
#[derive(Clone)]
pub struct ContextComponent(Arc<Module>);
impl ContextComponent {
    pub fn load(
        path: impl AsRef<Path>,
        limits: Limits,
        capabilities: Capabilities,
    ) -> Result<Self> {
        use std::io::Read;
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .map_err(|e| Error::Load(e.to_string()))?
            .take(limits.artifact_bytes.saturating_add(1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|e| Error::Load(e.to_string()))?;
        Self::from_bytes(&bytes, limits, capabilities)
    }
    pub fn from_bytes(bytes: &[u8], limits: Limits, capabilities: Capabilities) -> Result<Self> {
        Compiler::default().context_component(bytes, limits, capabilities)
    }
    pub(crate) fn prepare(
        compiler: &Compiler,
        bytes: &[u8],
        limits: Limits,
        mut capabilities: Capabilities,
    ) -> Result<Self> {
        for dependency in &mut capabilities.dependencies {
            dependency.descriptor = dependency
                .descriptor
                .take()
                .map(|d| d.with_validation(capabilities.validation));
        }
        if bytes.len() > limits.artifact_bytes
            || limits.pending_calls == 0
            || limits.pending_calls > 1024
            || limits.memory_bytes == 0
            || limits.fuel_per_call == 0
            || limits.resources > Semaphore::MAX_PERMITS
            || capabilities.max_handles == 0
            || capabilities.max_calls == 0
            || capabilities.max_calls > 4096
            || capabilities.max_requests == 0
            || capabilities.call_timeout.is_zero()
        {
            return Err(Error::InvalidInput("invalid limits".into()));
        }
        let code = compiler.compile(Profile::Context, bytes)?;
        compiler.preflight(|| {
            let mut module = Module {
                code,
                limits,
                capabilities,
                contract: ContractId::new("", ""),
                descriptor: None,
                requires: BTreeMap::new(),
            };
            let manifest = std::thread::scope(|scope| {
                scope
                    .spawn(|| {
                        let runtime = tokio::runtime::Builder::new_multi_thread()
                            .worker_threads(1)
                            .enable_all()
                            .build()
                            .map_err(unknown)?;
                        let _entered = runtime.enter();
                        let mut probe = module.instantiate(Weak::new(), true)?;
                        bounded(
                            module.capabilities.call_timeout,
                            probe
                                .bindings
                                .cordis_plugin_managed_service()
                                .call_describe(&mut probe.store),
                        )
                    })
                    .join()
                    .map_err(|_| unknown("component preflight panicked"))?
            })?;
            if manifest.provides.name.is_empty()
                || manifest.provides.name.len() > 4096
                || manifest.provides.revision.is_empty()
                || manifest.provides.revision.len() > 4096
            {
                return Err(Error::Contract("invalid component contract".into()));
            }
            module.descriptor = Some(
                contract::decode(
                    &output(
                        manifest.provides.descriptor,
                        cordis_contract::Validation::Audit,
                    )?
                    .control_only()
                    .map_err(unknown)?,
                )
                .map_err(Error::Contract)?
                .with_validation(module.capabilities.validation),
            );
            module.contract = ContractId::new(manifest.provides.name, manifest.provides.revision);
            for d in manifest.requires {
                if d.name.is_empty()
                    || d.name.len() > 4096
                    || d.revision.is_empty()
                    || d.revision.len() > 4096
                {
                    return Err(Error::Contract("invalid dependency contract".into()));
                }
                let descriptor = contract::decode(
                    &output(d.descriptor, cordis_contract::Validation::Audit)?
                        .control_only()
                        .map_err(unknown)?,
                )
                .map_err(Error::Contract)?;
                if module
                    .requires
                    .insert(d.name, (d.revision, descriptor))
                    .is_some()
                {
                    return Err(Error::Contract("duplicate dependency".into()));
                }
            }
            let mut configured = BTreeMap::new();
            for d in &module.capabilities.dependencies {
                let name = d.declaration.contract().name.as_ref();
                if let Some((_, required)) = module.requires.get(name) {
                    let actual = d.descriptor.as_ref().ok_or_else(|| {
                        Error::Contract(format!("dependencies.{name}: descriptor required"))
                    })?;
                    required
                        .compare(actual)
                        .map_err(|e| Error::Contract(format!("dependencies.{name}: {e}")))?;
                }
                if configured
                    .insert(
                        d.declaration.contract().name.to_string(),
                        (
                            d.declaration.contract().revision.to_string(),
                            d.descriptor.clone().ok_or_else(|| {
                                Error::Contract("dependency descriptor required".into())
                            })?,
                        ),
                    )
                    .is_some()
                {
                    return Err(Error::Contract("duplicate binding".into()));
                }
            }
            if module.requires != configured {
                return Err(Error::Contract(
                    "component dependency declarations do not match explicit bindings".into(),
                ));
            }
            let mut provisions = std::collections::BTreeSet::new();
            provisions.insert(module.contract.name.clone());
            for service in module.capabilities.provisions.values() {
                if !provisions.insert(service.contract().name.clone()) {
                    return Err(Error::Contract(
                        "duplicate provision or primary service collision".into(),
                    ));
                }
            }
            Ok(Self(Arc::new(module)))
        })
    }
    pub fn contract(&self) -> &ContractId {
        &self.0.contract
    }
    pub fn descriptor(&self) -> &Descriptor {
        self.0.descriptor()
    }
    pub fn plugin(
        &self,
        name: impl Into<String>,
        service: &Service<ContextService>,
        config: Value,
    ) -> Result<Plugin> {
        if service.contract() != self.contract() {
            return Err(Error::Contract("service contract mismatch".into()));
        }
        service
            .descriptor()
            .ok_or_else(|| Error::Contract("service descriptor required".into()))?
            .compare(self.descriptor())
            .map_err(|e| Error::Contract(e.to_string()))?;
        let service = service.clone();
        let declaration = service.clone();
        Ok(self
            .build_plugin(name, config, move |ctx, value| ctx.provide(&service, value))?
            .provides(&declaration))
    }
    /// Register a Context-capable candidate; promotion remains a host decision
    /// after ready succeeds. All Context effects retain native owner semantics.
    pub fn broker_plugin<F>(
        &self,
        name: impl Into<String>,
        broker: &Service<Broker<ContextService>>,
        config: Value,
        registered: F,
    ) -> Result<Plugin>
    where
        F: Fn(BrokerRegistration<ContextService>) -> cordis_runtime::Result<()>
            + Send
            + Sync
            + 'static,
    {
        if broker.contract() != self.contract() {
            return Err(Error::Contract("service contract mismatch".into()));
        }
        broker
            .descriptor()
            .ok_or_else(|| Error::Contract("service descriptor required".into()))?
            .compare(self.descriptor())
            .map_err(|e| Error::Contract(e.to_string()))?;
        let broker = broker.clone();
        let declaration = broker.clone();
        Ok(self
            .build_plugin(name, config, move |ctx, value| {
                registered(ctx.dependency(&broker)?.register(value)?)
            })?
            .requires(&declaration))
    }
    fn build_plugin<F>(&self, name: impl Into<String>, config: Value, publish: F) -> Result<Plugin>
    where
        F: Fn(&Context, Arc<ContextService>) -> cordis_runtime::Result<()> + Send + Sync + 'static,
    {
        input(&config.clone().into())?;
        let component = self.clone();
        let publish = Arc::new(publish);
        let plugin = Plugin::new(name, move |ctx| {
            let module = component.0.clone();
            let publish = publish.clone();
            let config = config.clone();
            async move {
                let instance = Arc::new_cyclic(|weak| Instance {
                    router: router::Router::new(weak.clone()),
                    capacity: Arc::new(Semaphore::new(module.limits.pending_calls)),
                    calls: Arc::new(Semaphore::new(module.capabilities.max_calls)),
                    cleanup_calls: Arc::new(Semaphore::new(module.capabilities.max_calls)),
                    resource_slots: Arc::new(Semaphore::new(module.limits.resources)),
                    module,
                    guest: Mutex::new(None),
                    worker: Mutex::new(None),
                    fault: Mutex::new(None),
                    activated: std::sync::atomic::AtomicBool::new(false),
                });
                let worker = instance.clone();
                let owned = ctx
                    .acquire(
                        "Wasm Context instance",
                        async move {
                            let result = tokio::task::spawn_blocking(move || {
                                let mut guest =
                                    worker.module.instantiate(Arc::downgrade(&worker), false)?;
                                let manifest = bounded(
                                    worker.module.capabilities.call_timeout,
                                    guest
                                        .bindings
                                        .cordis_plugin_managed_service()
                                        .call_describe(&mut guest.store),
                                )?;
                                let required_count = manifest.requires.len();
                                let descriptor = contract::decode(
                                    &output(
                                        manifest.provides.descriptor,
                                        cordis_contract::Validation::Audit,
                                    )?
                                    .control_only()
                                    .map_err(unknown)?,
                                )
                                .map_err(unknown)?;
                                let requires: BTreeMap<_, _> = manifest
                                    .requires
                                    .into_iter()
                                    .map(|d| {
                                        Ok((
                                            d.name,
                                            (
                                                d.revision,
                                                contract::decode(
                                                    &output(
                                                        d.descriptor,
                                                        cordis_contract::Validation::Audit,
                                                    )?
                                                    .control_only()
                                                    .map_err(unknown)?,
                                                )
                                                .map_err(unknown)?,
                                            ),
                                        ))
                                    })
                                    .collect::<Result<_>>()?;
                                if manifest.provides.name != worker.module.contract.name.as_ref()
                                    || manifest.provides.revision
                                        != worker.module.contract.revision.as_ref()
                                    || &descriptor != worker.module.descriptor()
                                    || required_count != requires.len()
                                    || requires != worker.module.requires
                                {
                                    return Err(unknown(
                                        "manifest changed between preflight and activation",
                                    ));
                                }
                                execution::start(&worker, guest);
                                Ok::<_, Error>(())
                            })
                            .await
                            .unwrap_or_else(|e| Err(unknown(e)));
                            if let Err(error) = result {
                                instance.fault.lock().unwrap().get_or_insert(error);
                            }
                            Ok(instance)
                        },
                        |instance, cleanup| async move {
                            instance.cleanup(&cleanup).await.map_err(Into::into)
                        },
                    )
                    .await?;
                let instance = owned.with(Clone::clone)?;
                let settings = cordis_component::Initialization::encode(
                    config,
                    instance.module.capabilities.validation,
                );
                instance.call(&ctx, "create", settings).await?;
                instance
                    .activated
                    .store(true, std::sync::atomic::Ordering::Release);
                instance.call(&ctx, "activate", Value::Null).await?;
                publish(
                    &ctx,
                    Arc::new(ContextService {
                        instance,
                        activation: ctx.activation(),
                    }),
                )
            }
        });
        let plugin = self
            .0
            .capabilities
            .dependencies
            .iter()
            .fold(plugin, |p, d| p.requires(&d.declaration));
        Ok(self
            .0
            .capabilities
            .provisions
            .values()
            .fold(plugin, |p, s| p.provides(s)))
    }
}
struct Data {
    limits: StoreLimits,
    wasi: crate::wasi::State,
    instance: Weak<Instance>,
}
impl wit::Host for Data {}
impl wasmtime_wasi::WasiView for Data {
    fn ctx(&mut self) -> wasmtime_wasi::WasiCtxView<'_> {
        self.wasi.view()
    }
}
impl HasData for Data {
    type Data<'a> = &'a mut Data;
}
impl bindings::cordis::plugin::host::Host for Data {}
impl<T: Send> bindings::cordis::plugin::host::HostWithStore<T> for Data {
    async fn call(
        accessor: &Accessor<T, Self>,
        context: String,
        command: String,
        args: wit::Payload,
    ) -> std::result::Result<wit::Payload, wit::Failure> {
        let instance = accessor.with(|mut access| access.get().instance.upgrade());
        let result = async {
            let instance = instance.ok_or(Error::Closed)?;
            let args = output(args, instance.module.capabilities.validation)?;
            let value = instance
                .router
                .call(&instance, &context, command, args)
                .await?;
            input(&value)
        }
        .await;
        result.map_err(failure)
    }
}
struct Guest {
    store: Store<Data>,
    bindings: bindings::ManagedPlugin,
}
impl Module {
    fn descriptor(&self) -> &Descriptor {
        self.descriptor.as_ref().expect("preflight completed")
    }
    fn instantiate(&self, instance: Weak<Instance>, probe: bool) -> Result<Guest> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(self.limits.memory_bytes)
            .table_elements(4096)
            .instances(8)
            .tables(8)
            .memories(1)
            .trap_on_grow_failure(true)
            .build();
        let empty = Wasi::default();
        let grants = if probe {
            &empty
        } else {
            self.capabilities.wasi.as_ref().unwrap_or(&empty)
        };
        let mut store = Store::new(
            &self.code.engine,
            Data {
                limits,
                wasi: crate::wasi::State::new(grants)?,
                instance,
            },
        );
        store.limiter(|s| &mut s.limits);
        store.set_fuel(self.limits.fuel_per_call).map_err(unknown)?;
        store
            .fuel_async_yield_interval(Some(10_000))
            .map_err(unknown)?;
        let mut linker = Linker::new(&self.code.engine);
        bindings::ManagedPlugin::add_to_linker::<_, Data>(&mut linker, |s| s).map_err(unknown)?;
        if self.capabilities.wasi.is_some() {
            wasmtime_wasi::p2::add_to_linker_async(&mut linker).map_err(unknown)?;
        }
        let bindings = bounded(
            self.capabilities.call_timeout,
            bindings::ManagedPlugin::instantiate_async(&mut store, &self.code.component, &linker),
        )?;
        Ok(Guest { store, bindings })
    }
}
struct Instance {
    module: Arc<Module>,
    guest: Mutex<Option<Guest>>,
    worker: Mutex<Option<tokio::sync::mpsc::UnboundedSender<execution::Message>>>,
    fault: Mutex<Option<Error>>,
    router: Arc<router::Router>,
    capacity: Arc<Semaphore>,
    calls: Arc<Semaphore>,
    cleanup_calls: Arc<Semaphore>,
    resource_slots: Arc<Semaphore>,
    activated: std::sync::atomic::AtomicBool,
}
impl Instance {
    fn check(&self) -> Result<()> {
        self.fault.lock().unwrap().clone().map_or(Ok(()), Err)
    }
    async fn call(self: &Arc<Self>, ctx: &Context, command: &str, args: Value) -> Result<Value> {
        self.call_payload(ctx, command, args.into())
            .await?
            .control_only()
            .map_err(unknown)
    }
    async fn call_payload(
        self: &Arc<Self>,
        ctx: &Context,
        command: &str,
        args: Payload,
    ) -> Result<Payload> {
        self.call_envelope(ctx, command, args.into())
            .await
            .map(Envelope::into_wire)
    }
    async fn call_envelope(
        self: &Arc<Self>,
        ctx: &Context,
        command: &str,
        args: Envelope,
    ) -> Result<Envelope> {
        self.call_with_lifetime(ctx, command, args, false).await
    }
    async fn call_with_lifetime(
        self: &Arc<Self>,
        ctx: &Context,
        command: &str,
        mut args: Envelope,
        background: bool,
    ) -> Result<Envelope> {
        self.check()?;
        let signature = if command == "invoke" {
            let method = args
                .control
                .get("method")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::InvalidInput("method required".into()))?;
            let signature = contract::method(&self.module.descriptor().interface().methods, method)
                .map_err(Error::InvalidInput)?;
            args.check(&signature.args).map_err(Error::InvalidInput)?;
            Some(signature.clone())
        } else {
            None
        };
        let errors = if command == "resource-acquire" {
            let kind = args.control["kind"]
                .as_str()
                .ok_or_else(|| Error::InvalidInput("kind required".into()))?;
            Some(
                contract::resource(self.module.descriptor(), kind)
                    .map_err(Error::InvalidInput)?
                    .errors
                    .clone(),
            )
        } else {
            signature.as_ref().map(|s| s.errors.clone())
        };
        let calls = if ctx.is_cleanup() {
            &self.cleanup_calls
        } else {
            &self.calls
        };
        let _call_permit = calls
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        let scope = self.router.enter(ctx)?;
        let token = scope.token.clone();
        let cleanup = ctx.is_cleanup();
        let acquiring = command == "resource-acquire";
        let command = command.to_owned();
        let args = input(&args)?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        let sender = self
            .worker
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| self.check().err().unwrap_or(Error::Closed))?;
        sender
            .send(execution::Message::Call(Box::new(execution::Call {
                token,
                cleanup,
                command,
                args,
                context: ctx.clone(),
                background,
                reply,
            })))
            .map_err(|_| unknown("component driver stopped"))?;
        let outcome = receive
            .await
            .unwrap_or_else(|_| Err(unknown("component driver failed")))
            .and_then(|value| {
                let mut value = Envelope::from(value);
                if let Some(signature) = &signature {
                    value.check(&signature.result).map_err(unknown)?;
                }
                Ok(value)
            });
        let outcome = outcome.map_err(|e| match &errors {
            Some(errors) => e.check_declared(errors, "operation errors"),
            None => e,
        });
        // Revoke admission, then join all admitted imports, including operations a
        // malicious guest left unawaited. Early return cannot bypass recovery.
        // Withdraw first so imports waiting for owner cancellation are woken
        // even when the guest traps after submitting but before awaiting them.
        if let Err(error @ Error::Unknown(_)) = &outcome {
            self.fault.lock().unwrap().get_or_insert(error.clone());
            let _ = ctx.fail(error.to_string());
        }
        let drained = scope.finish().await;
        let outcome = match (drained, outcome) {
            // An import rejection cannot downgrade an unknown guest outcome.
            (_, Err(error @ Error::Unknown(_))) => Err(error),
            // A resource may already exist even if draining an unawaited import
            // fails. Never reinterpret that as a safely rejected acquisition.
            (Err(error), Ok(_)) if acquiring => {
                Err(unknown(format!("acquisition imports failed: {error}")))
            }
            (drained, outcome) => drained.and(outcome),
        };
        // Awaited or caught import failures still belong to this instance.
        let outcome = self.check().and(outcome);
        if let Err(error @ Error::Unknown(_)) = &outcome {
            self.fault.lock().unwrap().get_or_insert(error.clone());
            let _ = ctx.fail(error.to_string());
        }
        outcome
    }
    fn callback<'a>(
        self: &'a Arc<Self>,
        ctx: &'a Context,
        id: u64,
        args: Value,
    ) -> BoxFuture<'a, Result<Value>> {
        Box::pin(self.call(ctx, "callback", crate::json!({"id":id,"args":args})))
    }
    fn callback_payload<'a>(
        self: &'a Arc<Self>,
        ctx: &'a Context,
        id: u64,
        mut payload: Payload,
    ) -> BoxFuture<'a, Result<Payload>> {
        payload.control = crate::json!({"id":id,"args":payload.control});
        Box::pin(self.call_payload(ctx, "callback", payload))
    }
    async fn cleanup(self: &Arc<Self>, ctx: &Context) -> Result<()> {
        self.check()?;
        if self.resource_slots.available_permits() != self.module.limits.resources {
            return Err(unknown("managed resources remain unreleased"));
        }
        if self.activated.load(std::sync::atomic::Ordering::Acquire) {
            self.call(ctx, "deactivate", Value::Null).await?;
        }
        self.call(ctx, "close", Value::Null).await?;
        let sender = self.worker.lock().unwrap().clone().ok_or(Error::Closed)?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        sender
            .send(execution::Message::Stop(reply))
            .map_err(|_| unknown("component driver stopped"))?;
        receive.await.map_err(unknown)?;
        self.check()?;
        Ok(())
    }
}
#[derive(Clone)]
pub struct ContextService {
    instance: Arc<Instance>,
    activation: cordis_kernel::Activation,
}
impl ContextService {
    pub async fn invoke_json(&self, ctx: &Context, method: &str, args: Value) -> Result<Value> {
        let signature = contract::method(
            &self.instance.module.descriptor().interface().methods,
            method,
        )
        .map_err(Error::InvalidInput)?;
        let result = self
            .invoke(ctx, method, crate::json_body(&signature.args, &args)?)
            .await?;
        crate::body_json(&signature.result, &result)
    }
    pub async fn invoke(&self, ctx: &Context, method: &str, args: Bytes) -> Result<Bytes> {
        self.invoke_envelope(ctx, method, Payload::message(args).into())
            .await?
            .one()
            .map_err(unknown)
    }
    async fn invoke_envelope(
        &self,
        ctx: &Context,
        method: &str,
        args: Envelope,
    ) -> Result<Envelope> {
        if ctx.activation() != self.activation {
            return Err(Error::Closed);
        }
        let permit = if ctx.is_cleanup() {
            None
        } else {
            Some(
                self.instance
                    .capacity
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| Error::Overloaded)?,
            )
        };
        let instance = self.instance.clone();
        let method = method.to_owned();
        ctx.operation(move |ctx| async move {
            let _permit = permit;
            Ok(instance
                .call_envelope(
                    &ctx,
                    "invoke",
                    args.with_control(crate::json!({"method":method})),
                )
                .await)
        })
        .await?
    }
}

// Used only on blocking workers or the isolated preflight thread. WASI awaits
// yield inside Wasmtime; timeout drops the engine future and faults the Store.
fn bounded<T>(
    timeout: Duration,
    future: impl std::future::Future<Output = wasmtime::Result<T>>,
) -> Result<T> {
    tokio::runtime::Handle::current().block_on(async {
        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| unknown("Wasm/WASI call deadline exceeded"))?
            .map_err(|e| unknown(format!("{e:#}")))
    })
}
