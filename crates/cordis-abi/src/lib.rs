//! Optional, trusted in-process C ABI host. See `docs/protocol/abi-v1.md`.
#[cfg(feature = "loader")]
mod artifact;
mod composition;
mod context;
mod resource_dependencies;
pub use composition::{CompositionLimits, FactoryBinding, ProvisionBinding};
mod managed;
mod reactive;
pub use managed::ManagedLimits;
pub use reactive::{EventBinding, MiddlewareBinding, ReactiveLimits};
mod ffi;
#[cfg(feature = "loader")]
pub use artifact::ArtifactOptions;
pub use context::{Capabilities, Dependency};
#[cfg(feature = "loader")]
pub mod loader;

use cordis_contract::{ContractId, portable as contract};
pub use cordis_contract::{Descriptor, Interface};
use cordis_kernel::Activation;
use cordis_runtime::{
    Broker, BrokerRegistration, Context, EffectSnapshot, Plugin, Resource, Service,
};
use cordis_wire::local::Envelope;
pub use cordis_wire::{Bytes, Payload, Value, json};
use ffi::{Module, Op, Reply};
use std::{
    fmt,
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;

pub type Result<T> = std::result::Result<T, Error>;
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Load(String),
    Contract(String),
    InvalidInput(String),
    Rejected { code: String, message: String },
    Refused { code: String, message: String },
    Unknown(String),
    Closed,
    Overloaded,
    Runtime(String),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<Error> for cordis_runtime::Error {
    fn from(value: Error) -> Self {
        match value {
            Error::Rejected { code, message } => Self::Portable(
                cordis_contract::Failure::Rejected(cordis_contract::ErrorInfo { code, message }),
            ),
            Error::Refused { code, message } => Self::Portable(cordis_contract::Failure::Refused(
                cordis_contract::ErrorInfo { code, message },
            )),
            Error::Unknown(message) => Self::Portable(cordis_contract::Failure::Unknown(
                cordis_contract::ErrorInfo {
                    code: "outcome_unknown".into(),
                    message,
                },
            )),
            other => Self::Message(other.to_string()),
        }
    }
}
impl From<cordis_runtime::Error> for Error {
    fn from(value: cordis_runtime::Error) -> Self {
        match value {
            cordis_runtime::Error::Closed => Self::Closed,
            cordis_runtime::Error::DuplicateNext => Self::Refused {
                code: "duplicate_next".into(),
                message: "next already admitted".into(),
            },
            cordis_runtime::Error::ExpiredContinuation => Self::Refused {
                code: "expired_next".into(),
                message: "middleware frame ended".into(),
            },
            cordis_runtime::Error::Portable(failure) => match failure {
                cordis_contract::Failure::Rejected(info) => Self::Rejected {
                    code: info.code,
                    message: info.message,
                },
                cordis_contract::Failure::Refused(info) => Self::Refused {
                    code: info.code,
                    message: info.message,
                },
                cordis_contract::Failure::Unknown(info) => {
                    Self::Unknown(format!("{}: {}", info.code, info.message))
                }
            },
            e => Self::Runtime(e.to_string()),
        }
    }
}

/// A loaded module. Clones pin code; each plugin activation creates its own instance.
#[derive(Clone)]
pub struct Library(Arc<Module>, Arc<Capabilities>);
impl Library {
    /// Load and check the v1 function table. Loading may run native constructors.
    ///
    /// # Safety
    /// The binary must be trusted and obey the complete `protocols/abi/include/cordis/abi.h`
    /// and `docs/protocol/abi-v1.md` contract: correct platform ABI, valid pointer
    /// lifetimes/alignment, thread-safe calls, no exceptions or panics across FFI,
    /// truthful acquisition outcomes, no work using code after successful destroy.
    /// Constructors/destructors must also be safe to execute. Validation cannot
    /// sandbox native code or establish these guarantees for an arbitrary binary.
    #[allow(unsafe_code)]
    pub unsafe fn load(path: impl AsRef<Path>) -> Result<Self> {
        // SAFETY: the public caller accepts the module's entire FFI contract.
        unsafe { Module::load(path.as_ref()) }
            .map(|module| Self(Arc::new(module), Arc::new(Capabilities::default())))
    }
    /// Verify and load an immutable package. The module retains every private
    /// dependency until its last code reference, including failed responsibility.
    ///
    /// # Safety
    /// All `Library::load` obligations apply. The staging parent, host external
    /// dependencies and loader environment must be trusted and stable. Native
    /// constructors and metadata entry points may run during preparation; their
    /// external effects are not automatically reversible. Dynamic loads beyond
    /// the static import graph must obey the declared package/host boundary too.
    #[cfg(feature = "loader")]
    #[allow(unsafe_code)]
    pub unsafe fn load_artifact(
        artifact: &cordis_loader::ArtifactRef,
        options: &ArtifactOptions,
    ) -> Result<Self> {
        // SAFETY: caller accepts constructors of every package member.
        let staged = unsafe { artifact::StagedArtifact::prepare(artifact, options) }?;
        // SAFETY: caller accepts both the binary and its dependency environment.
        let mut module = unsafe { Module::load(&staged.path()) }?;
        module.artifact = Some(staged);
        Ok(Self(Arc::new(module), Arc::new(Capabilities::default())))
    }
    pub fn contract(&self) -> &ContractId {
        &self.0.contract
    }
    pub fn descriptor(&self) -> &Descriptor {
        &self.0.descriptor
    }
    pub fn with_validation(mut self, validation: cordis_contract::Validation) -> Self {
        Arc::make_mut(&mut self.1).validation = validation;
        self
    }
    pub fn supports_resources(&self) -> bool {
        self.0.resources()
    }
    /// Whether the guest accepts the independent reverse-resource host extension.
    pub fn supports_dependency_resources(&self) -> bool {
        self.0.dependency_resources()
    }

    pub fn supports_managed(&self) -> bool {
        self.0.managed()
    }
    pub fn supports_reactive(&self) -> bool {
        self.0.reactive_manifest().is_some()
    }
    pub fn supports_context(&self) -> bool {
        self.0.context.is_some()
    }
    pub fn supports_composition(&self) -> bool {
        self.0.composition_manifest().is_some()
    }
    /// Bind the exact declared dependency adapters. Validation precedes creation.
    pub fn with_context(mut self, capabilities: Capabilities) -> Result<Self> {
        if !self.supports_context() {
            return Err(Error::Contract("ABI Context extension unavailable".into()));
        }
        capabilities.validate(
            self.0.context.as_ref().map(|ext| &ext.dependencies),
            self.0.context.as_ref().map(|ext| &ext.interfaces),
        )?;
        capabilities.validate_reactive(self.0.reactive_manifest())?;
        capabilities.validate_composition(self.0.composition_manifest(), self.contract())?;
        self.1 = Arc::new(capabilities);
        Ok(self)
    }

    /// Exact contract preflight before creating any plugin instance.
    pub fn plugin(
        &self,
        name: impl Into<String>,
        service: &Service<AbiService>,
        config: Value,
    ) -> Result<Plugin> {
        if service.contract() != self.contract() {
            return Err(Error::Contract(format!(
                "expected {:?}, library exports {:?}",
                service.contract(),
                self.contract()
            )));
        }
        service
            .descriptor()
            .ok_or_else(|| Error::Contract("ABI interface descriptor required".into()))?
            .compare(self.descriptor())
            .map_err(|e| Error::Contract(e.to_string()))?;
        let service = service.clone();
        let declaration = service.clone();
        Ok(self
            .build_plugin(name, config, move |ctx, value| ctx.provide(&service, value))?
            .provides(&declaration))
    }

    /// Register a candidate with a stable Broker after guest activation. The
    /// controller must await PluginHandle::ready before promoting its token.
    pub fn broker_plugin<F>(
        &self,
        name: impl Into<String>,
        broker: &Service<Broker<AbiService>>,
        config: Value,
        registered: F,
    ) -> Result<Plugin>
    where
        F: Fn(BrokerRegistration<AbiService>) -> cordis_runtime::Result<()> + Send + Sync + 'static,
    {
        if broker.contract() != self.contract() {
            return Err(Error::Contract(format!(
                "expected {:?}, library exports {:?}",
                broker.contract(),
                self.contract()
            )));
        }
        broker
            .descriptor()
            .ok_or_else(|| Error::Contract("ABI interface descriptor required".into()))?
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
        F: Fn(&Context, Arc<AbiService>) -> cordis_runtime::Result<()> + Send + Sync + 'static,
    {
        ffi::validate(&config)?;
        self.1.validate(
            self.0.context.as_ref().map(|ext| &ext.dependencies),
            self.0.context.as_ref().map(|ext| &ext.interfaces),
        )?;
        self.1.validate_reactive(self.0.reactive_manifest())?;
        self.1
            .validate_composition(self.0.composition_manifest(), self.contract())?;
        let library = self.clone();
        let publish = Arc::new(publish);
        Ok(self.1.declare(Plugin::new(name, move |ctx| {
            let module = library.0.clone();
            let capabilities = library.1.clone();
            let config =
                cordis_wire::Initialization::encode(config.clone(), capabilities.validation);
            let publish = publish.clone();
            async move {
                let activation = ctx.activation();
                let owned = ctx
                    .acquire(
                        "ABI instance",
                        async move {
                            // Construct the responsibility owner BEFORE calling foreign code.
                            let instance = Instance::new(module, capabilities)?;
                            let worker = instance.clone();
                            let outcome = tokio::task::spawn_blocking(move || {
                                let result = worker.module.execute_local(
                                    Op::Create,
                                    0,
                                    &config.into(),
                                    worker.router.capabilities.validation,
                                );
                                let mut state = worker.state.lock().unwrap();
                                match result {
                                    Ok(reply) => state.id = Ok(reply.handle),
                                    Err(error) => {
                                        state.closed = matches!(
                                            error,
                                            Error::Rejected { .. }
                                                | Error::Refused { .. }
                                                | Error::InvalidInput(_)
                                        );
                                        state.id = Err(error);
                                    }
                                }
                            })
                            .await;
                            if let Err(error) = outcome {
                                instance.fault(Error::Unknown(error.to_string()));
                            }
                            // Unknown acquisition must still get a cleanup ticket.
                            Ok(instance)
                        },
                        |instance, cleanup| async move {
                            instance.cleanup(cleanup).await.map_err(Into::into)
                        },
                    )
                    .await?;
                let instance = owned.with(Clone::clone)?;
                instance.id()?;
                instance.state.lock().unwrap().activation_attempted = true;
                instance
                    .run(&ctx, Op::Activate, "".into(), Value::Null.into())
                    .await?;
                publish(
                    &ctx,
                    Arc::new(AbiService {
                        instance,
                        activation,
                    }),
                )
            }
        })))
    }
}

struct State {
    id: Result<u64>,
    activation_attempted: bool,
    closed: bool,
    fault: Option<Error>,
    resources: usize,
}
struct Instance {
    descriptor: Descriptor,
    module: Arc<Module>,
    state: Mutex<State>,
    capacity: Arc<Semaphore>,
    router: Arc<context::Router>,
}
impl Instance {
    fn new(module: Arc<Module>, capabilities: Arc<Capabilities>) -> Result<Arc<Self>> {
        let host_id = context::Router::allocate_id()?;
        Ok(Arc::new_cyclic(|weak| Self {
            descriptor: module
                .descriptor
                .clone()
                .with_validation(capabilities.validation),
            router: context::Router::new(weak.clone(), capabilities, host_id),
            module,
            state: Mutex::new(State {
                id: Err(Error::Unknown("create has not returned".into())),
                activation_attempted: false,
                closed: false,
                fault: None,
                resources: 0,
            }),
            capacity: Arc::new(Semaphore::new(16)),
        }))
    }
    fn id(&self) -> Result<u64> {
        let state = self.state.lock().unwrap();
        if let Some(error) = &state.fault {
            return Err(error.clone());
        }
        if state.closed {
            return state.id.clone().and(Err(Error::Closed));
        }
        state.id.clone()
    }
    fn fault(&self, error: Error) {
        self.state.lock().unwrap().fault.get_or_insert(error);
    }
    async fn run(
        self: &Arc<Self>,
        context: &Context,
        op: Op,
        method: String,
        mut args: Envelope,
    ) -> Result<Reply<Envelope>> {
        let signature = if matches!(op, Op::Invoke) {
            let signature = contract::method(&self.descriptor.interface().methods, &method)
                .map_err(Error::InvalidInput)?;
            args.check(&signature.args).map_err(Error::InvalidInput)?;
            Some(signature.clone())
        } else {
            None
        };
        let errors = if matches!(op, Op::Acquire) {
            Some(
                contract::resource(&self.descriptor, &method)
                    .map_err(Error::InvalidInput)?
                    .errors
                    .clone(),
            )
        } else {
            signature.as_ref().map(|s| s.errors.clone())
        };
        let permit = self
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        let id = self.id()?;
        let instance = self.clone();
        let context_owned = context.clone();
        let outcome = tokio::spawn(async move {
            let _permit = permit;
            let result = instance
                .execute(&context_owned, op, id, &method, &args)
                .await
                .and_then(|reply| {
                    // The foreign completion is a new snapshot, never a receipt.
                    let mut value = Envelope::from(reply.value);
                    if let Some(signature) = signature {
                        value.check(&signature.result).map_err(Error::Unknown)?;
                    }
                    Ok(Reply {
                        value,
                        handle: reply.handle,
                    })
                });
            // A guest may catch a reverse-call failure, but cannot erase the
            // host's recorded uncertainty by returning a different outcome.
            let result = match instance.state.lock().unwrap().fault.clone() {
                Some(error) => Err(error),
                None => result,
            };
            let result = result.map_err(|e| match &errors {
                Some(errors) => e.check_declared(errors, "operation errors"),
                None => e,
            });
            if matches!(op, Op::Acquire) && result.is_ok() {
                instance.state.lock().unwrap().resources += 1;
            }
            if let Err(error @ Error::Unknown(_)) = &result {
                instance.fault(error.clone());
            }
            result
        })
        .await
        .unwrap_or_else(|e| Err(Error::Unknown(e.to_string())));
        if let Err(error @ Error::Unknown(_)) = &outcome {
            self.fault(error.clone());
            let _ = context.fail(error.to_string());
        }
        outcome
    }
    async fn execute(
        &self,
        ctx: &Context,
        op: Op,
        id: u64,
        method: &str,
        args: &Payload,
    ) -> Result<Reply> {
        if self.module.context.is_none() {
            return self
                .module
                .execute(op, id, method, args, self.router.capabilities.validation)
                .await;
        }
        let scope = self.router.scope(ctx, op)?;
        let result = async {
            if matches!(op, Op::Managed(14, _)) {
                let id = args.control["next"]
                    .as_u64()
                    .ok_or_else(|| Error::InvalidInput("missing next frame".into()))?;
                self.router.reactive.bind_frame(id, scope.grant)?;
            }
            self.module
                .execute_scoped(op, id, method, args, &scope)
                .await
        }
        .await;
        scope.finish().await;
        result
    }
    async fn managed_call(
        self: &Arc<Self>,
        ctx: Context,
        operation: u32,
        target: u64,
        args: Value,
    ) -> Result<Value> {
        self.managed_payload(ctx, operation, target, args.into())
            .await?
            .control_only()
            .map_err(|e| Error::Unknown(e.to_string()))
    }
    async fn managed_payload(
        self: &Arc<Self>,
        ctx: Context,
        operation: u32,
        target: u64,
        args: Payload,
    ) -> Result<Payload> {
        let instance = self.clone();
        let outcome = tokio::spawn(async move {
            instance
                .execute(
                    &ctx,
                    Op::Managed(operation, target),
                    instance.id()?,
                    "",
                    &args,
                )
                .await
                .map(|r| r.value)
        })
        .await
        .unwrap_or_else(|e| Err(Error::Unknown(e.to_string())));
        if let Err(error @ Error::Unknown(_)) = &outcome {
            self.fault(error.clone());
        }
        outcome
    }
    async fn run_cleanup(self: &Arc<Self>, ctx: Context, op: Op) -> Result<()> {
        let instance = self.clone();
        tokio::spawn(async move {
            instance
                .execute(&ctx, op, instance.id()?, "", &Value::Null.into())
                .await?;
            Ok(())
        })
        .await
        .unwrap_or_else(|e| Err(Error::Unknown(e.to_string())))
    }
    async fn cleanup(self: &Arc<Self>, ctx: Context) -> Result<()> {
        let instance = self.clone();
        tokio::spawn(async move {
            let (id, active) = {
                let state = instance.state.lock().unwrap();
                if state.closed {
                    return Ok(());
                }
                if let Some(error) = &state.fault {
                    return Err(error.clone());
                }
                if state.resources != 0 || !instance.router.recovered() {
                    return Err(Error::Unknown(
                        "ABI resources or effects remain unreleased".into(),
                    ));
                }
                (state.id.clone()?, state.activation_attempted)
            };
            if active {
                instance
                    .execute(&ctx, Op::Deactivate, id, "", &Value::Null.into())
                    .await?;
            }
            // Destroy synchronously joins the foreign executor, including the
            // tail of its last completion callback. Only this bounded teardown
            // boundary needs a blocking worker; business awaits do not.
            let module = instance.module.clone();
            let validation = instance.router.capabilities.validation;
            tokio::task::spawn_blocking(move || {
                module.execute_local(Op::Destroy, id, &Value::Null.into(), validation)
            })
            .await
            .map_err(|error| Error::Unknown(error.to_string()))??;
            instance.state.lock().unwrap().closed = true;
            Ok(())
        })
        .await
        .unwrap_or_else(|e| Err(Error::Unknown(e.to_string())))
    }
}
impl Drop for Instance {
    fn drop(&mut self) {
        if !self
            .state
            .get_mut()
            .map(|state| state.closed)
            .unwrap_or(false)
        {
            // Failed/abandoned recovery is NOT permission to unload live code.
            // Retain the module for process lifetime, without retrying user cleanup.
            std::mem::forget(self.module.clone());
        }
    }
}

/// Calls await foreign completion under native definition/use-site leases.
#[derive(Clone)]
pub struct AbiService {
    instance: Arc<Instance>,
    activation: Activation,
}
impl AbiService {
    fn check(&self, context: &Context, method: &str, args: &Bytes) -> Result<()> {
        if context.activation() != self.activation {
            return Err(Error::Closed);
        }
        if method.len() > cordis_wire::MAX_MESSAGE {
            return Err(Error::InvalidInput("method too long".into()));
        }
        if args.len() > cordis_wire::MAX_MESSAGE {
            return Err(Error::InvalidInput("message too large".into()));
        }
        Ok(())
    }
    /// Explicit JSON ingress/egress for applications using the protobuf JSON mapping.
    pub async fn invoke_json(&self, context: &Context, method: &str, args: Value) -> Result<Value> {
        let signature = contract::method(&self.instance.descriptor.interface().methods, method)
            .map_err(Error::InvalidInput)?;
        let value = self
            .invoke(context, method, json_body(&signature.args, &args)?)
            .await?;
        body_json(&signature.result, &value)
    }
    pub async fn acquire_json(
        &self,
        context: &Context,
        kind: &str,
        args: Value,
    ) -> Result<AbiResource> {
        let shape =
            contract::resource(&self.instance.descriptor, kind).map_err(Error::InvalidInput)?;
        self.acquire(context, kind, json_body(&shape.args, &args)?)
            .await
    }
    pub async fn invoke(&self, context: &Context, method: &str, args: Bytes) -> Result<Bytes> {
        self.invoke_envelope(context, method, Payload::message(args).into())
            .await?
            .one()
            .map_err(|e| Error::Unknown(e.to_string()))
    }
    async fn invoke_envelope(
        &self,
        context: &Context,
        method: &str,
        args: Envelope,
    ) -> Result<Envelope> {
        self.check(
            context,
            method,
            &args.one().map_err(|e| Error::InvalidInput(e.to_string()))?,
        )?;
        let instance = self.instance.clone();
        let method = method.to_owned();
        context
            .operation(move |context| async move {
                Ok(instance
                    .run(&context, Op::Invoke, method, args)
                    .await
                    .map(|reply| reply.value))
            })
            .await?
    }
    pub async fn acquire(&self, context: &Context, kind: &str, args: Bytes) -> Result<AbiResource> {
        self.acquire_envelope(context, kind, Payload::message(args).into())
            .await
    }
    async fn acquire_envelope(
        &self,
        context: &Context,
        kind: &str,
        mut args: Envelope,
    ) -> Result<AbiResource> {
        self.check(
            context,
            kind,
            &args.one().map_err(|e| Error::InvalidInput(e.to_string()))?,
        )?;
        let schema =
            contract::resource(&self.instance.descriptor, kind).map_err(Error::InvalidInput)?;
        args.check(&schema.args).map_err(Error::InvalidInput)?;
        if !self.instance.module.resources() {
            return Err(Error::InvalidInput("resources unsupported".into()));
        }
        let instance = self.instance.clone();
        let kind = kind.to_owned();
        context
            .operation(move |context| async move {
                Ok(Self::acquire_owned(instance, context, kind, args).await)
            })
            .await?
    }
    async fn acquire_owned(
        instance: Arc<Instance>,
        context: Context,
        kind: String,
        args: Envelope,
    ) -> Result<AbiResource> {
        // Keep fresh definition authority through a cancelled inner wait, so
        // late unknown results can still fault the correct implementation.
        let definition = context.clone();
        let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
        let released = Arc::new(Mutex::new(None));
        let observed = released.clone();
        let resource = context
            .acquire(
                format!("ABI resource: {kind}"),
                async move {
                    let acquired = instance
                        .run(&definition, Op::Acquire, kind.clone(), args)
                        .await
                        .map(|reply| reply.handle);
                    // Fault reporting may already have closed the owner's gate.
                    // Preserve the diagnostic without borrowing a closed resource.
                    let _ = outcome_tx.send(acquired.clone());
                    Ok(Ticket {
                        kind,
                        instance,
                        acquired,
                        released,
                    })
                },
                |ticket, cleanup| async move {
                    let result: Result<()> = async {
                        let id = match &ticket.acquired {
                            Ok(id) => *id,
                            Err(
                                Error::Rejected { .. }
                                | Error::Refused { .. }
                                | Error::InvalidInput(_)
                                | Error::Overloaded
                                | Error::Closed,
                            ) => return Ok(()),
                            Err(error) => return Err(error.clone()),
                        };
                        let instance = ticket.instance.clone();
                        instance
                            .run_cleanup(cleanup, Op::ResourceRelease(id))
                            .await?;
                        instance.state.lock().unwrap().resources -= 1;
                        Ok(())
                    }
                    .await;
                    *ticket.released.lock().unwrap() = Some(result.clone());
                    result.map_err(Into::into)
                },
            )
            .await?;
        let outcome = outcome_rx
            .await
            .map_err(|e| Error::Runtime(e.to_string()))?;
        if let Err(error) = outcome {
            if !matches!(error, Error::Unknown(_)) {
                resource.dispose().await?;
            }
            return Err(error);
        }
        Ok(AbiResource {
            resource,
            released: observed,
        })
    }
}
struct Ticket {
    kind: String,
    instance: Arc<Instance>,
    acquired: Result<u64>,
    released: Arc<Mutex<Option<Result<()>>>>,
}
/// A use-site scope owns the inverse; cloned handles share one disposal result.
#[derive(Clone)]
pub struct AbiResource {
    resource: Resource<Ticket>,
    released: Arc<Mutex<Option<Result<()>>>>,
}
impl AbiResource {
    pub async fn invoke_json(&self, method: &str, args: Value) -> Result<Value> {
        let signature = self
            .resource
            .with(|ticket| {
                let schema = contract::resource(&ticket.instance.descriptor, &ticket.kind)
                    .map_err(Error::InvalidInput)?;
                contract::method(&schema.methods, method)
                    .cloned()
                    .map_err(Error::InvalidInput)
            })
            .map_err(Error::from)??;
        let bytes = self
            .invoke(method, crate::json_body(&signature.args, &args)?)
            .await?;
        crate::body_json(&signature.result, &bytes)
    }

    pub(crate) fn downgrade(&self) -> WeakAbiResource {
        WeakAbiResource {
            resource: self.resource.downgrade(),
            released: self.released.clone(),
        }
    }
    pub async fn invoke(&self, method: &str, args: Bytes) -> Result<Bytes> {
        if args.len() > cordis_wire::MAX_MESSAGE {
            return Err(Error::InvalidInput("message too large".into()));
        }
        if method.len() > cordis_wire::MAX_MESSAGE {
            return Err(Error::InvalidInput("method too long".into()));
        }
        let method = method.to_owned();
        self.resource
            .call(move |ticket, context| {
                Box::pin(async move {
                    let id = *ticket
                        .acquired
                        .as_ref()
                        .expect("only confirmed handles are public");
                    let signature = (|| {
                        let resource =
                            contract::resource(&ticket.instance.descriptor, &ticket.kind)
                                .map_err(Error::InvalidInput)?;
                        let signature = contract::method(&resource.methods, &method)
                            .map_err(Error::InvalidInput)?;
                        signature
                            .args
                            .check_binary(&args)
                            .map_err(Error::InvalidInput)?;
                        Ok::<_, Error>(signature)
                    })();
                    let signature = match signature {
                        Ok(s) => s,
                        Err(e) => return Ok(Err(e)),
                    };
                    let outcome = ticket
                        .instance
                        .run(
                            &context,
                            Op::ResourceCall(id),
                            method,
                            Payload::message(args).into(),
                        )
                        .await
                        .map_err(|e| e.check_declared(&signature.errors, "resource errors"))
                        .and_then(|reply| {
                            let value = reply
                                .value
                                .one()
                                .map_err(|e| Error::Unknown(e.to_string()))?;
                            signature
                                .result
                                .check_binary(&value)
                                .map_err(Error::Unknown)?;
                            Ok(value)
                        });
                    if let Err(error @ Error::Unknown(_)) = &outcome {
                        ticket.instance.fault(error.clone());
                        let _ = context.fail(error.to_string());
                    }
                    Ok(outcome)
                })
            })
            .await?
    }
    pub async fn dispose(&self) -> Result<()> {
        self.resource.dispose().await.map_err(Into::into)
    }
    pub fn snapshot(&self) -> EffectSnapshot {
        self.resource.snapshot()
    }
}

/// The reverse registry must not retain the Scope owning its library instance.
#[derive(Clone)]
pub(crate) struct WeakAbiResource {
    resource: cordis_runtime::WeakResource<Ticket>,
    released: Arc<Mutex<Option<Result<()>>>>,
}
impl WeakAbiResource {
    pub async fn invoke(&self, method: &str, args: Bytes) -> Result<Bytes> {
        AbiResource {
            resource: self.resource.upgrade().ok_or(Error::Closed)?,
            released: self.released.clone(),
        }
        .invoke(method, args)
        .await
    }
    pub async fn dispose(&self) -> Result<()> {
        if let Some(effect) = self.resource.effect() {
            return effect.dispose().await.map_err(|e| {
                self.released
                    .lock()
                    .unwrap()
                    .as_ref()
                    .and_then(|r| r.clone().err())
                    .unwrap_or_else(|| e.into())
            });
        }
        self.released
            .lock()
            .unwrap()
            .clone()
            .unwrap_or(Err(Error::Closed))
    }
}

impl Error {
    pub(crate) fn check_declared(
        self,
        errors: &std::collections::BTreeSet<String>,
        path: &str,
    ) -> Self {
        if let Self::Rejected { code, .. } = &self
            && let Err(message) = cordis_contract::portable::error(errors, code, path)
        {
            return Self::Unknown(message);
        }
        self
    }
}

pub(crate) fn json_body(shape: &cordis_contract::MessageContract, value: &Value) -> Result<Bytes> {
    use prost::Message;
    let value = shape.parse_json(value).map_err(Error::InvalidInput)?;
    if value.encoded_len() > cordis_wire::MAX_MESSAGE {
        return Err(Error::InvalidInput("message too large".into()));
    }
    Ok(value.encode_to_vec().into())
}
pub(crate) fn body_json(shape: &cordis_contract::MessageContract, bytes: &Bytes) -> Result<Value> {
    let value = shape.parse_binary(bytes).map_err(Error::Unknown)?;
    serde_json::to_value(&value).map_err(|e| Error::Unknown(e.to_string()))
}
