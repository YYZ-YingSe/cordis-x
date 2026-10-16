//! Sandboxed WebAssembly component execution under Cordis Runtime ownership.
#![forbid(unsafe_code)]
mod compiler;
mod engine;
pub use compiler::{CacheLimits, Compiler, CompilerStats};
mod wasi;
pub use wasi::{Directory, Wasi};
#[cfg(feature = "loader")]
pub mod loader;
mod managed;
pub use managed::{Capabilities, ContextComponent, ContextResource, ContextService, Dependency};
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/plugin_bindings.rs"));
}
pub use engine::Limits;

pub use cordis_component::{Bytes, Payload, Value, json};
use cordis_contract::{ContractId, portable as contract};
pub use cordis_contract::{Descriptor, Interface};
use cordis_kernel::Activation;
use cordis_runtime::{
    Broker, BrokerRegistration, Context, EffectSnapshot, Plugin, Resource, Service,
};
use engine::{Guest, Module, Op, Reply};
use std::{
    fmt,
    io::Read,
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;
use wasmtime::component::ResourceAny;

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
pub struct Component(Arc<Module>);
impl Component {
    /// Read and compile a Component Model binary with explicit execution limits.
    /// Compilation is synchronous; applications may put it on a blocking worker.
    pub fn load(path: impl AsRef<Path>, limits: Limits) -> Result<Self> {
        let file = std::fs::File::open(path).map_err(|e| Error::Load(e.to_string()))?;
        let mut bytes = Vec::new();
        file.take(limits.artifact_bytes.saturating_add(1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|e| Error::Load(e.to_string()))?;
        Self::from_bytes(&bytes, limits)
    }
    pub fn from_bytes(bytes: &[u8], limits: Limits) -> Result<Self> {
        Compiler::default().component(bytes, limits)
    }
    pub fn contract(&self) -> &ContractId {
        &self.0.contract
    }
    pub fn descriptor(&self) -> &Descriptor {
        self.0.descriptor()
    }
    pub fn with_validation(mut self, validation: cordis_contract::Validation) -> Self {
        Arc::make_mut(&mut self.0).set_validation(validation);
        self
    }
    pub fn supports_resources(&self) -> bool {
        true
    }

    /// Exact contract preflight before creating any plugin instance.
    pub fn plugin(
        &self,
        name: impl Into<String>,
        service: &Service<WasmService>,
        config: Value,
    ) -> Result<Plugin> {
        if service.contract() != self.contract() {
            return Err(Error::Contract(format!(
                "expected {:?}, component exports {:?}",
                service.contract(),
                self.contract()
            )));
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

    /// Prepare an implementation that registers with a stable Broker. The host
    /// receives a candidate during initialization and promotes it only after
    /// PluginHandle::ready succeeds. The candidate never publishes itself.
    pub fn broker_plugin<F>(
        &self,
        name: impl Into<String>,
        broker: &Service<Broker<WasmService>>,
        config: Value,
        registered: F,
    ) -> Result<Plugin>
    where
        F: Fn(BrokerRegistration<WasmService>) -> cordis_runtime::Result<()>
            + Send
            + Sync
            + 'static,
    {
        if broker.contract() != self.contract() {
            return Err(Error::Contract(format!(
                "expected {:?}, component exports {:?}",
                broker.contract(),
                self.contract()
            )));
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
        F: Fn(&Context, Arc<WasmService>) -> cordis_runtime::Result<()> + Send + Sync + 'static,
    {
        engine::input(&config.clone().into())?;
        let library = self.clone();
        let publish = Arc::new(publish);
        Ok(Plugin::new(name, move |ctx| {
            let module = library.0.clone();
            let config = config.clone();
            let publish = publish.clone();
            async move {
                let activation = ctx.activation();
                let owned = ctx
                    .acquire(
                        "Wasm instance",
                        async move {
                            // Construct the responsibility owner BEFORE calling foreign code.
                            let instance = Arc::new(Instance::new(module));
                            let worker = instance.clone();
                            let outcome = tokio::task::spawn_blocking(move || {
                                let mut state = worker.state.lock().unwrap();
                                let result = (|| {
                                    let guest = worker.module.instantiate()?;
                                    state.guest = Some(guest);
                                    let guest = state.guest.as_mut().unwrap();
                                    let (id, descriptor) = guest.describe()?;
                                    if id != worker.module.contract
                                        || &descriptor != worker.module.descriptor()
                                    {
                                        return Err(Error::Unknown(
                                            "contract changed between instances".into(),
                                        ));
                                    }
                                    guest.create(&cordis_component::Initialization::encode(
                                        config,
                                        worker.module.validation,
                                    ))
                                })();
                                state.closed = matches!(
                                    result,
                                    Err(Error::Rejected { .. }
                                        | Error::Refused { .. }
                                        | Error::InvalidInput(_))
                                );
                                if state.closed {
                                    state.guest = None;
                                }
                                if let Err(error @ Error::Unknown(_)) = &result {
                                    state.fault = Some(error.clone());
                                }
                                state.id = result;
                            })
                            .await;
                            if let Err(error) = outcome {
                                instance.fault(Error::Unknown(error.to_string()));
                            }
                            // Unknown acquisition must still get a cleanup ticket.
                            Ok(instance)
                        },
                        |instance, _| async move { instance.cleanup().await.map_err(Into::into) },
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
                    Arc::new(WasmService {
                        instance,
                        activation,
                    }),
                )
            }
        }))
    }
}

struct State {
    id: Result<ResourceAny>,
    guest: Option<Guest>,
    activation_attempted: bool,
    closed: bool,
    fault: Option<Error>,
    resources: usize,
}
struct Instance {
    module: Arc<Module>,
    state: Mutex<State>,
    capacity: Arc<Semaphore>,
    #[cfg(test)]
    hook: Mutex<Option<tests::Hook>>,
}
impl Instance {
    fn new(module: Arc<Module>) -> Self {
        Self {
            #[cfg(test)]
            hook: Mutex::new(None),
            capacity: Arc::new(Semaphore::new(module.limits.pending_calls)),
            module,
            state: Mutex::new(State {
                id: Err(Error::Unknown("create has not returned".into())),
                guest: None,
                activation_attempted: false,
                closed: false,
                fault: None,
                resources: 0,
            }),
        }
    }
    fn id(&self) -> Result<ResourceAny> {
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
        args: Payload,
    ) -> Result<Reply> {
        let signature = if matches!(op, Op::Invoke) {
            let signature =
                contract::method(&self.module.descriptor().interface().methods, &method)
                    .map_err(Error::InvalidInput)?;
            signature
                .args
                .check_binary(&args.one().map_err(|e| Error::InvalidInput(e.to_string()))?)
                .map_err(Error::InvalidInput)?;
            Some(signature.clone())
        } else {
            None
        };
        let errors = if matches!(op, Op::Acquire) {
            Some(
                contract::resource(self.module.descriptor(), &method)
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
        let instance = self.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut state = instance.state.lock().unwrap();
            // Recheck after queued work: an earlier call may have faulted the store.
            if let Some(error) = &state.fault {
                return Err(error.clone());
            }
            if state.closed {
                return Err(Error::Closed);
            }
            let id = state.id.clone()?;
            if matches!(op, Op::Acquire) && state.resources >= instance.module.limits.resources {
                return Err(Error::Overloaded);
            }
            let result = state
                .guest
                .as_mut()
                .ok_or(Error::Closed)?
                .execute(id, op, &method, &args)
                .and_then(|reply| {
                    if let (Some(signature), Reply::Value(value)) = (&signature, &reply) {
                        signature
                            .result
                            .check_binary(value)
                            .map_err(Error::Unknown)?;
                    }
                    Ok(reply)
                });
            let result = result.map_err(|e| match &errors {
                Some(errors) => e.check_declared(errors, "operation errors"),
                None => e,
            });
            if matches!(op, Op::Acquire) && result.is_ok() {
                state.resources += 1;
            }
            if let Err(error @ Error::Unknown(_)) = &result {
                state.fault.get_or_insert(error.clone());
            }
            drop(state);
            #[cfg(test)]
            tests::after_call(&instance, op);
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
    async fn cleanup(self: &Arc<Self>) -> Result<()> {
        let instance = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut state = instance.state.lock().unwrap();
            if state.closed {
                return Ok(());
            }
            if let Some(error) = &state.fault {
                return Err(error.clone());
            }
            if state.resources != 0 {
                return Err(Error::Unknown("Wasm resources remain unreleased".into()));
            }
            let id = state.id.clone()?;
            let active = state.activation_attempted;
            state
                .guest
                .as_mut()
                .ok_or(Error::Closed)?
                .close(id, active)?;
            state.closed = true;
            state.guest = None;
            Ok(())
        })
        .await
        .unwrap_or_else(|e| Err(Error::Unknown(e.to_string())))
    }
}
// Store disposal does not execute business inverses. Unlike native code, an
// isolated store can be reclaimed after execution ends even if recovery failed;
// the Runtime effect ledger still records that failure, never a successful inverse.

/// Calls run on blocking workers under native definition/use-site leases.
#[derive(Clone)]
pub struct WasmService {
    instance: Arc<Instance>,
    activation: Activation,
}
impl WasmService {
    pub async fn invoke_json(&self, ctx: &Context, method: &str, args: Value) -> Result<Value> {
        let signature = contract::method(
            &self.instance.module.descriptor().interface().methods,
            method,
        )
        .map_err(Error::InvalidInput)?;
        let bytes = self
            .invoke(ctx, method, json_body(&signature.args, &args)?)
            .await?;
        body_json(&signature.result, &bytes)
    }
    pub async fn acquire_json(
        &self,
        ctx: &Context,
        kind: &str,
        args: Value,
    ) -> Result<WasmResource> {
        let schema = contract::resource(self.instance.module.descriptor(), kind)
            .map_err(Error::InvalidInput)?;
        self.acquire(ctx, kind, json_body(&schema.args, &args)?)
            .await
    }
    fn check(&self, context: &Context, method: &str, args: &Bytes) -> Result<()> {
        if context.activation() != self.activation {
            return Err(Error::Closed);
        }
        if method.len() > cordis_component::MAX_BYTES {
            return Err(Error::InvalidInput("method too long".into()));
        }
        if args.len() > cordis_component::MAX_BYTES {
            Err(Error::InvalidInput("message too large".into()))
        } else {
            Ok(())
        }
    }
    pub async fn invoke(&self, context: &Context, method: &str, args: Bytes) -> Result<Bytes> {
        self.check(context, method, &args)?;
        let instance = self.instance.clone();
        let method = method.to_owned();
        context
            .operation(move |context| async move {
                Ok(instance
                    .run(&context, Op::Invoke, method, Payload::message(args))
                    .await
                    .map(|reply| match reply {
                        Reply::Value(value) => value,
                        _ => unreachable!(),
                    }))
            })
            .await?
    }
    pub async fn acquire(
        &self,
        context: &Context,
        kind: &str,
        args: Bytes,
    ) -> Result<WasmResource> {
        self.check(context, kind, &args)?;
        let factory = contract::resource(self.instance.module.descriptor(), kind)
            .map_err(Error::InvalidInput)?;
        factory
            .args
            .check_binary(&args)
            .map_err(Error::InvalidInput)?;
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
        args: Bytes,
    ) -> Result<WasmResource> {
        // Retain fresh definition authority even when the caller drops a wait
        // inside its ServiceHandle callback before the guest returns.
        let definition = context.clone();
        let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
        let resource = context
            .acquire(
                format!("Wasm resource: {kind}"),
                async move {
                    let acquired = instance
                        .run(
                            &definition,
                            Op::Acquire,
                            kind.clone(),
                            Payload::message(args),
                        )
                        .await
                        .map(|reply| match reply {
                            Reply::Resource(handle) => handle,
                            _ => unreachable!(),
                        });
                    // Fault reporting may already have closed the owner's gate.
                    // Preserve the diagnostic without borrowing a closed resource.
                    let _ = outcome_tx.send(acquired.clone());
                    Ok(Ticket {
                        instance,
                        acquired,
                        kind,
                    })
                },
                |ticket, context| async move {
                    let id = match &ticket.acquired {
                        Ok(id) => *id,
                        Err(
                            Error::Rejected { .. }
                            | Error::Refused { .. }
                            | Error::InvalidInput(_)
                            | Error::Overloaded
                            | Error::Closed,
                        ) => return Ok(()),
                        Err(error) => return Err(error.clone().into()),
                    };
                    let instance = ticket.instance.clone();
                    let outcome = tokio::task::spawn_blocking(move || {
                        let mut state = instance.state.lock().unwrap();
                        if let Some(error) = &state.fault {
                            return Err(error.clone());
                        }
                        let result = state.guest.as_mut().ok_or(Error::Closed)?.release(id);
                        if let Err(error @ Error::Unknown(_)) = &result {
                            state.fault.get_or_insert(error.clone());
                        }
                        result?;
                        state.resources -= 1;
                        Ok::<_, Error>(())
                    })
                    .await
                    .unwrap_or_else(|e| Err(Error::Unknown(e.to_string())));
                    if let Err(error @ Error::Unknown(_)) = &outcome {
                        ticket.instance.fault(error.clone());
                        let _ = context.fail(error.to_string());
                    }
                    outcome.map_err(Into::into)
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
        Ok(WasmResource { resource })
    }
}

#[cfg(test)]
extern crate self as cordis_wasm;
#[cfg(test)]
mod tests;
struct Ticket {
    kind: String,
    instance: Arc<Instance>,
    acquired: Result<ResourceAny>,
}
/// A use-site scope owns the inverse; cloned handles share one disposal result.
#[derive(Clone)]
pub struct WasmResource {
    resource: Resource<Ticket>,
}
impl WasmResource {
    pub async fn invoke_json(&self, method: &str, args: Value) -> Result<Value> {
        let signature = self
            .resource
            .with(|ticket| {
                let schema = contract::resource(ticket.instance.module.descriptor(), &ticket.kind)
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

    pub async fn invoke(&self, method: &str, args: Bytes) -> Result<Bytes> {
        if method.len() > cordis_component::MAX_BYTES {
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
                        let factory =
                            contract::resource(ticket.instance.module.descriptor(), &ticket.kind)
                                .map_err(Error::InvalidInput)?;
                        let signature = contract::method(&factory.methods, &method)
                            .map_err(Error::InvalidInput)?;
                        signature
                            .args
                            .check_binary(&args)
                            .map_err(Error::InvalidInput)?;
                        Ok::<_, Error>(signature.clone())
                    })();
                    let signature = match signature {
                        Ok(value) => value,
                        Err(error) => return Ok(Err(error)),
                    };
                    let result = ticket
                        .instance
                        .run(
                            &context,
                            Op::ResourceCall(id),
                            method,
                            Payload::message(args),
                        )
                        .await
                        .map(|reply| match reply {
                            Reply::Value(value) => value,
                            _ => unreachable!(),
                        })
                        .map_err(|e| e.check_declared(&signature.errors, "resource errors"))
                        .and_then(|value| {
                            signature
                                .result
                                .check_binary(&value)
                                .map_err(Error::Unknown)?;
                            Ok(value)
                        });
                    if let Err(error @ Error::Unknown(_)) = &result {
                        ticket.instance.fault(error.clone());
                        let _ = context.fail(error.to_string());
                    }
                    Ok(result)
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
    if value.encoded_len() > cordis_component::MAX_BYTES {
        return Err(Error::InvalidInput("message too large".into()));
    }
    Ok(value.encode_to_vec().into())
}
pub(crate) fn body_json(shape: &cordis_contract::MessageContract, bytes: &Bytes) -> Result<Value> {
    let value = shape.parse_binary(bytes).map_err(Error::Unknown)?;
    serde_json::to_value(&value).map_err(engine::unknown)
}
