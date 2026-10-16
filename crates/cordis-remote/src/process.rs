use crate::{Error, Result, Value, http::Http, session::Session};
use cordis_contract::ContractId;
use cordis_kernel::Activation;
use cordis_runtime::{BoxFuture, Broker, BrokerRegistration, Context, Plugin, Service};
use cordis_wire::local::Envelope;
use cordis_wire::{Bytes, Identity, Payload, json};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

// Retained by definitions, sessions and their reaping workers, independently of
// the public caller's wait. No code-format dependency enters the runtime.
pub(crate) type CodeLease = Option<Arc<dyn Send + Sync>>;

#[derive(Clone, Copy, Debug)]
pub enum Endpoint {
    Ipc,
    Http,
}

/// Artifact selection is independent of endpoint and encoding. No language field.
#[derive(Clone, Debug)]
pub struct ProcessSpec {
    /// Host-selected business validation, fixed for this installation.
    pub validation: cordis_contract::Validation,
    pub executable: PathBuf,
    pub args: Vec<String>,
    pub endpoint: Endpoint,
    pub timeout: Duration,
    /// Grace period after a managed task receives cancellation.
    pub task_stop_timeout: Duration,
    pub dependencies: Vec<crate::Dependency>,
    pub events: Vec<crate::EventBinding>,
    pub middleware: Vec<crate::MiddlewareBinding>,
    pub factories: Vec<crate::FactoryBinding>,
    pub provisions: Vec<crate::ProvisionBinding>,
    pub composition: crate::CompositionLimits,
}
impl ProcessSpec {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            validation: cordis_contract::Validation::Checked,
            executable: executable.into(),
            args: Vec::new(),
            endpoint: Endpoint::Ipc,
            timeout: Duration::from_secs(10),
            task_stop_timeout: Duration::from_secs(5),
            dependencies: Vec::new(),
            events: Vec::new(),
            middleware: Vec::new(),
            factories: Vec::new(),
            provisions: Vec::new(),
            composition: crate::CompositionLimits::default(),
        }
    }
    pub fn middleware(mut self, binding: crate::MiddlewareBinding) -> Self {
        self.middleware.push(binding);
        self
    }
    pub fn factory(mut self, binding: crate::FactoryBinding) -> Self {
        self.factories.push(binding);
        self
    }
    pub fn provision(mut self, binding: crate::ProvisionBinding) -> Self {
        self.provisions.push(binding);
        self
    }
    pub fn event(mut self, binding: crate::EventBinding) -> Self {
        self.events.push(binding);
        self
    }
    pub fn requires(mut self, dependency: crate::Dependency) -> Self {
        self.dependencies.push(dependency);
        self
    }
    pub fn plugin(self, name: impl Into<String>, service: &Service<RemoteService>) -> Plugin {
        self.pinned_plugin(name, service, None)
    }
    pub(crate) fn pinned_plugin(
        self,
        name: impl Into<String>,
        service: &Service<RemoteService>,
        code: CodeLease,
    ) -> Plugin {
        let declaration = service.clone();
        let service = service.clone();
        self.build_plugin(
            name,
            service.contract().clone(),
            service.descriptor().cloned(),
            code,
            move |ctx, value| ctx.provide(&service, value),
        )
        .provides(&declaration)
    }
    /// Register a rolling candidate after handshake and activation. The
    /// controller must await Active before promoting the returned registration.
    pub fn broker_plugin<F>(
        self,
        name: impl Into<String>,
        broker: &Service<Broker<RemoteService>>,
        registered: F,
    ) -> Plugin
    where
        F: Fn(BrokerRegistration<RemoteService>) -> cordis_runtime::Result<()>
            + Send
            + Sync
            + 'static,
    {
        self.pinned_broker_plugin(name, broker, None, registered)
    }
    pub(crate) fn pinned_broker_plugin<F>(
        self,
        name: impl Into<String>,
        broker: &Service<Broker<RemoteService>>,
        code: CodeLease,
        registered: F,
    ) -> Plugin
    where
        F: Fn(BrokerRegistration<RemoteService>) -> cordis_runtime::Result<()>
            + Send
            + Sync
            + 'static,
    {
        let declaration = broker.clone();
        let broker = broker.clone();
        self.build_plugin(
            name,
            broker.contract().clone(),
            broker.descriptor().cloned(),
            code,
            move |ctx, value| registered(ctx.dependency(&broker)?.register(value)?),
        )
        .requires(&declaration)
    }
    fn build_plugin<F>(
        self,
        name: impl Into<String>,
        contract: ContractId,
        descriptor: Option<cordis_contract::Descriptor>,
        code: CodeLease,
        publish: F,
    ) -> Plugin
    where
        F: Fn(&Context, Arc<RemoteService>) -> cordis_runtime::Result<()> + Send + Sync + 'static,
    {
        let dependencies = self.dependencies.clone();
        let provisions = self.provisions.clone();
        let publish = Arc::new(publish);
        let mut plugin = Plugin::new(name, move |ctx| {
            let spec = self.clone();
            let contract = contract.clone();
            let descriptor = descriptor.clone();
            let code = code.clone();
            let publish = publish.clone();
            async move {
                let descriptor = descriptor.ok_or_else(|| {
                    cordis_runtime::Error::Message(format!(
                        "contract {contract}: process interface descriptor required"
                    ))
                })?;
                let value = spec.start(&ctx, &contract, descriptor, code).await?;
                publish(&ctx, value)
            }
        });
        for dependency in dependencies {
            plugin = dependency.declare(plugin);
        }
        for provision in provisions {
            plugin = plugin.provides(&provision.service);
        }
        plugin
    }

    async fn start(
        self,
        ctx: &Context,
        contract: &ContractId,
        descriptor: cordis_contract::Descriptor,
        code: CodeLease,
    ) -> cordis_runtime::Result<Arc<RemoteService>> {
        let descriptor = descriptor.with_validation(self.validation);
        let activation = ctx.activation();
        let router = crate::dependency::Router::new(&self)?;
        router.validate_primary(contract)?;
        let declarations = router.declarations();
        let dependency_interfaces = router.interfaces();
        let events = router.event_declarations();
        let middleware = router.middleware_declarations();
        let composition = router.composition_declarations();
        let launch = self.clone();
        let owned = ctx
            .acquire(
                "process host",
                async move {
                    Session::spawn(&launch, identity(activation), router, code)
                        .await
                        .map_err(Into::into)
                },
                |session, ctx| async move { session.close(&ctx).await.map_err(Into::into) },
            )
            .await?;
        let session = owned.with(Clone::clone)?;
        let ready = session.request("hello", json!({
            "capabilities": {"dependency_resources":true,"metadata":true,"effects":true,"effect_sequences":true,"scopes":true,"tasks":true,"events":true,"middleware":true,"composition":true},
            "validation": self.validation,
            "dependencies": declarations,
            "dependency_interfaces": dependency_interfaces,
            "events": events,
            "middleware": middleware,
            "composition": composition,
            "contract": {"name": contract.name.as_ref(), "revision": contract.revision.as_ref()},
            "interface": cordis_contract::portable::encode(&descriptor)
        })).await?;
        cordis_contract::portable::compare(&descriptor, &ready["interface"])
            .map_err(|e| Error::Protocol(format!("contract {contract}: {e}")))?;
        if ready["dependency_interfaces"].as_object().map(|v| v.len())
            != dependency_interfaces.as_object().map(|v| v.len())
        {
            return Err(Error::Protocol("dependency interface identities differ".into()).into());
        }
        for (name, expected) in dependency_interfaces
            .as_object()
            .expect("declarations are a map")
        {
            let expected =
                cordis_contract::portable::decode(expected).map_err(Error::InvalidInput)?;
            cordis_contract::portable::compare(&expected, &ready["dependency_interfaces"][name])
                .map_err(|e| Error::Protocol(format!("dependencies.{name}: {e}")))?;
        }
        if ready["contract"]["name"].as_str() != Some(&contract.name)
            || ready["contract"]["revision"].as_str() != Some(&contract.revision)
        {
            return Err(Error::Protocol("ready contract mismatch".into()).into());
        }
        if ready
            .get("dependencies")
            .cloned()
            .unwrap_or_else(|| json!({}))
            != declarations
            || (!self.dependencies.is_empty() && ready["capabilities"]["reverse_calls"] != true)
        {
            return Err(Error::Protocol(
                "dependency declarations or reverse-call capability mismatch".into(),
            )
            .into());
        }
        if ready.get("events").cloned().unwrap_or_else(|| json!({})) != events
            || (!self.events.is_empty() && ready["capabilities"]["events"] != true)
        {
            return Err(Error::Protocol("event declarations or capability mismatch".into()).into());
        }
        if ready
            .get("middleware")
            .cloned()
            .unwrap_or_else(|| json!({}))
            != middleware
            || (!self.middleware.is_empty() && ready["capabilities"]["middleware"] != true)
        {
            return Err(
                Error::Protocol("middleware declarations or capability mismatch".into()).into(),
            );
        }
        session.enable_middleware(ready["capabilities"]["middleware"] == true);
        let mut declared_composition = ready
            .get("composition")
            .cloned()
            .unwrap_or_else(|| json!({"factories":[],"provisions":{}}));
        // Factory order is not authority. JS sorts UTF-16 code units, whereas
        // Rust/C++ sort UTF-8, so normalize before comparing Unicode aliases.
        if let Some(factories) = declared_composition
            .get_mut("factories")
            .and_then(Value::as_array_mut)
        {
            factories.sort_by(|a, b| a.as_str().cmp(&b.as_str()));
        }
        if declared_composition != composition
            || ((!self.factories.is_empty() || !self.provisions.is_empty())
                && ready["capabilities"]["composition"] != true)
        {
            return Err(
                Error::Protocol("composition declarations or capability mismatch".into()).into(),
            );
        }
        session.enable_composition(ready["capabilities"]["composition"] == true);
        session.enable_metadata(ready["capabilities"]["metadata"] == true);
        session.enable_dependency_resources(ready["capabilities"]["dependency_resources"] == true);
        session.enable_events(ready["capabilities"]["events"] == true);
        session.enable_effects(ready["capabilities"]["effects"] == true);
        session.enable_sequences(ready["capabilities"]["effect_sequences"] == true);
        session.enable_scopes(ready["capabilities"]["scopes"] == true);
        session.enable_tasks(ready["capabilities"]["tasks"] == true);
        let ipc: Arc<dyn InvocationBackend> = Arc::new(Ipc(session.clone()));
        let http = ready["http"]
            .as_str()
            .map(|url| {
                Http::local(url, session.timeout(), self.validation)
                    .map(|http| Arc::new(http) as Arc<dyn InvocationBackend>)
            })
            .transpose()?;
        if matches!(self.endpoint, Endpoint::Http) && http.is_none() {
            return Err(Error::Protocol("host did not advertise an HTTP endpoint".into()).into());
        }
        session.mark_activation_attempted();
        session.scoped_request(ctx, "activate", Value::Null).await?;
        let monitor = session.clone();
        ctx.spawn("process health", move |ctx| async move {
            tokio::select! {
                _ = ctx.cancelled() => Ok(()),
                result = monitor.terminated() => {
                    let message = result.err().unwrap_or_else(|| Error::HostLost("unexpected graceful exit".into())).to_string();
                    ctx.fail(message)?; Ok(())
                }
            }
        })?;
        Ok(Arc::new(RemoteService {
            descriptor,
            ipc,
            http,
            endpoint: self.endpoint,
            session,
            activation,
            capacity: Arc::new(Semaphore::new(16)),
            cleanup_capacity: Arc::new(Semaphore::new(16)),
            resources: ready["capabilities"]["resources"]
                .as_bool()
                .unwrap_or(false),
        }))
    }
}
fn identity(a: Activation) -> Identity {
    Identity {
        incarnation: a.incarnation.to_string(),
        fiber: a.fiber.0.to_string(),
        epoch: a.epoch.to_string(),
    }
}

/// A proxy is invoked under the normal native ServiceHandle lease. No raw client is exported.
#[derive(Clone)]
pub struct RemoteService {
    pub(crate) descriptor: cordis_contract::Descriptor,
    ipc: Arc<dyn InvocationBackend>,
    http: Option<Arc<dyn InvocationBackend>>,
    endpoint: Endpoint,
    session: Session,
    activation: Activation,
    capacity: Arc<Semaphore>,
    pub(crate) resources: bool,
    cleanup_capacity: Arc<Semaphore>,
}
impl RemoteService {
    pub async fn invoke_json(&self, context: &Context, method: &str, args: Value) -> Result<Value> {
        self.invoke_json_via(context, self.endpoint, method, args)
            .await
    }
    pub async fn invoke_json_via(
        &self,
        context: &Context,
        endpoint: Endpoint,
        method: &str,
        args: Value,
    ) -> Result<Value> {
        let signature =
            cordis_contract::portable::method(&self.descriptor.interface().methods, method)
                .map_err(Error::InvalidInput)?;
        let input = crate::json_body(&signature.args, &args)?;
        let bytes = self.invoke_via(context, endpoint, method, input).await?;
        crate::body_json(&signature.result, &bytes)
    }
    pub async fn invoke(&self, context: &Context, method: &str, args: Bytes) -> Result<Bytes> {
        self.invoke_via(context, self.endpoint, method, args).await
    }
    pub async fn invoke_via(
        &self,
        context: &Context,
        endpoint: Endpoint,
        method: &str,
        args: Bytes,
    ) -> Result<Bytes> {
        self.invoke_envelope(context, endpoint, method, Payload::message(args).into())
            .await?
            .one()
            .map_err(Into::into)
    }
    pub(crate) async fn invoke_envelope(
        &self,
        context: &Context,
        endpoint: Endpoint,
        method: &str,
        args: Envelope,
    ) -> Result<Envelope> {
        self.managed(
            context,
            endpoint,
            "call",
            args.with_control(json!({"method":method})),
        )
        .await
    }

    pub(crate) async fn managed(
        &self,
        context: &Context,
        endpoint: Endpoint,
        command: &str,
        payload: Envelope,
    ) -> Result<Envelope> {
        if context.activation() != self.activation {
            return Err(Error::Closed);
        }
        let client = self.clone();
        let command = command.to_owned();
        context
            .operation(move |context| async move {
                Ok(client.execute(&context, endpoint, &command, payload).await)
            })
            .await
            .map_err(crate::resource::runtime_error)?
    }

    // Invoked only within an executor-owned operation or acquisition ticket.
    pub(crate) async fn execute(
        &self,
        context: &Context,
        endpoint: Endpoint,
        command: &str,
        mut payload: Envelope,
    ) -> Result<Envelope> {
        if context.activation() != self.activation {
            return Err(Error::Closed);
        }
        let signature = if command == "call" {
            let method = payload.control["method"]
                .as_str()
                .ok_or_else(|| Error::InvalidInput("missing method".into()))?;
            let signature =
                cordis_contract::portable::method(&self.descriptor.interface().methods, method)
                    .map_err(Error::InvalidInput)?;
            payload
                .check(&signature.args)
                .map_err(Error::InvalidInput)?;
            Some(signature)
        } else {
            if command == "resource_acquire" {
                let kind = payload.control["kind"]
                    .as_str()
                    .ok_or_else(|| Error::InvalidInput("missing kind".into()))?;
                let resource = cordis_contract::portable::resource(&self.descriptor, kind)
                    .map_err(Error::InvalidInput)?;
                payload.check(&resource.args).map_err(Error::InvalidInput)?;
            }
            None
        };
        let errors = if command == "resource_acquire" {
            Some(
                &cordis_contract::portable::resource(
                    &self.descriptor,
                    payload.control["kind"].as_str().expect("validated kind"),
                )
                .map_err(Error::InvalidInput)?
                .errors,
            )
        } else {
            signature.map(|s| &s.errors)
        };
        let capacity = if command == "resource_release" {
            &self.cleanup_capacity
        } else {
            &self.capacity
        };
        let permit = capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        let (request, _scope) =
            self.session
                .scoped_envelope(context, command, payload.into_wire())?;
        let backend = match endpoint {
            Endpoint::Ipc => &self.ipc,
            Endpoint::Http => self
                .http
                .as_ref()
                .ok_or_else(|| Error::InvalidInput("HTTP endpoint unavailable".into()))?,
        };
        let _permit = permit;
        let result = backend
            .invoke(request)
            .await
            .map_err(|e| match errors {
                Some(errors) => e.check_declared(errors, "operation errors"),
                None => e,
            })
            .and_then(|value| {
                let mut value = Envelope::from(value);
                if let Some(signature) = signature {
                    value.check(&signature.result).map_err(Error::Protocol)?;
                }
                Ok(value)
            });
        if let Err(
            error @ (Error::OutcomeUnknown(_)
            | Error::Protocol(_)
            | Error::HostLost(_)
            | Error::Io(_)),
        ) = &result
        {
            let _ = self.session.abort(error.clone()).await;
            let _ = context.fail(error.to_string());
        }
        result
    }

    pub(crate) fn endpoint(&self) -> Endpoint {
        self.endpoint
    }
    pub(crate) async fn invalidate(&self, context: &Context, error: Error) {
        let _ = self.session.abort(error.clone()).await;
        let _ = context.fail(error.to_string());
    }
}

trait InvocationBackend: Send + Sync {
    fn invoke(&self, request: cordis_wire::Request) -> BoxFuture<'_, Result<Payload>>;
}
struct Ipc(Session);
impl InvocationBackend for Ipc {
    fn invoke(&self, request: cordis_wire::Request) -> BoxFuture<'_, Result<Payload>> {
        Box::pin(self.0.send(request))
    }
}
impl InvocationBackend for Http {
    fn invoke(&self, request: cordis_wire::Request) -> BoxFuture<'_, Result<Payload>> {
        Box::pin(self.call(request))
    }
}
