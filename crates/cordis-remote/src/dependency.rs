use crate::{Bytes, Error, Payload, RemoteService, Result, Value, json};
use cordis_runtime::{BoxFuture, Context, Plugin, Service};
use cordis_wire::local::Envelope;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};

type Invoke =
    dyn Fn(Context, String, Envelope) -> BoxFuture<'static, Result<Envelope>> + Send + Sync;
type Acquire = dyn Fn(Context, String, Envelope) -> BoxFuture<'static, Result<crate::RemoteResource>>
    + Send
    + Sync;

/// An explicit adapter from a native service contract to portable method calls.
#[derive(Clone)]
pub struct Dependency {
    descriptor: Option<cordis_contract::Descriptor>,
    declaration: Service<()>,
    invoke: Arc<Invoke>,
    acquire: Option<Arc<Acquire>>,
}
impl fmt::Debug for Dependency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Dependency")
            .field(self.declaration.contract())
            .finish()
    }
}
impl Dependency {
    pub fn new<T, F>(
        service: &Service<T>,
        descriptor: cordis_contract::Descriptor,
        invoke: F,
    ) -> Self
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
                    let bytes = invoke(service, ctx, method, args.one()?).await?;
                    // An arbitrary application callback returns a new, unverified body.
                    Ok(Envelope::from(Payload::message(bytes)))
                })
            },
        )
    }
    /// Explicit JSON gateway for applications whose native service API is JSON.
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
                    .map_err(|e| Error::Protocol(e.to_string()))
            })
        })
    }
    fn bind<T, F>(
        service: &Service<T>,
        descriptor: Option<cordis_contract::Descriptor>,
        invoke: F,
    ) -> Self
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
            descriptor,
            declaration,
            acquire: None,
            invoke: Arc::new(move |ctx, method, args| {
                let service = service.clone();
                let invoke = invoke.clone();
                Box::pin(async move {
                    // Resolve through the invocation's definition site, preserving its use site.
                    let handle = ctx
                        .dependency(&service)
                        .map_err(crate::resource::runtime_error)?;
                    handle
                        .call(move |value, context| {
                            Box::pin(async move { Ok(invoke(value, context, method, args).await) })
                        })
                        .await
                        .map_err(crate::resource::runtime_error)?
                })
            }),
        }
    }
    /// Bind both value calls and caller-owned resource acquisition from a process service.
    pub fn remote(service: &Service<RemoteService>) -> Self {
        let mut dependency = Self::bind(
            service,
            service.descriptor().cloned(),
            |remote, ctx, method, args| {
                Box::pin(async move {
                    remote
                        .invoke_envelope(&ctx, remote.endpoint(), &method, args)
                        .await
                })
            },
        );
        let service = service.clone();
        dependency.acquire = Some(Arc::new(move |ctx, kind, args| {
            let service = service.clone();
            Box::pin(async move {
                ctx.dependency(&service)
                    .map_err(crate::resource::runtime_error)?
                    .call(move |remote, ctx| {
                        Box::pin(async move {
                            Ok(remote
                                .acquire_envelope(&ctx, remote.endpoint(), &kind, args)
                                .await)
                        })
                    })
                    .await
                    .map_err(crate::resource::runtime_error)?
            })
        }));
        dependency
    }
    pub(crate) fn declare(&self, plugin: Plugin) -> Plugin {
        plugin.requires(&self.declaration)
    }
}

pub(crate) struct Router {
    dependencies: BTreeMap<String, Dependency>,
    scopes: Mutex<BTreeMap<String, Context>>,
    effects: crate::effects::Effects,
    effects_enabled: AtomicBool,
    sequences: crate::sequence::Sequences,
    sequences_enabled: AtomicBool,
    children: crate::scope::Scopes,
    children_enabled: AtomicBool,
    middleware: crate::middleware::Middlewares,
    middleware_enabled: AtomicBool,
    events: crate::events::Events,
    events_enabled: AtomicBool,
    tasks: crate::task::Tasks,
    tasks_enabled: AtomicBool,
    composition: Arc<crate::composition::Composition>,
    composition_enabled: AtomicBool,
    metadata_enabled: AtomicBool,
    resources: crate::dependency_resources::Resources,
    resources_enabled: AtomicBool,
    metadata_contracts: BTreeSet<cordis_contract::ContractId>,
    primary: OnceLock<cordis_contract::ContractId>,
    session: OnceLock<crate::session::WeakSession>,
}
impl Router {
    pub async fn invoke_payload(
        self: &Arc<Self>,
        command: &str,
        payload: Payload,
    ) -> Result<Payload> {
        use crate::dependency_resources::{failure, success, text};
        if matches!(
            command,
            "child_install"
                | "value_provide"
                | "event_emit"
                | "event_join"
                | "event_status"
                | "middleware_run"
                | "middleware_next"
        ) {
            let enabled = if command.starts_with("event_") {
                &self.events_enabled
            } else if command.starts_with("middleware_") {
                &self.middleware_enabled
            } else {
                &self.composition_enabled
            };
            if !enabled.load(Ordering::Acquire) {
                return Err(Error::InvalidInput("capability not granted".into()));
            }
            if matches!(command, "event_join" | "event_status") {
                return self.events.invoke(command, &payload.control_only()?).await;
            }
            let args = payload.one()?;
            let control = payload.control;
            let context = self
                .scopes
                .lock()
                .unwrap()
                .get(&text(&control, "context")?)
                .cloned()
                .ok_or(Error::Closed)?;
            let router = self.clone();
            let command = command.to_owned();
            return context
                .operation(move |ctx| async move {
                    let result: Result<Payload> = async {
                        match command.as_str() {
                            "child_install" => {
                                Ok(match router.composition.install(ctx, control, args).await {
                                    Ok(value) => success(value).into(),
                                    Err(error) => failure(error, true).into(),
                                })
                            }
                            "value_provide" => router
                                .composition
                                .provide(&ctx, &control, args)
                                .map(Into::into),
                            "event_emit" => {
                                router.events.emit(&ctx, &control, args).map(Into::into)
                            }
                            "middleware_run" => router
                                .middleware
                                .run(
                                    &ctx,
                                    &control,
                                    args,
                                    router.session.get().ok_or(Error::Closed)?.clone(),
                                )
                                .await
                                .map(Payload::message),
                            "middleware_next" => router
                                .middleware
                                .continuation(&control)?
                                .run(args)
                                .await
                                .map(Payload::message)
                                .map_err(crate::middleware::runtime_error),
                            _ => unreachable!(),
                        }
                    }
                    .await;
                    Ok(result)
                })
                .await
                .map_err(crate::resource::runtime_error)?;
        }

        if !matches!(
            command,
            "dependency_call"
                | "cleanup_call"
                | "dependency_resource_acquire"
                | "dependency_resource_call"
                | "cleanup_resource_call"
                | "dependency_resource_dispose"
        ) {
            return self
                .invoke(command, payload.control_only()?)
                .await
                .map(Into::into);
        }
        let token = text(&payload.control, "context")?;
        let context = self
            .scopes
            .lock()
            .unwrap()
            .get(&token)
            .cloned()
            .ok_or(Error::Closed)?;
        if command == "cleanup_call"
            && (!context.is_cleanup()
                || !(self.tasks_enabled.load(Ordering::Acquire)
                    || self.events_enabled.load(Ordering::Acquire)
                    || self.middleware_enabled.load(Ordering::Acquire)))
        {
            return Err(Error::Closed);
        }
        if matches!(command, "dependency_call" | "cleanup_call") {
            let name = text(&payload.control, "dependency")?;
            let dependency = self.dependencies.get(&name).ok_or_else(|| Error::Refused {
                code: "undeclared_dependency".into(),
                message: name,
            })?;
            let method = text(&payload.control, "method")?;
            let signature = cordis_contract::portable::method(
                &dependency
                    .descriptor
                    .as_ref()
                    .expect("validated descriptor")
                    .interface()
                    .methods,
                &method,
            )
            .map_err(Error::InvalidInput)?
            .clone();
            let mut args = Envelope::from(Payload::message(payload.one()?));
            args.check(&signature.args).map_err(Error::InvalidInput)?;
            let invoke = dependency.invoke.clone();
            return context
                .operation(move |ctx| async move {
                    Ok(invoke(ctx, method, args)
                        .await
                        .map_err(|e| e.check_declared(&signature.errors, "dependency errors"))
                        .and_then(|mut value| {
                            value.check(&signature.result).map_err(Error::Protocol)?;
                            Ok(value.into_wire())
                        }))
                })
                .await
                .map_err(crate::resource::runtime_error)?;
        }
        if !self.resources_enabled.load(Ordering::Acquire) {
            return Ok(failure(
                Error::Refused {
                    code: "unsupported_resource".into(),
                    message: "dependency resource capability not negotiated".into(),
                },
                false,
            )
            .into());
        }
        if (command == "dependency_resource_acquire" && context.is_cleanup())
            || (command == "cleanup_resource_call" && !context.is_cleanup())
        {
            return Err(Error::Closed);
        }
        let router = self.clone();
        let command = command.to_owned();
        context
            .operation(move |ctx| async move {
                let result: Result<Payload> = async {
                    if command == "dependency_resource_acquire" {
                        let name = text(&payload.control, "dependency")?;
                        let dependency =
                            router
                                .dependencies
                                .get(&name)
                                .ok_or_else(|| Error::Refused {
                                    code: "undeclared_dependency".into(),
                                    message: name,
                                })?;
                        let acquire =
                            dependency.acquire.as_ref().ok_or_else(|| Error::Refused {
                                code: "unsupported_resource".into(),
                                message: "dependency has no resource binding".into(),
                            })?;
                        let kind = text(&payload.control, "kind")?;
                        let mut args = Envelope::from(Payload::message(payload.one()?));
                        let schema = cordis_contract::portable::resource(
                            dependency
                                .descriptor
                                .as_ref()
                                .expect("validated descriptor"),
                            &kind,
                        )
                        .map_err(Error::InvalidInput)?;
                        args.check(&schema.args).map_err(Error::InvalidInput)?;
                        let (id, entry) = router.resources.reserve(&payload.control)?;
                        return Ok(match acquire(ctx, kind, args).await {
                            Ok(resource) => entry.commit(id, resource).into(),
                            Err(error) => failure(error, true).into(),
                        });
                    }
                    let resource = router.resources.get(&payload.control)?;
                    if command == "dependency_resource_dispose" {
                        if !payload.messages.is_empty() {
                            return Err(Error::InvalidInput("unexpected business message".into()));
                        }
                        return Ok(match resource.dispose().await {
                            Ok(()) => success(Value::Null),
                            Err(e) => failure(e, false),
                        }
                        .into());
                    }
                    Ok(
                        match resource
                            .invoke(&text(&payload.control, "method")?, payload.one()?)
                            .await
                        {
                            Ok(bytes) => Payload::with_message(success(Value::Null), bytes),
                            Err(error) => failure(error, false).into(),
                        },
                    )
                }
                .await;
                Ok(match result {
                    Ok(value) => value,
                    Err(e) => failure(e, false).into(),
                })
            })
            .await
            .map_err(crate::resource::runtime_error)
    }
    pub fn new(spec: &crate::ProcessSpec) -> Result<Arc<Self>> {
        let mut entries = BTreeMap::new();
        for dependency in &spec.dependencies {
            if dependency.descriptor.is_none() {
                return Err(Error::InvalidInput(format!(
                    "dependency {}: interface descriptor required",
                    dependency.declaration.contract()
                )));
            }
            let name = dependency.declaration.contract().name.to_string();
            let mut dependency = dependency.clone();
            dependency.descriptor = dependency
                .descriptor
                .map(|d| d.with_validation(spec.validation));
            if entries.insert(name, dependency).is_some() {
                return Err(Error::InvalidInput("duplicate dependency name".into()));
            }
        }
        Ok(Arc::new(Self {
            dependencies: entries,
            scopes: Mutex::default(),
            effects: crate::effects::Effects::default(),
            effects_enabled: AtomicBool::new(false),
            sequences: crate::sequence::Sequences::default(),
            sequences_enabled: AtomicBool::new(false),
            children: crate::scope::Scopes::default(),
            children_enabled: AtomicBool::new(false),
            middleware: crate::middleware::Middlewares::new(&spec.middleware)?,
            middleware_enabled: AtomicBool::new(false),
            events: crate::events::Events::new(&spec.events)?,
            events_enabled: AtomicBool::new(false),
            tasks: crate::task::Tasks::default(),
            tasks_enabled: AtomicBool::new(false),
            composition: crate::composition::Composition::new(
                &spec.factories,
                &spec.provisions,
                spec.composition.clone(),
            )?,
            composition_enabled: AtomicBool::new(false),
            metadata_enabled: AtomicBool::new(false),
            resources: crate::dependency_resources::Resources::default(),
            resources_enabled: AtomicBool::new(false),
            metadata_contracts: spec
                .dependencies
                .iter()
                .map(|d| d.declaration.contract().clone())
                .chain(spec.provisions.iter().map(|p| p.service.contract().clone()))
                .collect(),
            primary: OnceLock::new(),
            session: OnceLock::new(),
        }))
    }
    pub fn attach(&self, session: crate::session::WeakSession) {
        let _ = self.session.set(session);
    }
    pub fn composition_declarations(&self) -> Value {
        self.composition.declarations()
    }
    pub fn validate_primary(&self, primary: &cordis_contract::ContractId) -> Result<()> {
        self.composition.validate_primary(primary)?;
        let _ = self.primary.set(primary.clone());
        Ok(())
    }
    pub fn enable_composition(&self, enabled: bool) {
        self.composition_enabled.store(enabled, Ordering::Release);
    }
    pub fn enable_metadata(&self, enabled: bool) {
        self.metadata_enabled.store(enabled, Ordering::Release);
    }
    pub fn enable_dependency_resources(&self, enabled: bool) {
        self.resources_enabled.store(enabled, Ordering::Release);
    }
    pub fn enable_sequences(&self, enabled: bool) {
        self.sequences_enabled.store(enabled, Ordering::Release);
    }
    pub fn middleware_declarations(&self) -> Value {
        self.middleware.declarations()
    }
    pub fn enable_middleware(&self, enabled: bool) {
        self.middleware_enabled.store(enabled, Ordering::Release);
    }
    pub fn event_declarations(&self) -> Value {
        self.events.declarations()
    }
    pub fn enable_events(&self, enabled: bool) {
        self.events_enabled.store(enabled, Ordering::Release);
    }
    pub fn enable_tasks(&self, enabled: bool) {
        self.tasks_enabled.store(enabled, Ordering::Release);
    }
    pub fn enable_scopes(&self, enabled: bool) {
        self.children_enabled.store(enabled, Ordering::Release);
    }
    pub fn enable_effects(&self, enabled: bool) {
        self.effects_enabled.store(enabled, Ordering::Release);
    }
    pub fn declarations(&self) -> Value {
        Value::Object(
            self.dependencies
                .iter()
                .map(|(name, dependency)| {
                    (
                        name.clone(),
                        json!(dependency.declaration.contract().revision.as_ref()),
                    )
                })
                .collect(),
        )
    }
    pub fn interfaces(&self) -> Value {
        Value::Object(
            self.dependencies
                .iter()
                .map(|(name, dependency)| {
                    (
                        name.clone(),
                        cordis_contract::portable::encode(
                            dependency
                                .descriptor
                                .as_ref()
                                .expect("validated at construction"),
                        ),
                    )
                })
                .collect(),
        )
    }
    pub fn scope(
        self: &Arc<Self>,
        context: &Context,
        payload: &mut Value,
    ) -> Result<Option<Scope>> {
        if self.dependencies.is_empty()
            && !self.effects_enabled.load(Ordering::Acquire)
            && !self.sequences_enabled.load(Ordering::Acquire)
            && !self.children_enabled.load(Ordering::Acquire)
            && !self.tasks_enabled.load(Ordering::Acquire)
            && !self.events_enabled.load(Ordering::Acquire)
            && !self.middleware_enabled.load(Ordering::Acquire)
            && !self.composition_enabled.load(Ordering::Acquire)
            && !self.metadata_enabled.load(Ordering::Acquire)
            && !self.resources_enabled.load(Ordering::Acquire)
        {
            return Ok(None);
        }
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).map_err(|e| Error::Io(e.to_string()))?;
        let token: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        if payload.is_null() {
            *payload = json!({});
        }
        let object = payload
            .as_object_mut()
            .ok_or_else(|| Error::InvalidInput("context requires object payload".into()))?;
        object.insert("context".into(), json!(token));
        self.scopes
            .lock()
            .unwrap()
            .insert(token.clone(), context.snapshot());
        Ok(Some(Scope {
            router: self.clone(),
            token,
        }))
    }
    pub async fn invoke(self: &Arc<Self>, command: &str, payload: Value) -> Result<Value> {
        if command.starts_with("child_") || command == "value_provide" {
            if !self.composition_enabled.load(Ordering::Acquire) {
                return Err(Error::InvalidInput(
                    "composition capability not negotiated".into(),
                ));
            }
            if !matches!(command, "child_install" | "value_provide") {
                return self
                    .composition
                    .control(
                        command,
                        &payload,
                        self.session.get().ok_or(Error::Closed)?.clone(),
                    )
                    .await;
            }
        } else if command.starts_with("middleware_") {
            if !self.middleware_enabled.load(Ordering::Acquire) {
                return Err(Error::InvalidInput(
                    "middleware capability not negotiated".into(),
                ));
            }
            if command == "middleware_dispose" {
                return self.middleware.dispose(&payload).await;
            }
        } else if command.starts_with("event_") {
            if !self.events_enabled.load(Ordering::Acquire) {
                return Err(Error::InvalidInput(
                    "events capability not negotiated".into(),
                ));
            }
            if !matches!(command, "event_register" | "event_emit") {
                return self
                    .events
                    .invoke(command, &payload)
                    .await?
                    .control_only()
                    .map_err(Into::into);
            }
        } else if command.starts_with("task_") {
            if !self.tasks_enabled.load(Ordering::Acquire) {
                return Err(Error::InvalidInput("task capability not negotiated".into()));
            }
            if command != "task_register" {
                return self.tasks.invoke(command, &payload).await;
            }
        } else if command.starts_with("scope_") {
            if !self.children_enabled.load(Ordering::Acquire) {
                return Err(Error::InvalidInput(
                    "scope capability not negotiated".into(),
                ));
            }
            if command != "scope_register" {
                return self
                    .children
                    .invoke(
                        command,
                        &payload,
                        self.session.get().ok_or(Error::Closed)?.clone(),
                    )
                    .await;
            }
        } else if command.starts_with("sequence_") {
            if !self.sequences_enabled.load(Ordering::Acquire) {
                return Err(Error::InvalidInput(
                    "sequence capability not negotiated".into(),
                ));
            }
            if command != "sequence_register" {
                return self.sequences.invoke(command, &payload).await;
            }
        } else if !matches!(
            command,
            "dependency_call" | "cleanup_call" | "context_metadata" | "cleanup_metadata"
        ) && !self.effects_enabled.load(Ordering::Acquire)
        {
            return Err(Error::InvalidInput(
                "effects capability not negotiated".into(),
            ));
        }
        if command == "effect_dispose" {
            return self.effects.dispose(&payload).await;
        }
        let text = |key: &str| {
            payload[key]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| Error::InvalidInput(format!("missing {key}")))
        };
        let token = text("context")?;
        let context = self
            .scopes
            .lock()
            .unwrap()
            .get(&token)
            .cloned()
            .ok_or(Error::Closed)?;
        if matches!(command, "context_metadata" | "cleanup_metadata") {
            if !self.metadata_enabled.load(Ordering::Acquire) {
                return Err(Error::InvalidInput(
                    "metadata capability not negotiated".into(),
                ));
            }
            if command == "cleanup_metadata" && !context.is_cleanup() {
                return Err(Error::Closed);
            }
            let name = text("name")?;
            let revision = text("revision")?;
            if name.is_empty() || revision.is_empty() || name.len() > 1024 || revision.len() > 1024
            {
                return Err(Error::InvalidInput("invalid metadata contract".into()));
            }
            let contract = cordis_contract::ContractId::new(name.clone(), revision.clone());
            if self.primary.get() != Some(&contract) && !self.metadata_contracts.contains(&contract)
            {
                return Err(Error::Refused {
                    code: "undeclared_metadata".into(),
                    message: contract.to_string(),
                });
            }
            return context
                .operation(move |ctx| async move {
                    let layers = ctx.metadata::<_, Value>(&Service::<()>::new(name, revision))?;
                    Ok(
                        json!({"revision":ctx.metadata_revision().map(|r|r.to_string()),
                    "layers":layers.iter().map(|v|v.as_ref()).collect::<Vec<_>>()}),
                    )
                })
                .await
                .map_err(crate::resource::runtime_error);
        }
        if command == "cleanup_call"
            && (!context.is_cleanup()
                || !(self.tasks_enabled.load(Ordering::Acquire)
                    || self.events_enabled.load(Ordering::Acquire)
                    || self.middleware_enabled.load(Ordering::Acquire)))
        {
            return Err(Error::Closed);
        }
        if command.starts_with("middleware_") {
            let router = self.clone();
            let command = command.to_owned();
            return context
                .operation(move |ctx| async move {
                    let result: Result<Value> = async {
                        match command.as_str() {
                            "middleware_register" => router.middleware.register(
                                &ctx,
                                &payload,
                                router.session.get().ok_or(Error::Closed)?.clone(),
                            ),
                            _ => Err(Error::InvalidInput("unknown middleware command".into())),
                        }
                    }
                    .await;
                    Ok(result)
                })
                .await
                .map_err(crate::middleware::runtime_error)?;
        }
        if command == "event_register" {
            let router = self.clone();
            return context
                .operation(move |ctx| async move {
                    Ok(router.events.register(
                        &ctx,
                        &payload,
                        router.session.get().ok_or(Error::Closed)?.clone(),
                    ))
                })
                .await
                .map_err(crate::resource::runtime_error)?;
        }
        if command == "task_register" {
            let router = self.clone();
            return context
                .operation(move |ctx| async move {
                    Ok(router.tasks.register(
                        &ctx,
                        &payload,
                        router.session.get().ok_or(Error::Closed)?.clone(),
                    ))
                })
                .await
                .map_err(crate::resource::runtime_error)?;
        }
        if command == "scope_register" {
            let router = self.clone();
            return context
                .operation(move |ctx| async move { Ok(router.children.register(&ctx, &payload)) })
                .await
                .map_err(crate::resource::runtime_error)?;
        }
        if command == "sequence_register" {
            let router = self.clone();
            return context
                .operation(move |ctx| async move {
                    Ok(router.sequences.register(
                        &ctx,
                        &payload,
                        router.session.get().ok_or(Error::Closed)?.clone(),
                    ))
                })
                .await
                .map_err(crate::resource::runtime_error)?;
        }
        if command == "effect_register" {
            let router = self.clone();
            return context
                .operation(move |ctx| async move {
                    Ok(router.effects.register(
                        &ctx,
                        &payload,
                        router.session.get().ok_or(Error::Closed)?.clone(),
                    ))
                })
                .await
                .map_err(crate::resource::runtime_error)?;
        }
        Err(Error::InvalidInput("unknown control command".into()))
    }
}
pub(crate) struct Scope {
    router: Arc<Router>,
    token: String,
}
impl Drop for Scope {
    fn drop(&mut self) {
        self.router.scopes.lock().unwrap().remove(&self.token);
    }
}

pub(crate) fn outcome(result: Result<Value>) -> Value {
    match result {
        Ok(value) => json!({"kind":"success", "value":value}),
        Err(Error::Remote { code, message }) => {
            json!({"kind":"failure", "error":{"code":code,"message":message}})
        }
        Err(error) => {
            let kind = if matches!(
                error,
                Error::Refused { .. } | Error::Closed | Error::Overloaded | Error::InvalidInput(_)
            ) {
                "refused"
            } else {
                "failure"
            };
            let (code, message) = match error {
                Error::Refused { code, message } => (code, message),
                Error::Closed => ("closed".into(), "invocation context expired".into()),
                Error::Overloaded => (
                    "overloaded".into(),
                    "dependency call capacity reached".into(),
                ),
                Error::InvalidInput(message) => ("invalid_argument".into(), message),
                other => ("dependency_failed".into(), other.to_string()),
            };
            json!({"kind":kind, "error":{"code":code, "message":message}})
        }
    }
}

pub(crate) fn payload_outcome(result: Result<cordis_wire::Payload>) -> cordis_wire::Outcome {
    use cordis_wire::{Outcome, RemoteError};
    match result {
        Ok(value) => Outcome::Success { value },
        Err(Error::Remote { code, message }) => Outcome::Failure {
            error: RemoteError { code, message },
        },
        Err(Error::Refused { code, message }) => Outcome::Refused {
            error: RemoteError { code, message },
        },
        Err(Error::Closed) => Outcome::Refused {
            error: RemoteError {
                code: "closed".into(),
                message: "invocation context expired".into(),
            },
        },
        Err(Error::Overloaded) => Outcome::Refused {
            error: RemoteError {
                code: "overloaded".into(),
                message: "dependency call capacity reached".into(),
            },
        },
        Err(Error::InvalidInput(message)) => Outcome::Refused {
            error: RemoteError {
                code: "invalid_argument".into(),
                message,
            },
        },
        Err(error) => Outcome::Unknown {
            error: RemoteError {
                code: "outcome_unknown".into(),
                message: error.to_string(),
            },
        },
    }
}
