use super::*;
use crate::json;
use cordis_runtime::{
    EffectStep, Inverse, Next, TaskHandle, WeakEffectHandle, WeakEffectSequence, WeakPluginHandle,
    WeakScopeHandle,
};
use std::sync::{
    OnceLock,
    atomic::{AtomicU64, Ordering},
};

struct Request {
    result: tokio::sync::watch::Receiver<Option<Result<Payload>>>,
    lane: u8,
}
struct Entry {
    context: Context,
    open: bool,
    requests: BTreeMap<u64, Request>,
}
enum Handle {
    Reserved,
    Effect(WeakEffectHandle),
    Scope(WeakScopeHandle),
    Task(TaskHandle),
    Dispatch(tokio::sync::watch::Receiver<Option<Result<Payload>>>),
    Sequence(WeakEffectSequence),
    Plugin(WeakPluginHandle),
    Resource(resources::WeakContextResource),
}
pub(super) struct Router {
    instance: Weak<Instance>,
    entries: Mutex<BTreeMap<String, Entry>>,
    handles: Mutex<BTreeMap<String, Handle>>,
    next: Mutex<BTreeMap<String, Next<Bytes, Bytes>>>,
    ids: AtomicU64,
}
pub(super) fn token() -> Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(unknown)?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}
fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key]
        .as_str()
        .ok_or_else(|| Error::InvalidInput(format!("missing {key}")))
}
fn callback_id(value: &Value) -> Result<u64> {
    value["callback"]
        .as_u64()
        .ok_or_else(|| Error::InvalidInput("missing callback".into()))
}
// A failed registration must not consume lifetime handle capacity. Successful
// grants remain as tombstones so disposal can reuse the native cached result.
struct Reservation<'a> {
    router: &'a Router,
    key: String,
}
impl Reservation<'_> {
    fn commit(self, handle: Handle) -> String {
        self.router
            .handles
            .lock()
            .unwrap()
            .insert(self.key.clone(), handle);
        self.key.clone()
    }
}
impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        let mut handles = self.router.handles.lock().unwrap();
        if matches!(handles.get(&self.key), Some(Handle::Reserved)) {
            handles.remove(&self.key);
        }
    }
}
fn validate_step(value: Value) -> Result<Value> {
    let valid = value.as_object().is_some_and(|o| {
        o.len() == 3
            && o.get("complete").is_some_and(Value::is_boolean)
            && o.get("inverse")
                .is_some_and(|v| v.is_null() || v.as_u64().is_some_and(|id| id > 0))
            && o.get("failure")
                .is_some_and(|v| v.is_null() || v.is_string())
    });
    if valid {
        Ok(value)
    } else {
        Err(unknown(
            "malformed Effect step; inverse responsibility unresolved",
        ))
    }
}
pub(super) struct Scope {
    router: Arc<Router>,
    pub token: String,
}
impl Router {
    pub fn new(instance: Weak<Instance>) -> Arc<Self> {
        Arc::new(Self {
            instance,
            entries: Mutex::default(),
            handles: Mutex::default(),
            next: Mutex::default(),
            ids: AtomicU64::new(1),
        })
    }
    pub fn enter(self: &Arc<Self>, ctx: &Context) -> Result<Scope> {
        let token = token()?;
        self.entries.lock().unwrap().insert(
            token.clone(),
            Entry {
                context: ctx.snapshot(),
                open: true,
                requests: BTreeMap::new(),
            },
        );
        Ok(Scope {
            router: self.clone(),
            token,
        })
    }
    pub async fn call(
        self: &Arc<Self>,
        instance: &Arc<Instance>,
        key: &str,
        command: String,
        args: Payload,
    ) -> Result<Payload> {
        if command.len() > 128 {
            return Err(Error::InvalidInput("command too long".into()));
        }
        let (ctx, id, send, mut receive) = {
            let mut entries = self.entries.lock().unwrap();
            let ctx = entries
                .get(key)
                .filter(|e| e.open)
                .ok_or(Error::Closed)?
                .context
                .clone();
            // Ordinary work cannot exhaust cleanup or explicit disposal lanes.
            let lane = if ctx.is_cleanup() {
                1
            } else if command == "dependency-resource-dispose" {
                2
            } else {
                0
            };
            if entries
                .values()
                .flat_map(|entry| entry.requests.values())
                .filter(|request| request.lane == lane)
                .count()
                >= instance.module.capabilities.max_requests
            {
                return Err(Error::Overloaded);
            }
            let id = self.ids.fetch_add(1, Ordering::Relaxed);
            let (send, receive) = tokio::sync::watch::channel(None);
            entries.get_mut(key).unwrap().requests.insert(
                id,
                Request {
                    result: receive.clone(),
                    lane,
                },
            );
            (ctx, id, send, receive)
        };
        let router = self.clone();
        // The admitted operation owns its responsibility even if the native
        // component task traps or its import future is subsequently dropped.
        tokio::spawn(async move {
            let result = ctx
                .operation(move |ctx| async move {
                    Ok(router.dispatch_payload(ctx, command, args).await)
                })
                .await
                .map_err(Error::from)
                .and_then(|result| result);
            send.send_replace(Some(result));
        });
        let result = wait_result(&mut receive).await;
        if let Some(entry) = self.entries.lock().unwrap().get_mut(key) {
            entry.requests.remove(&id);
        }
        result
    }
    fn reserve(&self) -> Result<Reservation<'_>> {
        let instance = self.instance.upgrade().ok_or(Error::Closed)?;
        let mut handles = self.handles.lock().unwrap();
        if handles.len() >= instance.module.capabilities.max_handles {
            return Err(Error::Overloaded);
        }
        let grant = token()?;
        handles.insert(grant.clone(), Handle::Reserved);
        Ok(Reservation {
            router: self,
            key: grant,
        })
    }
    fn subscription_cleanup(
        &self,
        ctx: &Context,
        id: u64,
        register: impl FnOnce() -> cordis_runtime::Result<cordis_runtime::EffectHandle>,
    ) -> Result<WeakEffectHandle> {
        let native = Arc::new(OnceLock::<WeakEffectHandle>::new());
        let release = native.clone();
        let instance = self.instance.clone();
        // Storage precedes native registration, so ordered recovery unregisters
        // and drains callbacks before releasing guest captures.
        let cleanup = ctx.defer("Wasm callback storage", move |ctx| async move {
            if let Some(effect) = release.get().and_then(WeakEffectHandle::upgrade) {
                effect.dispose().await?;
            }
            let instance = instance.upgrade().ok_or(cordis_runtime::Error::Closed)?;
            instance.call(&ctx, "forget", json!(id)).await?;
            Ok(())
        })?;
        let effect = register()?;
        let _ = native.set(effect.downgrade());
        Ok(cleanup.downgrade())
    }
    async fn dispatch_payload(
        self: Arc<Self>,
        ctx: Context,
        command: String,
        payload: Payload,
    ) -> Result<Payload> {
        let instance = self.instance.upgrade().ok_or(Error::Closed)?;
        let args = &payload.control;
        match command.as_str() {
            "invoke" => {
                let name = text(args, "dependency")?;
                let dependency = instance
                    .module
                    .capabilities
                    .dependencies
                    .iter()
                    .find(|d| d.declaration.contract().name.as_ref() == name)
                    .ok_or_else(|| refused("undeclared_dependency", name))?;
                let method = text(args, "method")?;
                let signature = contract::method(
                    &dependency
                        .descriptor
                        .as_ref()
                        .expect("validated binding")
                        .interface()
                        .methods,
                    method,
                )
                .map_err(Error::InvalidInput)?;
                let mut value =
                    Envelope::from(Payload::message(payload.one().map_err(invalid_payload)?));
                value.check(&signature.args).map_err(Error::InvalidInput)?;
                let result = (dependency.invoke)(ctx.clone(), method.into(), value)
                    .await
                    .map_err(|e| e.check_declared(&signature.errors, "dependency errors"))
                    .and_then(|mut value| {
                        value.check(&signature.result).map_err(unknown)?;
                        Ok(value.with_control(Value::Null).into_wire())
                    });
                if let Err(error @ Error::Unknown(_)) = &result {
                    instance.fault.lock().unwrap().get_or_insert(error.clone());
                    let _ = ctx.fail(error.to_string());
                }
                result
            }
            "dependency-resource-acquire" => {
                if ctx.is_cleanup() {
                    return Err(refused(
                        "cleanup_resource",
                        "cleanup cannot acquire resources",
                    ));
                }
                let name = text(args, "dependency")?;
                let kind = text(args, "kind")?.to_owned();
                let dependency = instance
                    .module
                    .capabilities
                    .dependencies
                    .iter()
                    .find(|d| d.declaration.contract().name.as_ref() == name)
                    .ok_or_else(|| refused("undeclared_dependency", name))?;
                let acquire = dependency.acquire.as_ref().ok_or_else(|| {
                    refused("unsupported_resource", "dependency has no resource binding")
                })?;
                let mut value =
                    Envelope::from(Payload::message(payload.one().map_err(invalid_payload)?));
                let factory = contract::resource(
                    dependency.descriptor.as_ref().expect("validated binding"),
                    &kind,
                )
                .map_err(Error::InvalidInput)?;
                value.check(&factory.args).map_err(Error::InvalidInput)?;
                let grant = self.reserve()?;
                let resource = acquire(ctx, kind, value)
                    .await
                    .map_err(resources::uncertainty)?;
                Ok(json!(grant.commit(Handle::Resource(resource.downgrade()))).into())
            }
            "dependency-resource-invoke" => {
                let resource = match self.handles.lock().unwrap().get(text(args, "grant")?) {
                    Some(Handle::Resource(r)) => r.clone(),
                    _ => return Err(Error::Closed),
                };
                resource
                    .invoke(
                        text(args, "method")?,
                        payload.one().map_err(invalid_payload)?,
                    )
                    .await
                    .map(Payload::message)
                    .map_err(resources::uncertainty)
            }
            "dependency-resource-dispose" => {
                payload.clone().control_only().map_err(invalid_payload)?;
                if ctx.is_cleanup() {
                    return Err(refused(
                        "cleanup_resource",
                        "cleanup cannot synchronously dispose resources",
                    ));
                }
                let resource = match self.handles.lock().unwrap().get(text(args, "grant")?) {
                    Some(Handle::Resource(r)) => r.clone(),
                    _ => return Err(Error::Closed),
                };
                resource
                    .dispose()
                    .await
                    .map(|_| Value::Null.into())
                    .map_err(resources::uncertainty)
            }
            "install" => {
                let name = text(args, "factory")?;
                let factory = instance
                    .module
                    .capabilities
                    .factories
                    .get(name)
                    .ok_or_else(|| refused("undeclared_factory", name))?
                    .clone();
                let config = payload.one().map_err(invalid_payload)?;
                let grant = self.reserve()?;
                let child =
                    tokio::task::spawn_blocking(move || ctx.install_with(|| Ok(factory(config)?)))
                        .await
                        .map_err(unknown)?
                        .map_err(Error::from)?;
                Ok(json!(grant.commit(Handle::Plugin(child.downgrade()))).into())
            }
            "provide" => {
                let name = text(args, "service")?;
                let service = instance
                    .module
                    .capabilities
                    .provisions
                    .get(name)
                    .ok_or_else(|| refused("undeclared_provision", name))?;
                ctx.provide(service, Arc::new(payload.one().map_err(invalid_payload)?))?;
                Ok(Value::Null.into())
            }
            "emit" | "event-emit" => {
                let name = text(args, "event")?;
                let event = instance
                    .module
                    .capabilities
                    .events
                    .get(name)
                    .ok_or_else(|| refused("undeclared_event", name))?;
                let value = payload.one().map_err(invalid_payload)?;
                let grant = if command == "event-emit" {
                    Some(self.reserve()?)
                } else {
                    None
                };
                let dispatch = if args["serial"] == true {
                    event.emit_serial(&ctx, value)?
                } else {
                    event.emit(&ctx, value)?
                };
                if let Some(grant) = grant {
                    let (send, receive) = tokio::sync::watch::channel(None);
                    tokio::spawn(async move {
                        send.send_replace(Some(
                            dispatch.join().await.map(flows_payload).map_err(Into::into),
                        ));
                    });
                    Ok(json!(grant.commit(Handle::Dispatch(receive))).into())
                } else {
                    Ok(flows_payload(dispatch.join().await?))
                }
            }
            "event-join" | "event-status" => {
                payload.clone().control_only().map_err(invalid_payload)?;
                let mut result = match self.handles.lock().unwrap().get(text(args, "grant")?) {
                    Some(Handle::Dispatch(d)) => d.clone(),
                    _ => return Err(Error::Closed),
                };
                if command == "event-status" {
                    Ok(
                        json!({"state":if result.borrow().is_some(){"completed"}else{"running"}})
                            .into(),
                    )
                } else {
                    wait_result(&mut result).await
                }
            }
            "next" => {
                let next = self
                    .next
                    .lock()
                    .unwrap()
                    .get(text(args, "grant")?)
                    .cloned()
                    .ok_or(Error::Closed)?;
                next.run(payload.one().map_err(invalid_payload)?)
                    .await
                    .map(Payload::message)
                    .map_err(Into::into)
            }
            "run" => {
                let name = text(args, "name")?;
                let chain = instance
                    .module
                    .capabilities
                    .middleware
                    .get(name)
                    .cloned()
                    .ok_or_else(|| refused("undeclared_middleware", name))?;
                let id = callback_id(args)?;
                let weak = self.instance.clone();
                chain
                    .run(
                        &ctx,
                        payload.one().map_err(invalid_payload)?,
                        move |args, ctx| {
                            let weak = weak.clone();
                            async move {
                                weak.upgrade()
                                    .ok_or(Error::Closed)?
                                    .callback_payload(&ctx, id, Payload::message(args))
                                    .await
                                    .and_then(|value| value.one().map_err(unknown))
                                    .map_err(Into::into)
                            }
                        },
                    )
                    .await
                    .map(Payload::message)
                    .map_err(Into::into)
            }
            _ => self
                .dispatch(
                    ctx,
                    command,
                    payload.control_only().map_err(invalid_payload)?,
                )
                .await
                .map(Into::into),
        }
    }
    async fn dispatch(
        self: Arc<Self>,
        ctx: Context,
        command: String,
        args: Value,
    ) -> Result<Value> {
        let instance = self.instance.upgrade().ok_or(Error::Closed)?;
        match command.as_str() {
            "metadata" => {
                let name = text(&args, "name")?;
                let revision = text(&args, "revision")?;
                if name.is_empty()
                    || revision.is_empty()
                    || name.len() > 1024
                    || revision.len() > 1024
                {
                    return Err(refused("invalid_argument", "invalid metadata contract"));
                }
                let service = cordis_runtime::Service::<()>::new(name, revision);
                let contract = service.contract();
                if &instance.module.contract != contract
                    && !instance
                        .module
                        .capabilities
                        .dependencies
                        .iter()
                        .any(|d| d.declaration.contract() == contract)
                    && !instance
                        .module
                        .capabilities
                        .provisions
                        .values()
                        .any(|p| p.contract() == contract)
                {
                    return Err(refused("undeclared_metadata", contract.to_string()));
                }
                let layers = ctx.metadata::<_, Value>(&service)?;
                Ok(
                    json!({"revision":ctx.metadata_revision().map(|r|r.to_string()),
                    "layers":layers.iter().map(|v|v.as_ref()).collect::<Vec<_>>()}),
                )
            }
            "info" => Ok(
                json!({"definition":format!("{:?}",ctx.activation()),"owner":format!("{:?}",ctx.owner()),"caller":ctx.caller().map(|c|format!("{c:?}")),"cleanup":ctx.is_cleanup()}),
            ),
            "defer" => {
                let grant = self.reserve()?;
                let id = callback_id(&args)?;
                let weak = self.instance.clone();
                let handle = ctx.defer(text(&args, "label")?, move |cleanup| async move {
                    let instance = weak.upgrade().ok_or(cordis_runtime::Error::Closed)?;
                    instance.callback(&cleanup, id, Value::Null).await?;
                    instance.call(&cleanup, "forget", json!(id)).await?;
                    Ok(())
                })?;
                Ok(json!(grant.commit(Handle::Effect(handle.downgrade()))))
            }
            "dispose" => {
                let handle = match self.handles.lock().unwrap().get(text(&args, "grant")?) {
                    Some(Handle::Effect(h)) => h.upgrade().ok_or(Error::Closed)?,
                    _ => return Err(Error::Closed),
                };
                handle.dispose().await?;
                Ok(Value::Null)
            }
            "scope" => {
                let grant = self.reserve()?;
                let scope = ctx
                    .scope(text(&args, "label")?)
                    .map_err(|e| refused("scope_rejected", e))?;
                Ok(json!(grant.commit(Handle::Scope(scope.downgrade()))))
            }
            "scope-run" | "scope-dispose" | "scope-ready" | "scope-status" => {
                let scope = match self.handles.lock().unwrap().get(text(&args, "grant")?) {
                    Some(Handle::Scope(s)) => s.upgrade().ok_or(Error::Closed)?,
                    _ => return Err(Error::Closed),
                };
                if command == "scope-ready" {
                    scope.ready().await?;
                    return Ok(Value::Null);
                }
                if command == "scope-status" {
                    let s = scope.snapshot();
                    return Ok(
                        json!({"state":format!("{:?}",s.state).to_lowercase(),"retired":s.retired,"error":s.outcome.map(|e|format!("{e:?}"))}),
                    );
                }
                if command == "scope-dispose" {
                    scope.dispose().await?;
                    Ok(Value::Null)
                } else {
                    let id = callback_id(&args)?;
                    scope
                        .run(move |ctx| async move {
                            instance
                                .callback(&ctx, id, Value::Null)
                                .await
                                .map_err(Into::into)
                        })
                        .await
                        .map_err(Into::into)
                }
            }
            "spawn" => {
                let grant = self.reserve()?;
                let id = callback_id(&args)?;
                let weak = self.instance.clone();
                let task = ctx.spawn(text(&args, "label")?, move |task| async move {
                    let instance = weak.upgrade().ok_or(cordis_runtime::Error::Closed)?;
                    instance
                        .call_with_lifetime(
                            &task,
                            "callback",
                            json!({"id":id,"args":null}).into(),
                            true,
                        )
                        .await?;
                    instance.call(&task, "forget", json!(id)).await?;
                    Ok(())
                })?;
                Ok(json!(grant.commit(Handle::Task(task))))
            }
            "join" | "task-status" => {
                let task = match self.handles.lock().unwrap().get(text(&args, "grant")?) {
                    Some(Handle::Task(t)) => t.clone(),
                    _ => return Err(Error::Closed),
                };
                if command == "task-status" {
                    return Ok(match task.outcome() {
                        None => json!({"state":"running"}),
                        Some(Ok(())) => json!({"state":"completed"}),
                        Some(Err(e)) => json!({"state":"failed","error":e.to_string()}),
                    });
                }
                task.join().await?;
                Ok(Value::Null)
            }
            "effect" => {
                let grant = self.reserve()?;
                let id = callback_id(&args)?;
                let weak = self.instance.clone();
                let release = weak.clone();
                let sequence = ctx.effect_steps_with_cleanup(
                    text(&args, "label")?,
                    move |step| {
                        let weak = weak.clone();
                        async move {
                            let result = match weak.upgrade() {
                                Some(instance) => instance.callback(&step, id, Value::Null).await,
                                None => Err(Error::Closed),
                            };
                            let value = match result.and_then(validate_step) {
                                Ok(value) => value,
                                Err(error @ (Error::Rejected { .. } | Error::Refused { .. })) => {
                                    return Ok(EffectStep::failed(error.into(), None));
                                }
                                Err(error) => {
                                    let copy = error.clone();
                                    return Ok(EffectStep::failed(
                                        error.into(),
                                        Some(Inverse::new(
                                            "unknown Wasm step",
                                            move |_| async move { Err(copy.into()) },
                                        )),
                                    ));
                                }
                            };
                            let inverse = value["inverse"].as_u64().map(|callback| {
                                Inverse::new("Wasm step inverse", move |cleanup| async move {
                                    let instance =
                                        weak.upgrade().ok_or(cordis_runtime::Error::Closed)?;
                                    instance.callback(&cleanup, callback, Value::Null).await?;
                                    instance.call(&cleanup, "forget", json!(callback)).await?;
                                    Ok(())
                                })
                            });
                            Ok(if let Some(message) = value["failure"].as_str() {
                                EffectStep::failed(
                                    cordis_runtime::Error::Message(message.into()),
                                    inverse,
                                )
                            } else if value["complete"] == true {
                                EffectStep::complete(inverse)
                            } else {
                                EffectStep::yielded(inverse)
                            })
                        }
                    },
                    Inverse::new("Wasm effect producer", move |cleanup| async move {
                        let instance = release.upgrade().ok_or(cordis_runtime::Error::Closed)?;
                        instance.call(&cleanup, "forget", json!(id)).await?;
                        Ok(())
                    }),
                )?;
                Ok(json!(grant.commit(Handle::Sequence(sequence.downgrade()))))
            }
            "effect-ready" | "effect-dispose" | "effect-status" => {
                let sequence = match self.handles.lock().unwrap().get(text(&args, "grant")?) {
                    Some(Handle::Sequence(s)) => s.upgrade().ok_or(Error::Closed)?,
                    _ => return Err(Error::Closed),
                };
                if command == "effect-status" {
                    let s = sequence.snapshot();
                    return Ok(json!({"label":s.label,"state":format!("{:?}",s.status)}));
                }
                if command == "effect-ready" {
                    Ok(json!(format!("{:?}", sequence.ready().await?)))
                } else {
                    sequence.dispose().await?;
                    Ok(Value::Null)
                }
            }
            "plugin-ready" | "plugin-dispose" | "plugin-settled" | "plugin-status" => {
                let plugin = match self.handles.lock().unwrap().get(text(&args, "grant")?) {
                    Some(Handle::Plugin(p)) => p.upgrade().ok_or(Error::Closed)?,
                    _ => return Err(Error::Closed),
                };
                if matches!(command.as_str(), "plugin-settled" | "plugin-status") {
                    let s = if command == "plugin-settled" {
                        plugin.settled().await?
                    } else {
                        plugin.snapshot()
                    };
                    return Ok(
                        json!({"state":format!("{:?}",s.state).to_lowercase(),"retired":s.retired,"outcome":s.outcome.map(|e|format!("{e:?}"))}),
                    );
                }
                if command == "plugin-ready" {
                    plugin.ready().await?;
                } else {
                    plugin.dispose().await?;
                }
                Ok(Value::Null)
            }
            "cancelled-for" => {
                let ms = args
                    .as_u64()
                    .filter(|ms| *ms <= 60_000)
                    .ok_or_else(|| Error::InvalidInput("wait must be 0..60000 ms".into()))?;
                Ok(json!(
                    tokio::time::timeout(Duration::from_millis(ms), ctx.cancelled())
                        .await
                        .is_ok()
                ))
            }
            "cancelled" => {
                ctx.cancelled().await;
                Ok(Value::Null)
            }
            "sleep" => {
                let ms = args
                    .as_u64()
                    .filter(|ms| *ms <= 60_000)
                    .ok_or_else(|| Error::InvalidInput("sleep must be 0..60000 ms".into()))?;
                tokio::select! {_=tokio::time::sleep(Duration::from_millis(ms))=>{},_=ctx.cancelled()=>{}}
                Ok(Value::Null)
            }
            "on" => {
                let name = text(&args, "event")?;
                let event = instance
                    .module
                    .capabilities
                    .events
                    .get(name)
                    .ok_or_else(|| refused("undeclared_event", name))?;
                let grant = self.reserve()?;
                let id = callback_id(&args)?;
                let weak = self.instance.clone();
                let effect = self.subscription_cleanup(&ctx, id, || {
                    ctx.on(event, move |args, ctx| {
                        let weak = weak.clone();
                        async move {
                            let instance = weak.upgrade().ok_or(cordis_runtime::Error::Closed)?;
                            let value = instance
                                .callback_payload(&ctx, id, Payload::message(args))
                                .await?;
                            parse_flow(value).map_err(Into::into)
                        }
                    })
                })?;
                Ok(json!(grant.commit(Handle::Effect(effect))))
            }
            "middleware" => {
                let name = text(&args, "name")?;
                let chain = instance
                    .module
                    .capabilities
                    .middleware
                    .get(name)
                    .ok_or_else(|| refused("undeclared_middleware", name))?;
                let grant = self.reserve()?;
                let id = callback_id(&args)?;
                let weak = self.instance.clone();
                let effect = self.subscription_cleanup(&ctx, id, || {
                    ctx.middleware(chain, move |args, ctx, next| {
                        let weak = weak.clone();
                        async move {
                            let instance = weak.upgrade().ok_or(cordis_runtime::Error::Closed)?;
                            let key = token()?;
                            instance
                                .router
                                .next
                                .lock()
                                .unwrap()
                                .insert(key.clone(), next);
                            let result = instance
                                .callback_payload(
                                    &ctx,
                                    id,
                                    Payload::with_message(json!({"next":key}), args),
                                )
                                .await
                                .and_then(|value| value.one().map_err(unknown));
                            instance.router.next.lock().unwrap().remove(&key);
                            result.map_err(Into::into)
                        }
                    })
                })?;
                Ok(json!(grant.commit(Handle::Effect(effect))))
            }
            _ => Err(refused("unknown_context_method", command)),
        }
    }
}
async fn wait_result<T: Clone>(
    result: &mut tokio::sync::watch::Receiver<Option<Result<T>>>,
) -> Result<T> {
    loop {
        if let Some(value) = result.borrow().clone() {
            return value;
        }
        result.changed().await.map_err(unknown)?;
    }
}
impl Scope {
    pub async fn finish(self) -> Result<()> {
        let requests: Vec<_> = {
            let mut entries = self.router.entries.lock().unwrap();
            let entry = entries.get_mut(&self.token).ok_or(Error::Closed)?;
            entry.open = false;
            entry
                .requests
                .values()
                .map(|request| request.result.clone())
                .collect()
        };
        let mut failure = None;
        for mut result in requests {
            if let Err(error) = wait_result(&mut result).await {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        self.router.entries.lock().unwrap().remove(&self.token);
    }
}

fn invalid_payload(error: impl ToString) -> Error {
    Error::InvalidInput(error.to_string())
}
fn flows_payload(flows: Vec<std::ops::ControlFlow<Bytes>>) -> Payload {
    let mut tags = Vec::with_capacity(flows.len());
    let mut messages = Vec::new();
    for flow in flows {
        match flow {
            cordis_runtime::Continue(()) => tags.push(json!("continue")),
            cordis_runtime::Break(value) => {
                tags.push(json!("break"));
                messages.push(value);
            }
        }
    }
    Payload {
        control: Value::Array(tags),
        messages,
    }
}
fn parse_flow(value: Payload) -> Result<std::ops::ControlFlow<Bytes>> {
    if value.control.as_object().is_none_or(|o| o.len() != 1) {
        return Err(unknown("invalid event flow"));
    }
    match value.control["kind"].as_str() {
        Some("continue") if value.messages.is_empty() => Ok(cordis_runtime::Continue(())),
        Some("break") => Ok(cordis_runtime::Break(value.one().map_err(unknown)?)),
        _ => Err(unknown("invalid event flow")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn malformed_effect_steps_are_unknown_instead_of_silently_losing_inverses() {
        for value in [
            Value::Null,
            crate::json!({}),
            json!({"inverse":0,"complete":true,"failure":null}),
            json!({"inverse":"1","complete":true,"failure":null}),
            json!({"inverse":1,"complete":null,"failure":null}),
            json!({"inverse":1,"complete":false,"failure":false}),
        ] {
            assert!(matches!(validate_step(value), Err(Error::Unknown(_))));
        }
        assert!(validate_step(json!({"inverse":1,"complete":false,"failure":"failed"})).is_ok());
        assert!(validate_step(json!({"inverse":null,"complete":true,"failure":null})).is_ok());
    }
}
