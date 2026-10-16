use crate::{Bytes, Error, Payload, Result, Value, json, session::WeakSession};
use cordis_runtime::{Context, Event, WeakEffectHandle};
use std::{
    collections::BTreeMap,
    ops::ControlFlow,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::watch;

/// An installation-owned mapping. Peers select aliases, never arbitrary realms.
#[derive(Clone)]
pub struct EventBinding {
    alias: String,
    event: Event<Bytes, Bytes>,
}
impl std::fmt::Debug for EventBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("EventBinding")
            .field(&self.alias)
            .field(self.event.contract())
            .field(&self.event.realm())
            .finish()
    }
}
impl EventBinding {
    pub fn new(alias: impl Into<String>, event: &Event<Bytes, Bytes>) -> Self {
        Self {
            alias: alias.into(),
            event: event.clone(),
        }
    }
}
pub(crate) struct Events {
    bindings: BTreeMap<String, Event<Bytes, Bytes>>,
    listeners: Mutex<BTreeMap<String, Arc<Listener>>>,
    dispatches: Mutex<BTreeMap<String, Dispatch>>,
    next_call: Arc<AtomicU64>,
}
struct Listener {
    grant: String,
    handle: OnceLock<WeakEffectHandle>,
    native: OnceLock<WeakEffectHandle>,
    failure: Mutex<Option<Error>>,
    released: Mutex<Option<Result<Value>>>,
}
struct Dispatch {
    grant: String,
    result: watch::Receiver<Option<Payload>>,
}
impl Events {
    pub fn new(bindings: &[EventBinding]) -> Result<Self> {
        let mut entries = BTreeMap::new();
        for b in bindings {
            if b.alias.is_empty()
                || b.alias.len() > 1024
                || entries.insert(b.alias.clone(), b.event.clone()).is_some()
            {
                return Err(Error::InvalidInput(
                    "invalid or duplicate event alias".into(),
                ));
            }
        }
        Ok(Self {
            bindings: entries,
            listeners: Mutex::default(),
            dispatches: Mutex::default(),
            next_call: Arc::new(AtomicU64::new(0)),
        })
    }
    pub fn declarations(&self) -> Value {
        Value::Object(self.bindings.iter().map(|(alias, event)| (alias.clone(), json!({"name":event.contract().name.as_ref(),"revision":event.contract().revision.as_ref(),"realm":event.realm().0.to_string()}))).collect())
    }
    fn event(&self, payload: &Value) -> Result<Event<Bytes, Bytes>> {
        self.bindings
            .get(&crate::scope::text(payload, "event")?)
            .cloned()
            .ok_or_else(|| Error::InvalidInput("event alias is not bound by installation".into()))
    }
    pub fn register(&self, ctx: &Context, payload: &Value, session: WeakSession) -> Result<Value> {
        let event = self.event(payload)?;
        let id = crate::scope::identifier(payload, "listener", 'l')?;
        let mut entries = self.listeners.lock().unwrap();
        if entries.contains_key(&id) {
            return Err(Error::InvalidInput("duplicate listener".into()));
        }
        if entries.len() >= 4096 {
            return Err(Error::Overloaded);
        }
        let entry = Arc::new(Listener {
            grant: grant()?,
            handle: OnceLock::new(),
            native: OnceLock::new(),
            failure: Mutex::default(),
            released: Mutex::default(),
        });
        let release = entry.clone();
        let release_id = id.clone();
        let release_session = session.clone();
        // Register release below native unregistration: automatic LIFO recovery
        // removes and drains the native listener before releasing SDK captures.
        let handle = ctx
            .defer("remote event subscription", move |cleanup| async move {
                let result = async {
                    if let Some(native) = release.native.get().and_then(WeakEffectHandle::upgrade) {
                        native
                            .dispose()
                            .await
                            .map_err(crate::resource::runtime_error)?;
                    }
                    if let Some(error) = release.failure.lock().unwrap().clone() {
                        return Err(error);
                    }
                    release_session
                        .upgrade()
                        .ok_or(Error::Closed)?
                        .scoped_request(&cleanup, "event_release", json!({"listener":release_id}))
                        .await
                }
                .await;
                *release.released.lock().unwrap() = Some(result.clone());
                result.map(|_| ()).map_err(Into::into)
            })
            .map_err(crate::resource::runtime_error)?;
        let _ = entry.handle.set(handle.downgrade());
        entries.insert(id.clone(), entry.clone());
        let callback_entry = entry.clone();
        let callback_id = id.clone();
        let next = self.next_call.clone();
        let native = ctx
            .on(&event, move |args, ctx| {
                let (entry, id, session, next) = (
                    callback_entry.clone(),
                    callback_id.clone(),
                    session.clone(),
                    next.clone(),
                );
                async move {
                    let call = next.fetch_add(1, Ordering::Relaxed);
                    if call >= 4096 {
                        return Err(Error::Overloaded.into());
                    }
                    let reply = match session.upgrade() {
                        Some(session) => {
                            session
                                .scoped_payload(
                                    &ctx,
                                    "event_call",
                                    Payload::with_message(
                                        json!({"listener":id,"call":format!("v{call}")}),
                                        args,
                                    ),
                                )
                                .await
                        }
                        None => Err(Error::HostLost("event host no longer exists".into())),
                    };
                    let result = match reply {
                        Ok(value) => match parse_flow(value) {
                            Ok(flow) => return Ok(flow),
                            Err(e) => e,
                        },
                        Err(error @ (Error::Remote { .. } | Error::Refused { .. })) => {
                            return Err(error.into());
                        }
                        Err(error) if crate::scope::not_executed(&error) => {
                            return Err(error.into());
                        }
                        Err(error) => error,
                    };
                    let failure = Error::OutcomeUnknown(format!("event listener {id}: {result}"));
                    *entry.failure.lock().unwrap() = Some(failure.clone());
                    let cleanup = failure.clone();
                    ctx.defer("unknown remote event callback", move |_| async move {
                        Err(cleanup.into())
                    })?;
                    ctx.fail(failure.to_string())?;
                    Err(failure.into())
                }
            })
            .map_err(crate::resource::runtime_error)?;
        let _ = entry.native.set(native.downgrade());
        Ok(json!({"listener":id,"grant":entry.grant}))
    }
    pub fn emit(&self, ctx: &Context, payload: &Value, args: Bytes) -> Result<Value> {
        let event = self.event(payload)?;
        let id = crate::scope::identifier(payload, "dispatch", 'd')?;
        let serial = match payload["mode"].as_str() {
            Some("serial") => true,
            Some("parallel") => false,
            _ => return Err(Error::InvalidInput("invalid event mode".into())),
        };
        let mut entries = self.dispatches.lock().unwrap();
        if entries.contains_key(&id) {
            return Err(Error::InvalidInput("duplicate dispatch".into()));
        }
        if entries.len() >= 4096
            || entries
                .values()
                .filter(|d| d.result.borrow().is_none())
                .count()
                >= 16
        {
            return Err(Error::Overloaded);
        }
        let grant = grant()?;
        let dispatch = if serial {
            event.emit_serial(ctx, args)
        } else {
            event.emit(ctx, args)
        }
        .map_err(crate::resource::runtime_error)?;
        let (tx, result) = watch::channel(None);
        entries.insert(
            id.clone(),
            Dispatch {
                grant: grant.clone(),
                result,
            },
        );
        tokio::spawn(async move {
            let outcome = match dispatch.join().await {
                Ok(flows) => {
                    let flows = if serial {
                        vec![
                            flows
                                .into_iter()
                                .find(|flow| matches!(flow, ControlFlow::Break(_)))
                                .unwrap_or(ControlFlow::Continue(())),
                        ]
                    } else {
                        flows
                    };
                    let mut result = Payload::from(json!({"kind":"success","flows":[]}));
                    let mut tags = Vec::with_capacity(flows.len());
                    for flow in flows {
                        match flow {
                            ControlFlow::Continue(()) => tags.push(json!("continue")),
                            ControlFlow::Break(bytes) => {
                                tags.push(json!("break"));
                                result.messages.push(bytes);
                            }
                        }
                    }
                    result.control["flows"] = Value::Array(tags);
                    result
                }
                Err(error) if serial => {
                    let failure = crate::dependency_resources::failure(
                        crate::resource::runtime_error(error),
                        true,
                    );
                    json!({"kind":failure["status"], "error":failure["error"]}).into()
                }
                Err(error) => {
                    let errors = match error {
                        cordis_runtime::Error::Aggregate(errors) => errors,
                        error => vec![error],
                    };
                    let errors: Vec<_> = errors
                        .into_iter()
                        .map(|e| json!({"code":"listener_failed","message":e.to_string()}))
                        .collect();
                    json!({"kind":"failure","errors":errors}).into()
                }
            };
            tx.send_replace(Some(outcome));
        });
        Ok(json!({"dispatch":id,"grant":grant}))
    }
    pub async fn invoke(&self, command: &str, payload: &Value) -> Result<Payload> {
        let grant = crate::scope::text(payload, "grant")?;
        if command == "event_dispose" {
            let id = crate::scope::text(payload, "listener")?;
            let entry = self
                .listeners
                .lock()
                .unwrap()
                .get(&id)
                .filter(|e| e.grant == grant)
                .cloned()
                .ok_or(Error::Closed)?;
            if let Some(handle) = entry.handle.get().and_then(WeakEffectHandle::upgrade) {
                handle
                    .dispose()
                    .await
                    .map_err(crate::resource::runtime_error)?;
            }
            return entry
                .released
                .lock()
                .unwrap()
                .clone()
                .unwrap_or(Err(Error::Closed))
                .map(Into::into);
        }
        let id = crate::scope::text(payload, "dispatch")?;
        let mut result = self
            .dispatches
            .lock()
            .unwrap()
            .get(&id)
            .filter(|d| d.grant == grant)
            .map(|d| d.result.clone())
            .ok_or(Error::Closed)?;
        match command {
            "event_status" => {
                let value = result.borrow().clone();
                Ok(match value {
                    Some(value) => json!({"outcome":value.control}).into(),
                    None => json!({"outcome":null}).into(),
                })
            }
            "event_join" => loop {
                if let Some(value) = result.borrow().clone() {
                    return Ok(value);
                }
                result.changed().await.map_err(|_| Error::Closed)?;
            },
            _ => Err(Error::InvalidInput("unknown event command".into())),
        }
    }
}
fn grant() -> Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| Error::Io(e.to_string()))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}
fn parse_flow(value: Payload) -> Result<ControlFlow<Bytes>> {
    match value.control["kind"].as_str() {
        Some("continue")
            if value.control.as_object().is_some_and(|v| v.len() == 1)
                && value.messages.is_empty() =>
        {
            Ok(ControlFlow::Continue(()))
        }
        Some("break") if value.control.as_object().is_some_and(|v| v.len() == 1) => {
            Ok(ControlFlow::Break(value.one()?))
        }
        _ => Err(Error::Protocol("invalid event control flow".into())),
    }
}
