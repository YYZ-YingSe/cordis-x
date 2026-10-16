use crate::{Bytes, Error, Payload, Result, Value, json, session::WeakSession};
use cordis_runtime::{Context, Middleware, Next, WeakEffectHandle};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

/// Installation-owned channel binding; peers cannot choose arbitrary realms.
#[derive(Clone)]
pub struct MiddlewareBinding {
    alias: String,
    middleware: Middleware<Bytes, Bytes>,
}
impl std::fmt::Debug for MiddlewareBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("MiddlewareBinding")
            .field(&self.alias)
            .field(self.middleware.contract())
            .field(&self.middleware.realm())
            .finish()
    }
}
impl MiddlewareBinding {
    pub fn new(alias: impl Into<String>, middleware: &Middleware<Bytes, Bytes>) -> Self {
        Self {
            alias: alias.into(),
            middleware: middleware.clone(),
        }
    }
}
struct Handler {
    grant: String,
    handle: OnceLock<WeakEffectHandle>,
    native: OnceLock<WeakEffectHandle>,
    failure: Mutex<Option<Error>>,
    released: Mutex<Option<Result<Value>>>,
}
struct Continuation {
    context: String,
    grant: String,
    next: Next<Bytes, Bytes>,
}
#[derive(Default)]
struct Calls {
    next: AtomicU64,
    active: Mutex<BTreeMap<String, Continuation>>,
}
struct Frame {
    calls: Arc<Calls>,
    id: String,
}
impl Drop for Frame {
    fn drop(&mut self) {
        self.calls.active.lock().unwrap().remove(&self.id);
    }
}
pub(crate) struct Middlewares {
    bindings: BTreeMap<String, Middleware<Bytes, Bytes>>,
    handlers: Mutex<BTreeMap<String, Arc<Handler>>>,
    runs: Mutex<BTreeSet<String>>,
    calls: Arc<Calls>,
}
impl Middlewares {
    pub fn new(bindings: &[MiddlewareBinding]) -> Result<Self> {
        let mut entries = BTreeMap::new();
        for b in bindings {
            if b.alias.is_empty()
                || b.alias.len() > 1024
                || entries
                    .insert(b.alias.clone(), b.middleware.clone())
                    .is_some()
            {
                return Err(Error::InvalidInput(
                    "invalid or duplicate middleware alias".into(),
                ));
            }
        }
        Ok(Self {
            bindings: entries,
            handlers: Mutex::default(),
            runs: Mutex::default(),
            calls: Arc::default(),
        })
    }
    pub fn declarations(&self) -> Value {
        Value::Object(self.bindings.iter().map(|(alias, m)| (alias.clone(), json!({"name":m.contract().name.as_ref(),"revision":m.contract().revision.as_ref(),"realm":m.realm().0.to_string()}))).collect())
    }
    fn binding(&self, payload: &Value) -> Result<Middleware<Bytes, Bytes>> {
        self.bindings
            .get(&crate::scope::text(payload, "middleware")?)
            .cloned()
            .ok_or_else(|| {
                Error::InvalidInput("middleware alias is not bound by installation".into())
            })
    }
    pub fn register(&self, ctx: &Context, payload: &Value, session: WeakSession) -> Result<Value> {
        let middleware = self.binding(payload)?;
        let id = crate::scope::identifier(payload, "handler", 'm')?;
        let mut entries = self.handlers.lock().unwrap();
        if entries.contains_key(&id) {
            return Err(Error::InvalidInput("duplicate middleware handler".into()));
        }
        if entries.len() >= 4096 {
            return Err(Error::Overloaded);
        }
        let entry = Arc::new(Handler {
            grant: grant()?,
            handle: OnceLock::new(),
            native: OnceLock::new(),
            failure: Mutex::default(),
            released: Mutex::default(),
        });
        let release = entry.clone();
        let release_id = id.clone();
        let release_session = session.clone();
        let handle = ctx
            .defer(
                "remote middleware registration",
                move |cleanup| async move {
                    let result = async {
                        if let Some(native) =
                            release.native.get().and_then(WeakEffectHandle::upgrade)
                        {
                            native.dispose().await.map_err(runtime_error)?;
                        }
                        if let Some(error) = release.failure.lock().unwrap().clone() {
                            return Err(error);
                        }
                        release_session
                            .upgrade()
                            .ok_or(Error::Closed)?
                            .scoped_request(
                                &cleanup,
                                "middleware_release",
                                json!({"handler":release_id}),
                            )
                            .await
                    }
                    .await;
                    *release.released.lock().unwrap() = Some(result.clone());
                    result.map(|_| ()).map_err(Into::into)
                },
            )
            .map_err(runtime_error)?;
        let _ = entry.handle.set(handle.downgrade());
        entries.insert(id.clone(), entry.clone());
        let callback_id = id.clone();
        let callback_entry = entry.clone();
        let calls = self.calls.clone();
        let native = ctx
            .middleware(&middleware, move |args, ctx, next| {
                let (id, entry, session, calls) = (
                    callback_id.clone(),
                    callback_entry.clone(),
                    session.clone(),
                    calls.clone(),
                );
                async move {
                    let result = async {
                        let call = call_id(&calls)?;
                        let grant = grant()?;
                        let session = session
                            .upgrade()
                            .ok_or_else(|| Error::HostLost("middleware host lost".into()))?;
                        let (request, _scope) = session.scoped_envelope(
                            &ctx,
                            "middleware_call",
                            Payload::with_message(
                                json!({"handler":id,"call":call,"next":grant}),
                                args,
                            ),
                        )?;
                        let token = crate::scope::text(&request.payload.control, "context")?;
                        calls.active.lock().unwrap().insert(
                            call.clone(),
                            Continuation {
                                context: token,
                                grant,
                                next,
                            },
                        );
                        let _frame = Frame { calls, id: call };
                        session.send(request).await
                    }
                    .await;
                    let result = callback_outcome(&ctx, result).await;
                    if let Err(error @ Error::OutcomeUnknown(_)) = &result {
                        *entry.failure.lock().unwrap() = Some(error.clone());
                    }
                    result.map_err(Into::into)
                }
            })
            .map_err(runtime_error)?;
        let _ = entry.native.set(native.downgrade());
        Ok(json!({"handler":id,"grant":entry.grant}))
    }
    pub async fn dispose(&self, payload: &Value) -> Result<Value> {
        let id = crate::scope::text(payload, "handler")?;
        let grant = crate::scope::text(payload, "grant")?;
        let entry = self
            .handlers
            .lock()
            .unwrap()
            .get(&id)
            .filter(|e| e.grant == grant)
            .cloned()
            .ok_or(Error::Closed)?;
        if let Some(handle) = entry.handle.get().and_then(WeakEffectHandle::upgrade) {
            handle.dispose().await.map_err(runtime_error)?;
        }
        entry
            .released
            .lock()
            .unwrap()
            .clone()
            .unwrap_or(Err(Error::Closed))
    }
    pub fn continuation(&self, payload: &Value) -> Result<Next<Bytes, Bytes>> {
        let id = crate::scope::text(payload, "call")?;
        let grant = crate::scope::text(payload, "grant")?;
        let context = crate::scope::text(payload, "context")?;
        self.calls
            .active
            .lock()
            .unwrap()
            .get(&id)
            .filter(|c| c.context == context && c.grant == grant)
            .map(|c| c.next.clone())
            .ok_or(Error::Closed)
    }
    pub async fn run(
        &self,
        ctx: &Context,
        payload: &Value,
        args: Bytes,
        session: WeakSession,
    ) -> Result<Bytes> {
        let middleware = self.binding(payload)?;
        let run = crate::scope::identifier(payload, "run", 'q')?;
        {
            let mut runs = self.runs.lock().unwrap();
            if runs.contains(&run) {
                return Err(Error::InvalidInput("duplicate middleware run".into()));
            }
            if runs.len() >= 4096 {
                return Err(Error::Overloaded);
            }
            runs.insert(run.clone());
        }
        let calls = self.calls.clone();
        middleware
            .run(ctx, args, move |args, ctx| {
                let (run, session, calls) = (run.clone(), session.clone(), calls.clone());
                async move {
                    let result = async {
                        let call = call_id(&calls)?;
                        session
                            .upgrade()
                            .ok_or_else(|| Error::HostLost("terminal host lost".into()))?
                            .scoped_payload(
                                &ctx,
                                "middleware_terminal",
                                Payload::with_message(json!({"run":run,"call":call}), args),
                            )
                            .await
                    }
                    .await;
                    callback_outcome(&ctx, result).await.map_err(Into::into)
                }
            })
            .await
            .map_err(runtime_error)
    }
}
fn call_id(calls: &Calls) -> Result<String> {
    let n = calls.next.fetch_add(1, Ordering::Relaxed);
    if n >= 4096 {
        return Err(Error::Overloaded);
    }
    Ok(format!("w{n}"))
}
fn grant() -> Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| Error::Io(e.to_string()))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}
// Rejected and Refused are known completions. Effects admitted before either
// result remain owned by the callback Context; neither implies rollback.
async fn callback_outcome(ctx: &Context, reply: Result<Payload>) -> Result<Bytes> {
    let unknown = match reply {
        Ok(value) => match value.one() {
            Ok(bytes) => return Ok(bytes),
            Err(e) => Error::Protocol(e.to_string()),
        },
        Err(error @ (Error::Remote { .. } | Error::Refused { .. })) => return Err(error),
        Err(error) if crate::scope::not_executed(&error) => return Err(error),
        Err(error) => error,
    };
    let failure = Error::OutcomeUnknown(format!("middleware callback: {unknown}"));
    let cleanup = failure.clone();
    ctx.defer("unknown remote middleware callback", move |_| async move {
        Err(cleanup.into())
    })
    .map_err(runtime_error)?;
    ctx.fail(failure.to_string()).map_err(runtime_error)?;
    Err(failure)
}
pub(crate) fn runtime_error(error: cordis_runtime::Error) -> Error {
    match error {
        cordis_runtime::Error::DuplicateNext => Error::Refused {
            code: "duplicate_next".into(),
            message: "next already admitted".into(),
        },
        cordis_runtime::Error::ExpiredContinuation => Error::Refused {
            code: "expired_next".into(),
            message: "middleware frame ended".into(),
        },
        other => crate::resource::runtime_error(other),
    }
}
