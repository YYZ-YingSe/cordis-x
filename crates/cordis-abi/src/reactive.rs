//! Bounded event and middleware adapters over native ownership and continuations.
use crate::{
    Bytes, Error, Instance, Payload, Result, Value, context::Router, context::next_id, ffi::Reply,
    json,
};
use cordis_runtime::{Context, Event, Middleware, Next, WeakEffectHandle};
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::ControlFlow,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{Semaphore, watch};

#[derive(Clone)]
pub struct EventBinding {
    pub(crate) alias: String,
    pub(crate) event: Event<Bytes, Bytes>,
}
impl EventBinding {
    pub fn new(alias: impl Into<String>, event: &Event<Bytes, Bytes>) -> Self {
        Self {
            alias: alias.into(),
            event: event.clone(),
        }
    }
}
#[derive(Clone)]
pub struct MiddlewareBinding {
    pub(crate) alias: String,
    pub(crate) middleware: Middleware<Bytes, Bytes>,
}
impl MiddlewareBinding {
    pub fn new(alias: impl Into<String>, middleware: &Middleware<Bytes, Bytes>) -> Self {
        Self {
            alias: alias.into(),
            middleware: middleware.clone(),
        }
    }
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
impl std::fmt::Debug for MiddlewareBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("MiddlewareBinding")
            .field(&self.alias)
            .field(self.middleware.contract())
            .field(&self.middleware.realm())
            .finish()
    }
}
#[derive(Clone, Debug)]
pub struct ReactiveLimits {
    pub subscriptions: usize,
    pub dispatches: usize,
    pub runs: usize,
    pub calls: usize,
    pub active_dispatches: usize,
    pub active_callbacks: usize,
    pub active_runs: usize,
}
impl Default for ReactiveLimits {
    fn default() -> Self {
        Self {
            subscriptions: 1024,
            dispatches: 4096,
            runs: 4096,
            calls: 16384,
            active_dispatches: 16,
            active_callbacks: 32,
            active_runs: 16,
        }
    }
}
impl crate::Capabilities {
    pub(crate) fn validate_reactive(&self, manifest: Option<&Value>) -> Result<()> {
        for n in [
            self.reactive.active_dispatches,
            self.reactive.active_callbacks,
            self.reactive.active_runs,
        ] {
            if n > Semaphore::MAX_PERMITS {
                return Err(Error::InvalidInput("invalid Reactive capacity".into()));
            }
        }
        let mut expected = json!({"events":{},"middleware":{}});
        for binding in &self.events {
            declaration(
                &mut expected["events"],
                &binding.alias,
                binding.event.contract(),
            )?;
        }
        for binding in &self.middleware {
            declaration(
                &mut expected["middleware"],
                &binding.alias,
                binding.middleware.contract(),
            )?;
        }
        if manifest.unwrap_or(&json!({"events":{},"middleware":{}})) != &expected {
            return Err(Error::Contract(
                "Reactive bindings must exactly match the guest manifest".into(),
            ));
        }
        Ok(())
    }
}
fn declaration(map: &mut Value, alias: &str, contract: &cordis_contract::ContractId) -> Result<()> {
    if alias.is_empty()
        || alias.len() > 1024
        || contract.name.is_empty()
        || contract.revision.is_empty()
        || map
            .as_object_mut()
            .unwrap()
            .insert(
                alias.into(),
                json!({"name":contract.name.as_ref(),"revision":contract.revision.as_ref()}),
            )
            .is_some()
    {
        return Err(Error::Contract(
            "invalid or duplicate Reactive binding".into(),
        ));
    }
    Ok(())
}
#[derive(Default)]
struct Registry {
    callbacks: BTreeSet<u64>,
    subscriptions: BTreeMap<u64, Arc<Subscription>>,
    dispatches: BTreeMap<u64, watch::Receiver<Option<Result<Payload>>>>,
    runs: usize,
    frames: BTreeMap<u64, Frame>,
}
struct Frame {
    grant: u64,
    next: Next<Bytes, Bytes>,
}
struct Subscription {
    callback: u64,
    handle: OnceLock<WeakEffectHandle>,
    native: OnceLock<WeakEffectHandle>,
    failure: Mutex<Option<Error>>,
    released: Mutex<Option<Result<()>>>,
}
pub(crate) struct Reactive {
    entries: Mutex<Registry>,
    calls: AtomicUsize,
    callbacks: Arc<Semaphore>,
    terminals: Arc<Semaphore>,
    runs: Semaphore,
    joins: Semaphore,
    disposals: Semaphore,
}
impl Reactive {
    pub fn new(caps: &crate::Capabilities) -> Self {
        let callbacks = caps.reactive.active_callbacks.min(caps.max_contexts);
        let waits = caps.max_callbacks.min(caps.max_contexts);
        Self {
            entries: Mutex::default(),
            calls: AtomicUsize::new(0),
            callbacks: Arc::new(Semaphore::new(callbacks)),
            terminals: Arc::new(Semaphore::new(
                caps.reactive.active_runs.min(caps.max_contexts),
            )),
            runs: Semaphore::new(caps.reactive.active_runs),
            joins: Semaphore::new(waits),
            disposals: Semaphore::new(waits),
        }
    }
    pub fn recovered(&self) -> bool {
        let entries = self.entries.lock().unwrap();
        entries
            .subscriptions
            .values()
            .all(|e| matches!(*e.released.lock().unwrap(), Some(Ok(()))))
            && entries.frames.is_empty()
    }
    pub fn bind_frame(&self, id: u64, grant: u64) -> Result<()> {
        let mut entries = self.entries.lock().unwrap();
        let frame = entries.frames.get_mut(&id).ok_or(Error::Closed)?;
        if frame.grant != 0 {
            return Err(Error::Closed);
        }
        frame.grant = grant;
        Ok(())
    }
}
struct FrameGuard {
    router: Arc<Router>,
    id: u64,
}
impl Drop for FrameGuard {
    fn drop(&mut self) {
        self.router
            .reactive
            .entries
            .lock()
            .unwrap()
            .frames
            .remove(&self.id);
    }
}
impl Router {
    #[allow(clippy::too_many_arguments)]
    pub async fn react(
        self: &Arc<Self>,
        grant: u64,
        operation: u32,
        target: u64,
        callback: u64,
        alias: String,
        args: Payload,
    ) -> Result<Reply> {
        if !self
            .instance
            .upgrade()
            .is_some_and(|i| i.module.reactive_manifest().is_some())
        {
            return Err(Error::Closed);
        }
        if crate::context::in_cleanup() && operation != 8 {
            return Err(refused(
                "cleanup_wait",
                "Reactive operations during cleanup are unsupported",
            ));
        }
        if alias.len() > 1024 {
            return Err(Error::InvalidInput("Reactive alias too long".into()));
        }
        if !matches!(operation, 4 | 6 | 7) {
            args.clone().control_only().map_err(invalid_payload)?;
        }
        let value = match operation {
            1 | 2 => {
                let ctx = self.context(grant)?;
                let router = self.clone();
                let id = ctx
                    .operation(move |ctx| async move {
                        Ok(router.subscribe(&ctx, operation, callback, alias, args.control))
                    })
                    .await??;
                return Ok(Reply {
                    value: Value::Null.into(),
                    handle: id,
                });
            }
            3 => {
                self.unsubscribe(target).await?;
                Value::Null.into()
            }
            4 => {
                return Ok(Reply {
                    value: Value::Null.into(),
                    handle: self.emit(grant, alias, args)?,
                });
            }
            5 | 8 => {
                let mut rx = self
                    .reactive
                    .entries
                    .lock()
                    .unwrap()
                    .dispatches
                    .get(&target)
                    .cloned()
                    .ok_or(Error::Closed)?;
                if operation == 8 {
                    json!({"outcome":rx.borrow().as_ref().map(outcome)}).into()
                } else {
                    let _permit = self
                        .reactive
                        .joins
                        .try_acquire()
                        .map_err(|_| Error::Overloaded)?;
                    loop {
                        if let Some(result) = rx.borrow().clone() {
                            break result?;
                        }
                        rx.changed().await.map_err(|_| Error::Closed)?;
                    }
                }
            }
            6 => Payload::message(
                self.middleware_run(grant, callback, alias, args.one().map_err(invalid_payload)?)
                    .await?,
            ),
            7 => {
                let _ctx = self.context(grant)?;
                let next = self
                    .reactive
                    .entries
                    .lock()
                    .unwrap()
                    .frames
                    .get(&target)
                    .filter(|f| f.grant == grant)
                    .map(|f| f.next.clone())
                    .ok_or_else(|| {
                        refused(
                            "expired_next",
                            "continuation does not belong to this active frame",
                        )
                    })?;
                Payload::message(
                    next.run(args.one().map_err(invalid_payload)?)
                        .await
                        .map_err(runtime_error)?,
                )
            }
            _ => return Err(Error::InvalidInput("unknown Reactive operation".into())),
        };
        Ok(Reply { value, handle: 0 })
    }
    fn subscribe(
        self: &Arc<Self>,
        ctx: &Context,
        kind: u32,
        callback: u64,
        alias: String,
        args: Value,
    ) -> Result<u64> {
        let event = self
            .capabilities
            .events
            .iter()
            .find(|b| b.alias == alias)
            .map(|b| b.event.clone());
        let middleware = self
            .capabilities
            .middleware
            .iter()
            .find(|b| b.alias == alias)
            .map(|b| b.middleware.clone());
        if (kind == 1 && event.is_none()) || (kind == 2 && middleware.is_none()) {
            return Err(refused("unbound", "alias is not bound by host"));
        }
        let initializing = if kind == 1 {
            args.as_bool()
                .ok_or_else(|| Error::InvalidInput("on requires initializing boolean".into()))?
        } else {
            false
        };
        let instance = self.instance.upgrade().ok_or(Error::Closed)?;
        let mut entries = self.reactive.entries.lock().unwrap();
        check_callback(&entries, callback)?;
        if entries.subscriptions.len() >= self.capabilities.reactive.subscriptions {
            return Err(Error::Overloaded);
        }
        let id = next_id()?;
        let entry = Arc::new(Subscription {
            callback,
            handle: OnceLock::new(),
            native: OnceLock::new(),
            failure: Mutex::default(),
            released: Mutex::default(),
        });
        let release = entry.clone();
        let release_instance = instance.clone();
        let handle = ctx.defer("ABI Reactive callback", move |cleanup| async move {
            let result = async {
                if let Some(native) = release.native.get().and_then(WeakEffectHandle::upgrade) {
                    native.dispose().await?;
                }
                if let Some(error) = release.failure.lock().unwrap().clone() {
                    return Err(error);
                }
                release_instance
                    .managed_call(cleanup, 16, callback, Value::Null)
                    .await
                    .map(|_| ())
            }
            .await;
            *release.released.lock().unwrap() = Some(result.clone());
            result.map_err(Into::into)
        })?;
        let _ = entry.handle.set(handle.downgrade());
        entries.callbacks.insert(callback);
        entries.subscriptions.insert(id, entry.clone());
        let router = self.clone();
        let invoked = entry.clone();
        let native = if let Some(event) = event.filter(|_| kind == 1) {
            let handler = move |args, ctx| {
                let (router, entry, instance) = (router.clone(), invoked.clone(), instance.clone());
                async move {
                    let result = router
                        .callback(&instance, &ctx, 13, callback, Payload::message(args))
                        .await
                        .and_then(parse_flow);
                    router.record_unknown(&instance, &ctx, Some(&entry), result.as_ref().err())?;
                    result.map_err(Into::into)
                }
            };
            if initializing {
                ctx.on_initializing(&event, handler)
            } else {
                ctx.on(&event, handler)
            }
        } else {
            ctx.middleware(&middleware.unwrap(), move |args, ctx, next| {
                let (router, entry, instance) = (router.clone(), invoked.clone(), instance.clone());
                async move {
                    let id = next_id()?;
                    router
                        .reactive
                        .entries
                        .lock()
                        .unwrap()
                        .frames
                        .insert(id, Frame { grant: 0, next });
                    let frame = FrameGuard {
                        router: router.clone(),
                        id,
                    };
                    let result = router
                        .callback(
                            &instance,
                            &ctx,
                            14,
                            callback,
                            Payload::with_message(json!({"next":id}), args),
                        )
                        .await
                        .and_then(|p| p.one().map_err(|e| Error::Unknown(e.to_string())));
                    drop(frame);
                    router.record_unknown(&instance, &ctx, Some(&entry), result.as_ref().err())?;
                    result.map_err(Into::into)
                }
            })
        };
        match native {
            Ok(native) => {
                let _ = entry.native.set(native.downgrade());
                Ok(id)
            }
            // A release was already registered. Keep SDK captures until it runs.
            Err(error) => Err(Error::Unknown(format!(
                "partial Reactive registration: {error}"
            ))),
        }
    }
    async fn unsubscribe(&self, id: u64) -> Result<()> {
        let entry = self
            .reactive
            .entries
            .lock()
            .unwrap()
            .subscriptions
            .get(&id)
            .cloned()
            .ok_or(Error::Closed)?;
        if matches!(crate::managed::active(), Some((13 | 14, callback)) if callback == entry.callback)
        {
            return Err(refused(
                "self_wait",
                "callback cannot unsubscribe itself synchronously",
            ));
        }
        let _permit = self
            .reactive
            .disposals
            .try_acquire()
            .map_err(|_| Error::Overloaded)?;
        if let Some(handle) = entry.handle.get().and_then(WeakEffectHandle::upgrade) {
            handle.dispose().await?;
        }
        entry
            .released
            .lock()
            .unwrap()
            .clone()
            .unwrap_or(Err(Error::Closed))
    }
    fn emit(&self, grant: u64, alias: String, args: Payload) -> Result<u64> {
        let ctx = self.context(grant)?;
        let event = &self
            .capabilities
            .events
            .iter()
            .find(|b| b.alias == alias)
            .ok_or_else(|| refused("unbound", "event alias is not bound"))?
            .event;
        let serial = args
            .control
            .get("serial")
            .and_then(Value::as_bool)
            .ok_or_else(|| Error::InvalidInput("missing dispatch mode".into()))?;
        let input = args.one().map_err(invalid_payload)?;
        let mut entries = self.reactive.entries.lock().unwrap();
        if entries.dispatches.len() >= self.capabilities.reactive.dispatches
            || entries
                .dispatches
                .values()
                .filter(|d| d.borrow().is_none())
                .count()
                >= self.capabilities.reactive.active_dispatches
        {
            return Err(Error::Overloaded);
        }
        let id = next_id()?;
        let instance = self.instance.upgrade().ok_or(Error::Closed)?;
        let dispatch = if serial {
            event.emit_serial(&ctx, input)
        } else {
            event.emit(&ctx, input)
        }?;
        let (tx, rx) = watch::channel(None);
        entries.dispatches.insert(id, rx);
        tokio::spawn(async move {
            let _instance = instance;
            let result = dispatch.join().await.map(flows_payload).map_err(Into::into);
            tx.send_replace(Some(result));
        });
        Ok(id)
    }
    async fn middleware_run(
        self: &Arc<Self>,
        grant: u64,
        callback: u64,
        alias: String,
        args: Bytes,
    ) -> Result<Bytes> {
        let ctx = self.context(grant)?;
        let middleware = self
            .capabilities
            .middleware
            .iter()
            .find(|b| b.alias == alias)
            .ok_or_else(|| refused("unbound", "middleware alias is not bound"))?
            .middleware
            .clone();
        let _permit = self
            .reactive
            .runs
            .try_acquire()
            .map_err(|_| Error::Overloaded)?;
        {
            let mut entries = self.reactive.entries.lock().unwrap();
            check_callback(&entries, callback)?;
            if entries.runs >= self.capabilities.reactive.runs {
                return Err(Error::Overloaded);
            }
            entries.runs += 1;
            entries.callbacks.insert(callback);
        }
        let router = self.clone();
        let instance = self.instance.upgrade().ok_or(Error::Closed)?;
        let unknown = Arc::new(Mutex::new(None));
        let failure = unknown.clone();
        let result = middleware
            .run(&ctx, args, move |args, ctx| {
                let (router, instance, failure) =
                    (router.clone(), instance.clone(), failure.clone());
                async move {
                    let result = router
                        .callback(&instance, &ctx, 15, callback, Payload::message(args))
                        .await
                        .and_then(|p| p.one().map_err(|e| Error::Unknown(e.to_string())));
                    if let Err(error @ Error::Unknown(_)) = &result {
                        *failure.lock().unwrap() = Some(error.clone());
                    }
                    router.record_unknown(&instance, &ctx, None, result.as_ref().err())?;
                    result.map_err(Into::into)
                }
            })
            .await
            .map_err(runtime_error);
        // C++ owns this one-shot callback until this synchronous host call ends.
        // An unknown terminal retains its captures and native failed inverse.
        let failure = unknown.lock().unwrap().clone();
        failure.map_or(result, Err)
    }
    async fn callback(
        &self,
        instance: &Arc<Instance>,
        ctx: &Context,
        operation: u32,
        callback: u64,
        args: Payload,
    ) -> Result<Payload> {
        let capacity = if operation == 15 {
            &self.reactive.terminals
        } else {
            &self.reactive.callbacks
        };
        let _permit = capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        self.reactive
            .calls
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < self.capabilities.reactive.calls).then(|| n + 1)
            })
            .map_err(|_| Error::Overloaded)?;
        instance
            .managed_payload(ctx.clone(), operation, callback, args)
            .await
    }
    fn record_unknown(
        &self,
        instance: &Arc<Instance>,
        ctx: &Context,
        entry: Option<&Subscription>,
        error: Option<&Error>,
    ) -> Result<()> {
        if let Some(error @ Error::Unknown(_)) = error {
            if let Some(entry) = entry {
                *entry.failure.lock().unwrap() = Some(error.clone());
            }
            let failure = error.clone();
            let pin = instance.clone();
            ctx.defer("unknown ABI Reactive callback", move |_| async move {
                let _pin = pin;
                Err(failure.into())
            })?;
            instance.fault(error.clone());
            ctx.fail(error.to_string())?;
        }
        Ok(())
    }
}
fn check_callback(entries: &Registry, callback: u64) -> Result<()> {
    if callback == 0 || entries.callbacks.contains(&callback) {
        Err(Error::InvalidInput(
            "invalid or reused Reactive callback".into(),
        ))
    } else {
        Ok(())
    }
}
fn refused(code: &str, message: &str) -> Error {
    Error::Refused {
        code: code.into(),
        message: message.into(),
    }
}
fn runtime_error(error: cordis_runtime::Error) -> Error {
    match error {
        cordis_runtime::Error::DuplicateNext => refused("duplicate_next", "next already admitted"),
        cordis_runtime::Error::ExpiredContinuation => {
            refused("expired_next", "middleware frame ended")
        }
        other => other.into(),
    }
}
fn invalid_payload(e: impl ToString) -> Error {
    Error::InvalidInput(e.to_string())
}
fn flows_payload(flows: Vec<ControlFlow<Bytes>>) -> Payload {
    let mut messages = vec![];
    let control = Value::Array(
        flows
            .into_iter()
            .map(|flow| match flow {
                ControlFlow::Continue(()) => json!("continue"),
                ControlFlow::Break(value) => {
                    messages.push(value);
                    json!("break")
                }
            })
            .collect(),
    );
    Payload { control, messages }
}
fn parse_flow(value: Payload) -> Result<ControlFlow<Bytes>> {
    if value.control.as_object().map(|o| o.len()) != Some(1) {
        return Err(Error::Unknown("invalid ABI event control flow".into()));
    }
    match value.control["kind"].as_str() {
        Some("continue") if value.messages.is_empty() => Ok(ControlFlow::Continue(())),
        Some("break") => value
            .one()
            .map(ControlFlow::Break)
            .map_err(|e| Error::Unknown(e.to_string())),
        _ => Err(Error::Unknown("invalid ABI event control flow".into())),
    }
}
fn outcome(result: &Result<Payload>) -> Value {
    match result {
        Ok(_) => json!({"kind":"success"}),
        Err(error) => json!({"kind":"failure","error":error.to_string()}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ambiguous_event_flow_is_unknown_not_a_known_business_rejection() {
        for value in [
            Value::Null,
            json!(false),
            json!({"kind":"continue","value":1}),
            json!({"kind":"break"}),
            json!({"kind":"break","value":1,"extra":true}),
            json!({"kind":"unknown"}),
        ] {
            assert!(matches!(parse_flow(value.into()), Err(Error::Unknown(_))));
        }
    }
}
