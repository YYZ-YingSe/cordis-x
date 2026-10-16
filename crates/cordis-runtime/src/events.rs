use crate::{
    BoxFuture, Context, EffectHandle, Error, Result,
    gate::{Gate, Lease},
    protected,
};
use cordis_contract::{ContractId, RealmId, Slot};
use std::{
    any::{Any, TypeId},
    collections::BTreeMap,
    future::Future,
    marker::PhantomData,
    ops::ControlFlow,
    sync::{Arc, Mutex},
};
use tokio::sync::oneshot;

pub use std::ops::ControlFlow::{Break, Continue};

#[derive(Default)]
pub(crate) struct Registry {
    next: u64,
    entries: BTreeMap<(Slot, bool), Bucket>,
}
struct Bucket {
    signature: TypeId,
    handlers: BTreeMap<u64, Arc<dyn Any + Send + Sync>>,
}

impl Registry {
    fn insert<H: Any + Send + Sync>(
        &mut self,
        slot: Slot,
        middleware: bool,
        handler: H,
    ) -> Result<u64> {
        let bucket = self
            .entries
            .entry((slot.clone(), middleware))
            .or_insert_with(|| Bucket {
                signature: TypeId::of::<H>(),
                handlers: BTreeMap::new(),
            });
        if bucket.signature != TypeId::of::<H>() {
            return Err(Error::TypeMismatch(slot.contract));
        }
        self.next += 1;
        bucket.handlers.insert(self.next, Arc::new(handler));
        Ok(self.next)
    }

    fn snapshot<H: Any + Send + Sync>(&self, slot: &Slot, middleware: bool) -> Result<Vec<Arc<H>>> {
        let Some(bucket) = self.entries.get(&(slot.clone(), middleware)) else {
            return Ok(Vec::new());
        };
        bucket
            .handlers
            .values()
            .map(|handler| {
                handler
                    .clone()
                    .downcast::<H>()
                    .map_err(|_| Error::TypeMismatch(slot.contract.clone()))
            })
            .collect()
    }

    fn remove(
        &mut self,
        slot: &Slot,
        middleware: bool,
        id: u64,
    ) -> Option<Arc<dyn Any + Send + Sync>> {
        let key = (slot.clone(), middleware);
        let bucket = self.entries.get_mut(&key)?;
        let removed = bucket.handlers.remove(&id);
        if bucket.handlers.is_empty() {
            self.entries.remove(&key);
        }
        removed
    }
}

type ListenerFn<T, R> =
    dyn Fn(T, Context) -> BoxFuture<'static, Result<ControlFlow<R>>> + Send + Sync;
struct Listener<T, R> {
    gate: Arc<Gate>,
    context: Context,
    initializing: bool,
    callback: Arc<ListenerFn<T, R>>,
}

/// Typed, ordered event. Portable short-circuiting uses explicit Continue/Break tags.
pub struct Event<T, R = ()> {
    slot: Slot,
    marker: PhantomData<fn(T) -> R>,
}
impl<T, R> Clone for Event<T, R> {
    fn clone(&self) -> Self {
        Self {
            slot: self.slot.clone(),
            marker: PhantomData,
        }
    }
}
impl<T, R> Event<T, R> {
    pub fn new(name: impl Into<Arc<str>>, revision: impl Into<Arc<str>>) -> Self {
        Self {
            slot: Slot::new(ContractId::new(name, revision), RealmId::ROOT),
            marker: PhantomData,
        }
    }
    pub fn contract(&self) -> &ContractId {
        &self.slot.contract
    }
    pub fn realm(&self) -> RealmId {
        self.slot.realm
    }
    pub fn in_realm(mut self, realm: RealmId) -> Self {
        self.slot.realm = realm;
        self
    }
}

/// An admitted dispatch continues when the receiver is dropped. Errors also enter runtime diagnostics.
#[must_use = "join the dispatch to observe listener results; errors remain in Runtime::diagnostics"]
pub struct Dispatch<T> {
    result: oneshot::Receiver<Result<T>>,
}
impl<T> Dispatch<T> {
    pub async fn join(self) -> Result<T> {
        self.result.await.map_err(|_| Error::RuntimeStopped)?
    }
}

impl Context {
    pub fn on<T, R, F, Fut>(&self, event: &Event<T, R>, handler: F) -> Result<EffectHandle>
    where
        T: Clone + Send + 'static,
        R: Send + 'static,
        F: Fn(T, Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ControlFlow<R>>> + Send + 'static,
    {
        self.listen(event, handler, false)
    }

    /// Initialization handshakes may receive events before service publication.
    pub fn on_initializing<T, R, F, Fut>(
        &self,
        event: &Event<T, R>,
        handler: F,
    ) -> Result<EffectHandle>
    where
        T: Clone + Send + 'static,
        R: Send + 'static,
        F: Fn(T, Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ControlFlow<R>>> + Send + 'static,
    {
        self.listen(event, handler, true)
    }

    fn listen<T, R, F, Fut>(
        &self,
        event: &Event<T, R>,
        handler: F,
        initializing: bool,
    ) -> Result<EffectHandle>
    where
        T: Clone + Send + 'static,
        R: Send + 'static,
        F: Fn(T, Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ControlFlow<R>>> + Send + 'static,
    {
        let gate = Gate::new();
        gate.activate();
        self.register_handler(
            event.slot.clone(),
            false,
            Some(gate.clone()),
            move |context| Listener {
                gate,
                context,
                initializing,
                callback: Arc::new(move |arg, ctx| Box::pin(handler(arg, ctx))),
            },
        )
    }

    fn register_handler<H: Any + Send + Sync>(
        &self,
        slot: Slot,
        middleware: bool,
        gate: Option<Arc<Gate>>,
        handler: impl FnOnce(Context) -> H,
    ) -> Result<EffectHandle> {
        let _lease = self.enter_owner()?;
        if self.cleanup {
            return Err(Error::Closed);
        }
        let inner = self.inner.upgrade().ok_or(Error::RuntimeStopped)?;
        let mut registry = inner.events.lock().unwrap();
        registry.snapshot::<H>(&slot, middleware)?;
        let id = registry.next + 1;
        let weak = Arc::downgrade(&inner);
        let cleanup_slot = slot.clone();
        let registration = self.defer(
            format!("listener {} #{id}", slot.contract),
            move |_| async move {
                if let Some(gate) = &gate {
                    gate.close();
                }
                if let Some(inner) = weak.upgrade() {
                    let removed =
                        inner
                            .events
                            .lock()
                            .unwrap()
                            .remove(&cleanup_slot, middleware, id);
                    drop(removed); // user captures are dropped outside the registry lock
                }
                if let Some(gate) = gate {
                    gate.drained().await;
                    gate.finish();
                }
                Ok(())
            },
        );
        let registration = registration?;
        registry.insert(slot, middleware, handler(registration.context.clone()))?;
        Ok(registration)
    }
}

impl<T: Clone + Send + 'static, R: Send + 'static> Event<T, R> {
    pub fn emit(&self, context: &Context, input: T) -> Result<Dispatch<Vec<ControlFlow<R>>>> {
        self.dispatch(context, input, false)
    }

    /// Admit an ordered dispatch immediately; its waiter may be dropped safely.
    pub fn emit_serial(
        &self,
        context: &Context,
        input: T,
    ) -> Result<Dispatch<Vec<ControlFlow<R>>>> {
        self.dispatch(context, input, true)
    }
    pub async fn parallel(&self, context: &Context, input: T) -> Result<Vec<ControlFlow<R>>> {
        self.emit(context, input)?.join().await
    }

    pub async fn serial(&self, context: &Context, input: T) -> Result<ControlFlow<R>> {
        let results = self.dispatch(context, input, true)?.join().await?;
        Ok(results
            .into_iter()
            .find(|r| matches!(r, ControlFlow::Break(_)))
            .unwrap_or(ControlFlow::Continue(())))
    }

    fn dispatch(
        &self,
        context: &Context,
        input: T,
        serial: bool,
    ) -> Result<Dispatch<Vec<ControlFlow<R>>>> {
        let mut emitter = context.call_leases()?;
        if context.definition.activation != context.owner.activation {
            emitter.push(context.definition_lease()?);
        }
        let inner = context.inner.upgrade().ok_or(Error::RuntimeStopped)?;
        let listeners = inner
            .events
            .lock()
            .unwrap()
            .snapshot::<Listener<T, R>>(&self.slot, false)?;
        let admitted: Vec<_> = listeners
            .into_iter()
            .filter_map(|listener| {
                let mut leases = vec![listener.gate.enter(false, false).ok()?];
                leases.push(
                    listener
                        .context
                        .owner
                        .gate
                        .enter(listener.initializing, false)
                        .ok()?,
                );
                if listener.context.definition.activation != listener.context.owner.activation {
                    leases.push(if listener.context.dynamic.is_empty() {
                        listener
                            .context
                            .definition
                            .gate
                            .enter(listener.initializing, false)
                            .ok()?
                    } else {
                        listener.context.definition_lease().ok()?
                    });
                }
                Some((listener, leases))
            })
            .collect();
        let caller = context.activation();
        let (tx, result) = oneshot::channel();
        let diagnostics = inner.clone();
        inner.executor.spawn(async move {
            let invoke = |listener: Arc<Listener<T, R>>, leases: Vec<Lease>, value: T| async move {
                let (mut ctx, authority, pins) = listener.context.operation_context()?;
                ctx = ctx.with_barrier(listener.gate.clone());
                ctx.caller = Some(caller);
                let result = protected(async move { (listener.callback)(value, ctx).await }).await;
                authority.expire();
                drop(pins);
                drop(leases);
                result
            };
            let outcome = if serial {
                let mut outputs = Vec::new();
                let mut error = None;
                for (listener, lease) in admitted {
                    match invoke(listener, lease, input.clone()).await {
                        Ok(value) => {
                            let stop = matches!(value, ControlFlow::Break(_));
                            outputs.push(value);
                            if stop {
                                break;
                            }
                        }
                        Err(e) => {
                            error = Some(e);
                            break;
                        }
                    }
                }
                error.map_or(Ok(outputs), Err)
            } else {
                let results = futures_util::future::join_all(
                    admitted
                        .into_iter()
                        .map(|(h, l)| invoke(h, l, input.clone())),
                )
                .await;
                let mut outputs = Vec::new();
                let mut errors = Vec::new();
                for result in results {
                    match result {
                        Ok(value) => outputs.push(value),
                        Err(e) => errors.push(e),
                    }
                }
                if errors.is_empty() {
                    Ok(outputs)
                } else {
                    Err(Error::Aggregate(errors))
                }
            };
            if let Err(error) = &outcome {
                diagnostics.record(error.clone());
            }
            drop(emitter);
            let _ = tx.send(outcome);
        });
        Ok(Dispatch { result })
    }
}

type MiddlewareFn<T, R> =
    dyn Fn(T, Context, Next<T, R>) -> BoxFuture<'static, Result<R>> + Send + Sync;
type Terminal<T, R> = dyn Fn(T, Context) -> BoxFuture<'static, Result<R>> + Send + Sync;
struct MiddlewareHandler<T, R> {
    gate: Arc<Gate>,
    context: Context,
    callback: Arc<MiddlewareFn<T, R>>,
}

pub struct Middleware<T, R> {
    slot: Slot,
    marker: PhantomData<fn(T) -> R>,
}
impl<T, R> Clone for Middleware<T, R> {
    fn clone(&self) -> Self {
        Self {
            slot: self.slot.clone(),
            marker: PhantomData,
        }
    }
}
impl<T, R> Middleware<T, R> {
    pub fn new(name: impl Into<Arc<str>>, revision: impl Into<Arc<str>>) -> Self {
        Self {
            slot: Slot::new(ContractId::new(name, revision), RealmId::ROOT),
            marker: PhantomData,
        }
    }
    pub fn contract(&self) -> &ContractId {
        &self.slot.contract
    }
    pub fn realm(&self) -> RealmId {
        self.slot.realm
    }
    pub fn in_realm(mut self, realm: RealmId) -> Self {
        self.slot.realm = realm;
        self
    }
}

struct Chain<T, R> {
    handlers: Vec<Arc<MiddlewareHandler<T, R>>>,
    terminal: Arc<Terminal<T, R>>,
    context: Context,
}

struct Continuation {
    open: bool,
    used: bool,
    completion: Option<oneshot::Receiver<()>>,
}

/// Continuations are single-use and valid only while their middleware frame is running.
pub struct Next<T, R> {
    chain: Arc<Chain<T, R>>,
    index: usize,
    frame: Arc<Mutex<Continuation>>,
}
impl<T, R> Clone for Next<T, R> {
    fn clone(&self) -> Self {
        Self {
            chain: self.chain.clone(),
            index: self.index,
            frame: self.frame.clone(),
        }
    }
}
impl<T: Send + 'static, R: Send + 'static> Next<T, R> {
    /// Admission occurs on first poll. Once admitted, execution survives a dropped wait;
    /// the enclosing dispatch joins it before releasing any listener or emitter lease.
    pub fn run(&self, value: T) -> BoxFuture<'static, Result<R>> {
        let chain = self.chain.clone();
        let index = self.index;
        let frame = self.frame.clone();
        Box::pin(async move {
            let rx = {
                // Closing the frame and admitting a continuation share this lock.
                let mut frame = frame.lock().unwrap();
                if !frame.open {
                    return Err(Error::ExpiredContinuation);
                }
                if frame.used {
                    return Err(Error::DuplicateNext);
                }
                frame.used = true;
                let (tx, rx) = oneshot::channel();
                let (done, completion) = oneshot::channel();
                frame.completion = Some(completion);
                let executor = chain.context.executor().clone();
                executor.spawn(async move {
                    let result = run_chain(chain.clone(), index, value).await;
                    if let Err(error) = &result
                        && let Some(inner) = chain.context.inner.upgrade()
                    {
                        inner.record(error.clone());
                    }
                    let _ = tx.send(result);
                    let _ = done.send(());
                });
                rx
            };
            rx.await.map_err(|_| Error::RuntimeStopped)?
        })
    }
}

fn run_chain<T: Send + 'static, R: Send + 'static>(
    chain: Arc<Chain<T, R>>,
    index: usize,
    value: T,
) -> BoxFuture<'static, Result<R>> {
    Box::pin(async move {
        if index == chain.handlers.len() {
            return protected(async { (chain.terminal)(value, chain.context.clone()).await }).await;
        }
        let handler = &chain.handlers[index];
        let (mut context, authority, pins) = handler.context.operation_context()?;
        context = context.with_barrier(handler.gate.clone());
        context.caller = Some(chain.context.activation());
        let frame = Arc::new(Mutex::new(Continuation {
            open: true,
            used: false,
            completion: None,
        }));
        let next = Next {
            chain: chain.clone(),
            index: index + 1,
            frame: frame.clone(),
        };
        let result = protected(async { (handler.callback)(value, context, next).await }).await;
        let completion = {
            let mut frame = frame.lock().unwrap();
            frame.open = false;
            frame.completion.take()
        };
        authority.expire();
        drop(pins);
        if let Some(completion) = completion {
            completion.await.map_err(|_| Error::RuntimeStopped)?;
        }
        result
    })
}

impl Context {
    pub fn middleware<T, R, F, Fut>(
        &self,
        middleware: &Middleware<T, R>,
        handler: F,
    ) -> Result<EffectHandle>
    where
        T: Send + 'static,
        R: Send + 'static,
        F: Fn(T, Context, Next<T, R>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
    {
        let gate = Gate::new();
        gate.activate();
        self.register_handler(
            middleware.slot.clone(),
            true,
            Some(gate.clone()),
            move |context| MiddlewareHandler {
                gate,
                context,
                callback: Arc::new(move |value, ctx, next| Box::pin(handler(value, ctx, next))),
            },
        )
    }
}

impl<T: Send + 'static, R: Send + 'static> Middleware<T, R> {
    pub async fn run<F, Fut>(&self, context: &Context, input: T, terminal: F) -> Result<R>
    where
        F: Fn(T, Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
    {
        let mut leases = context.call_leases()?;
        if context.definition.activation != context.owner.activation {
            leases.push(context.definition_lease()?);
        }
        let inner = context.inner.upgrade().ok_or(Error::RuntimeStopped)?;
        let handlers = inner
            .events
            .lock()
            .unwrap()
            .snapshot::<MiddlewareHandler<T, R>>(&self.slot, true)?;
        let handlers = handlers
            .into_iter()
            .filter_map(|handler| {
                let mut admitted = vec![handler.gate.enter(false, false).ok()?];
                admitted.push(handler.context.owner.gate.enter(false, false).ok()?);
                if handler.context.definition.activation != handler.context.owner.activation {
                    admitted.push(handler.context.definition_lease().ok()?);
                }
                leases.extend(admitted);
                Some(handler)
            })
            .collect();
        let (context, authority, pins) = context.operation_context()?;
        let chain = Arc::new(Chain {
            handlers,
            context,
            terminal: Arc::new(move |value, ctx| Box::pin(terminal(value, ctx))),
        });
        let (tx, rx) = oneshot::channel();
        let diagnostics = inner.clone();
        inner.executor.spawn(async move {
            let result = run_chain(chain.clone(), 0, input).await;
            authority.expire();
            drop(pins);
            if let Err(error) = &result {
                diagnostics.record(error.clone());
            }
            drop(leases);
            let _ = tx.send(result);
        });
        rx.await.map_err(|_| Error::RuntimeStopped)?
    }
}
