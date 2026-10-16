use super::*;
use cordis_sdk_core::typed::Flow;
use cordis_sdk_core::{
    Context, ResourceObject,
    backend::{Callback, Listener, MessageCallback},
};
use futures_util::FutureExt;
use std::{
    cell::{Cell, RefCell},
    panic::AssertUnwindSafe,
    rc::Rc,
};
use tokio::sync::{Notify, mpsc, oneshot};

#[derive(Clone)]
pub(crate) struct Client(mpsc::Sender<Message>);
pub struct Service {
    pub(crate) client: Client,
    pub(crate) descriptor: Arc<cordis_contract::Descriptor>,
}
impl Service {
    pub async fn invoke(&self, ctx: &rt::Context, method: &str, args: Payload) -> Result<Payload> {
        let signature =
            cordis_contract::portable::method(&self.descriptor.interface().methods, method)
                .map_err(|e| refused("unknown_method", e))?;
        args.check(&signature.args)
            .map_err(|e| refused("invalid_argument", e))?;
        let value = self
            .client
            .invoke(ctx, method, args)
            .await
            .map_err(|e| e.check_declared(&signature.errors, "result"))?;
        value
            .check(&signature.result)
            .map_err(cordis_sdk_core::typed::violation)?;
        Ok(value)
    }
    /// External JSON ingress. This conversion is never used by generated clients.
    pub async fn invoke_json(&self, ctx: &rt::Context, method: &str, args: Value) -> Result<Value> {
        let signature =
            cordis_contract::portable::method(&self.descriptor.interface().methods, method)
                .map_err(|e| refused("unknown_method", e))?;
        let args = Payload::from_json(signature.args.clone(), &args)
            .map_err(|e| refused("invalid_argument", e))?;
        self.invoke(ctx, method, args)
            .await?
            .to_json()
            .map_err(cordis_sdk_core::typed::violation)
    }
}
enum Action {
    Lifecycle(bool),
    Invoke(String, Payload),
    Acquire(String, Payload),
    ResourceCall(u64, String, Payload),
    Release(u64),
    Callback(u64, Value),
    MessageCallback(u64, Payload),
    Listener(u64, Payload),
    Middleware(u64, Payload, rt::Next<Payload, Payload>),
    Forget(u64),
    Close,
}
enum Response {
    Value(Value),
    Message(Payload),
    Flow(Flow<Payload>),
}
impl Response {
    fn flow(self) -> Result<Flow<Payload>> {
        match self {
            Self::Flow(flow) => Ok(flow),
            _ => Err(unknown("expected listener reply")),
        }
    }
    fn value(self) -> Result<Value> {
        match self {
            Self::Value(value) => Ok(value),
            _ => Err(unknown("expected control reply")),
        }
    }
    fn message(self) -> Result<Payload> {
        match self {
            Self::Message(value) => Ok(value),
            _ => Err(unknown("expected message reply")),
        }
    }
}
struct Message {
    ctx: Option<rt::Context>,
    action: Action,
    reply: oneshot::Sender<Result<Response>>,
}
#[derive(Clone)]
enum StoredCallback {
    Control(Callback),
    Message(MessageCallback),
    Listener(Listener),
}
pub(crate) struct Instance {
    pub capabilities: Arc<Capabilities>,
    pub primary: cordis_sdk_core::Contract,
    client: mpsc::WeakSender<Message>,
    callbacks: RefCell<BTreeMap<u64, StoredCallback>>,
    next: Cell<u64>,
    resources: RefCell<BTreeMap<u64, Option<Rc<dyn ResourceObject>>>>,
    next_resource: Cell<u64>,
}
pub(crate) struct CallbackReservation {
    instance: Rc<Instance>,
    id: Option<u64>,
}
impl CallbackReservation {
    pub fn commit(mut self, callback: Callback) -> u64 {
        let id = self.id.take().expect("live reservation");
        let old = {
            let mut callbacks = self.instance.callbacks.borrow_mut();
            std::mem::replace(
                callbacks.get_mut(&id).expect("reserved callback"),
                StoredCallback::Control(callback),
            )
        };
        drop(old);
        id
    }
}
impl Drop for CallbackReservation {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            self.instance.remove(id);
        }
    }
}
impl Instance {
    fn reserve_resource(&self) -> Result<u64> {
        if self.resources.borrow().len() >= 4096 {
            return Err(refused("overloaded", "resource capacity exhausted"));
        }
        let id = self
            .next_resource
            .get()
            .checked_add(1)
            .ok_or_else(|| refused("exhausted", "resource IDs exhausted"))?;
        self.next_resource.set(id);
        self.resources.borrow_mut().insert(id, None);
        Ok(id)
    }
    fn resource(&self, id: u64) -> Result<Rc<dyn ResourceObject>> {
        self.resources
            .borrow()
            .get(&id)
            .and_then(Clone::clone)
            .ok_or_else(|| refused("closed", "resource absent"))
    }
    fn remove_resource(&self, id: u64) {
        let removed = self.resources.borrow_mut().remove(&id);
        drop(removed);
    }
    pub fn reserve(self: &Rc<Self>) -> Result<CallbackReservation> {
        let id = self.insert(Rc::new(|_, _| {
            Box::pin(async { Err(refused("pending", "callback not committed")) })
        }))?;
        Ok(CallbackReservation {
            instance: self.clone(),
            id: Some(id),
        })
    }
    pub fn insert(&self, callback: Callback) -> Result<u64> {
        self.store(StoredCallback::Control(callback))
    }
    pub fn insert_message(&self, callback: MessageCallback) -> Result<u64> {
        self.store(StoredCallback::Message(callback))
    }
    pub fn insert_listener(&self, callback: Listener) -> Result<u64> {
        self.store(StoredCallback::Listener(callback))
    }
    fn callback(&self, id: u64) -> Result<StoredCallback> {
        self.callbacks
            .borrow()
            .get(&id)
            .cloned()
            .ok_or_else(|| refused("closed", "callback absent"))
    }
    fn store(&self, callback: StoredCallback) -> Result<u64> {
        if self.callbacks.borrow().len() >= 4096 {
            return Err(refused("overloaded", "callback capacity exhausted"));
        }
        let id = self
            .next
            .get()
            .checked_add(1)
            .ok_or_else(|| refused("exhausted", "callback IDs exhausted"))?;
        self.next.set(id);
        self.callbacks.borrow_mut().insert(id, callback);
        Ok(id)
    }
    pub fn remove(&self, id: u64) {
        let removed = self.callbacks.borrow_mut().remove(&id);
        drop(removed);
    }
    pub fn client(&self) -> Result<Client> {
        self.client
            .upgrade()
            .map(Client)
            .ok_or_else(|| refused("closed", "instance closed"))
    }
}
pub(crate) struct Frame {
    pub open: Cell<bool>,
    pub pending: Cell<usize>,
    pub changed: Notify,
}
impl Frame {
    pub fn new() -> Rc<Self> {
        Rc::new(Self {
            open: Cell::new(true),
            pending: Cell::new(0),
            changed: Notify::new(),
        })
    }
    async fn finish(&self) {
        self.open.set(false);
        while self.pending.get() != 0 {
            self.changed.notified().await;
        }
    }
}
impl Client {
    pub async fn start<P: Plugin>(config: Value, capabilities: Arc<Capabilities>) -> Result<Self> {
        let (sender, mut receiver) = mpsc::channel::<Message>(64);
        let weak = sender.downgrade();
        let (ready, result) = oneshot::channel();
        std::thread::Builder::new()
            .name("cordis-rust-plugin".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(e) => {
                        let _ = ready.send(Err(unknown(e)));
                        return;
                    }
                };
                runtime.block_on(tokio::task::LocalSet::new().run_until(async move {
                    let plugin =
                        match std::panic::catch_unwind(AssertUnwindSafe(|| P::create(config))) {
                            Ok(Ok(plugin)) => Rc::new(plugin),
                            Ok(Err(error)) => {
                                let _ = ready.send(Err(error));
                                return;
                            }
                            Err(_) => {
                                let _ = ready.send(Err(unknown("plugin construction panicked")));
                                return;
                            }
                        };
                    let instance = Rc::new(Instance {
                        capabilities,
                        primary: P::manifest().provides,
                        client: weak,
                        callbacks: RefCell::default(),
                        next: Cell::new(0),
                        resources: RefCell::default(),
                        next_resource: Cell::new(0),
                    });
                    let _ = ready.send(Ok(()));
                    let mut jobs = tokio::task::JoinSet::new();
                    while let Some(message) = receiver.recv().await {
                        if let Action::Close = message.action {
                            while jobs.join_next().await.is_some() {}
                            if !instance.resources.borrow().is_empty() {
                                let _ = message
                                    .reply
                                    .send(Err(unknown("resource obligations remain")));
                                continue;
                            }
                            let callbacks = std::mem::take(&mut *instance.callbacks.borrow_mut());
                            drop(callbacks);
                            drop(plugin);
                            let _ = message.reply.send(Ok(Response::Value(Value::Null)));
                            return;
                        }
                        if let Action::Forget(id) = message.action {
                            instance.remove(id);
                            let _ = message.reply.send(Ok(Response::Value(Value::Null)));
                            continue;
                        }
                        let instance = instance.clone();
                        let plugin = plugin.clone();
                        jobs.spawn_local(async move {
                            let frame = Frame::new();
                            let ctx = Context::from_backend(Rc::new(super::backend::Native {
                                ctx: message.ctx.expect("callback context"),
                                instance: Rc::downgrade(&instance),
                                frame: frame.clone(),
                                next: match &message.action {
                                    Action::Middleware(_, _, next) => Some(next.clone()),
                                    _ => None,
                                },
                            }));
                            let result = AssertUnwindSafe(async {
                                let result = match message.action {
                                    Action::Lifecycle(activate) => {
                                        if activate {
                                            plugin.activate(ctx).await?;
                                        } else {
                                            plugin.deactivate(ctx).await?;
                                        }
                                        Ok(Value::Null)
                                    }
                                    Action::Invoke(method, args) => {
                                        return plugin
                                            .invoke(ctx, method, args)
                                            .await
                                            .map(Response::Message);
                                    }
                                    Action::Acquire(kind, args) => {
                                        // Reserve before user code can acquire anything. A panic or
                                        // Unknown keeps the reservation and blocks instance closure.
                                        let id = instance.reserve_resource()?;
                                        match plugin.acquire(ctx, kind, args).await {
                                            Ok(resource) => {
                                                *instance
                                                    .resources
                                                    .borrow_mut()
                                                    .get_mut(&id)
                                                    .expect("reserved resource") = Some(resource);
                                                Ok(json!(id))
                                            }
                                            Err(failure) => {
                                                if matches!(
                                                    failure,
                                                    Failure::Rejected(_) | Failure::Refused(_)
                                                ) {
                                                    instance.remove_resource(id);
                                                }
                                                Err(failure)
                                            }
                                        }
                                    }
                                    Action::ResourceCall(id, method, args) => {
                                        return instance
                                            .resource(id)?
                                            .invoke(ctx, method, args)
                                            .await
                                            .map(Response::Message);
                                    }
                                    Action::Release(id) => {
                                        instance.resource(id)?.close(ctx).await?;
                                        instance.remove_resource(id);
                                        Ok(Value::Null)
                                    }
                                    Action::Callback(id, args) => {
                                        let StoredCallback::Control(callback) =
                                            instance.callback(id)?
                                        else {
                                            return Err(unknown("callback kind mismatch"));
                                        };
                                        callback(ctx, args).await
                                    }
                                    Action::MessageCallback(id, args)
                                    | Action::Middleware(id, args, _) => {
                                        let StoredCallback::Message(callback) =
                                            instance.callback(id)?
                                        else {
                                            return Err(unknown("callback kind mismatch"));
                                        };
                                        return callback(ctx, args).await.map(Response::Message);
                                    }
                                    Action::Listener(id, args) => {
                                        let StoredCallback::Listener(callback) =
                                            instance.callback(id)?
                                        else {
                                            return Err(unknown("callback kind mismatch"));
                                        };
                                        return callback(ctx, args).await.map(Response::Flow);
                                    }
                                    _ => unreachable!(),
                                };
                                result.map(Response::Value)
                            })
                            .catch_unwind()
                            .await
                            .unwrap_or_else(|_| Err(unknown("plugin callback panicked")));
                            frame.finish().await;
                            let _ = message.reply.send(result);
                        });
                        while jobs.try_join_next().is_some() {}
                    }
                    while jobs.join_next().await.is_some() {}
                }));
            })
            .map_err(unknown)?;
        result.await.map_err(unknown)??;
        Ok(Self(sender))
    }
    async fn send(&self, ctx: Option<rt::Context>, action: Action) -> Result<Response> {
        let (reply, result) = oneshot::channel();
        self.0
            .try_send(Message { ctx, action, reply })
            .map_err(|e| refused("unavailable", e))?;
        result.await.map_err(unknown)?
    }
    pub async fn lifecycle(&self, ctx: &rt::Context, activate: bool) -> Result<()> {
        let client = self.clone();
        ctx.operation(move |ctx| async move {
            Ok(client.send(Some(ctx), Action::Lifecycle(activate)).await)
        })
        .await
        .map_err(unknown)??
        .value()
        .map(|_| ())
    }
    pub async fn invoke(&self, ctx: &rt::Context, method: &str, args: Payload) -> Result<Payload> {
        let client = self.clone();
        let method = method.to_owned();
        ctx.operation(move |ctx| async move {
            Ok(client.send(Some(ctx), Action::Invoke(method, args)).await)
        })
        .await
        .map_err(unknown)??
        .message()
    }
    pub async fn callback(&self, ctx: rt::Context, id: u64, args: Value) -> Result<Value> {
        self.send(Some(ctx.snapshot()), Action::Callback(id, args))
            .await?
            .value()
    }
    pub async fn message_callback(
        &self,
        ctx: rt::Context,
        id: u64,
        args: Payload,
    ) -> Result<Payload> {
        self.send(Some(ctx.snapshot()), Action::MessageCallback(id, args))
            .await?
            .message()
    }
    pub async fn listener(
        &self,
        ctx: rt::Context,
        id: u64,
        args: Payload,
    ) -> Result<Flow<Payload>> {
        self.send(Some(ctx.snapshot()), Action::Listener(id, args))
            .await?
            .flow()
    }
    pub async fn acquire(&self, ctx: rt::Context, kind: String, args: Payload) -> Result<u64> {
        self.send(Some(ctx.snapshot()), Action::Acquire(kind, args))
            .await?
            .value()?
            .as_u64()
            .ok_or_else(|| unknown("invalid resource acknowledgement"))
    }
    pub async fn resource_call(
        &self,
        ctx: rt::Context,
        id: u64,
        method: String,
        args: Payload,
    ) -> Result<Payload> {
        self.send(Some(ctx.snapshot()), Action::ResourceCall(id, method, args))
            .await?
            .message()
    }
    pub async fn release(&self, ctx: rt::Context, id: u64) -> Result<()> {
        self.send(Some(ctx.snapshot()), Action::Release(id))
            .await
            .map(|_| ())
    }
    pub async fn middleware(
        &self,
        ctx: rt::Context,
        id: u64,
        args: Payload,
        next: rt::Next<Payload, Payload>,
    ) -> Result<Payload> {
        self.send(Some(ctx.snapshot()), Action::Middleware(id, args, next))
            .await?
            .message()
    }
    pub async fn forget(&self, id: u64) -> Result<()> {
        self.send(None, Action::Forget(id)).await.map(|_| ())
    }
    pub async fn close(&self) -> Result<()> {
        self.send(None, Action::Close).await.map(|_| ())
    }
}
