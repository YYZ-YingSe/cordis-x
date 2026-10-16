use super::*;
use super::{
    backend::Abi,
    callbacks::{Frame, Registry},
    ffi::Host,
};
use cordis_sdk_core::Context;
use futures_util::FutureExt;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
tokio::task_local! { pub(crate) static CALLER: (u64, u64); }
#[derive(Clone)]
pub(crate) struct Client {
    pub validation: cordis_sdk_core::Validation,
    send: mpsc::Sender<Message>,
    thread: Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
}
struct Message {
    action: Action,
    reply: Box<dyn FnOnce(Result<Payload>) + Send>,
}
enum Action {
    Dispatch(Host, u32, u64, String, Payload),
    Close,
}
struct Instance<P> {
    validation: cordis_sdk_core::Validation,
    manifest: Rc<cordis_sdk_core::PluginManifest>,
    bindings: Rc<cordis_sdk_core::Bindings>,
    plugin: Rc<P>,
    // Keep resolved contracts alive until this instance has fully drained.
    interface: cordis_sdk_core::Interface,
    registry: Rc<Registry>,
    resources: super::resources::Resources,
    attempted: Cell<bool>,
    active: Cell<bool>,
    deactivation: RefCell<Option<Result<Payload>>>,
    inflight: Cell<usize>,
    cleanup_inflight: Cell<usize>,
}
impl<P: Plugin> Instance<P> {
    async fn dispatch(
        &self,
        host: Host,
        op: u32,
        target: u64,
        method: String,
        args: Payload,
    ) -> Result<Payload> {
        if op == 1 {
            if self.attempted.replace(true) {
                return Err(refused("closed", "activation already attempted"));
            }
        } else if !self.attempted.get() {
            return Err(refused("closed", "activation not attempted"));
        }
        if matches!(op, 2 | 4 | 5) && !self.active.get() {
            return Err(refused("closed", "instance inactive"));
        }
        if op == 3 {
            if let Some(result) = self.deactivation.borrow().clone() {
                return result;
            }
            if self.inflight.get() != 0
                || self.cleanup_inflight.get() != 0
                || !self.registry.recovered()
                || !self.resources.recovered()
            {
                return Err(refused("owned", "callbacks remain"));
            }
            self.active.set(false);
        }
        let cleanup = matches!(op, 3 | 6 | 7 | 9 | 10 | 16);
        let inflight = if cleanup {
            &self.cleanup_inflight
        } else {
            &self.inflight
        };
        if inflight.get() >= 128 {
            return Err(refused("overloaded", "callback capacity reached"));
        }
        inflight.set(inflight.get() + 1);
        let frame = Frame::new();
        let backend = Abi {
            host,
            registry: self.registry.clone(),
            frame: frame.clone(),
            cleanup,
            task: op == 12,
            validation: self.validation,
            manifest: self.manifest.clone(),
            bindings: self.bindings.clone(),
        };
        let ctx = Context::from_backend(Rc::new(backend));
        let result = std::panic::AssertUnwindSafe(async {
            if matches!(op, 2 | 4 | 5 | 13 | 14 | 15) {
                return match op {
                    2 => {
                        let signature =
                            cordis_sdk_core::contract::method(&self.interface.methods, &method)
                                .map_err(|e| refused("invalid_argument", e))?;
                        let input = cordis_sdk_core::message::incoming::<P>(
                            signature.args.clone(),
                            args.one().map_err(|e| refused("invalid_argument", e))?,
                        )
                        .map_err(|e| refused("invalid_argument", e))?;
                        let output = self
                            .plugin
                            .clone()
                            .invoke(ctx, method, input)
                            .await
                            .map_err(|e| e.check_declared(&signature.errors, "method result"))?;
                        output.check(&signature.result).map_err(unknown)?;
                        Ok(Payload::message(
                            output.encode(cordis_wire::MAX_MESSAGE).map_err(unknown)?,
                        ))
                    }
                    4 => {
                        let contract = self
                            .interface
                            .resources
                            .get(&method)
                            .ok_or_else(|| refused("unknown_resource_kind", &method))?;
                        let input = cordis_sdk_core::message::incoming::<P>(
                            contract.args.clone(),
                            args.one().map_err(|e| refused("invalid_argument", e))?,
                        )
                        .map_err(|e| refused("invalid_argument", e))?;
                        let (id, entry) = self.resources.reserve(contract.clone())?;
                        match self.plugin.clone().acquire(ctx, method, input).await {
                            Ok(object) => {
                                *entry.value.borrow_mut() = Some(object);
                                Ok(json!(id).into())
                            }
                            Err(failure) => {
                                if matches!(failure, Failure::Rejected(_) | Failure::Refused(_)) {
                                    self.resources.reject(id);
                                }
                                Err(failure)
                            }
                        }
                    }
                    5 => self
                        .resources
                        .get(target)?
                        .invoke(
                            ctx,
                            frame.clone(),
                            method,
                            args.one().map_err(|e| refused("invalid_argument", e))?,
                        )
                        .await
                        .map(Payload::message),
                    13 | 14 => {
                        self.registry
                            .get(&format!("r{target}"))?
                            .call_payload(ctx, args, false, false)
                            .await
                    }
                    15 => {
                        self.registry
                            .get(&format!("q{target}"))?
                            .call_payload(ctx, args, true, true)
                            .await
                    }
                    _ => unreachable!(),
                };
            }
            let args = args
                .control_only()
                .map_err(|e| refused("invalid_argument", e))?;
            let control = match op {
                6 => {
                    self.resources
                        .get(target)?
                        .close(ctx, frame.clone())
                        .await?;
                    Ok(Value::Null)
                }
                1 => {
                    self.plugin.clone().activate(ctx).await?;
                    self.active.set(true);
                    Ok(Value::Null)
                }
                3 => {
                    self.plugin.clone().deactivate(ctx).await?;
                    Ok(Value::Null)
                }
                7 => {
                    self.registry
                        .get(&format!("e{target}"))?
                        .call(ctx, Value::Null, true, true)
                        .await
                }
                8..=10 => {
                    self.registry
                        .sequence_call(
                            match op {
                                8 => "sequence_next",
                                9 => "sequence_undo",
                                _ => "sequence_release",
                            },
                            &format!("s{target}"),
                            &args.as_u64().unwrap_or(0).to_string(),
                            ctx,
                        )
                        .await
                }
                11 => {
                    self.registry
                        .get(&format!("u{target}"))?
                        .call(ctx, Value::Null, true, true)
                        .await
                }
                12 => {
                    let entry = self.registry.get(&format!("t{target}"))?;
                    let result = entry.clone().call(ctx, Value::Null, true, true).await;
                    if !matches!(result, Err(Failure::Unknown(_))) {
                        let removed = entry.callback.borrow_mut().take();
                        drop(removed);
                    }
                    result
                }
                16 => {
                    self.registry.release(&format!("r{target}"))?;
                    Ok(Value::Null)
                }
                _ => Err(refused("unsupported", "unknown callback operation")),
            };
            control.map(Into::into)
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(unknown("plugin callback panicked")));
        frame.finish().await;
        inflight.set(inflight.get() - 1);
        if op == 3 {
            *self.deactivation.borrow_mut() = Some(result.clone());
        }
        result
    }
}
impl Client {
    pub fn start<P: Plugin>(
        config: Value,
        validation: cordis_sdk_core::Validation,
    ) -> Result<Self> {
        let (send, mut receive) = mpsc::channel::<Message>(256);
        let (ready, result) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("cordis-abi-plugin".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = ready.send(Err(unknown(e)));
                        return;
                    }
                };
                runtime.block_on(tokio::task::LocalSet::new().run_until(async move {
                    let (plugin, interface) =
                        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            let mut interface = P::interface();
                            interface.set_validation(validation);
                            P::create(config).map(|plugin| (Rc::new(plugin), interface))
                        })) {
                            Ok(Ok(instance)) => instance,
                            Ok(Err(e)) => {
                                let _ = ready.send(Err(e));
                                return;
                            }
                            Err(_) => {
                                let _ = ready.send(Err(unknown("constructor panicked")));
                                return;
                            }
                        };
                    let instance = Rc::new(Instance {
                        validation,
                        manifest: Rc::new(P::manifest()),
                        bindings: Rc::new(P::bindings()),
                        plugin,
                        interface,
                        registry: Rc::new(Registry::default()),
                        resources: Default::default(),
                        attempted: Cell::new(false),
                        active: Cell::new(false),
                        deactivation: RefCell::default(),
                        inflight: Cell::new(0),
                        cleanup_inflight: Cell::new(0),
                    });
                    let _ = ready.send(Ok(()));
                    let mut jobs = tokio::task::JoinSet::new();
                    while let Some(message) = receive.recv().await {
                        while jobs.try_join_next().is_some() {}
                        match message.action {
                            Action::Close => {
                                if !jobs.is_empty()
                                    || instance.inflight.get() != 0
                                    || instance.cleanup_inflight.get() != 0
                                    || (instance.attempted.get()
                                        && !matches!(*instance.deactivation.borrow(), Some(Ok(_))))
                                    || !instance.registry.recovered()
                                    || !instance.resources.recovered()
                                {
                                    (message.reply)(Err(refused(
                                        "owned",
                                        "instance not recovered",
                                    )));
                                    continue;
                                }
                                while jobs.join_next().await.is_some() {}
                                drop(instance);
                                (message.reply)(Ok(Value::Null.into()));
                                return;
                            }
                            Action::Dispatch(host, op, target, method, args) => {
                                let instance = instance.clone();
                                jobs.spawn_local(async move {
                                    let result = CALLER
                                        .scope(
                                            (host.host, host.grant),
                                            instance.dispatch(host, op, target, method, args),
                                        )
                                        .await;
                                    (message.reply)(result);
                                });
                            }
                        }
                        while jobs.try_join_next().is_some() {}
                    }
                    while jobs.join_next().await.is_some() {}
                }));
            })
            .map_err(unknown)?;
        match result.recv().map_err(unknown)? {
            Ok(()) => Ok(Self {
                validation,
                send,
                thread: Arc::new(Mutex::new(Some(thread))),
            }),
            Err(e) => {
                let _ = thread.join();
                Err(e)
            }
        }
    }
    fn send(&self, action: Action) -> Result<Payload> {
        let (reply, result) = std::sync::mpsc::sync_channel(1);
        self.send
            .try_send(Message {
                action,
                reply: Box::new(move |value| {
                    let _ = reply.send(value);
                }),
            })
            .map_err(|e| refused("overloaded", e))?;
        result.recv().map_err(unknown)?
    }
    pub fn dispatch(
        &self,
        host: Host,
        op: u32,
        target: u64,
        method: String,
        args: Payload,
        reply: impl FnOnce(Result<Payload>) + Send + 'static,
    ) -> Result<()> {
        self.send
            .try_send(Message {
                action: Action::Dispatch(host, op, target, method, args),
                reply: Box::new(reply),
            })
            .map_err(|e| refused("overloaded", e))
    }
    pub fn close(&self) -> Result<()> {
        self.send(Action::Close)?;
        if let Some(thread) = self.thread.lock().unwrap().take() {
            thread.join().map_err(|_| unknown("executor panicked"))?;
        }
        Ok(())
    }
}
