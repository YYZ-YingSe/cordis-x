use super::*;
use callbacks::{Frame, Registry};
use cordis_sdk_core::{
    LocalFuture, Next,
    backend::{Backend, Callback, Handle, Operation, Reply},
};
use ffi::{Call, Host, Reply as HostReply};
use futures_util::FutureExt;
use std::{any::Any, rc::Weak, time::Duration};
#[derive(Clone)]
pub(crate) struct Abi {
    pub validation: cordis_sdk_core::Validation,
    pub host: Host,
    pub registry: Rc<Registry>,
    pub frame: Rc<Frame>,
    pub cleanup: bool,
    pub task: bool,
    pub manifest: Rc<cordis_sdk_core::PluginManifest>,
    pub bindings: Rc<cordis_sdk_core::Bindings>,
}
#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Effect,
    Sequence,
    Scope,
    Task,
    Subscription,
    Next,
    Child,
    Resource,
}
struct Capability {
    owner: Weak<Registry>,
    id: u64,
    kind: Kind,
}
fn numeric(id: &str) -> u64 {
    id[1..].parse().expect("registry ID")
}
impl Abi {
    fn handle(&self, id: u64, kind: Kind) -> Handle {
        Handle::new(Capability {
            owner: Rc::downgrade(&self.registry),
            id,
            kind,
        })
    }
    fn id(&self, handle: &Handle, kind: Kind) -> Result<u64> {
        let cap = handle
            .downcast_ref::<Capability>()
            .ok_or_else(|| refused("invalid_handle", "foreign backend"))?;
        if cap.kind != kind || !Weak::ptr_eq(&cap.owner, &Rc::downgrade(&self.registry)) {
            return Err(refused("invalid_handle", "foreign capability"));
        }
        Ok(cap.id)
    }
    async fn managed(
        &self,
        op: u32,
        target: u64,
        callback: u64,
        label: String,
        args: Value,
    ) -> Result<HostReply> {
        self.host
            .call(Call::Managed(op, target, callback, label, args))
            .await
    }
    async fn reactive(
        &self,
        op: u32,
        target: u64,
        callback: u64,
        label: String,
        args: Value,
    ) -> Result<HostReply> {
        self.host
            .call(Call::Reactive(op, target, callback, label, args.into()))
            .await
    }
    async fn reactive_payload(
        &self,
        op: u32,
        target: u64,
        callback: u64,
        label: String,
        args: Payload,
    ) -> Result<HostReply> {
        self.host
            .call(Call::Reactive(op, target, callback, label, args))
            .await
    }
    async fn register_payload(
        &self,
        prefix: char,
        callback: callbacks::WireCallback,
        kind: Kind,
        call: impl FnOnce(u64) -> Call,
    ) -> Result<Reply> {
        let id = self.registry.insert_payload(prefix, callback)?;
        let result = self.host.call(call(numeric(&id))).await;
        if matches!(result, Err(Failure::Rejected(_) | Failure::Refused(_))) {
            self.registry.remove(&id);
        }
        Ok(Reply::Handle(self.handle(result?.handle, kind)))
    }
    async fn register(
        &self,
        prefix: char,
        callback: Callback,
        kind: Kind,
        call: impl FnOnce(u64) -> Call,
    ) -> Result<Reply> {
        let id = self.registry.insert(prefix, callback)?;
        let result = self.host.call(call(numeric(&id))).await;
        if matches!(result, Err(Failure::Rejected(_) | Failure::Refused(_))) {
            self.registry.remove(&id);
        }
        Ok(Reply::Handle(self.handle(result?.handle, kind)))
    }
    async fn execute(&self, operation: Operation) -> Result<Reply> {
        let result = match operation {
            Operation::Acquire {
                dependency,
                kind,
                args,
            } => {
                if !self.manifest.requires.iter().any(|d| d.name == dependency) {
                    return Err(refused("undeclared_dependency", dependency));
                }
                let reply = self
                    .host
                    .call(Call::Resource(
                        1,
                        0,
                        dependency,
                        kind,
                        Payload::message(
                            args.encode(cordis_wire::MAX_MESSAGE)
                                .map_err(|e| refused("invalid_argument", e))?,
                        ),
                    ))
                    .await?;
                return Ok(Reply::Handle(self.handle(reply.handle, Kind::Resource)));
            }
            Operation::ResourceInvoke {
                resource,
                method,
                args,
                result,
            } => {
                let reply = self
                    .host
                    .call(Call::Resource(
                        2,
                        self.id(&resource, Kind::Resource)?,
                        String::new(),
                        method,
                        Payload::message(
                            args.encode(cordis_wire::MAX_MESSAGE)
                                .map_err(|e| refused("invalid_argument", e))?,
                        ),
                    ))
                    .await?;
                return cordis_sdk_core::Message::from_bytes(
                    result,
                    reply.value.one().map_err(unknown)?,
                )
                .map(Reply::Message)
                .map_err(unknown);
            }
            Operation::ResourceDispose { resource } => {
                self.host
                    .call(Call::Resource(
                        3,
                        self.id(&resource, Kind::Resource)?,
                        String::new(),
                        String::new(),
                        Value::Null.into(),
                    ))
                    .await?
            }
            Operation::Info => return Ok(Reply::Value(json!({"cleanup":self.cleanup}))),
            Operation::Invoke {
                dependency,
                method,
                args,
                result,
            } => {
                if !self.manifest.requires.iter().any(|d| d.name == dependency) {
                    return Err(refused("undeclared_dependency", dependency));
                }
                let reply = self
                    .host
                    .call(Call::Invoke(
                        dependency,
                        method,
                        Payload::message(
                            args.encode(cordis_wire::MAX_MESSAGE)
                                .map_err(|e| refused("invalid_argument", e))?,
                        ),
                    ))
                    .await?;
                return cordis_sdk_core::Message::from_bytes(
                    result,
                    reply.value.one().map_err(unknown)?,
                )
                .map(Reply::Message)
                .map_err(unknown);
            }
            Operation::Metadata { name, revision } => {
                self.managed(14, 0, 0, name, json!(revision)).await?
            }
            Operation::Defer { label, cleanup } => {
                return self
                    .register('e', cleanup, Kind::Effect, |id| Call::Defer(id, label))
                    .await;
            }
            Operation::Dispose { effect } => {
                let kind = effect
                    .downcast_ref::<Capability>()
                    .ok_or_else(|| refused("invalid_handle", "foreign effect"))?
                    .kind;
                let id = self.id(&effect, kind)?;
                match kind {
                    Kind::Effect => self.host.call(Call::Dispose(id)).await?,
                    Kind::Subscription => {
                        self.reactive(3, id, 0, String::new(), Value::Null).await?
                    }
                    _ => return Err(refused("invalid_handle", "expected effect")),
                }
            }
            Operation::Scope { label } => {
                return Ok(Reply::Handle(self.handle(
                    self.managed(5, 0, 0, label, Value::Null).await?.handle,
                    Kind::Scope,
                )));
            }
            Operation::ScopeRun { scope, run } => {
                let scope = self.id(&scope, Kind::Scope)?;
                let id = self.registry.insert('u', run)?;
                let result = self
                    .managed(7, scope, numeric(&id), String::new(), Value::Null)
                    .await;
                if !matches!(result, Err(Failure::Unknown(_))) {
                    self.registry.remove(&id);
                }
                result?
            }
            Operation::ScopeDispose { scope } => {
                self.managed(
                    8,
                    self.id(&scope, Kind::Scope)?,
                    0,
                    String::new(),
                    Value::Null,
                )
                .await?
            }
            Operation::Spawn { label, task } => {
                return self
                    .register('t', task, Kind::Task, |id| {
                        Call::Managed(10, 0, id, label, Value::Null)
                    })
                    .await;
            }
            Operation::TaskJoin { task } => {
                self.managed(
                    11,
                    self.id(&task, Kind::Task)?,
                    0,
                    String::new(),
                    Value::Null,
                )
                .await?
            }
            Operation::Cancelled => {
                if !self.task {
                    return Err(refused("closed", "not a task context"));
                }
                self.managed(13, 0, 0, String::new(), Value::Null).await?;
                return Ok(Reply::Value(Value::Null));
            }
            Operation::Sleep { milliseconds } => {
                if milliseconds > 60_000 {
                    return Err(refused("invalid_argument", "sleep must be 0..60000 ms"));
                }
                if self.task {
                    self.managed(13, 0, 0, String::new(), json!(milliseconds))
                        .await?;
                } else {
                    tokio::time::sleep(Duration::from_millis(milliseconds)).await;
                }
                return Ok(Reply::Value(Value::Null));
            }
            Operation::Effect { label, next } => {
                let id = self.registry.sequence(next)?;
                let result = self.managed(1, 0, numeric(&id), label, Value::Null).await;
                if matches!(result, Err(Failure::Rejected(_) | Failure::Refused(_))) {
                    self.registry.remove_sequence(&id);
                }
                return Ok(Reply::Handle(self.handle(result?.handle, Kind::Sequence)));
            }
            Operation::SequenceReady { sequence } => {
                let value = self
                    .managed(
                        2,
                        self.id(&sequence, Kind::Sequence)?,
                        0,
                        String::new(),
                        Value::Null,
                    )
                    .await?
                    .value;
                let result = match value.control_only().map_err(unknown)?["completion"].as_str() {
                    Some("completed") => "Completed",
                    Some("stopped") => "Stopped",
                    _ => return Err(unknown("invalid completion")),
                };
                return Ok(Reply::Value(json!(result)));
            }
            Operation::SequenceDispose { sequence } => {
                self.managed(
                    3,
                    self.id(&sequence, Kind::Sequence)?,
                    0,
                    String::new(),
                    Value::Null,
                )
                .await?
            }
            Operation::On {
                event,
                handler,
                signature,
            } => {
                if !self.bindings.events.contains_key(&event) {
                    return Err(refused("undeclared_event", event));
                }
                let callback: callbacks::WireCallback = Rc::new(move |ctx, payload| {
                    let handler = handler.clone();
                    let signature = signature.clone();
                    Box::pin(async move {
                        let input = binary_input(&signature.args, payload)?;
                        match handler(ctx, input)
                            .await
                            .map_err(|e| e.check_declared(&signature.errors, "event result"))?
                        {
                            cordis_sdk_core::typed::Flow::Continue => {
                                Ok(json!({"kind":"continue"}).into())
                            }
                            cordis_sdk_core::typed::Flow::Break(value) => {
                                value.check(&signature.result).map_err(unknown)?;
                                Ok(Payload::with_message(
                                    json!({"kind":"break"}),
                                    value.encode(cordis_wire::MAX_MESSAGE).map_err(unknown)?,
                                ))
                            }
                        }
                    })
                });
                return self
                    .register_payload('r', callback, Kind::Subscription, |id| {
                        Call::Reactive(1, 0, id, event, json!(false).into())
                    })
                    .await;
            }
            Operation::Emit {
                event,
                args,
                serial,
                signature,
            } => {
                if !self.bindings.events.contains_key(&event) {
                    return Err(refused("undeclared_event", event));
                }
                let id = self
                    .reactive_payload(
                        4,
                        0,
                        0,
                        event,
                        Payload::with_message(
                            json!({"serial":serial}),
                            args.encode(cordis_wire::MAX_MESSAGE).map_err(unknown)?,
                        ),
                    )
                    .await?
                    .handle;
                let payload = self
                    .reactive(5, id, 0, String::new(), Value::Null)
                    .await?
                    .value;
                let tags = payload
                    .control
                    .as_array()
                    .ok_or_else(|| unknown("invalid dispatch result"))?;
                let mut messages = payload.messages.into_iter();
                let mut flows = Vec::with_capacity(tags.len());
                for tag in tags {
                    flows.push(match tag.as_str() {
                        Some("continue") => cordis_sdk_core::typed::Flow::Continue,
                        Some("break") => cordis_sdk_core::typed::Flow::Break(
                            cordis_sdk_core::Message::from_bytes(
                                signature.result.clone(),
                                messages
                                    .next()
                                    .ok_or_else(|| unknown("missing event body"))?,
                            )
                            .map_err(unknown)?,
                        ),
                        _ => return Err(unknown("invalid event flow")),
                    });
                }
                if messages.next().is_some() {
                    return Err(unknown("unexpected event body"));
                }
                return Ok(Reply::Flows(flows));
            }
            Operation::Middleware {
                name,
                handler,
                signature,
            } => {
                if !self.bindings.middleware.contains_key(&name) {
                    return Err(refused("undeclared_middleware", name));
                }
                let callback: callbacks::WireCallback = Rc::new(move |ctx, payload| {
                    let handler = handler.clone();
                    let signature = signature.clone();
                    Box::pin(async move {
                        let backend = ctx
                            .backend()
                            .as_any()
                            .downcast_ref::<Abi>()
                            .expect("ABI context");
                        let id = payload.control["next"]
                            .as_u64()
                            .filter(|v| *v != 0)
                            .ok_or_else(|| unknown("invalid continuation"))?;
                        let next = Next::from_handle(backend.handle(id, Kind::Next));
                        let input = binary_input(&signature.args, payload)?;
                        binary_output(
                            &signature.result,
                            handler(ctx, input, next).await.map_err(|e| {
                                e.check_declared(&signature.errors, "middleware result")
                            })?,
                        )
                    })
                });
                return self
                    .register_payload('r', callback, Kind::Subscription, |id| {
                        Call::Reactive(2, 0, id, name, Value::Null.into())
                    })
                    .await;
            }
            Operation::Run {
                name,
                args,
                terminal,
                signature,
            } => {
                if !self.bindings.middleware.contains_key(&name) {
                    return Err(refused("undeclared_middleware", name));
                }
                let callback_signature = signature.clone();
                let callback: callbacks::WireCallback = Rc::new(move |ctx, payload| {
                    let terminal = terminal.clone();
                    let signature = callback_signature.clone();
                    Box::pin(async move {
                        let input = binary_input(&signature.args, payload)?;
                        binary_output(
                            &signature.result,
                            terminal(ctx, input).await.map_err(|e| {
                                e.check_declared(&signature.errors, "terminal result")
                            })?,
                        )
                    })
                });
                let id = self.registry.insert_payload('q', callback)?;
                let result = self
                    .reactive_payload(
                        6,
                        0,
                        numeric(&id),
                        name,
                        Payload::message(args.encode(cordis_wire::MAX_MESSAGE).map_err(unknown)?),
                    )
                    .await;
                if !matches!(result, Err(Failure::Unknown(_))) {
                    self.registry.remove(&id);
                }
                return cordis_sdk_core::Message::from_bytes(
                    signature.result,
                    result?.value.one().map_err(unknown)?,
                )
                .map(Reply::Message)
                .map_err(unknown);
            }
            Operation::Next { next, args, result } => {
                let value = self
                    .reactive_payload(
                        7,
                        self.id(&next, Kind::Next)?,
                        0,
                        String::new(),
                        Payload::message(args.encode(cordis_wire::MAX_MESSAGE).map_err(unknown)?),
                    )
                    .await?
                    .value;
                return cordis_sdk_core::Message::from_bytes(result, value.one().map_err(unknown)?)
                    .map(Reply::Message)
                    .map_err(unknown);
            }
            Operation::Install { factory, config } => {
                let config = Payload::message(
                    config
                        .encode(cordis_wire::MAX_MESSAGE)
                        .map_err(|e| refused("invalid_argument", e))?,
                );
                if !self.bindings.factories.contains(&factory) {
                    return Err(refused("undeclared_factory", factory));
                }
                return Ok(Reply::Handle(
                    self.handle(
                        self.host
                            .call(Call::Composition(1, 0, factory, config))
                            .await?
                            .handle,
                        Kind::Child,
                    ),
                ));
            }
            Operation::ChildReady { child } => {
                self.host
                    .call(Call::Composition(
                        2,
                        self.id(&child, Kind::Child)?,
                        String::new(),
                        Value::Null.into(),
                    ))
                    .await?
            }
            Operation::ChildDispose { child } => {
                self.host
                    .call(Call::Composition(
                        4,
                        self.id(&child, Kind::Child)?,
                        String::new(),
                        Value::Null.into(),
                    ))
                    .await?
            }
            Operation::Provide { service, value } => {
                let value = Payload::message(
                    value
                        .encode(cordis_wire::MAX_MESSAGE)
                        .map_err(|e| refused("invalid_argument", e))?,
                );
                if !self.bindings.provisions.contains_key(&service) {
                    return Err(refused("undeclared_provision", service));
                }
                self.host
                    .call(Call::Composition(6, 0, service, value))
                    .await?
            }
        };
        Ok(Reply::Value(result.value.control_only().map_err(unknown)?))
    }
}
impl Backend for Abi {
    fn validation(&self) -> cordis_sdk_core::Validation {
        self.validation
    }
    fn submit(&self, operation: Operation) -> LocalFuture<Reply> {
        let backend = self.clone();
        Box::pin(async move {
            if !backend.frame.open.get() {
                return Err(refused("closed", "Context expired"));
            }
            if backend.cleanup
                && matches!(
                    operation,
                    Operation::Defer { .. }
                        | Operation::Acquire { .. }
                        | Operation::Effect { .. }
                        | Operation::Scope { .. }
                        | Operation::Spawn { .. }
                        | Operation::On { .. }
                        | Operation::Middleware { .. }
                        | Operation::Install { .. }
                        | Operation::Provide { .. }
                )
            {
                return Err(refused("closed", "cleanup cannot acquire"));
            }
            // Tokio task-local values are not inherited by spawn_local. Keep
            // the actual executing callback's identity with admitted work,
            // including work whose business waiter is subsequently cancelled.
            let caller = worker::CALLER
                .try_with(|caller| *caller)
                .map_err(|_| refused("closed", "ABI operation requires an executing callback"))?;
            let frame = backend.frame.clone();
            frame.pending.set(frame.pending.get() + 1);
            let (send, receive) = tokio::sync::oneshot::channel();
            tokio::task::spawn_local(async move {
                let result = std::panic::AssertUnwindSafe(
                    worker::CALLER.scope(caller, backend.execute(operation)),
                )
                .catch_unwind()
                .await
                .unwrap_or_else(|_| Err(unknown("adapter operation panicked")));
                frame.pending.set(frame.pending.get() - 1);
                frame.changed.notify_one();
                let _ = send.send(result);
            });
            receive.await.map_err(unknown)?
        })
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn binary_input(
    contract: &cordis_sdk_core::MessageContract,
    payload: Payload,
) -> Result<cordis_sdk_core::Message> {
    cordis_sdk_core::Message::from_bytes(
        contract.clone(),
        payload.one().map_err(|e| refused("invalid_argument", e))?,
    )
    .map_err(|e| refused("invalid_argument", e))
}
fn binary_output(
    contract: &cordis_sdk_core::MessageContract,
    value: cordis_sdk_core::Message,
) -> Result<Payload> {
    value.check(contract).map_err(unknown)?;
    Ok(Payload::message(
        value.encode(cordis_wire::MAX_MESSAGE).map_err(unknown)?,
    ))
}
