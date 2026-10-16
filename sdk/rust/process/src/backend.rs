use super::{
    callbacks::{Frame, Registry, WireCallback, control_callback},
    channel::Channel,
    *,
};
use cordis_sdk_core::{
    LocalFuture, Next,
    backend::{Backend, Callback, Handle, Operation, Reply},
};
use futures_util::FutureExt;
use std::{any::Any, rc::Weak, time::Duration};

#[derive(Clone)]
pub struct Process {
    pub channel: Rc<Channel>,
    pub registry: Rc<Registry>,
    pub frame: Rc<Frame>,
    pub token: String,
    pub cleanup: bool,
    pub dependencies: Rc<BTreeMap<String, String>>,
    pub bindings: Rc<cordis_sdk_core::Bindings>,
}
struct Capability {
    owner: Weak<Registry>,
    key: &'static str,
    id: String,
    grant: String,
}
impl Process {
    fn resource_capability(&self) -> Result<()> {
        if self.channel.dependency_resources.get() {
            Ok(())
        } else {
            Err(refused(
                "unsupported_resource",
                "dependency resource capability not negotiated",
            ))
        }
    }
    fn capability(&self, reply: Value, key: &'static str, id: Option<&str>) -> Result<Handle> {
        let value = text(&reply, key).map_err(unknown)?;
        let grant = text(&reply, "grant").map_err(unknown)?;
        if id.is_some_and(|id| id != value)
            || grant.len() != 32
            || !grant
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(unknown("invalid capability acknowledgement"));
        }
        Ok(Handle::new(Capability {
            owner: Rc::downgrade(&self.registry),
            key,
            id: value.into(),
            grant: grant.into(),
        }))
    }
    pub fn next(&self, payload: &Value) -> Result<Next> {
        let reply = json!({"call":text(payload,"call")?,"grant":text(payload,"next")?});
        Ok(Next::from_handle(self.capability(reply, "call", None)?))
    }
    fn args(&self, handle: &Handle, key: &'static str) -> Result<Value> {
        let cap = handle
            .downcast_ref::<Capability>()
            .ok_or_else(|| refused("invalid_handle", "foreign backend"))?;
        if cap.key != key || !Weak::ptr_eq(&cap.owner, &Rc::downgrade(&self.registry)) {
            return Err(refused("invalid_handle", "foreign capability"));
        }
        let mut args = json!({"grant":cap.grant});
        args[key] = json!(cap.id);
        Ok(args)
    }
    async fn register(
        &self,
        command: &str,
        key: &'static str,
        prefix: char,
        args: Value,
        callback: Callback,
    ) -> Result<Reply> {
        self.register_payload(command, key, prefix, args, control_callback(callback))
            .await
    }
    async fn register_payload(
        &self,
        command: &str,
        key: &'static str,
        prefix: char,
        mut args: Value,
        callback: WireCallback,
    ) -> Result<Reply> {
        let id = self.registry.insert_payload(prefix, callback)?;
        args[key] = json!(id);
        args["context"] = json!(self.token);
        let result = self.channel.request(command, args).await;
        if matches!(result, Err(Failure::Rejected(_) | Failure::Refused(_))) {
            self.registry.remove(&id);
        }
        Ok(Reply::Handle(self.capability(result?, key, Some(&id))?))
    }
    async fn execute(&self, operation: Operation) -> Result<Reply> {
        let (command, mut args) = match operation {
            Operation::Acquire {
                dependency,
                kind,
                args,
            } => {
                self.resource_capability()?;
                if !self.dependencies.contains_key(&dependency) {
                    return Err(refused("undeclared_dependency", dependency));
                }
                let args = args
                    .encode(cordis_wire::MAX_MESSAGE)
                    .map_err(|e| refused("invalid_argument", e))?;
                let id = self.registry.id('r')?;
                let reply = self.channel.request_payload("dependency_resource_acquire", Payload::with_message(json!({"context":self.token,"dependency":dependency,"kind":kind,"resource":id}), args)).await?.control_only().map_err(unknown)?;
                return Ok(Reply::Handle(self.capability(
                    operation_outcome(reply)?,
                    "resource",
                    Some(&id),
                )?));
            }
            Operation::ResourceInvoke {
                resource,
                method,
                args,
                result,
            } => {
                self.resource_capability()?;
                let mut payload = self.args(&resource, "resource")?;
                payload["context"] = json!(self.token);
                payload["method"] = json!(method);
                let bytes = args
                    .encode(cordis_wire::MAX_MESSAGE)
                    .map_err(|e| refused("invalid_argument", e))?;
                let command = if self.cleanup {
                    "cleanup_resource_call"
                } else {
                    "dependency_resource_call"
                };
                let value = self
                    .channel
                    .request_payload(command, Payload::with_message(payload, bytes))
                    .await?;
                operation_outcome(value.control.clone())?;
                return cordis_sdk_core::Message::from_bytes(result, value.one().map_err(unknown)?)
                    .map(Reply::Message)
                    .map_err(unknown);
            }
            Operation::ResourceDispose { resource } => {
                self.resource_capability()?;
                let mut payload = self.args(&resource, "resource")?;
                payload["context"] = json!(self.token);
                return operation_outcome(
                    self.channel
                        .request("dependency_resource_dispose", payload)
                        .await?,
                )
                .map(Reply::Value);
            }
            Operation::Info => return Ok(Reply::Value(json!({"cleanup":self.cleanup}))),
            Operation::Metadata { name, revision } => (
                if self.cleanup {
                    "cleanup_metadata"
                } else {
                    "context_metadata"
                },
                json!({"name":name,"revision":revision}),
            ),
            Operation::Invoke {
                dependency,
                method,
                args,
                result,
            } => {
                if !self.dependencies.contains_key(&dependency) {
                    return Err(refused("undeclared_dependency", dependency));
                }
                let args = args
                    .encode(cordis_wire::MAX_MESSAGE)
                    .map_err(|e| refused("invalid_argument", e))?;
                let command = if self.cleanup {
                    "cleanup_call"
                } else {
                    "dependency_call"
                };
                let value = self
                    .channel
                    .request_payload(
                        command,
                        Payload::with_message(
                            json!({"context":self.token,"dependency":dependency,"method":method}),
                            args,
                        ),
                    )
                    .await?;
                return cordis_sdk_core::Message::from_bytes(result, value.one().map_err(unknown)?)
                    .map(Reply::Message)
                    .map_err(unknown);
            }
            Operation::Defer { label, cleanup } => {
                return self
                    .register(
                        "effect_register",
                        "effect",
                        'e',
                        json!({"label":label}),
                        cleanup,
                    )
                    .await;
            }
            Operation::Dispose { effect } => {
                let cap = effect
                    .downcast_ref::<Capability>()
                    .ok_or_else(|| refused("invalid_handle", "foreign effect"))?;
                let command = match cap.key {
                    "effect" => "effect_dispose",
                    "listener" => "event_dispose",
                    "handler" => "middleware_dispose",
                    _ => return Err(refused("invalid_handle", "expected effect")),
                };
                (command, self.args(&effect, cap.key)?)
            }
            Operation::Scope { label } => {
                let id = self.registry.id('c')?;
                let result = self
                    .channel
                    .request(
                        "scope_register",
                        json!({"context":self.token,"scope":id,"label":label}),
                    )
                    .await?;
                return Ok(Reply::Handle(self.capability(
                    result,
                    "scope",
                    Some(&id),
                )?));
            }
            Operation::ScopeRun { scope, run } => {
                let mut args = self.args(&scope, "scope")?;
                let id = self.registry.insert('u', run)?;
                args["run"] = json!(id);
                let result = self.channel.request("scope_run", args).await;
                if !matches!(result, Err(Failure::Unknown(_))) {
                    self.registry.remove(&id);
                }
                return result.map(Reply::Value);
            }
            Operation::ScopeDispose { scope } => ("scope_dispose", self.args(&scope, "scope")?),
            Operation::Spawn { label, task } => {
                return self
                    .register("task_register", "task", 't', json!({"label":label}), task)
                    .await;
            }
            Operation::TaskJoin { task } => ("task_join", self.args(&task, "task")?),
            Operation::Cancelled => {
                let mut cancel = self
                    .frame
                    .cancellation
                    .clone()
                    .ok_or_else(|| refused("closed", "not a task context"))?;
                while !*cancel.borrow_and_update() {
                    cancel.changed().await.map_err(unknown)?;
                }
                return Ok(Reply::Value(Value::Null));
            }
            Operation::Sleep { milliseconds } => {
                if milliseconds > 60_000 {
                    return Err(refused("invalid_argument", "sleep must be 0..60000 ms"));
                }
                let delay = tokio::time::sleep(Duration::from_millis(milliseconds));
                if let Some(mut cancel) = self.frame.cancellation.clone() {
                    tokio::select! { _=delay=>{}, _=async { while !*cancel.borrow_and_update() { if cancel.changed().await.is_err() {break;} } }=>{} }
                } else {
                    delay.await;
                }
                return Ok(Reply::Value(Value::Null));
            }
            Operation::Effect { label, next } => {
                let id = self.registry.sequence(next)?;
                let result = self
                    .channel
                    .request(
                        "sequence_register",
                        json!({"context":self.token,"sequence":id,"label":label}),
                    )
                    .await;
                if matches!(result, Err(Failure::Rejected(_) | Failure::Refused(_))) {
                    self.registry.remove_sequence(&id);
                }
                return Ok(Reply::Handle(self.capability(
                    result?,
                    "sequence",
                    Some(&id),
                )?));
            }
            Operation::SequenceReady { sequence } => {
                let reply = self
                    .channel
                    .request("sequence_ready", self.args(&sequence, "sequence")?)
                    .await?;
                let completion = match text(&reply, "completion")? {
                    "completed" => "Completed",
                    "stopped" => "Stopped",
                    _ => return Err(unknown("invalid sequence completion")),
                };
                return Ok(Reply::Value(json!(completion)));
            }
            Operation::SequenceDispose { sequence } => {
                ("sequence_dispose", self.args(&sequence, "sequence")?)
            }
            Operation::On {
                event,
                handler,
                signature,
            } => {
                if !self.bindings.events.contains_key(&event) {
                    return Err(refused("undeclared_event", event));
                }
                let callback: WireCallback = Rc::new(move |ctx, payload| {
                    let handler = handler.clone();
                    let signature = signature.clone();
                    Box::pin(async move {
                        let input = cordis_sdk_core::Message::from_bytes(
                            signature.args.clone(),
                            payload.one().map_err(unknown)?,
                        )
                        .map_err(|e| refused("invalid_argument", e))?;
                        let flow = handler(ctx, input)
                            .await
                            .map_err(|e| e.check_declared(&signature.errors, "event errors"))?;
                        Ok(match flow {
                            cordis_sdk_core::typed::Flow::Continue => {
                                json!({"kind":"continue"}).into()
                            }
                            cordis_sdk_core::typed::Flow::Break(value) => {
                                value.check(&signature.result).map_err(unknown)?;
                                Payload::with_message(
                                    json!({"kind":"break"}),
                                    value.encode(cordis_wire::MAX_MESSAGE).map_err(unknown)?,
                                )
                            }
                        })
                    })
                });
                return self
                    .register_payload(
                        "event_register",
                        "listener",
                        'l',
                        json!({"event":event}),
                        callback,
                    )
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
                let id = self.registry.id('d')?;
                let input = Payload::with_message(
                    json!({"context":self.token,"event":event,"mode":if serial {"serial"}else{"parallel"},"dispatch":id}),
                    args.encode(cordis_wire::MAX_MESSAGE)
                        .map_err(|e| refused("invalid_argument", e))?,
                );
                let reply = self
                    .channel
                    .request_payload("event_emit", input)
                    .await?
                    .control_only()
                    .map_err(unknown)?;
                let cap = self.capability(reply, "dispatch", Some(&id))?;
                let result = self
                    .channel
                    .request_payload("event_join", self.args(&cap, "dispatch")?.into())
                    .await?;
                if result.control["kind"] != "success" {
                    if matches!(
                        result.control["kind"].as_str(),
                        Some("rejected" | "refused" | "unknown")
                    ) {
                        return operation_outcome(json!({"status":result.control["kind"],"error":result.control["error"]})).map(Reply::Value);
                    }
                    return Err(unknown(format!(
                        "event dispatch failed: {}",
                        result.control
                    )));
                }
                let tags = result.control["flows"]
                    .as_array()
                    .ok_or_else(|| unknown("missing event flow tags"))?;
                if serial && tags.len() != 1 {
                    return Err(unknown("invalid serial event result"));
                }
                let mut bodies = result.messages.into_iter();
                let mut flows = Vec::with_capacity(tags.len());
                for tag in tags {
                    flows.push(match tag.as_str() {
                        Some("continue") => cordis_sdk_core::typed::Flow::Continue,
                        Some("break") => cordis_sdk_core::typed::Flow::Break(
                            cordis_sdk_core::Message::from_bytes(
                                signature.result.clone(),
                                bodies.next().ok_or_else(|| unknown("missing break body"))?,
                            )
                            .map_err(unknown)?,
                        ),
                        _ => return Err(unknown("invalid event flow tag")),
                    });
                }
                if bodies.next().is_some() {
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
                let callback: WireCallback = Rc::new(move |ctx, payload| {
                    let handler = handler.clone();
                    let signature = signature.clone();
                    Box::pin(async move {
                        let backend = ctx
                            .backend()
                            .as_any()
                            .downcast_ref::<Process>()
                            .expect("process context");
                        let next = backend.next(&payload.control)?;
                        let input = cordis_sdk_core::Message::from_bytes(
                            signature.args.clone(),
                            payload.one().map_err(unknown)?,
                        )
                        .map_err(|e| refused("invalid_argument", e))?;
                        let value = handler(ctx, input, next).await.map_err(|e| {
                            e.check_declared(&signature.errors, "middleware errors")
                        })?;
                        value.check(&signature.result).map_err(unknown)?;
                        Ok(Payload::message(
                            value.encode(cordis_wire::MAX_MESSAGE).map_err(unknown)?,
                        ))
                    })
                });
                return self
                    .register_payload(
                        "middleware_register",
                        "handler",
                        'm',
                        json!({"middleware":name}),
                        callback,
                    )
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
                let shape = signature.clone();
                let callback: WireCallback = Rc::new(move |ctx, payload| {
                    let terminal = terminal.clone();
                    let shape = shape.clone();
                    Box::pin(async move {
                        let input = cordis_sdk_core::Message::from_bytes(
                            shape.args.clone(),
                            payload.one().map_err(unknown)?,
                        )
                        .map_err(|e| refused("invalid_argument", e))?;
                        let value = terminal(ctx, input)
                            .await
                            .map_err(|e| e.check_declared(&shape.errors, "terminal errors"))?;
                        value.check(&shape.result).map_err(unknown)?;
                        Ok(Payload::message(
                            value.encode(cordis_wire::MAX_MESSAGE).map_err(unknown)?,
                        ))
                    })
                });
                let id = self.registry.insert_payload('q', callback)?;
                let input = Payload::with_message(
                    json!({"context":self.token,"middleware":name,"run":id}),
                    args.encode(cordis_wire::MAX_MESSAGE)
                        .map_err(|e| refused("invalid_argument", e))?,
                );
                let result = self.channel.request_payload("middleware_run", input).await;
                if !matches!(result, Err(Failure::Unknown(_))) {
                    self.registry.remove(&id);
                }
                return cordis_sdk_core::Message::from_bytes(
                    signature.result,
                    result?.one().map_err(unknown)?,
                )
                .map(Reply::Message)
                .map_err(unknown);
            }
            Operation::Next { next, args, result } => {
                let mut control = self.args(&next, "call")?;
                control["context"] = json!(self.token);
                let input = Payload::with_message(
                    control,
                    args.encode(cordis_wire::MAX_MESSAGE)
                        .map_err(|e| refused("invalid_argument", e))?,
                );
                let value = self
                    .channel
                    .request_payload("middleware_next", input)
                    .await?;
                return cordis_sdk_core::Message::from_bytes(result, value.one().map_err(unknown)?)
                    .map(Reply::Message)
                    .map_err(unknown);
            }
            Operation::Install { factory, config } => {
                let config = config
                    .encode(cordis_wire::MAX_MESSAGE)
                    .map_err(|e| refused("invalid_argument", e))?;
                if !self.bindings.factories.contains(&factory) {
                    return Err(refused("undeclared_factory", factory));
                }
                let reply = self
                    .channel
                    .request_payload(
                        "child_install",
                        Payload::with_message(
                            json!({"context":self.token,"factory":factory}),
                            config,
                        ),
                    )
                    .await?
                    .control_only()
                    .map_err(unknown)?;
                return Ok(Reply::Handle(self.capability(
                    operation_outcome(reply)?,
                    "child",
                    None,
                )?));
            }
            Operation::ChildReady { child } => ("child_ready", self.args(&child, "child")?),
            Operation::ChildDispose { child } => ("child_dispose", self.args(&child, "child")?),
            Operation::Provide { service, value } => {
                let value = value
                    .encode(cordis_wire::MAX_MESSAGE)
                    .map_err(|e| refused("invalid_argument", e))?;
                if !self.bindings.provisions.contains_key(&service) {
                    return Err(refused("undeclared_provision", service));
                }
                return self
                    .channel
                    .request_payload(
                        "value_provide",
                        Payload::with_message(
                            json!({"context":self.token,"service":service}),
                            value,
                        ),
                    )
                    .await?
                    .control_only()
                    .map(Reply::Value)
                    .map_err(unknown);
            }
        };
        args["context"] = json!(self.token);
        self.channel.request(command, args).await.map(Reply::Value)
    }
}
fn operation_outcome(value: Value) -> Result<Value> {
    match value["status"].as_str() {
        Some("ok")
            if value
                .as_object()
                .is_some_and(|o| o.len() == 2 && o.contains_key("value")) =>
        {
            Ok(value["value"].clone())
        }
        Some("rejected" | "refused" | "unknown")
            if value.as_object().is_some_and(|o| o.len() == 2)
                && value["error"].as_object().is_some_and(|o| o.len() == 2) =>
        {
            let info = ErrorInfo {
                code: text(&value["error"], "code").map_err(unknown)?.into(),
                message: text(&value["error"], "message").map_err(unknown)?.into(),
            };
            Err(if value["status"] == "rejected" {
                Failure::Rejected(info)
            } else if value["status"] == "refused" {
                Failure::Refused(info)
            } else {
                Failure::Unknown(info)
            })
        }
        _ => Err(unknown("malformed operation outcome")),
    }
}
impl Backend for Process {
    fn validation(&self) -> cordis_sdk_core::Validation {
        self.channel.validation.get()
    }
    fn submit(&self, operation: Operation) -> LocalFuture<Reply> {
        let backend = self.clone();
        Box::pin(async move {
            if !backend.frame.open.get() || backend.token.is_empty() {
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
            let frame = backend.frame.clone();
            frame.pending.set(frame.pending.get() + 1);
            let (send, receive) = tokio::sync::oneshot::channel();
            tokio::task::spawn_local(async move {
                let result = std::panic::AssertUnwindSafe(backend.execute(operation))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_acknowledgements_preserve_uncertainty() {
        assert_eq!(
            operation_outcome(json!({"status":"ok","value":null})).unwrap(),
            Value::Null
        );
        assert!(matches!(
            operation_outcome(
                json!({"status":"rejected","error":{"code":"factory","message":"no effect"}})
            ),
            Err(Failure::Rejected(_) | Failure::Refused(_))
        ));
        assert!(matches!(
            operation_outcome(
                json!({"status":"unknown","error":{"code":"lost","message":"uncertain"}})
            ),
            Err(Failure::Unknown(_))
        ));
        for malformed in [
            Value::Null,
            json!({"status":"ok"}),
            json!({"status":"rejected","error":{"code":"missing-message"}}),
            json!({"status":"rejected","error":{"code":"x","message":"x","extra":true}}),
            json!({"status":"ok","value":null,"extra":true}),
        ] {
            assert!(matches!(
                operation_outcome(malformed),
                Err(Failure::Unknown(_))
            ));
        }
    }
}
