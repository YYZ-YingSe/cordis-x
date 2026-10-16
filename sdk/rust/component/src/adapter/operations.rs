use super::ComponentBackend;
use super::callbacks::WireCallback;
use cordis_component::{MAX_BYTES, Payload};
use cordis_sdk_core::typed::{Flow, violation};
use cordis_sdk_core::{
    Result, Value,
    backend::{Callback, Handle, Operation, Reply},
    json, refused,
};
use std::rc::Rc;

// Only this adapter chooses a string representation for host grants.
struct ComponentHandle(String);
struct ResourceHandle(String);
fn resource_grant(handle: &Handle) -> Result<&str> {
    handle
        .downcast_ref::<ResourceHandle>()
        .map(|h| h.0.as_str())
        .ok_or_else(|| refused("invalid_handle", "not a component resource"))
}
fn grant(handle: &Handle) -> Result<&str> {
    handle
        .downcast_ref::<ComponentHandle>()
        .map(|h| h.0.as_str())
        .ok_or_else(|| {
            refused(
                "invalid_handle",
                "capability belongs to a different backend",
            )
        })
}
fn handle(value: Value) -> Result<Reply> {
    value
        .as_str()
        .map(|s| Reply::Handle(Handle::new(ComponentHandle(s.into()))))
        .ok_or_else(|| refused("invalid_reply", "expected component grant"))
}

fn invalid(e: impl ToString) -> cordis_sdk_core::Failure {
    refused("invalid_argument", e.to_string())
}
fn binary_input(
    shape: &cordis_sdk_core::MessageContract,
    payload: &Payload,
) -> Result<cordis_sdk_core::Message> {
    cordis_sdk_core::Message::from_bytes(shape.clone(), payload.one().map_err(invalid)?)
        .map_err(invalid)
}
fn binary_output(
    shape: &cordis_sdk_core::MessageContract,
    value: cordis_sdk_core::Message,
) -> Result<Payload> {
    value.check(shape).map_err(violation)?;
    Ok(Payload::message(
        value.encode(MAX_BYTES).map_err(violation)?,
    ))
}
impl ComponentBackend {
    async fn register(&self, command: &str, mut args: Value, callback: Callback) -> Result<Reply> {
        // Publish before host admission, including acknowledgement loss.
        let id = self.callbacks.insert(callback);
        args["callback"] = json!(id);
        let result = self.request(command, args).await;
        self.callbacks.registration_result(id, &result);
        handle(result?)
    }
    async fn register_payload(
        &self,
        command: &str,
        mut args: Value,
        callback: WireCallback,
    ) -> Result<Reply> {
        let id = self.callbacks.insert_payload(callback);
        args["callback"] = json!(id);
        let result = self.request(command, args).await;
        self.callbacks.registration_result(id, &result);
        handle(result?)
    }
    async fn temporary(&self, command: &str, mut args: Value, callback: Callback) -> Result<Reply> {
        let id = self.callbacks.insert(callback);
        args["callback"] = json!(id);
        let result = self.request(command, args).await;
        if !matches!(result, Err(cordis_sdk_core::Failure::Unknown(_))) {
            self.callbacks.remove(id);
        }
        result.map(Reply::Value)
    }
    pub(super) async fn execute(&self, operation: Operation) -> Result<Reply> {
        use Operation::*;
        let (command, args) = match operation {
            Acquire {
                dependency,
                kind,
                args,
            } => {
                let args = args.encode(MAX_BYTES).map_err(invalid)?;
                let value = self
                    .request_payload(
                        "dependency-resource-acquire",
                        Payload::with_message(json!({"dependency":dependency,"kind":kind}), args),
                    )
                    .await?
                    .control_only()
                    .map_err(violation)?;
                return value
                    .as_str()
                    .filter(|s| {
                        s.len() == 32
                            && s.bytes()
                                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
                    })
                    .map(|s| Reply::Handle(Handle::new(ResourceHandle(s.into()))))
                    .ok_or_else(|| {
                        cordis_sdk_core::Failure::Unknown(cordis_sdk_core::ErrorInfo {
                            code: "invalid_reply".into(),
                            message: "missing acquired resource grant".into(),
                        })
                    });
            }
            ResourceInvoke {
                resource,
                method,
                args,
                result,
            } => {
                let args = args.encode(MAX_BYTES).map_err(invalid)?;
                let value = self
                    .request_payload(
                        "dependency-resource-invoke",
                        Payload::with_message(
                            json!({"grant":resource_grant(&resource)?,"method":method}),
                            args,
                        ),
                    )
                    .await?;
                return cordis_sdk_core::Message::from_bytes(
                    result,
                    value.one().map_err(violation)?,
                )
                .map(Reply::Message)
                .map_err(cordis_sdk_core::typed::violation);
            }
            ResourceDispose { resource } => (
                "dependency-resource-dispose",
                json!({"grant":resource_grant(&resource)?}),
            ),
            Info => ("info", Value::Null),
            Metadata { name, revision } => ("metadata", json!({"name":name,"revision":revision})),
            Invoke {
                dependency,
                method,
                args,
                result,
            } => {
                let args = args.encode(MAX_BYTES).map_err(invalid)?;
                let value = self
                    .request_payload(
                        "invoke",
                        Payload::with_message(
                            json!({"dependency":dependency,"method":method}),
                            args,
                        ),
                    )
                    .await?;
                return cordis_sdk_core::Message::from_bytes(
                    result,
                    value.one().map_err(violation)?,
                )
                .map(Reply::Message)
                .map_err(cordis_sdk_core::typed::violation);
            }
            Defer { label, cleanup } => {
                return self
                    .register("defer", json!({"label":label}), cleanup)
                    .await;
            }
            Effect { label, next } => {
                let callbacks = self.callbacks.clone();
                let next: Callback = Rc::new(move |ctx, _| {
                    let future = next(ctx);
                    let callbacks = callbacks.clone();
                    Box::pin(async move {
                        let (inverse, complete, failure) = future.await?.into_parts();
                        let inverse = inverse.map(|callback| callbacks.insert(callback));
                        Ok(json!({"inverse":inverse,"complete":complete,"failure":failure}))
                    })
                });
                return self.register("effect", json!({"label":label}), next).await;
            }
            Scope { label } => return handle(self.request("scope", json!({"label":label})).await?),
            Cancelled => ("cancelled", Value::Null),
            Sleep { milliseconds } => ("sleep", json!(milliseconds)),
            Spawn { label, task } => {
                return self.register("spawn", json!({"label":label}), task).await;
            }
            On {
                event,
                handler,
                signature,
            } => {
                let callback: WireCallback = Rc::new(move |ctx, payload| {
                    let handler = handler.clone();
                    let signature = signature.clone();
                    Box::pin(async move {
                        let args = binary_input(&signature.args, &payload)?;
                        let flow = handler(ctx, args)
                            .await
                            .map_err(|e| e.check_declared(&signature.errors, "listener"))?;
                        Ok(match flow {
                            Flow::Continue => json!({"kind":"continue"}).into(),
                            Flow::Break(value) => {
                                let mut result = binary_output(&signature.result, value)?;
                                result.control = json!({"kind":"break"});
                                result
                            }
                        })
                    })
                });
                return self
                    .register_payload("on", json!({"event":event}), callback)
                    .await;
            }
            Emit {
                event,
                args,
                serial,
                signature,
            } => {
                let value = self
                    .request_payload(
                        "emit",
                        Payload::with_message(
                            json!({"event":event,"serial":serial}),
                            args.encode(MAX_BYTES).map_err(invalid)?,
                        ),
                    )
                    .await?;
                let tags = value
                    .control
                    .as_array()
                    .ok_or_else(|| violation("expected event flow tags"))?;
                let mut bodies = value.messages.into_iter();
                let mut flows = Vec::with_capacity(tags.len());
                for tag in tags {
                    flows.push(match tag.as_str() {
                        Some("continue") => Flow::Continue,
                        Some("break") => Flow::Break(
                            cordis_sdk_core::Message::from_bytes(
                                signature.result.clone(),
                                bodies
                                    .next()
                                    .ok_or_else(|| violation("missing break body"))?,
                            )
                            .map_err(violation)?,
                        ),
                        _ => return Err(violation("invalid flow tag")),
                    });
                }
                if bodies.next().is_some() {
                    return Err(violation("unexpected event body"));
                }
                return Ok(Reply::Flows(flows));
            }
            Middleware {
                name,
                handler,
                signature,
            } => {
                let callback: WireCallback = Rc::new(move |ctx, payload| {
                    let handler = handler.clone();
                    let signature = signature.clone();
                    Box::pin(async move {
                        let next = payload.control["next"]
                            .as_str()
                            .ok_or_else(|| refused("invalid_callback", "missing next grant"))?;
                        let next = cordis_sdk_core::Next::from_handle(Handle::new(
                            ComponentHandle(next.into()),
                        ));
                        let input = binary_input(&signature.args, &payload)?;
                        let value = handler(ctx, input, next)
                            .await
                            .map_err(|e| e.check_declared(&signature.errors, "middleware"))?;
                        binary_output(&signature.result, value)
                    })
                });
                return self
                    .register_payload("middleware", json!({"name":name}), callback)
                    .await;
            }
            Run {
                name,
                args,
                terminal,
                signature,
            } => {
                let shape = signature.clone();
                let callback: WireCallback = Rc::new(move |ctx, payload| {
                    let terminal = terminal.clone();
                    let shape = shape.clone();
                    Box::pin(async move {
                        let input = binary_input(&shape.args, &payload)?;
                        let value = terminal(ctx, input)
                            .await
                            .map_err(|e| e.check_declared(&shape.errors, "terminal"))?;
                        binary_output(&shape.result, value)
                    })
                });
                let id = self.callbacks.insert_payload(callback);
                let args = Payload::with_message(
                    json!({"name":name,"callback":id}),
                    args.encode(MAX_BYTES).map_err(invalid)?,
                );
                let result = self.request_payload("run", args).await;
                if !matches!(result, Err(cordis_sdk_core::Failure::Unknown(_))) {
                    self.callbacks.remove(id);
                }
                return cordis_sdk_core::Message::from_bytes(
                    signature.result,
                    result?.one().map_err(violation)?,
                )
                .map(Reply::Message)
                .map_err(violation);
            }
            Install { factory, config } => {
                let config = config
                    .encode(MAX_BYTES)
                    .map_err(|e| refused("invalid_argument", e))?;
                return handle(
                    self.request_payload(
                        "install",
                        Payload::with_message(json!({"factory":factory}), config),
                    )
                    .await?
                    .control_only()
                    .map_err(violation)?,
                );
            }
            Provide { service, value } => {
                let value = value
                    .encode(MAX_BYTES)
                    .map_err(|e| refused("invalid_argument", e))?;
                return self
                    .request_payload(
                        "provide",
                        Payload::with_message(json!({"service":service}), value),
                    )
                    .await?
                    .control_only()
                    .map(Reply::Value)
                    .map_err(violation);
            }
            Dispose { effect } => ("dispose", json!({"grant":grant(&effect)?})),
            ScopeRun { scope, run } => {
                return self
                    .temporary("scope-run", json!({"grant":grant(&scope)?}), run)
                    .await;
            }
            ScopeDispose { scope } => ("scope-dispose", json!({"grant":grant(&scope)?})),
            TaskJoin { task } => ("join", json!({"grant":grant(&task)?})),
            Next { next, args, result } => {
                let args = args.encode(MAX_BYTES).map_err(invalid)?;
                let value = self
                    .request_payload(
                        "next",
                        Payload::with_message(json!({"grant":grant(&next)?}), args),
                    )
                    .await?;
                return cordis_sdk_core::Message::from_bytes(
                    result,
                    value.one().map_err(violation)?,
                )
                .map(Reply::Message)
                .map_err(violation);
            }
            SequenceReady { sequence } => ("effect-ready", json!({"grant":grant(&sequence)?})),
            SequenceDispose { sequence } => ("effect-dispose", json!({"grant":grant(&sequence)?})),
            ChildReady { child } => ("plugin-ready", json!({"grant":grant(&child)?})),
            ChildDispose { child } => ("plugin-dispose", json!({"grant":grant(&child)?})),
        };
        self.request(command, args).await.map(Reply::Value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn resource_capabilities_cannot_be_confused_with_other_component_handles() {
        assert!(resource_grant(&Handle::new(ComponentHandle("grant".into()))).is_err());
        assert!(grant(&Handle::new(ResourceHandle("grant".into()))).is_err());
    }
}
