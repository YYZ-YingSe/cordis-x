use super::{
    worker::{Frame, Instance},
    *,
};
use cordis_sdk_core::{
    LocalFuture, Next,
    backend::{Backend, Callback, Handle, Operation, Reply},
};
use futures_util::FutureExt;
use std::{
    any::Any,
    panic::AssertUnwindSafe,
    rc::{Rc, Weak},
    sync::{Arc, OnceLock},
    time::Duration,
};

#[derive(Clone)]
pub(crate) struct Native {
    pub ctx: rt::Context,
    pub instance: Weak<Instance>,
    pub frame: Rc<Frame>,
    pub next: Option<rt::Next<Payload, Payload>>,
}
#[derive(Clone)]
enum Kind {
    Resource(Rc<super::Resource>),
    Effect(rt::WeakEffectHandle, u64),
    Scope(rt::WeakScopeHandle),
    Task(rt::TaskHandle),
    Sequence(rt::WeakEffectSequence),
    Child(rt::WeakPluginHandle),
    Next(rt::Next<Payload, Payload>),
}
struct Capability {
    owner: Weak<Instance>,
    kind: Kind,
}
impl Native {
    fn handle(&self, kind: Kind) -> Handle {
        Handle::new(Capability {
            owner: self.instance.clone(),
            kind,
        })
    }
    fn capability(&self, handle: &Handle) -> Result<Kind> {
        let value = handle
            .downcast_ref::<Capability>()
            .ok_or_else(|| refused("invalid_handle", "foreign backend"))?;
        if !Weak::ptr_eq(&self.instance, &value.owner) {
            return Err(refused("invalid_handle", "foreign instance"));
        }
        Ok(value.kind.clone())
    }
    fn subscription(
        &self,
        id: u64,
        client: super::worker::Client,
        register: impl FnOnce() -> rt::Result<rt::EffectHandle>,
    ) -> Result<Handle> {
        let native = Arc::new(OnceLock::<rt::WeakEffectHandle>::new());
        let release = native.clone();
        // Register storage first: LIFO scope recovery unregisters and drains the
        // native handler before forgetting its SDK callback. Manual disposal
        // performs that same order while the owning scope is still open.
        let cleanup = self
            .ctx
            .defer("portable callback storage", move |_| async move {
                if let Some(effect) = release.get().and_then(rt::WeakEffectHandle::upgrade) {
                    effect.dispose().await?;
                }
                client.forget(id).await.map_err(native_error)
            })
            .map_err(unknown)?;
        let effect = register().map_err(rejected)?;
        let _ = native.set(effect.downgrade());
        Ok(self.handle(Kind::Effect(cleanup.downgrade(), id)))
    }
    async fn execute(self, op: Operation) -> Result<Reply> {
        let instance = self
            .instance
            .upgrade()
            .ok_or_else(|| refused("closed", "instance retired"))?;
        let client = instance.client()?;
        let ctx = &self.ctx;
        let value = match op {
            Operation::Acquire {
                dependency,
                kind,
                args,
            } => {
                let dependency = instance
                    .capabilities
                    .dependencies
                    .iter()
                    .find(|d| d.declaration.contract().name.as_ref() == dependency)
                    .ok_or_else(|| refused("undeclared_dependency", dependency))?;
                let acquire = dependency.acquire.as_ref().ok_or_else(|| {
                    refused("unsupported_resource", "dependency has no resource binding")
                })?;
                let resource = acquire(ctx.clone(), kind, args).await?;
                return Ok(Reply::Handle(
                    self.handle(Kind::Resource(Rc::new(resource))),
                ));
            }
            Operation::ResourceInvoke {
                resource,
                method,
                args,
                result,
            } => {
                let Kind::Resource(resource) = self.capability(&resource)? else {
                    return Err(refused("invalid_handle", "expected resource"));
                };
                let value = resource.invoke(&method, args).await?;
                value
                    .check(&result)
                    .map_err(cordis_sdk_core::typed::violation)?;
                return Ok(Reply::Message(value));
            }
            Operation::ResourceDispose { resource } => {
                let Kind::Resource(resource) = self.capability(&resource)? else {
                    return Err(refused("invalid_handle", "expected resource"));
                };
                resource.dispose().await?;
                Value::Null
            }
            Operation::Info => {
                json!({"definition":format!("{:?}",ctx.activation()),"owner":format!("{:?}",ctx.owner()),"caller":ctx.caller().map(|c|format!("{c:?}")),"cleanup":ctx.is_cleanup()})
            }
            Operation::Metadata { name, revision } => {
                let contract = rt::Service::<()>::new(name, revision);
                let permitted = (contract.contract().name.as_ref() == instance.primary.name
                    && contract.contract().revision.as_ref() == instance.primary.revision)
                    || instance
                        .capabilities
                        .dependencies
                        .iter()
                        .any(|d| d.declaration.contract() == contract.contract())
                    || instance
                        .capabilities
                        .provisions
                        .values()
                        .any(|p| p.contract() == contract.contract());
                if !permitted {
                    return Err(refused("undeclared_metadata", contract.contract()));
                }
                let layers = ctx.metadata::<_, Value>(&contract).map_err(rejected)?;
                json!({"revision":ctx.metadata_revision().map(|r| r.to_string()), "layers":layers.iter().map(|v|v.as_ref()).collect::<Vec<_>>()})
            }
            Operation::Invoke {
                dependency,
                method,
                args,
                result,
            } => {
                let dependency = instance
                    .capabilities
                    .dependencies
                    .iter()
                    .find(|d| d.declaration.contract().name.as_ref() == dependency)
                    .ok_or_else(|| refused("undeclared_dependency", dependency))?;
                let signature = cordis_contract::portable::method(
                    &dependency.descriptor.interface().methods,
                    &method,
                )
                .map_err(|e| refused("invalid_argument", e))?;
                args.check(&signature.args)
                    .map_err(|e| refused("invalid_argument", e))?;
                let value = (dependency.invoke)(ctx.clone(), method, args)
                    .await
                    .map_err(|e| e.check_declared(&signature.errors, "dependency result"))?;
                value
                    .check(&signature.result)
                    .map_err(cordis_sdk_core::typed::violation)?;
                value
                    .check(&result)
                    .map_err(cordis_sdk_core::typed::violation)?;
                return Ok(Reply::Message(value));
            }
            Operation::Defer { label, cleanup } => {
                let id = instance.insert(cleanup)?;
                let result = ctx.defer(label, move |ctx| async move {
                    client
                        .callback(ctx, id, Value::Null)
                        .await
                        .map_err(native_error)?;
                    client.forget(id).await.map_err(native_error)
                });
                let effect = match result {
                    Ok(h) => h,
                    Err(e) => {
                        instance.remove(id);
                        return Err(rejected(e));
                    }
                };
                return Ok(Reply::Handle(
                    self.handle(Kind::Effect(effect.downgrade(), id)),
                ));
            }
            Operation::Dispose { effect } => {
                let Kind::Effect(effect, id) = self.capability(&effect)? else {
                    return Err(refused("invalid_handle", "expected effect"));
                };
                effect
                    .upgrade()
                    .ok_or_else(|| refused("closed", "effect retired"))?
                    .dispose()
                    .await
                    .map_err(unknown)?;
                instance.remove(id);
                Value::Null
            }
            Operation::Scope { label } => {
                let scope = ctx.scope(label).map_err(rejected)?;
                return Ok(Reply::Handle(self.handle(Kind::Scope(scope.downgrade()))));
            }
            Operation::ScopeRun { scope, run } => {
                let Kind::Scope(scope) = self.capability(&scope)? else {
                    return Err(refused("invalid_handle", "expected scope"));
                };
                let scope = scope
                    .upgrade()
                    .ok_or_else(|| refused("closed", "scope retired"))?;
                let id = instance.insert(run)?;
                let result = scope
                    .run(move |ctx| async move { Ok(client.callback(ctx, id, Value::Null).await) })
                    .await;
                instance.remove(id);
                result.map_err(runtime_failure)??
            }
            Operation::ScopeDispose { scope } => {
                let Kind::Scope(scope) = self.capability(&scope)? else {
                    return Err(refused("invalid_handle", "expected scope"));
                };
                scope
                    .upgrade()
                    .ok_or_else(|| refused("closed", "scope retired"))?
                    .dispose()
                    .await
                    .map_err(unknown)?;
                Value::Null
            }
            Operation::Spawn { label, task } => {
                let id = instance.insert(task)?;
                let task = ctx.spawn(label, move |ctx| async move {
                    let result = client.callback(ctx, id, Value::Null).await;
                    client.forget(id).await.map_err(native_error)?;
                    result.map(|_| ()).map_err(native_error)
                });
                let task = match task {
                    Ok(t) => t,
                    Err(e) => {
                        instance.remove(id);
                        return Err(rejected(e));
                    }
                };
                return Ok(Reply::Handle(self.handle(Kind::Task(task))));
            }
            Operation::TaskJoin { task } => {
                let Kind::Task(task) = self.capability(&task)? else {
                    return Err(refused("invalid_handle", "expected task"));
                };
                task.join().await.map_err(unknown)?;
                Value::Null
            }
            Operation::Cancelled => {
                ctx.cancelled().await;
                Value::Null
            }
            Operation::Sleep { milliseconds } => {
                if milliseconds > 60_000 {
                    return Err(refused("invalid_argument", "sleep must be 0..60000 ms"));
                }
                tokio::select! { _ = tokio::time::sleep(Duration::from_millis(milliseconds)) => {}, _ = ctx.cancelled() => {} }
                Value::Null
            }
            Operation::Effect { label, next } => {
                let weak = self.instance.clone();
                let callback: Callback = Rc::new(move |ctx, _| {
                    let weak = weak.clone();
                    let next = next.clone();
                    Box::pin(async move {
                        // Admit cleanup storage before the producer can acquire anything.
                        let reserved = weak
                            .upgrade()
                            .ok_or_else(|| refused("closed", "instance retired"))?
                            .reserve()?;
                        let (inverse, complete, failure) = next(ctx).await?.into_parts();
                        let inverse = inverse.map(|inverse| reserved.commit(inverse));
                        Ok(json!({"inverse":inverse,"complete":complete,"failure":failure}))
                    })
                });
                let id = instance.insert(callback)?;
                let release = client.clone();
                let sequence = ctx.effect_steps_with_cleanup(
                    label,
                    move |ctx| {
                        let client = client.clone();
                        async move {
                            let value = match client.callback(ctx, id, Value::Null).await {
                                Ok(value) => value,
                                Err(e @ (Failure::Rejected(_) | Failure::Refused(_))) => {
                                    return Ok(rt::EffectStep::failed(native_error(e), None));
                                }
                                Err(e) => {
                                    let copy = e.clone();
                                    return Ok(rt::EffectStep::failed(
                                        native_error(e),
                                        Some(rt::Inverse::new(
                                            "unknown Rust step",
                                            move |_| async move { Err(native_error(copy)) },
                                        )),
                                    ));
                                }
                            };
                            let inverse = value["inverse"].as_u64().map(|inverse| {
                                rt::Inverse::new("Rust step inverse", move |ctx| async move {
                                    client
                                        .callback(ctx, inverse, Value::Null)
                                        .await
                                        .map_err(native_error)?;
                                    client.forget(inverse).await.map_err(native_error)
                                })
                            });
                            Ok(if let Some(failure) = value["failure"].as_str() {
                                rt::EffectStep::failed(rt::Error::Message(failure.into()), inverse)
                            } else if value["complete"] == true {
                                rt::EffectStep::complete(inverse)
                            } else {
                                rt::EffectStep::yielded(inverse)
                            })
                        }
                    },
                    rt::Inverse::new("Rust effect producer", move |_| async move {
                        release.forget(id).await.map_err(native_error)
                    }),
                );
                let sequence = match sequence {
                    Ok(s) => s,
                    Err(e) => {
                        instance.remove(id);
                        return Err(rejected(e));
                    }
                };
                return Ok(Reply::Handle(
                    self.handle(Kind::Sequence(sequence.downgrade())),
                ));
            }
            Operation::SequenceReady { sequence } => {
                let Kind::Sequence(sequence) = self.capability(&sequence)? else {
                    return Err(refused("invalid_handle", "expected sequence"));
                };
                json!(format!(
                    "{:?}",
                    sequence
                        .upgrade()
                        .ok_or_else(|| refused("closed", "sequence retired"))?
                        .ready()
                        .await
                        .map_err(unknown)?
                ))
            }
            Operation::SequenceDispose { sequence } => {
                let Kind::Sequence(sequence) = self.capability(&sequence)? else {
                    return Err(refused("invalid_handle", "expected sequence"));
                };
                sequence
                    .upgrade()
                    .ok_or_else(|| refused("closed", "sequence retired"))?
                    .dispose()
                    .await
                    .map_err(unknown)?;
                Value::Null
            }
            Operation::On {
                event,
                handler,
                signature: _,
            } => {
                let event = instance
                    .capabilities
                    .events
                    .get(&event)
                    .ok_or_else(|| refused("undeclared_event", event))?;
                let id = instance.insert_listener(handler)?;
                let cleanup = client.clone();
                let subscription = self.subscription(id, cleanup, || {
                    ctx.on(event, move |args, ctx| {
                        let client = client.clone();
                        async move {
                            let value =
                                client.listener(ctx, id, args).await.map_err(native_error)?;
                            Ok(match value {
                                cordis_sdk_core::typed::Flow::Break(value) => rt::Break(value),
                                cordis_sdk_core::typed::Flow::Continue => rt::Continue(()),
                            })
                        }
                    })
                });
                if subscription.is_err() {
                    instance.remove(id);
                }
                return subscription.map(Reply::Handle);
            }
            Operation::Emit {
                event,
                args,
                serial,
                signature: _,
            } => {
                let event = instance
                    .capabilities
                    .events
                    .get(&event)
                    .ok_or_else(|| refused("undeclared_event", event))?;
                let dispatch = if serial {
                    event.emit_serial(ctx, args)
                } else {
                    event.emit(ctx, args)
                }
                .map_err(rejected)?;
                return Ok(Reply::Flows(
                    dispatch
                        .join()
                        .await
                        .map_err(runtime_failure)?
                        .into_iter()
                        .map(|flow| match flow {
                            rt::Break(v) => cordis_sdk_core::typed::Flow::Break(v),
                            rt::Continue(()) => cordis_sdk_core::typed::Flow::Continue,
                        })
                        .collect(),
                ));
            }
            Operation::Middleware {
                name,
                handler,
                signature: _,
            } => {
                let chain = instance
                    .capabilities
                    .middleware
                    .get(&name)
                    .ok_or_else(|| refused("undeclared_middleware", name))?;
                // The worker transfers the actual native continuation.
                let id = instance.insert_message(Rc::new(move |ctx, args| {
                    let backend = ctx
                        .backend()
                        .as_any()
                        .downcast_ref::<Native>()
                        .expect("native context");
                    let next = backend.next.clone().expect("native continuation");
                    let next = Next::from_handle(backend.handle(Kind::Next(next)));
                    handler(ctx, args, next)
                }))?;
                let cleanup = client.clone();
                let subscription = self.subscription(id, cleanup, || {
                    ctx.middleware(chain, move |args, ctx, next| {
                        let client = client.clone();
                        async move {
                            client
                                .middleware(ctx, id, args, next)
                                .await
                                .map_err(native_error)
                        }
                    })
                });
                if subscription.is_err() {
                    instance.remove(id);
                }
                return subscription.map(Reply::Handle);
            }
            Operation::Next { next, args, result } => {
                let Kind::Next(next) = self.capability(&next)? else {
                    return Err(refused("invalid_handle", "expected next"));
                };
                let value = next.run(args).await.map_err(runtime_failure)?;
                value
                    .check(&result)
                    .map_err(cordis_sdk_core::typed::violation)?;
                return Ok(Reply::Message(value));
            }
            Operation::Run {
                name,
                args,
                terminal,
                signature,
            } => {
                let chain = instance
                    .capabilities
                    .middleware
                    .get(&name)
                    .ok_or_else(|| refused("undeclared_middleware", name))?
                    .clone();
                let id = instance.insert_message(terminal)?;
                let result = chain
                    .run(ctx, args, move |args, ctx| {
                        let client = client.clone();
                        async move {
                            client
                                .message_callback(ctx, id, args)
                                .await
                                .map_err(native_error)
                        }
                    })
                    .await;
                instance.remove(id);
                let value = result.map_err(runtime_failure)?;
                value
                    .check(&signature.result)
                    .map_err(cordis_sdk_core::typed::violation)?;
                return Ok(Reply::Message(value));
            }
            Operation::Install { factory, config } => {
                let factory = instance
                    .capabilities
                    .factories
                    .get(&factory)
                    .ok_or_else(|| refused("undeclared_factory", factory))?
                    .clone();
                let context = ctx.clone();
                let child =
                    tokio::task::spawn_blocking(move || context.install_with(|| factory(config)))
                        .await
                        .map_err(unknown)?
                        .map_err(|error| match error {
                            rt::Error::Closed => {
                                refused("closed", "context cannot install children")
                            }
                            other => runtime_failure(other),
                        })?;
                return Ok(Reply::Handle(self.handle(Kind::Child(child.downgrade()))));
            }
            Operation::ChildReady { child } => {
                let Kind::Child(child) = self.capability(&child)? else {
                    return Err(refused("invalid_handle", "expected child"));
                };
                child
                    .upgrade()
                    .ok_or_else(|| refused("closed", "child retired"))?
                    .ready()
                    .await
                    .map_err(unknown)?;
                Value::Null
            }
            Operation::ChildDispose { child } => {
                let Kind::Child(child) = self.capability(&child)? else {
                    return Err(refused("invalid_handle", "expected child"));
                };
                child
                    .upgrade()
                    .ok_or_else(|| refused("closed", "child retired"))?
                    .dispose()
                    .await
                    .map_err(unknown)?;
                Value::Null
            }
            Operation::Provide { service, value } => {
                let service = instance
                    .capabilities
                    .provisions
                    .get(&service)
                    .ok_or_else(|| refused("undeclared_provision", service))?;
                ctx.provide(service, Arc::new(value)).map_err(rejected)?;
                Value::Null
            }
        };
        Ok(Reply::Value(value))
    }
}
impl Backend for Native {
    fn validation(&self) -> cordis_sdk_core::Validation {
        self.instance
            .upgrade()
            .map_or(cordis_sdk_core::Validation::Checked, |instance| {
                instance.capabilities.validation
            })
    }
    fn submit(&self, operation: Operation) -> LocalFuture<Reply> {
        let native = self.clone();
        Box::pin(async move {
            if !native.frame.open.get() {
                return Err(refused("closed", "callback authority expired"));
            }
            let frame = native.frame.clone();
            frame.pending.set(frame.pending.get() + 1);
            let (sender, receiver) = tokio::sync::oneshot::channel();
            tokio::task::spawn_local(async move {
                let result = AssertUnwindSafe(native.execute(operation))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| Err(unknown("adapter operation panicked")));
                frame.pending.set(frame.pending.get() - 1);
                frame.changed.notify_one();
                let _ = sender.send(result);
            });
            receiver.await.map_err(unknown)?
        })
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}
