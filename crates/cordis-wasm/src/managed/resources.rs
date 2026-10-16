use super::*;
use cordis_runtime::{EffectSnapshot, Resource, WeakResource};
use tokio::sync::OwnedSemaphorePermit;

// Preserve foreign uncertainty when a reverse factory fails through Runtime.
pub(super) fn uncertainty(error: Error) -> Error {
    match error {
        Error::Runtime(message) => unknown(message),
        e => e,
    }
}
fn safe_rejection(error: &Error) -> bool {
    matches!(
        error,
        Error::Rejected { .. }
            | Error::Refused { .. }
            | Error::Closed
            | Error::InvalidInput(_)
            | Error::Overloaded
    )
}
struct Ticket {
    kind: String,
    instance: Arc<Instance>,
    acquired: Result<u64>,
    owner: String,
    slot: Mutex<Option<OwnedSemaphorePermit>>,
    released: Arc<Mutex<Option<Result<()>>>>,
}
/// A managed component resource whose inverse belongs to the acquiring scope.
#[derive(Clone)]
pub struct ContextResource {
    resource: Resource<Ticket>,
    released: Arc<Mutex<Option<Result<()>>>>,
}
impl ContextService {
    pub async fn acquire_json(
        &self,
        ctx: &Context,
        kind: &str,
        args: Value,
    ) -> Result<ContextResource> {
        let schema = contract::resource(self.instance.module.descriptor(), kind)
            .map_err(Error::InvalidInput)?;
        self.acquire(ctx, kind, crate::json_body(&schema.args, &args)?)
            .await
    }
    pub async fn acquire(&self, ctx: &Context, kind: &str, args: Bytes) -> Result<ContextResource> {
        self.acquire_envelope(ctx, kind, Payload::message(args).into())
            .await
    }
    pub(super) async fn acquire_envelope(
        &self,
        ctx: &Context,
        kind: &str,
        mut args: Envelope,
    ) -> Result<ContextResource> {
        if ctx.activation() != self.activation {
            return Err(Error::Closed);
        }
        let factory = contract::resource(self.instance.module.descriptor(), kind)
            .map_err(Error::InvalidInput)?;
        args.check(&factory.args).map_err(Error::InvalidInput)?;
        if kind.len() > cordis_component::MAX_BYTES {
            return Err(Error::InvalidInput("kind too long".into()));
        }
        if ctx.is_cleanup() {
            return Err(refused(
                "cleanup_resource",
                "cleanup cannot acquire resources",
            ));
        }
        let instance = self.instance.clone();
        instance.check()?;
        let slot = instance
            .resource_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        let permit = instance
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        let owner = router::token()?;
        let kind = kind.to_owned();
        ctx.operation(move |ctx| async move {
            let _permit = permit;
            Ok(Self::acquire_owned(instance, ctx, kind, args, owner, slot).await)
        })
        .await?
    }
    async fn acquire_owned(
        instance: Arc<Instance>,
        ctx: Context,
        kind: String,
        args: Envelope,
        owner: String,
        slot: OwnedSemaphorePermit,
    ) -> Result<ContextResource> {
        let definition = ctx.clone();
        let (send, receive) = tokio::sync::oneshot::channel();
        let released = Arc::new(Mutex::new(None));
        let observed = released.clone();
        let resource = ctx
            .acquire(
                format!("managed Wasm resource: {kind}"),
                async move {
                    let acquired = instance
                        .call_envelope(
                            &definition,
                            "resource-acquire",
                            args.with_control(crate::json!({"owner":owner,"kind":kind})),
                        )
                        .await
                        .and_then(|value| {
                            value
                                .into_wire()
                                .control_only()
                                .map_err(unknown)?
                                .as_u64()
                                .filter(|n| *n > 0)
                                .ok_or_else(|| unknown("invalid acquired resource ID"))
                        });
                    if let Err(error @ Error::Unknown(_)) = &acquired {
                        instance.fault.lock().unwrap().get_or_insert(error.clone());
                        let _ = definition.fail(error.to_string());
                    }
                    let _ = send.send(acquired.clone());
                    Ok(Ticket {
                        instance,
                        acquired,
                        kind,
                        owner,
                        slot: Mutex::new(Some(slot)),
                        released,
                    })
                },
                |ticket, cleanup| async move {
                    let result: Result<()> = async {
                        let id = match &ticket.acquired {
                            Ok(id) => *id,
                            Err(e) if safe_rejection(e) => {
                                ticket.slot.lock().unwrap().take();
                                return Ok(());
                            }
                            Err(e) => return Err(e.clone()),
                        };
                        let result = ticket
                            .instance
                            .call(
                                &cleanup,
                                "resource-release",
                                crate::json!({"owner":ticket.owner,"resource":id}),
                            )
                            .await?;
                        if !result.is_null() {
                            let error = unknown("invalid resource release acknowledgement");
                            ticket
                                .instance
                                .fault
                                .lock()
                                .unwrap()
                                .get_or_insert(error.clone());
                            let _ = cleanup.fail(error.to_string());
                            return Err(error);
                        }
                        ticket.slot.lock().unwrap().take();
                        Ok(())
                    }
                    .await;
                    *ticket.released.lock().unwrap() = Some(result.clone());
                    result.map_err(Into::into)
                },
            )
            .await?;
        let outcome = receive.await.map_err(unknown)?;
        if let Err(error) = outcome {
            if safe_rejection(&error) {
                resource.dispose().await?;
            }
            return Err(error);
        }
        Ok(ContextResource {
            resource,
            released: observed,
        })
    }
}
impl ContextResource {
    pub async fn invoke_json(&self, method: &str, args: Value) -> Result<Value> {
        let signature = self
            .resource
            .with(|ticket| {
                let schema = contract::resource(ticket.instance.module.descriptor(), &ticket.kind)
                    .map_err(Error::InvalidInput)?;
                contract::method(&schema.methods, method)
                    .cloned()
                    .map_err(Error::InvalidInput)
            })
            .map_err(Error::from)??;
        let bytes = self
            .invoke(method, crate::json_body(&signature.args, &args)?)
            .await?;
        crate::body_json(&signature.result, &bytes)
    }

    pub async fn invoke(&self, method: &str, args: Bytes) -> Result<Bytes> {
        if method.len() > cordis_component::MAX_BYTES {
            return Err(Error::InvalidInput("method too long".into()));
        }
        let method = method.to_owned();
        self.resource
            .call(move |ticket, ctx| {
                Box::pin(async move {
                    let instance = &ticket.instance;
                    let signature = (|| {
                        let factory =
                            contract::resource(instance.module.descriptor(), &ticket.kind)
                                .map_err(Error::InvalidInput)?;
                        let signature = contract::method(&factory.methods, &method)
                            .map_err(Error::InvalidInput)?;
                        signature
                            .args
                            .check_binary(&args)
                            .map_err(Error::InvalidInput)?;
                        Ok::<_, Error>(signature.clone())
                    })();
                    let signature = match signature {
                        Ok(value) => value,
                        Err(error) => return Ok(Err(error)),
                    };
                    let permit = if ctx.is_cleanup() {
                        None
                    } else {
                        match instance.capacity.clone().try_acquire_owned() {
                            Ok(permit) => Some(permit),
                            Err(_) => return Ok(Err(Error::Overloaded)),
                        }
                    };
                    let request = crate::json!({
                        "resource": ticket.acquired.as_ref().expect("confirmed resource"),
                        "owner": ticket.owner, "method": method,
                    });
                    let result = instance
                        .call_payload(
                            &ctx,
                            "resource-invoke",
                            Payload::with_message(request, args),
                        )
                        .await
                        .and_then(|value| value.one().map_err(unknown));
                    let result = result
                        .map_err(|e| e.check_declared(&signature.errors, "resource errors"))
                        .and_then(|value| {
                            signature.result.check_binary(&value).map_err(unknown)?;
                            Ok(value)
                        });
                    if let Err(error @ Error::Unknown(_)) = &result {
                        instance.fault.lock().unwrap().get_or_insert(error.clone());
                        let _ = ctx.fail(error.to_string());
                    }
                    drop(permit);
                    Ok(result)
                })
            })
            .await?
    }
    pub async fn dispose(&self) -> Result<()> {
        self.resource.dispose().await.map_err(Into::into)
    }
    pub fn snapshot(&self) -> EffectSnapshot {
        self.resource.snapshot()
    }
    pub(super) fn downgrade(&self) -> WeakContextResource {
        WeakContextResource {
            resource: self.resource.downgrade(),
            released: self.released.clone(),
        }
    }
}
#[derive(Clone)]
pub(super) struct WeakContextResource {
    resource: WeakResource<Ticket>,
    released: Arc<Mutex<Option<Result<()>>>>,
}
impl WeakContextResource {
    pub async fn invoke(&self, method: &str, args: Bytes) -> Result<Bytes> {
        ContextResource {
            resource: self.resource.upgrade().ok_or(Error::Closed)?,
            released: self.released.clone(),
        }
        .invoke(method, args)
        .await
    }
    pub async fn dispose(&self) -> Result<()> {
        if let Some(effect) = self.resource.effect() {
            return effect.dispose().await.map_err(|e| {
                self.released
                    .lock()
                    .unwrap()
                    .as_ref()
                    .and_then(|r| r.clone().err())
                    .unwrap_or_else(|| e.into())
            });
        }
        self.released
            .lock()
            .unwrap()
            .clone()
            .unwrap_or(Err(Error::Closed))
    }
}
