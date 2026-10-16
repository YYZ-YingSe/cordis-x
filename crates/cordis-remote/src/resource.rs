use crate::{Bytes, Endpoint, Error, Payload, RemoteService, Result, Value, json};
use cordis_runtime::{Context, EffectSnapshot, Resource};
use cordis_wire::local::Envelope;
use std::sync::{Arc, Mutex};

/// Opaque, caller-owned remote resource. It cannot be rebound to a new session.
#[derive(Clone)]
pub struct RemoteResource {
    resource: Resource<Ticket>,
    released: Arc<Mutex<Option<Result<()>>>>,
}
struct Ticket {
    kind: String,
    client: RemoteService,
    endpoint: Endpoint,
    owner: String,
    acquired: Result<String>,
    released: Arc<Mutex<Option<Result<()>>>>,
}
pub(crate) fn runtime_error(error: cordis_runtime::Error) -> Error {
    match error {
        cordis_runtime::Error::Closed => Error::Closed,
        error @ cordis_runtime::Error::Retired(_) => Error::Refused {
            code: "retired".into(),
            message: error.to_string(),
        },
        // These are known, cached recovery failures. Reporting the failed
        // recovery does not release its inverses or claim their effects undone.
        // Opaque execution failures below remain uncertain.
        error @ (cordis_runtime::Error::CleanupFailed(_, _)
        | cordis_runtime::Error::CleanupBlocked(_, _)) => Error::Remote {
            code: "cleanup_failed".into(),
            message: error.to_string(),
        },
        cordis_runtime::Error::Portable(failure) => match failure {
            cordis_contract::Failure::Rejected(info) => Error::Remote {
                code: info.code,
                message: info.message,
            },
            cordis_contract::Failure::Refused(info) => Error::Refused {
                code: info.code,
                message: info.message,
            },
            cordis_contract::Failure::Unknown(info) => {
                Error::OutcomeUnknown(format!("{}: {}", info.code, info.message))
            }
        },
        other => Error::Resource(other.to_string()),
    }
}
pub(crate) fn valid_resource_id(value: &str) -> bool {
    value.len() >= 2
        && value.len() <= 21
        && value.starts_with('r')
        && value.as_bytes()[1..].iter().all(u8::is_ascii_digit)
}
impl RemoteService {
    pub async fn acquire_json(
        &self,
        context: &Context,
        kind: &str,
        args: Value,
    ) -> Result<RemoteResource> {
        self.acquire_json_via(context, self.endpoint(), kind, args)
            .await
    }
    pub async fn acquire_json_via(
        &self,
        context: &Context,
        endpoint: Endpoint,
        kind: &str,
        args: Value,
    ) -> Result<RemoteResource> {
        let schema = cordis_contract::portable::resource(&self.descriptor, kind)
            .map_err(Error::InvalidInput)?;
        self.acquire_via(
            context,
            endpoint,
            kind,
            crate::json_body(&schema.args, &args)?,
        )
        .await
    }

    pub async fn acquire(
        &self,
        context: &Context,
        kind: &str,
        args: Bytes,
    ) -> Result<RemoteResource> {
        self.acquire_via(context, self.endpoint(), kind, args).await
    }
    pub async fn acquire_via(
        &self,
        context: &Context,
        endpoint: Endpoint,
        kind: &str,
        args: Bytes,
    ) -> Result<RemoteResource> {
        self.acquire_envelope(context, endpoint, kind, Payload::message(args).into())
            .await
    }
    pub(crate) async fn acquire_envelope(
        &self,
        context: &Context,
        endpoint: Endpoint,
        kind: &str,
        mut args: Envelope,
    ) -> Result<RemoteResource> {
        if !self.resources {
            return Err(Error::Refused {
                code: "unsupported_resource".into(),
                message: "host does not support resource handles".into(),
            });
        }
        // Reject invalid input before there can be an uncertain remote effect.
        let schema = cordis_contract::portable::resource(&self.descriptor, kind)
            .map_err(Error::InvalidInput)?;
        args.check(&schema.args).map_err(Error::InvalidInput)?;
        let client = self.clone();
        let kind = kind.to_owned();
        context
            .operation(move |context| async move {
                Ok(client.acquire_owned(context, endpoint, kind, args).await)
            })
            .await
            .map_err(runtime_error)?
    }
    async fn acquire_owned(
        &self,
        context: Context,
        endpoint: Endpoint,
        kind: String,
        args: Envelope,
    ) -> Result<RemoteResource> {
        let client = self.clone();
        let definition = context.clone();
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).map_err(|e| Error::Io(e.to_string()))?;
        let owner: String = nonce.iter().map(|b| format!("{b:02x}")).collect();
        let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
        let released = Arc::new(Mutex::new(None));
        let observed = released.clone();
        let resource = context.acquire(format!("remote resource: {kind}"), async move {
            let response = client.execute(&definition, endpoint, "resource_acquire", args.with_control(json!({"kind":kind,"owner":owner}))).await;
            let acquired = match response {
                Ok(value) => {
                    if let Some(id) = value.control["resource"].as_str().filter(|id| valid_resource_id(id))
                        && value.control["owner"].as_str() == Some(&owner) { Ok(id.to_owned()) }
                    else {
                        let error = Error::Protocol("resource acquisition returned invalid handle or ownership grant".into());
                        client.invalidate(&definition, error.clone()).await; Err(error)
                    }
                }
                Err(error) => Err(error),
            };
            let _ = outcome_tx.send(acquired.clone());
            // Even an uncertain result is a tracked ticket, never an unrecorded Err.
            Ok(Ticket { kind, client, endpoint, owner, acquired, released })
        }, |ticket, cleanup| async move {
            let result = match &ticket.acquired {
                Ok(id) => ticket.client.managed(&cleanup, ticket.endpoint, "resource_release", json!({"resource":id,"owner":ticket.owner}).into()).await.map(|_| ()),
                    Err(Error::Remote { .. } | Error::Refused { .. } | Error::Overloaded | Error::InvalidInput(_)) => Ok(()), // rejected before acquisition committed
                Err(error) => Err(error.clone()), // unknown responsibility remains observable
            };
            *ticket.released.lock().unwrap() = Some(result.clone());
            result.map_err(Into::into)
        }).await.map_err(runtime_error)?;
        let outcome = outcome_rx
            .await
            .map_err(|e| Error::Resource(e.to_string()))?;
        if let Err(error) = outcome {
            if matches!(
                error,
                Error::Remote { .. }
                    | Error::Refused { .. }
                    | Error::Overloaded
                    | Error::InvalidInput(_)
            ) {
                resource.dispose().await.map_err(runtime_error)?;
            }
            return Err(error);
        }
        Ok(RemoteResource {
            resource,
            released: observed,
        })
    }
}
impl RemoteResource {
    pub async fn invoke_json(&self, method: &str, args: Value) -> Result<Value> {
        let signature = self
            .resource
            .with(|ticket| {
                let schema =
                    cordis_contract::portable::resource(&ticket.client.descriptor, &ticket.kind)
                        .map_err(Error::InvalidInput)?;
                cordis_contract::portable::method(&schema.methods, method)
                    .cloned()
                    .map_err(Error::InvalidInput)
            })
            .map_err(runtime_error)??;
        let bytes = self
            .invoke(method, crate::json_body(&signature.args, &args)?)
            .await?;
        crate::body_json(&signature.result, &bytes)
    }

    pub(crate) fn downgrade(&self) -> WeakRemoteResource {
        WeakRemoteResource {
            resource: self.resource.downgrade(),
            released: self.released.clone(),
        }
    }
    pub async fn invoke(&self, method: &str, args: Bytes) -> Result<Bytes> {
        let method = method.to_owned();
        self.resource
            .call(move |ticket, context| {
                Box::pin(async move {
                    // Keep admission failures inside the adapter result. Converting
                    // through Runtime's opaque error would erase known rejection.
                    let signature = (|| {
                        let schema = cordis_contract::portable::resource(
                            &ticket.client.descriptor,
                            &ticket.kind,
                        )
                        .map_err(Error::InvalidInput)?;
                        let signature = cordis_contract::portable::method(&schema.methods, &method)
                            .map_err(Error::InvalidInput)?;
                        signature
                            .args
                            .check_binary(&args)
                            .map_err(Error::InvalidInput)?;
                        Ok::<_, Error>(signature)
                    })();
                    let signature = match signature {
                        Ok(signature) => signature,
                        Err(error) => return Ok(Err(error)),
                    };
                    let id = ticket
                        .acquired
                        .as_ref()
                        .expect("only confirmed tickets become public handles");
                    let result = ticket
                        .client
                        .execute(
                            &context,
                            ticket.endpoint,
                            "resource_call",
                            Payload::with_message(
                                json!({"resource":id,"owner":ticket.owner,"method":method}),
                                args,
                            )
                            .into(),
                        )
                        .await
                        .and_then(|value| value.one().map_err(Into::into))
                        .map_err(|e| e.check_declared(&signature.errors, "resource errors"));
                    if let Err(error @ Error::Protocol(_)) = &result {
                        ticket.client.invalidate(&context, error.clone()).await;
                    }
                    if let Ok(value) = &result
                        && let Err(message) = signature.result.check_binary(value)
                    {
                        let error = Error::Protocol(message);
                        ticket.client.invalidate(&context, error.clone()).await;
                        return Ok(Err(error));
                    }
                    Ok(result)
                })
            })
            .await
            .map_err(runtime_error)?
    }
    /// Concurrent disposal shares the same inverse; in-flight calls finish first.
    pub async fn dispose(&self) -> Result<()> {
        self.resource.dispose().await.map_err(runtime_error)
    }
    pub fn snapshot(&self) -> EffectSnapshot {
        self.resource.snapshot()
    }
}

#[derive(Clone)]
pub(crate) struct WeakRemoteResource {
    resource: cordis_runtime::WeakResource<Ticket>,
    released: Arc<Mutex<Option<Result<()>>>>,
}
impl WeakRemoteResource {
    pub async fn invoke(&self, method: &str, args: Bytes) -> Result<Bytes> {
        let resource = self.resource.upgrade().ok_or(Error::Closed)?;
        RemoteResource {
            resource,
            released: self.released.clone(),
        }
        .invoke(method, args)
        .await
    }
    pub async fn dispose(&self) -> Result<()> {
        // Prefer the Runtime's completion barrier while it remains reachable:
        // a successful remote response may still have continuations to drain.
        if let Some(effect) = self.resource.effect() {
            return effect.dispose().await.map_err(runtime_error);
        }
        self.released
            .lock()
            .unwrap()
            .clone()
            .unwrap_or(Err(Error::Closed))
    }
}
