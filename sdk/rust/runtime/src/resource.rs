use super::{native_error, rejected, unknown, worker::Client, *};

#[cfg(test)]
mod tests;

/// A native handle to a portable resource. Runtime owns admission, lifetime,
/// dynamic Broker pins and the cached cleanup outcome.
#[derive(Clone)]
pub struct Resource {
    resource: rt::Resource<Ticket>,
    pub(crate) contract: cordis_contract::Resource,
    released: Arc<std::sync::Mutex<Option<Result<()>>>>,
}
struct Ticket {
    client: Client,
    acquired: Result<u64>,
}

impl Service {
    pub async fn acquire(&self, ctx: &rt::Context, kind: &str, args: Payload) -> Result<Resource> {
        let contract = cordis_contract::portable::resource(&self.descriptor, kind)
            .map_err(|e| refused("unknown_resource_kind", e))?
            .clone();
        let args = args
            .bind(&contract.args)
            .map_err(|e| refused("invalid_argument", e))?;
        let service = self.client.clone();
        let kind = kind.to_owned();
        ctx.operation(
            move |ctx| async move { Ok(acquire(service, ctx, kind, args, contract).await) },
        )
        .await
        .map_err(rejected)?
    }
    pub async fn acquire_json(
        &self,
        ctx: &rt::Context,
        kind: &str,
        args: Value,
    ) -> Result<Resource> {
        let contract = cordis_contract::portable::resource(&self.descriptor, kind)
            .map_err(|e| refused("unknown_resource_kind", e))?;
        let args = Payload::from_json(contract.args.clone(), &args)
            .map_err(|e| refused("invalid_argument", e))?;
        self.acquire(ctx, kind, args).await
    }
}

async fn acquire(
    client: Client,
    ctx: rt::Context,
    kind: String,
    args: Payload,
    contract: cordis_contract::Resource,
) -> Result<Resource> {
    let released = Arc::new(std::sync::Mutex::new(None));
    let cleanup_result = released.clone();
    let callback = ctx.snapshot();
    let errors = contract.errors.clone();
    let (sender, outcome) = tokio::sync::oneshot::channel();
    let resource = ctx
        .acquire(
            format!("portable resource: {kind}"),
            async move {
                let acquired = client
                    .acquire(callback, kind, args)
                    .await
                    .map_err(|e| e.check_declared(&errors, "resource factory"));
                let _ = sender.send(acquired.clone());
                // Unknown is a tracked ticket too: an error must not erase
                // responsibility or permit the instance to close early.
                Ok(Ticket { client, acquired })
            },
            move |ticket, cleanup| async move {
                let result = match &ticket.acquired {
                    Ok(id) => ticket
                        .client
                        .release(cleanup, *id)
                        .await
                        .map_err(native_error),
                    Err(Failure::Rejected(_) | Failure::Refused(_)) => Ok(()),
                    Err(failure @ Failure::Unknown(_)) => Err(native_error(failure.clone())),
                };
                *cleanup_result.lock().unwrap() = Some(result.clone().map_err(runtime_failure));
                result
            },
        )
        .await
        .map_err(rejected)?;
    if let Err(failure) = outcome.await.map_err(unknown)? {
        if matches!(failure, Failure::Rejected(_) | Failure::Refused(_)) {
            resource.dispose().await.map_err(unknown)?;
        }
        return Err(failure);
    }
    Ok(Resource {
        resource,
        contract,
        released,
    })
}

impl Resource {
    pub async fn invoke(&self, method: &str, args: Payload) -> Result<Payload> {
        let signature = cordis_contract::portable::method(&self.contract.methods, method)
            .map_err(|e| refused("unknown_method", e))?;
        let args = args
            .bind(&signature.args)
            .map_err(|e| refused("invalid_argument", e))?;
        let signature = signature.clone();
        let method = method.to_owned();
        self.resource
            .call(move |ticket, ctx| {
                Box::pin(async move {
                    let id = *ticket.acquired.as_ref().expect("confirmed resource");
                    let result = ticket
                        .client
                        .resource_call(ctx, id, method, args)
                        .await
                        .map_err(|e| e.check_declared(&signature.errors, "resource result"))
                        .and_then(|value| {
                            value
                                .check(&signature.result)
                                .map_err(cordis_sdk_core::typed::violation)?;
                            Ok(value)
                        });
                    Ok(result)
                })
            })
            .await
            .map_err(rejected)?
    }
    pub async fn invoke_json(&self, method: &str, args: Value) -> Result<Value> {
        let signature = cordis_contract::portable::method(&self.contract.methods, method)
            .map_err(|e| refused("unknown_method", e))?;
        let args = Payload::from_json(signature.args.clone(), &args)
            .map_err(|e| refused("invalid_argument", e))?;
        self.invoke(method, args)
            .await?
            .to_json()
            .map_err(cordis_sdk_core::typed::violation)
    }
    pub async fn dispose(&self) -> Result<()> {
        self.resource.dispose().await.map_err(unknown)
    }
    pub fn snapshot(&self) -> rt::EffectSnapshot {
        self.resource.snapshot()
    }
}

/// Retains only cleanup evidence and a weak capability owned by Runtime.
#[derive(Clone)]
pub(crate) struct WeakResource {
    resource: rt::WeakResource<Ticket>,
    contract: cordis_contract::Resource,
    released: Arc<std::sync::Mutex<Option<Result<()>>>>,
}
impl Resource {
    pub(crate) fn downgrade(&self) -> WeakResource {
        WeakResource {
            resource: self.resource.downgrade(),
            contract: self.contract.clone(),
            released: self.released.clone(),
        }
    }
}
impl WeakResource {
    pub(crate) fn is_released(&self) -> bool {
        self.resource.is_released()
    }
    pub(crate) fn upgrade(&self) -> Result<Resource> {
        Ok(Resource {
            resource: self
                .resource
                .upgrade()
                .ok_or_else(|| refused("closed", "resource retired"))?,
            contract: self.contract.clone(),
            released: self.released.clone(),
        })
    }
    pub(crate) async fn dispose(&self) -> Result<()> {
        if let Some(effect) = self.resource.effect() {
            effect.dispose().await.map_err(runtime_failure)
        } else {
            self.released
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| Err(refused("closed", "resource retired")))
        }
    }
}
