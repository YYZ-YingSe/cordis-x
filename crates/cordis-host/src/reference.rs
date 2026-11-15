//! Encoding-independent binding for transferable interface objects. Execution
//! adapters implement Object once; the runtime supplies scope ownership and
//! caller-preserving dispatch for every combination of execution backends.
use crate::{Body, Result, refused, service_failure};
use cordis_contract::Descriptor;
use cordis_runtime::{BoxFuture, Context};
use std::sync::Arc;

pub trait Object: Send + Sync + 'static {
    fn invoke(&self, ctx: Context, method: String, args: Body) -> BoxFuture<'_, Result<Body>>;
    fn close(&self, ctx: Context) -> BoxFuture<'_, Result<()>>;
}
struct Export {
    descriptor: Descriptor,
    digest: String,
    object: Arc<dyn Object>,
}

/// Export an implementation, never a global service name. The exact descriptor
/// travels with the host object and is checked before a consumer calls it.
pub async fn export(
    ctx: &Context,
    descriptor: Descriptor,
    object: Arc<dyn Object>,
) -> Result<String> {
    ctx.export_reference(
        "interface object",
        Export {
            digest: descriptor.digest(),
            descriptor,
            object,
        },
        |value, ctx| async move {
            value
                .object
                .close(ctx)
                .await
                .map_err(cordis_runtime::Error::Portable)
        },
    )
    .await
    .map_err(|error| match error {
        // The resource layer detects recovery cycles before taking ownership.
        // Preserve that known refusal so adapters can drop their reserved slot.
        cordis_runtime::Error::WouldDeadlock => refused(
            "would_deadlock",
            "interface export would create a recovery cycle",
        ),
        other => service_failure(other),
    })
}

/// Invoke using the receiving caller's context. No dependency-name lookup or
/// provider selection occurs here: the reference already selects one object.
pub async fn invoke(
    ctx: &Context,
    key: &str,
    expected: &str,
    method: &str,
    args: Body,
) -> Result<Body> {
    let reference = ctx.reference::<Export>(key).map_err(service_failure)?;
    let expected = expected.to_owned();
    let method = method.to_owned();
    reference
        .call(ctx, move |target, ctx| {
            Box::pin(async move {
                let result = async {
                    if expected != target.digest {
                        return Err(refused(
                            "interface_mismatch",
                            "reference does not implement the requested interface",
                        ));
                    }
                    let signature = cordis_contract::portable::method(
                        &target.descriptor.interface().methods,
                        &method,
                    )
                    .map_err(|e| refused("unknown_method", e))?;
                    let args = args
                        .into_message(&signature.args)
                        .map_err(|e| refused("invalid_argument", e))?;
                    let value = target
                        .object
                        .invoke(ctx, method, args.into())
                        .await
                        .map_err(|e| e.check_declared(&signature.errors, "interface result"))?;
                    value
                        .into_message(&signature.result)
                        .map(Into::into)
                        .map_err(crate::unknown)
                }
                .await;
                Ok(result)
            })
        })
        .await
        .map_err(service_failure)?
}

pub async fn dispose(ctx: &Context, key: &str) -> Result<()> {
    match ctx.find_reference::<Export>(key) {
        Ok(Some(reference)) => reference.dispose().await.map_err(service_failure),
        // A successfully released export is withdrawn. Releasing it again has
        // no effect; failed cleanup stays registered and returns its evidence.
        Ok(None) => Ok(()),
        Err(error) => Err(service_failure(error)),
    }
}
