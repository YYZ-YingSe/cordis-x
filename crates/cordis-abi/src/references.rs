//! ABI representation of the common interface-reference binding. The existing
//! asynchronous callback ABI carries calls and close acknowledgements; object
//! lifetime and caller identity remain owned by Runtime.
use crate::*;
use cordis_runtime::{BoxFuture, Context};
use std::sync::Weak;
pub(crate) struct Object {
    pub instance: Weak<Instance>,
    pub callback: u64,
}
impl cordis_host::reference::Object for Object {
    fn invoke(
        &self,
        ctx: Context,
        method: String,
        args: cordis_host::Body,
    ) -> BoxFuture<'_, cordis_host::Result<cordis_host::Body>> {
        Box::pin(async move {
            let instance = self
                .instance
                .upgrade()
                .ok_or_else(|| cordis_host::refused("closed", "ABI instance retired"))?;
            let value = instance
                .managed_payload(
                    ctx,
                    17,
                    self.callback,
                    Payload::with_message(
                        json!({"method":method}),
                        args.into_bytes().map_err(cordis_host::unknown)?,
                    ),
                )
                .await
                .map_err(cordis_contract::Failure::from)?;
            Ok(cordis_host::Body::from_bytes(
                value.one().map_err(cordis_host::unknown)?,
            ))
        })
    }
    fn close(&self, ctx: Context) -> BoxFuture<'_, cordis_host::Result<()>> {
        Box::pin(async move {
            let instance = self
                .instance
                .upgrade()
                .ok_or_else(|| cordis_host::refused("closed", "ABI instance retired"))?;
            instance
                .managed_call(ctx.clone(), 18, self.callback, json!({"close":true}))
                .await
                .map_err(cordis_contract::Failure::from)?;
            instance
                .managed_call(ctx, 16, self.callback, Value::Null)
                .await
                .map_err(cordis_contract::Failure::from)?;
            Ok(())
        })
    }
}
