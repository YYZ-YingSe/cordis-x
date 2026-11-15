//! Component object references use the same host object table as native and
//! process objects. Existing component callback frames carry actual caller
//! context; closing the exported object is tracked by its runtime resource.
use super::*;
pub(super) struct Object {
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
                .ok_or_else(|| cordis_host::refused("closed", "component retired"))?;
            let value = instance
                .callback_payload(
                    &ctx,
                    self.callback,
                    Payload::with_message(
                        crate::json!({"method":method}),
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
                .ok_or_else(|| cordis_host::refused("closed", "component retired"))?;
            instance
                .callback(&ctx, self.callback, crate::json!({"close":true}))
                .await
                .map_err(cordis_contract::Failure::from)?;
            instance
                .call(&ctx, "forget", crate::json!(self.callback))
                .await
                .map_err(cordis_contract::Failure::from)?;
            Ok(())
        })
    }
}
