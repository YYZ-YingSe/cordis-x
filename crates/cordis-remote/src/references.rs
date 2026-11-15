//! Process/HTTP representation of the common host reference binding. Object
//! callbacks reuse scoped requests and protobuf business payloads; no plugin
//! implements a model-specific broker or a second lifecycle protocol.
use crate::{Payload, Value, json, session::WeakSession};
use cordis_host::{Body, Result};
use cordis_runtime::{BoxFuture, Context};
use std::sync::Arc;

pub(crate) struct Object {
    session: WeakSession,
    callback: String,
}
impl Object {
    pub fn new(session: WeakSession, callback: String) -> Arc<Self> {
        Arc::new(Self { session, callback })
    }
}
impl cordis_host::reference::Object for Object {
    fn invoke(&self, ctx: Context, method: String, args: Body) -> BoxFuture<'_, Result<Body>> {
        Box::pin(async move {
            let session = self
                .session
                .upgrade()
                .ok_or_else(|| cordis_host::refused("closed", "reference endpoint retired"))?;
            let payload = Payload::with_message(
                json!({"callback":self.callback,"method":method}),
                args.into_bytes().map_err(cordis_host::unknown)?,
            );
            let value = session
                .scoped_payload(&ctx, "reference_call", payload)
                .await
                .map_err(cordis_contract::Failure::from)?;
            Ok(Body::from_bytes(value.one().map_err(cordis_host::unknown)?))
        })
    }
    fn close(&self, ctx: Context) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let session = self
                .session
                .upgrade()
                .ok_or_else(|| cordis_host::refused("closed", "reference endpoint retired"))?;
            session
                .scoped_request(
                    &ctx,
                    "reference_close",
                    json!({"callback":self.callback,"close":true}),
                )
                .await
                .map(|_: Value| ())
                .map_err(cordis_contract::Failure::from)
        })
    }
}
