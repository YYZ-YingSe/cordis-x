use super::ComponentBackend;
use crate::{
    bindings::cordis::plugin::host,
    conversion::{decode, encode},
};
use cordis_component::Payload;
use cordis_sdk_core::{Failure, Result, Value};
impl ComponentBackend {
    pub(super) async fn request(&self, command: &str, args: Value) -> Result<Value> {
        self.request_payload(command, args.into())
            .await?
            .control_only()
            .map_err(unknown)
    }
    pub(super) async fn request_payload(&self, command: &str, args: Payload) -> Result<Payload> {
        let value = host::call(self.token.clone(), command.to_owned(), encode(&args)?)
            .await
            .map_err(Failure::from)?;
        decode(value, self.callbacks.validation()).map_err(unknown)
    }
}
fn unknown(error: impl ToString) -> Failure {
    Failure::Unknown(cordis_sdk_core::ErrorInfo {
        code: "invalid_result".into(),
        message: error.to_string(),
    })
}
