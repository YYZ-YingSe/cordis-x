//! JSON fault probes deliberately exercise dynamic data outside generated DTOs.
use cordis_sdk_core::{
    Context, Message, MessageContract, Resource, Result, Value, refused, typed::violation,
};

pub fn message(value: Value) -> Result<Message> {
    Message::from_json(MessageContract::json_value(), &value).map_err(violation)
}
pub fn value(message: Message) -> Result<Value> {
    message.to_json().map_err(violation)
}
pub async fn invoke(ctx: &Context, dependency: &str, method: &str, args: Value) -> Result<Value> {
    value(
        ctx.invoke(
            dependency,
            method,
            message(args)?,
            &MessageContract::json_value(),
        )
        .await?,
    )
}
pub async fn acquire(ctx: &Context, dependency: &str, kind: &str, args: Value) -> Result<Resource> {
    ctx.acquire(dependency, kind, message(args)?).await
}
pub async fn resource(
    resource: &Resource,
    ctx: &Context,
    method: &str,
    args: Value,
) -> Result<Value> {
    value(
        resource
            .invoke(ctx, method, message(args)?, &MessageContract::json_value())
            .await?,
    )
}
pub fn input(args: Message) -> Result<Value> {
    args.to_json().map_err(|e| refused("invalid_argument", e))
}
