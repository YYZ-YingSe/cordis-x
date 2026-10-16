//! Explicit JSON gateways. Generated native calls use `message` instead.
use crate::{Context, Message, Plugin, Result, Value, refused, typed::violation};
use std::rc::Rc;

pub async fn invoke<P: Plugin>(
    plugin: Rc<P>,
    ctx: Context,
    method: &str,
    args: Value,
) -> Result<Value> {
    let interface = P::interface();
    let signature = crate::contract::method(&interface.methods, method)
        .map_err(|e| refused("unknown_method", e))?;
    let args = Message::from_json(signature.args.clone(), &args)
        .map_err(|e| refused("invalid_argument", format!("args: {e}")))?;
    let value = plugin
        .invoke(ctx, method.into(), args)
        .await
        .map_err(|e| e.check_declared(&signature.errors, "result"))?;
    value.check(&signature.result).map_err(violation)?;
    value.to_json().map_err(violation)
}

/// Foreign adapters use these at their real JSON boundary. Native callbacks
/// retain Message and Flow directly, including continuation authority.
pub fn message(shape: &crate::MessageContract, value: &Value) -> Result<Message> {
    Message::from_json(shape.clone(), value).map_err(violation)
}
pub fn flow(shape: &crate::MessageContract, value: Value) -> Result<crate::typed::Flow<Message>> {
    if value.is_null() {
        return Ok(crate::typed::Flow::Continue);
    }
    let Value::Object(mut fields) = value else {
        return Err(violation("invalid event flow"));
    };
    if fields.len() != 1 {
        return Err(violation("invalid event flow"));
    }
    let value = fields
        .remove("break")
        .ok_or_else(|| violation("invalid event flow"))?;
    message(shape, &value).map(crate::typed::Flow::Break)
}
/// The current Process/ABI tagged dispatch result. Unknown tags and missing
/// payloads are malformed results, not an implicit Continue or a null Break.
pub fn tagged_flow(
    shape: &crate::MessageContract,
    value: Value,
) -> Result<crate::typed::Flow<Message>> {
    let Value::Object(mut fields) = value else {
        return Err(violation("invalid event flow"));
    };
    let kind = fields.remove("kind");
    match kind.as_ref().and_then(Value::as_str) {
        Some("continue") if fields.is_empty() => Ok(crate::typed::Flow::Continue),
        Some("break") if fields.len() == 1 && fields.contains_key("value") => {
            message(shape, &fields.remove("value").unwrap()).map(crate::typed::Flow::Break)
        }
        _ => Err(violation("invalid event flow")),
    }
}
pub fn callback(
    handler: crate::backend::MessageCallback,
    signature: crate::Method,
) -> crate::backend::Callback {
    Rc::new(move |ctx, value| {
        let handler = handler.clone();
        let signature = signature.clone();
        Box::pin(async move {
            let args = Message::from_json(signature.args, &value)
                .map_err(|e| refused("invalid_argument", e))?;
            let value = handler(ctx, args)
                .await
                .map_err(|e| e.check_declared(&signature.errors, "callback"))?;
            value.check(&signature.result).map_err(violation)?;
            value.to_json().map_err(violation)
        })
    })
}
pub fn listener(
    handler: crate::backend::Listener,
    signature: crate::Method,
) -> crate::backend::Callback {
    Rc::new(move |ctx, value| {
        let handler = handler.clone();
        let signature = signature.clone();
        Box::pin(async move {
            let args = Message::from_json(signature.args, &value)
                .map_err(|e| refused("invalid_argument", e))?;
            match handler(ctx, args)
                .await
                .map_err(|e| e.check_declared(&signature.errors, "listener"))?
            {
                crate::typed::Flow::Continue => Ok(Value::Null),
                crate::typed::Flow::Break(value) => {
                    value.check(&signature.result).map_err(violation)?;
                    Ok(crate::json!({"break":value.to_json().map_err(violation)?}))
                }
            }
        })
    })
}
pub type Middleware = Rc<dyn Fn(Context, Value, crate::Next) -> crate::FutureResult>;
pub fn middleware(handler: crate::backend::Middleware, signature: crate::Method) -> Middleware {
    Rc::new(move |ctx, value, next| {
        let handler = handler.clone();
        let signature = signature.clone();
        Box::pin(async move {
            let args = Message::from_json(signature.args, &value)
                .map_err(|e| refused("invalid_argument", e))?;
            let value = handler(ctx, args, next)
                .await
                .map_err(|e| e.check_declared(&signature.errors, "middleware"))?;
            value.check(&signature.result).map_err(violation)?;
            value.to_json().map_err(violation)
        })
    })
}

/// The current JSON command envelope, decoded only by foreign ingress.
pub fn invocation(value: Value) -> crate::Result<(String, Value)> {
    let Value::Object(mut object) = value else {
        return Err(refused("invalid_argument", "expected invocation object"));
    };
    let method = object
        .remove("method")
        .and_then(|v| v.as_str().map(str::to_owned))
        .ok_or_else(|| refused("invalid_argument", "missing method"))?;
    let args = object
        .remove("args")
        .ok_or_else(|| refused("invalid_argument", "missing args"))?;
    if !object.is_empty() {
        return Err(refused("invalid_argument", "undeclared invocation field"));
    }
    Ok((method, args))
}

/// JSON configuration ingress for generated constructors. It uses the standard
/// borrowed descriptor parser; an overridable Codec::decode cannot bypass it.
pub fn input<T: crate::typed::Codec>(value: Value, shape: &crate::MessageContract) -> Result<T> {
    let decode = || -> std::result::Result<T, String> {
        if shape != T::contract() {
            return Err("codec/contract message mismatch".into());
        }
        shape
            .parse_json(&value)?
            .transcode_to()
            .map_err(|e| e.to_string())
    };
    decode().map_err(|e| refused("invalid_argument", format!("args: {e}")))
}
