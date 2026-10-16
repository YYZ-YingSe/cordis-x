use super::resources::Resources;
use crate::{
    adapter::{Callbacks, ComponentBackend},
    bindings::cordis::plugin::types,
    conversion::{decode, encode},
};
use cordis_component::Payload;
use cordis_sdk_core::{Failure, LocalFuture, Plugin, Value, json, refused};
use std::{cell::RefCell, rc::Rc};

pub struct Instance<P> {
    plugin: RefCell<Option<Rc<P>>>,
    // The instance owns its immutable schema; calls must not rebuild descriptor
    // pools and spend the shared Store fuel budget on metadata preparation.
    interface: RefCell<Rc<cordis_sdk_core::Interface>>,
    callbacks: Callbacks,
    resources: Rc<Resources>,
}
impl<P: Plugin> Default for Instance<P> {
    fn default() -> Self {
        Self {
            plugin: RefCell::new(None),
            interface: RefCell::new(Rc::new(P::interface())),
            callbacks: Callbacks::default(),
            resources: Rc::default(),
        }
    }
}
fn invalid(error: impl ToString) -> Failure {
    refused("invalid_argument", error.to_string())
}
fn unknown(error: impl ToString) -> Failure {
    Failure::Unknown(cordis_sdk_core::ErrorInfo {
        code: "invalid_result".into(),
        message: error.to_string(),
    })
}
pub async fn call<P: Plugin>(
    instance: &'static std::thread::LocalKey<Instance<P>>,
    token: String,
    command: String,
    bytes: types::Payload,
) -> std::result::Result<types::Payload, types::Failure> {
    let validation = instance.with(|s| {
        if s.plugin.borrow().is_none() {
            cordis_sdk_core::Validation::Audit
        } else {
            s.callbacks.validation()
        }
    });
    let payload = decode(bytes, validation)?;
    let args = payload.control.clone();
    let callbacks = instance.with(|s| s.callbacks.clone());
    let ctx = ComponentBackend::context(token, callbacks.clone());
    let resources = instance.with(|s| s.resources.clone());
    let future: LocalFuture<Payload> = match command.as_str() {
        "create" => Box::pin(async move {
            if instance.with(|s| s.plugin.borrow().is_some()) {
                return Err(refused("already_created", "instance already exists"));
            }
            let settings =
                cordis_component::Initialization::decode(payload.control_only().map_err(invalid)?)
                    .map_err(invalid)?;
            instance.with(|s| {
                Rc::make_mut(&mut s.interface.borrow_mut()).set_validation(settings.validation);
                s.callbacks.set_validation(settings.validation);
            });
            let plugin = P::create(settings.config)?;
            let old = instance.with(|s| s.plugin.replace(Some(Rc::new(plugin))));
            drop(old);
            Ok(Value::Null.into())
        }),
        "callback" => {
            let id = args["id"]
                .as_u64()
                .ok_or_else(|| refused("invalid_callback", "missing id"))?;
            callbacks.invoke_payload(
                id,
                ctx,
                Payload {
                    control: args["args"].clone(),
                    messages: payload.messages,
                },
            )?
        }
        "forget" => {
            payload.control_only().map_err(invalid)?;
            if let Some(id) = args.as_u64() {
                callbacks.remove(id);
            }
            Box::pin(async { Ok(Value::Null.into()) })
        }
        "close" => {
            payload.control_only().map_err(invalid)?;
            if !resources.recovered() {
                return Err(refused("resources_pending", "resources remain unreleased").into());
            }
            let old = instance.with(|s| s.plugin.take());
            drop(old);
            callbacks.clear();
            Box::pin(async { Ok(Value::Null.into()) })
        }
        "resource-acquire" => {
            let plugin = instance
                .with(|s| s.plugin.borrow().clone())
                .ok_or_else(|| refused("closed", "instance absent"))?;
            let kind = text(&args, "kind")?.to_owned();
            let interface = instance.with(|s| s.interface.borrow().clone());
            let contract = interface
                .resources
                .get(&kind)
                .ok_or_else(|| refused("unknown_resource_kind", &kind))?;
            let value = cordis_sdk_core::message::incoming::<P>(
                contract.args.clone(),
                payload.one().map_err(invalid)?,
            )
            .map_err(invalid)?;
            let (id, entry) = resources.reserve(text(&args, "owner")?, contract.clone())?;
            Box::pin(async move {
                match plugin.acquire(ctx, kind, value).await {
                    Ok(value) => {
                        *entry.value.borrow_mut() = Some(value);
                        Ok(json!(id).into())
                    }
                    Err(error) => {
                        if matches!(error, Failure::Rejected(_) | Failure::Refused(_)) {
                            resources.reject(id);
                        }
                        Err(error)
                    }
                }
            })
        }
        "resource-invoke" => {
            let entry = resources.get(&args)?;
            let method = text(&args, "method")?.to_owned();
            let bytes = payload.one().map_err(invalid)?;
            Box::pin(async move { entry.invoke(ctx, method, bytes).await.map(Payload::message) })
        }
        "resource-release" => {
            payload.control_only().map_err(invalid)?;
            let entry = resources.get(&args)?;
            Box::pin(async move { entry.close(ctx).await.map(|_| Value::Null.into()) })
        }
        _ => {
            let plugin = instance
                .with(|s| s.plugin.borrow().clone())
                .ok_or_else(|| refused("closed", "instance absent"))?;
            Box::pin(async move {
                match command.as_str() {
                    "activate" => {
                        payload.control_only().map_err(invalid)?;
                        plugin.activate(ctx).await?;
                        Ok(Value::Null.into())
                    }
                    "deactivate" => {
                        payload.control_only().map_err(invalid)?;
                        plugin.deactivate(ctx).await?;
                        Ok(Value::Null.into())
                    }
                    "invoke" => {
                        let method = text(&args, "method")?;
                        let interface = instance.with(|s| s.interface.borrow().clone());
                        let signature =
                            cordis_sdk_core::contract::method(&interface.methods, method)
                                .map_err(invalid)?;
                        let args = cordis_sdk_core::message::incoming::<P>(
                            signature.args.clone(),
                            payload.one().map_err(invalid)?,
                        )
                        .map_err(invalid)?;
                        let value = plugin
                            .invoke(ctx, method.into(), args)
                            .await
                            .map_err(|e| e.check_declared(&signature.errors, "method errors"))?;
                        value.check(&signature.result).map_err(unknown)?;
                        Ok(Payload::message(
                            value.encode(cordis_component::MAX_BYTES).map_err(unknown)?,
                        ))
                    }
                    _ => Err(refused("unknown_command", command)),
                }
            })
        }
    };
    encode(&future.await?).map_err(|e| unknown(e).into())
}
fn text<'a>(args: &'a Value, key: &str) -> cordis_sdk_core::Result<&'a str> {
    args[key]
        .as_str()
        .ok_or_else(|| refused("invalid_argument", format!("missing {key}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cordis_sdk_core::{Context, Contract, Interface, Message, PluginManifest, Validation};
    use futures_util::FutureExt;
    use std::cell::Cell;
    thread_local! {
        static CREATED: Cell<usize> = const { Cell::new(0) };
        static ENTERED: Cell<usize> = const { Cell::new(0) };
        static INSTANCE: Instance<Probe> = Instance::default();
    }
    struct Probe;
    impl Plugin for Probe {
        fn manifest() -> PluginManifest {
            PluginManifest {
                provides: Contract {
                    name: "carrier-probe".into(),
                    revision: "1".into(),
                },
                requires: vec![],
            }
        }
        fn interface() -> Interface {
            Interface::value_dispatch(false)
        }
        fn create(_: Value) -> cordis_sdk_core::Result<Self> {
            CREATED.with(|n| n.set(n.get() + 1));
            Ok(Self)
        }
        fn invoke(self: Rc<Self>, _: Context, _: String, args: Message) -> LocalFuture<Message> {
            ENTERED.with(|n| n.set(n.get() + 1));
            Box::pin(async { Ok(args) })
        }
    }
    fn dispatch(command: &str, bytes: Vec<u8>) -> std::result::Result<Vec<u8>, types::Failure> {
        call(&INSTANCE, "test".into(), command.into(), bytes)
            .now_or_never()
            .expect("probe never suspends")
    }
    #[test]
    fn bootstrap_is_strict_and_instance_policy_controls_foreign_payload_admission() {
        for policy in [Validation::Contract, Validation::Checked, Validation::Audit] {
            let before = CREATED.get();
            let init = Payload::from(cordis_component::Initialization::encode(
                Value::Null,
                policy,
            ));
            let mut unknown = encode(&init).unwrap();
            unknown.extend([0x78, 1]);
            assert!(matches!(
                dispatch("create", unknown),
                Err(types::Failure::Refused(_))
            ));
            assert_eq!(CREATED.get(), before);
            dispatch("create", encode(&init).unwrap()).unwrap();
            dispatch("activate", encode(&Value::Null.into()).unwrap()).unwrap();
            let body =
                Message::from_json(cordis_sdk_core::MessageContract::json_value(), &Value::Null)
                    .unwrap()
                    .encode(cordis_component::MAX_BYTES)
                    .unwrap();
            let payload = Payload::with_message(json!({"method":"echo"}), body);
            let mut bytes = encode(&payload).unwrap();
            bytes.extend([0x78, 1]);
            let before = ENTERED.get();
            let result = dispatch("invoke", bytes);
            assert_eq!(result.is_err(), policy.audits());
            assert_eq!(ENTERED.get() - before, usize::from(!policy.audits()));
            dispatch("deactivate", encode(&Value::Null.into()).unwrap()).unwrap();
            dispatch("close", encode(&Value::Null.into()).unwrap()).unwrap();
        }
    }
}
