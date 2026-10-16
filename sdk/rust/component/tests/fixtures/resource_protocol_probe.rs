//! Injects malformed resource acknowledgements without changing the real SDK.
#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
use cordis_sdk_core::*;
use std::rc::Rc;
struct Probe;
impl Plugin for Probe {
    fn dependency_interfaces() -> std::collections::BTreeMap<String, Interface> {
        std::collections::BTreeMap::from([(
            "example.logger".into(),
            Interface::value_dispatch(true),
        )])
    }
    fn interface() -> cordis_sdk_core::Interface {
        cordis_sdk_core::Interface::value_dispatch(true)
    }
    fn manifest() -> PluginManifest {
        PluginManifest {
            provides: Contract {
                name: "example.counters".into(),
                revision: "1".into(),
            },
            requires: vec![Contract {
                name: "example.logger".into(),
                revision: "1".into(),
            }],
        }
    }
    fn create(_: Value) -> Result<Self> {
        Ok(Self)
    }
    fn activate(self: Rc<Self>, ctx: Context) -> cordis_sdk_core::LocalFuture<()> {
        Box::pin(async move {
            self.call_probe(ctx, "activate".into(), Value::Null)
                .await
                .map(|_| ())
        })
    }
    fn deactivate(self: Rc<Self>, ctx: Context) -> cordis_sdk_core::LocalFuture<()> {
        Box::pin(async move {
            self.call_probe(ctx, "deactivate".into(), Value::Null)
                .await
                .map(|_| ())
        })
    }
    fn invoke(
        self: Rc<Self>,
        ctx: Context,
        method: String,
        args: cordis_sdk_core::Message,
    ) -> cordis_sdk_core::message::FutureResult {
        Box::pin(async move {
            let interface = Self::interface();
            let signature = cordis_sdk_core::contract::method(&interface.methods, &method)
                .map_err(|e| cordis_sdk_core::refused("unknown_method", e))?;
            let args = test_json::input(args)?;
            let output = self.call_probe(ctx, method, args).await?;
            cordis_sdk_core::Message::from_json(signature.result.clone(), &output)
                .map_err(cordis_sdk_core::typed::violation)
        })
    }
}
impl Probe {
    fn call_probe(self: Rc<Self>, ctx: Context, command: String, args: Value) -> FutureResult {
        Box::pin(async move {
            match command.as_str() {
                "probe-acquire" => {
                    test_json::invoke(&ctx, "example.logger", "acquire", args).await?;
                    Ok(Value::Null)
                }
                "probe-release" => {
                    test_json::invoke(&ctx, "example.logger", "release", args).await?;
                    Ok(Value::Null)
                }
                _ => test_json::invoke(&ctx, "example.logger", &command, args).await,
            }
        })
    }
}

#[allow(unsafe_code)]
mod exports {
    use super::*;
    use bindings::{
        cordis::plugin::types,
        exports::cordis::plugin::managed_service::{Guest, Manifest},
    };
    use cordis_guest::{
        __private::{Instance, call},
        bindings,
    };
    struct Adapter;
    std::thread_local! {
        static INSTANCE: Instance<Probe> = Default::default();
        static CONFIG: std::cell::RefCell<Value> = const { std::cell::RefCell::new(Value::Null) };
    }
    impl Guest for Adapter {
        fn describe() -> Manifest {
            cordis_guest::__private::describe::<Probe>()
        }
        async fn call(
            token: String,
            _cleanup: bool,
            command: String,
            args: types::Payload,
        ) -> std::result::Result<types::Payload, types::Failure> {
            if command == "create" {
                let value = cordis_component::decode(args.clone())
                    .and_then(cordis_component::Payload::control_only)
                    .and_then(cordis_component::Initialization::decode)
                    .map_err(|e| types::Failure::from(refused("invalid_argument", e)))?;
                CONFIG.with(|config| *config.borrow_mut() = value.config);
            }
            let probe = match command.as_str() {
                "resource-acquire" => Some(("probe-acquire", "acquired")),
                "resource-release" => Some(("probe-release", "released")),
                _ => None,
            };
            if let Some((method, field)) = probe {
                let value = cordis_component::decode(args)
                    .map(|p| p.control)
                    .map_err(|e| types::Failure::from(refused("invalid_argument", e)))?;
                let input = cordis_component::encode(&cordis_component::Payload::with_message(
                    json!({"method":method}),
                    cordis_sdk_core::Message::from_json(
                        cordis_sdk_core::MessageContract::json_value(),
                        &value,
                    )
                    .unwrap()
                    .encode(cordis_component::MAX_BYTES)
                    .unwrap(),
                ))
                .map_err(|e| types::Failure::from(refused("invalid_argument", e)))?;
                call(&INSTANCE, token, "invoke".into(), input).await?;
                // Inject the raw acknowledgement at the foreign boundary;
                // a typed business result cannot forge malformed wire data.
                let output = CONFIG
                    .with(|config| config.borrow().get(field).cloned())
                    .unwrap_or(if field == "acquired" {
                        json!(1)
                    } else {
                        Value::Null
                    });
                let mut encoded =
                    cordis_component::encode(&cordis_component::Payload::from(output))
                        .map_err(|e| types::Failure::from(typed::violation(e)))?;
                if CONFIG.with(|config| config.borrow()["unknown_carrier"] == true) {
                    encoded.extend([0x78, 1]);
                }
                return Ok(encoded);
            }
            call(&INSTANCE, token, command, args).await
        }
    }
    #[cfg(target_arch = "wasm32")]
    bindings::export_bindings!(Adapter with_types_in bindings);
}

#[path = "../../../tests/support/json.rs"]
#[allow(dead_code)]
mod test_json;
