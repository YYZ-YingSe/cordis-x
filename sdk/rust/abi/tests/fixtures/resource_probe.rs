//! Fault injection kept separate from the shared business example.
use cordis_sdk_core::*;
use std::rc::Rc;
struct Probe;
struct Object {
    args: Value,
    old: Context,
}
impl Plugin for Probe {
    fn dependency_interfaces() -> std::collections::BTreeMap<String, cordis_sdk_core::Interface> {
        std::collections::BTreeMap::from([(
            "example.logger".into(),
            cordis_sdk_core::Interface::value_dispatch(true),
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
            let output = self
                .call_probe(
                    ctx,
                    "invoke".into(),
                    cordis_sdk_core::json!({"method":method,"args":args}),
                )
                .await?;
            cordis_sdk_core::Message::from_json(signature.result.clone(), &output)
                .map_err(cordis_sdk_core::typed::violation)
        })
    }
    fn acquire(
        self: Rc<Self>,
        ctx: Context,
        _: String,
        args: cordis_sdk_core::Message,
    ) -> LocalFuture<Rc<dyn ResourceObject>> {
        Box::pin(async move {
            let args = test_json::input(args)?;
            test_json::invoke(&ctx, "example.logger", "acquire", args.clone()).await?;
            assert!(args["panic_acquire"] != true, "acquire panic injection");
            if args["reject"] == true {
                return Err(error("factory_rejected", "no acquired resource"));
            }
            if args["unknown"] == true {
                return Err(Failure::Unknown(ErrorInfo {
                    code: "uncertain".into(),
                    message: "unknown acquisition".into(),
                }));
            }
            Ok(Rc::new(Object { args, old: ctx }) as Rc<dyn ResourceObject>)
        })
    }
}
impl Probe {
    fn call_probe(self: Rc<Self>, ctx: Context, command: String, args: Value) -> FutureResult {
        Box::pin(async move {
            if command == "invoke" && args["method"] == "reentered" {
                return Ok(json!(42));
            }
            test_json::invoke(&ctx, "example.logger", &command, Value::Null).await
        })
    }
}

impl ResourceObject for Object {
    fn invoke(
        self: Rc<Self>,
        ctx: Context,
        method: String,
        args: cordis_sdk_core::Message,
    ) -> cordis_sdk_core::message::FutureResult {
        Box::pin(async move {
            let output = self
                .invoke_probe(ctx, method, test_json::input(args)?)
                .await?;
            test_json::message(output)
        })
    }
    fn close(self: Rc<Self>, ctx: Context) -> LocalFuture<()> {
        Box::pin(async move {
            test_json::invoke(&ctx, "example.logger", "release", self.args.clone()).await?;
            assert!(self.args["panic_release"] != true, "close panic injection");
            if self.args["fail_release"] == true {
                return Err(error("close_failed", "retain resource"));
            }
            Ok(())
        })
    }
}
impl Object {
    fn invoke_probe(self: Rc<Self>, ctx: Context, method: String, _: Value) -> FutureResult {
        Box::pin(async move {
            if method == "expired" {
                return Ok(json!(
                    test_json::invoke(&self.old, "example.logger", "must-not-run", Value::Null)
                        .await
                        .is_err()
                ));
            }
            test_json::invoke(&ctx, "example.logger", "call", self.args.clone()).await
        })
    }
}

cordis_sdk_abi::export!(crate::Probe);

#[path = "../../../tests/support/json.rs"]
#[allow(dead_code)]
mod test_json;
