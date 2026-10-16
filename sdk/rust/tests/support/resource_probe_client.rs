// Raw arguments are intentional: this fixture sends malformed and uncertain operations.
use cordis_sdk_core::*;
use std::{rc::Rc, cell::RefCell};
pub struct ProbeClient(RefCell<Option<Resource>>);
impl Plugin for ProbeClient {
    fn dependency_interfaces() -> std::collections::BTreeMap<String, Interface> {
        std::collections::BTreeMap::from([("example.counters".into(), Interface::value_dispatch(true))])
    }
    fn interface() -> cordis_sdk_core::Interface {
        cordis_sdk_core::Interface::value_dispatch(true)
    }
    fn manifest() -> PluginManifest {
        PluginManifest {
            provides: Contract {
                name: "example.counter-client".into(),
                revision: "1".into(),
            },
            requires: vec![Contract { name: "example.counters".into(), revision: "1".into() }],
        }
    }
    fn create(_: Value) -> Result<Self> {
        Ok(Self(RefCell::default()))
    }
    fn activate(self: Rc<Self>, ctx: Context) -> cordis_sdk_core::LocalFuture<()> {
        Box::pin(async move { self.call_probe(ctx, "activate".into(), Value::Null).await.map(|_| ()) })
    }
    fn deactivate(self: Rc<Self>, ctx: Context) -> cordis_sdk_core::LocalFuture<()> {
        Box::pin(async move { self.call_probe(ctx, "deactivate".into(), Value::Null).await.map(|_| ()) })
    }
    fn invoke(self: Rc<Self>, ctx: Context, method: String, args: cordis_sdk_core::Message) -> cordis_sdk_core::message::FutureResult {
        Box::pin(async move {
            let interface = Self::interface();
            let signature = cordis_sdk_core::contract::method(&interface.methods, &method)
                .map_err(|e| cordis_sdk_core::refused("unknown_method", e))?;
            let args = test_json::input(args)?;
            let output = self.call_probe(ctx, "invoke".into(), cordis_sdk_core::json!({"method":method,"args":args})).await?;
            cordis_sdk_core::Message::from_json(signature.result.clone(), &output).map_err(cordis_sdk_core::typed::violation)
        })
    }
}
impl ProbeClient {
    fn call_probe(self: Rc<Self>, ctx: Context, command: String, args: Value) -> FutureResult {
        Box::pin(async move {
            if command == "activate" || command == "deactivate" {
                return Ok(Value::Null);
            }
            if command != "invoke" {
                return Err(error("unknown_command", command));
            }
            if args["method"] == "acquire" {
                let resource = test_json::acquire(&ctx, "example.counters", "counter", args["args"].clone())
                    .await?;
                *self.0.borrow_mut() = Some(resource);
                return Ok(Value::Null);
            }
            let resource = self
                .0
                .borrow()
                .clone()
                .ok_or_else(|| error("empty", "acquire a counter first"))?;
            if args["method"] == "dispose" {
                resource.dispose(&ctx).await?;
                return Ok(Value::Null);
            }
            test_json::resource(&resource, &ctx,
                    args["method"].as_str().unwrap_or(""),
                    args["args"].clone(),
                )
                .await
        })
    }
}


#[path = "json.rs"]
#[allow(dead_code)]
pub(crate) mod test_json;
