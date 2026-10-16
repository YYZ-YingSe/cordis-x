use cordis_sdk_core::*;
use std::{cell::Cell, collections::BTreeMap, rc::Rc};

fn method(args: MessageContract, result: MessageContract) -> Method {
    Method {
        args,
        result,
        errors: Default::default(),
    }
}
fn fallible(args: MessageContract, result: MessageContract) -> Method {
    Method {
        args,
        result,
        errors: ["expected".into()].into(),
    }
}
fn failure(mode: i64) -> Failure {
    match mode {
        -1 => error("expected", "declared failure"),
        -2 => error("closed", "business code is not a runtime refusal"),
        -3 => refused("closed", "runtime refusal"),
        _ => Failure::Unknown(ErrorInfo {
            code: "expected".into(),
            message: "uncertain execution".into(),
        }),
    }
}
pub fn dependency_interface() -> Interface {
    Interface {
        controls: Default::default(),
        methods: Dispatch::Named(BTreeMap::from([
            (
                "echo".into(),
                fallible(
                    MessageContract::standard("google.protobuf.Int64Value"),
                    MessageContract::standard("google.protobuf.Int64Value"),
                ),
            ),
            (
                "observe".into(),
                method(
                    MessageContract::standard("google.protobuf.StringValue"),
                    MessageContract::standard("google.protobuf.Empty"),
                ),
            ),
        ])),
        resources: Dispatch::Named(BTreeMap::new()),
        capabilities: Default::default(),
    }
}
pub struct ContractProbe(Cell<i64>);
impl Plugin for ContractProbe {
    fn interface() -> Interface {
        Interface {
            controls: Default::default(),
            methods: Dispatch::Named(BTreeMap::from([
                (
                    "echo".into(),
                    fallible(
                        MessageContract::standard("google.protobuf.Int64Value"),
                        MessageContract::standard("google.protobuf.Int64Value"),
                    ),
                ),
                (
                    "calls".into(),
                    method(
                        MessageContract::standard("google.protobuf.Empty"),
                        MessageContract::standard("google.protobuf.Int64Value"),
                    ),
                ),
                (
                    "bad".into(),
                    method(
                        MessageContract::standard("google.protobuf.Empty"),
                        MessageContract::standard("google.protobuf.Int64Value"),
                    ),
                ),
                (
                    "error".into(),
                    fallible(
                        MessageContract::standard("google.protobuf.Int64Value"),
                        MessageContract::standard("google.protobuf.Int64Value"),
                    ),
                ),
                (
                    "reverse".into(),
                    fallible(
                        MessageContract::standard("google.protobuf.Int64Value"),
                        MessageContract::standard("google.protobuf.Int64Value"),
                    ),
                ),
                (
                    "reverse-suppressed".into(),
                    method(
                        MessageContract::standard("google.protobuf.Int64Value"),
                        MessageContract::standard("google.protobuf.Int64Value"),
                    ),
                ),
                (
                    "reverse-bad-args".into(),
                    method(
                        MessageContract::standard("google.protobuf.Empty"),
                        MessageContract::standard("google.protobuf.Empty"),
                    ),
                ),
            ])),
            resources: Dispatch::Named(BTreeMap::from([(
                "counter".into(),
                ResourceContract {
                    args: MessageContract::standard("google.protobuf.Int64Value"),
                    methods: Dispatch::Named(BTreeMap::from([
                        (
                            "get".into(),
                            method(
                                MessageContract::standard("google.protobuf.Empty"),
                                MessageContract::standard("google.protobuf.Int64Value"),
                            ),
                        ),
                        (
                            "add".into(),
                            method(
                                MessageContract::standard("google.protobuf.Int64Value"),
                                MessageContract::standard("google.protobuf.Int64Value"),
                            ),
                        ),
                        (
                            "bad".into(),
                            method(
                                MessageContract::standard("google.protobuf.Empty"),
                                MessageContract::standard("google.protobuf.Int64Value"),
                            ),
                        ),
                        (
                            "error".into(),
                            fallible(
                                MessageContract::standard("google.protobuf.Int64Value"),
                                MessageContract::standard("google.protobuf.Int64Value"),
                            ),
                        ),
                    ])),
                    errors: ["expected".into()].into(),
                    ownership: "caller".into(),
                    release: "drain_once".into(),
                },
            )])),
            capabilities: Default::default(),
        }
    }
    fn dependency_interfaces() -> BTreeMap<String, Interface> {
        BTreeMap::from([("test.observer".into(), dependency_interface())])
    }
    fn manifest() -> PluginManifest {
        PluginManifest {
            provides: Contract {
                name: "test.structure".into(),
                revision: "1".into(),
            },
            requires: vec![Contract {
                name: "test.observer".into(),
                revision: "1".into(),
            }],
        }
    }
    fn create(_: Value) -> Result<Self> {
        Ok(Self(Cell::new(0)))
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
            if args.as_str().unwrap().parse::<i64>().unwrap() < 0 {
                return Err(failure(args.as_str().unwrap().parse::<i64>().unwrap()));
            }
            invoke_observer(&ctx, "test.observer", "observe", json!("acquire")).await?;
            Ok(Rc::new(Counter(Cell::new(
                args.as_str().unwrap().parse::<i64>().unwrap(),
            ))) as Rc<dyn ResourceObject>)
        })
    }
}
impl ContractProbe {
    fn call_probe(self: Rc<Self>, ctx: Context, command: String, args: Value) -> FutureResult {
        Box::pin(async move {
            if command == "activate" || command == "deactivate" {
                return invoke_observer(&ctx, "test.observer", "observe", json!(command)).await;
            }
            if command != "invoke" {
                return Err(error("command", command));
            }
            match args["method"].as_str().unwrap() {
                "calls" => Ok(json!(self.0.get())),
                "error" => Err(failure(
                    args["args"].as_str().unwrap().parse::<i64>().unwrap(),
                )),
                "echo" => {
                    self.0.set(self.0.get() + 1);
                    Ok(args["args"].clone())
                }
                "bad" => Ok(json!("wrong result type")),
                "reverse-suppressed" => {
                    let _ =
                        invoke_observer(&ctx, "test.observer", "echo", args["args"].clone()).await;
                    Ok(json!(42))
                }
                "reverse" => {
                    invoke_observer(&ctx, "test.observer", "echo", args["args"].clone()).await
                }
                "reverse-bad-args" => {
                    invoke_observer(&ctx, "test.observer", "echo", json!("bad")).await
                }
                _ => Err(error("method", "undeclared")),
            }
        })
    }
}

struct Counter(Cell<i64>);
impl ResourceObject for Counter {
    fn invoke(
        self: Rc<Self>,
        ctx: Context,
        method: String,
        args: cordis_sdk_core::Message,
    ) -> cordis_sdk_core::message::FutureResult {
        Box::pin(async move {
            let interface = ContractProbe::interface();
            let resource = interface.resources.get("counter").unwrap();
            let signature = resource
                .methods
                .get(&method)
                .ok_or_else(|| refused("unknown_method", &method))?;
            let output = self
                .invoke_probe(ctx, method, test_json::input(args)?)
                .await?;
            Message::from_json(signature.result.clone(), &output).map_err(typed::violation)
        })
    }
    fn close(self: Rc<Self>, ctx: Context) -> LocalFuture<()> {
        Box::pin(async move {
            invoke_observer(&ctx, "test.observer", "observe", json!("release")).await?;
            Ok(())
        })
    }
}
impl Counter {
    fn invoke_probe(self: Rc<Self>, _: Context, method: String, args: Value) -> FutureResult {
        Box::pin(async move {
            match method.as_str() {
                "get" => Ok(json!(self.0.get())),
                "error" => Err(failure(args.as_str().unwrap().parse::<i64>().unwrap())),
                "add" => {
                    self.0
                        .set(self.0.get() + args.as_str().unwrap().parse::<i64>().unwrap());
                    Ok(json!(self.0.get()))
                }
                "bad" => Ok(json!("wrong resource result")),
                _ => Err(error("method", "undeclared")),
            }
        })
    }
}

#[path = "json.rs"]
#[allow(dead_code)]
mod test_json;

async fn invoke_observer(
    ctx: &Context,
    dependency: &str,
    method: &str,
    args: Value,
) -> Result<Value> {
    let interface = dependency_interface();
    let signature = interface
        .methods
        .get(method)
        .ok_or_else(|| refused("unknown_method", method))?;
    let args = Message::from_json(signature.args.clone(), &args)
        .map_err(|e| refused("invalid_argument", e))?;
    test_json::value(
        ctx.invoke(dependency, method, args, &signature.result)
            .await?,
    )
}
