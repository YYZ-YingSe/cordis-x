use super::*;
use cordis_sdk_core::{Contract, FutureResult, LocalFuture, PluginManifest, ResourceObject};

#[derive(Default)]
struct Probe {
    acquired: Cell<usize>,
    closed: Cell<usize>,
}
impl Plugin for Probe {
    fn dependency_interfaces() -> std::collections::BTreeMap<String, cordis_sdk_core::Interface> {
        std::collections::BTreeMap::from([(
            "test.upstream".into(),
            cordis_sdk_core::Interface::value_dispatch(true),
        )])
    }
    fn interface() -> cordis_sdk_core::Interface {
        cordis_sdk_core::Interface::value_dispatch(true)
    }
    fn manifest() -> PluginManifest {
        PluginManifest {
            provides: Contract {
                name: "test.counter".into(),
                revision: "1".into(),
            },
            requires: vec![Contract {
                name: "test.upstream".into(),
                revision: "1".into(),
            }],
        }
    }
    fn create(_: Value) -> Result<Self> {
        Ok(Self::default())
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
        _: Context,
        _: String,
        _: cordis_sdk_core::Message,
    ) -> LocalFuture<Rc<dyn ResourceObject>> {
        Box::pin(async move {
            self.acquired.set(self.acquired.get() + 1);
            Ok(self as Rc<dyn ResourceObject>)
        })
    }
}
impl Probe {
    fn call_probe(self: Rc<Self>, ctx: Context, method: String, args: Value) -> FutureResult {
        Box::pin(async move {
            if method == "invoke" && args["method"] == "reverse" {
                test_json::acquire(&ctx, "test.upstream", "counter", Value::Null).await?;
            }
            Ok(Value::Null)
        })
    }
}

impl ResourceObject for Probe {
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
    fn close(self: Rc<Self>, _: Context) -> LocalFuture<()> {
        Box::pin(async move {
            self.closed.set(self.closed.get() + 1);
            Ok(())
        })
    }
}
impl Probe {
    fn invoke_probe(self: Rc<Self>, _: Context, _: String, _: Value) -> FutureResult {
        Box::pin(async { Ok(json!(42)) })
    }
}

fn fixture() -> (Host<Probe>, mpsc::Receiver<cordis_wire::Bytes>) {
    let (send, receive) = mpsc::channel(256);
    (
        Host {
            descriptor: cordis_sdk_core::Descriptor::new(Probe::interface()).unwrap(),
            channel: Channel::new(send),
            bindings: Rc::new(Default::default()),
            plugin: Rc::new(Probe::default()),
            dependencies: Rc::new(BTreeMap::from([("test.upstream".into(), "1".into())])),
            registry: Rc::new(Registry::default()),
            resources: Default::default(),
            active: Cell::new(false),
            attempted: Cell::new(false),
            closing: Cell::new(false),
            seen: RefCell::default(),
            inflight: Cell::new(0),
            idle: Notify::new(),
            lanes: std::array::from_fn(|_| Cell::new(0)),
            url: None,
            done: watch::channel(None).0,
        },
        receive,
    )
}

#[tokio::test]
async fn audit_checks_reject_foreign_fields_before_factory_and_fault_invalid_results() {
    use cordis_sdk_core::{Descriptor, Dispatch, MessageContract};
    tokio::task::LocalSet::new()
        .run_until(async {
            let (mut host, _output) = fixture();
            start(&host, json!({})).await;
            host.channel.validation.set(cordis_sdk_core::Validation::Audit);
            let mut schema = Probe::interface();
            if let Dispatch::Dynamic(resource) = &mut schema.resources {
                resource.args = MessageContract::standard("google.protobuf.Int64Value");
                if let Dispatch::Dynamic(method) = &mut resource.methods {
                    method.args = MessageContract::standard("google.protobuf.Empty");
                    method.result = MessageContract::standard("google.protobuf.BoolValue"); // Probe deliberately returns integer 42.
                }
            }
            host.descriptor = Descriptor::new(schema).unwrap();
            let invalid = host
                .execute(
                    &request(
                        3,
                        "resource_acquire",
                        json!({
                            "kind":"counter", "owner":"b".repeat(32), "args":"bad"
                        }),
                    )
                )
                .await;
            assert!(matches!(invalid, Err(Failure::Rejected(_) | Failure::Refused(_))));
            assert_eq!(host.plugin.acquired.get(), 0);
            let resource = host
                .execute(
                    &request_message(4,"resource_acquire",json!({"kind":"counter","owner":"b".repeat(32)}),
                        MessageContract::standard("google.protobuf.Int64Value"),json!(0)),
                )
                .await
                .unwrap();
            assert_eq!(host.plugin.acquired.get(), 1);
            for (n, command, payload) in [
                (5, "call", json!({"method":"get"})),
                (6, "resource_call", json!({"resource":resource.control["resource"], "owner":resource.control["owner"], "method":"get"})),
            ] {
                let rejected = host.execute(&request(n, command, payload)).await;
                assert!(matches!(rejected, Err(Failure::Rejected(_) | Failure::Refused(_))));
            }
            let result = host.dispatch(request_message(7,"resource_call",json!({
                "resource":resource.control["resource"],"owner":resource.control["owner"],"method":"get"
            }),MessageContract::standard("google.protobuf.Empty"),json!({}))).await;
            assert!(matches!(result, Err(Failure::Unknown(_))));
            assert!(host.done.borrow().is_some());
            assert_eq!(host.plugin.closed.get(), 0);
        })
        .await;
}
fn request(n: u64, command: &str, mut payload: Value) -> Request {
    let body = if matches!(command, "call" | "resource_acquire" | "resource_call") {
        payload.as_object_mut().unwrap().remove("args")
    } else {
        None
    };
    payload["context"] = json!("c".repeat(32));
    let mut payload = cordis_wire::Payload::from(payload);
    if let Some(body) = body {
        payload.messages.push(
            test_json::message(body)
                .unwrap()
                .encode(cordis_wire::MAX_MESSAGE)
                .unwrap(),
        );
    }
    Request {
        session: "a".repeat(32),
        operation: n.to_string(),
        command: command.into(),
        payload,
        activation: Identity {
            incarnation: "1".into(),
            fiber: "2".into(),
            epoch: "3".into(),
        },
    }
}
fn request_message(
    n: u64,
    command: &str,
    metadata: Value,
    shape: cordis_sdk_core::MessageContract,
    args: Value,
) -> Request {
    let mut request = request(n, command, metadata);
    request.payload.messages.push(
        cordis_sdk_core::Message::from_json(shape, &args)
            .unwrap()
            .encode(cordis_wire::MAX_MESSAGE)
            .unwrap(),
    );
    request
}
async fn start(host: &Host<Probe>, capabilities: Value) -> Value {
    let ready = host
        .execute(
            &request(
                1,
                "hello",
                json!({
                    "contract":{"name":"test.counter","revision":"1"},
                    "interface":Probe::interface(), "dependency_interfaces":Probe::dependency_interfaces(),
                    "dependencies":{"test.upstream":"1"}, "events":{}, "middleware":{},
                    "composition":{"factories":[],"provisions":{}}, "validation":"checked",
                    "capabilities":capabilities
                }),
            )
        )
        .await
        .unwrap();
    host.execute(&request(2, "activate", json!({})))
        .await
        .unwrap();
    ready.control_only().unwrap()
}

#[tokio::test]
async fn host_without_dependency_resource_capability_is_rejected_locally() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (host, mut output) = fixture();
            let ready = start(&host, json!({})).await;
            assert_eq!(ready["capabilities"]["resources"], true);
            assert_eq!(ready["capabilities"]["dependency_resources"], false);
            let error = host
                .execute(&request(3, "call", json!({"method":"reverse","args":null})))
                .await
                .unwrap_err();
            assert!(matches!(error, Failure::Refused(e) if e.code == "unsupported_resource"));
            assert!(
                output.try_recv().is_err(),
                "unsupported operation must not send a reverse request"
            );
        })
        .await;
}

#[tokio::test]
async fn resource_grants_sessions_and_operation_replay_are_checked_before_business() {
    let (host, _) = fixture();
    let ready = start(&host, json!({"dependency_resources":true})).await;
    assert_eq!(ready["capabilities"]["dependency_resources"], true);
    let owner = "b".repeat(32);
    let acquired = host
        .execute(&request(
            3,
            "resource_acquire",
            json!({"kind":"counter","args":null,"owner":owner}),
        ))
        .await
        .unwrap();
    let id = &acquired.control["resource"];
    assert_eq!(host.plugin.acquired.get(), 1);
    let replay = request(
        3,
        "resource_acquire",
        json!({"kind":"counter","args":null,"owner":owner}),
    );
    assert!(host.execute(&replay).await.is_err());
    assert_eq!(host.plugin.acquired.get(), 1);
    let mut stale = request(4, "resource_release", json!({"resource":id,"owner":owner}));
    stale.session = "d".repeat(32);
    assert!(host.execute(&stale).await.is_err());
    assert!(
        host.execute(&request(
            5,
            "resource_release",
            json!({"resource":id,"owner":"e".repeat(32)})
        ))
        .await
        .is_err()
    );
    assert_eq!(host.plugin.closed.get(), 0);
    assert_eq!(
        host.execute(&request(
            6,
            "resource_call",
            json!({"resource":id,"owner":owner,"method":"get","args":null})
        ))
        .await
        .unwrap()
        .one()
        .map(|bytes| cordis_sdk_core::Message::from_bytes(
            cordis_sdk_core::MessageContract::json_value(),
            bytes
        )
        .unwrap()
        .to_json()
        .unwrap())
        .unwrap(),
        json!(42.0)
    );
    for n in [7, 8] {
        host.execute(&request(
            n,
            "resource_release",
            json!({"resource":id,"owner":owner}),
        ))
        .await
        .unwrap();
    }
    assert_eq!(host.plugin.closed.get(), 1);
    assert!(
        host.execute(&request(
            9,
            "resource_call",
            json!({"resource":id,"owner":owner,"method":"get","args":null})
        ))
        .await
        .is_err()
    );
    host.execute(&request(10, "deactivate", json!({})))
        .await
        .unwrap();
}

#[path = "../../tests/support/json.rs"]
#[allow(dead_code)]
mod test_json;
