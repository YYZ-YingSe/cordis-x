use cordis_remote::{Endpoint, ProcessSpec, RemoteResource, RemoteService, json};
use cordis_sdk::*;
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

fn wire_json(value: cordis_remote::Value) -> cordis_remote::Bytes {
    cordis_sdk_core::Message::from_json(cordis_sdk_core::MessageContract::json_value(), &value)
        .unwrap()
        .encode(cordis_wire::MAX_MESSAGE)
        .unwrap()
}
fn unwire_json(value: cordis_remote::Bytes) -> cordis_remote::Value {
    cordis_sdk_core::Message::from_bytes(cordis_sdk_core::MessageContract::json_value(), value)
        .unwrap()
        .to_json()
        .unwrap()
}
static NEXT: AtomicUsize = AtomicUsize::new(0);
async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(15), future)
        .await
        .expect("remote operation did not settle")
}
fn calculator() -> Service<RemoteService> {
    Service::new("example.calculator", "1").with_descriptor(
        cordis_contract::portable::decode(
            &cordis_example_contracts::resources::calculator_process::SERVICE
                .parse()
                .unwrap(),
        )
        .unwrap(),
    )
}
async fn call(
    handle: &ServiceHandle<RemoteService>,
    endpoint: Endpoint,
    method: &str,
    args: cordis_remote::Value,
) -> Result<cordis_remote::Value> {
    let method = method.to_owned();
    handle
        .call(move |remote, ctx| {
            Box::pin(async move {
                remote
                    .invoke_json_via(&ctx, endpoint, &method, args)
                    .await
                    .map_err(Into::into)
            })
        })
        .await
}

struct RawProcess {
    child: tokio::process::Child,
    input: tokio::process::ChildStdin,
    output: tokio::process::ChildStdout,
    session: String,
    next: u64,
    results: std::collections::BTreeMap<String, cordis_contract::MessageContract>,
}
impl RawProcess {
    async fn sdk(backend: Backend) -> Self {
        use std::process::Stdio;
        let spec = backend.spec("calculator");
        let mut command = tokio::process::Command::new(spec.executable);
        command.args(spec.args);
        // This harness only exchanges stdio frames; HTTP has separate coverage.
        command.arg("--stdio-only");
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let output = child.stdout.take().unwrap();
        let mut result = Self {
            child,
            input,
            output,
            session: "0123456789abcdef0123456789abcdef".into(),
            next: 0,
            results: Default::default(),
        };
        let request = result.request(
            "hello",
            json!({"validation":"checked", "contract":{"name":"example.calculator","revision":"1"}, "interface": cordis_contract::portable::encode(calculator().descriptor().unwrap()), "dependency_interfaces": {}}),
        );
        assert_eq!(result.exchange(request).await["outcome"]["kind"], "success");
        let request = result.request("activate", json!(null));
        assert_eq!(result.exchange(request).await["outcome"]["kind"], "success");
        result
    }
    fn request(&mut self, command: &str, payload: cordis_remote::Value) -> cordis_remote::Value {
        self.next += 1;
        json!({"session":self.session,"activation":{"incarnation":"1","fiber":"1","epoch":"1"},
            "operation":self.next.to_string(),"command":command,"payload":payload})
    }
    // JSON is the assertion language of this fault-injection fixture. Its wire
    // is exclusively the generated protobuf carrier with independent bodies.
    fn encode(&mut self, request: &cordis_remote::Value) -> cordis_wire::Bytes {
        let command = request["command"].as_str().unwrap();
        let operation = request["operation"].as_str().unwrap().to_owned();
        let mut control = request["payload"].clone();
        let descriptor = calculator().descriptor().unwrap().clone();
        let schema = match command {
            "call" => cordis_contract::portable::method(
                &descriptor.interface().methods,
                control["method"].as_str().unwrap(),
            )
            .ok()
            .map(|s| (s.args.clone(), Some(s.result.clone()))),
            "resource_acquire" => {
                cordis_contract::portable::resource(&descriptor, control["kind"].as_str().unwrap())
                    .ok()
                    .map(|s| (s.args.clone(), None))
            }
            "resource_call" => cordis_contract::portable::resource(&descriptor, "counter")
                .ok()
                .and_then(|r| {
                    cordis_contract::portable::method(
                        &r.methods,
                        control["method"].as_str().unwrap(),
                    )
                    .ok()
                })
                .map(|s| (s.args.clone(), Some(s.result.clone()))),
            _ => None,
        };
        let mut messages = vec![];
        if matches!(command, "call" | "resource_acquire" | "resource_call")
            && let Some(args) = control.as_object_mut().and_then(|m| m.remove("args"))
        {
            let body = if let Some((shape, result)) = schema {
                if let Some(result) = result {
                    self.results.insert(operation.clone(), result);
                }
                // Invalid semantic input is deliberately sent as malformed
                // protobuf so receiver-side refusal is tested before entry.
                cordis_sdk_core::Message::from_json(shape, &args)
                    .and_then(|v| v.encode(cordis_wire::MAX_MESSAGE))
                    .unwrap_or_else(|_| vec![255].into())
            } else {
                vec![255].into()
            };
            messages.push(body);
        }
        cordis_wire::Request {
            session: request["session"].as_str().unwrap().into(),
            activation: serde_json::from_value(request["activation"].clone()).unwrap(),
            operation,
            command: command.into(),
            payload: cordis_wire::Payload { control, messages },
        }
        .encode()
        .unwrap()
    }
    fn decode(&self, bytes: Vec<u8>) -> cordis_remote::Value {
        let cordis_wire::Incoming::Response(response) = cordis_wire::decode(bytes.into()).unwrap()
        else {
            panic!("response expected")
        };
        let outcome = match response.outcome {
            cordis_wire::Outcome::Success { value } => {
                let value = if value.messages.is_empty() {
                    value.control_only().unwrap()
                } else {
                    cordis_sdk_core::Message::from_bytes(
                        self.results[&response.operation].clone(),
                        value.one().unwrap(),
                    )
                    .unwrap()
                    .to_json()
                    .unwrap()
                };
                json!({"kind":"success","value":value})
            }
            cordis_wire::Outcome::Failure { error } => json!({"kind":"failure","error":error}),
            cordis_wire::Outcome::Refused { error } => json!({"kind":"refused","error":error}),
            cordis_wire::Outcome::Unknown { error } => json!({"kind":"unknown","error":error}),
        };
        json!({"session":response.session,"activation":response.activation,"operation":response.operation,"outcome":outcome})
    }
    async fn exchange(&mut self, request: cordis_remote::Value) -> cordis_remote::Value {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let bytes = self.encode(&request);
        self.input.write_u32(bytes.len() as u32).await.unwrap();
        self.input.write_all(&bytes).await.unwrap();
        self.input.flush().await.unwrap();
        let length = bounded(self.output.read_u32()).await.unwrap();
        assert!(length <= cordis_wire::MAX_MESSAGE as u32);
        let mut bytes = vec![0; length as usize];
        self.output.read_exact(&mut bytes).await.unwrap();
        self.decode(bytes)
    }
    async fn finish(mut self) {
        let request = self.request("deactivate", json!(null));
        assert_eq!(self.exchange(request).await["outcome"]["kind"], "success");
        assert!(bounded(self.child.wait()).await.unwrap().success());
    }
}

#[tokio::test]
async fn sdks_reject_stale_identity_duplicate_operation() {
    for &backend in Backend::ALL {
        let mut process = RawProcess::sdk(backend).await;
        let request = process.request("call", json!({"method":"echo","args":42}));
        assert_eq!(
            process.exchange(request.clone()).await["outcome"]["value"],
            json!(42.0)
        );
        assert_eq!(
            process.exchange(request).await["outcome"]["error"]["code"],
            "duplicate_operation"
        );
        for field in ["session", "epoch"] {
            let mut request = process.request("call", json!({"method":"echo","args":42}));
            if field == "session" {
                request[field] = json!("fedcba9876543210fedcba9876543210");
            } else {
                request["activation"][field] = json!("2");
            }
            assert_eq!(
                process.exchange(request).await["outcome"]["error"]["code"],
                "stale_session"
            );
        }
        process.finish().await;
    }
}

#[tokio::test]
async fn sdks_rejects_malformed_frames_and_ambiguous_payloads_without_unbounded_allocation() {
    use tokio::io::AsyncWriteExt;
    for &backend in Backend::ALL {
        for bytes in [
            vec![0, 0],                         // truncated u32 header
            vec![0, 0, 0, 9, 0x0a, 1],          // truncated protobuf frame
            vec![0xff; 4],                      // excessive declared length
            vec![0, 0, 0, 1, 0x80],             // unterminated protobuf varint
            vec![0, 0, 0, 3, 0x0a, 0xff, 0x7f], // impossible field length
            vec![0, 0, 0, 2, 0x78, 1],          // unknown field / missing frame direction
        ] {
            let mut process = RawProcess::sdk(backend).await;
            process.input.write_all(&bytes).await.unwrap();
            process.input.shutdown().await.unwrap();
            // ChildStdin shutdown alone does not close the pipe's write end.
            drop(process.input);
            assert!(!bounded(process.child.wait()).await.unwrap().success());
        }
    }
}

#[tokio::test]
async fn rust_rejects_mismatched_peer_responses_and_keeps_cleanup_unconfirmed() {
    for fault in [
        "unknown_field",
        "session",
        "activation",
        "operation",
        "truncated",
    ] {
        let runtime = Runtime::new();
        let mut spec = python_spec();
        if fault == "unknown_field" {
            spec.validation = cordis_contract::Validation::Audit;
        }
        spec.args.extend([
            format!(
                "{}/tests/fixtures/faulty_peer.py",
                env!("CARGO_MANIFEST_DIR")
            ),
            fault.into(),
        ]);
        let saved = Arc::new(Mutex::new(None));
        let consumer = runtime
            .install(
                Plugin::new("consumer", {
                    let saved = saved.clone();
                    move |ctx| {
                        let saved = saved.clone();
                        async move {
                            *saved.lock().unwrap() = Some(ctx.dependency(&calculator())?);
                            Ok(())
                        }
                    }
                })
                .requires(&calculator()),
            )
            .unwrap();
        let provider = runtime
            .install(spec.plugin("faulty", &calculator()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        bounded(consumer.ready()).await.unwrap();
        let handle = saved.lock().unwrap().take().unwrap();
        let error = bounded(call(&handle, Endpoint::Ipc, "echo", json!("42")))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Protocol"), "{fault}: {error}");
        assert!(bounded(provider.settled()).await.is_err());
        assert!(bounded(runtime.shutdown()).await.is_err());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typescript_host_uses_the_same_runtime_contract_and_both_endpoints() {
    let runtime = Runtime::new();
    let mut spec = ProcessSpec::new("node");

    spec.args = vec![format!(
        "{}/../../sdk/typescript/examples/calculator.ts",
        env!("CARGO_MANIFEST_DIR")
    )];
    let saved = Arc::new(Mutex::new(None));
    let consumer = runtime
        .install(
            Plugin::new("consumer", {
                let saved = saved.clone();
                move |ctx| {
                    let saved = saved.clone();
                    async move {
                        *saved.lock().unwrap() = Some(ctx.dependency(&calculator())?);
                        ctx.defer("cleanup", |ctx| async move {
                            assert_eq!(
                                call(
                                    &ctx.dependency(&calculator())?,
                                    Endpoint::Http,
                                    "add",
                                    json!({"left":20,"right":22})
                                )
                                .await?,
                                json!("42")
                            );
                            Ok(())
                        })?;
                        Ok(())
                    }
                }
            })
            .requires(&calculator()),
        )
        .unwrap();
    let provider = runtime
        .install(spec.plugin("typescript", &calculator()))
        .unwrap();
    bounded(provider.ready()).await.unwrap();
    bounded(consumer.ready()).await.unwrap();
    let handle = saved.lock().unwrap().take().unwrap();
    let mut resources = Vec::new();
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        let input = json!({"unicode":"中文 🦀", "values":[null,true,-9_007_199_254_740_991f64]});
        assert_eq!(
            call(&handle, endpoint, "echo", input.clone())
                .await
                .unwrap(),
            input
        );
        assert_eq!(
            call(&handle, endpoint, "add", json!({"left":20,"right":22}))
                .await
                .unwrap(),
            json!("42")
        );
        let resource = acquire(&handle, endpoint, json!({"initial":40}))
            .await
            .unwrap();
        assert_eq!(
            resource.invoke_json("add", json!(2)).await.unwrap(),
            json!("42")
        );
        resources.push(resource);
    }
    bounded(provider.restart()).await.unwrap();
    bounded(consumer.ready()).await.unwrap();
    for resource in resources {
        assert!(matches!(
            resource.invoke_json("get", json!({})).await,
            Err(cordis_remote::Error::Closed)
        ));
    }
    bounded(runtime.shutdown()).await.unwrap();
}

async fn acquire(
    handle: &ServiceHandle<RemoteService>,
    endpoint: Endpoint,
    args: cordis_remote::Value,
) -> Result<RemoteResource> {
    handle
        .call(move |remote, ctx| {
            Box::pin(async move {
                remote
                    .acquire_json_via(&ctx, endpoint, "counter", args)
                    .await
                    .map_err(Into::into)
            })
        })
        .await
}

#[tokio::test]
async fn normal_calculator_sdks_reject_invalid_fields_and_fault_hooks_before_execution() {
    for &backend in Backend::ALL {
        let mut process = RawProcess::sdk(backend).await;
        for (method, args) in [
            ("add", json!({"left": 1})),
            ("add", json!({"left": "bad", "right": 2})),
            ("add", json!({"left": 1, "right": 2, "offset": 3})),
            ("code-version", json!(null)),
            ("metadata-for", json!({"name": "example.calculator"})),
            ("hold", json!("must-not-block")),
            ("crash", json!({})),
        ] {
            let request = process.request("call", json!({"method":method,"args":args}));
            let reply = process.exchange(request).await;
            assert_eq!(reply["outcome"]["kind"], "refused");
            assert_eq!(reply["outcome"]["error"]["code"], "invalid_argument");
        }
        let owner = "0123456789abcdef0123456789abcdef";
        for args in [
            json!({"initial": []}),
            json!({"initial": "bad"}),
            json!({"wait": "must-not-block"}),
            json!({"fail_close": true}),
        ] {
            let request = process.request(
                "resource_acquire",
                json!({"kind":"counter","args":args,"owner":owner}),
            );
            assert_eq!(
                process.exchange(request).await["outcome"]["kind"],
                "refused"
            );
        }
        let request = process.request(
            "resource_acquire",
            json!({"kind":"counter","args":{},"owner":owner}),
        );
        let reply = process.exchange(request).await;
        assert_eq!(reply["outcome"]["kind"], "success");
        let id = reply["outcome"]["value"]["resource"].clone();
        for (method, args) in [
            ("add", json!("bad")),
            ("get", json!(null)),
            ("crash", json!({})),
        ] {
            let request = process.request(
                "resource_call",
                json!({"resource":id,"owner":owner,"method":method,"args":args}),
            );
            assert_eq!(
                process.exchange(request).await["outcome"]["kind"],
                "refused"
            );
        }
        let request = process.request(
            "resource_call",
            json!({"resource":id,"owner":owner,"method":"get","args":{}}),
        );
        assert_eq!(process.exchange(request).await["outcome"]["value"], "0");
        let request = process.request("resource_release", json!({"resource":id,"owner":owner}));
        assert_eq!(
            process.exchange(request).await["outcome"]["kind"],
            "success"
        );
        let request = process.request(
            "call",
            json!({"method":"add","args":{"left":"9223372036854775807","right":1}}),
        );
        let reply = process.exchange(request).await;
        assert_eq!(reply["outcome"]["kind"], "failure");
        assert_eq!(reply["outcome"]["error"]["code"], "overflow");
        let request = process.request(
            "call",
            json!({"method":"add","args":{"left":20,"right":22}}),
        );
        assert_eq!(process.exchange(request).await["outcome"]["value"], "42");
        process.finish().await;
    }
}

#[path = "support/dependencies.rs"]
mod dependencies;

#[path = "support/intercepts.rs"]
mod intercepts;
#[cfg(unix)]
#[path = "support/loader.rs"]
mod loader;
#[cfg(feature = "loader")]
#[path = "support/updates.rs"]
mod updates;

#[path = "support/effects.rs"]
mod effects;

#[path = "support/sequences.rs"]
mod sequences;

#[path = "support/scopes.rs"]
mod scopes;

#[path = "support/tasks.rs"]
mod tasks;

#[path = "support/events.rs"]
mod events;

#[path = "support/middleware.rs"]
mod middleware;

#[path = "support/composition.rs"]
mod composition;

#[path = "support/backends.rs"]
mod backends;
use backends::{Backend, python, python_spec};

#[cfg(unix)]
#[path = "support/cpp.rs"]
mod cpp;
#[cfg(unix)]
use cpp::{binary, test_calculator};
