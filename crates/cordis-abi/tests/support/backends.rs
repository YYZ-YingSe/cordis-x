use cordis_abi::{AbiResource, AbiService, Library, Value, json};
use cordis_remote::{RemoteResource, RemoteService};
use cordis_sdk::*;
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

pub(crate) trait Counter: Send + Sync {
    fn invoke_json(&self, method: &'static str, args: Value) -> BoxFuture<'_, Result<Value>>;
}
pub(crate) trait Calculator: Send + Sync + 'static {
    fn invoke_json(
        &self,
        ctx: Context,
        method: &'static str,
        args: Value,
    ) -> BoxFuture<'_, Result<Value>>;
    fn acquire(&self, ctx: Context) -> BoxFuture<'_, Result<Box<dyn Counter>>>;
}
macro_rules! adapter {
    ($service:ty, $resource:ty) => {
        impl Calculator for $service {
            fn invoke_json(&self, ctx: Context, method: &'static str, args: Value) -> BoxFuture<'_, Result<Value>> {
                Box::pin(async move { self.invoke_json(&ctx, method, args).await.map_err(Into::into) })
            }
            fn acquire(&self, ctx: Context) -> BoxFuture<'_, Result<Box<dyn Counter>>> {
                Box::pin(async move { Ok(Box::new(self.acquire_json(&ctx, "counter", json!({"initial":40})).await?) as Box<dyn Counter>) })
            }
        }
        impl Counter for $resource {
            fn invoke_json(&self, method: &'static str, args: Value) -> BoxFuture<'_, Result<Value>> {
                Box::pin(async move { self.invoke_json(method, args).await.map_err(Into::into) })
            }
        }
    };
}
#[allow(unused_imports)] // Also reused by cordis-wasm's backend comparison.
pub(crate) use adapter;
adapter!(AbiService, AbiResource);
adapter!(RemoteService, RemoteResource);

struct Native(Arc<AtomicBool>);
impl Calculator for Native {
    fn invoke_json(
        &self,
        _: Context,
        method: &'static str,
        args: Value,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            assert!(self.0.load(Ordering::SeqCst));
            match method {
                "echo" => Ok(args),
                "add" => Ok(json!(
                    args["left"].as_i64().unwrap() + args["right"].as_i64().unwrap()
                )),
                _ => unreachable!(),
            }
        })
    }
    fn acquire(&self, ctx: Context) -> BoxFuture<'_, Result<Box<dyn Counter>>> {
        let open = self.0.clone();
        Box::pin(async move {
            let resource = ctx
                .acquire(
                    "native counter",
                    async { Ok(Mutex::new(40i64)) },
                    move |_, _| async move {
                        assert!(open.load(Ordering::SeqCst));
                        Ok(())
                    },
                )
                .await?;
            Ok(Box::new(resource) as Box<dyn Counter>)
        })
    }
}
impl Counter for Resource<Mutex<i64>> {
    fn invoke_json(&self, method: &'static str, args: Value) -> BoxFuture<'_, Result<Value>> {
        Box::pin(self.call(move |value, _| {
            Box::pin(async move {
                let mut value = value.lock().unwrap();
                if method == "add" {
                    *value += args.as_i64().unwrap();
                }
                Ok(json!(*value))
            })
        }))
    }
}

// Compare current protobuf values. Int64 has canonical decimal strings;
// google.protobuf.Value has double-valued numbers, including integral doubles.
fn integer_result(value: Value) -> Value {
    let shape = cordis_contract::MessageContract::standard("google.protobuf.Int64Value");
    shape.to_json(&shape.parse_json(&value).unwrap()).unwrap()
}

/// A bounded acceptance workload, not a throughput or indefinite soak claim.
/// Every backend runs the same moves, epoch changes and stale-handle checks.
const COMPOSITION_ROUNDS: usize = 16;

async fn composition_cycles<T: Calculator>(service: Service<T>, plugin: Plugin) -> Vec<Value> {
    let rt = Runtime::new();
    let provider = rt.install(plugin).unwrap();
    provider.ready().await.unwrap();
    let saved = Arc::new(Mutex::new(None));
    let trace = Arc::new(Mutex::new(Vec::new()));
    let client = rt
        .install(
            Plugin::new("repeated client", {
                let service = service.clone();
                let saved = saved.clone();
                let trace = trace.clone();
                move |ctx| {
                    let service = service.clone();
                    let saved = saved.clone();
                    let trace = trace.clone();
                    async move {
                        *saved.lock().unwrap() = Some(ctx.dependency(&service)?);
                        ctx.defer("old dependency cleanup", move |ctx| async move {
                            let value = ctx
                                .dependency(&service)?
                                .call(|s, ctx| {
                                    s.invoke_json(ctx, "add", json!({"left":20,"right":22}))
                                })
                                .await?;
                            assert_eq!(integer_result(value), json!("42"));
                            trace.lock().unwrap().push(json!("cleaned"));
                            Ok(())
                        })?;
                        Ok(())
                    }
                }
            })
            .requires(&service),
        )
        .unwrap();
    client.ready().await.unwrap();
    let mut owner = rt
        .install(Plugin::new("initial owner", |_| async { Ok(()) }))
        .unwrap();
    owner.ready().await.unwrap();
    rt.reparent(&[
        (provider.clone(), owner.snapshot().activation),
        (client.clone(), owner.snapshot().activation),
    ])
    .unwrap();
    for round in 0..COMPOSITION_ROUNDS {
        let handle = saved.lock().unwrap().as_ref().unwrap().clone();
        let counter = handle.call(|s, ctx| s.acquire(ctx)).await.unwrap();
        assert_eq!(client.effects().len(), 2);
        let before = (
            provider.snapshot().activation.unwrap(),
            client.snapshot().activation.unwrap(),
        );
        let next = rt
            .install(Plugin::new(format!("owner-{round}"), |_| async { Ok(()) }))
            .unwrap();
        next.ready().await.unwrap();
        // A call stream runs alongside control-plane updates. Existing fault
        // suites separately gate individual in-flight operations deterministically.
        let (started, running) = tokio::sync::oneshot::channel();
        let calls = tokio::spawn({
            let handle = handle.clone();
            async move {
                started.send(()).unwrap();
                for _ in 0..8 {
                    let value = handle
                        .call(|s, ctx| s.invoke_json(ctx, "add", json!({"left":20,"right":22})))
                        .await
                        .unwrap();
                    assert_eq!(integer_result(value), json!("42"));
                    tokio::task::yield_now().await;
                }
            }
        });
        running.await.unwrap();
        rt.reparent(&[
            (provider.clone(), next.snapshot().activation),
            (client.clone(), next.snapshot().activation),
        ])
        .unwrap();
        let view = View::new().isolate(&service);
        assert!(
            rt.reconfigure_realms(&[(provider.clone(), view.clone()), (client.clone(), view)])
                .unwrap()
                .withdrawn
                .is_empty()
        );
        owner.dispose().await.unwrap();
        calls.await.unwrap();
        assert_eq!(
            (
                provider.snapshot().activation.unwrap(),
                client.snapshot().activation.unwrap()
            ),
            before
        );
        assert_eq!(
            integer_result(counter.invoke_json("add", json!(round + 1)).await.unwrap()),
            json!((41 + round).to_string())
        );
        trace
            .lock()
            .unwrap()
            .push(json!({"round": round, "value": 41 + round}));
        // Restart preserves the Fiber but must expire this epoch's handles and
        // recover its caller-owned resource before destroying the old provider.
        provider.restart().await.unwrap();
        client.ready().await.unwrap();
        assert_eq!(
            provider.snapshot().activation.unwrap().fiber,
            before.0.fiber
        );
        assert_eq!(
            provider.snapshot().activation.unwrap().epoch,
            before.0.epoch + 1
        );
        assert_eq!(
            client.snapshot().activation.unwrap().epoch,
            before.1.epoch + 1
        );
        assert!(counter.invoke_json("get", json!({})).await.is_err());
        assert!(matches!(
            handle
                .call(|s, ctx| s.invoke_json(ctx, "echo", Value::Null))
                .await,
            Err(Error::Closed)
        ));
        assert_eq!(client.effects().len(), 1);
        assert_eq!(
            rt.snapshots()
                .iter()
                .filter(|s| s.state != Lifecycle::Inactive)
                .count(),
            3
        );
        owner = next;
    }
    owner.dispose().await.unwrap();
    rt.shutdown().await.unwrap();
    assert!(
        rt.snapshots()
            .iter()
            .all(|s| s.state == Lifecycle::Inactive)
    );
    assert!(rt.diagnostics().is_empty());
    assert_eq!(
        trace
            .lock()
            .unwrap()
            .iter()
            .filter(|v| **v == json!("cleaned"))
            .count(),
        COMPOSITION_ROUNDS + 1
    );
    trace.lock().unwrap().clone()
}

// One application scenario runs unchanged across native/ABI/IPC/HTTP. Trace
// observes results, consumer inverse calls, caller ownership, and stale handles.
pub(crate) async fn scenario<T: Calculator>(service: Service<T>, plugin: Plugin) -> Vec<Value> {
    let cycles = tokio::time::timeout(
        Duration::from_secs(120),
        composition_cycles(service.clone(), plugin.clone()),
    )
    .await
    .expect("repeated composition did not drain");
    let rt = Runtime::new();
    let provider = rt.install(plugin).unwrap();
    provider.ready().await.unwrap();
    let saved = Arc::new(Mutex::new(None));
    let trace = Arc::new(Mutex::new(Vec::new()));
    let client = rt
        .install(
            Plugin::new("client", {
                let service = service.clone();
                let saved = saved.clone();
                let trace = trace.clone();
                move |ctx| {
                    let service = service.clone();
                    let saved = saved.clone();
                    let trace = trace.clone();
                    async move {
                        *saved.lock().unwrap() = Some(ctx.dependency(&service)?);
                        ctx.defer(
                            "consumer cleanup invokes old provider",
                            move |ctx| async move {
                                let value = ctx
                                    .dependency(&service)?
                                    .call(|service, ctx| {
                                        service.invoke_json(
                                            ctx,
                                            "add",
                                            json!({"left":20,"right":22}),
                                        )
                                    })
                                    .await?;
                                trace
                                    .lock()
                                    .unwrap()
                                    .push(json!({"cleanup":integer_result(value)}));
                                Ok(())
                            },
                        )?;
                        Ok(())
                    }
                }
            })
            .requires(&service),
        )
        .unwrap();
    client.ready().await.unwrap();
    let handle = saved.lock().unwrap().take().unwrap();
    for (method, args) in [
        ("echo", json!({"中文":[null,true,42]})),
        ("add", json!({"left":20,"right":22})),
    ] {
        let value = handle
            .call(move |service, ctx| service.invoke_json(ctx, method, args))
            .await
            .unwrap();
        trace.lock().unwrap().push(if method == "add" {
            integer_result(value)
        } else {
            let shape = cordis_contract::MessageContract::standard("google.protobuf.Value");
            shape.to_json(&shape.parse_json(&value).unwrap()).unwrap()
        });
    }
    let counter = handle
        .call(|service, ctx| service.acquire(ctx))
        .await
        .unwrap();
    let value = integer_result(counter.invoke_json("add", json!(2)).await.unwrap());
    trace.lock().unwrap().push(value);
    assert_eq!(client.effects().len(), 2); // consumer inverse + acquired resource

    let old_group = rt
        .install(Plugin::new("old group", |_| async { Ok(()) }))
        .unwrap();
    let new_group = rt
        .install(Plugin::new("new group", |_| async { Ok(()) }))
        .unwrap();
    old_group.ready().await.unwrap();
    new_group.ready().await.unwrap();
    rt.reparent(&[
        (provider.clone(), old_group.snapshot().activation),
        (client.clone(), old_group.snapshot().activation),
    ])
    .unwrap();

    // Moving discovery addresses together must preserve the actual foreign
    // instance, cached caller handle and the counter created in the old realm.
    let old_provider = provider.snapshot().activation;
    let old_client = client.snapshot().activation;
    rt.reparent(&[
        (provider.clone(), new_group.snapshot().activation),
        (client.clone(), new_group.snapshot().activation),
    ])
    .unwrap();
    old_group.dispose().await.unwrap();
    assert_eq!(provider.snapshot().activation, old_provider);
    assert_eq!(client.snapshot().activation, old_client);
    let moved = View::new().isolate(&service);
    let report = rt
        .reconfigure_realms(&[
            (provider.clone(), moved.clone()),
            (client.clone(), moved.clone()),
        ])
        .unwrap();
    assert!(report.withdrawn.is_empty());
    assert_eq!(provider.snapshot().activation, old_provider);
    assert_eq!(client.snapshot().activation, old_client);
    let value = integer_result(counter.invoke_json("get", json!({})).await.unwrap());
    assert_eq!(value, json!("42"));
    trace.lock().unwrap().push(json!({"migrated": value}));
    let fresh = rt
        .install_in(
            moved,
            Plugin::new("new realm caller", {
                let service = service.clone();
                move |ctx| {
                    let service = service.clone();
                    async move {
                        let value = ctx
                            .dependency(&service)?
                            .call(|service, ctx| {
                                service.invoke_json(ctx, "add", json!({"left":20,"right":22}))
                            })
                            .await?;
                        assert_eq!(integer_result(value), json!("42"));
                        Ok(())
                    }
                }
            })
            .requires(&service),
        )
        .unwrap();
    fresh.ready().await.unwrap();
    new_group.dispose().await.unwrap();
    assert!(provider.snapshot().retired);
    assert!(client.snapshot().retired);
    assert!(
        counter
            .invoke_json("get", cordis_abi::json!({}))
            .await
            .is_err()
    );
    assert!(matches!(
        handle
            .call(|service, ctx| service.invoke_json(ctx, "echo", Value::Null))
            .await,
        Err(Error::Closed)
    ));
    rt.shutdown().await.unwrap();
    let mut result = trace.lock().unwrap().clone();
    result.extend(cycles);
    result
}

fn build(source: &str, target: &str, artifact: &str, cmake_target: &str) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let target = root.join(target);
    for args in [
        vec![
            if cmake_target == "cordis_calculator" {
                "-DCORDIS_BUILD_ABI=OFF".into()
            } else {
                "-DCORDIS_BUILD_PROCESS=OFF".into()
            },
            "-DCORDIS_BUILD_EXAMPLES=ON".into(),
            "-DBUILD_TESTING=ON".into(),
            "-S".into(),
            root.join(source).into_os_string(),
            "-B".into(),
            target.clone().into_os_string(),
        ],
        vec![
            "--build".into(),
            target.clone().into_os_string(),
            "--target".into(),
            cmake_target.into(),
            "--config".into(),
            "Debug".into(),
            "-j".into(),
            "2".into(),
        ],
    ] {
        let output = Command::new("cmake").args(&args).output().unwrap();
        assert!(
            output.status.success(),
            "cmake {args:?} failed ({}):\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let path = target.join(artifact);
    if path.exists() {
        path
    } else {
        target.join("Debug").join(artifact)
    }
}
pub(crate) fn compare(
    endpoint: Option<cordis_remote::Endpoint>,
) -> impl std::future::Future<Output = Vec<Value>> {
    static ABI: OnceLock<PathBuf> = OnceLock::new();
    let path = ABI.get_or_init(|| {
        build(
            "sdk/cpp",
            "target/abi-backends",
            &format!(
                "{}cordis_abi_calculator{}",
                std::env::consts::DLL_PREFIX,
                std::env::consts::DLL_SUFFIX
            ),
            "cordis_abi_calculator",
        )
    });
    // The C++ process adapter requires POSIX. TypeScript IPC/HTTP does not;
    // keep its acceptance scenario available on Windows as well.
    let executable = if endpoint.is_some() && cfg!(unix) {
        static PROCESS: OnceLock<PathBuf> = OnceLock::new();
        let executable = PROCESS.get_or_init(|| {
            build(
                "sdk/cpp",
                "target/abi-process-backends",
                "cordis_calculator",
                "cordis_calculator",
            )
        });
        Some(executable)
    } else {
        None
    };
    async move {
        let service = Service::<Native>::new("example.calculator", "1");
        let native = Plugin::new("native", {
            let service = service.clone();
            move |ctx| {
                let service = service.clone();
                async move {
                    let open = Arc::new(AtomicBool::new(true));
                    let close = open.clone();
                    ctx.defer("native provider", move |_| async move {
                        close.store(false, Ordering::SeqCst);
                        Ok(())
                    })?;
                    ctx.provide(&service, Arc::new(Native(open)))
                }
            }
        })
        .provides(&service);
        let expected = scenario(service, native).await;
        // SAFETY: the library is built from our trusted C++ example immediately above.
        #[allow(unsafe_code)]
        let library = unsafe { Library::load(path) }.unwrap();
        let service = Service::<AbiService>::new("example.calculator", "1").with_descriptor(
            cordis_contract::portable::decode(
                &cordis_example_contracts::resources::calculator_abi::SERVICE
                    .parse()
                    .unwrap(),
            )
            .unwrap(),
        );
        let plugin = library
            .plugin("abi", &service, cordis_abi::json!({}))
            .unwrap();
        assert_eq!(scenario(service, plugin).await, expected);
        if let Some(endpoint) = endpoint {
            let service = Service::<RemoteService>::new("example.calculator", "1").with_descriptor(
                cordis_contract::portable::decode(
                    &cordis_example_contracts::resources::calculator_process::SERVICE
                        .parse()
                        .unwrap(),
                )
                .unwrap(),
            );
            if let Some(executable) = executable {
                let mut spec = cordis_remote::ProcessSpec::new(executable);
                spec.endpoint = endpoint;
                if matches!(endpoint, cordis_remote::Endpoint::Ipc) {
                    spec.args.push("--stdio-only".into());
                }
                assert_eq!(
                    scenario(service.clone(), spec.plugin("process", &service)).await,
                    expected
                );
            }
            let mut spec = cordis_remote::ProcessSpec::new("node");
            spec.args.push(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../../sdk/typescript/examples/calculator.ts")
                    .to_string_lossy()
                    .into_owned(),
            );
            spec.endpoint = endpoint;
            assert_eq!(
                scenario(service.clone(), spec.plugin("typescript", &service)).await,
                expected
            );
        }
        expected
    }
}
