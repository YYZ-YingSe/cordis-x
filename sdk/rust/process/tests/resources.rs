use cordis_portable_example::resources::{CounterClient, CounterPlugin};
use cordis_remote::{
    Dependency, Endpoint, ProcessSpec, RemoteResource, RemoteService, Value, json,
};
use cordis_runtime::{self as rt, Plugin, Runtime, Service};
use cordis_sdk_runtime as native;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

#[path = "../../tests/support/cpp_resources.rs"]
mod cpp;

#[path = "support/resource_faults.rs"]
mod faults;

async fn bounded<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(20), f)
        .await
        .expect("resource scenario timed out")
}
fn artifact() -> &'static Path {
    static BUILD: OnceLock<PathBuf> = OnceLock::new();
    BUILD.get_or_init(|| {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let target = root.join("target/rust-process-sdk");
        let output = std::process::Command::new(env!("CARGO"))
            .current_dir(&root)
            .args([
                "build",
                "-p",
                "cordis-sdk-process",
                "--example",
                "resource_plugin",
                "--example",
                "resource_probe",
                "--offline",
                "--locked",
                "--target-dir",
            ])
            .arg(&target)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        target
            .join("debug/examples")
            .join(format!("resource_plugin{}", std::env::consts::EXE_SUFFIX))
    })
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Language {
    Rust,
    Cpp,
    TypeScript,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn structural_contracts_reject_drift_and_invalid_inputs_across_languages() {
    use cordis_remote::{Descriptor, Dispatch, Interface, MessageContract};
    let mut languages = vec![Language::Rust, Language::TypeScript];
    if cfg!(unix) {
        languages.push(Language::Cpp);
    }
    for language in languages {
        for fault in 0..3 {
            let runtime = Runtime::new();
            let logger = Service::<u32>::new("example.counter-log", "1");
            let binding = logger.clone();
            let provider = runtime
                .install(
                    Plugin::new("logger", move |ctx| {
                        let binding = binding.clone();
                        async move { ctx.provide(&binding, Arc::new(0)) }
                    })
                    .provides(&logger),
                )
                .unwrap();
            provider.ready().await.unwrap();
            let mut interface = <CounterPlugin as cordis_sdk_core::Plugin>::interface();
            if fault == 0
                && let Dispatch::Named(resources) = &mut interface.resources
            {
                resources.get_mut("counter").unwrap().args =
                    MessageContract::standard("google.protobuf.StringValue");
            }
            let counters = Service::<RemoteService>::new("example.counters", "1")
                .with_descriptor(Descriptor::new(interface).unwrap());
            let dependency = Descriptor::new(if fault == 1 {
                Interface::value_dispatch(false)
            } else {
                cordis_portable_example::resources::generated::CounterLogClient::interface()
            })
            .unwrap();
            let plugin = runtime
                .install(
                    language_spec(Endpoint::Ipc, false, language)
                        .requires(Dependency::new_json(&logger, dependency, |_, _, _, _| {
                            Box::pin(async { Ok(json!({})) })
                        }))
                        .plugin("counters", &counters),
                )
                .unwrap();
            let ready = bounded(plugin.ready()).await;
            if fault < 2 {
                let error = ready.unwrap_err().to_string();
                assert!(
                    error.contains(if fault == 0 {
                        "args"
                    } else {
                        "example.counter-log"
                    }),
                    "{language:?}: {error}"
                );
            } else {
                ready.unwrap();
                let (_, handle) = owner(&runtime, &counters).await;
                let invalid = handle
                    .call(|s, ctx| {
                        Box::pin(async move {
                            s.acquire_json(&ctx, "counter", json!("bad"))
                                .await
                                .map_err(Into::into)
                        })
                    })
                    .await;
                assert!(invalid.is_err());
                let resource = handle
                    .call(|s, ctx| {
                        Box::pin(async move {
                            s.acquire_json(&ctx, "counter", json!(10))
                                .await
                                .map_err(Into::into)
                        })
                    })
                    .await
                    .unwrap();
                assert!(matches!(
                    resource.invoke_json("add", json!("bad")).await,
                    Err(cordis_remote::Error::InvalidInput(_))
                ));
                assert!(resource.invoke_json("unknown", json!({})).await.is_err());
                assert_eq!(
                    resource
                        .invoke_json("get", cordis_sdk_core::json!({}))
                        .await
                        .unwrap(),
                    json!("10")
                );
                assert_eq!(
                    resource.invoke_json("add", json!(2)).await.unwrap(),
                    json!("12")
                );
                let client_service = Service::<RemoteService>::new("example.counter-client", "1")
                    .with_descriptor(Descriptor::new(cordis_portable_example::resources::generated::CounterHolderClient::interface()).unwrap());
                let client = runtime
                    .install(
                        language_spec(Endpoint::Ipc, true, language)
                            .requires(Dependency::remote(&counters))
                            .plugin("client", &client_service),
                    )
                    .unwrap();
                bounded(client.ready()).await.unwrap();
                let (_, handle) = owner(&runtime, &client_service).await;
                let client = Binding::Process(handle);
                client.invoke_json("acquire", json!(10)).await.unwrap();
                let rejected = client
                    .invoke_json("add", json!("bad"))
                    .await
                    .unwrap_err()
                    .to_string();
                assert!(
                    rejected.contains("InvalidInput"),
                    "{language:?}: {rejected}"
                );
                assert_eq!(
                    client
                        .invoke_json("get", cordis_sdk_core::json!({}))
                        .await
                        .unwrap(),
                    json!("10")
                );
            }
            bounded(runtime.shutdown()).await.unwrap();
        }
    }
}

#[tokio::test]
async fn process_binding_without_a_descriptor_is_rejected_before_launch() {
    let runtime = Runtime::new();
    let service = Service::<RemoteService>::new("test.missing", "1");
    let plugin = runtime
        .install(ProcessSpec::new("/no/such/cordis-test-executable").plugin("missing", &service))
        .unwrap();
    let error = plugin.ready().await.unwrap_err().to_string();
    assert!(
        error.contains("process interface descriptor required"),
        "{error}"
    );
    runtime.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_dependency_result_is_unknown_after_native_execution() {
    use cordis_remote::{Descriptor, Dispatch, Interface, MessageContract};
    use std::sync::atomic::{AtomicBool, Ordering};
    let runtime = Runtime::new();
    let logger = Service::<u32>::new("example.logger", "1");
    let binding = logger.clone();
    runtime
        .install(
            Plugin::new("logger", move |ctx| {
                let binding = binding.clone();
                async move { ctx.provide(&binding, Arc::new(0)) }
            })
            .provides(&logger),
        )
        .unwrap()
        .ready()
        .await
        .unwrap();
    let mut interface = Interface::value_dispatch(false);
    if let Dispatch::Dynamic(method) = &mut interface.methods {
        method.result = MessageContract::standard("google.protobuf.Int64Value");
    }
    let called = Arc::new(AtomicBool::new(false));
    let observed = called.clone();
    let python = std::env::var_os("CORDIS_TEST_PYTHON")
        .unwrap_or_else(|| if cfg!(windows) { "python" } else { "python3" }.into());
    let mut spec = ProcessSpec::new(python).requires(Dependency::new_json(
        &logger,
        Descriptor::new(interface).unwrap(),
        move |_, _, _, _| {
            observed.store(true, Ordering::SeqCst);
            Box::pin(async { Ok(json!("wrong result type")) })
        },
    ));
    spec.args.push(cordis_test_contracts::PYTHON_RUNNER.into());
    spec.args.push(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../crates/cordis-remote/tests/fixtures/dependency_peer.py")
            .to_string_lossy()
            .into_owned(),
    );
    let service = Service::<RemoteService>::new("example.probe", "1")
        .with_descriptor(Descriptor::new(Interface::value_dispatch(true)).unwrap());
    let provider = runtime.install(spec.plugin("peer", &service)).unwrap();
    bounded(provider.ready()).await.unwrap();
    let (_, handle) = owner(&runtime, &service).await;
    let result =
        bounded(handle.call(|s, ctx| {
            Box::pin(async move { Ok(s.invoke_json(&ctx, "probe", json!({})).await) })
        }))
        .await
        .unwrap();
    assert!(called.load(Ordering::SeqCst));
    assert!(
        matches!(result, Err(cordis_remote::Error::Protocol(_))),
        "{result:?}"
    );
    assert!(bounded(runtime.shutdown()).await.is_err());
}
fn typescript_spec(path: &str) -> ProcessSpec {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let mut spec = ProcessSpec::new("node");
    spec.args.push(
        root.join("sdk/typescript")
            .join(path)
            .to_string_lossy()
            .into_owned(),
    );
    spec
}
fn language_spec(endpoint: Endpoint, client: bool, language: Language) -> ProcessSpec {
    let mut spec = match language {
        Language::Rust => ProcessSpec::new(artifact()),
        Language::Cpp => ProcessSpec::new(cpp::build().join("cordis_resources_process")),
        Language::TypeScript => typescript_spec("examples/resources/process.ts"),
    };
    spec.endpoint = endpoint;

    if matches!(endpoint, Endpoint::Ipc) {
        spec.args.push("--stdio-only".into());
    }
    if client {
        spec.args.push("--client".into());
    }
    spec
}
enum Binding {
    Native(rt::ServiceHandle<native::Service>),
    Process(rt::ServiceHandle<RemoteService>),
}
impl Binding {
    async fn invoke_json(&self, method: &'static str, args: Value) -> rt::Result<Value> {
        match self {
            Self::Native(h) => {
                h.call(move |s, ctx| {
                    Box::pin(async move {
                        s.invoke_json(&ctx, method, args)
                            .await
                            .map_err(|e| rt::Error::Message(e.to_string()))
                    })
                })
                .await
            }
            Self::Process(h) => {
                h.call(move |s, ctx| {
                    Box::pin(
                        async move { s.invoke_json(&ctx, method, args).await.map_err(Into::into) },
                    )
                })
                .await
            }
        }
    }
}
async fn owner<T: Send + Sync + 'static>(
    runtime: &Runtime,
    service: &Service<T>,
) -> (rt::PluginHandle, rt::ServiceHandle<T>) {
    let saved = Arc::new(Mutex::new(None));
    let target = saved.clone();
    let binding = service.clone();
    let owner = runtime
        .install(
            Plugin::new("owner", move |ctx| {
                let target = target.clone();
                let binding = binding.clone();
                async move {
                    *target.lock().unwrap() = Some(ctx.dependency(&binding)?);
                    Ok(())
                }
            })
            .requires(service),
        )
        .unwrap();
    bounded(owner.ready()).await.unwrap();
    let handle = saved.lock().unwrap().take().unwrap();
    (owner, handle)
}
async fn scenario(mode: Option<(Endpoint, Language, Language)>) -> Vec<Value> {
    let runtime = Runtime::new();
    let logs = Arc::new(Mutex::new(Vec::new()));
    let logger = Service::<Mutex<Vec<Value>>>::new("example.counter-log", "1");
    let binding = logger.clone();
    let records = logs.clone();
    runtime
        .install(
            Plugin::new("logger", move |ctx| {
                let binding = binding.clone();
                let records = records.clone();
                async move { ctx.provide(&binding, records) }
            })
            .provides(&logger),
        )
        .unwrap();
    let (first, a, b) = if let Some((endpoint, provider_language, client_language)) = mode {
        let counters = Service::<RemoteService>::new("example.counters", "1").with_descriptor(
            cordis_remote::Descriptor::new(
                serde_json::from_str(cordis_example_contracts::resources::counters::COUNTERS)
                    .unwrap(),
            )
            .unwrap(),
        );
        runtime
            .install(
                language_spec(endpoint, false, provider_language)
                    .requires(Dependency::new_json(
                        &logger,
                        cordis_remote::Descriptor::new(cordis_portable_example::resources::generated::CounterLogClient::interface())
                        .unwrap(),
                        |log, _, _, value| {
                            Box::pin(async move {
                                log.lock().unwrap().push(value);
                                Ok(json!({}))
                            })
                        },
                    ))
                    .plugin("counters", &counters),
            )
            .unwrap();
        let client = Service::<RemoteService>::new("example.counter-client", "1").with_descriptor(
            cordis_remote::Descriptor::new(
                cordis_portable_example::resources::generated::CounterHolderClient::interface(),
            )
            .unwrap(),
        );
        runtime
            .install(
                language_spec(endpoint, true, client_language)
                    .requires(Dependency::remote(&counters))
                    .plugin("client", &client),
            )
            .unwrap();
        let (first, a) = owner(&runtime, &client).await;
        let (_, b) = owner(&runtime, &client).await;
        (first, Binding::Process(a), Binding::Process(b))
    } else {
        let counters = Service::<native::Service>::new("example.counters", "1").with_descriptor(
            cordis_sdk_core::Descriptor::new(
                <CounterPlugin as cordis_sdk_core::Plugin>::interface(),
            )
            .unwrap(),
        );
        runtime
            .install(
                native::plugin::<CounterPlugin>(
                    "counters",
                    &counters,
                    json!({}),
                    native::Capabilities {
                        dependencies: vec![native::Dependency::new(
                            &logger,
                            cordis_sdk_core::Descriptor::new(
                                cordis_portable_example::resources::generated::CounterLogClient::interface(),
                            )
                            .unwrap(),
                            |log, _, _, value| {
                                Box::pin(async move {
                                    log.lock().unwrap().push(value.to_json().map_err(cordis_sdk_core::typed::violation)?);
                                    cordis_sdk_core::Message::new(cordis_portable_example::resources::generated::data::google::protobuf::Empty {}).map_err(cordis_sdk_core::typed::violation)
                                })
                            },
                        )],
                        ..Default::default()
                    },
                )
                .unwrap(),
            )
            .unwrap();
        let client = Service::<native::Service>::new("example.counter-client", "1");
        runtime
            .install(
                native::plugin::<CounterClient>(
                    "client",
                    &client,
                    json!({}),
                    native::Capabilities {
                        dependencies: vec![native::Dependency::portable(&counters).unwrap()],
                        ..Default::default()
                    },
                )
                .unwrap(),
            )
            .unwrap();
        let (first, a) = owner(&runtime, &client).await;
        let (_, b) = owner(&runtime, &client).await;
        (first, Binding::Native(a), Binding::Native(b))
    };
    if mode.is_some_and(|(_, _, language)| language != Language::Rust) {
        assert_eq!(
            bounded(a.invoke_json("scoped", json!(5))).await.unwrap(),
            json!("5")
        );
        assert_eq!(*logs.lock().unwrap(), vec![json!({"counter_closed":"5"})]);
        logs.lock().unwrap().clear();
    }
    // Scope must transport the canonical representation without narrowing int64.
    for value in ["9223372036854775807", "-9223372036854775808"] {
        assert_eq!(
            bounded(a.invoke_json("scoped", json!(value)))
                .await
                .unwrap(),
            json!(value)
        );
    }
    bounded(a.invoke_json("acquire", json!("9223372036854775807")))
        .await
        .unwrap();
    assert!(bounded(a.invoke_json("add", json!(1))).await.is_err());
    assert_eq!(
        bounded(a.invoke_json("get", json!({}))).await.unwrap(),
        json!("9223372036854775807")
    );
    bounded(a.invoke_json("release", json!({}))).await.unwrap();
    logs.lock().unwrap().clear();
    bounded(a.invoke_json("acquire", json!(40))).await.unwrap();
    assert_eq!(
        bounded(b.invoke_json("add", json!(2))).await.unwrap(),
        json!("42")
    );
    bounded(first.dispose()).await.unwrap();
    assert_eq!(*logs.lock().unwrap(), vec![json!({"counter_closed":"42"})]);
    assert!(
        bounded(b.invoke_json("get", cordis_sdk_core::json!({})))
            .await
            .is_err()
    );
    bounded(b.invoke_json("release", json!({}))).await.unwrap();
    bounded(b.invoke_json("acquire", json!(7))).await.unwrap();
    bounded(b.invoke_json("release", json!({}))).await.unwrap();
    bounded(b.invoke_json("release", json!({}))).await.unwrap();
    bounded(b.invoke_json("acquire", json!(9))).await.unwrap();
    bounded(runtime.shutdown()).await.unwrap();
    let result = logs.lock().unwrap().clone();
    assert_eq!(
        result,
        vec![
            json!({"counter_closed":"42"}),
            json!({"counter_closed":"7"}),
            json!({"counter_closed":"9"})
        ]
    );
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_resource_business_matches_native_for_all_process_endpoints() {
    let expected = scenario(None).await;
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        eprintln!("resources {endpoint:?}");
        assert_eq!(
            scenario(Some((endpoint, Language::Rust, Language::Rust))).await,
            expected
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exported_resources_are_released_on_restart_and_old_handles_expire() {
    for language in [Language::Rust, Language::TypeScript] {
        for endpoint in [Endpoint::Ipc, Endpoint::Http] {
            let runtime = Runtime::new();
            let logs = Arc::new(Mutex::new(Vec::new()));
            let logger = Service::<Mutex<Vec<Value>>>::new("example.counter-log", "1");
            let binding = logger.clone();
            let records = logs.clone();
            runtime
                .install(
                    Plugin::new("logger", move |ctx| {
                        let binding = binding.clone();
                        let records = records.clone();
                        async move { ctx.provide(&binding, records) }
                    })
                    .provides(&logger),
                )
                .unwrap();
            let service = Service::<RemoteService>::new("example.counters", "1").with_descriptor(
                cordis_remote::Descriptor::new(
                    serde_json::from_str(cordis_example_contracts::resources::counters::COUNTERS)
                        .unwrap(),
                )
                .unwrap(),
            );
            let provider = runtime
                    .install(
                        language_spec(endpoint, false, language)
                            .requires(Dependency::new_json(
                                &logger,
                                cordis_remote::Descriptor::new(
                                    cordis_portable_example::resources::generated::CounterLogClient::interface(),
                                )
                                .unwrap(),
                                |log, _, _, value| {
                                    Box::pin(async move {
                                        log.lock().unwrap().push(value);
                                        Ok(json!({}))
                                    })
                                },
                            ))
                            .plugin("counters", &service),
                    )
                    .unwrap();
            let (_, handle) = owner(&runtime, &service).await;
            let resource: RemoteResource = bounded(handle.call(|s, ctx| {
                Box::pin(async move {
                    s.acquire_json(&ctx, "counter", json!(40))
                        .await
                        .map_err(Into::into)
                })
            }))
            .await
            .unwrap();
            assert_eq!(
                resource.invoke_json("add", json!(2)).await.unwrap(),
                json!("42")
            );
            bounded(provider.restart()).await.unwrap();
            assert!(
                resource
                    .invoke_json("get", cordis_sdk_core::json!({}))
                    .await
                    .is_err()
            );
            assert_eq!(*logs.lock().unwrap(), vec![json!({"counter_closed":"42"})]);
            bounded(runtime.shutdown()).await.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(unix)]
async fn cpp_resources_compose_with_cpp_and_rust_on_all_endpoints() {
    let expected = scenario(None).await;
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        for (provider, client) in [
            (Language::Cpp, Language::Cpp),
            (Language::Cpp, Language::Rust),
            (Language::Rust, Language::Cpp),
        ] {
            eprintln!("resources {endpoint:?}: provider={provider:?} client={client:?}");
            assert_eq!(scenario(Some((endpoint, provider, client))).await, expected);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typescript_resources_compose_with_typescript_rust_and_cpp_on_all_endpoints() {
    let expected = scenario(None).await;
    let mut pairs = vec![
        (Language::TypeScript, Language::TypeScript),
        (Language::TypeScript, Language::Rust),
        (Language::Rust, Language::TypeScript),
    ];
    if cfg!(unix) {
        pairs.extend([
            (Language::TypeScript, Language::Cpp),
            (Language::Cpp, Language::TypeScript),
        ]);
    }
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        for &(provider, client) in &pairs {
            eprintln!("resources {endpoint:?}: provider={provider:?} client={client:?}");
            assert_eq!(scenario(Some((endpoint, provider, client))).await, expected);
        }
    }
}
