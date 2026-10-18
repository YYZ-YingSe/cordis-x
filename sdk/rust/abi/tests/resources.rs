use cordis_abi::{AbiResource, AbiService, Capabilities, Dependency, Library};
use cordis_portable_example::resources::{CounterClient, CounterPlugin};
use cordis_runtime::{self as rt, Plugin, Runtime, Service};
use cordis_sdk_core::{Value, json};
use cordis_sdk_runtime as native;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

#[path = "../../tests/support/cpp_resources.rs"]
mod cpp;

#[path = "../../package_policy.rs"]
mod package_policy;

#[path = "support/resource_faults.rs"]
mod faults;

async fn bounded<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(20), f)
        .await
        .expect("resource scenario timed out")
}
fn artifact(name: &str) -> PathBuf {
    static BUILD: OnceLock<PathBuf> = OnceLock::new();
    BUILD
        .get_or_init(|| {
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
            let target = cordis_test_contracts::native::cargo_target();
            let output = std::process::Command::new(env!("CARGO"))
                .current_dir(&root)
                .args([
                    "build",
                    "-p",
                    "cordis-sdk-abi",
                    "--example",
                    "resource_counter",
                    "--example",
                    "resource_client",
                    "--example",
                    "abi_resource_probe",
                    "--example",
                    "abi_resource_probe_client",
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
            target.join("debug/examples")
        })
        .join(format!(
            "{}{name}{}",
            std::env::consts::DLL_PREFIX,
            std::env::consts::DLL_SUFFIX
        ))
}
#[allow(unsafe_code)]
fn library(name: &str, capabilities: Capabilities) -> Library {
    let path = if name.starts_with("cordis_resource_") {
        cpp::build().join(format!(
            "{}{name}{}",
            std::env::consts::DLL_PREFIX,
            std::env::consts::DLL_SUFFIX
        ))
    } else {
        artifact(name)
    };
    // SAFETY: these are repository-owned Rust/C++ example libraries built here.
    let library = unsafe { Library::load(path) }.unwrap();
    assert!(library.supports_resources());
    assert!(library.supports_dependency_resources());
    library.with_context(capabilities).unwrap()
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
enum Binding {
    Native(rt::ServiceHandle<native::Service>),
    Abi(rt::ServiceHandle<AbiService>),
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
            Self::Abi(h) => {
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
async fn scenario(abi: Option<(bool, bool)>) -> Vec<Value> {
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
    let (first, a, b) = if let Some((cpp_provider, cpp_client)) = abi {
        let counters = Service::<AbiService>::new("example.counters", "1").with_descriptor(cordis_sdk_core::Descriptor::new(<cordis_portable_example::resources::CounterPlugin as cordis_sdk_core::Plugin>::interface()).unwrap());
        let lib = library(
            if cpp_provider {
                "cordis_resource_counter"
            } else {
                "resource_counter"
            },
            Capabilities {
                dependencies: vec![Dependency::new_json(
                    &logger,
                    cordis_sdk_core::Descriptor::new(
                        cordis_portable_example::resources::generated::CounterLogClient::interface(
                        ),
                    )
                    .unwrap(),
                    |log, _, _, value| {
                        Box::pin(async move {
                            log.lock().unwrap().push(value);
                            Ok(json!({}))
                        })
                    },
                )],
                ..Default::default()
            },
        );
        let provider = runtime
            .install(lib.plugin("counters", &counters, json!({})).unwrap())
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let client = Service::<AbiService>::new("example.counter-client", "1").with_descriptor(
            cordis_sdk_core::Descriptor::new(
                cordis_portable_example::resources::generated::CounterHolderClient::interface(),
            )
            .unwrap(),
        );
        let lib = library(
            if cpp_client {
                "cordis_resource_client"
            } else {
                "resource_client"
            },
            Capabilities {
                dependencies: vec![Dependency::abi(&counters)],
                ..Default::default()
            },
        );
        let plugin = runtime
            .install(lib.plugin("client", &client, json!({})).unwrap())
            .unwrap();
        bounded(plugin.ready()).await.unwrap();
        let (first, a) = owner(&runtime, &client).await;
        let (_, b) = owner(&runtime, &client).await;
        (first, Binding::Abi(a), Binding::Abi(b))
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
    if abi.is_some_and(|(_, cpp)| cpp) {
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
async fn same_counter_business_matches_native_through_two_dynamic_libraries() {
    assert_eq!(scenario(None).await, scenario(Some((false, false))).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reverse_resource_registry_does_not_keep_library_code_after_successful_recovery() {
    for cpp in [false, true] {
        code_recovery(cpp).await;
    }
}
async fn code_recovery(cpp: bool) {
    let directory =
        std::env::temp_dir().join(format!("cordis-rust-resource-code-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let options = cordis_abi::ArtifactOptions {
        package: package_policy::example_policy(directory.clone()),
    };
    let staged = || -> Vec<PathBuf> {
        std::fs::read_dir(&directory)
            .unwrap()
            .flat_map(|d| match std::fs::read_dir(d.unwrap().path()) {
                Ok(files) => files.map(|f| f.unwrap().path()).collect::<Vec<_>>(),
                // Recovery can remove a staging directory between the outer
                // listing and this read. Its disappearance confirms cleanup;
                // other I/O failures must still fail the observation.
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                Err(error) => panic!("cannot inspect staged library: {error}"),
            })
            .collect()
    };
    let load = |name: &str, capabilities| {
        let path = if cpp {
            cpp::build().join(format!(
                "{}cordis_{name}{}",
                std::env::consts::DLL_PREFIX,
                std::env::consts::DLL_SUFFIX
            ))
        } else {
            artifact(name)
        };
        let root = path.parent().unwrap();
        let name = path.file_name().unwrap().to_str().unwrap().to_owned();
        let artifact = cordis_loader::package::PackageManifest::from_files(
            root,
            name.clone(),
            vec![(name, cordis_loader::package::FileKind::Library)],
            &options.package,
        )
        .unwrap()
        .write(root)
        .unwrap();
        // SAFETY: verified copies of these repository-owned Rust/C++ example libraries.
        #[allow(unsafe_code)]
        unsafe { Library::load_artifact(&artifact, &options) }
            .unwrap()
            .with_context(capabilities)
            .unwrap()
    };
    let runtime = Runtime::new();
    let logger = Service::<()>::new("example.counter-log", "1");
    let binding = logger.clone();
    runtime
        .install(
            Plugin::new("logger", move |ctx| {
                let binding = binding.clone();
                async move { ctx.provide(&binding, Arc::new(())) }
            })
            .provides(&logger),
        )
        .unwrap();
    let counters = Service::<AbiService>::new("example.counters", "1").with_descriptor(cordis_sdk_core::Descriptor::new(<cordis_portable_example::resources::CounterPlugin as cordis_sdk_core::Plugin>::interface()).unwrap());
    let library = load(
        "resource_counter",
        Capabilities {
            dependencies: vec![Dependency::new_json(
                &logger,
                cordis_sdk_core::Descriptor::new(
                    cordis_portable_example::resources::generated::CounterLogClient::interface(),
                )
                .unwrap(),
                |_, _, _, _| Box::pin(async { Ok(json!({})) }),
            )],
            ..Default::default()
        },
    );
    runtime
        .install(library.plugin("counters", &counters, json!({})).unwrap())
        .unwrap()
        .ready()
        .await
        .unwrap();
    drop(library);
    let service = Service::<AbiService>::new("example.counter-client", "1").with_descriptor(
        cordis_sdk_core::Descriptor::new(
            cordis_portable_example::resources::generated::CounterHolderClient::interface(),
        )
        .unwrap(),
    );
    let library = load(
        "resource_client",
        Capabilities {
            dependencies: vec![Dependency::abi(&counters)],
            ..Default::default()
        },
    );
    runtime
        .install(library.plugin("client", &service, json!({})).unwrap())
        .unwrap()
        .ready()
        .await
        .unwrap();
    drop(library);
    let (_, handle) = owner(&runtime, &service).await;
    let handle = Binding::Abi(handle);
    bounded(handle.invoke_json("acquire", json!(40)))
        .await
        .unwrap();
    assert_eq!(
        staged().len(),
        2,
        "both libraries stay pinned while the resource is alive"
    );
    assert_eq!(
        bounded(handle.invoke_json("add", json!(2))).await.unwrap(),
        json!("42")
    );
    bounded(runtime.shutdown()).await.unwrap();
    drop((handle, runtime));
    bounded(async {
        while !staged().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_resources_compose_with_cpp_and_rust_through_dynamic_libraries() {
    let expected = scenario(None).await;
    for pair in [(true, true), (true, false), (false, true)] {
        assert_eq!(scenario(Some(pair)).await, expected);
    }
}
