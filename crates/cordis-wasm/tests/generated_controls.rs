//! One generated service exercises native ABI and two actual Wasm guests.
#[path = "../../cordis-codegen/tests/support/next_probe.rs"]
mod next_probe;
use cordis_abi::Bytes;
use cordis_portable_example::workflows::generated::{WorkflowClient, WorkflowControls};
use cordis_runtime as rt;
use cordis_sdk_core::{self as sdk, json};
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}
fn run(command: &mut Command) {
    let output = command.current_dir(root()).output().unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
fn rust_guest(example: &str) -> Vec<u8> {
    let mut command = Command::new("cargo");
    command.args([
        "build",
        "--offline",
        "--locked",
        "-p",
        "cordis-guest",
        "--example",
        example,
        "--target",
        "wasm32-unknown-unknown",
        "--release",
        "--target-dir",
        "target/workflow-components/rust",
    ]);
    command.env_remove("CARGO_ENCODED_RUSTFLAGS");
    if let Some(rustc) = std::env::var_os("CORDIS_WASM_RUSTC") {
        command.env("RUSTC", rustc);
    }
    run(&mut command);
    std::fs::read(root().join(format!(
        "target/workflow-components/rust/wasm32-unknown-unknown/release/examples/{example}.wasm"
    )))
    .unwrap()
}
fn component(cpp: bool) -> Vec<u8> {
    let core = if cpp {
        let output = root().join("target/workflow-components/cpp.wasm");
        run(Command::new(
            std::env::var_os("PYTHON")
                .unwrap_or_else(|| if cfg!(windows) { "python" } else { "python3" }.into()),
        )
        .args([
            "sdk/cpp/tools/build-component.py",
            "--sdk-source",
            "sdk/cpp",
            "--include",
            "examples/generated/cpp",
            "--proto-path",
            "examples/resources/contracts",
            "--core",
            "--proto",
            "workflows.proto",
            "sdk/cpp/examples/workflows/component.cpp",
        ])
        .arg(&output));
        std::fs::read(output).unwrap()
    } else {
        rust_guest("wasm_workflows")
    };
    let mut builder = wit_component::ComponentEncoder::default();
    let encoder = builder.module(&core).unwrap();
    let encoder = if cpp {
        encoder
            .adapter(
                "wasi_snapshot_preview1",
                wasi_preview1_component_adapter_provider::WASI_SNAPSHOT_PREVIEW1_REACTOR_ADAPTER,
            )
            .unwrap()
    } else {
        encoder
    };
    encoder.validate(true).encode().unwrap()
}
trait Call: Send + Sync + 'static {
    fn invoke(
        &self,
        ctx: rt::Context,
        method: &'static str,
        args: sdk::Value,
    ) -> rt::BoxFuture<'_, rt::Result<sdk::Value>>;
}
macro_rules! call {
    ($t:ty) => {
        impl Call for $t {
            fn invoke(
                &self,
                ctx: rt::Context,
                method: &'static str,
                args: sdk::Value,
            ) -> rt::BoxFuture<'_, rt::Result<sdk::Value>> {
                Box::pin(async move {
                    self.invoke_json(&ctx, method, args)
                        .await
                        .map_err(Into::into)
                })
            }
        }
    };
}
call!(cordis_abi::AbiService);
call!(cordis_wasm::ContextService);
struct Setup {
    validation: sdk::Validation,
    settings: rt::Service<Bytes>,
    created: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
}
impl Setup {
    fn new(validation: sdk::Validation) -> Self {
        Self {
            validation,
            settings: rt::Service::new("example.settings", "1"),
            created: Arc::new(AtomicUsize::new(0)),
            closed: Arc::new(AtomicUsize::new(0)),
        }
    }
    fn factory(&self) -> impl Fn(Bytes) -> rt::Result<rt::Plugin> + Send + Sync + 'static {
        let validation = self.validation;
        let created = self.created.clone();
        let closed = self.closed.clone();
        move |config| {
            WorkflowControls::worker()
                .with_validation(validation)
                .build(sdk::Message::from_bytes(<cordis_portable_example::workflows::generated::data::workflows::WorkerConfig as sdk::typed::Codec>::contract().clone(),config).unwrap(), |config| {
                    if config.label.as_deref() == Some("reject") {
                        return Err(sdk::error("invalid_worker", "factory refusal"));
                    }
                    created.fetch_add(1, Ordering::SeqCst);
                    let closed = closed.clone();
                    Ok(rt::Plugin::new(config.label.unwrap(), move |ctx| {
                        let closed = closed.clone();
                        async move {
                            ctx.defer("child", move |_| async move {
                                closed.fetch_add(1, Ordering::SeqCst);
                                Ok(())
                            })?;
                            Ok(())
                        }
                    }))
                })
                .map_err(Into::into)
        }
    }
}
async fn scenario<S: Call>(setup: Setup, service: rt::Service<S>, plugin: rt::Plugin) {
    let runtime = rt::Runtime::new();
    let provider = runtime.install(plugin).unwrap();
    provider.ready().await.unwrap();
    let saved = Arc::new(Mutex::new(None));
    let owner = runtime
        .install(
            rt::Plugin::new("consumer", {
                let saved = saved.clone();
                let service = service.clone();
                let settings = setup.settings.clone();
                move |ctx| {
                    let saved = saved.clone();
                    let service = service.clone();
                    let settings = settings.clone();
                    async move {
                        assert_eq!(
                            WorkflowControls::settings()
                                .decode(sdk::Message::from_bytes(<cordis_portable_example::workflows::generated::data::workflows::Settings as sdk::typed::Codec>::contract().clone(),ctx.dependency(&settings)?.with(|v, _| Ok(v.clone()))?).unwrap())
                                .unwrap()
                                .answer,
                            Some(42)
                        );
                        *saved.lock().unwrap() = Some(ctx.dependency(&service)?);
                        Ok(())
                    }
                }
            })
            .requires(&service)
            .requires(&setup.settings),
        )
        .unwrap();
    owner.ready().await.unwrap();
    let handle: rt::ServiceHandle<S> = saved.lock().unwrap().take().unwrap();
    for (method, args, expected) in [
        ("run", json!({"value":"5"}), json!({"value":"15"})),
        ("run", json!({"value":"0"}), json!({"value":"100"})),
        (
            "serial",
            json!({"value":"5"}),
            json!({"stopped":{"value":"6"}}),
        ),
        ("serial", json!({"value":"-1"}), json!({"continued":{}})),
        (
            "parallel",
            json!({"value":"5"}),
            json!({"values":[{"stopped":{"value":"6"}}]}),
        ),
        ("spawn_worker", json!({"label":"owned-child"}), json!({})),
    ] {
        assert_eq!(
            handle
                .call(move |s, ctx| s.invoke(ctx, method, args))
                .await
                .unwrap(),
            expected,
            "{method}"
        );
    }
    for (method, args, code) in [
        ("run", json!({"value":"9223372036854775807"}), "overflow"),
        ("serial", json!({"value":"9223372036854775807"}), "overflow"),
        ("spawn_worker", json!({"label":"reject"}), "invalid_worker"),
    ] {
        let error = handle
            .call(move |s, ctx| s.invoke(ctx, method, args))
            .await
            .unwrap_err();
        assert!(
            matches!(&error,rt::Error::Portable(sdk::Failure::Rejected(info)) if info.code==code),
            "{method}: {error:?}"
        );
    }
    for expired in [false, true] {
        let probe = next_probe::install::<Bytes>(&runtime, expired).await;
        if expired {
            handle
                .call(|s, ctx| s.invoke(ctx, "run", json!({"value":"5"})))
                .await
                .unwrap();
        }
        let error = handle
            .call(|s, ctx| s.invoke(ctx, "run", json!({"value":"5"})))
            .await
            .unwrap_err();
        let code = if expired {
            "expired_next"
        } else {
            "duplicate_next"
        };
        assert!(
            matches!(&error, rt::Error::Portable(sdk::Failure::Refused(info)) if info.code == code),
            "{error:?}"
        );
        probe.dispose().await.unwrap();
    }
    assert_eq!(setup.created.load(Ordering::SeqCst), 1);
    owner.dispose().await.unwrap();
    assert_eq!(setup.closed.load(Ordering::SeqCst), 1);
    provider.dispose().await.unwrap();
    runtime.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_controls_run_through_rust_and_cpp_abi() {
    run(Command::new("cmake").args([
        "-S",
        "sdk/cpp",
        "-DCORDIS_BUILD_EXAMPLES=ON",
        "-DBUILD_TESTING=ON",
        "-B",
        "target/cpp",
        "-DCMAKE_BUILD_TYPE=Debug",
        "-DCORDIS_BUILD_EXAMPLES=ON",
        "-DBUILD_TESTING=ON",
    ]));
    run(Command::new("cmake").args([
        "--build",
        "target/cpp",
        "--target",
        "cordis_workflows_abi",
        "-j",
        "4",
    ]));
    run(Command::new("cargo").args([
        "build",
        "--offline",
        "-p",
        "cordis-sdk-abi",
        "--example",
        "abi_workflows",
    ]));
    for (directory, name) in [
        ("target/cpp", "cordis_workflows_abi"),
        ("target/debug/examples", "abi_workflows"),
    ] {
        for validation in [
            sdk::Validation::Contract,
            sdk::Validation::Checked,
            sdk::Validation::Audit,
        ] {
            let setup = Setup::new(validation);
            // SAFETY: this fixture was built from this repository's trusted ABI SDK above.
            #[allow(unsafe_code)]
            let module = unsafe {
                cordis_abi::Library::load(root().join(format!(
                    "{directory}/{}{name}{}",
                    std::env::consts::DLL_PREFIX,
                    std::env::consts::DLL_SUFFIX
                )))
            }
            .unwrap()
            .with_context(cordis_abi::Capabilities {
                validation,
                events: vec![cordis_abi::EventBinding::new(
                    "tick",
                    &rt::Event::new("example.tick", "1"),
                )],
                middleware: vec![cordis_abi::MiddlewareBinding::new(
                    "request",
                    &rt::Middleware::new("example.request", "1"),
                )],
                factories: vec![cordis_abi::FactoryBinding::new("worker", setup.factory())],
                provisions: vec![cordis_abi::ProvisionBinding::new(
                    "settings",
                    &setup.settings,
                )],
                ..Default::default()
            })
            .unwrap();
            let service = rt::Service::new("example.workflow", "1")
                .with_descriptor(sdk::Descriptor::new(WorkflowClient::interface()).unwrap());
            let plugin = module.plugin("abi", &service, json!({})).unwrap();
            tokio::time::timeout(
                std::time::Duration::from_secs(30),
                scenario(setup, service, plugin),
            )
            .await
            .unwrap();
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_controls_run_through_rust_and_cpp_wasm() {
    for cpp in [false, true] {
        let bytes = component(cpp);
        for validation in [
            sdk::Validation::Contract,
            sdk::Validation::Checked,
            sdk::Validation::Audit,
        ] {
            let setup = Setup::new(validation);
            let factory = setup.factory();
            let module = cordis_wasm::ContextComponent::from_bytes(
                &bytes,
                cordis_wasm::Limits {
                    fuel_per_call: 100_000_000,
                    ..Default::default()
                },
                cordis_wasm::Capabilities {
                    validation,
                    events: [("tick".into(), rt::Event::new("example.tick", "1"))].into(),
                    middleware: [(
                        "request".into(),
                        rt::Middleware::new("example.request", "1"),
                    )]
                    .into(),
                    factories: [(
                        "worker".into(),
                        Arc::new(move |v| factory(v).map_err(Into::into))
                            as Arc<dyn Fn(Bytes) -> cordis_wasm::Result<rt::Plugin> + Send + Sync>,
                    )]
                    .into(),
                    provisions: [("settings".into(), setup.settings.clone())].into(),
                    wasi: cpp.then(cordis_wasm::Wasi::default),
                    ..Default::default()
                },
            )
            .unwrap();
            let service = rt::Service::new("example.workflow", "1")
                .with_descriptor(sdk::Descriptor::new(WorkflowClient::interface()).unwrap());
            let plugin = module.plugin("wasm", &service, json!({})).unwrap();
            eprintln!("generated workflow: cpp={cpp}, validation={validation:?}");
            tokio::time::timeout(
                std::time::Duration::from_secs(30),
                scenario(setup, service, plugin),
            )
            .await
            .unwrap();
        }
    }
}

// A burst uses the default finite Store budget. Rebuilding the entire interface
// for each admitted call exhausted that budget before ordinary DTO calls ended.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_binary_calls_complete_concurrently_with_default_store_limits() {
    use cordis_portable_example::generated::{
        AdderClient,
        data::{cordis::data::Int64, example::AddArgs},
    };
    use sdk::typed::Codec;
    let bytes = wit_component::ComponentEncoder::default()
        .module(&rust_guest("wasm_adder"))
        .unwrap()
        .validate(true)
        .encode()
        .unwrap();
    let module =
        cordis_wasm::ContextComponent::from_bytes(&bytes, Default::default(), Default::default())
            .unwrap();
    let service = rt::Service::new("example.adder", "1")
        .with_descriptor(sdk::Descriptor::new(AdderClient::interface()).unwrap());
    let runtime = rt::Runtime::new();
    let provider = runtime
        .install(module.plugin("adder", &service, json!({})).unwrap())
        .unwrap();
    let saved = Arc::new(Mutex::new(None));
    let consumer = runtime
        .install(
            rt::Plugin::new("consumer", {
                let saved = saved.clone();
                let service = service.clone();
                move |ctx| {
                    let saved = saved.clone();
                    let service = service.clone();
                    async move {
                        *saved.lock().unwrap() = Some(ctx.dependency(&service)?);
                        Ok(())
                    }
                }
            })
            .requires(&service),
        )
        .unwrap();
    provider.ready().await.unwrap();
    consumer.ready().await.unwrap();
    let handle = saved.lock().unwrap().take().unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let handle = handle.clone();
        let barrier = barrier.clone();
        tasks.spawn(async move {
            for i in 0..32 {
                let left = 9_007_199_254_741_000 + i;
                let input = sdk::Message::new(AddArgs {
                    left: Some(left),
                    right: Some(42),
                })
                .unwrap()
                .encode(cordis_component::MAX_BYTES)
                .unwrap();
                barrier.wait().await;
                let output = handle
                    .call(move |s, ctx| {
                        Box::pin(
                            async move { s.invoke(&ctx, "add", input).await.map_err(Into::into) },
                        )
                    })
                    .await
                    .unwrap();
                let output = sdk::Message::from_bytes(Int64::contract().clone(), output)
                    .unwrap()
                    .into_typed::<Int64>()
                    .unwrap();
                assert_eq!(output.value, Some(left + 42));
            }
        });
    }
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        runtime.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
