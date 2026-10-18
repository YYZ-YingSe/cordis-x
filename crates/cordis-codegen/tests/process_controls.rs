//! Generated control paths execute through real process transports and Runtime ownership.
use cordis_portable_example::workflows::generated::{WorkflowClient, WorkflowControls};
use cordis_remote::{
    Endpoint, EventBinding, FactoryBinding, MiddlewareBinding, ProcessSpec, ProvisionBinding,
    RemoteService,
};
use cordis_sdk as rt;
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
fn build(language: &str) -> PathBuf {
    let root = root();
    for (program, args) in [
        (
            "cargo",
            vec![
                "build",
                "--offline",
                "-p",
                "cordis-sdk-process",
                "--example",
                "process_workflows",
            ],
        ),
        (
            "cmake",
            vec![
                "-S",
                "sdk/cpp",
                "-B",
                "target/cpp",
                "-DCMAKE_BUILD_TYPE=Debug",
                "-DCORDIS_BUILD_EXAMPLES=ON",
                "-DBUILD_TESTING=ON",
            ],
        ),
        (
            "cmake",
            vec![
                "--build",
                "target/cpp",
                "--target",
                "cordis_workflows",
                "-j",
                "4",
            ],
        ),
    ] {
        if (program == "cargo" && language != "rust") || (program == "cmake" && language != "cpp") {
            continue;
        }
        let output = Command::new(program)
            .args(args)
            .current_dir(&root)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    root
}
async fn scenario(root: &Path, language: &str, endpoint: Endpoint, validation: sdk::Validation) {
    let runtime = rt::Runtime::new();
    let settings = rt::Service::<cordis_remote::Bytes>::new("example.settings", "1");
    let created = Arc::new(AtomicUsize::new(0));
    let closed = Arc::new(AtomicUsize::new(0));
    let factory = FactoryBinding::new("worker", {
        let created = created.clone();
        let closed = closed.clone();
        move |config| {
            WorkflowControls::worker()
                .with_validation(validation)
                .build(sdk::Message::from_bytes(<cordis_portable_example::workflows::generated::data::workflows::WorkerConfig as sdk::typed::Codec>::contract().clone(), config).map_err(|e| sdk::refused("invalid_argument", e))?, |config| {
                    if config.label.as_deref() == Some("reject") {
                        return Err(sdk::error("invalid_worker", "rejected by factory"));
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
    });
    let mut spec = match language {
        "rust" => ProcessSpec::new(root.join(format!(
            "target/debug/examples/process_workflows{}",
            std::env::consts::EXE_SUFFIX
        ))),
        "cpp" => ProcessSpec::new(root.join("target/cpp/cordis_workflows")),
        "typescript" => {
            let mut spec = ProcessSpec::new("node");
            spec.args.push(
                root.join("sdk/typescript/examples/workflows.ts")
                    .to_string_lossy()
                    .into_owned(),
            );
            spec
        }
        _ => unreachable!(),
    }
    .event(EventBinding::new(
        "tick",
        &rt::Event::new("example.tick", "1"),
    ))
    .middleware(MiddlewareBinding::new(
        "request",
        &rt::Middleware::new("example.request", "1"),
    ))
    .factory(factory)
    .provision(ProvisionBinding::new("settings", &settings));
    spec.validation = validation;
    spec.endpoint = endpoint;
    if matches!(endpoint, Endpoint::Ipc) {
        spec.args.push("--stdio-only".into());
    }
    let service = rt::Service::<RemoteService>::new("example.workflow", "1")
        .with_descriptor(sdk::Descriptor::new(WorkflowClient::interface()).unwrap());
    let provider = runtime.install(spec.plugin("workflow", &service)).unwrap();
    provider.ready().await.unwrap();
    let saved = Arc::new(Mutex::new(None));
    let owner = runtime
        .install(
            rt::Plugin::new("consumer", {
                let saved = saved.clone();
                let service = service.clone();
                let settings = settings.clone();
                move |ctx| {
                    let saved = saved.clone();
                    let service = service.clone();
                    let settings = settings.clone();
                    async move {
                        let config = ctx
                            .dependency(&settings)?
                            .with(|value, _| Ok(value.clone()))?;
                        assert_eq!(
                            WorkflowControls::settings()
                                .with_validation(validation)
                                .decode(sdk::Message::from_bytes(<cordis_portable_example::workflows::generated::data::workflows::Settings as sdk::typed::Codec>::contract().clone(), config).unwrap())
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
            .requires(&settings),
        )
        .unwrap();
    owner.ready().await.unwrap();
    let handle: rt::ServiceHandle<RemoteService> = saved.lock().unwrap().take().unwrap();
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
        let result = handle
            .call(move |s, ctx| {
                Box::pin(async move { s.invoke_json(&ctx, method, args).await.map_err(Into::into) })
            })
            .await
            .unwrap();
        assert_eq!(result, expected, "{language} {endpoint:?} {method}");
    }
    for (method, args, code) in [
        ("run", json!({"value":"9223372036854775807"}), "overflow"),
        ("serial", json!({"value":"9223372036854775807"}), "overflow"),
        ("spawn_worker", json!({"label":"reject"}), "invalid_worker"),
    ] {
        let error = handle
            .call(move |s, ctx| {
                Box::pin(async move { Ok(s.invoke_json(&ctx, method, args).await.unwrap_err()) })
            })
            .await
            .unwrap();
        assert!(
            matches!(&error,cordis_remote::Error::Remote{code:c,..} if c==code),
            "{language} {method}: {error:?}"
        );
    }
    assert_eq!(created.load(Ordering::SeqCst), 1);
    owner.dispose().await.unwrap();
    assert_eq!(closed.load(Ordering::SeqCst), 1);
    provider.dispose().await.unwrap();
    runtime.shutdown().await.unwrap();
}
async fn transports(language: &str) {
    let root = build(language);
    for validation in [
        sdk::Validation::Contract,
        sdk::Validation::Checked,
        sdk::Validation::Audit,
    ] {
        for endpoint in [Endpoint::Ipc, Endpoint::Http] {
            eprintln!("controls: {language} {endpoint:?} {validation:?}");
            tokio::time::timeout(
                std::time::Duration::from_secs(60),
                scenario(&root, language, endpoint, validation),
            )
            .await
            .expect("control scenario deadline");
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_controls_rust() {
    transports("rust").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_controls_typescript() {
    transports("typescript").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(unix)] // The C++ process adapter uses POSIX I/O; Rust/TypeScript run on Windows too.
async fn generated_controls_cpp() {
    transports("cpp").await;
}
