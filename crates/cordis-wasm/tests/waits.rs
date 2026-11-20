//! One business probe verifies observation withdrawal and real-work draining
//! across all Rust execution channels. Fixtures are built from the current SDK, reusing normal Cargo outputs.
#![allow(unsafe_code)]
use cordis_host::{Dependency, HostBindings};
use cordis_sdk as rt;
use cordis_sdk_core::{self as sdk, Plugin as _, json};
use cordis_test_contracts::waits::{Probe, interface};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug)]
enum Backend {
    Native,
    Abi,
    Process,
    Wasm,
}
fn descriptor() -> sdk::Descriptor {
    sdk::Descriptor::new(Probe::interface()).unwrap()
}
fn build_fixture(arguments: &[&str], wasm: bool) -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut command = std::process::Command::new(env!("CARGO"));
    command
        .current_dir(&root)
        .args(["build", "--locked", "--offline"])
        .args(arguments);
    // Guest builds use their own target directory and never inherit host flags.
    // The external development environment owns compiler/linker installation.
    if wasm {
        if let Some(rustc) = std::env::var_os("CORDIS_WASM_RUSTC") {
            command.env("RUSTC", rustc);
        }
        command.env_remove("CARGO_ENCODED_RUSTFLAGS");
    }
    let output = command.output().expect("run fixture build");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    root
}
fn artifacts() -> &'static PathBuf {
    static BUILD: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    BUILD.get_or_init(|| {
        build_fixture(
            &[
                "-p",
                "cordis-sdk-abi",
                "--example",
                "abi_waits",
                "-p",
                "cordis-sdk-process",
                "--example",
                "process_waits",
            ],
            false,
        )
        .join("target/debug/examples")
    })
}
fn component() -> &'static Vec<u8> {
    static BUILD: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    BUILD.get_or_init(|| {
        let root = build_fixture(
            &[
                "-p",
                "cordis-guest",
                "--example",
                "wasm_waits",
                "--target",
                "wasm32-unknown-unknown",
                "--release",
                "--target-dir",
                "target/wasm-waits",
            ],
            true,
        );
        let core = std::fs::read(
            root.join("target/wasm-waits/wasm32-unknown-unknown/release/examples/wasm_waits.wasm"),
        )
        .unwrap();
        wit_component::ComponentEncoder::default()
            .module(&core)
            .unwrap()
            .validate(true)
            .encode()
            .unwrap()
    })
}

fn provider(backend: Backend, bindings: HostBindings) -> (rt::Plugin, Dependency) {
    match backend {
        Backend::Native => {
            let service = rt::Service::<cordis_sdk_runtime::Service>::new("test.waits", "1")
                .with_descriptor(descriptor());
            let plugin =
                cordis_sdk_runtime::plugin::<Probe>("probe", &service, json!({}), bindings)
                    .unwrap();
            (plugin, Dependency::new(&service).unwrap())
        }
        Backend::Abi => {
            let service = rt::Service::<cordis_abi::AbiService>::new("test.waits", "1")
                .with_descriptor(descriptor());
            let file = artifacts().join(format!(
                "{}abi_waits{}",
                std::env::consts::DLL_PREFIX,
                std::env::consts::DLL_SUFFIX
            ));
            // SAFETY: the test builds this trusted fixture from current sources.
            let library = unsafe { cordis_abi::Library::load(file) }
                .unwrap()
                .with_context(cordis_abi::Capabilities {
                    bindings,
                    ..Default::default()
                })
                .unwrap();
            (
                library.plugin("probe", &service, json!({})).unwrap(),
                Dependency::new(&service).unwrap(),
            )
        }
        Backend::Process => {
            let service = rt::Service::<cordis_remote::RemoteService>::new("test.waits", "1")
                .with_descriptor(descriptor());
            let mut spec = cordis_remote::ProcessSpec::new(
                artifacts().join(format!("process_waits{}", std::env::consts::EXE_SUFFIX)),
            );
            spec.session.bindings = bindings;
            (
                spec.plugin("probe", &service),
                Dependency::new(&service).unwrap(),
            )
        }
        Backend::Wasm => {
            let service = rt::Service::<cordis_wasm::ContextService>::new("test.waits", "1")
                .with_descriptor(descriptor());
            let module = cordis_wasm::ContextComponent::from_bytes(
                component(),
                cordis_wasm::Limits::default(),
                cordis_wasm::Capabilities {
                    bindings,
                    max_requests: Some(4),
                    ..Default::default()
                },
            )
            .unwrap();
            (
                module.plugin("probe", &service, json!({})).unwrap(),
                Dependency::new(&service).unwrap(),
            )
        }
    }
}
async fn scenario(backend: Backend) {
    let runtime = rt::Runtime::new();
    let writes = Arc::new(AtomicUsize::new(0));
    let log = rt::Service::<AtomicUsize>::new("test.wait-log", "1");
    let source = runtime
        .install(
            rt::Plugin::new("log", {
                let log = log.clone();
                let writes = writes.clone();
                move |ctx| {
                    let log = log.clone();
                    let writes = writes.clone();
                    async move { ctx.provide(&log, writes) }
                }
            })
            .provides(&log),
        )
        .unwrap();
    source.ready().await.unwrap();
    let binding = Dependency::json(
        &log,
        sdk::Descriptor::new(interface(&["write"])).unwrap(),
        |writes, _, _, args| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                writes.fetch_add(1, Ordering::SeqCst);
                Ok::<_, sdk::Failure>(args)
            })
        },
    );
    let (plugin, dependency) = provider(
        backend,
        HostBindings {
            dependencies: vec![binding],
            factories: vec![cordis_host::FactoryBinding::new("delayed-child", |_| {
                Ok(rt::Plugin::new("delayed-child", |_| async {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    Ok(())
                }))
            })],
            ..Default::default()
        },
    );
    let implementation = runtime.install(plugin).unwrap();
    implementation.ready().await.unwrap();
    let completed_writes = writes.clone();
    let consumer = runtime
        .install(dependency.declare(rt::Plugin::new("consumer", {
            let dependency = dependency.clone();
            move |ctx| {
                let dependency = dependency.clone();
                let writes = writes.clone();
                async move {
                    for method in [
                        "race",
                        "cancel-race",
                        "invalid-wait",
                        "work",
                        "join-race",
                        "join-again",
                        "ready-race",
                        "ready-again",
                        "child-race",
                        "child-again",
                        "independent",
                        "start-task",
                    ] {
                        let before = Instant::now();
                        let input = sdk::Message::from_json(
                            sdk::MessageContract::json_value(),
                            &json!(null),
                        )
                        .unwrap();
                        let output = dependency
                            .invoke(ctx.clone(), method.into(), input.into())
                            .await
                            .map_err(|e| rt::Error::Message(e.to_string()))?;
                        assert_eq!(
                            output
                                .into_message(&sdk::MessageContract::json_value())
                                .unwrap()
                                .to_json()
                                .unwrap(),
                            json!(true)
                        );
                        let elapsed = before.elapsed();
                        eprintln!("{backend:?} {method}: {elapsed:?}");
                        assert!(
                            elapsed
                                < Duration::from_secs(if method.ends_with("-again") {
                                    4
                                } else {
                                    1
                                }),
                            "{method} waited for an abandoned observation: {elapsed:?}"
                        );
                        if method == "work" {
                            assert_eq!(
                                writes.load(Ordering::SeqCst),
                                1,
                                "admitted write was discarded"
                            );
                            assert!(
                                elapsed >= Duration::from_millis(90),
                                "callback escaped real-work draining"
                            );
                        }
                    }
                    Ok(())
                }
            }
        })))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), consumer.ready())
        .await
        .expect("wait probe stalled")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), consumer.dispose())
        .await
        .expect("task cancellation did not settle")
        .unwrap();
    assert_eq!(
        completed_writes.load(Ordering::SeqCst),
        2,
        "late inverse was lost or released twice"
    );
    implementation.dispose().await.unwrap();
    source.dispose().await.unwrap();
}
#[tokio::test]
async fn native_waits_and_work_have_distinct_lifetimes() {
    scenario(Backend::Native).await;
}
#[tokio::test]
async fn abi_waits_and_work_have_distinct_lifetimes() {
    scenario(Backend::Abi).await;
}
#[tokio::test]
async fn process_waits_and_work_have_distinct_lifetimes() {
    scenario(Backend::Process).await;
}
#[tokio::test]
async fn wasm_waits_and_work_have_distinct_lifetimes() {
    scenario(Backend::Wasm).await;
}

/// Exercise the C++ canonical awaiter as well as the Rust guest runtime. The
/// external WASI SDK and libraries are supplied by the development environment.
#[tokio::test]
async fn cpp_component_withdraws_join_and_preserves_task() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = root.join("build/wasm/observations/probe.wasm");
    let built = std::process::Command::new(
        std::env::var_os("PYTHON")
            .unwrap_or_else(|| if cfg!(windows) { "python" } else { "python3" }.into()),
    )
    .current_dir(&root)
    .args([
        "sdk/cpp/tools/build-component.py",
        "--sdk-source",
        "sdk/cpp",
        "--core",
        "sdk/cpp/tests/component/observations.cpp",
    ])
    .arg(&output)
    .output()
    .unwrap();
    assert!(
        built.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&built.stdout),
        String::from_utf8_lossy(&built.stderr)
    );
    let core = std::fs::read(output).unwrap();
    let bytes = wit_component::ComponentEncoder::default()
        .module(&core)
        .unwrap()
        .adapter(
            "wasi_snapshot_preview1",
            wasi_preview1_component_adapter_provider::WASI_SNAPSHOT_PREVIEW1_REACTOR_ADAPTER,
        )
        .unwrap()
        .validate(true)
        .encode()
        .unwrap();
    let module = cordis_wasm::ContextComponent::from_bytes(
        &bytes,
        Default::default(),
        cordis_wasm::Capabilities {
            wasi: Some(Default::default()),
            ..Default::default()
        },
    )
    .unwrap();
    let service = rt::Service::<cordis_wasm::ContextService>::new("test.cpp.observations", "1")
        .with_descriptor(module.descriptor().clone());
    let runtime = rt::Runtime::new();
    let provider = runtime
        .install(module.plugin("cpp", &service, json!({})).unwrap())
        .unwrap();
    provider.ready().await.unwrap();
    let dependency = Dependency::new(&service).unwrap();
    let consumer = runtime
        .install(
            dependency
                .clone()
                .declare(rt::Plugin::new("consumer", move |ctx| {
                    let dependency = dependency.clone();
                    async move {
                        for method in ["race", "again"] {
                            let before = Instant::now();
                            let input = sdk::Message::from_json(
                                sdk::MessageContract::json_value(),
                                &json!(null),
                            )
                            .unwrap();
                            dependency
                                .invoke(ctx.clone(), method.into(), input.into())
                                .await
                                .map_err(|e| rt::Error::Message(e.to_string()))?;
                            if method == "race" {
                                assert!(before.elapsed() < Duration::from_secs(1));
                            }
                        }
                        Ok(())
                    }
                })),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), consumer.ready())
        .await
        .unwrap()
        .unwrap();
    consumer.dispose().await.unwrap();
    provider.dispose().await.unwrap();
}
