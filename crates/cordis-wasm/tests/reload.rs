#![cfg(feature = "loader")]
#[path = "support/context.rs"]
mod context_guest;
mod support;
use cordis_loader::{ArtifactRef, Configuration, Entry, Loader, LoaderSnapshot, Phase, Registry};
use cordis_sdk::*;
use cordis_wasm::{
    Limits, Value, WasmService, json,
    loader::{register_component, register_context_component},
};
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

fn v2_bytes() -> &'static [u8] {
    static BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    BYTES.get_or_init(|| {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let target = root.join("target/wasm-reload-v2");
        let mut command = Command::new(env!("CARGO"));
        command
            .current_dir(&root)
            .args([
                "build",
                "-p",
                "cordis-wasm-calculator",
                "--target",
                "wasm32-unknown-unknown",
                "--release",
                "--features",
                "test-fixture,version-v2",
                "--offline",
                "--locked",
            ])
            .arg("--target-dir")
            .arg(&target);
        if let Some(rustc) = std::env::var_os("CORDIS_WASM_RUSTC") {
            command.env("RUSTC", rustc);
        }
        command.env_remove("CARGO_ENCODED_RUSTFLAGS");
        let result = command.output().unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        support::package(
            &std::fs::read(
                target.join("wasm32-unknown-unknown/release/cordis_wasm_calculator.wasm"),
            )
            .unwrap(),
        )
    })
}
struct Files(PathBuf);
impl Files {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "cordis-reload-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn write(&self, name: &str, bytes: &[u8]) -> ArtifactRef {
        let path = self.0.join(name);
        std::fs::write(&path, bytes).unwrap();
        ArtifactRef::from_bytes(path.to_str().unwrap(), bytes)
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Gate {
    armed: AtomicBool,
    entered: Semaphore,
    release: Semaphore,
}
impl Default for Gate {
    fn default() -> Self {
        Self {
            armed: AtomicBool::new(false),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        }
    }
}
// Loader apply includes cold Cranelift compilation; keep the shorter bound on
// ready/calls/drain/cleanup, which must settle independently of compilation.
async fn prepare<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(60), future)
        .await
        .expect("artifact preparation stalled")
}
struct App {
    rt: Runtime,
    loader: Loader,
    saved: Arc<Mutex<Option<ServiceHandle<WasmService>>>>,
    trace: Arc<Mutex<Vec<String>>>,
    gate: Arc<Gate>,
    files: Files,
    v1: ArtifactRef,
    v2: ArtifactRef,
}
fn config(artifact: ArtifactRef, settings: Value) -> Configuration {
    Configuration {
        entries: vec![
            Entry::new("component", "wasm", settings).artifact(artifact),
            Entry::new("client", "client", Value::Null),
            Entry::new("idle", "idle", Value::Null),
        ],
    }
}
fn entry<'a>(snapshot: &'a LoaderSnapshot, id: &str) -> &'a cordis_loader::EntrySnapshot {
    snapshot.entries.iter().find(|e| e.id == id).unwrap()
}
async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("version handoff stalled")
}
async fn version(handle: &ServiceHandle<WasmService>) -> Result<Value> {
    handle
        .call(|s, ctx| {
            Box::pin(async move {
                s.invoke_json(&ctx, "code-version", json!({}))
                    .await
                    .map_err(Into::into)
            })
        })
        .await
}
impl App {
    async fn new(settings: Value) -> Self {
        let files = Files::new();
        let v1 = files.write("v1.wasm", support::bytes());
        let v2 = files.write("v2.wasm", v2_bytes());
        assert_ne!(v1.sha256, v2.sha256);
        let rt = Runtime::new();
        let service = Service::<WasmService>::new("example.calculator", "1").with_descriptor(
            cordis_contract::portable::decode(
                &cordis_test_contracts::resources::rust_fixture::SERVICE
                    .parse()
                    .unwrap(),
            )
            .unwrap(),
        );
        let mut registry = Registry::new();
        register_component(
            &mut registry,
            "wasm",
            service.clone(),
            cordis_wasm::Compiler::default(),
            Limits::default(),
        )
        .unwrap();
        register_component(
            &mut registry,
            "wrong",
            Service::new("example.calculator", "2").with_descriptor(
                cordis_contract::portable::decode(
                    &cordis_test_contracts::resources::rust_fixture::SERVICE
                        .parse()
                        .unwrap(),
                )
                .unwrap(),
            ),
            cordis_wasm::Compiler::default(),
            Limits::default(),
        )
        .unwrap();
        register_component(
            &mut registry,
            "small",
            service.clone(),
            cordis_wasm::Compiler::default(),
            Limits {
                artifact_bytes: 8,
                ..Limits::default()
            },
        )
        .unwrap();
        register_context_component(
            &mut registry,
            "no-wasi",
            Service::new("example.context", "1").with_descriptor(
                cordis_contract::Descriptor::new(
                    cordis_portable_example::context_fixture_interface(),
                )
                .unwrap(),
            ),
            cordis_wasm::Compiler::default(),
            Limits::default(),
            Default::default(),
        )
        .unwrap();
        register_context_component(
            &mut registry,
            "no-dependency",
            Service::new("example.context", "1").with_descriptor(
                cordis_contract::Descriptor::new(
                    cordis_portable_example::context_fixture_interface(),
                )
                .unwrap(),
            ),
            cordis_wasm::Compiler::default(),
            Limits::default(),
            cordis_wasm::Capabilities {
                wasi: Some(Default::default()),
                ..Default::default()
            },
        )
        .unwrap();
        let saved = Arc::new(Mutex::new(None));
        let trace = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new(Gate::default());
        registry
            .register("client", {
                let saved = saved.clone();
                let trace = trace.clone();
                let gate = gate.clone();
                move |_| {
                    let saved = saved.clone();
                    let trace = trace.clone();
                    let gate = gate.clone();
                    let service = service.clone();
                    Ok(Plugin::new("client", {
                        let service = service.clone();
                        move |ctx| {
                            let service = service.clone();
                            let saved = saved.clone();
                            let trace = trace.clone();
                            let gate = gate.clone();
                            async move {
                                let handle = ctx.dependency(&service)?;
                                let current = version(&handle).await?;
                                *saved.lock().unwrap() = Some(handle);
                                trace.lock().unwrap().push(format!("ready {current}"));
                                ctx.defer("client cleanup", move |cleanup| async move {
                                    let handle = cleanup.dependency(&service)?;
                                    let old = version(&handle).await?;
                                    trace.lock().unwrap().push(format!("cleanup {old}"));
                                    if gate.armed.swap(false, Ordering::SeqCst) {
                                        gate.entered.add_permits(1);
                                        gate.release.acquire().await.unwrap().forget();
                                        assert_eq!(version(&handle).await?, old);
                                    }
                                    Ok(())
                                })?;
                                Ok(())
                            }
                        }
                    })
                    .requires(&service))
                }
            })
            .unwrap();
        registry
            .register("idle", |_| Ok(Plugin::new("idle", |_| async { Ok(()) })))
            .unwrap();
        let loader = Loader::new(rt.clone(), registry);
        loader.apply(config(v1.clone(), settings)).await.unwrap();
        bounded(loader.settled()).await.unwrap();
        Self {
            rt,
            loader,
            saved,
            trace,
            gate,
            files,
            v1,
            v2,
        }
    }
    fn handle(&self) -> ServiceHandle<WasmService> {
        self.saved.lock().unwrap().as_ref().unwrap().clone()
    }
}

#[tokio::test]
async fn real_releases_handoff_after_consumer_recovery_and_expire_old_resources() {
    let app = App::new(json!({})).await;
    let first = app.loader.snapshot();
    let old = app.handle();
    let resource = old
        .call(|s, ctx| {
            Box::pin(async move {
                s.acquire_json(&ctx, "counter", json!({"initial":42}))
                    .await
                    .map_err(Into::into)
            })
        })
        .await
        .unwrap();
    let next = config(app.v2.clone(), json!({}));
    let plan = app.loader.plan(next.clone()).await.unwrap();
    assert_eq!(plan.retire, ["component"]);
    assert_eq!(plan.artifacts["component"], app.v2);
    assert_eq!(version(&old).await.unwrap(), "v1");
    app.loader.apply(next).await.unwrap();
    let updated = bounded(app.loader.settled()).await.unwrap();
    assert_eq!(version(&app.handle()).await.unwrap(), "v2");
    assert_eq!(
        *app.trace.lock().unwrap(),
        ["ready \"v1\"", "cleanup \"v1\"", "ready \"v2\""]
    );
    assert_eq!(
        entry(&first, "idle").instance.as_ref().unwrap().activation,
        entry(&updated, "idle")
            .instance
            .as_ref()
            .unwrap()
            .activation
    );
    assert_eq!(
        entry(&updated, "component")
            .actual
            .as_ref()
            .unwrap()
            .artifact,
        Some(app.v2.clone())
    );
    assert_eq!(entry(&updated, "component").actual_revision, Some(2));
    assert!(version(&old).await.is_err());
    assert!(resource.invoke_json("get", json!({})).await.is_err());
    assert_eq!(resource.snapshot().status, EffectStatus::Released);
    let unchanged = app
        .loader
        .apply(config(app.v2.clone(), json!({})))
        .await
        .unwrap();
    assert!(unchanged.plan.install.is_empty());
    app.rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_candidates_preserve_running_version_and_both_revisions() {
    let app = App::new(json!({})).await;
    let before = app.loader.snapshot();
    let corrupt = app.files.write("corrupt.wasm", b"not a component");
    let imports = app.files.write(
        "imports.wasm",
        &wat::parse_str(r#"(component (import "ambient" (func)))"#).unwrap(),
    );
    let context = app.files.write("context.wasm", context_guest::bytes());
    let mismatch = ArtifactRef {
        sha256: "0".repeat(64),
        ..app.v2.clone()
    };
    for (source, artifact, message) in [
        ("wasm", corrupt, "Load"),
        ("wasm", imports, "Unknown"),
        ("wasm", mismatch, "SHA-256 mismatch"),
        ("small", app.v2.clone(), "exceeds"),
        ("wrong", app.v2.clone(), "Contract"),
        ("no-wasi", context.clone(), "Unknown"),
        ("no-dependency", context, "declarations"),
    ] {
        let mut next = config(artifact, json!({}));
        next.entries[0].source = source.into();
        let error = app.loader.apply(next).await.unwrap_err();
        assert!(error.to_string().contains(message), "{source}: {error}");
        let after = app.loader.snapshot();
        assert_eq!(after.desired_revision, before.desired_revision);
        assert_eq!(after.applied_revision, before.applied_revision);
        assert_eq!(after.configuration, before.configuration);
        assert_eq!(
            entry(&after, "component")
                .instance
                .as_ref()
                .unwrap()
                .activation,
            entry(&before, "component")
                .instance
                .as_ref()
                .unwrap()
                .activation
        );
        assert_eq!(version(&app.handle()).await.unwrap(), "v1");
        assert_eq!(*app.trace.lock().unwrap(), ["ready \"v1\""]);
    }
    app.rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn preview_does_not_authorize_changed_file_but_prepared_handoff_pins_exact_code() {
    let app = App::new(json!({})).await;
    let next = config(app.v2.clone(), json!({}));
    app.loader.plan(next.clone()).await.unwrap();
    std::fs::write(&app.v2.location, b"overwritten before apply").unwrap();
    assert!(
        app.loader
            .apply(next.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("SHA-256 mismatch")
    );
    assert_eq!(version(&app.handle()).await.unwrap(), "v1");
    std::fs::write(&app.v2.location, v2_bytes()).unwrap();
    app.gate.armed.store(true, Ordering::SeqCst);
    let worker = app.loader.clone();
    let updating = tokio::spawn(async move { worker.apply(next).await });
    bounded(app.gate.entered.acquire()).await.unwrap().forget();
    let switching = app.loader.snapshot();
    assert_eq!(switching.phase, Phase::Applying);
    assert_eq!(switching.applied_revision, 1);
    assert_eq!(switching.desired_revision, 2);
    assert_eq!(
        entry(&switching, "component")
            .actual
            .as_ref()
            .unwrap()
            .artifact,
        Some(app.v1.clone())
    );
    assert_eq!(
        entry(&switching, "component")
            .desired
            .as_ref()
            .unwrap()
            .artifact,
        Some(app.v2.clone())
    );
    // Preparation already compiled v2. Cancellation and replacing the pathname
    // cannot make this accepted update silently activate some other code.
    updating.abort();
    let _ = updating.await;
    std::fs::write(&app.v2.location, support::bytes()).unwrap();
    app.gate.release.add_permits(1);
    bounded(app.loader.settled()).await.unwrap();
    assert_eq!(version(&app.handle()).await.unwrap(), "v2");
    assert!(
        app.loader
            .apply(config(app.v2.clone(), json!({})))
            .await
            .is_err()
    );
    assert_eq!(version(&app.handle()).await.unwrap(), "v2");
    app.rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_old_inverse_keeps_actual_code_and_blocks_new_installation() {
    let app = App::new(json!({"fail":"release"})).await;
    let old = app.handle();
    let resource = old
        .call(|s, ctx| {
            Box::pin(async move {
                s.acquire_json(&ctx, "counter", json!({}))
                    .await
                    .map_err(Into::into)
            })
        })
        .await
        .unwrap();
    let next = config(app.v2.clone(), json!({}));
    assert!(prepare(app.loader.apply(next.clone())).await.is_err());
    let failed = app.loader.snapshot();
    assert_eq!(failed.phase, Phase::Failed);
    assert_eq!(failed.applied_revision, 1);
    let component = entry(&failed, "component");
    assert_eq!(
        component.actual.as_ref().unwrap().artifact,
        Some(app.v1.clone())
    );
    assert_eq!(
        component.desired.as_ref().unwrap().artifact,
        Some(app.v2.clone())
    );
    assert!(
        entry(&failed, "client")
            .effects
            .iter()
            .any(|e| matches!(e.status, EffectStatus::Failed(_)))
    );
    let first = resource.dispose().await.unwrap_err();
    assert_eq!(first, resource.dispose().await.unwrap_err());
    assert!(prepare(app.loader.apply(next)).await.is_err());
    assert_eq!(*app.trace.lock().unwrap(), ["ready \"v1\""]);
    assert!(app.rt.shutdown().await.is_err());
}

#[tokio::test]
async fn new_initialization_failure_is_visible_without_claiming_rollback() {
    let app = App::new(json!({})).await;
    let old = app.handle();
    app.loader
        .apply(config(app.v2.clone(), json!({"fail":"activate"})))
        .await
        .unwrap();
    assert!(bounded(app.loader.settled()).await.is_err());
    let failed = app.loader.snapshot();
    let component = entry(&failed, "component");
    assert_eq!(failed.applied_revision, 2);
    assert_eq!(
        component.actual.as_ref().unwrap().artifact,
        Some(app.v2.clone())
    );
    assert!(component.instance.as_ref().unwrap().outcome.is_some());
    assert!(version(&old).await.is_err());
    assert_eq!(
        *app.trace.lock().unwrap(),
        ["ready \"v1\"", "cleanup \"v1\""]
    );
    app.loader
        .apply(config(app.v2.clone(), json!({})))
        .await
        .unwrap();
    bounded(app.loader.settled()).await.unwrap();
    assert_eq!(version(&app.handle()).await.unwrap(), "v2");
    app.rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_inflight_call_keeps_old_code_until_execution_finishes() {
    let app = App::new(json!({})).await;
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let old = app.handle();
    let waiting = tokio::spawn({
        let entered = entered.clone();
        let release = release.clone();
        async move {
            old.call(move |s, ctx| {
                Box::pin(async move {
                    entered.add_permits(1);
                    release.acquire().await.unwrap().forget();
                    assert_eq!(s.invoke_json(&ctx, "code-version", json!({})).await?, "v1");
                    Ok(())
                })
            })
            .await
        }
    });
    bounded(entered.acquire()).await.unwrap().forget();
    waiting.abort();
    let _ = waiting.await;
    let worker = app.loader.clone();
    let next = config(app.v2.clone(), json!({}));
    let mut updating = tokio::spawn(async move { worker.apply(next).await });
    // Applying starts after cold compilation; only the subsequent drain uses
    // the shorter execution bound.
    prepare(async {
        while app.loader.snapshot().phase != Phase::Applying {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut updating)
            .await
            .is_err()
    );
    assert_eq!(*app.trace.lock().unwrap(), ["ready \"v1\""]);
    release.add_permits(1);
    bounded(updating).await.unwrap().unwrap();
    bounded(app.loader.settled()).await.unwrap();
    assert_eq!(version(&app.handle()).await.unwrap(), "v2");
    app.rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn context_artifact_uses_explicit_bindings_and_wasi_without_widening_authority() {
    use cordis_wasm::{Capabilities, ContextService, Dependency, Wasi};
    let files = Files::new();
    let artifact = files.write("context.wasm", context_guest::bytes());
    let rt = Runtime::new();
    let logger = Service::<()>::new("example.context-log", "1");
    let service = Service::<ContextService>::new("example.context", "1").with_descriptor(
        cordis_contract::Descriptor::new(cordis_portable_example::context_fixture_interface())
            .unwrap(),
    );
    let mut registry = Registry::new();
    registry
        .register("logger", {
            let logger = logger.clone();
            move |_| {
                let logger = logger.clone();
                Ok(Plugin::new("logger", {
                    let logger = logger.clone();
                    move |ctx| {
                        let logger = logger.clone();
                        async move { ctx.provide(&logger, Arc::new(())) }
                    }
                })
                .provides(&logger))
            }
        })
        .unwrap();
    register_context_component(
        &mut registry,
        "context",
        service.clone(),
        cordis_wasm::Compiler::default(),
        Limits::default(),
        Capabilities {
            dependencies: vec![Dependency::new_json(
                &logger,
                cordis_contract::Descriptor::new(
                    cordis_portable_example::context::ContextLogClient::interface(),
                )
                .unwrap(),
                |_, _, _, args| Box::pin(async move { Ok(args) }),
            )],
            wasi: Some(Wasi {
                environment: [("ALLOWED".into(), "yes".into())].into(),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .unwrap();
    registry
        .register("client", move |_| {
            let service = service.clone();
            Ok(Plugin::new("client", {
                let service = service.clone();
                move |ctx| {
                    let service = service.clone();
                    async move {
                        ctx.dependency(&service)?
                            .call(|s, ctx| {
                                Box::pin(async move {
                                    assert_eq!(
                                        s.invoke_json(&ctx, "echo", json!(42)).await?,
                                        json!(42.0)
                                    );
                                    assert_eq!(
                                        s.invoke_json(&ctx, "wasi-env", json!({})).await?,
                                        json!({"entries":[{"name":"ALLOWED","value":"yes"}]})
                                    );
                                    Ok(())
                                })
                            })
                            .await
                    }
                }
            })
            .requires(&service))
        })
        .unwrap();
    let loader = Loader::new(rt.clone(), registry);
    loader
        .apply(Configuration {
            entries: vec![
                Entry::new("code", "context", json!({})).artifact(artifact.clone()),
                Entry::new("logger", "logger", Value::Null),
                Entry::new("client", "client", Value::Null),
            ],
        })
        .await
        .unwrap();
    bounded(loader.settled()).await.unwrap();
    // Unknown business config cannot manufacture additional WASI authority.
    prepare(loader.apply(Configuration {
        entries: vec![
                Entry::new(
                    "code",
                    "context",
                    json!({"wasi":{"environment":{"UNAUTHORIZED":"bad"}}}),
                )
                .artifact(artifact),
                Entry::new("logger", "logger", Value::Null),
            ],
    }))
    .await
    .unwrap();
    let rejected = bounded(loader.settled()).await.unwrap_err().to_string();
    assert!(
        rejected.contains("invalid_argument") && rejected.contains("wasi"),
        "{rejected}"
    );
    rt.shutdown().await.unwrap();
}

struct Rolling {
    rt: Runtime,
    loader: Loader,
    client: PluginHandle,
    handle: ServiceHandle<Broker<WasmService>>,
    candidates: Arc<Mutex<std::collections::BTreeMap<Activation, BrokerRegistration<WasmService>>>>,
    files: Files,
    old: Entry,
    new: Entry,
}
impl Rolling {
    async fn new(settings: Value) -> Self {
        let files = Files::new();
        let old = Entry::new("v1", "candidate", settings)
            .artifact(files.write("old.wasm", support::bytes()));
        let new =
            Entry::new("v2", "candidate", json!({})).artifact(files.write("new.wasm", v2_bytes()));
        let rt = Runtime::new();
        let service = Service::<Broker<WasmService>>::new("example.calculator", "1")
            .with_descriptor(
                cordis_contract::portable::decode(
                    &cordis_test_contracts::resources::rust_fixture::SERVICE
                        .parse()
                        .unwrap(),
                )
                .unwrap(),
            );
        let broker = rt
            .install(
                Plugin::new("broker", {
                    let service = service.clone();
                    move |ctx| {
                        let service = service.clone();
                        async move { ctx.provide_broker(&service) }
                    }
                })
                .provides(&service),
            )
            .unwrap();
        bounded(broker.ready()).await.unwrap();
        let saved = Arc::new(Mutex::new(None));
        let client = rt
            .install(
                Plugin::new("client", {
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
        bounded(client.ready()).await.unwrap();
        let handle = saved.lock().unwrap().take().unwrap();
        let candidates = Arc::new(Mutex::new(std::collections::BTreeMap::new()));
        let mut registry = Registry::new();
        cordis_wasm::loader::register_broker_component(
            &mut registry,
            "candidate",
            service,
            cordis_wasm::Compiler::default(),
            Limits::default(),
            {
                let candidates = candidates.clone();
                move |registration| {
                    candidates.lock().unwrap().insert(
                        registration.activation().ok_or(Error::Closed)?,
                        registration,
                    );
                    Ok(())
                }
            },
        )
        .unwrap();
        let loader = Loader::new(rt.clone(), registry);
        prepare(loader.apply(Configuration {
            entries: vec![old.clone()],
        }))
        .await
        .unwrap();
        bounded(loader.settled()).await.unwrap();
        let app = Self {
            rt,
            loader,
            client,
            handle,
            candidates,
            files,
            old,
            new,
        };
        app.promote("v1");
        app
    }
    fn promote(&self, id: &str) {
        let activation = entry(&self.loader.snapshot(), id)
            .instance
            .as_ref()
            .unwrap()
            .activation
            .unwrap();
        self.candidates.lock().unwrap()[&activation]
            .promote()
            .unwrap();
    }
    async fn version(&self) -> Value {
        self.handle
            .route(|s, ctx| {
                Box::pin(async move {
                    s.invoke_json(&ctx, "code-version", json!({}))
                        .await
                        .map_err(Into::into)
                })
            })
            .await
            .unwrap()
    }
    async fn counter(&self) -> cordis_wasm::WasmResource {
        self.handle
            .route(|s, ctx| {
                Box::pin(async move {
                    s.acquire_json(&ctx, "counter", json!({"initial": 42}))
                        .await
                        .map_err(Into::into)
                })
            })
            .await
            .unwrap()
    }
    async fn switch(&self) {
        prepare(self.loader.apply(Configuration {
            entries: vec![self.old.clone(), self.new.clone()],
        }))
        .await
        .unwrap();
        bounded(self.loader.settled()).await.unwrap();
        assert_eq!(self.version().await, "v1");
        self.promote("v2");
        assert_eq!(self.version().await, "v2");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_rolling_wasm_keeps_old_resource_and_consumer_across_cancelled_retirement_wait() {
    let app = Rolling::new(json!({})).await;
    let activation = app.client.snapshot().activation;
    let resource = app.counter().await;
    app.switch().await;
    let waiter = tokio::spawn({
        let loader = app.loader.clone();
        let new = app.new.clone();
        async move { loader.apply(Configuration { entries: vec![new] }).await }
    });
    // This wait includes artifact preparation; subsequent drain keeps its shorter bound.
    prepare(async {
        loop {
            if entry(&app.loader.snapshot(), "v1")
                .instance
                .as_ref()
                .unwrap()
                .state
                == Lifecycle::Unloading
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!waiter.is_finished());
    waiter.abort();
    let _ = waiter.await;
    assert_eq!(resource.invoke_json("add", json!(1)).await.unwrap(), "43");
    assert_eq!(app.version().await, "v2");
    resource.dispose().await.unwrap();
    bounded(async {
        while app.loader.snapshot().phase == Phase::Applying {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(app.loader.snapshot().phase, Phase::Idle);
    assert_eq!(app.client.snapshot().activation, activation);
    assert!(resource.invoke_json("get", json!({})).await.is_err());
    assert_eq!(app.version().await, "v2");
    bounded(app.rt.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_wasm_bad_preflight_and_failed_initialization_preserve_live_route() {
    let app = Rolling::new(json!({})).await;
    let activation = app.client.snapshot().activation;
    let bad = Entry::new("bad", "candidate", json!({}))
        .artifact(app.files.write("bad.wasm", b"invalid component"));
    assert!(
        app.loader
            .apply(Configuration {
                entries: vec![app.old.clone(), bad]
            })
            .await
            .is_err()
    );
    assert_eq!(app.version().await, "v1");
    let broken = Entry::new("broken", "candidate", json!({"fail":"create"}))
        .artifact(app.new.artifact.clone().unwrap());
    prepare(app.loader.apply(Configuration {
        entries: vec![app.old.clone(), broken],
    }))
    .await
    .unwrap();
    assert!(bounded(app.loader.settled()).await.is_err());
    assert_eq!(app.version().await, "v1");
    assert_eq!(app.client.snapshot().activation, activation);
    app.switch().await;
    bounded(app.rt.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_wasm_failed_resource_inverse_blocks_old_entry_but_v2_keeps_serving() {
    let app = Rolling::new(json!({"fail":"release"})).await;
    let resource = app.counter().await;
    app.switch().await;
    let error = resource.dispose().await.unwrap_err();
    assert_eq!(resource.dispose().await.unwrap_err(), error);
    let result = prepare(app.loader.apply(Configuration {
        entries: vec![app.new.clone()],
    }))
    .await;
    assert!(result.is_err());
    let snapshot = app.loader.snapshot();
    let old = entry(&snapshot, "v1");
    assert_eq!(snapshot.phase, Phase::Failed);
    assert_eq!(old.instance.as_ref().unwrap().state, Lifecycle::Unloading);
    // No implementation inverse may begin while this caller resource is unresolved.
    assert!(
        old.effects
            .iter()
            .all(|effect| effect.status == EffectStatus::Owned)
    );
    assert_eq!(app.version().await, "v2");
    assert!(bounded(app.rt.shutdown()).await.is_err());
}
