#[path = "../../../../sdk/rust/package_policy.rs"]
mod package_policy;
use super::*;
use cordis_loader::package::{FileKind, HOST_TARGET, PackageFile, PackageManifest, digest};
use cordis_loader::{ArtifactRef, Configuration, Entry, Loader, Registry};
use cordis_remote::{
    ArtifactOptions, Launch, ProcessArtifact, Value, loader::register_broker_process,
};
use std::collections::BTreeMap;

struct Files(PathBuf);
impl Files {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "cordis-process-update-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        Self(root)
    }
    fn options(&self, backend: Backend) -> ArtifactOptions {
        let mut options = ArtifactOptions {
            package: package_policy::example_policy(self.0.join("staged")),
            ..Default::default()
        };
        if backend.is_typescript() {
            let output = std::process::Command::new("node")
                .args(["-p", "process.execPath"])
                .output()
                .unwrap();
            assert!(output.status.success());
            let version = std::process::Command::new("node")
                .args(["-p", "process.version"])
                .output()
                .unwrap();
            options.package.host_dependencies.insert(
                "interpreter".into(),
                format!("node:{}", String::from_utf8(version.stdout).unwrap().trim()),
            );
            options.launch = Launch::Interpreter {
                executable: String::from_utf8(output.stdout).unwrap().trim().into(),
                args: vec![],
            };
        }
        options
    }
    fn artifact(&self, backend: Backend, version: &str) -> ArtifactRef {
        let root = self.0.join(version);
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("cordis-package.json");
        match backend {
            Backend::TypeScript => {
                let pack = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../sdk/typescript/examples/package-calculator.mjs");
                let output = std::process::Command::new("node")
                    .arg(pack)
                    .arg(&path)
                    .arg(version)
                    .output()
                    .unwrap();
                assert!(
                    output.status.success(),
                    "{}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            #[cfg(unix)]
            Backend::Cpp => self.cpp_artifact(&root, version),
        }
        ArtifactRef::from_file(
            path,
            package_policy::example_policy(self.0.join("staged")).max_manifest_bytes,
        )
        .unwrap()
    }
    fn count(&self, version: &str, event: &str) -> usize {
        std::fs::read_to_string(self.0.join(format!("{version}.log")))
            .unwrap_or_default()
            .lines()
            .filter(|s| *s == event)
            .count()
    }
    fn staged(&self) -> usize {
        std::fs::read_dir(self.0.join("staged"))
            .map(|r| r.count())
            .unwrap_or(0)
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        // A live session or unresolved responsibility can still retain a snapshot.
        if self.staged() == 0 {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
async fn client<T: Send + Sync + 'static>(
    rt: &Runtime,
    service: &Service<T>,
) -> (PluginHandle, Arc<Mutex<Option<ServiceHandle<T>>>>) {
    let saved = Arc::new(Mutex::new(None));
    let handle = rt
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
            .requires(service),
        )
        .unwrap();
    bounded(handle.ready()).await.unwrap();
    (handle, saved)
}
async fn version(handle: &ServiceHandle<RemoteService>) -> Value {
    call(handle, Endpoint::Ipc, "code-version", serde_json::json!({}))
        .await
        .unwrap()
}
struct Rolling {
    rt: Runtime,
    loader: Loader,
    consumer: PluginHandle,
    handle: ServiceHandle<Broker<RemoteService>>,
    tokens: Arc<Mutex<BTreeMap<Activation, BrokerRegistration<RemoteService>>>>,
    old: Entry,
    new: Entry,
    files: Files,
}
impl Rolling {
    async fn new(backend: Backend, endpoint: Endpoint) -> Self {
        let files = Files::new();
        let old = Entry::new("v1", "process", json!({"version":"v1"}))
            .artifact(files.artifact(backend, "v1"));
        let new = Entry::new("v2", "process", json!({"version":"v2"}))
            .artifact(files.artifact(backend, "v2"));
        let rt = Runtime::new();
        let service = Service::<Broker<RemoteService>>::new("example.calculator", "1")
            .with_descriptor(backend.rolling_service().descriptor().unwrap().clone());
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
        let (consumer, saved) = client(&rt, &service).await;
        let handle = saved.lock().unwrap().take().unwrap();
        let tokens = Arc::new(Mutex::new(BTreeMap::new()));
        let mut registry = Registry::new();
        let root = files.0.clone();
        register_broker_process(
            &mut registry,
            "process",
            service,
            files.options(backend),
            move |config| {
                let mut spec = ProcessSpec::new("ignored-artifact-template");
                spec.endpoint = endpoint;

                if matches!(endpoint, Endpoint::Ipc) {
                    spec.args.push("--stdio-only".into());
                }
                if !backend.is_typescript() && config["plain_args"] != true {
                    spec.args.extend([
                        "--test-hooks".into(),
                        "--trace".into(),
                        root.join(format!("{}.log", config["version"].as_str().unwrap()))
                            .display()
                            .to_string(),
                    ]);
                    if config["fail_activate"] == true {
                        spec.args.push("--fail-activate".into());
                    }
                    if config["fail_deactivate"] == true {
                        spec.args.push("--fail-deactivate".into());
                    }
                }
                Ok(spec)
            },
            {
                let tokens = tokens.clone();
                move |token| {
                    // Registration occurs before Active; publication is never implicit.
                    assert!(token.promote().is_err());
                    tokens
                        .lock()
                        .unwrap()
                        .insert(token.activation().unwrap(), token);
                    Ok(())
                }
            },
        )
        .unwrap();
        let loader = Loader::new(rt.clone(), registry);
        bounded(loader.apply(Configuration {
            entries: vec![old.clone()],
        }))
        .await
        .unwrap();
        bounded(loader.settled()).await.unwrap();
        let app = Self {
            rt,
            loader,
            consumer,
            handle,
            tokens,
            old,
            new,
            files,
        };
        app.promote("v1");
        app
    }
    fn activation(&self, id: &str) -> Activation {
        self.loader
            .snapshot()
            .entries
            .iter()
            .find(|e| e.id == id)
            .unwrap()
            .instance
            .as_ref()
            .unwrap()
            .activation
            .unwrap()
    }
    fn promote(&self, id: &str) {
        self.tokens.lock().unwrap()[&self.activation(id)]
            .promote()
            .unwrap();
    }
    async fn call(&self, method: &str, args: Value) -> Result<Value> {
        let method = method.to_owned();
        self.handle
            .route(move |s, ctx| {
                Box::pin(
                    async move { s.invoke_json(&ctx, &method, args).await.map_err(Into::into) },
                )
            })
            .await
    }
    async fn counter(&self, fail: bool) -> RemoteResource {
        self.handle
            .route(move |s, ctx| {
                Box::pin(async move {
                    s.acquire_json(
                        &ctx,
                        "counter",
                        if fail {
                            json!({"initial":42,"fail_close":true})
                        } else {
                            json!({"initial":42})
                        },
                    )
                    .await
                    .map_err(Into::into)
                })
            })
            .await
            .unwrap()
    }
    async fn switch(&self) {
        bounded(self.loader.apply(Configuration {
            entries: vec![self.old.clone(), self.new.clone()],
        }))
        .await
        .unwrap();
        bounded(self.loader.settled()).await.unwrap();
        assert_eq!(
            self.call("code-version", serde_json::json!({}))
                .await
                .unwrap(),
            "v1"
        );
        self.promote("v2");
        assert_eq!(
            self.call("code-version", serde_json::json!({}))
                .await
                .unwrap(),
            "v2"
        );
    }
    fn retire(&self) -> tokio::task::JoinHandle<cordis_loader::Result<cordis_loader::ApplyReport>> {
        let loader = self.loader.clone();
        let new = self.new.clone();
        tokio::spawn(async move { loader.apply(Configuration { entries: vec![new] }).await })
    }
    async fn closing(&self) {
        bounded(async {
            loop {
                let s = self.loader.snapshot();
                if s.entries
                    .iter()
                    .find(|e| e.id == "v1")
                    .unwrap()
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
    }
    async fn shutdown(&self) {
        bounded(self.rt.shutdown()).await.unwrap();
        let tokens = std::mem::take(&mut *self.tokens.lock().unwrap());
        drop(tokens);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn executable_and_script_packages_restart_pin_the_verified_bytes_and_imports() {
    for &backend in Backend::ALL {
        let files = Files::new();
        let old = files.artifact(backend, "v1");
        let new = files.artifact(backend, "v2");
        let code = ProcessArtifact::prepare(&old, &files.options(backend)).unwrap();
        std::fs::copy(&new.location, &old.location).unwrap();
        assert!(ProcessArtifact::prepare(&old, &files.options(backend)).is_err());
        let rt = Runtime::new();
        let provider = rt
            .install(
                code.plugin("fixed", &calculator(), ProcessSpec::new("ignored"))
                    .unwrap(),
            )
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (consumer, saved) = client(&rt, &calculator()).await;
        let first = saved.lock().unwrap().as_ref().unwrap().clone();
        assert_eq!(version(&first).await, "v1");
        bounded(provider.restart()).await.unwrap();
        bounded(provider.ready()).await.unwrap();
        bounded(consumer.ready()).await.unwrap();
        let restarted = saved.lock().unwrap().as_ref().unwrap().clone();
        assert_eq!(version(&restarted).await, "v1");
        assert!(
            call(&first, Endpoint::Ipc, "code-version", serde_json::json!({}))
                .await
                .is_err()
        );
        bounded(rt.shutdown()).await.unwrap();
        drop((first, restarted, saved, provider, consumer, code, rt));
        bounded(async {
            while files.staged() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rolling_processes_preserve_consumers_and_old_resources_across_endpoints() {
    for &backend in Backend::ALL {
        for endpoint in [Endpoint::Ipc, Endpoint::Http] {
            let app = Rolling::new(backend, endpoint).await;
            let activation = app.consumer.snapshot().activation;
            let old_activation = app.activation("v1");
            let resource = app.counter(false).await;
            app.switch().await;
            let wait = app.retire();
            app.closing().await;
            assert!(!wait.is_finished());
            assert_eq!(
                resource
                    .invoke_json("code-version", serde_json::json!({}))
                    .await
                    .unwrap(),
                "v1"
            );
            assert_eq!(resource.invoke_json("add", json!(1)).await.unwrap(), "43");
            assert_eq!(
                app.call("code-version", serde_json::json!({}))
                    .await
                    .unwrap(),
                "v2"
            );
            if !backend.is_typescript() {
                assert_eq!(app.files.count("v1", "deactivated"), 0);
            }
            wait.abort();
            let _ = wait.await;
            resource.dispose().await.unwrap();
            bounded(app.loader.settled()).await.unwrap();
            assert_eq!(app.consumer.snapshot().activation, activation);
            assert!(
                resource
                    .invoke_json("get", serde_json::json!({}))
                    .await
                    .is_err()
            );
            if !backend.is_typescript() {
                assert_eq!(app.files.count("v1", "deactivated"), 1);
            }
            drop(resource);
            let old = app.tokens.lock().unwrap().remove(&old_activation).unwrap();
            assert!(old.promote().is_err());
            drop(old);
            bounded(async {
                while app.files.staged() != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await;
            app.shutdown().await;
        }
    }
}

#[test]
fn script_package_preparation_does_not_execute_and_requires_host_interpreter() {
    let files = Files::new();
    let mut options = files.options(Backend::TypeScript);
    let source = b"throw new Error('must not execute in preparation');";
    std::fs::write(files.0.join("main.mjs"), source).unwrap();
    let manifest = PackageManifest {
        target: HOST_TARGET.into(),
        entry: "main.mjs".into(),
        files: vec![PackageFile {
            path: "main.mjs".into(),
            sha256: digest(source),
            kind: FileKind::Data,
        }],
        host_dependencies: [(
            "interpreter".into(),
            options.package.host_dependencies["interpreter"].clone(),
        )]
        .into(),
    };
    let reference = manifest.write(&files.0).unwrap();
    drop(ProcessArtifact::prepare(&reference, &options).unwrap());
    assert_eq!(files.staged(), 0);
    options.launch = Launch::Executable;
    assert!(ProcessArtifact::prepare(&reference, &options).is_err());
    options = files.options(Backend::TypeScript);
    options.package.host_dependencies.remove("interpreter");
    assert!(ProcessArtifact::prepare(&reference, &options).is_err());
    assert_eq!(files.staged(), 0);
}

#[cfg(unix)]
#[path = "updates_cpp.rs"]
mod cpp;
