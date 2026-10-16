#![cfg(feature = "loader")]
#[path = "support/artifacts.rs"]
mod artifacts;
use artifacts::binary;
use cordis_abi::{
    AbiResource, AbiService, ArtifactOptions, Library, Value, json,
    loader::{register_broker_library, register_library},
};
use cordis_loader::{ArtifactRef, Configuration, Entry, Loader, LoaderSnapshot, Phase, Registry};
use cordis_sdk::*;
use std::{
    collections::BTreeMap,
    future::Future,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .expect("ABI update stalled")
}
struct Files {
    root: PathBuf,
}
impl Files {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "cordis-abi-update-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        Self { root }
    }
    fn artifact(&self, name: &str, library: &str) -> ArtifactRef {
        let root = self.root.join(name);
        std::fs::create_dir_all(&root).unwrap();
        let source = binary(library);
        let path = root.join(source.file_name().unwrap());
        std::fs::copy(source, &path).unwrap();
        artifacts::package(&path)
    }
    fn private_package(&self, generation: &str) -> ArtifactRef {
        use cordis_loader::package::{FileKind, PackageManifest};
        let root = self.root.join(format!("private-{generation}"));
        std::fs::create_dir_all(&root).unwrap();
        let mut members = Vec::new();
        for target in [
            format!("cordis_package_abi_{generation}"),
            format!("cordis_package_helper_{generation}_{generation}"),
            format!("cordis_package_leaf_{generation}_{generation}"),
        ] {
            let source = binary(&target);
            let name = source.file_name().unwrap().to_str().unwrap().to_owned();
            std::fs::copy(source, root.join(&name)).unwrap();
            members.push((name, FileKind::Library));
        }
        PackageManifest::from_files(
            &root,
            members[0].0.clone(),
            members,
            &self.options().package,
        )
        .unwrap()
        .write(&root)
        .unwrap()
    }
    fn options(&self) -> ArtifactOptions {
        artifacts::options(self.root.join("staged"))
    }
    fn member(&self, artifact: &ArtifactRef) -> PathBuf {
        let manifest: cordis_loader::package::PackageManifest =
            serde_json::from_slice(&std::fs::read(&artifact.location).unwrap()).unwrap();
        std::path::Path::new(&artifact.location)
            .parent()
            .unwrap()
            .join(manifest.entry)
    }
    fn config(&self, version: &str, mut config: Value) -> Value {
        config["trace"] = json!(self.root.join(version));
        config
    }
    fn count(&self, version: &str, event: &str) -> usize {
        std::fs::read_to_string(self.root.join(version))
            .unwrap_or_default()
            .lines()
            .filter(|line| *line == event)
            .count()
    }
    async fn wait(&self, version: &str, event: &str) {
        bounded(async {
            while self.count(version, event) == 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await;
    }
    fn gate(&self, name: &str) -> Value {
        json!(self.root.join(name))
    }
    fn open(&self, name: &str) {
        std::fs::write(self.root.join(name), b"open").unwrap();
    }
    fn staged(&self) -> Vec<PathBuf> {
        let mut paths = Vec::new();
        if let Ok(entries) = std::fs::read_dir(self.options().package.staging_dir) {
            for dir in entries {
                for file in std::fs::read_dir(dir.unwrap().path()).unwrap() {
                    paths.push(file.unwrap().path());
                }
            }
        }
        paths.sort();
        paths
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        // Failed recovery intentionally retains the loaded module AND its file.
        // Tests must not erase that evidence or mutate a retained code snapshot.
        if self.staged().is_empty() {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}
fn load(artifact: &ArtifactRef, options: &ArtifactOptions) -> cordis_abi::Result<Library> {
    // SAFETY: only repository fixtures/system dependencies are supplied. Malformed
    // fixtures violate checked fields, never memory validity or native ABI rules.
    #[allow(unsafe_code)]
    unsafe {
        Library::load_artifact(artifact, options)
    }
}
fn entry<'a>(snapshot: &'a LoaderSnapshot, id: &str) -> &'a cordis_loader::EntrySnapshot {
    snapshot.entries.iter().find(|e| e.id == id).unwrap()
}
async fn ordinary_version(handle: &ServiceHandle<AbiService>) -> Result<Value> {
    handle
        .call(|s, ctx| {
            Box::pin(async move {
                s.invoke_json(&ctx, "code-version", cordis_wire::json!({}))
                    .await
                    .map_err(Into::into)
            })
        })
        .await
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

struct Rolling {
    rt: Runtime,
    loader: Loader,
    client: PluginHandle,
    handle: ServiceHandle<Broker<AbiService>>,
    tokens: Arc<Mutex<BTreeMap<Activation, BrokerRegistration<AbiService>>>>,
    files: Files,
    old: Entry,
    new: Entry,
}
impl Rolling {
    async fn new(mut settings: Value) -> Self {
        let files = Files::new();
        // Gate paths must be per-test, not global fixture state.
        for key in ["invoke_gate", "acquire_gate", "release_gate"] {
            if settings[key] == true {
                settings[key] = files.gate(key);
            }
        }
        let old = Entry::new("v1", "library", files.config("v1", settings))
            .artifact(files.artifact("old", "cordis_abi_fixture"));
        let new = Entry::new("v2", "library", files.config("v2", cordis_wire::json!({})))
            .artifact(files.artifact("new", "cordis_abi_fixture_v2"));
        let rt = Runtime::new();
        let service = Service::<Broker<AbiService>>::new("example.calculator", "1")
            .with_descriptor(
                cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true))
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
        let (client, saved) = client(&rt, &service).await;
        let handle = saved.lock().unwrap().take().unwrap();
        let tokens = Arc::new(Mutex::new(BTreeMap::new()));
        let mut registry = Registry::new();
        // SAFETY: all artifacts are repository fixture libraries. Private staging
        // and system dependency lookup are controlled by this test process.
        #[allow(unsafe_code)]
        unsafe {
            register_broker_library(&mut registry, "library", service, files.options(), {
                let tokens = tokens.clone();
                move |token| {
                    tokens
                        .lock()
                        .unwrap()
                        .insert(token.activation().ok_or(Error::Closed)?, token);
                    Ok(())
                }
            })
            .unwrap();
        }
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
            client,
            handle,
            tokens,
            files,
            old,
            new,
        };
        app.promote("v1");
        app
    }
    fn activation(&self, id: &str) -> Activation {
        entry(&self.loader.snapshot(), id)
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
    async fn version(&self) -> Value {
        self.handle
            .route(|s, ctx| {
                Box::pin(async move {
                    s.invoke_json(&ctx, "code-version", cordis_wire::json!({}))
                        .await
                        .map_err(Into::into)
                })
            })
            .await
            .unwrap()
    }
    async fn counter(&self) -> AbiResource {
        self.handle
            .route(|s, ctx| {
                Box::pin(async move {
                    s.acquire_json(&ctx, "counter", json!({"initial":42}))
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
        assert_eq!(self.version().await, "v1");
        self.promote("v2");
        assert_eq!(self.version().await, "v2");
    }
    fn retire_old(
        &self,
    ) -> tokio::task::JoinHandle<cordis_loader::Result<cordis_loader::ApplyReport>> {
        let loader = self.loader.clone();
        let new = self.new.clone();
        tokio::spawn(async move { loader.apply(Configuration { entries: vec![new] }).await })
    }
    async fn closing(&self) {
        bounded(async {
            loop {
                if entry(&self.loader.snapshot(), "v1")
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
        self.tokens.lock().unwrap().clear();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verified_code_survives_source_overwrite_restart_and_independent_versions() {
    let files = Files::new();
    let source = files.artifact("mutable", "cordis_abi_calculator");
    let library = load(&source, &files.options()).unwrap();
    let first_snapshot = files.staged();
    assert_eq!(first_snapshot.len(), 1);
    assert!(
        std::fs::metadata(&first_snapshot[0])
            .unwrap()
            .permissions()
            .readonly()
    );
    let rt = Runtime::new();
    let service = Service::new("example.calculator", "1").with_descriptor(
        cordis_contract::portable::decode(
            &cordis_example_contracts::resources::calculator_abi::SERVICE
                .parse()
                .unwrap(),
        )
        .unwrap(),
    );
    let provider = rt
        .install(
            library
                .plugin("v1", &service, cordis_wire::json!({}))
                .unwrap(),
        )
        .unwrap();
    let (consumer, saved) = client(&rt, &service).await;
    let first = saved.lock().unwrap().take().unwrap();
    let new_source = files.artifact("mutable", "cordis_abi_calculator_v2");
    assert!(load(&source, &files.options()).is_err()); // old package identity cannot select v2
    let newer = load(&new_source, &files.options()).unwrap();
    assert_eq!(files.staged().len(), 2);
    assert_eq!(ordinary_version(&first).await.unwrap(), "v1");
    bounded(provider.restart()).await.unwrap();
    bounded(consumer.ready()).await.unwrap();
    let restarted = saved.lock().unwrap().take().unwrap();
    assert_eq!(ordinary_version(&restarted).await.unwrap(), "v1");
    assert!(ordinary_version(&first).await.is_err());
    let local = View::new().isolate(&service);
    let second = rt
        .install_in(
            local.clone(),
            newer
                .plugin("v2", &service, cordis_wire::json!({}))
                .unwrap(),
        )
        .unwrap();
    bounded(second.ready()).await.unwrap();
    let saved2 = Arc::new(Mutex::new(None));
    let other = rt
        .install_in(
            local,
            Plugin::new("other client", {
                let service = service.clone();
                let saved = saved2.clone();
                move |ctx| {
                    let service = service.clone();
                    let saved = saved.clone();
                    async move {
                        *saved.lock().unwrap() = Some(ctx.dependency(&service)?);
                        Ok(())
                    }
                }
            })
            .requires(&service),
        )
        .unwrap();
    bounded(other.ready()).await.unwrap();
    let handle2 = saved2.lock().unwrap().take().unwrap();
    assert_eq!(ordinary_version(&handle2).await.unwrap(), "v2");
    bounded(rt.shutdown()).await.unwrap();
    drop((
        first, restarted, handle2, library, newer, provider, second, consumer, other, rt,
    ));
    bounded(async {
        while !files.staged().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rolling_resources_pin_v1_until_release_and_last_code_reference_disappears() {
    let app = Rolling::new(json!({"release_gate":true})).await;
    let activation = app.client.snapshot().activation;
    let old = app.activation("v1");
    let resource = app.counter().await;
    app.switch().await;
    let retiring = app.retire_old();
    app.closing().await;
    assert!(!retiring.is_finished());
    assert_eq!(
        resource
            .invoke_json("code-version", cordis_wire::json!({}))
            .await
            .unwrap(),
        "v1"
    );
    assert_eq!(
        resource.invoke_json("add", json!(1)).await.unwrap(),
        json!(43.0)
    );
    assert_eq!(app.version().await, "v2");
    let release = tokio::spawn({
        let resource = resource.clone();
        async move { resource.dispose().await }
    });
    app.files.wait("v1", "release_gate-entered").await;
    assert_eq!(app.files.count("v1", "deactivate"), 0);
    assert_eq!(app.files.count("v1", "unload"), 0);
    app.files.open("release_gate");
    bounded(release).await.unwrap().unwrap();
    bounded(retiring).await.unwrap().unwrap();
    assert_eq!(app.files.count("v1", "destroy"), 1);
    assert_eq!(app.files.count("v1", "unload"), 0);
    assert!(
        resource
            .invoke_json("get", cordis_wire::json!({}))
            .await
            .is_err()
    );
    drop(resource);
    let token = app.tokens.lock().unwrap().remove(&old).unwrap();
    token.dispose().await.unwrap();
    assert!(token.promote().is_err());
    drop(token);
    app.files.wait("v1", "unload").await;
    assert_eq!(app.client.snapshot().activation, activation);
    assert_eq!(app.version().await, "v2");
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_call_wait_and_retirement_wait_still_drain_old_native_execution() {
    let app = Rolling::new(json!({"invoke_gate":true})).await;
    let observed = Arc::new(Mutex::new(None));
    let waiting = tokio::spawn({
        let handle = app.handle.clone();
        let observed = observed.clone();
        async move {
            handle
                .route(move |s, ctx| {
                    Box::pin(async move {
                        *observed.lock().unwrap() =
                            Some(s.invoke_json(&ctx, "hold-version", Value::Null).await?);
                        Ok(())
                    })
                })
                .await
        }
    });
    app.files.wait("v1", "invoke_gate-entered").await;
    waiting.abort();
    let _ = waiting.await;
    // The C++ SDK serializes each instance: do not probe v1 while its held
    // invocation owns that mutex. Candidate v2 has an independent instance.
    bounded(app.loader.apply(Configuration {
        entries: vec![app.old.clone(), app.new.clone()],
    }))
    .await
    .unwrap();
    bounded(app.loader.settled()).await.unwrap();
    app.promote("v2");
    let retiring = app.retire_old();
    app.closing().await;
    retiring.abort();
    let _ = retiring.await;
    assert_eq!(app.files.count("v1", "deactivate"), 0);
    assert_eq!(app.version().await, "v2");
    app.files.open("invoke_gate");
    bounded(async {
        while app.loader.snapshot().phase == Phase::Applying {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(app.loader.snapshot().phase, Phase::Idle);
    assert_eq!(*observed.lock().unwrap(), Some(json!("v1")));
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_candidates_fail_before_retirement_and_staging_is_reclaimed() {
    let app = Rolling::new(cordis_wire::json!({})).await;
    let active = app.client.snapshot().activation;
    let before = app.loader.snapshot();
    let live_files = app.files.staged();
    let mut digest = app.new.clone();
    digest.artifact.as_mut().unwrap().sha256 = "0".repeat(64);
    let invalid_path = app.files.root.join("invalid");
    std::fs::write(&invalid_path, b"not a native library").unwrap();
    let invalid = Entry::new("invalid", "library", cordis_wire::json!({}))
        .artifact(ArtifactRef::from_file(invalid_path, 100).unwrap());
    let bad_table = Entry::new("table", "library", cordis_wire::json!({}))
        .artifact(app.files.artifact("bad-table", "cordis_abi_bad_1"));
    for bad in [digest, invalid, bad_table] {
        assert!(
            bounded(app.loader.apply(Configuration {
                entries: vec![app.old.clone(), bad]
            }))
            .await
            .is_err()
        );
        assert_eq!(
            app.loader.snapshot().desired_revision,
            before.desired_revision
        );
        assert_eq!(app.version().await, "v1");
        assert_eq!(app.files.staged(), live_files);
    }
    let mut small = app.files.options();
    small.package.max_bytes = 1;
    assert!(load(app.new.artifact.as_ref().unwrap(), &small).is_err());
    let mut registry = Registry::new();
    // SAFETY: same trusted binaries; this intentionally mismatches only the service revision.
    #[allow(unsafe_code)]
    unsafe {
        register_library(
            &mut registry,
            "library",
            Service::new("example.calculator", "2").with_descriptor(
                cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true))
                    .unwrap(),
            ),
            app.files.options(),
        )
        .unwrap();
    }
    let incompatible = Loader::new(app.rt.clone(), registry);
    assert!(
        bounded(incompatible.apply(Configuration {
            entries: vec![app.new.clone()]
        }))
        .await
        .is_err()
    );
    assert_eq!(app.files.staged(), live_files);
    assert_eq!(app.client.snapshot().activation, active);
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_candidate_initialization_preserves_v1_and_explicit_retry_can_promote_v2() {
    let app = Rolling::new(cordis_wire::json!({})).await;
    let mut bad = app.new.clone();
    bad.config["fail_activate"] = json!(true);
    bounded(app.loader.apply(Configuration {
        entries: vec![app.old.clone(), bad],
    }))
    .await
    .unwrap();
    assert!(bounded(app.loader.settled()).await.is_err());
    assert_eq!(app.version().await, "v1");
    let snapshot = app.loader.snapshot();
    assert!(matches!(
        entry(&snapshot, "v2").instance.as_ref().unwrap().outcome,
        Some(Outcome::InitializationFailed(_))
    ));
    assert_eq!(app.files.count("v2", "destroy"), 1);
    assert_eq!(app.tokens.lock().unwrap().len(), 1); // failure never registers a candidate
    app.switch().await;
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_old_resource_inverse_is_cached_and_keeps_the_module_and_snapshot() {
    let app = Rolling::new(json!({"fail_release":true})).await;
    let resource = app.counter().await;
    let old_snapshot = app.files.staged()[0].clone();
    app.switch().await;
    let error = resource.dispose().await.unwrap_err();
    assert_eq!(resource.dispose().await.unwrap_err(), error);
    assert!(bounded(app.retire_old()).await.unwrap().is_err());
    let snapshot = app.loader.snapshot();
    assert_eq!(snapshot.phase, Phase::Failed);
    assert_eq!(
        entry(&snapshot, "v1").instance.as_ref().unwrap().state,
        Lifecycle::Unloading
    );
    assert!(
        entry(&snapshot, "v1")
            .effects
            .iter()
            .all(|effect| effect.status == EffectStatus::Owned)
    );
    assert_eq!(app.files.count("v1", "release"), 1);
    assert_eq!(app.files.count("v1", "deactivate"), 0);
    assert_eq!(app.files.count("v1", "unload"), 0);
    assert_eq!(app.version().await, "v2");
    assert!(bounded(app.rt.shutdown()).await.is_err());
    drop(resource);
    drop(app);
    assert!(old_snapshot.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_inner_acquisition_keeps_fault_authority_and_unknown_code_responsibility() {
    let app = Rolling::new(json!({"acquire_gate":true})).await;
    let entered = app.files.root.join("v1");
    app.handle.route(move |s, ctx| Box::pin(async move {
        let acquisition=s.acquire_json(&ctx,"unknown",cordis_wire::json!({})); tokio::pin!(acquisition);
        tokio::select! {
            result=&mut acquisition => panic!("unexpected early result: {}",result.is_ok()),
            _=async { loop { if std::fs::read_to_string(&entered).unwrap_or_default().contains("acquire_gate-entered") {break;} tokio::time::sleep(Duration::from_millis(2)).await; } } => {}
        }
        Ok(())
    })).await.unwrap();
    let activation = app.activation("v1");
    app.files.open("acquire_gate");
    bounded(async {
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
    // The routed caller is independent of the failing implementation.
    assert_eq!(app.client.snapshot().state, Lifecycle::Active);
    assert_eq!(
        entry(&app.loader.snapshot(), "v1")
            .instance
            .as_ref()
            .unwrap()
            .activation,
        Some(activation)
    );
    assert!(bounded(app.rt.shutdown()).await.is_err());
    assert_eq!(app.files.count("v1", "unload"), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_handoff_rechecks_preview_but_pins_prepared_code_through_cancelled_apply() {
    let files = Files::new();
    let v1 = files.artifact("old", "cordis_abi_fixture");
    let v2 = files.artifact("new", "cordis_abi_fixture_v2");
    let rt = Runtime::new();
    let service = Service::<AbiService>::new("example.calculator", "1").with_descriptor(
        cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true)).unwrap(),
    );
    let mut registry = Registry::new();
    // SAFETY: only trusted fixture libraries enter this test's private staging.
    #[allow(unsafe_code)]
    unsafe {
        register_library(&mut registry, "library", service.clone(), files.options()).unwrap();
    }
    let loader = Loader::new(rt.clone(), registry);
    let old = Entry::new(
        "code",
        "library",
        files.config("v1", cordis_wire::json!({})),
    )
    .artifact(v1);
    let new = Entry::new(
        "code",
        "library",
        files.config("v2", cordis_wire::json!({})),
    )
    .artifact(v2.clone());
    let saved = Arc::new(Mutex::new(None));
    let cleanup = Arc::new(Semaphore::new(0));
    let resume = Arc::new(Semaphore::new(0));
    let pause = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let versions = Arc::new(Mutex::new(Vec::new()));
    let consumer = rt
        .install(
            Plugin::new("client", {
                let service = service.clone();
                let saved = saved.clone();
                let cleanup = cleanup.clone();
                let resume = resume.clone();
                let pause = pause.clone();
                let versions = versions.clone();
                move |ctx| {
                    let service = service.clone();
                    let saved = saved.clone();
                    let cleanup = cleanup.clone();
                    let resume = resume.clone();
                    let pause = pause.clone();
                    let versions = versions.clone();
                    async move {
                        *saved.lock().unwrap() = Some(ctx.dependency(&service)?);
                        ctx.defer("uses old code", move |ctx| async move {
                            let version = ordinary_version(&ctx.dependency(&service)?).await?;
                            versions.lock().unwrap().push(version);
                            if pause.swap(false, Ordering::SeqCst) {
                                cleanup.add_permits(1);
                                resume.acquire().await.unwrap().forget();
                            }
                            Ok(())
                        })?;
                        Ok(())
                    }
                }
            })
            .requires(&service),
        )
        .unwrap();
    let metrics = rt
        .install(Plugin::new("unaffected", |_| async { Ok(()) }))
        .unwrap();
    let unaffected = metrics.ready().await.unwrap().activation;
    bounded(loader.apply(Configuration { entries: vec![old] }))
        .await
        .unwrap();
    bounded(consumer.ready()).await.unwrap();
    let old_activation = consumer.snapshot().activation;
    let old_handle = saved.lock().unwrap().take().unwrap();
    let resource = old_handle
        .call(|s, ctx| {
            Box::pin(async move {
                s.acquire_json(&ctx, "counter", json!({"initial":42}))
                    .await
                    .map_err(Into::into)
            })
        })
        .await
        .unwrap();
    let next = Configuration { entries: vec![new] };
    let plan = bounded(loader.plan(next.clone())).await.unwrap();
    assert_eq!(plan.artifacts["code"], v2);
    std::fs::copy(binary("cordis_abi_fixture"), files.member(&v2)).unwrap();
    assert!(bounded(loader.apply(next.clone())).await.is_err());
    assert_eq!(ordinary_version(&old_handle).await.unwrap(), "v1");
    std::fs::copy(binary("cordis_abi_fixture_v2"), files.member(&v2)).unwrap();
    let waiting = tokio::spawn({
        let loader = loader.clone();
        async move { loader.apply(next).await }
    });
    bounded(cleanup.acquire()).await.unwrap().forget();
    assert_eq!(files.count("v1", "deactivate"), 0);
    assert_eq!(files.count("v2", "create"), 0);
    std::fs::copy(binary("cordis_abi_fixture"), files.member(&v2)).unwrap();
    waiting.abort();
    let _ = waiting.await;
    resume.add_permits(1);
    bounded(async {
        while loader.snapshot().phase == Phase::Applying {
            tokio::task::yield_now().await;
        }
    })
    .await;
    bounded(loader.settled()).await.unwrap();
    bounded(consumer.ready()).await.unwrap();
    let current = saved.lock().unwrap().take().unwrap();
    assert_eq!(ordinary_version(&current).await.unwrap(), "v2");
    assert!(ordinary_version(&old_handle).await.is_err());
    assert!(
        resource
            .invoke_json("get", cordis_wire::json!({}))
            .await
            .is_err()
    );
    assert_ne!(consumer.snapshot().activation, old_activation);
    assert_eq!(metrics.snapshot().activation, unaffected);
    assert_eq!(
        entry(&loader.snapshot(), "code")
            .actual
            .as_ref()
            .unwrap()
            .artifact,
        Some(v2)
    );
    bounded(rt.shutdown()).await.unwrap();
    assert_eq!(*versions.lock().unwrap(), vec![json!("v1"), json!("v2")]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_ordinary_handoff_retains_actual_code_and_never_initializes_replacement() {
    let files = Files::new();
    let rt = Runtime::new();
    let mut registry = Registry::new();
    let old = Entry::new(
        "code",
        "library",
        files.config("v1", json!({"fail_deactivate":true})),
    )
    .artifact(files.artifact("old", "cordis_abi_fixture"));
    let new = Entry::new(
        "code",
        "library",
        files.config("v2", cordis_wire::json!({})),
    )
    .artifact(files.artifact("new", "cordis_abi_fixture_v2"));
    // SAFETY: only trusted fixtures/system dependencies and a private staging tree.
    #[allow(unsafe_code)]
    unsafe {
        register_library(
            &mut registry,
            "library",
            Service::new("example.calculator", "1").with_descriptor(
                cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true))
                    .unwrap(),
            ),
            files.options(),
        )
        .unwrap();
    }
    let loader = Loader::new(rt.clone(), registry);
    bounded(loader.apply(Configuration {
        entries: vec![old.clone()],
    }))
    .await
    .unwrap();
    bounded(loader.settled()).await.unwrap();
    let old_snapshot = files.staged()[0].clone();
    for _ in 0..2 {
        assert!(
            bounded(loader.apply(Configuration {
                entries: vec![new.clone()]
            }))
            .await
            .is_err()
        );
    }
    let snapshot = loader.snapshot();
    let code = entry(&snapshot, "code");
    assert_eq!(code.actual.as_ref().unwrap().artifact, old.artifact);
    assert_eq!(code.desired.as_ref().unwrap().artifact, new.artifact);
    assert_eq!(files.count("v1", "deactivate"), 1);
    assert_eq!(files.count("v1", "destroy"), 0);
    assert_eq!(files.count("v2", "create"), 0);
    assert!(bounded(rt.shutdown()).await.is_err());
    drop(loader);
    drop(rt);
    assert!(old_snapshot.exists());
}

fn private_test_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_dependency_packages_roll_with_old_resources_and_reject_tampered_members() {
    let _guard = private_test_lock().lock().await;
    use cordis_loader::package::PackageManifest;
    let files = Files::new();
    let first = files.private_package("v1");
    let next = files.private_package("v2");
    let rt = Runtime::new();
    let service = Service::<Broker<AbiService>>::new("example.calculator", "1").with_descriptor(
        cordis_contract::portable::decode(
            &cordis_example_contracts::resources::calculator_abi::SERVICE
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
    broker.ready().await.unwrap();
    let (consumer, saved) = client(&rt, &service).await;
    let consumer_activation = consumer.snapshot().activation;
    let handle = saved.lock().unwrap().take().unwrap();
    let tokens = Arc::new(Mutex::new(BTreeMap::new()));
    let mut registry = Registry::new();
    #[allow(unsafe_code)]
    // SAFETY: repository-built, generation-named private dependencies and fixed SDK externals.
    unsafe {
        register_broker_library(&mut registry, "package", service, files.options(), {
            let tokens = tokens.clone();
            move |token| {
                tokens
                    .lock()
                    .unwrap()
                    .insert(token.activation().unwrap(), token);
                Ok(())
            }
        })
        .unwrap();
    }
    let loader = Loader::new(rt.clone(), registry);
    let old = Entry::new("old", "package", json!({})).artifact(first.clone());
    let new = Entry::new("new", "package", json!({})).artifact(next.clone());
    loader
        .apply(Configuration {
            entries: vec![old.clone()],
        })
        .await
        .unwrap();
    loader.settled().await.unwrap();
    let old_activation = entry(&loader.snapshot(), "old")
        .instance
        .as_ref()
        .unwrap()
        .activation
        .unwrap();
    tokens.lock().unwrap()[&old_activation].promote().unwrap();
    let resource = handle
        .route(|s, ctx| {
            Box::pin(async move {
                s.acquire_json(&ctx, "counter", json!({"initial":40}))
                    .await
                    .map_err(Into::into)
            })
        })
        .await
        .unwrap();
    let old_paths = files.staged();
    assert_eq!(old_paths.len(), 3);
    // Source-only corruption cannot affect executing code and cannot pass the
    // retained-entry verifier, even though the manifest digest itself is unchanged.
    let manifest: PackageManifest =
        serde_json::from_slice(&std::fs::read(&first.location).unwrap()).unwrap();
    let helper = manifest
        .files
        .iter()
        .find(|f| f.path != manifest.entry)
        .unwrap();
    let source_helper = std::path::Path::new(&first.location)
        .parent()
        .unwrap()
        .join(&helper.path);
    let helper_bytes = std::fs::read(&source_helper).unwrap();
    std::fs::write(&source_helper, b"changed helper").unwrap();
    let revision = loader.snapshot().desired_revision;
    assert!(
        loader
            .apply(Configuration {
                entries: vec![old.clone(), new.clone()]
            })
            .await
            .is_err()
    );
    assert_eq!(loader.snapshot().desired_revision, revision);
    assert_eq!(
        resource
            .invoke_json("code-version", json!({}))
            .await
            .unwrap(),
        "v1"
    );
    std::fs::write(&source_helper, helper_bytes).unwrap();
    loader
        .apply(Configuration {
            entries: vec![old.clone(), new.clone()],
        })
        .await
        .unwrap();
    loader.settled().await.unwrap();
    let new_activation = entry(&loader.snapshot(), "new")
        .instance
        .as_ref()
        .unwrap()
        .activation
        .unwrap();
    tokens.lock().unwrap()[&new_activation].promote().unwrap();
    assert_eq!(
        handle
            .route(|s, ctx| Box::pin(async move {
                s.invoke_json(&ctx, "code-version", json!({}))
                    .await
                    .map_err(Into::into)
            }))
            .await
            .unwrap(),
        "v2"
    );
    assert_eq!(
        resource
            .invoke_json("code-version", json!({}))
            .await
            .unwrap(),
        "v1"
    );
    let retiring = tokio::spawn({
        let loader = loader.clone();
        async move { loader.apply(Configuration { entries: vec![new] }).await }
    });
    bounded(async {
        while entry(&loader.snapshot(), "old")
            .instance
            .as_ref()
            .unwrap()
            .state
            != Lifecycle::Unloading
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!retiring.is_finished());
    assert!(old_paths.iter().all(|p| p.exists()));
    resource.dispose().await.unwrap();
    bounded(retiring).await.unwrap().unwrap();
    assert!(old_paths.iter().all(|p| p.exists()));
    drop(resource);
    drop(tokens.lock().unwrap().remove(&old_activation));
    bounded(async {
        while old_paths.iter().any(|p| p.exists()) {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(consumer.snapshot().activation, consumer_activation);
    rt.shutdown().await.unwrap();
    tokens.lock().unwrap().clear();
    drop((loader, handle, consumer, broker, rt));
    bounded(async {
        while !files.staged().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

#[tokio::test]
async fn shared_private_dependency_retains_its_original_snapshot_until_last_module() {
    let _guard = private_test_lock().lock().await;
    let first = Files::new();
    let second = Files::new();
    let a = load(&first.private_package("v1"), &first.options()).unwrap();
    let original = first.staged();
    let b = load(&second.private_package("v1"), &second.options()).unwrap();
    drop(a);
    assert!(original.iter().all(|p| p.exists()));
    drop(b);
    assert!(first.staged().is_empty());
    assert!(second.staged().is_empty());
}

#[tokio::test]
async fn private_link_identity_conflicts_and_missing_imports_are_rejected_before_loading() {
    let _guard = private_test_lock().lock().await;
    use cordis_loader::package::{PackageManifest, digest};
    let first = Files::new();
    let second = Files::new();
    let old = load(&first.private_package("v1"), &first.options()).unwrap();
    let reference = second.private_package("v1");
    let root = std::path::Path::new(&reference.location).parent().unwrap();
    let mut manifest: PackageManifest =
        serde_json::from_slice(&std::fs::read(&reference.location).unwrap()).unwrap();
    let helper = manifest
        .files
        .iter_mut()
        .find(|f| f.path != manifest.entry)
        .unwrap();
    let path = root.join(&helper.path);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.push(0);
    std::fs::write(path, &bytes).unwrap();
    helper.sha256 = digest(&bytes);
    let changed = manifest.write(root).unwrap();
    assert!(second.options().package.verify(&changed).is_ok());
    let error = match load(&changed, &second.options()) {
        Ok(_) => panic!("conflicting dependency accepted"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("native link identity"),
        "{error}"
    );
    assert!(second.staged().is_empty());
    manifest.files.retain(|f| f.path == manifest.entry);
    let missing = manifest.write(root).unwrap();
    assert!(second.options().package.verify(&missing).is_err());
    assert!(second.staged().is_empty());
    drop(old);
    assert!(first.staged().is_empty());
}
