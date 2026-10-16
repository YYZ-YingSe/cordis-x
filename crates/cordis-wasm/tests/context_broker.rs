#![cfg(feature = "loader")]
use cordis_portable_example::context::ContextLogClient;
use cordis_portable_example::context_fixture_interface;
#[path = "support/context.rs"]
mod guest;
use cordis_loader::{ArtifactRef, Configuration, Entry, Loader, Registry};
use cordis_sdk::*;
use cordis_wasm::{Capabilities, ContextService, Dependency, Limits, Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
#[track_caller]
fn bounded<T>(
    future: impl std::future::Future<Output = T>,
) -> impl std::future::Future<Output = T> {
    let location = std::panic::Location::caller();
    async move {
        tokio::time::timeout(Duration::from_secs(10), future)
            .await
            .unwrap_or_else(|_| panic!("Context Broker did not settle at {location}"))
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
    compiler: cordis_wasm::Compiler,
    rt: Runtime,
    loader: Loader,
    client: PluginHandle,
    handle: ServiceHandle<Broker<ContextService>>,
    candidates: Arc<Mutex<BTreeMap<Activation, BrokerRegistration<ContextService>>>>,
    log: Arc<Mutex<Vec<Value>>>,
    factories: Arc<AtomicUsize>,
    directory: std::path::PathBuf,
    old: Entry,
    new: Entry,
}
impl Drop for App {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
impl App {
    async fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let rt = Runtime::new();
        let log = Arc::new(Mutex::new(Vec::new()));
        let logger = Service::<Mutex<Vec<Value>>>::new("example.context-log", "1");
        let provider = rt
            .install(
                Plugin::new("logger", {
                    let logger = logger.clone();
                    let log = log.clone();
                    move |ctx| {
                        let logger = logger.clone();
                        let log = log.clone();
                        async move { ctx.provide(&logger, log) }
                    }
                })
                .provides(&logger),
            )
            .unwrap();
        provider.ready().await.unwrap();
        let broker = Service::<Broker<ContextService>>::new("example.context", "1")
            .with_descriptor(
                cordis_contract::Descriptor::new(context_fixture_interface()).unwrap(),
            );
        let provider = rt
            .install(
                Plugin::new("broker", {
                    let broker = broker.clone();
                    move |ctx| {
                        let broker = broker.clone();
                        async move { ctx.provide_broker(&broker) }
                    }
                })
                .provides(&broker),
            )
            .unwrap();
        provider.ready().await.unwrap();
        let saved = Arc::new(Mutex::new(None));
        let client = rt
            .install(
                Plugin::new("client", {
                    let saved = saved.clone();
                    let broker = broker.clone();
                    move |ctx| {
                        let saved = saved.clone();
                        let broker = broker.clone();
                        async move {
                            *saved.lock().unwrap() = Some(ctx.dependency(&broker)?);
                            Ok(())
                        }
                    }
                })
                .requires(&broker),
            )
            .unwrap();
        client.ready().await.unwrap();
        let handle = saved.lock().unwrap().take().unwrap();
        let factories = Arc::new(AtomicUsize::new(0));
        let mut caps = Capabilities {
            wasi: Some(cordis_wasm::Wasi::default()),
            dependencies: vec![Dependency::new_json(
                &logger,
                cordis_contract::Descriptor::new(ContextLogClient::interface()).unwrap(),
                |log, _, _, args| {
                    Box::pin(async move {
                        log.lock().unwrap().push(args.clone());
                        Ok(args)
                    })
                },
            )],
            ..Capabilities::default()
        };
        caps.events
            .insert("messages".into(), Event::new("messages", "1"));
        caps.middleware
            .insert("pipeline".into(), Middleware::new("pipeline", "1"));
        caps.factories.insert("worker".into(), {
            let factories = factories.clone();
            Arc::new(move |config| {
                cordis_portable_example::context::ContextControls::worker()
                    .build(cordis_sdk_core::Message::from_bytes(<cordis_portable_example::context::data::cordis::data::Unit as cordis_sdk_core::typed::Codec>::contract().clone(),config).unwrap(), |_| {
                        factories.fetch_add(1, Ordering::SeqCst);
                        Ok(Plugin::new("worker", |_| async { Ok(()) }))
                    })
                    .map_err(|e| cordis_wasm::Error::from(Error::from(e)))
            })
        });
        let candidates = Arc::new(Mutex::new(BTreeMap::new()));
        let compiler = cordis_wasm::Compiler::default();
        let mut registry = Registry::new();
        cordis_wasm::loader::register_context_broker_component(
            &mut registry,
            "candidate",
            broker,
            compiler.clone(),
            Limits::default(),
            caps,
            {
                let candidates = candidates.clone();
                move |c| {
                    candidates
                        .lock()
                        .unwrap()
                        .insert(c.activation().ok_or(Error::Closed)?, c);
                    Ok(())
                }
            },
        )
        .unwrap();
        let directory = std::env::temp_dir().join(format!(
            "cordis-context-broker-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        let old_path = directory.join("v1.wasm");
        let new_path = directory.join("v2.wasm");
        std::fs::write(&old_path, guest::bytes()).unwrap();
        std::fs::write(&new_path, guest::v2_bytes()).unwrap();
        let old = Entry::new("v1", "candidate", json!({}))
            .artifact(ArtifactRef::from_file(old_path, Limits::default().artifact_bytes).unwrap());
        let new = Entry::new("v2", "candidate", json!({}))
            .artifact(ArtifactRef::from_file(new_path, Limits::default().artifact_bytes).unwrap());
        let loader = Loader::new(rt.clone(), registry);
        prepare(loader.apply(Configuration {
            entries: vec![old.clone()],
        }))
        .await
        .unwrap();
        bounded(loader.settled()).await.unwrap();
        let app = Self {
            compiler,
            rt,
            loader,
            client,
            handle,
            candidates,
            log,
            factories,
            directory,
            old,
            new,
        };
        app.promote("v1");
        app
    }
    fn promote(&self, id: &str) {
        let activation = self
            .loader
            .snapshot()
            .entries
            .iter()
            .find(|e| e.id == id)
            .unwrap()
            .instance
            .as_ref()
            .unwrap()
            .activation
            .unwrap();
        self.candidates.lock().unwrap()[&activation]
            .promote()
            .unwrap();
    }
    async fn call(&self, method: &str, args: Value) -> Result<Value> {
        let method = method.to_owned();
        bounded(self.handle.route(move |s, ctx| {
            Box::pin(async move { s.invoke_json(&ctx, &method, args).await.map_err(Into::into) })
        }))
        .await
    }
    async fn owner(&self) -> (PluginHandle, ServiceHandle<Broker<ContextService>>) {
        let saved = Arc::new(Mutex::new(None));
        let service = Service::<Broker<ContextService>>::new("example.context", "1")
            .with_descriptor(
                cordis_contract::Descriptor::new(context_fixture_interface()).unwrap(),
            );
        let client = self
            .rt
            .install(
                Plugin::new("resource owner", {
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
        (client, handle)
    }
    async fn retire(
        &self,
    ) -> tokio::task::JoinHandle<cordis_loader::Result<cordis_loader::ApplyReport>> {
        let before = self.compiler.stats();
        let loader = self.loader.clone();
        let new = self.new.clone();
        let waiter =
            tokio::spawn(async move { loader.apply(Configuration { entries: vec![new] }).await });
        // Retained code is rehashed, with no compilation or metadata preflight.
        bounded(async {
            loop {
                if self
                    .loader
                    .snapshot()
                    .entries
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
        let after = self.compiler.stats();
        assert_eq!(after.compilations, before.compilations);
        assert_eq!(after.preflights, before.preflights);
        waiter
    }
    async fn upgrade(&self) {
        prepare(self.loader.apply(Configuration {
            entries: vec![self.old.clone(), self.new.clone()],
        }))
        .await
        .unwrap();
        bounded(self.loader.settled()).await.unwrap();
        self.promote("v2");
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_context_versions_keep_consumers_and_old_callback_responsibility() {
    let app = App::new().await;
    let before = app.client.snapshot().activation;
    assert_eq!(app.call("code-version", json!({})).await.unwrap(), "v1");
    let (owner, old_handle) = app.owner().await;
    for (method, args) in [
        ("subscribe", json!("old")),
        ("task", json!({})),
        ("events", json!({})),
    ] {
        old_handle
            .route(move |s, ctx| {
                Box::pin(async move { s.invoke_json(&ctx, method, args).await.map_err(Into::into) })
            })
            .await
            .unwrap();
    }
    app.upgrade().await;
    assert_eq!(app.call("code-version", json!({})).await.unwrap(), "v2");
    assert!(!app.log.lock().unwrap().contains(&json!({"released":"old"})));
    let waiter = app.retire().await;
    assert!(!waiter.is_finished());
    assert_eq!(app.call("code-version", json!({})).await.unwrap(), "v2");
    // Cancellation of the caller's wait must not cancel Loader retirement.
    waiter.abort();
    let _ = waiter.await;
    bounded(owner.dispose()).await.unwrap();
    bounded(app.loader.settled()).await.unwrap();
    assert_eq!(app.client.snapshot().activation, before);
    let log = app.log.lock().unwrap().clone();
    assert!(log.contains(&json!({"released":"old"})));
    assert!(log.contains(&json!("task-stopped")));
    let deactivated = log.iter().position(|v| v == "deactivate").unwrap();
    assert!(
        log.iter()
            .position(|v| v == &json!({"released":"old"}))
            .unwrap()
            < deactivated
    );
    assert!(app.call("install", json!({})).await.is_err());
    assert_eq!(
        app.factories.load(Ordering::SeqCst),
        0,
        "Broker must reject before running a factory"
    );
    assert!(app.call("scope", json!({})).await.is_err());
    bounded(app.rt.shutdown()).await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_old_context_inverse_preserves_new_route_and_blocks_retirement() {
    let app = App::new().await;
    let (owner, handle) = app.owner().await;
    handle
        .route(|s, ctx| {
            Box::pin(async move {
                s.invoke_json(&ctx, "failed-defer", json!({}))
                    .await
                    .map_err(Into::into)
            })
        })
        .await
        .unwrap();
    app.upgrade().await;
    let waiter = app.retire().await;
    assert!(!waiter.is_finished());
    assert!(bounded(owner.dispose()).await.is_err());
    assert!(bounded(waiter).await.unwrap().is_err());
    assert!(bounded(app.loader.settled()).await.is_err());
    assert_eq!(app.call("code-version", json!({})).await.unwrap(), "v2");
    assert!(!app.log.lock().unwrap().contains(&json!("deactivate")));
    assert!(bounded(app.rt.shutdown()).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn context_broker_bad_preflight_and_rejected_activation_preserve_live_route() {
    let app = App::new().await;
    let activation = app.client.snapshot().activation;
    let path = app.directory.join("invalid.wasm");
    std::fs::write(&path, b"invalid component").unwrap();
    let bad = Entry::new("bad", "candidate", json!({}))
        .artifact(ArtifactRef::from_file(path, 1024).unwrap());
    assert!(
        prepare(app.loader.apply(Configuration {
            entries: vec![app.old.clone(), bad]
        }))
        .await
        .is_err()
    );
    assert_eq!(app.call("code-version", json!({})).await.unwrap(), "v1");
    let rejected = Entry::new("rejected", "candidate", json!({"provide":42}))
        .artifact(app.new.artifact.clone().unwrap());
    prepare(app.loader.apply(Configuration {
        entries: vec![app.old.clone(), rejected],
    }))
    .await
    .unwrap();
    assert!(bounded(app.loader.settled()).await.is_err());
    assert_eq!(app.call("code-version", json!({})).await.unwrap(), "v1");
    assert_eq!(app.client.snapshot().activation, activation);
    bounded(app.rt.shutdown()).await.unwrap();
}
