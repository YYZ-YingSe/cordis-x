use super::*;
use cordis_remote::{Dependency, Value};
use std::sync::atomic::AtomicBool;

fn subscriptions() -> Service<RemoteService> {
    Service::new("example.audited-subscriptions", "1").with_descriptor(
        cordis_remote::Descriptor::new(
            cordis_portable_example::lifecycle::AuditedSubscriptionsClient::interface(),
        )
        .unwrap(),
    )
}
pub(super) struct Log {
    pub(super) open: AtomicBool,
    pub(super) events: Mutex<Vec<(String, u64)>>,
    pub(super) block: Mutex<Option<String>>,
    pub(super) fail: Mutex<Option<String>>,
    pub(super) entered: Semaphore,
    pub(super) release: Semaphore,
}
impl Log {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            open: AtomicBool::new(true),
            events: Mutex::default(),
            block: Mutex::default(),
            fail: Mutex::default(),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        })
    }
    pub(super) fn names(&self) -> Vec<String> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .map(|(name, _)| name.clone())
            .collect()
    }
}
fn log_plugin_for(log: Arc<Log>, service: Service<Log>) -> Plugin {
    let provided = service.clone();
    Plugin::new("logger", move |ctx| {
        let service = service.clone();
        let log = log.clone();
        async move {
            let cleanup = log.clone();
            ctx.defer("native logger", move |_| async move {
                cleanup.open.store(false, Ordering::SeqCst);
                cleanup
                    .events
                    .lock()
                    .unwrap()
                    .push(("logger.closed".into(), 0));
                Ok(())
            })?;
            ctx.provide(&service, log)
        }
    })
    .provides(&provided)
}
pub(super) fn lifecycle_logger() -> Service<Log> {
    Service::new("example.lifecycle-log", "1")
}
pub(super) fn lifecycle_log_plugin(log: Arc<Log>) -> Plugin {
    log_plugin_for(log, Service::new("example.lifecycle-log", "1"))
}
pub(super) fn lifecycle_log_dependency() -> Dependency {
    Dependency::new_json(
        &Service::<Log>::new("example.lifecycle-log", "1"),
        cordis_remote::Descriptor::new(
            cordis_portable_example::lifecycle::LifecycleLogClient::interface(),
        )
        .unwrap(),
        |log, ctx, method, args| {
            Box::pin(async move {
                record(log, ctx, method, args).await?;
                Ok(json!({}))
            })
        },
    )
}
async fn record(log: &Log, ctx: Context, method: String, args: Value) -> cordis_remote::Result<()> {
    assert!(log.open.load(Ordering::SeqCst));
    assert_eq!(method, "write");
    let name = args.as_str().unwrap().to_owned();
    log.events
        .lock()
        .unwrap()
        .push((name.clone(), ctx.owner().fiber.0));
    let block = log.block.lock().unwrap().as_ref() == Some(&name);
    if block {
        log.entered.add_permits(1);
        log.release.acquire().await.unwrap().forget();
    }
    assert!(log.open.load(Ordering::SeqCst));
    if log.fail.lock().unwrap().as_ref() == Some(&name) {
        return Err(cordis_remote::Error::Remote {
            code: "cleanup_failed".into(),
            message: name,
        });
    }
    Ok(())
}
fn quiet_subscriptions() -> Service<RemoteService> {
    Service::new("example.subscriptions", "1").with_descriptor(
        cordis_remote::Descriptor::new(
            cordis_portable_example::lifecycle::SubscriptionsClient::interface(),
        )
        .unwrap(),
    )
}
fn spec(backend: Backend, endpoint: Endpoint, logging: bool) -> ProcessSpec {
    let mut spec = backend.spec("subscriptions");

    spec.endpoint = endpoint;
    if matches!(endpoint, Endpoint::Ipc) {
        spec.args.push("--stdio-only".into());
    }
    if logging {
        spec = spec.requires(lifecycle_log_dependency());
        spec.args.push("--logger".into());
    }
    spec
}
async fn consumer(rt: &Runtime) -> (PluginHandle, ServiceHandle<RemoteService>) {
    consumer_for(rt, subscriptions()).await
}
async fn probe_consumer(rt: &Runtime) -> (PluginHandle, ServiceHandle<RemoteService>) {
    consumer_for(rt, probe_service()).await
}
async fn consumer_for(
    rt: &Runtime,
    service: Service<RemoteService>,
) -> (PluginHandle, ServiceHandle<RemoteService>) {
    let saved = Arc::new(Mutex::new(None));
    let client = rt
        .install(
            Plugin::new("caller", {
                let saved = saved.clone();
                let service = service.clone();
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
    bounded(client.ready()).await.unwrap();
    let handle = saved.lock().unwrap().take().unwrap();
    (client, handle)
}
async fn invoke_json(
    handle: &ServiceHandle<RemoteService>,
    endpoint: Endpoint,
    method: &str,
    args: Value,
) -> Result<Value> {
    bounded(call(handle, endpoint, method, args)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_effects_are_caller_owned_independent_and_lifo_across_sdks_endpoints() {
    for &backend in Backend::ALL {
        for endpoint in [Endpoint::Ipc, Endpoint::Http] {
            let rt = Runtime::new();
            let log = Log::new();
            // Definition-side dependency is isolated; the caller declares no logger.
            let view = View::new().isolate(&lifecycle_logger());
            rt.install_in(view.clone(), lifecycle_log_plugin(log.clone()))
                .unwrap();
            let provider = rt
                .install_in(
                    view,
                    spec(backend, endpoint, true).plugin("subscriptions", &subscriptions()),
                )
                .unwrap();
            bounded(provider.ready()).await.unwrap();
            let (client, handle) = consumer(&rt).await;
            let (_, observer) = consumer(&rt).await;
            for name in ["first", "second", "manual"] {
                invoke_json(&handle, endpoint, "subscribe", json!(name))
                    .await
                    .unwrap();
            }
            assert_eq!(client.effects().len(), 3);
            assert_eq!(
                invoke_json(
                    &observer,
                    endpoint,
                    "publish",
                    json!("still subscribed after return")
                )
                .await
                .unwrap(),
                json!("3")
            );
            // Disposing through a later invocation keeps the original owner's cleanup Context.
            invoke_json(&observer, endpoint, "unsubscribe", json!("manual"))
                .await
                .unwrap();
            invoke_json(&observer, endpoint, "unsubscribe", json!("manual"))
                .await
                .unwrap();
            assert_eq!(
                invoke_json(&observer, endpoint, "count", json!({}))
                    .await
                    .unwrap(),
                json!("2")
            );
            let owner = client.snapshot().id.0;
            bounded(client.dispose()).await.unwrap();
            assert_eq!(
                invoke_json(&observer, endpoint, "publish", json!("no listeners"))
                    .await
                    .unwrap(),
                json!("0")
            );
            assert_eq!(
                log.events
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(name, _)| name.starts_with("released:"))
                    .cloned()
                    .collect::<Vec<_>>(),
                vec![
                    ("released:manual".into(), owner),
                    ("released:second".into(), owner),
                    ("released:first".into(), owner)
                ]
            );
            bounded(rt.shutdown()).await.unwrap();
            assert!(
                log.names()
                    .ends_with(&["subscriptions.closed".into(), "logger.closed".into()])
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_effects_work_without_declared_dependencies() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let provider = rt
            .install(
                spec(backend, Endpoint::Ipc, false).plugin("subscriptions", &quiet_subscriptions()),
            )
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (client, handle) = consumer_for(&rt, quiet_subscriptions()).await;
        let (_, observer) = consumer_for(&rt, quiet_subscriptions()).await;
        invoke_json(&handle, Endpoint::Ipc, "subscribe", json!("plain"))
            .await
            .unwrap();
        assert_eq!(client.effects().len(), 1);
        bounded(client.dispose()).await.unwrap();
        assert_eq!(
            invoke_json(&observer, Endpoint::Ipc, "count", json!({}))
                .await
                .unwrap(),
            json!("0")
        );
        bounded(rt.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_remote_waits_preserve_late_registration_and_running_cleanup() {
    for &backend in Backend::ALL {
        for during_cleanup in [false, true] {
            let rt = Runtime::new();
            let log = Log::new();
            *log.block.lock().unwrap() = Some(
                if during_cleanup {
                    "released:late"
                } else {
                    "opening:late"
                }
                .into(),
            );
            rt.install(lifecycle_log_plugin(log.clone())).unwrap();
            let provider = rt
                .install(
                    spec(backend, Endpoint::Http, true).plugin("subscriptions", &subscriptions()),
                )
                .unwrap();
            bounded(provider.ready()).await.unwrap();
            let (_, handle) = consumer(&rt).await;
            if during_cleanup {
                invoke_json(&handle, Endpoint::Http, "subscribe", json!("late"))
                    .await
                    .unwrap();
            }
            let waiting = tokio::spawn(async move {
                call(
                    &handle,
                    Endpoint::Http,
                    if during_cleanup {
                        "unsubscribe"
                    } else {
                        "subscribe"
                    },
                    json!("late"),
                )
                .await
            });
            bounded(log.entered.acquire()).await.unwrap().forget();
            waiting.abort();
            let _ = waiting.await;
            let mut stopping = tokio::spawn(async move { provider.dispose().await });
            assert!(
                tokio::time::timeout(Duration::from_millis(40), &mut stopping)
                    .await
                    .is_err()
            );
            assert!(!log.names().contains(&"subscriptions.closed".into()));
            log.release.add_permits(1);
            bounded(stopping).await.unwrap().unwrap();
            bounded(rt.shutdown()).await.unwrap();
            assert_eq!(
                log.names(),
                [
                    "opening:late",
                    "released:late",
                    "subscriptions.closed",
                    "logger.closed"
                ]
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_remote_effect_cleanup_is_cached_and_blocks_provider_recovery() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        *log.fail.lock().unwrap() = Some("released:broken".into());
        rt.install(lifecycle_log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc, true).plugin("subscriptions", &subscriptions()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (client, handle) = consumer(&rt).await;
        invoke_json(&handle, Endpoint::Ipc, "subscribe", json!("broken"))
            .await
            .unwrap();
        for _ in 0..2 {
            assert!(
                invoke_json(&handle, Endpoint::Ipc, "unsubscribe", json!("broken"))
                    .await
                    .is_err()
            );
        }
        assert!(matches!(
            client.effects()[0].status,
            EffectStatus::Failed(_)
        ));
        assert_eq!(
            log.names()
                .iter()
                .filter(|n| *n == "released:broken")
                .count(),
            1
        );
        assert!(matches!(
            bounded(provider.dispose()).await,
            Err(Error::CleanupBlocked(_, _))
        ));
        assert!(!log.names().contains(&"subscriptions.closed".into()));
        assert!(bounded(rt.shutdown()).await.is_err());
        assert!(log.open.load(Ordering::SeqCst));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_remote_disposers_do_not_starve_cleanup_workers() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        *log.block.lock().unwrap() = Some("released:shared".into());
        rt.install(lifecycle_log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc, true).plugin("subscriptions", &subscriptions()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (_, handle) = consumer(&rt).await;
        invoke_json(&handle, Endpoint::Ipc, "subscribe", json!("shared"))
            .await
            .unwrap();
        let mut calls = Vec::new();
        for _ in 0..8 {
            let handle = handle.clone();
            calls.push(tokio::spawn(async move {
                call(&handle, Endpoint::Ipc, "unsubscribe", json!("shared")).await
            }));
        }
        bounded(log.entered.acquire()).await.unwrap().forget();
        // All business workers may now be waiting on the same cleanup result.
        log.release.add_permits(1);
        for task in calls {
            bounded(task).await.unwrap().unwrap();
        }
        assert_eq!(
            log.names()
                .iter()
                .filter(|n| *n == "released:shared")
                .count(),
            1
        );
        bounded(rt.shutdown()).await.unwrap();
    }
}

// Deliberately malformed wire peers have their own test descriptor.
fn probe_service() -> Service<RemoteService> {
    Service::new("example.subscriptions", "1").with_descriptor(
        cordis_remote::Descriptor::new(cordis_remote::Interface::value_dispatch(true)).unwrap(),
    )
}
fn peer(mode: &str) -> (ProcessSpec, PathBuf) {
    let trace = std::env::temp_dir().join(format!(
        "cordis-effects-{}-{}.log",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let mut spec = python_spec();
    spec.args.extend([
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/effect_peer.py")
            .display()
            .to_string(),
        mode.into(),
        trace.display().to_string(),
    ]);
    if mode == "cleanup_hang" {
        spec.timeout = Duration::from_millis(200);
    }
    (spec, trace)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn effect_protocol_rejects_expired_contexts_duplicates_and_forged_disposal_grants() {
    let rt = Runtime::new();
    let (spec, trace) = peer("normal");
    let provider = rt.install(spec.plugin("peer", &probe_service())).unwrap();
    bounded(provider.ready()).await.unwrap();
    let (client, handle) = probe_consumer(&rt).await;
    invoke_json(&handle, Endpoint::Ipc, "cache", Value::Null)
        .await
        .unwrap();
    assert_eq!(
        invoke_json(&handle, Endpoint::Ipc, "stale", Value::Null)
            .await
            .unwrap()["error"]["code"],
        "closed"
    );
    assert!(client.effects().is_empty());
    invoke_json(&handle, Endpoint::Ipc, "subscribe", Value::Null)
        .await
        .unwrap();
    for (method, code) in [
        ("duplicate", "invalid_argument"),
        ("forged", "closed"),
        ("unknown", "closed"),
    ] {
        assert_eq!(
            invoke_json(&handle, Endpoint::Ipc, method, Value::Null)
                .await
                .unwrap()["error"]["code"],
            code
        );
    }
    assert_eq!(client.effects().len(), 1);
    for _ in 0..2 {
        assert_eq!(
            invoke_json(&handle, Endpoint::Ipc, "dispose", Value::Null)
                .await
                .unwrap()["kind"],
            "success"
        );
    }
    bounded(client.dispose()).await.unwrap();
    // The independent handle remains idempotent even after its native scope is gone.
    let (_, observer) = probe_consumer(&rt).await;
    assert_eq!(
        invoke_json(&observer, Endpoint::Ipc, "dispose", Value::Null)
            .await
            .unwrap()["kind"],
        "success"
    );
    bounded(rt.shutdown()).await.unwrap();
    assert_eq!(
        std::fs::read_to_string(&trace).unwrap(),
        "released\nregistration_rejected\ndeactivated\n"
    );
    std::fs::remove_file(trace).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registered_remote_effects_clean_up_after_failed_activation() {
    for backend in std::iter::once(None).chain(Backend::ALL.iter().copied().map(Some)) {
        let rt = Runtime::new();
        let (mut spec, trace) = peer("activation_fail");
        if let Some(backend) = backend {
            spec = backend.effect_fixture();
            spec.args.push(trace.display().to_string());
        }
        let provider = rt.install(spec.plugin("peer", &probe_service())).unwrap();
        assert!(bounded(provider.ready()).await.is_err());
        assert!(bounded(provider.settled()).await.is_err());
        assert_eq!(
            std::fs::read_to_string(&trace).unwrap(),
            "released\ndeactivated\n"
        );
        let _ = bounded(rt.shutdown()).await;
        std::fs::remove_file(trace).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn host_loss_and_cleanup_timeouts_retain_failed_remote_effects() {
    for mode in [
        "registration_crash",
        "cleanup_crash",
        "cleanup_hang",
        "dispose_crash",
        "normal",
    ] {
        let rt = Runtime::new();
        let (spec, trace) = peer(mode);
        let provider = rt.install(spec.plugin("peer", &probe_service())).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (client, handle) = probe_consumer(&rt).await;
        let result = invoke_json(&handle, Endpoint::Ipc, "subscribe", Value::Null).await;
        if mode == "registration_crash" {
            assert!(result.is_err());
        } else {
            result.unwrap();
            if mode == "normal" {
                assert!(
                    invoke_json(&handle, Endpoint::Ipc, "crash", serde_json::json!({}))
                        .await
                        .is_err()
                );
            } else if mode == "dispose_crash" {
                assert!(
                    invoke_json(&handle, Endpoint::Ipc, "dispose", Value::Null)
                        .await
                        .is_err()
                );
            }
        }
        assert!(bounded(client.dispose()).await.is_err());
        assert_eq!(
            client.effects().len(),
            1,
            "{mode}: lost cleanup responsibility"
        );
        assert!(
            matches!(client.effects()[0].status, EffectStatus::Failed(_)),
            "{mode}"
        );
        assert!(bounded(provider.dispose()).await.is_err());
        assert!(bounded(rt.shutdown()).await.is_err());
        let text = std::fs::read_to_string(&trace).unwrap_or_default();
        assert!(!text.contains("deactivated"));
        assert!(text.matches("released").count() <= 1);
        let _ = std::fs::remove_file(trace);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_subscriptions_validate_before_effects_and_reserve_before_logging() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        *log.block.lock().unwrap() = Some("opening:one".into());
        rt.install(lifecycle_log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc, true).plugin("subscriptions", &subscriptions()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle) = consumer(&rt).await;
        for value in [json!(42), json!({"name":"one"}), json!(["one"])] {
            assert!(
                invoke_json(&handle, Endpoint::Ipc, "subscribe", value)
                    .await
                    .is_err()
            );
        }
        assert!(owner.effects().is_empty());
        assert!(log.names().is_empty());
        let pending = tokio::spawn({
            let handle = handle.clone();
            async move { invoke_json(&handle, Endpoint::Ipc, "subscribe", json!("one")).await }
        });
        bounded(log.entered.acquire()).await.unwrap().forget();
        let error = invoke_json(&handle, Endpoint::Ipc, "subscribe", json!("one"))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("duplicate_subscription"),
            "{error}"
        );
        assert_eq!(log.names(), ["opening:one"]);
        assert_eq!(
            invoke_json(&handle, Endpoint::Ipc, "count", json!({}))
                .await
                .unwrap(),
            json!("0")
        );
        log.release.add_permits(1);
        bounded(pending).await.unwrap().unwrap();
        assert_eq!(owner.effects().len(), 1);
        assert_eq!(
            invoke_json(&handle, Endpoint::Ipc, "count", json!({}))
                .await
                .unwrap(),
            json!("1")
        );
        bounded(rt.shutdown()).await.unwrap();
        assert_eq!(
            log.names(),
            [
                "opening:one",
                "released:one",
                "subscriptions.closed",
                "logger.closed"
            ]
        );
    }
}
