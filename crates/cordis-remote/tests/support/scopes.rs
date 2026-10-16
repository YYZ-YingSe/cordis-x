use super::effects::{
    Log, lifecycle_log_dependency as log_dependency, lifecycle_log_plugin as log_plugin,
};
use super::*;
use cordis_remote::Value;

fn connections() -> Service<RemoteService> {
    Service::new("example.connections", "1").with_descriptor(
        cordis_remote::Descriptor::new(
            cordis_portable_example::lifecycle::ConnectionsClient::interface(),
        )
        .unwrap(),
    )
}
fn spec(backend: Backend, endpoint: Endpoint) -> ProcessSpec {
    let mut spec = backend.spec("scopes");
    spec.endpoint = endpoint;

    if matches!(endpoint, Endpoint::Ipc) {
        spec.args.push("--stdio-only".into());
    }
    spec.requires(log_dependency())
}
async fn consumer(rt: &Runtime) -> (PluginHandle, ServiceHandle<RemoteService>) {
    consumer_for(rt, connections()).await
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
            Plugin::new("scope owner", {
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
    name: &str,
) -> Result<Value> {
    bounded(call(handle, endpoint, method, json!(name))).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scopes_group_nested_children_and_sequences_across_sdks_endpoints() {
    for &backend in Backend::ALL {
        for endpoint in [Endpoint::Ipc, Endpoint::Http] {
            let rt = Runtime::new();
            let log = Log::new();
            rt.install(log_plugin(log.clone())).unwrap();
            let provider = rt
                .install(spec(backend, endpoint).plugin("connections", &connections()))
                .unwrap();
            bounded(provider.ready()).await.unwrap();
            let (owner, handle) = consumer(&rt).await;
            let (_, observer) = consumer(&rt).await;
            invoke_json(&handle, endpoint, "open", "run").await.unwrap();
            assert_eq!(
                invoke_json(&observer, endpoint, "count", "run")
                    .await
                    .unwrap(),
                json!("4")
            );
            assert_eq!(
                invoke_json(&observer, endpoint, "run", "run")
                    .await
                    .unwrap(),
                "ok"
            );
            let before = log.events.lock().unwrap().clone();
            let child = before[0].1;
            assert_ne!(child, owner.id().0);
            assert!(before.iter().all(|(_, id)| *id == child));
            for _ in 0..2 {
                invoke_json(&observer, endpoint, "release", "run")
                    .await
                    .unwrap();
            }
            assert_eq!(
                invoke_json(&observer, endpoint, "count", "run")
                    .await
                    .unwrap(),
                json!("0")
            );
            assert_eq!(owner.snapshot().state, Lifecycle::Active);
            assert_eq!(
                invoke_json(&observer, endpoint, "status", "run")
                    .await
                    .unwrap()["state"],
                "inactive"
            );
            let error = invoke_json(&observer, endpoint, "run", "run")
                .await
                .unwrap_err();
            assert!(
                matches!(error, Error::Portable(cordis_sdk_core::Failure::Refused(ref info)) if info.code == "retired"),
                "{error}"
            );
            assert_eq!(
                log.names(),
                [
                    "run/buffer.open",
                    "run/subscribe:1",
                    "run/subscribe:2",
                    "run/run",
                    "run/watcher.close",
                    "run/unsubscribe:2",
                    "run/unsubscribe:1",
                    "run/buffer.close"
                ]
            );
            bounded(rt.shutdown()).await.unwrap();
            assert!(
                log.names()
                    .ends_with(&["connections.closed".into(), "logger.closed".into()])
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parent_stop_and_cancelled_run_wait_keep_late_scope_effects_owned() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        *log.block.lock().unwrap() = Some("run/late.enter".into());
        rt.install(log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc).plugin("connections", &connections()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle) = consumer(&rt).await;
        let (_, observer) = consumer(&rt).await;
        invoke_json(&handle, Endpoint::Ipc, "open", "run")
            .await
            .unwrap();
        let running =
            tokio::spawn(
                async move { invoke_json(&handle, Endpoint::Ipc, "contribute", "run").await },
            );
        bounded(log.entered.acquire()).await.unwrap().forget();
        running.abort();
        let _ = running.await;
        let stopping = tokio::spawn({
            let owner = owner.clone();
            async move { owner.dispose().await }
        });
        bounded(async {
            while owner.snapshot().state != Lifecycle::Unloading {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(!log.names().contains(&"run/buffer.close".into()));
        log.release.add_permits(1);
        bounded(stopping).await.unwrap().unwrap();
        assert_eq!(
            invoke_json(&observer, Endpoint::Ipc, "count", "run")
                .await
                .unwrap(),
            json!("0")
        );
        let names = log.names();
        let late = names.iter().position(|s| s == "run/late.close").unwrap();
        let buffer = names.iter().position(|s| s == "run/buffer.close").unwrap();
        assert!(late < buffer);
        assert!(
            invoke_json(&observer, Endpoint::Ipc, "run", "run")
                .await
                .is_err()
        );
        bounded(rt.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_child_cleanup_is_cached_and_blocks_parent_and_provider() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        *log.fail.lock().unwrap() = Some("run/unsubscribe:2".into());
        rt.install(log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc).plugin("connections", &connections()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle) = consumer(&rt).await;
        invoke_json(&handle, Endpoint::Ipc, "open", "run")
            .await
            .unwrap();
        let first = invoke_json(&handle, Endpoint::Ipc, "release", "run")
            .await
            .unwrap_err();
        assert!(
            matches!(first, Error::Portable(cordis_sdk_core::Failure::Rejected(ref info)) if info.code == "cleanup_failed"),
            "{first}"
        );
        assert_eq!(
            invoke_json(&handle, Endpoint::Ipc, "release", "run")
                .await
                .unwrap_err(),
            first
        );
        let status = invoke_json(&handle, Endpoint::Ipc, "status", "run")
            .await
            .unwrap();
        assert_eq!(status["state"], "unloading");
        assert!(!status["error"].is_null());
        assert_eq!(
            log.names()
                .iter()
                .filter(|s| s.as_str() == "run/unsubscribe:2")
                .count(),
            1
        );
        assert!(!log.names().contains(&"run/buffer.close".into()));
        assert!(bounded(owner.dispose()).await.is_err());
        assert!(bounded(provider.dispose()).await.is_err());
        assert!(bounded(rt.shutdown()).await.is_err());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dependency_restart_recovers_old_scopes_before_replacing_their_definition() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        let logger = rt.install(log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc).plugin("connections", &connections()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (_, handle) = consumer(&rt).await;
        invoke_json(&handle, Endpoint::Ipc, "open", "run")
            .await
            .unwrap();
        // Retire instead of reusing the test logger's closed flag; replacement
        // gets a distinct value while old inverses must still see the old value.
        bounded(logger.dispose()).await.unwrap();
        assert_eq!(
            log.names(),
            [
                "run/buffer.open",
                "run/subscribe:1",
                "run/subscribe:2",
                "run/watcher.close",
                "run/unsubscribe:2",
                "run/unsubscribe:1",
                "run/buffer.close",
                "connections.closed",
                "logger.closed"
            ]
        );
        assert!(
            invoke_json(&handle, Endpoint::Ipc, "run", "run")
                .await
                .is_err()
        );
        let next = Log::new();
        rt.install(log_plugin(next.clone())).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (_, fresh) = consumer(&rt).await;
        invoke_json(&fresh, Endpoint::Ipc, "open", "new")
            .await
            .unwrap();
        bounded(rt.shutdown()).await.unwrap();
        assert!(next.names().contains(&"new/buffer.close".into()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_failure_recovers_child_scopes_before_process_deactivation() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        rt.install(log_plugin(log.clone())).unwrap();
        let mut spec = spec(backend, Endpoint::Ipc);
        spec.args.push("--fail-activate".into());
        let provider = rt
            .install(spec.plugin("connections", &connections()))
            .unwrap();
        let error = bounded(provider.ready()).await.unwrap_err();
        assert!(
            error.to_string().contains("startup_failed"),
            "backend={backend:?}: {error}"
        );
        assert_eq!(log.names(), ["startup.cleanup", "connections.closed"]);
        bounded(rt.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_scope_callbacks_leave_capacity_for_nested_runs_and_cleanup() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        rt.install(log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc).plugin("connections", &connections()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle) = consumer(&rt).await;
        let mut tasks = Vec::new();
        for i in 0..8 {
            let handle = handle.clone();
            tasks.push(tokio::spawn(async move {
                invoke_json(&handle, Endpoint::Ipc, "open", &format!("run{i}")).await
            }));
        }
        for task in tasks {
            bounded(task).await.unwrap().unwrap();
        }
        bounded(owner.dispose()).await.unwrap();
        assert_eq!(
            log.names()
                .iter()
                .filter(|s| s.ends_with("buffer.close"))
                .count(),
            8
        );
        bounded(rt.shutdown()).await.unwrap();
    }
}

// Deliberately malformed wire peers have their own test descriptor.
fn probe_service() -> Service<RemoteService> {
    Service::new("example.connections", "1").with_descriptor(
        cordis_remote::Descriptor::new(cordis_remote::Interface::value_dispatch(true)).unwrap(),
    )
}
fn peer(mode: &str) -> (ProcessSpec, PathBuf) {
    let trace = std::env::temp_dir().join(format!(
        "cordis-scope-{}-{}.log",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let mut spec = python_spec();
    spec.args.extend([
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/scope_peer.py")
            .display()
            .to_string(),
        mode.into(),
        trace.display().to_string(),
    ]);
    if mode == "hang" {
        spec.timeout = Duration::from_millis(200);
    }
    (spec, trace)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scope_protocol_rejects_stale_contexts_forged_grants_and_duplicate_ids() {
    for mode in ["normal", "business_error", "reject"] {
        let rt = Runtime::new();
        let (spec, trace) = peer(mode);
        let provider = rt.install(spec.plugin("peer", &probe_service())).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (_, handle) = probe_consumer(&rt).await;
        invoke_json(&handle, Endpoint::Ipc, "open", "run")
            .await
            .unwrap();
        assert_eq!(
            invoke_json(&handle, Endpoint::Ipc, "ready", "run")
                .await
                .unwrap()["kind"],
            "success"
        );
        for (method, code) in [("duplicate", "invalid_argument"), ("forged", "closed")] {
            assert_eq!(
                invoke_json(&handle, Endpoint::Ipc, method, "run")
                    .await
                    .unwrap()["error"]["code"],
                code
            );
        }
        let outcome = invoke_json(&handle, Endpoint::Ipc, "run", "run")
            .await
            .unwrap();
        assert_eq!(
            outcome["kind"],
            if mode == "normal" {
                "success"
            } else if mode == "reject" {
                "refused"
            } else {
                "failure"
            }
        );
        let status = invoke_json(&handle, Endpoint::Ipc, "status", "run")
            .await
            .unwrap()["value"]
            .clone();
        assert_eq!(
            status["runs"][0]["state"],
            if mode == "reject" {
                "not_executed"
            } else {
                "completed"
            }
        );
        assert_eq!(
            invoke_json(&handle, Endpoint::Ipc, "duplicate_run", "run")
                .await
                .unwrap()["error"]["code"],
            "invalid_argument"
        );
        if mode == "normal" {
            assert_eq!(
                invoke_json(&handle, Endpoint::Ipc, "run", "run")
                    .await
                    .unwrap()["value"],
                json!(43.0)
            );
        }
        for _ in 0..2 {
            assert_eq!(
                invoke_json(&handle, Endpoint::Ipc, "close", "run")
                    .await
                    .unwrap()["kind"],
                "success"
            );
        }
        let refused = invoke_json(&handle, Endpoint::Ipc, "run", "run")
            .await
            .unwrap();
        assert_eq!(refused["kind"], "refused");
        assert_eq!(refused["error"]["code"], "retired");
        bounded(rt.shutdown()).await.unwrap();
        let text = std::fs::read_to_string(&trace).unwrap();
        assert_eq!(
            text.lines().filter(|s| *s == "cleanup").count(),
            usize::from(mode != "reject")
        );
        if mode == "normal" {
            assert!(text.contains("stale_rejected"));
        }
        std::fs::remove_file(trace).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_scope_runs_and_failed_cleanup_retain_child_responsibility() {
    for mode in ["crash", "hang", "malformed", "cleanup_crash"] {
        let rt = Runtime::new();
        let (spec, trace) = peer(mode);
        let provider = rt.install(spec.plugin("peer", &probe_service())).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle) = probe_consumer(&rt).await;
        invoke_json(&handle, Endpoint::Ipc, "open", "run")
            .await
            .unwrap();
        let _ = invoke_json(&handle, Endpoint::Ipc, "run", "run").await;
        if mode == "malformed" {
            let status = invoke_json(&handle, Endpoint::Ipc, "status", "run")
                .await
                .unwrap();
            assert_eq!(status["value"]["runs"][0]["state"], "unknown");
            assert_eq!(
                invoke_json(&handle, Endpoint::Ipc, "run", "run")
                    .await
                    .unwrap()["kind"],
                "failure"
            );
            assert_eq!(
                invoke_json(&handle, Endpoint::Ipc, "close", "run")
                    .await
                    .unwrap()["kind"],
                "failure"
            );
        }
        assert!(bounded(owner.dispose()).await.is_err(), "{mode}");
        assert!(bounded(provider.dispose()).await.is_err(), "{mode}");
        assert!(bounded(rt.shutdown()).await.is_err());
        let text = std::fs::read_to_string(&trace).unwrap();
        assert!(text.contains("registered"));
        assert!(!text.contains("deactivated"));
        if mode != "cleanup_crash" {
            assert!(!text.contains("cleanup"), "{mode}: {text}");
        }
        std::fs::remove_file(trace).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_scope_registration_reply_leaves_no_orphaned_child() {
    let rt = Runtime::new();
    let (spec, trace) = peer("register_crash");
    let provider = rt.install(spec.plugin("peer", &probe_service())).unwrap();
    bounded(provider.ready()).await.unwrap();
    let (owner, handle) = probe_consumer(&rt).await;
    assert!(
        invoke_json(&handle, Endpoint::Ipc, "open", "run")
            .await
            .is_err()
    );
    bounded(owner.dispose()).await.unwrap();
    assert!(
        rt.snapshots()
            .iter()
            .filter(|s| s.name == "test child")
            .all(|s| s.retired && s.state == Lifecycle::Inactive)
    );
    assert!(bounded(provider.dispose()).await.is_err());
    assert!(bounded(rt.shutdown()).await.is_err());
    let _ = std::fs::remove_file(trace);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_scopes_validate_before_creation_and_reject_concurrent_open() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        *log.block.lock().unwrap() = Some("run/buffer.open".into());
        rt.install(log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc).plugin("connections", &connections()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle) = consumer(&rt).await;
        for value in [json!(42), json!({"name":"run"})] {
            assert!(call(&handle, Endpoint::Ipc, "open", value).await.is_err());
        }
        assert!(owner.effects().is_empty());
        assert!(log.names().is_empty());
        let pending = tokio::spawn({
            let handle = handle.clone();
            async move { invoke_json(&handle, Endpoint::Ipc, "open", "run").await }
        });
        bounded(log.entered.acquire()).await.unwrap().forget();
        let error = invoke_json(&handle, Endpoint::Ipc, "open", "run")
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("duplicate_connection"),
            "{error}"
        );
        log.release.add_permits(1);
        bounded(pending).await.unwrap().unwrap();
        assert_eq!(
            invoke_json(&handle, Endpoint::Ipc, "count", "run")
                .await
                .unwrap(),
            json!("4")
        );
        invoke_json(&handle, Endpoint::Ipc, "release", "run")
            .await
            .unwrap();
        assert_eq!(
            invoke_json(&handle, Endpoint::Ipc, "count", "run")
                .await
                .unwrap(),
            json!("0")
        );
        let status = invoke_json(&handle, Endpoint::Ipc, "status", "run")
            .await
            .unwrap();
        assert_eq!(status["state"], "inactive");
        assert_eq!(status["retired"], true);
        bounded(rt.shutdown()).await.unwrap();
        assert_eq!(
            log.names()
                .iter()
                .filter(|n| *n == "run/buffer.close")
                .count(),
            1
        );
    }
}
