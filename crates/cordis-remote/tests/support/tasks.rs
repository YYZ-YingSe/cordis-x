use super::effects::{
    Log, lifecycle_log_dependency as log_dependency, lifecycle_log_plugin as log_plugin,
};
use super::*;
use cordis_remote::Value;

fn tasks() -> Service<RemoteService> {
    Service::new("example.tasks", "1").with_descriptor(
        cordis_remote::Descriptor::new(
            cordis_portable_example::lifecycle::HeartbeatsClient::interface(),
        )
        .unwrap(),
    )
}
fn spec(backend: Backend, endpoint: Endpoint) -> ProcessSpec {
    let mut spec = backend.spec("tasks");
    spec.endpoint = endpoint;

    if matches!(endpoint, Endpoint::Ipc) {
        spec.args.push("--stdio-only".into());
    }
    spec.requires(log_dependency())
}
async fn consumer(rt: &Runtime) -> (PluginHandle, ServiceHandle<RemoteService>) {
    consumer_for(rt, tasks()).await
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
async fn tasks_outlive_rpc_deadlines_and_stop_before_cleanup_across_sdks_and_endpoints() {
    for &backend in Backend::ALL {
        for endpoint in [Endpoint::Ipc, Endpoint::Http] {
            let rt = Runtime::new();
            let log = Log::new();
            rt.install(log_plugin(log.clone())).unwrap();
            let mut spec = spec(backend, endpoint);
            spec.timeout = Duration::from_millis(300);
            let provider = rt.install(spec.plugin("tasks", &tasks())).unwrap();
            bounded(provider.ready()).await.unwrap();
            let (owner, handle) = consumer(&rt).await;
            let (_, observer) = consumer(&rt).await;
            invoke_json(&handle, endpoint, "open", "run").await.unwrap();
            tokio::time::sleep(Duration::from_millis(650)).await;
            assert_eq!(
                invoke_json(&observer, endpoint, "status", "run")
                    .await
                    .unwrap()["state"],
                "running"
            );
            invoke_json(&observer, endpoint, "release", "run")
                .await
                .unwrap();
            for _ in 0..2 {
                invoke_json(&observer, endpoint, "join", "run")
                    .await
                    .unwrap();
            }
            assert_eq!(
                invoke_json(&observer, endpoint, "status", "run")
                    .await
                    .unwrap()["state"],
                "completed"
            );
            assert!(log.names().ends_with(&[
                "run/stopped".into(),
                "run/last.cleanup".into(),
                "run/task.cleanup".into(),
                "run/connection.close".into()
            ]));
            assert_eq!(owner.snapshot().state, Lifecycle::Active);
            bounded(rt.shutdown()).await.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_join_wait_does_not_stop_tasks_or_lose_late_contributions() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        *log.block.lock().unwrap() = Some("run/stopped".into());
        rt.install(log_plugin(log.clone())).unwrap();
        let mut spec = spec(backend, Endpoint::Ipc);
        spec.args.push("--task-direct".into());
        let provider = rt.install(spec.plugin("tasks", &tasks())).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle) = consumer(&rt).await;
        invoke_json(&handle, Endpoint::Ipc, "open", "run")
            .await
            .unwrap();
        let joining = tokio::spawn({
            let handle = handle.clone();
            async move { invoke_json(&handle, Endpoint::Ipc, "join", "run").await }
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        joining.abort();
        let _ = joining.await;
        assert_eq!(
            invoke_json(&handle, Endpoint::Ipc, "status", "run")
                .await
                .unwrap()["state"],
            "running"
        );
        let disposing = tokio::spawn(async move { owner.dispose().await });
        bounded(log.entered.acquire()).await.unwrap().forget();
        assert!(!disposing.is_finished());
        assert!(!log.names().contains(&"run/connection.close".into()));
        log.release.add_permits(1);
        bounded(disposing).await.unwrap().unwrap();
        assert!(log.names().ends_with(&[
            "run/stopped".into(),
            "run/last.cleanup".into(),
            "run/task.cleanup".into(),
            "run/connection.close".into()
        ]));
        bounded(rt.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task_stop_grace_is_independent_of_the_ordinary_rpc_deadline() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        *log.block.lock().unwrap() = Some("run/stopped".into());
        rt.install(log_plugin(log.clone())).unwrap();
        let mut spec = spec(backend, Endpoint::Ipc);
        spec.timeout = Duration::from_millis(300);
        spec.task_stop_timeout = Duration::from_secs(2);
        let provider = rt.install(spec.plugin("tasks", &tasks())).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle) = consumer(&rt).await;
        invoke_json(&handle, Endpoint::Ipc, "open", "run")
            .await
            .unwrap();
        let stopping = tokio::spawn(async move { owner.dispose().await });
        bounded(log.entered.acquire()).await.unwrap().forget();
        tokio::time::sleep(Duration::from_millis(650)).await;
        assert!(!stopping.is_finished());
        assert_eq!(provider.snapshot().state, Lifecycle::Active);
        log.release.add_permits(1);
        bounded(stopping).await.unwrap().unwrap();
        bounded(rt.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_dependencies_remain_usable_until_tasks_exit_and_effects_reverse() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        *log.block.lock().unwrap() = Some("run/stopped".into());
        let logger = rt.install(log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc).plugin("tasks", &tasks()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (_, handle) = consumer(&rt).await;
        invoke_json(&handle, Endpoint::Ipc, "open", "run")
            .await
            .unwrap();
        let stopping = tokio::spawn(async move { logger.dispose().await });
        bounded(log.entered.acquire()).await.unwrap().forget();
        assert!(log.open.load(Ordering::SeqCst));
        assert!(!stopping.is_finished());
        log.release.add_permits(1);
        bounded(stopping).await.unwrap().unwrap();
        assert!(log.names().ends_with(&[
            "run/last.cleanup".into(),
            "run/task.cleanup".into(),
            "run/connection.close".into(),
            "tasks.closed".into(),
            "logger.closed".into()
        ]));
        let next = Log::new();
        rt.install(log_plugin(next.clone())).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (_, fresh) = consumer(&rt).await;
        invoke_json(&fresh, Endpoint::Ipc, "open", "new")
            .await
            .unwrap();
        bounded(rt.shutdown()).await.unwrap();
        assert!(next.names().contains(&"new/connection.close".into()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_task_capacity_still_allows_cancellation_dependencies_and_cleanup() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        rt.install(log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc).plugin("tasks", &tasks()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle) = consumer(&rt).await;
        for i in 0..16 {
            invoke_json(&handle, Endpoint::Ipc, "open", &i.to_string())
                .await
                .unwrap();
        }
        assert!(
            invoke_json(&handle, Endpoint::Ipc, "open", "overflow")
                .await
                .is_err()
        );
        assert_eq!(
            invoke_json(&handle, Endpoint::Ipc, "status", "0")
                .await
                .unwrap()["state"],
            "running"
        );
        bounded(owner.dispose()).await.unwrap();
        let names = log.names();
        assert_eq!(names.iter().filter(|s| s.ends_with("/stopped")).count(), 16);
        assert_eq!(
            names
                .iter()
                .filter(|s| s.ends_with("/connection.close"))
                .count(),
            17
        );
        bounded(rt.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task_failure_is_cached_and_retains_failed_join_in_recovery() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        rt.install(log_plugin(log.clone())).unwrap();
        let mut spec = spec(backend, Endpoint::Ipc);
        spec.args.push("--task-fail".into());
        let provider = rt.install(spec.plugin("tasks", &tasks())).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle) = consumer(&rt).await;
        invoke_json(&handle, Endpoint::Ipc, "open", "run")
            .await
            .unwrap();
        let error = invoke_json(&handle, Endpoint::Ipc, "join", "run")
            .await
            .unwrap_err();
        assert_eq!(
            invoke_json(&handle, Endpoint::Ipc, "join", "run")
                .await
                .unwrap_err(),
            error
        );
        assert_eq!(
            invoke_json(&handle, Endpoint::Ipc, "status", "run")
                .await
                .unwrap()["state"],
            "completed"
        );
        assert!(bounded(owner.dispose()).await.is_err());
        assert!(log.names().contains(&"run/task.cleanup".into()));
        assert!(!log.names().contains(&"run/connection.close".into()));
        assert!(bounded(provider.dispose()).await.is_err());
        assert!(bounded(rt.shutdown()).await.is_err());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn initialization_failure_signals_and_joins_tasks_before_deactivation() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        rt.install(log_plugin(log.clone())).unwrap();
        let mut spec = spec(backend, Endpoint::Ipc);
        spec.args
            .extend(["--fail-activate".into(), "--task-direct".into()]);
        let provider = rt.install(spec.plugin("tasks", &tasks())).unwrap();
        assert!(bounded(provider.ready()).await.is_err());
        bounded(provider.dispose()).await.unwrap();
        assert!(log.names().ends_with(&[
            "startup/stopped".into(),
            "startup/last.cleanup".into(),
            "startup/task.cleanup".into(),
            "startup/connection.close".into(),
            "tasks.closed".into()
        ]));
        bounded(rt.shutdown()).await.unwrap();
    }
}

// Deliberately malformed wire peers have their own test descriptor.
fn probe_service() -> Service<RemoteService> {
    Service::new("example.tasks", "1").with_descriptor(
        cordis_remote::Descriptor::new(cordis_remote::Interface::value_dispatch(true)).unwrap(),
    )
}
fn peer(mode: &str) -> (ProcessSpec, PathBuf) {
    let trace = std::env::temp_dir().join(format!(
        "cordis-task-{}-{}.log",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let mut spec = python_spec();
    spec.args.extend([
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/task_peer.py")
            .display()
            .to_string(),
        mode.into(),
        trace.display().to_string(),
    ]);
    spec.timeout = Duration::from_millis(250);
    spec.task_stop_timeout = Duration::from_millis(250);
    (spec, trace)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task_grants_duplicates_expired_contexts_and_known_admission_rejection() {
    for mode in ["finite", "reject", "business_error"] {
        let rt = Runtime::new();
        let (spec, trace) = peer(mode);
        let provider = rt.install(spec.plugin("peer", &probe_service())).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle) = probe_consumer(&rt).await;
        invoke_json(&handle, Endpoint::Ipc, "open", "run")
            .await
            .unwrap();
        let joined = invoke_json(&handle, Endpoint::Ipc, "join", "run")
            .await
            .unwrap();
        assert_eq!(
            joined["kind"],
            if mode == "finite" {
                "success"
            } else if mode == "reject" {
                "refused"
            } else {
                "failure"
            }
        );
        let status = invoke_json(&handle, Endpoint::Ipc, "status", "run")
            .await
            .unwrap();
        assert_eq!(
            status["value"]["state"],
            if mode == "reject" {
                "not_executed"
            } else {
                "completed"
            }
        );
        for method in ["forged", "duplicate", "stale", "forged_cleanup"] {
            assert_eq!(
                invoke_json(&handle, Endpoint::Ipc, method, "run")
                    .await
                    .unwrap()["kind"],
                "refused"
            );
        }
        assert_eq!(
            bounded(owner.dispose()).await.is_ok(),
            mode != "business_error"
        );
        assert_eq!(
            bounded(provider.dispose()).await.is_ok(),
            mode != "business_error"
        );
        assert_eq!(
            bounded(rt.shutdown()).await.is_ok(),
            mode != "business_error"
        );
        let text = std::fs::read_to_string(&trace).unwrap();
        assert_eq!(text.lines().filter(|s| *s == "started").count(), 1);
        assert_eq!(text.contains("cleanup"), mode != "reject");
        std::fs::remove_file(trace).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task_crash_malformed_reply_and_uncooperative_stop_retain_unknown_effects() {
    for mode in ["crash", "malformed", "hang", "ack_no_exit"] {
        let rt = Runtime::new();
        let (mut spec, trace) = peer(mode);
        spec.timeout = Duration::from_secs(2);
        let provider = rt.install(spec.plugin("peer", &probe_service())).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle) = probe_consumer(&rt).await;
        let _ = invoke_json(&handle, Endpoint::Ipc, "open", "run").await;
        bounded(async {
            while !std::fs::read_to_string(&trace)
                .unwrap_or_default()
                .contains("registered")
            {
                tokio::task::yield_now().await;
            }
        })
        .await;
        if matches!(mode, "hang" | "ack_no_exit") {
            // Observe a live task before creating a pending join and stopping it.
            tokio::time::sleep(Duration::from_millis(550)).await;
            assert_eq!(
                invoke_json(&handle, Endpoint::Ipc, "status", "run")
                    .await
                    .unwrap()["value"]["state"],
                "running"
            );
        }
        let joining = if matches!(mode, "hang" | "ack_no_exit") {
            let handle = handle.clone();
            let joining =
                tokio::spawn(
                    async move { invoke_json(&handle, Endpoint::Ipc, "join", "run").await },
                );
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(!joining.is_finished());
            Some(joining)
        } else {
            None
        };
        assert!(bounded(owner.dispose()).await.is_err(), "{mode}");
        if let Some(joining) = joining {
            assert!(bounded(joining).await.unwrap().is_err());
        }
        assert!(bounded(provider.dispose()).await.is_err(), "{mode}");
        assert!(bounded(rt.shutdown()).await.is_err());
        let text = std::fs::read_to_string(&trace).unwrap();
        assert!(!text.contains("cleanup"), "{mode}: {text}");
        assert!(!text.contains("deactivated"), "{mode}: {text}");
        if matches!(mode, "hang" | "ack_no_exit") {
            assert!(text.contains("cancelled"));
        }
        std::fs::remove_file(trace).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task_registration_ack_loss_cannot_create_an_unowned_task() {
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
    // The request may or may not have been admitted before the peer exited. Any
    // admitted task must settle and retain uncertainty, never run unowned.
    let _ = bounded(owner.dispose()).await;
    assert!(bounded(provider.dispose()).await.is_err());
    assert!(bounded(rt.shutdown()).await.is_err());
    assert!(
        !std::fs::read_to_string(&trace)
            .unwrap_or_default()
            .contains("deactivated")
    );
    let _ = std::fs::remove_file(trace);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_tasks_validate_before_spawn_and_share_one_completed_task() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        rt.install(log_plugin(log.clone())).unwrap();
        let mut process = spec(backend, Endpoint::Ipc);
        process
            .args
            .extend(["--task-finite".into(), "--task-direct".into()]);
        let provider = rt.install(process.plugin("tasks", &tasks())).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle) = consumer(&rt).await;
        for value in [json!(42), json!({"name":"run"})] {
            assert!(call(&handle, Endpoint::Ipc, "open", value).await.is_err());
        }
        assert!(owner.effects().is_empty());
        assert!(log.names().is_empty());
        let (a, b) = tokio::join!(
            invoke_json(&handle, Endpoint::Ipc, "open", "run"),
            invoke_json(&handle, Endpoint::Ipc, "open", "run")
        );
        assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
        let error = a.err().or_else(|| b.err()).unwrap();
        assert!(
            error.to_string().contains("duplicate_connection"),
            "{error}"
        );
        invoke_json(&handle, Endpoint::Ipc, "join", "run")
            .await
            .unwrap();
        invoke_json(&handle, Endpoint::Ipc, "join", "run")
            .await
            .unwrap();
        assert_eq!(
            invoke_json(&handle, Endpoint::Ipc, "status", "run")
                .await
                .unwrap(),
            json!({"state":"completed","outcome":{"succeeded":{}}})
        );
        let error = invoke_json(&handle, Endpoint::Ipc, "release", "run")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("direct_task"), "{error}");
        bounded(owner.dispose()).await.unwrap();
        assert_eq!(
            log.names(),
            ["run/started", "run/task.cleanup", "run/connection.close"]
        );
        bounded(rt.shutdown()).await.unwrap();
    }
}
