use super::effects::{
    Log, lifecycle_log_dependency as log_dependency, lifecycle_log_plugin as log_plugin,
};
use super::*;
use cordis_remote::{MiddlewareBinding, Value};
fn service() -> Service<RemoteService> {
    Service::new("example.middleware", "1").with_descriptor(
        cordis_remote::Descriptor::new(
            cordis_portable_example::lifecycle::PipelineClient::interface(),
        )
        .unwrap(),
    )
}
fn pipeline() -> Middleware<cordis_remote::Bytes, cordis_remote::Bytes> {
    Middleware::new("example.middleware.request", "1").in_realm(RealmId(71))
}
fn spec(backend: Backend, endpoint: Endpoint) -> ProcessSpec {
    let mut s = backend.spec("middleware");
    if matches!(endpoint, Endpoint::Ipc) {
        s.args.push("--stdio-only".into());
    }
    s.endpoint = endpoint;

    s.requires(log_dependency())
        .middleware(MiddlewareBinding::new("request", &pipeline()))
}
async fn consumer(rt: &Runtime) -> (PluginHandle, ServiceHandle<RemoteService>, Context) {
    consumer_for(rt, service()).await
}
async fn probe_consumer(rt: &Runtime) -> (PluginHandle, ServiceHandle<RemoteService>, Context) {
    consumer_for(rt, probe_service()).await
}
async fn consumer_for(
    rt: &Runtime,
    service: Service<RemoteService>,
) -> (PluginHandle, ServiceHandle<RemoteService>, Context) {
    let saved = Arc::new(Mutex::new(None));
    let owner = rt
        .install(
            Plugin::new("owner", {
                let saved = saved.clone();
                let service = service.clone();
                move |ctx| {
                    let service = service.clone();
                    let saved = saved.clone();
                    async move {
                        *saved.lock().unwrap() = Some((ctx.dependency(&service)?, ctx));
                        Ok(())
                    }
                }
            })
            .requires(&service),
        )
        .unwrap();
    bounded(owner.ready()).await.unwrap();
    let (handle, ctx) = saved.lock().unwrap().take().unwrap();
    (owner, handle, ctx)
}
async fn invoke_json(h: &ServiceHandle<RemoteService>, method: &str, args: Value) -> Result<Value> {
    bounded(call(h, Endpoint::Ipc, method, args)).await
}
async fn portable(endpoint: Endpoint) {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        rt.install(log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, endpoint).plugin("pipeline", &service()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, h, ctx) = consumer(&rt).await;
        for id in ["a", "b"] {
            bounded(call(
                &h,
                endpoint,
                "register_handler",
                json!({"id":id,"behavior":"WRAP"}),
            ))
            .await
            .unwrap();
        }
        assert_eq!(
            bounded(call(&h, endpoint, "run", json!(10))).await.unwrap(),
            json!("30")
        );
        assert_eq!(
            log.names(),
            ["a.enter", "b.enter", "terminal.enter", "b.exit", "a.exit"]
        );
        assert!(
            invoke_json(&h, "expired", json!(1))
                .await
                .unwrap_err()
                .to_string()
                .contains("expired_next")
        );
        assert_eq!(
            Middleware::<Value, Value>::new("example.middleware.request", "1")
                .run(&ctx, json!(1), |v, _| async { Ok(v) })
                .await
                .unwrap(),
            json!(1)
        );
        assert_eq!(
            Middleware::<Value, Value>::new("example.middleware.request", "2")
                .in_realm(RealmId(71))
                .run(&ctx, json!(1), |v, _| async { Ok(v) })
                .await
                .unwrap(),
            json!(1)
        );
        for _ in 0..2 {
            invoke_json(&h, "release", json!("a")).await.unwrap();
        }
        assert_eq!(
            invoke_json(&h, "run", json!(10)).await.unwrap(),
            json!("25")
        );
        invoke_json(&h, "release", json!("b")).await.unwrap();
        invoke_json(
            &h,
            "register_handler",
            json!({"id":"short","behavior":"SHORT"}),
        )
        .await
        .unwrap();
        assert_eq!(
            invoke_json(&h, "run", json!(1)).await.unwrap(),
            json!("101")
        );
        invoke_json(&h, "release", json!("short")).await.unwrap();
        invoke_json(
            &h,
            "register_handler",
            json!({"id":"duplicate","behavior":"DUPLICATE"}),
        )
        .await
        .unwrap();
        let error = invoke_json(&h, "run", json!(1)).await.unwrap_err();
        assert!(error.to_string().contains("duplicate_next"), "{error}");
        invoke_json(&h, "release", json!("duplicate"))
            .await
            .unwrap();
        invoke_json(
            &h,
            "register_handler",
            json!({"id":"fail","behavior":"FAIL"}),
        )
        .await
        .unwrap();
        assert!(
            invoke_json(&h, "run", json!(1))
                .await
                .unwrap_err()
                .to_string()
                .contains("business failure")
        );
        invoke_json(&h, "release", json!("fail")).await.unwrap();
        assert_eq!(invoke_json(&h, "run", json!(2)).await.unwrap(), json!("4"));
        let owner_id = owner.snapshot().id.0;
        bounded(owner.dispose()).await.unwrap();
        assert!(
            log.events
                .lock()
                .unwrap()
                .iter()
                .filter(|(n, _)| n.ends_with(".cleanup"))
                .all(|(_, id)| *id == owner_id)
        );
        bounded(rt.shutdown()).await.unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn portable_middleware_ipc_orders_transforms_short_circuits_and_owns_resources() {
    portable(Endpoint::Ipc).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn portable_middleware_http_orders_transforms_short_circuits_and_owns_resources() {
    portable(Endpoint::Http).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_run_and_unregister_wait_for_downstream_and_late_resources() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        rt.install(log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc).plugin("pipeline", &service()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, h, _) = consumer(&rt).await;
        invoke_json(&h, "register_handler", json!({"id":"a","behavior":"WRAP"}))
            .await
            .unwrap();
        *log.block.lock().unwrap() = Some("terminal.enter".into());
        let running = tokio::spawn({
            let h = h.clone();
            async move { invoke_json(&h, "run", json!(3)).await }
        });
        bounded(log.entered.acquire()).await.unwrap().forget();
        running.abort();
        let _ = running.await;
        let unregister = tokio::spawn({
            let h = h.clone();
            async move { invoke_json(&h, "release", json!("a")).await }
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!unregister.is_finished());
        unregister.abort();
        let _ = unregister.await;
        let close = tokio::spawn(async move { owner.dispose().await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!close.is_finished());
        log.release.add_permits(1);
        bounded(close).await.unwrap().unwrap();
        assert_eq!(
            log.names(),
            [
                "a.enter",
                "terminal.enter",
                "a.exit",
                "terminal.cleanup",
                "a.cleanup"
            ]
        );
        bounded(rt.shutdown()).await.unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sixteen_nested_callbacks_can_reach_terminal_and_cleanup() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        rt.install(log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc).plugin("pipeline", &service()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, h, _) = consumer(&rt).await;
        for i in 0..16 {
            invoke_json(
                &h,
                "register_handler",
                json!({"id":format!("h{i}"),"behavior":"WRAP"}),
            )
            .await
            .unwrap();
        }
        assert_eq!(invoke_json(&h, "run", json!(0)).await.unwrap(), json!("80"));
        invoke_json(
            &h,
            "register_handler",
            json!({"id":"overflow","behavior":"WRAP"}),
        )
        .await
        .unwrap();
        let error = invoke_json(&h, "run", json!(0)).await.unwrap_err();
        assert!(
            error.to_string().contains("overloaded"),
            "backend={backend:?}: {error}; diagnostics={:?}",
            rt.diagnostics()
        );
        invoke_json(&h, "release", json!("overflow")).await.unwrap();
        assert_eq!(invoke_json(&h, "run", json!(0)).await.unwrap(), json!("80"));
        bounded(owner.dispose()).await.unwrap();
        bounded(rt.shutdown()).await.unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_failure_releases_middleware_and_binding_mismatch_rejects() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        rt.install(log_plugin(log.clone())).unwrap();
        let mut s = spec(backend, Endpoint::Ipc);
        s.args.extend(["--listen".into(), "--fail-activate".into()]);
        let provider = rt.install(s.plugin("failed", &service())).unwrap();
        assert!(bounded(provider.ready()).await.is_err());
        bounded(provider.dispose()).await.unwrap();
        let mut s = spec(backend, Endpoint::Ipc);
        s.middleware.clear();
        let mismatch = rt.install(s.plugin("mismatch", &service())).unwrap();
        assert!(bounded(mismatch.ready()).await.is_err());
        bounded(mismatch.dispose()).await.unwrap();
        bounded(rt.shutdown()).await.unwrap();
    }
}

// Raw peers deliberately send malformed messages and forged capabilities.
fn probe_service() -> Service<RemoteService> {
    Service::new("example.middleware", "1").with_descriptor(
        cordis_remote::Descriptor::new(cordis_remote::Interface::value_dispatch(true)).unwrap(),
    )
}
fn probe_pipeline() -> Middleware<cordis_remote::Bytes, cordis_remote::Bytes> {
    Middleware::new("example.request", "1").in_realm(RealmId(71))
}
fn peer(mode: &str) -> (ProcessSpec, PathBuf) {
    let trace = std::env::temp_dir().join(format!(
        "cordis-middleware-{}-{}.log",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let mut s = python_spec();
    s.args.extend([
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/middleware_peer.py")
            .display()
            .to_string(),
        mode.into(),
        trace.display().to_string(),
    ]);
    s.timeout = Duration::from_millis(500);
    (
        s.middleware(MiddlewareBinding::new("request", &probe_pipeline())),
        trace,
    )
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn middleware_protocol_rejects_forged_duplicate_expired_and_cross_frame_next() {
    for mode in ["finite", "reject", "business_error", "next", "probe"] {
        let rt = Runtime::new();
        let (s, trace) = peer(mode);
        let provider = rt.install(s.plugin("peer", &probe_service())).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, h, ctx) = probe_consumer(&rt).await;
        invoke_json(&h, "open", Value::Null).await.unwrap();
        let dispatch = tokio::spawn(async move {
            probe_pipeline()
                .run(&ctx, wire_json(json!(1)), |v, _| async move {
                    Ok(wire_json(json!(unwire_json(v).as_f64().unwrap() + 10.0)))
                })
                .await
        });
        if mode == "probe" {
            bounded(async {
                loop {
                    if std::fs::read_to_string(&trace)
                        .unwrap_or_default()
                        .contains("registered")
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await;
            assert_eq!(
                invoke_json(&h, "probe", Value::Null).await.unwrap()["kind"],
                "refused"
            );
        }
        let result = bounded(dispatch).await.unwrap();
        assert_eq!(
            result.is_ok(),
            matches!(mode, "finite" | "next" | "probe"),
            "{mode}: {result:?}"
        );
        if matches!(mode, "next" | "probe") {
            assert_eq!(unwire_json(result.unwrap()), json!(17.0));
        }
        for method in [
            "forged",
            "duplicate",
            "stale",
            "unbound",
            "forged_cleanup",
            "late_next",
        ] {
            assert_eq!(
                invoke_json(&h, method, Value::Null).await.unwrap()["kind"],
                "refused",
                "{mode}/{method}"
            );
        }
        for _ in 0..2 {
            assert_eq!(
                invoke_json(&h, "close", Value::Null).await.unwrap()["kind"],
                "success"
            );
        }
        bounded(owner.dispose()).await.unwrap();
        bounded(rt.shutdown()).await.unwrap();
        let text = std::fs::read_to_string(&trace).unwrap();
        assert_eq!(text.lines().filter(|s| *s == "released").count(), 1);
        assert_eq!(text.contains("cleanup"), mode != "reject");
        std::fs::remove_file(trace).unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_middleware_and_terminal_outcomes_block_earlier_cleanup() {
    for mode in [
        "crash",
        "hang",
        "malformed",
        "terminal_crash",
        "terminal_hang",
        "terminal_malformed",
    ] {
        let rt = Runtime::new();
        let (s, trace) = peer(mode);
        let provider = rt.install(s.plugin("peer", &probe_service())).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, h, ctx) = probe_consumer(&rt).await;
        if mode.starts_with("terminal_") {
            let result = invoke_json(&h, "run", Value::Null).await;
            assert!(result.is_err() || result.unwrap()["kind"] == "unknown");
        } else {
            invoke_json(&h, "open", Value::Null).await.unwrap();
            assert!(
                bounded(probe_pipeline().run(&ctx, wire_json(json!(1)), |v, _| async { Ok(v) }))
                    .await
                    .is_err()
            );
        }
        assert!(bounded(owner.dispose()).await.is_err());
        assert!(bounded(provider.dispose()).await.is_err());
        assert!(bounded(rt.shutdown()).await.is_err());
        let text = std::fs::read_to_string(&trace).unwrap();
        assert!(text.contains("registered"));
        assert!(
            !text.contains("cleanup")
                && !text.contains("released")
                && !text.contains("deactivated"),
            "{mode}: {text}"
        );
        std::fs::remove_file(trace).unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_middleware_registration_ack_retains_recovery_responsibility() {
    let rt = Runtime::new();
    let (s, trace) = peer("register_crash");
    let provider = rt.install(s.plugin("peer", &probe_service())).unwrap();
    bounded(provider.ready()).await.unwrap();
    let (owner, h, _) = probe_consumer(&rt).await;
    assert!(invoke_json(&h, "open", Value::Null).await.is_err());
    let _ = bounded(owner.dispose()).await;
    assert!(bounded(provider.dispose()).await.is_err());
    assert!(bounded(rt.shutdown()).await.is_err());
    let _ = std::fs::remove_file(trace);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn early_remote_reply_cannot_release_an_admitted_native_next() {
    let rt = Runtime::new();
    let (mut s, trace) = peer("early");
    s.timeout = Duration::from_secs(5);
    let provider = rt.install(s.plugin("peer", &probe_service())).unwrap();
    bounded(provider.ready()).await.unwrap();
    let (owner, h, ctx) = probe_consumer(&rt).await;
    invoke_json(&h, "open", Value::Null).await.unwrap();
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let dispatch = tokio::spawn({
        let entered = entered.clone();
        let release = release.clone();
        let trace = trace.clone();
        async move {
            probe_pipeline()
                .run(&ctx, wire_json(json!(1)), move |_, ctx| {
                    let entered = entered.clone();
                    let release = release.clone();
                    let trace = trace.clone();
                    async move {
                        use std::io::Write;
                        writeln!(
                            std::fs::OpenOptions::new()
                                .append(true)
                                .open(trace)
                                .unwrap(),
                            "terminal_entered"
                        )
                        .unwrap();
                        entered.add_permits(1);
                        release.acquire().await.unwrap().forget();
                        ctx.defer("late native", |_| async { Ok(()) })?;
                        Ok(wire_json(json!(7)))
                    }
                })
                .await
        }
    });
    bounded(entered.acquire()).await.unwrap().forget();
    let dispose = tokio::spawn(async move { owner.dispose().await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!dispatch.is_finished() && !dispose.is_finished());
    release.add_permits(1);
    assert_eq!(
        unwire_json(bounded(dispatch).await.unwrap().unwrap()),
        json!(9.0)
    );
    bounded(dispose).await.unwrap().unwrap();
    bounded(rt.shutdown()).await.unwrap();
    std::fs::remove_file(trace).unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn middleware_callbacks_and_cleanup_use_old_dependencies_before_reactivation() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        let logger = rt.install(log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc).plugin("pipeline", &service()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (_, h, _) = consumer(&rt).await;
        invoke_json(&h, "register_handler", json!({"id":"a","behavior":"WRAP"}))
            .await
            .unwrap();
        *log.block.lock().unwrap() = Some("terminal.enter".into());
        let dispatch = tokio::spawn({
            let h = h.clone();
            async move { invoke_json(&h, "run", json!(1)).await }
        });
        bounded(log.entered.acquire()).await.unwrap().forget();
        let close = tokio::spawn(async move { logger.dispose().await });
        bounded(async {
            while provider.snapshot().state != Lifecycle::Unloading {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(log.open.load(Ordering::SeqCst));
        assert!(!close.is_finished());
        log.release.add_permits(1);
        assert_eq!(bounded(dispatch).await.unwrap().unwrap(), json!("7"));
        bounded(close).await.unwrap().unwrap();
        assert!(log.names().ends_with(&[
            "terminal.cleanup".into(),
            "a.cleanup".into(),
            "middleware.closed".into(),
            "logger.closed".into()
        ]));
        rt.install(log_plugin(Log::new())).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (_, fresh, _) = consumer(&rt).await;
        assert_eq!(
            invoke_json(&fresh, "run", json!(1)).await.unwrap(),
            json!("2")
        );
        bounded(rt.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_middleware_preserves_integer_bounds_and_rejects_duplicate_registration() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        rt.install(log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc).plugin("pipeline", &service()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle, _) = consumer(&rt).await;
        for args in [
            json!({}),
            json!({"id":"one"}),
            json!({"id":"one","behavior":999}),
        ] {
            assert!(
                invoke_json(&handle, "register_handler", args)
                    .await
                    .is_err()
            );
        }
        assert!(owner.effects().is_empty());
        assert!(log.names().is_empty());
        for value in [i64::MIN, i64::MAX] {
            assert_eq!(
                invoke_json(&handle, "identity", json!(value.to_string()))
                    .await
                    .unwrap(),
                json!(value.to_string())
            );
            let error = invoke_json(&handle, "run", json!(value.to_string()))
                .await
                .unwrap_err();
            assert!(error.to_string().contains("overflow"), "{error}");
        }
        assert_eq!(
            invoke_json(&handle, "run", json!(2)).await.unwrap(),
            json!("4")
        );
        let args = json!({"id":"one","behavior":"WRAP"});
        let (a, b) = tokio::join!(
            invoke_json(&handle, "register_handler", args.clone()),
            invoke_json(&handle, "register_handler", args)
        );
        assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
        let error = a.err().or_else(|| b.err()).unwrap();
        assert!(error.to_string().contains("duplicate_handler"), "{error}");
        assert_eq!(
            invoke_json(&handle, "run", json!(1)).await.unwrap(),
            json!("7")
        );
        invoke_json(&handle, "release", json!("one")).await.unwrap();
        assert!(
            invoke_json(
                &handle,
                "register_handler",
                json!({"id":"one","behavior":"WRAP"})
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("duplicate_handler")
        );
        bounded(rt.shutdown()).await.unwrap();
    }
}
