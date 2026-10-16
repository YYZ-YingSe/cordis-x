use super::effects::{
    Log, lifecycle_log_dependency as log_dependency, lifecycle_log_plugin as log_plugin,
    lifecycle_logger as logger,
};
use super::*;
use cordis_remote::Value;

fn pipeline() -> Service<RemoteService> {
    Service::new("example.pipeline", "1").with_descriptor(
        cordis_remote::Descriptor::new(
            cordis_portable_example::lifecycle::SequencesClient::interface(),
        )
        .unwrap(),
    )
}
fn spec(backend: Backend, endpoint: Endpoint) -> ProcessSpec {
    let mut spec = backend.spec("sequence");
    spec.endpoint = endpoint;

    if matches!(endpoint, Endpoint::Ipc) {
        spec.args.push("--stdio-only".into());
    }
    spec.requires(log_dependency())
}
async fn consumer(rt: &Runtime) -> (PluginHandle, ServiceHandle<RemoteService>) {
    consumer_for(rt, pipeline()).await
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
            Plugin::new("sequence caller", {
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
) -> Result<Value> {
    bounded(call(handle, endpoint, method, json!("run"))).await
}
async fn native_trace() -> Vec<String> {
    let rt = Runtime::new();
    let log = Log::new();
    rt.install(log_plugin(log.clone())).unwrap();
    let service = Service::<()>::new("native.pipeline", "1");
    let definition = service.clone();
    let provider = rt
        .install(
            Plugin::new("native pipeline", move |ctx| {
                let definition = definition.clone();
                async move {
                    ctx.defer("pipeline", |cleanup| async move {
                        cleanup.dependency(&logger())?.with(|log, ctx| {
                            log.events
                                .lock()
                                .unwrap()
                                .push(("pipeline.closed".into(), ctx.owner().fiber.0));
                            Ok(())
                        })?;
                        Ok(())
                    })?;
                    ctx.provide(&definition, Arc::new(()))
                }
            })
            .requires(&logger())
            .provides(&service),
        )
        .unwrap();
    bounded(provider.ready()).await.unwrap();
    let saved = Arc::new(Mutex::new(None));
    let client = rt
        .install(
            Plugin::new("native caller", {
                let saved = saved.clone();
                let service = service.clone();
                move |ctx| {
                    let saved = saved.clone();
                    let service = service.clone();
                    async move {
                        let mut n = 0;
                        let sequence = ctx.dependency(&service)?.with(|_, ctx| {
                            ctx.effect_steps("pipeline", move |step| {
                                n += 1;
                                let current = n;
                                async move {
                                    step.dependency(&logger())?.with(|log, ctx| {
                                        log.events.lock().unwrap().push((
                                            format!("run/step:{current}"),
                                            ctx.owner().fiber.0,
                                        ));
                                        Ok(())
                                    })?;
                                    let inverse =
                                        Inverse::new("buffer", move |cleanup| async move {
                                            cleanup.dependency(&logger())?.with(|log, ctx| {
                                                log.events.lock().unwrap().push((
                                                    format!("run/undo:{current}"),
                                                    ctx.owner().fiber.0,
                                                ));
                                                Ok(())
                                            })?;
                                            Ok(())
                                        });
                                    Ok(if current == 3 {
                                        EffectStep::complete(Some(inverse))
                                    } else {
                                        EffectStep::yielded(Some(inverse))
                                    })
                                }
                            })
                        })?;
                        sequence.ready().await?;
                        *saved.lock().unwrap() = Some(sequence);
                        Ok(())
                    }
                }
            })
            .requires(&service),
        )
        .unwrap();
    bounded(client.ready()).await.unwrap();
    let sequence = saved.lock().unwrap().take().unwrap();
    bounded(sequence.dispose()).await.unwrap();
    bounded(rt.shutdown()).await.unwrap();
    log.names()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequences_collect_the_final_inverse_and_keep_original_owner_across_all_endpoints() {
    let native = native_trace().await;
    for &backend in Backend::ALL {
        for endpoint in [Endpoint::Ipc, Endpoint::Http] {
            let rt = Runtime::new();
            let log = Log::new();
            let view = View::new().isolate(&logger());
            rt.install_in(view.clone(), log_plugin(log.clone()))
                .unwrap();
            let provider = rt
                .install_in(
                    view,
                    spec(backend, endpoint).plugin("pipeline", &pipeline()),
                )
                .unwrap();
            bounded(provider.ready()).await.unwrap();
            let (client, handle) = consumer(&rt).await;
            let (_, observer) = consumer(&rt).await;
            invoke_json(&handle, endpoint, "start").await.unwrap();
            assert_eq!(
                invoke_json(&observer, endpoint, "ready").await.unwrap(),
                json!({"completion":"completed"})
            );
            assert_eq!(
                invoke_json(&observer, endpoint, "count").await.unwrap(),
                json!("3")
            );
            assert_eq!(client.effects().len(), 1);
            invoke_json(&observer, endpoint, "release").await.unwrap();
            invoke_json(&observer, endpoint, "release").await.unwrap();
            assert_eq!(
                invoke_json(&observer, endpoint, "count").await.unwrap(),
                json!("0")
            );
            let status = invoke_json(&observer, endpoint, "status").await.unwrap();
            assert_eq!(status["steps"].as_array().unwrap().len(), 3);
            assert!(
                status["steps"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|s| s["state"] == "released")
            );
            let owner = client.snapshot().id.0;
            assert!(
                log.events
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|(_, id)| *id == owner)
            );
            bounded(client.dispose()).await.unwrap();
            assert_eq!(
                invoke_json(&observer, endpoint, "ready").await.unwrap(),
                json!({"completion":"completed"})
            );
            invoke_json(&observer, endpoint, "release").await.unwrap();
            bounded(rt.shutdown()).await.unwrap();
            assert_eq!(log.names(), native);
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_ready_waits_and_scope_stop_collect_the_late_step_but_never_start_the_next() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        *log.block.lock().unwrap() = Some("run/step:2".into());
        rt.install(log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Http).plugin("pipeline", &pipeline()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (client, handle) = consumer(&rt).await;
        let (_, observer) = consumer(&rt).await;
        invoke_json(&handle, Endpoint::Http, "start").await.unwrap();
        bounded(log.entered.acquire()).await.unwrap().forget();
        let waiter =
            tokio::spawn(async move { invoke_json(&handle, Endpoint::Http, "ready").await });
        tokio::task::yield_now().await;
        waiter.abort();
        let _ = waiter.await;
        let mut stopping = tokio::spawn(async move { client.dispose().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(40), &mut stopping)
                .await
                .is_err()
        );
        assert_eq!(log.names(), ["run/step:1", "run/step:2"]);
        log.release.add_permits(1);
        bounded(stopping).await.unwrap().unwrap();
        assert_eq!(
            invoke_json(&observer, Endpoint::Http, "ready")
                .await
                .unwrap(),
            json!({"completion":"stopped"})
        );
        assert_eq!(
            invoke_json(&observer, Endpoint::Http, "count")
                .await
                .unwrap(),
            json!("0")
        );
        bounded(rt.shutdown()).await.unwrap();
        assert_eq!(
            log.names(),
            [
                "run/step:1",
                "run/step:2",
                "run/undo:2",
                "run/undo:1",
                "pipeline.closed",
                "logger.closed"
            ]
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forward_error_retains_inverses_until_explicit_dispose_and_cleanup_failure_stops_earlier_recovery()
 {
    for &backend in Backend::ALL {
        for cleanup_failure in [false, true] {
            let rt = Runtime::new();
            let log = Log::new();
            *log.fail.lock().unwrap() = Some(
                if cleanup_failure {
                    "run/undo:2"
                } else {
                    "run/step:2"
                }
                .into(),
            );
            rt.install(log_plugin(log.clone())).unwrap();
            let provider = rt
                .install(spec(backend, Endpoint::Ipc).plugin("pipeline", &pipeline()))
                .unwrap();
            bounded(provider.ready()).await.unwrap();
            let (client, handle) = consumer(&rt).await;
            invoke_json(&handle, Endpoint::Ipc, "start").await.unwrap();
            assert_eq!(
                invoke_json(&handle, Endpoint::Ipc, "ready").await.is_ok(),
                cleanup_failure
            );
            assert!(!log.names().iter().any(|n| n.contains("undo")));
            for _ in 0..2 {
                let disposed = invoke_json(&handle, Endpoint::Ipc, "release").await;
                if cleanup_failure {
                    assert_eq!(
                        disposed.unwrap_err(),
                        cordis_remote::Error::Remote {
                            code: "cleanup_failed".into(),
                            message: "run/undo:2".into(),
                        }
                        .into()
                    );
                } else {
                    disposed.unwrap();
                }
            }
            if cleanup_failure {
                assert_eq!(
                    invoke_json(&handle, Endpoint::Ipc, "status").await.unwrap()["state"],
                    "failed"
                );
                assert!(matches!(
                    client.effects()[0].status,
                    EffectStatus::Failed(_)
                ));
                assert_eq!(
                    log.names(),
                    [
                        "run/step:1",
                        "run/step:2",
                        "run/step:3",
                        "run/undo:3",
                        "run/undo:2"
                    ]
                );
                assert!(bounded(provider.dispose()).await.is_err());
                assert!(bounded(rt.shutdown()).await.is_err());
            } else {
                assert_eq!(log.names(), ["run/step:1", "run/step:2", "run/undo:1"]);
                bounded(rt.shutdown()).await.unwrap();
            }
        }
    }
}

// Deliberately malformed wire peers have their own test descriptor.
fn probe_service() -> Service<RemoteService> {
    Service::new("example.pipeline", "1").with_descriptor(
        cordis_remote::Descriptor::new(cordis_remote::Interface::value_dispatch(true)).unwrap(),
    )
}
fn peer(mode: &str) -> (ProcessSpec, PathBuf) {
    let trace = std::env::temp_dir().join(format!(
        "cordis-sequences-{}-{}.log",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let mut spec = python_spec();
    spec.args.extend([
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/sequence_peer.py")
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
async fn sequence_protocol_distinguishes_rejection_from_failure_with_inverse_and_rejects_stale_authority()
 {
    for mode in ["normal", "reject", "forward_with_inverse", "stale"] {
        let rt = Runtime::new();
        let (spec, trace) = peer(mode);
        let provider = rt.install(spec.plugin("peer", &probe_service())).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (client, handle) = probe_consumer(&rt).await;
        invoke_json(&handle, Endpoint::Ipc, "start").await.unwrap();
        let ready = invoke_json(&handle, Endpoint::Ipc, "ready").await.unwrap();
        assert_eq!(
            ready["kind"],
            if mode == "reject" {
                "refused"
            } else if mode == "forward_with_inverse" {
                "failure"
            } else {
                "success"
            }
        );
        assert_eq!(
            invoke_json(&handle, Endpoint::Ipc, "duplicate")
                .await
                .unwrap()["error"]["code"],
            "invalid_argument"
        );
        assert_eq!(
            invoke_json(&handle, Endpoint::Ipc, "forged").await.unwrap()["error"]["code"],
            "closed"
        );
        let status = invoke_json(&handle, Endpoint::Ipc, "status").await.unwrap()["value"].clone();
        assert_eq!(
            status["steps"][1]["state"],
            if mode == "reject" {
                "not_executed"
            } else {
                "owned"
            }
        );
        for _ in 0..2 {
            assert_eq!(
                invoke_json(&handle, Endpoint::Ipc, "dispose")
                    .await
                    .unwrap()["kind"],
                "success"
            );
        }
        bounded(client.dispose()).await.unwrap();
        bounded(rt.shutdown()).await.unwrap();
        let text = std::fs::read_to_string(&trace).unwrap();
        if mode == "reject" {
            assert!(!text.contains("undo:2"));
        } else {
            assert!(text.contains("undo:2\nundo:1"));
        }
        assert!(text.ends_with("undo:1\nproducer_released\ndeactivated\n"));
        std::fs::remove_file(trace).unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_steps_and_lost_cleanup_keep_the_group_failed_and_block_earlier_inverses() {
    for mode in ["crash", "hang", "malformed", "undo_crash", "register_crash"] {
        let rt = Runtime::new();
        let (spec, trace) = peer(mode);
        let provider = rt.install(spec.plugin("peer", &probe_service())).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (client, handle) = probe_consumer(&rt).await;
        let _ = invoke_json(&handle, Endpoint::Ipc, "start").await;
        let _ = invoke_json(&handle, Endpoint::Ipc, "ready").await;
        if mode == "malformed" {
            let status = invoke_json(&handle, Endpoint::Ipc, "status").await.unwrap();
            assert_eq!(status["value"]["steps"][1]["state"], "unknown");
            let failed = invoke_json(&handle, Endpoint::Ipc, "dispose")
                .await
                .unwrap();
            assert_eq!(failed["kind"], "unknown");
            assert_eq!(
                invoke_json(&handle, Endpoint::Ipc, "dispose")
                    .await
                    .unwrap(),
                failed
            );
            let status = invoke_json(&handle, Endpoint::Ipc, "status").await.unwrap();
            assert_eq!(status["value"]["state"], "failed");
            assert_eq!(status["value"]["steps"][1]["state"], "unknown");
        }
        assert!(bounded(client.dispose()).await.is_err(), "{mode}");
        assert_eq!(client.effects().len(), 1, "{mode}");
        assert!(
            matches!(client.effects()[0].status, EffectStatus::Failed(_)),
            "{mode}"
        );
        assert!(bounded(provider.dispose()).await.is_err());
        assert!(bounded(rt.shutdown()).await.is_err());
        let text = std::fs::read_to_string(&trace).unwrap_or_default();
        assert!(!text.contains("undo:1"), "{mode}: {text}");
        assert!(!text.contains("producer_released"));
        assert!(!text.contains("deactivated"));
        let _ = std::fs::remove_file(trace);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispose_can_stop_a_running_step_and_a_cancelled_dispose_wait_does_not_cancel_recovery() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        *log.block.lock().unwrap() = Some("run/step:2".into());
        rt.install(log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc).plugin("pipeline", &pipeline()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (_, handle) = consumer(&rt).await;
        invoke_json(&handle, Endpoint::Ipc, "start").await.unwrap();
        bounded(log.entered.acquire()).await.unwrap().forget();
        let other = handle.clone();
        let disposing =
            tokio::spawn(async move { invoke_json(&other, Endpoint::Ipc, "release").await });
        bounded(async {
            loop {
                if invoke_json(&handle, Endpoint::Ipc, "status").await.unwrap()["state"]
                    == "releasing"
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        disposing.abort();
        let _ = disposing.await;
        log.release.add_permits(1);
        invoke_json(&handle, Endpoint::Ipc, "release")
            .await
            .unwrap();
        assert_eq!(
            invoke_json(&handle, Endpoint::Ipc, "ready").await.unwrap(),
            json!({"completion":"stopped"})
        );
        bounded(rt.shutdown()).await.unwrap();
        assert_eq!(
            log.names(),
            [
                "run/step:1",
                "run/step:2",
                "run/undo:2",
                "run/undo:1",
                "pipeline.closed",
                "logger.closed"
            ]
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequences_remain_recoverable_after_sdk_activation_fails() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let (_, trace) = peer("unused");
        let mut spec = backend.effect_fixture();
        spec.args
            .extend([trace.display().to_string(), "--sequence".into()]);
        let service = Service::<RemoteService>::new("example.subscriptions", "1").with_descriptor(
            cordis_remote::Descriptor::new(cordis_remote::Interface::value_dispatch(true)).unwrap(),
        );
        let provider = rt.install(spec.plugin("failed startup", &service)).unwrap();
        assert!(bounded(provider.ready()).await.is_err());
        assert!(bounded(provider.settled()).await.is_err());
        let trace_text = std::fs::read_to_string(&trace).unwrap();
        assert!(
            trace_text == "step\nreleased\ndeactivated\n" || trace_text == "deactivated\n",
            "{trace_text}"
        );
        let _ = bounded(rt.shutdown()).await;
        std::fs::remove_file(trace).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dependency_replacement_drains_old_sequences_before_closing_the_old_binding() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let generations = Arc::new(Mutex::new(Vec::<Arc<Log>>::new()));
        let logs = rt
            .install(
                Plugin::new("logger generations", {
                    let generations = generations.clone();
                    move |ctx| {
                        let log = Log::new();
                        {
                            let mut all = generations.lock().unwrap();
                            if all.is_empty() {
                                *log.block.lock().unwrap() = Some("run/step:2".into());
                            }
                            all.push(log.clone());
                        }
                        async move {
                            let cleanup = log.clone();
                            ctx.defer("logger", move |_| async move {
                                cleanup.open.store(false, Ordering::SeqCst);
                                cleanup
                                    .events
                                    .lock()
                                    .unwrap()
                                    .push(("logger.closed".into(), 0));
                                Ok(())
                            })?;
                            ctx.provide(&logger(), log)
                        }
                    }
                })
                .provides(&logger()),
            )
            .unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Http).plugin("pipeline", &pipeline()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (_, old) = consumer(&rt).await;
        invoke_json(&old, Endpoint::Http, "start").await.unwrap();
        let first = generations.lock().unwrap()[0].clone();
        bounded(first.entered.acquire()).await.unwrap().forget();
        let restarting = tokio::spawn(async move { logs.restart().await });
        bounded(async {
            while provider.snapshot().state != Lifecycle::Unloading {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(first.open.load(Ordering::SeqCst));
        assert_eq!(first.names(), ["run/step:1", "run/step:2"]);
        first.release.add_permits(1);
        bounded(restarting).await.unwrap().unwrap();
        bounded(provider.ready()).await.unwrap();
        assert_eq!(
            first.names(),
            [
                "run/step:1",
                "run/step:2",
                "run/undo:2",
                "run/undo:1",
                "pipeline.closed",
                "logger.closed"
            ]
        );
        assert!(invoke_json(&old, Endpoint::Http, "start").await.is_err());
        let (_, fresh) = consumer(&rt).await;
        invoke_json(&fresh, Endpoint::Http, "start").await.unwrap();
        assert_eq!(
            invoke_json(&fresh, Endpoint::Http, "ready").await.unwrap(),
            json!({"completion":"completed"})
        );
        bounded(rt.shutdown()).await.unwrap();
        assert_eq!(
            generations.lock().unwrap()[1].names(),
            [
                "run/step:1",
                "run/step:2",
                "run/step:3",
                "run/undo:3",
                "run/undo:2",
                "run/undo:1",
                "pipeline.closed",
                "logger.closed"
            ]
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_sequences_validate_before_start_and_keep_final_inverse_once() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        rt.install(log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc).plugin("pipeline", &pipeline()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle) = consumer(&rt).await;
        for value in [json!(42), json!({"name":"run"})] {
            assert!(call(&handle, Endpoint::Ipc, "start", value).await.is_err());
        }
        assert!(owner.effects().is_empty());
        assert!(log.names().is_empty());
        let (a, b) = tokio::join!(
            invoke_json(&handle, Endpoint::Ipc, "start"),
            invoke_json(&handle, Endpoint::Ipc, "start")
        );
        assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
        let error = a.err().or_else(|| b.err()).unwrap();
        assert!(error.to_string().contains("duplicate_sequence"), "{error}");
        assert_eq!(
            invoke_json(&handle, Endpoint::Ipc, "ready").await.unwrap(),
            json!({"completion":"completed"})
        );
        let status = invoke_json(&handle, Endpoint::Ipc, "status").await.unwrap();
        assert_eq!(
            status["forward"],
            json!({"succeeded":{"completion":"completed"}})
        );
        invoke_json(&handle, Endpoint::Ipc, "release")
            .await
            .unwrap();
        invoke_json(&handle, Endpoint::Ipc, "release")
            .await
            .unwrap();
        assert_eq!(
            invoke_json(&handle, Endpoint::Ipc, "count").await.unwrap(),
            json!("0")
        );
        assert_eq!(
            log.names(),
            [
                "run/step:1",
                "run/step:2",
                "run/step:3",
                "run/undo:3",
                "run/undo:2",
                "run/undo:1"
            ]
        );
        bounded(rt.shutdown()).await.unwrap();
    }
}
