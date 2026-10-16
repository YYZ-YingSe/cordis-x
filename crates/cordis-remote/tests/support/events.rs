use super::effects::{
    Log, lifecycle_log_dependency as log_dependency, lifecycle_log_plugin as log_plugin,
};
use super::*;
use cordis_remote::{EventBinding, Value};

fn events() -> Service<RemoteService> {
    Service::new("example.events", "1").with_descriptor(
        cordis_remote::Descriptor::new(
            cordis_portable_example::lifecycle::EventsClient::interface(),
        )
        .unwrap(),
    )
}
fn tick_body() -> cordis_remote::Bytes {
    cordis_sdk_core::Message::from_json(<cordis_portable_example::lifecycle::data::lifecycle::TickValue as cordis_sdk_core::typed::Codec>::contract().clone(),&json!({"value":"0"})).unwrap().encode(cordis_wire::MAX_MESSAGE).unwrap()
}
fn tick() -> Event<cordis_remote::Bytes, cordis_remote::Bytes> {
    Event::new("example.events.tick", "1")
}
fn spec(
    backend: Backend,
    endpoint: Endpoint,
    event: &Event<cordis_remote::Bytes, cordis_remote::Bytes>,
) -> ProcessSpec {
    let mut spec = backend.spec("events");
    if matches!(endpoint, Endpoint::Ipc) {
        spec.args.push("--stdio-only".into());
    }
    spec.endpoint = endpoint;

    spec.requires(log_dependency())
        .event(EventBinding::new("tick", event))
}
async fn consumer(rt: &Runtime) -> (PluginHandle, ServiceHandle<RemoteService>, Context) {
    consumer_for(rt, events()).await
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
            Plugin::new("event owner", {
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
async fn invoke_json(
    handle: &ServiceHandle<RemoteService>,
    method: &str,
    args: Value,
) -> Result<Value> {
    bounded(call(handle, Endpoint::Ipc, method, args)).await
}

async fn portable_events(endpoint: Endpoint) {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        rt.install(log_plugin(log.clone())).unwrap();
        let event = tick().in_realm(RealmId(72));
        let provider = rt
            .install(spec(backend, endpoint, &event).plugin("events", &events()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle, ctx) = consumer(&rt).await;
        for (id, behavior) in [("first", "BREAK"), ("second", "CONTINUE")] {
            bounded(call(
                &handle,
                endpoint,
                "listen",
                json!({"id":id,"behavior":behavior}),
            ))
            .await
            .unwrap();
        }
        assert_eq!(
            bounded(call(&handle, endpoint, "serial", json!({"value":"42"})))
                .await
                .unwrap(),
            json!({"stopped":{"value":"42"}})
        );
        assert_eq!(log.names(), ["first.enter"]);
        assert_eq!(
            bounded(call(&handle, endpoint, "parallel", json!({"value":"7"})))
                .await
                .unwrap(),
            json!({"values":[{"stopped":{"value":"7"}},{"continued":{}}]})
        );
        assert!(tick().parallel(&ctx, tick_body()).await.unwrap().is_empty());
        assert!(
            Event::<cordis_remote::Bytes, cordis_remote::Bytes>::new(
                "example.events.tick",
                "other"
            )
            .in_realm(RealmId(72))
            .parallel(&ctx, tick_body())
            .await
            .unwrap()
            .is_empty()
        );
        invoke_json(&handle, "unsubscribe", json!("first"))
            .await
            .unwrap();
        invoke_json(&handle, "unsubscribe", json!("first"))
            .await
            .unwrap();
        assert_eq!(
            event.parallel(&ctx, tick_body()).await.unwrap(),
            vec![Continue(())]
        );
        bounded(owner.dispose()).await.unwrap();
        bounded(rt.shutdown()).await.unwrap();
        assert_eq!(
            log.names()
                .iter()
                .filter(|s| s.as_str() == "first.cleanup")
                .count(),
            2
        );
        assert_eq!(
            log.names()
                .iter()
                .filter(|s| s.as_str() == "second.cleanup")
                .count(),
            2
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn portable_events_ipc_preserve_order_short_circuit_and_subscription_ownership() {
    portable_events(Endpoint::Ipc).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn portable_events_http_preserve_order_short_circuit_and_subscription_ownership() {
    portable_events(Endpoint::Http).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_emit_and_unsubscribe_waits_drain_late_callbacks() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        *log.block.lock().unwrap() = Some("first.enter".into());
        rt.install(log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc, &tick()).plugin("events", &events()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle, ctx) = consumer(&rt).await;
        invoke_json(
            &handle,
            "listen",
            json!({"id":"first","behavior":"CONTINUE"}),
        )
        .await
        .unwrap();
        invoke_json(&handle, "emit", json!({"value":"0"}))
            .await
            .unwrap();
        bounded(log.entered.acquire()).await.unwrap().forget();
        let waiting = tokio::spawn({
            let handle = handle.clone();
            async move { invoke_json(&handle, "join", json!({})).await }
        });
        waiting.abort();
        let _ = waiting.await;
        let disposal = tokio::spawn({
            let handle = handle.clone();
            async move { invoke_json(&handle, "unsubscribe", json!("first")).await }
        });
        bounded(async {
            while !owner
                .effects()
                .iter()
                .any(|e| e.label.starts_with("listener ") && e.status == EffectStatus::Releasing)
            {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(!disposal.is_finished());
        disposal.abort();
        let _ = disposal.await;
        assert!(!log.names().contains(&"first.cleanup".into()));
        log.release.add_permits(1);
        invoke_json(&handle, "unsubscribe", json!("first"))
            .await
            .unwrap();
        assert_eq!(
            invoke_json(&handle, "join", json!({})).await.unwrap(),
            json!({"values":[{"continued":{}}]})
        );
        assert!(tick().parallel(&ctx, tick_body()).await.unwrap().is_empty());
        bounded(owner.dispose()).await.unwrap();
        assert_eq!(
            log.names()
                .iter()
                .filter(|s| s.as_str() == "first.cleanup")
                .count(),
            1
        );
        bounded(rt.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_business_errors_aggregate_and_remain_observable_after_dropped_waits() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        rt.install(log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc, &tick()).plugin("events", &events()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (_, handle, _) = consumer(&rt).await;
        for id in ["a", "b"] {
            invoke_json(&handle, "listen", json!({"id":id,"behavior":"FAIL"}))
                .await
                .unwrap();
        }
        invoke_json(&handle, "emit", json!({"value":"0"}))
            .await
            .unwrap();
        let first = invoke_json(&handle, "join", json!({})).await.unwrap_err();
        assert_eq!(
            invoke_json(&handle, "join", json!({})).await.unwrap_err(),
            first
        );
        let status = invoke_json(&handle, "status", serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(status["failed"]["errors"].as_array().unwrap().len(), 2);
        assert_eq!(rt.diagnostics().len(), 1);
        assert!(
            invoke_json(&handle, "serial", json!({"value":"0"}))
                .await
                .is_err()
        );
        assert_eq!(
            log.names()
                .iter()
                .filter(|s| s.as_str() == "b.enter")
                .count(),
            1
        );
        bounded(rt.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn events_use_old_dependencies_through_callback_drain_and_restart() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        *log.block.lock().unwrap() = Some("first.enter".into());
        let logger = rt.install(log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc, &tick()).plugin("events", &events()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (_, handle, _) = consumer(&rt).await;
        invoke_json(
            &handle,
            "listen",
            json!({"id":"first","behavior":"CONTINUE"}),
        )
        .await
        .unwrap();
        invoke_json(&handle, "emit", json!({"value":"0"}))
            .await
            .unwrap();
        bounded(log.entered.acquire()).await.unwrap().forget();
        let stopping = tokio::spawn(async move { logger.dispose().await });
        bounded(async {
            while provider.snapshot().state != Lifecycle::Unloading {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(log.open.load(Ordering::SeqCst));
        assert!(!stopping.is_finished());
        log.release.add_permits(1);
        bounded(stopping).await.unwrap().unwrap();
        assert!(log.names().ends_with(&[
            "first.cleanup".into(),
            "events.closed".into(),
            "logger.closed".into()
        ]));
        rt.install(log_plugin(Log::new())).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (_, fresh, _) = consumer(&rt).await;
        assert_eq!(
            invoke_json(&fresh, "parallel", json!({"value":"0"}))
                .await
                .unwrap(),
            json!({})
        );
        bounded(rt.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_startup_failure_releases_registration_and_binding_mismatch_is_rejected() {
    for &backend in Backend::ALL {
        for fail in [false, true] {
            let rt = Runtime::new();
            rt.install(log_plugin(Log::new())).unwrap();
            let event = if fail {
                tick()
            } else {
                Event::new("wrong.event", "1")
            };
            let mut spec = spec(backend, Endpoint::Ipc, &event);
            if fail {
                spec.args.push("--fail-activate".into());
            }
            let provider = rt.install(spec.plugin("events", &events())).unwrap();
            assert!(bounded(provider.ready()).await.is_err());
            bounded(provider.dispose()).await.unwrap();
            bounded(rt.shutdown()).await.unwrap();
        }
    }
}

// Raw peers deliberately send malformed messages and forged capabilities.
fn probe_service() -> Service<RemoteService> {
    Service::new("example.events", "1").with_descriptor(
        cordis_remote::Descriptor::new(cordis_remote::Interface::value_dispatch(true)).unwrap(),
    )
}
fn probe_tick() -> Event<cordis_remote::Bytes, cordis_remote::Bytes> {
    Event::new("example.tick", "1")
}
fn peer(mode: &str) -> (ProcessSpec, PathBuf) {
    let trace = std::env::temp_dir().join(format!(
        "cordis-event-{}-{}.log",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let mut spec = python_spec();
    spec.args.extend([
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/event_peer.py")
            .display()
            .to_string(),
        mode.into(),
        trace.display().to_string(),
    ]);
    spec.timeout = Duration::from_millis(250);
    (spec.event(EventBinding::new("tick", &probe_tick())), trace)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_grants_aliases_stale_contexts_and_business_errors_obey_the_protocol() {
    for mode in ["finite", "reject", "business_error"] {
        let rt = Runtime::new();
        let (spec, trace) = peer(mode);
        let provider = rt.install(spec.plugin("peer", &probe_service())).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle, ctx) = probe_consumer(&rt).await;
        invoke_json(&handle, "open", Value::Null).await.unwrap();
        assert_eq!(
            bounded(probe_tick().parallel(&ctx, wire_json(Value::Null)))
                .await
                .is_ok(),
            mode == "finite"
        );
        for method in ["forged", "duplicate", "stale", "unbound", "forged_cleanup"] {
            assert_eq!(
                invoke_json(&handle, method, Value::Null).await.unwrap()["kind"],
                "refused"
            );
        }
        for _ in 0..2 {
            assert_eq!(
                invoke_json(&handle, "close", Value::Null).await.unwrap()["kind"],
                "success"
            );
        }
        assert!(
            probe_tick()
                .parallel(&ctx, wire_json(Value::Null))
                .await
                .unwrap()
                .is_empty()
        );
        bounded(owner.dispose()).await.unwrap();
        bounded(rt.shutdown()).await.unwrap();
        let text = std::fs::read_to_string(&trace).unwrap();
        assert_eq!(text.lines().filter(|s| *s == "released").count(), 1);
        assert_eq!(text.contains("cleanup"), mode != "reject");
        std::fs::remove_file(trace).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_event_callback_outcomes_block_earlier_cleanup() {
    for mode in ["crash", "hang", "malformed"] {
        let rt = Runtime::new();
        let (spec, trace) = peer(mode);
        let provider = rt.install(spec.plugin("peer", &probe_service())).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle, ctx) = probe_consumer(&rt).await;
        invoke_json(&handle, "open", Value::Null).await.unwrap();
        assert!(
            bounded(probe_tick().parallel(&ctx, wire_json(Value::Null)))
                .await
                .is_err()
        );
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
async fn event_registration_ack_loss_never_leaves_an_unowned_subscription() {
    let rt = Runtime::new();
    let (spec, trace) = peer("register_crash");
    let provider = rt.install(spec.plugin("peer", &probe_service())).unwrap();
    bounded(provider.ready()).await.unwrap();
    let (owner, handle, _) = probe_consumer(&rt).await;
    assert!(invoke_json(&handle, "open", Value::Null).await.is_err());
    let _ = bounded(owner.dispose()).await;
    assert!(bounded(provider.dispose()).await.is_err());
    assert!(bounded(rt.shutdown()).await.is_err());
    let _ = std::fs::remove_file(trace);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_event_callback_capacity_can_drain_dependencies_and_close() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        *log.block.lock().unwrap() = Some("first.enter".into());
        rt.install(log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc, &tick()).plugin("events", &events()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle, ctx) = consumer(&rt).await;
        invoke_json(
            &handle,
            "listen",
            json!({"id":"first","behavior":"CONTINUE"}),
        )
        .await
        .unwrap();
        let mut dispatches = Vec::new();
        for _ in 0..16 {
            dispatches.push(tick().emit(&ctx, tick_body()).unwrap());
        }
        bounded(log.entered.acquire_many(16))
            .await
            .unwrap()
            .forget();
        assert!(bounded(tick().parallel(&ctx, tick_body())).await.is_err());
        let stopping = tokio::spawn(async move { owner.dispose().await });
        log.release.add_permits(16);
        for dispatch in dispatches {
            bounded(dispatch.join()).await.unwrap();
        }
        bounded(stopping).await.unwrap().unwrap();
        assert_eq!(
            log.names()
                .iter()
                .filter(|s| s.as_str() == "first.cleanup")
                .count(),
            16
        );
        bounded(rt.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_event_dispatches_have_capacity_for_callbacks_and_joins() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        rt.install(log_plugin(Log::new())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc, &tick()).plugin("events", &events()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (_, handle, _) = consumer(&rt).await;
        invoke_json(
            &handle,
            "listen",
            json!({"id":"nested","behavior":"NESTED"}),
        )
        .await
        .unwrap();
        let mut waits = Vec::new();
        for _ in 0..8 {
            let handle = handle.clone();
            waits.push(tokio::spawn(async move {
                invoke_json(&handle, "parallel", json!({"value":"1"})).await
            }));
        }
        for wait in waits {
            assert_eq!(
                bounded(wait).await.unwrap().unwrap(),
                json!({"values":[{"stopped":{"nested":{"values":[{"continued":{}}]}}}]})
            );
        }
        bounded(rt.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_event_inputs_are_isolated_and_duplicate_registration_is_atomic() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Log::new();
        rt.install(log_plugin(log.clone())).unwrap();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc, &tick()).plugin("events", &events()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle, _) = consumer(&rt).await;
        for args in [
            json!({}),
            json!({"id":"one"}),
            json!({"id":"one","behavior":999}),
            json!({"id":"one","behavior":"BREAK","unknown":true}),
        ] {
            assert!(invoke_json(&handle, "listen", args).await.is_err());
        }
        assert!(owner.effects().is_empty());
        assert!(log.names().is_empty());
        let args = json!({"id":"one","behavior":"BREAK"});
        let (a, b) = tokio::join!(
            invoke_json(&handle, "listen", args.clone()),
            invoke_json(&handle, "listen", args)
        );
        assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
        let error = a.err().or_else(|| b.err()).unwrap();
        assert!(error.to_string().contains("duplicate_listener"), "{error}");
        for value in [i64::MIN, i64::MAX] {
            assert_eq!(
                invoke_json(&handle, "serial", json!({"value":value.to_string()}))
                    .await
                    .unwrap(),
                json!({"stopped":{"value":value.to_string()}})
            );
        }
        assert_eq!(
            invoke_json(&handle, "count", json!({})).await.unwrap(),
            json!("2")
        );
        for args in [json!({}), json!({"value":"9223372036854775808"})] {
            assert!(invoke_json(&handle, "serial", args).await.is_err());
        }
        assert_eq!(
            invoke_json(&handle, "count", json!({})).await.unwrap(),
            json!("2")
        );
        invoke_json(&handle, "unsubscribe", json!("one"))
            .await
            .unwrap();
        assert!(
            invoke_json(&handle, "listen", json!({"id":"one","behavior":"BREAK"}))
                .await
                .unwrap_err()
                .to_string()
                .contains("duplicate_listener")
        );
        bounded(rt.shutdown()).await.unwrap();
        assert_eq!(
            log.names()
                .iter()
                .filter(|name| *name == "one.cleanup")
                .count(),
            2
        );
    }
}
