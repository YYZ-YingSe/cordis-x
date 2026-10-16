include!("support/context.rs");
use cordis_abi::{EventBinding, MiddlewareBinding};
fn event() -> Event<cordis_abi::Bytes, cordis_abi::Bytes> {
    Event::new("example.tick", "1")
}
fn chain() -> Middleware<cordis_abi::Bytes, cordis_abi::Bytes> {
    Middleware::new("example.chain", "1")
}
fn reactive_caps() -> Capabilities {
    Capabilities {
        events: vec![EventBinding::new("tick", &event())],
        middleware: vec![MiddlewareBinding::new("chain", &chain())],
        ..caps()
    }
}
async fn entered(logger: &Logger) {
    bounded(logger.entered.acquire()).await.unwrap().forget();
}
fn rejected(error: AbiError, expected: &str) {
    assert!(
        matches!(&error, AbiError::Refused{code,..} if code == expected),
        "{error:?}"
    );
}

#[test]
fn negotiation_and_binding_mismatches_fail_before_creation() {
    for kind in 1..=6 {
        // SAFETY: fixtures provide valid native memory with deliberately invalid metadata.
        #[allow(unsafe_code)]
        let result = unsafe {
            Library::load(artifacts::binary(&format!(
                "cordis_abi_bad_reactive_{kind}"
            )))
        };
        assert!(result.is_err(), "kind {kind}");
    }
    let files = Files::new("cordis_abi_reactive");
    assert!(files.load().supports_reactive());
    assert!(!Files::new("cordis_abi_managed").load().supports_reactive());
    assert!(files.load().with_context(caps()).is_err());
    let mut config = reactive_caps();
    config.events.push(EventBinding::new("tick", &event()));
    assert!(files.load().with_context(config).is_err());
    let mut config = reactive_caps();
    config.middleware = vec![MiddlewareBinding::new(
        "chain",
        &Middleware::new("example.chain", "2"),
    )];
    assert!(files.load().with_context(config).is_err());
    assert_eq!(files.count("create"), 0);
}

#[tokio::test]
async fn events_preserve_order_short_circuit_and_independent_unsubscription() {
    let files = Files::new("cordis_abi_reactive");
    let rig = rig(&files, cordis_wire::json!({}), reactive_caps()).await;
    for args in [
        json!({"id":"a"}),
        json!({"id":"b","break":42}),
        json!({"id":"c"}),
    ] {
        invoke_json(&rig.handle, "on", args).await.unwrap();
    }
    assert_eq!(
        invoke_json(&rig.handle, "parallel", json!(7))
            .await
            .unwrap(),
        json!([{"kind":"continue"},{"kind":"break","value":42.0},{"kind":"continue"}])
    );
    assert_eq!(
        invoke_json(&rig.handle, "serial", json!(8)).await.unwrap(),
        json!({"kind":"break","value":42.0})
    );
    assert_eq!(files.count("event:c"), 1);
    let (a, b) = tokio::join!(
        invoke_json(&rig.handle, "dispose", json!(1)),
        invoke_json(&rig.handle, "dispose", json!(1))
    );
    a.unwrap();
    b.unwrap();
    assert_eq!(files.count("released:b"), 1);
    assert_eq!(
        invoke_json(&rig.handle, "serial", Value::Null)
            .await
            .unwrap(),
        json!({"kind":"continue"})
    );
    rejected(
        invoke_json(&rig.handle, "expired", Value::Null)
            .await
            .unwrap_err(),
        "closed",
    );
    bounded(rig.client.dispose()).await.unwrap();
    assert_eq!(files.count("released:a"), 1);
    assert_eq!(files.count("released:c"), 1);
    bounded(rig.rt.shutdown()).await.unwrap();
    drop(rig);
    files.wait("unload").await;
}

#[tokio::test]
async fn event_callback_uses_registration_owner_and_emitter_caller() {
    let files = Files::new("cordis_abi_reactive");
    let rig = rig(&files, cordis_wire::json!({}), reactive_caps()).await;
    let service = Service::<AbiService>::new("example.context", "1").with_descriptor(
        cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true)).unwrap(),
    );
    let (emitter, emitter_handle) = client(&rig.rt, &service).await;
    invoke_json(&rig.handle, "on", json!({"late":true}))
        .await
        .unwrap();
    invoke_json(&emitter_handle, "parallel", json!(9))
        .await
        .unwrap();
    let records = rig.logger.records("event");
    assert_eq!(records[0].owner.fiber, rig.client.id());
    // The logger sees ABI as its immediate caller; use-side ownership stays with listener.
    assert_eq!(records[0].caller.unwrap().fiber, rig.provider.id());
    assert!(!records[0].cleanup);
    bounded(emitter.dispose()).await.unwrap();
    assert_eq!(files.count("late-cleanup"), 0);
    bounded(rig.client.dispose()).await.unwrap();
    assert_eq!(files.count("late-cleanup"), 1);
    assert!(rig.logger.records("cleanup")[0].cleanup);
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn dispatch_handle_outlives_method_and_caches_outcome() {
    let files = Files::new("cordis_abi_reactive");
    let rig = rig(&files, cordis_wire::json!({}), reactive_caps()).await;
    invoke_json(&rig.handle, "on", json!({"hold":true}))
        .await
        .unwrap();
    let id = invoke_json(&rig.handle, "emit", json!({"input":12}))
        .await
        .unwrap();
    entered(&rig.logger).await;
    assert_eq!(
        invoke_json(&rig.handle, "status", id.clone())
            .await
            .unwrap()["outcome"],
        Value::Null
    );
    let waiter = tokio::spawn({
        let h = rig.handle.clone();
        let id = id.clone();
        async move { invoke_json(&h, "join", id).await }
    });
    tokio::task::yield_now().await;
    waiter.abort();
    let _ = waiter.await;
    rig.logger.gate.add_permits(1);
    let result = bounded(invoke_json(&rig.handle, "join", id.clone()))
        .await
        .unwrap();
    assert_eq!(
        bounded(invoke_json(&rig.handle, "join", id.clone()))
            .await
            .unwrap(),
        result
    );
    assert_eq!(
        invoke_json(&rig.handle, "status", id).await.unwrap()["outcome"]["kind"],
        "success"
    );
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn cancelled_unsubscribe_still_drains_callback_and_late_effect() {
    let files = Files::new("cordis_abi_reactive");
    let rig = rig(&files, cordis_wire::json!({}), reactive_caps()).await;
    invoke_json(
        &rig.handle,
        "on",
        json!({"id":"held","hold":true,"late":true}),
    )
    .await
    .unwrap();
    let dispatch = invoke_json(&rig.handle, "emit", json!({"input":1}))
        .await
        .unwrap();
    entered(&rig.logger).await;
    let unsubscribe = tokio::spawn({
        let h = rig.handle.clone();
        async move { invoke_json(&h, "dispose", json!(0)).await }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!unsubscribe.is_finished());
    unsubscribe.abort();
    let _ = unsubscribe.await;
    assert_eq!(files.count("released:held"), 0);
    let close = tokio::spawn({
        let p = rig.client.clone();
        async move { p.dispose().await }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!close.is_finished());
    rig.logger.gate.add_permits(1);
    bounded(close).await.unwrap().unwrap();
    assert_eq!(files.count("released:held"), 1);
    assert_eq!(files.count("late-cleanup"), 1);
    drop(dispatch);
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn event_errors_are_aggregated_cached_and_do_not_fault_known_callbacks() {
    let files = Files::new("cordis_abi_reactive");
    let rig = rig(&files, cordis_wire::json!({}), reactive_caps()).await;
    for id in ["a", "b"] {
        invoke_json(&rig.handle, "on", json!({"id":id,"reject":true}))
            .await
            .unwrap();
    }
    let dispatch = invoke_json(&rig.handle, "emit", cordis_wire::json!({}))
        .await
        .unwrap();
    let first = bounded(invoke_json(&rig.handle, "join", dispatch.clone()))
        .await
        .unwrap_err();
    assert_eq!(
        invoke_json(&rig.handle, "join", dispatch)
            .await
            .unwrap_err(),
        first
    );
    assert_eq!(files.count("event:a"), 1);
    assert_eq!(files.count("event:b"), 1);
    assert_eq!(rig.provider.snapshot().state, Lifecycle::Active);
    assert!(!rig.rt.diagnostics().is_empty());
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn initialization_listeners_are_opt_in_and_startup_failure_recovers() {
    for enabled in [false, true] {
        let files = Files::new("cordis_abi_reactive");
        let rig = rig(&files, json!({"initializing":enabled}), reactive_caps()).await;
        assert_eq!(files.count("event:initializing"), usize::from(enabled));
        bounded(rig.rt.shutdown()).await.unwrap();
    }
    let files = Files::new("cordis_abi_reactive");
    let rt = Runtime::new();
    install_logger(&rt, Logger::new(), View::new()).await;
    let library = files.load().with_context(reactive_caps()).unwrap();
    let service = Service::new("example.context", "1").with_descriptor(
        cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true)).unwrap(),
    );
    let provider = rt
        .install(
            library
                .plugin("startup", &service, files.config(json!({"startup":true})))
                .unwrap(),
        )
        .unwrap();
    assert!(bounded(provider.ready()).await.is_err());
    let _ = bounded(provider.dispose()).await;
    assert_eq!(files.count("released:startup"), 1);
    assert_eq!(files.count("released:startup-middleware"), 1);
    assert_eq!(files.count("destroy"), 1);
    let _ = bounded(rt.shutdown()).await;
}

#[tokio::test]
async fn middleware_transforms_wraps_and_rejects_duplicate_and_expired_next() {
    let files = Files::new("cordis_abi_reactive");
    let rig = rig(&files, cordis_wire::json!({}), reactive_caps()).await;
    invoke_json(
        &rig.handle,
        "use",
        json!({"id":"outer","add":1,"multiply":10,"duplicate":true}),
    )
    .await
    .unwrap();
    invoke_json(
        &rig.handle,
        "use",
        json!({"id":"inner","add":2,"multiply":2}),
    )
    .await
    .unwrap();
    assert_eq!(
        invoke_json(&rig.handle, "run", json!(3)).await.unwrap(),
        json!(120.0)
    );
    assert_eq!(rig.logger.records("duplicate")[0].args, "duplicate_next");
    rejected(
        invoke_json(&rig.handle, "saved-next", json!(0))
            .await
            .unwrap_err(),
        "closed",
    );
    rejected(
        invoke_json(&rig.handle, "expired", Value::Null)
            .await
            .unwrap_err(),
        "closed",
    );
    invoke_json(&rig.handle, "dispose", json!(0)).await.unwrap();
    assert_eq!(
        invoke_json(&rig.handle, "run", json!(3)).await.unwrap(),
        json!(10.0)
    );
    bounded(rig.rt.shutdown()).await.unwrap();
    assert_eq!(files.count("terminal-cleanup"), 2);
}

#[tokio::test]
async fn middleware_short_circuit_and_known_failures_release_terminal_callbacks() {
    for args in [json!({"short":42}), json!({"reject":true})] {
        let files = Files::new("cordis_abi_reactive");
        let rig = rig(&files, cordis_wire::json!({}), reactive_caps()).await;
        invoke_json(&rig.handle, "use", args.clone()).await.unwrap();
        let result = invoke_json(&rig.handle, "run", json!(1)).await;
        if args.get("short").is_some() {
            assert_eq!(result.unwrap(), json!(42.0));
        } else {
            assert!(result.is_err());
        }
        assert_eq!(files.count("terminal"), 0);
        bounded(rig.rt.shutdown()).await.unwrap();
        assert_eq!(files.count("destroy"), 1);
    }
    let files = Files::new("cordis_abi_reactive");
    let rig = rig(&files, json!({"reject_terminal":true}), reactive_caps()).await;
    assert!(invoke_json(&rig.handle, "run", json!(1)).await.is_err());
    bounded(rig.rt.shutdown()).await.unwrap();
    assert_eq!(files.count("destroy"), 1);
}

#[tokio::test]
async fn cancelled_chain_keeps_downstream_and_unregister_pending_until_return() {
    let files = Files::new("cordis_abi_reactive");
    let rig = rig(&files, json!({"hold_terminal":true}), reactive_caps()).await;
    invoke_json(&rig.handle, "use", json!({"id":"held"}))
        .await
        .unwrap();
    let call = tokio::spawn({
        let h = rig.handle.clone();
        async move { invoke_json(&h, "run", json!(2)).await }
    });
    entered(&rig.logger).await;
    call.abort();
    let _ = call.await;
    let dispose = tokio::spawn({
        let h = rig.handle.clone();
        async move { invoke_json(&h, "dispose", json!(0)).await }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!dispose.is_finished());
    assert_eq!(files.count("released:held"), 0);
    rig.logger.gate.add_permits(1);
    bounded(dispose).await.unwrap().unwrap();
    assert_eq!(files.count("after:held"), 1);
    bounded(rig.rt.shutdown()).await.unwrap();
    assert_eq!(files.count("terminal-cleanup"), 1);
}

#[tokio::test]
async fn unknown_event_middleware_and_terminal_retain_failed_responsibility_and_code() {
    for mode in ["event", "middleware", "terminal"] {
        let files = Files::new("cordis_abi_reactive");
        let rig = rig(
            &files,
            json!({"unknown_terminal":mode=="terminal"}),
            reactive_caps(),
        )
        .await;
        invoke_json(&rig.handle, "before", Value::Null)
            .await
            .unwrap();
        if mode != "terminal" {
            invoke_json(
                &rig.handle,
                if mode == "event" { "on" } else { "use" },
                json!({"unknown":true}),
            )
            .await
            .unwrap();
        }
        assert!(
            bounded(invoke_json(
                &rig.handle,
                if mode == "event" { "parallel" } else { "run" },
                json!(1)
            ))
            .await
            .is_err()
        );
        assert!(bounded(rig.rt.shutdown()).await.is_err());
        drop(rig);
        assert_eq!(files.count("older"), 0, "{mode}");
        assert_eq!(files.count("destroy"), 0, "{mode}");
        assert_eq!(files.count("unload"), 0, "{mode}");
    }
}

#[tokio::test]
async fn subscription_dispatch_run_and_callback_limits_do_not_orphan_captures() {
    let files = Files::new("cordis_abi_reactive");
    let mut capabilities = reactive_caps();
    capabilities.reactive.subscriptions = 1;
    capabilities.reactive.dispatches = 1;
    capabilities.reactive.runs = 1;
    capabilities.reactive.calls = 1;
    let rig = rig(&files, cordis_wire::json!({}), capabilities).await;
    invoke_json(&rig.handle, "on", json!({"id":"a"}))
        .await
        .unwrap();
    rejected(
        invoke_json(&rig.handle, "on", json!({"id":"b"}))
            .await
            .unwrap_err(),
        "overloaded",
    );
    let d = invoke_json(&rig.handle, "emit", cordis_wire::json!({}))
        .await
        .unwrap();
    invoke_json(&rig.handle, "join", d).await.unwrap();
    rejected(
        invoke_json(&rig.handle, "emit", cordis_wire::json!({}))
            .await
            .unwrap_err(),
        "overloaded",
    );
    assert!(invoke_json(&rig.handle, "run", json!(1)).await.is_err());
    rejected(
        invoke_json(&rig.handle, "run", json!(1)).await.unwrap_err(),
        "overloaded",
    );
    rejected(
        invoke_json(&rig.handle, "on", json!({"alias":"missing"}))
            .await
            .unwrap_err(),
        "unbound",
    );
    bounded(rig.rt.shutdown()).await.unwrap();
    assert_eq!(files.count("destroy"), 1);
}

#[tokio::test]
async fn nested_callbacks_have_separate_terminal_capacity_and_self_disposal_is_rejected() {
    let files = Files::new("cordis_abi_reactive");
    let mut capabilities = reactive_caps();
    capabilities.reactive.active_callbacks = 4;
    capabilities.max_contexts = 4;
    let rig = rig(&files, cordis_wire::json!({}), capabilities).await;
    invoke_json(&rig.handle, "on", json!({"nested":true,"self":true}))
        .await
        .unwrap();
    invoke_json(&rig.handle, "parallel", json!(2))
        .await
        .unwrap();
    assert_eq!(rig.logger.records("self_wait").len(), 3);
    assert!(
        rig.logger
            .records("self_wait")
            .iter()
            .all(|r| r.args == "self_wait")
    );
    for n in 0..4 {
        invoke_json(&rig.handle, "use", json!({"id":format!("handler{n}")}))
            .await
            .unwrap();
    }
    assert_eq!(
        invoke_json(&rig.handle, "run", json!(0)).await.unwrap(),
        json!(4.0)
    );
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn host_bound_realms_are_isolated() {
    let files = Files::new("cordis_abi_reactive");
    let other = RealmId(913);
    let mut capabilities = reactive_caps();
    capabilities.events = vec![EventBinding::new("tick", &event().in_realm(other))];
    let rig = rig(&files, cordis_wire::json!({}), capabilities).await;
    invoke_json(&rig.handle, "on", json!({"id":"isolated"}))
        .await
        .unwrap();
    let saved = Arc::new(Mutex::new(None));
    let emitter = rig
        .rt
        .install(Plugin::new("emitter", {
            let saved = saved.clone();
            move |ctx| {
                let saved = saved.clone();
                async move {
                    *saved.lock().unwrap() = Some(ctx);
                    Ok(())
                }
            }
        }))
        .unwrap();
    emitter.ready().await.unwrap();
    let ctx = saved.lock().unwrap().clone().unwrap();
    assert!(
        event()
            .parallel(&ctx, wire_json(json!(1)))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        event()
            .in_realm(other)
            .parallel(&ctx, wire_json(json!(1)))
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(files.count("event:isolated"), 1);
    bounded(rig.rt.shutdown()).await.unwrap();
}

async fn route(
    handle: &ServiceHandle<Broker<AbiService>>,
    method: &str,
    args: Value,
) -> Result<Value> {
    let method = method.to_owned();
    handle
        .route(move |s, ctx| {
            Box::pin(async move { s.invoke_json(&ctx, &method, args).await.map_err(Into::into) })
        })
        .await
}
#[tokio::test]
async fn broker_old_subscriptions_keep_definition_dependencies_and_code_after_switch() {
    let old = Files::new("cordis_abi_reactive");
    let new = Files::new("cordis_abi_reactive_v2");
    let rt = Runtime::new();
    let old_logger = Logger::new();
    let new_logger = Logger::new();
    let old_view = View::new().isolate(&logger_service());
    install_logger(&rt, old_logger.clone(), old_view.clone()).await;
    install_logger(&rt, new_logger.clone(), View::new()).await;
    let service = Service::<Broker<AbiService>>::new("example.context", "1").with_descriptor(
        cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true)).unwrap(),
    );
    let declaration = service.clone();
    let broker = rt
        .install(
            Plugin::new("broker", move |ctx| {
                let service = declaration.clone();
                async move { ctx.provide_broker(&service) }
            })
            .provides(&service),
        )
        .unwrap();
    broker.ready().await.unwrap();
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let register = |files: &Files, name: &str| {
        let registrations = registrations.clone();
        files
            .load()
            .with_context(reactive_caps())
            .unwrap()
            .broker_plugin(
                name,
                &service,
                files.config(cordis_wire::json!({})),
                move |candidate| {
                    registrations.lock().unwrap().push(candidate);
                    Ok(())
                },
            )
            .unwrap()
    };
    let v1 = rt.install_in(old_view, register(&old, "v1")).unwrap();
    v1.ready().await.unwrap();
    registrations.lock().unwrap()[0].promote().unwrap();
    let (consumer, handle) = client(&rt, &service).await;
    let activation = consumer.snapshot().activation;
    route(&handle, "on", json!({"id":"old","late":true}))
        .await
        .unwrap();
    route(&handle, "use", json!({"id":"old-middleware","add":4}))
        .await
        .unwrap();
    let v2 = rt.install(register(&new, "v2")).unwrap();
    v2.ready().await.unwrap();
    registrations.lock().unwrap()[1].promote().unwrap();
    assert_eq!(route(&handle, "version", Value::Null).await.unwrap(), "v2");
    let retiring = tokio::spawn({
        let v1 = v1.clone();
        async move { v1.dispose().await }
    });
    bounded(async {
        while v1.snapshot().state != Lifecycle::Unloading {
            tokio::task::yield_now().await;
        }
    })
    .await;
    route(&handle, "parallel", json!(3)).await.unwrap();
    assert_eq!(route(&handle, "run", json!(1)).await.unwrap(), json!(5.0));
    assert_eq!(old_logger.records("event")[0].args["version"], "v1");
    assert_eq!(old_logger.records("middleware")[0].args, "v1");
    assert!(new_logger.records("event").is_empty());
    assert_eq!(consumer.snapshot().activation, activation);
    assert!(!retiring.is_finished());
    assert_eq!(old.count("unload"), 0);
    bounded(consumer.dispose()).await.unwrap();
    bounded(retiring).await.unwrap().unwrap();
    assert_eq!(old_logger.records("cleanup")[0].args, "v1");
    assert_eq!(old.count("released:old"), 1);
    assert_eq!(old.count("released:old-middleware"), 1);
    drop(registrations.lock().unwrap().remove(0));
    drop(v1);
    old.wait("unload").await;
    bounded(rt.shutdown()).await.unwrap();
}

#[cfg(feature = "loader")]
#[tokio::test]
async fn loader_preflights_reactive_bindings_and_runs_pinned_callbacks() {
    use cordis_abi::loader::register_context_library;
    use cordis_loader::{Configuration, Entry, Loader, Registry};
    let files = Files::new("cordis_abi_reactive");
    let rt = Runtime::new();
    install_logger(&rt, Logger::new(), View::new()).await;
    let service = Service::<AbiService>::new("example.context", "1").with_descriptor(
        cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true)).unwrap(),
    );
    let options = artifacts::options(files.root.join("staged"));
    let artifact = artifacts::package(&files.binary);
    let mut registry = Registry::new();
    // SAFETY: trusted repository fixture and host-owned staging directory.
    #[allow(unsafe_code)]
    unsafe {
        register_context_library(
            &mut registry,
            "library",
            service.clone(),
            options.clone(),
            reactive_caps(),
        )
        .unwrap();
        register_context_library(&mut registry, "missing", service.clone(), options, caps())
            .unwrap();
    }
    let loader = Loader::new(rt.clone(), registry);
    assert!(
        loader
            .apply(Configuration {
                entries: vec![
                    Entry::new("bad", "missing", files.config(cordis_wire::json!({})))
                        .artifact(artifact.clone())
                ]
            })
            .await
            .is_err()
    );
    assert_eq!(files.count("create"), 0);
    loader
        .apply(Configuration {
            entries: vec![
                Entry::new("good", "library", files.config(cordis_wire::json!({})))
                    .artifact(artifact),
            ],
        })
        .await
        .unwrap();
    bounded(loader.settled()).await.unwrap();
    let (_, handle) = client(&rt, &service).await;
    invoke_json(&handle, "on", json!({"id":"loader"}))
        .await
        .unwrap();
    invoke_json(&handle, "use", json!({"id":"loader-middleware"}))
        .await
        .unwrap();
    invoke_json(&handle, "parallel", json!(1)).await.unwrap();
    assert_eq!(
        invoke_json(&handle, "run", json!(1)).await.unwrap(),
        json!(2.0)
    );
    loader.apply(Configuration::default()).await.unwrap();
    assert_eq!(files.count("released:loader"), 1);
    assert_eq!(files.count("released:loader-middleware"), 1);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn full_callback_capacity_drains_without_using_cleanup_or_join_slots() {
    let files = Files::new("cordis_abi_reactive");
    let mut capabilities = reactive_caps();
    capabilities.max_contexts = 2;
    capabilities.max_callbacks = 2;
    capabilities.reactive.active_callbacks = 2;
    let rig = rig(&files, cordis_wire::json!({}), capabilities).await;
    for n in 0..2 {
        invoke_json(
            &rig.handle,
            "on",
            json!({"id":format!("held{n}"),"hold":true,"late":true}),
        )
        .await
        .unwrap();
    }
    let dispatch = invoke_json(&rig.handle, "emit", cordis_wire::json!({}))
        .await
        .unwrap();
    entered(&rig.logger).await;
    entered(&rig.logger).await;
    let close = tokio::spawn({
        let p = rig.provider.clone();
        async move { p.dispose().await }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!close.is_finished());
    rig.logger.gate.add_permits(2);
    bounded(close).await.unwrap().unwrap();
    assert_eq!(files.count("late-cleanup"), 2);
    assert_eq!(files.count("destroy"), 1);
    drop(dispatch);
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn failed_callback_contribution_blocks_provider_and_older_effects() {
    let files = Files::new("cordis_abi_reactive");
    let rig = rig(&files, cordis_wire::json!({}), reactive_caps()).await;
    invoke_json(&rig.handle, "before", Value::Null)
        .await
        .unwrap();
    invoke_json(&rig.handle, "on", json!({"late":true,"fail_cleanup":true}))
        .await
        .unwrap();
    invoke_json(&rig.handle, "parallel", json!(1))
        .await
        .unwrap();
    assert!(bounded(rig.client.dispose()).await.is_err());
    assert!(bounded(rig.client.dispose()).await.is_err());
    assert_eq!(files.count("late-cleanup"), 1);
    assert_eq!(files.count("older"), 0);
    assert!(bounded(rig.rt.shutdown()).await.is_err());
    drop(rig);
    assert_eq!(files.count("destroy"), 0);
    assert_eq!(files.count("unload"), 0);
}

#[tokio::test]
async fn active_next_rejects_a_different_context_grant() {
    let files = Files::new("cordis_abi_reactive");
    let rig = rig(&files, cordis_wire::json!({}), reactive_caps()).await;
    invoke_json(&rig.handle, "use", json!({"hold":true}))
        .await
        .unwrap();
    let running = tokio::spawn({
        let h = rig.handle.clone();
        async move { invoke_json(&h, "run", json!(1)).await }
    });
    entered(&rig.logger).await;
    rejected(
        invoke_json(&rig.handle, "foreign-next", json!(9))
            .await
            .unwrap_err(),
        "expired_next",
    );
    rig.logger.gate.add_permits(1);
    assert_eq!(bounded(running).await.unwrap().unwrap(), json!(2.0));
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn early_handler_return_cannot_release_an_admitted_next_or_its_code() {
    let files = Files::new("cordis_abi_reactive");
    let rig = rig(&files, json!({"hold_terminal":true}), reactive_caps()).await;
    invoke_json(&rig.handle, "use", json!({"id":"detached","detached":true}))
        .await
        .unwrap();
    let running = tokio::spawn({
        let h = rig.handle.clone();
        async move { invoke_json(&h, "run", json!(1)).await }
    });
    entered(&rig.logger).await;
    files.wait("early-handler-return").await;
    assert!(!running.is_finished());
    let unregister = tokio::spawn({
        let h = rig.handle.clone();
        async move { invoke_json(&h, "dispose", json!(0)).await }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!unregister.is_finished());
    assert_eq!(files.count("released:detached"), 0);
    rig.logger.gate.add_permits(1);
    assert_eq!(bounded(running).await.unwrap().unwrap(), json!(99.0));
    bounded(unregister).await.unwrap().unwrap();
    assert_eq!(rig.logger.records("next-result")[0].args, json!(2.0));
    assert_eq!(files.count("detached-next-finished"), 1);
    bounded(rig.rt.shutdown()).await.unwrap();
    assert_eq!(files.count("terminal-cleanup"), 1);
}

#[tokio::test]
async fn malformed_native_event_result_preserves_unknown_responsibility() {
    let files = Files::new("cordis_abi_reactive");
    let rig = rig(&files, json!({"malformed_flow":true}), reactive_caps()).await;
    invoke_json(&rig.handle, "before", Value::Null)
        .await
        .unwrap();
    invoke_json(&rig.handle, "on", cordis_wire::json!({}))
        .await
        .unwrap();
    assert!(
        invoke_json(&rig.handle, "parallel", json!(1))
            .await
            .is_err()
    );
    assert!(bounded(rig.rt.shutdown()).await.is_err());
    drop(rig);
    assert_eq!(files.count("older"), 0);
    assert_eq!(files.count("destroy"), 0);
    assert_eq!(files.count("unload"), 0);
}
