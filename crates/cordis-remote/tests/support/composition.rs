use super::*;
use cordis_remote::{CompositionLimits, FactoryBinding, ProvisionBinding, Value};
fn service() -> Service<RemoteService> {
    Service::new("example.composition", "1").with_descriptor(
        cordis_remote::Descriptor::new(
            cordis_portable_example::lifecycle::ComposerClient::interface(),
        )
        .unwrap(),
    )
}
fn settings() -> Service<cordis_remote::Bytes> {
    Service::new("example.settings", "1")
}
fn spec(backend: Backend, endpoint: Endpoint, log: Arc<Mutex<Vec<String>>>) -> ProcessSpec {
    let mut spec = backend.spec("composition");

    spec.endpoint = endpoint;
    if matches!(endpoint, Endpoint::Ipc) {
        spec.args.push("--stdio-only".into());
    }
    spec.factory(FactoryBinding::new("worker", move |config| {
        cordis_portable_example::lifecycle::ComposerControls::worker()
            .build(cordis_sdk_core::Message::from_bytes(<cordis_portable_example::lifecycle::data::lifecycle::WorkerConfig as cordis_sdk_core::typed::Codec>::contract().clone(),config).unwrap(), |config| {
                let name = config.name.expect("validated required name");
                if name == "panic" {
                    panic!("definition factory panic");
                }
                if name == "reject" {
                    return Err(cordis_sdk_core::error(
                        "worker_rejected",
                        "factory rejected",
                    ));
                }
                let log = log.clone();
                let plugin = Plugin::new(name.clone(), move |ctx| {
                    let log = log.clone();
                    let name = name.clone();
                    async move {
                        log.lock().unwrap().push(format!("{name}.open"));
                        ctx.defer("worker", move |_| async move {
                            log.lock().unwrap().push(format!("{name}.close"));
                            if name == "fail_close" {
                                return Err(Error::Message("worker cleanup failed".into()));
                            }
                            Ok(())
                        })?;
                        Ok(())
                    }
                });
                Ok(if config.pending {
                    plugin.requires(&Service::<()>::new("missing", "1"))
                } else {
                    plugin
                })
            })
            .map_err(Error::Portable)
    }))
    .provision(ProvisionBinding::new("settings", &settings()))
}
async fn consumer(rt: &Runtime) -> (PluginHandle, ServiceHandle<RemoteService>) {
    consumer_for(rt, service()).await
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
            Plugin::new("consumer", {
                let saved = saved.clone();
                let service = service.clone();
                move |ctx| {
                    let service = service.clone();
                    let saved = saved.clone();
                    async move {
                        let config = ctx
                            .dependency(&settings())?
                            .call(|v, _| Box::pin(async move { Ok((*v).clone()) }))
                            .await?;
                        assert_eq!(cordis_sdk_core::Message::from_bytes(<cordis_portable_example::lifecycle::data::lifecycle::Settings as cordis_sdk_core::typed::Codec>::contract().clone(),config).unwrap().to_json().unwrap(),json!({"answer":42}));
                        *saved.lock().unwrap() = Some(ctx.dependency(&service)?);
                        Ok(())
                    }
                }
            })
            .requires(&service)
            .requires(&settings()),
        )
        .unwrap();
    bounded(client.ready()).await.unwrap();
    let handle = saved.lock().unwrap().take().unwrap();
    (client, handle)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sdk_children_and_staged_values_use_native_ownership_in_all_transports() {
    for &backend in Backend::ALL {
        for endpoint in [Endpoint::Ipc, Endpoint::Http] {
            let rt = Runtime::new();
            let log = Arc::new(Mutex::new(Vec::new()));
            let provider = rt
                .install(spec(backend, endpoint, log.clone()).plugin("composer", &service()))
                .unwrap();
            bounded(provider.ready()).await.unwrap();
            let (a, ha) = consumer(&rt).await;
            let (b, hb) = consumer(&rt).await;
            for (handle, name) in [(&ha, "a"), (&hb, "b")] {
                assert_eq!(
                    bounded(call(handle, endpoint, "install", json!({"name":name})))
                        .await
                        .unwrap()["state"],
                    "active"
                );
            }
            assert!(
                call(&ha, endpoint, "undeclared", json!({"name":"denied"}))
                    .await
                    .is_err()
            );
            assert_eq!(*log.lock().unwrap(), ["a.open", "b.open"]);
            bounded(a.dispose()).await.unwrap();
            assert_eq!(*log.lock().unwrap(), ["a.open", "b.open", "a.close"]);
            assert_eq!(
                call(&hb, endpoint, "status", json!("b")).await.unwrap()["state"],
                "active"
            );
            for _ in 0..2 {
                bounded(call(&hb, endpoint, "release", json!("b")))
                    .await
                    .unwrap();
            }
            bounded(b.dispose()).await.unwrap();
            bounded(rt.shutdown()).await.unwrap();
            assert_eq!(
                *log.lock().unwrap(),
                ["a.open", "b.open", "a.close", "b.close"]
            );
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejected_publication_and_failed_activation_never_publish_partial_service() {
    for &backend in Backend::ALL {
        for flag in ["--missing", "--duplicate", "--fail"] {
            let rt = Runtime::new();
            let log = Arc::new(Mutex::new(Vec::new()));
            let mut spec = spec(backend, Endpoint::Ipc, log.clone());
            spec.args.push(flag.into());
            let provider = rt.install(spec.plugin("invalid", &service())).unwrap();
            assert!(bounded(provider.ready()).await.is_err(), "{flag}");
            let _ = bounded(provider.dispose()).await;
            let entries = log.lock().unwrap().clone();
            assert_eq!(
                entries.iter().filter(|v| v.ends_with(".open")).count(),
                entries.iter().filter(|v| v.ends_with(".close")).count()
            );
            let _ = bounded(rt.shutdown()).await;
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn factory_failure_releases_capacity_but_successful_ids_are_never_reused() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let mut spec = spec(backend, Endpoint::Http, Arc::default());
        spec.composition = CompositionLimits { children: 1 };
        let provider = rt.install(spec.plugin("composer", &service())).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (_, handle) = consumer(&rt).await;
        for name in ["reject", "panic"] {
            assert!(
                call(&handle, Endpoint::Http, "install", json!({"name":name}))
                    .await
                    .is_err()
            );
        }
        call(
            &handle,
            Endpoint::Http,
            "install_pending",
            json!({"name":"pending","pending":true}),
        )
        .await
        .unwrap();
        assert_eq!(
            call(&handle, Endpoint::Http, "status", json!("pending"))
                .await
                .unwrap()["state"],
            "inactive"
        );
        call(&handle, Endpoint::Http, "release", json!("pending"))
            .await
            .unwrap();
        assert!(
            call(&handle, Endpoint::Http, "install", json!({"name":"full"}))
                .await
                .unwrap_err()
                .to_string()
                .contains("overloaded")
        );
        bounded(rt.shutdown()).await.unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_child_inverse_blocks_whole_provider_cleanup() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let provider = rt
            .install(spec(backend, Endpoint::Ipc, Arc::default()).plugin("composer", &service()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (_, handle) = consumer(&rt).await;
        call(
            &handle,
            Endpoint::Ipc,
            "install",
            json!({"name":"fail_close"}),
        )
        .await
        .unwrap();
        assert!(bounded(provider.dispose()).await.is_err());
        assert!(bounded(rt.shutdown()).await.is_err());
    }
}

// Raw peers deliberately send malformed messages and forged capabilities.
fn probe_service() -> Service<RemoteService> {
    Service::new("example.composition", "1").with_descriptor(
        cordis_remote::Descriptor::new(cordis_remote::Interface::value_dispatch(true)).unwrap(),
    )
}
fn peer(mode: &str, log: Arc<Mutex<Vec<String>>>) -> ProcessSpec {
    let mut spec = spec(Backend::TypeScript, Endpoint::Ipc, log);
    spec.executable = python().into();
    spec.args = vec![
        cordis_test_contracts::PYTHON_RUNNER.into(),
        format!(
            "{}/tests/fixtures/composition_peer.py",
            env!("CARGO_MANIFEST_DIR")
        ),
        mode.into(),
    ];
    spec
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_composition_rejects_forged_handles_stale_contexts_and_unbound_authority() {
    let rt = Runtime::new();
    let log = Arc::default();
    let provider = rt
        .install(
            peer("normal", log)
                .factory(FactoryBinding::new("\u{e000}", |_| {
                    Ok(Plugin::new("unused", |_| async { Ok(()) }))
                }))
                .factory(FactoryBinding::new("😀", |_| {
                    Ok(Plugin::new("unused", |_| async { Ok(()) }))
                }))
                .plugin("peer", &probe_service()),
        )
        .unwrap();
    bounded(provider.ready()).await.unwrap();
    let (_, handle) = probe_consumer(&rt).await;
    let invoke = |method: &'static str, command: &str, payload: Value| {
        call(
            &handle,
            Endpoint::Ipc,
            method,
            json!({"command":command,"payload":payload}),
        )
    };
    let reply = invoke(
        "request",
        "child_install",
        json!({"factory":"worker","config":{"name":"child"}}),
    )
    .await
    .unwrap();
    assert_eq!(reply["kind"], "success");
    assert_eq!(reply["value"]["status"], "ok");
    let child = reply["value"]["value"].clone();
    assert_eq!(
        invoke(
            "request",
            "child_install",
            json!({"factory":"missing","config":null})
        )
        .await
        .unwrap()["value"]["status"],
        "refused"
    );
    for payload in [json!({"factory":"worker","config":null,"context":"0".repeat(32)})] {
        assert_eq!(
            invoke("request", "child_install", payload).await.unwrap()["kind"],
            "refused"
        );
    }
    assert_eq!(
        invoke(
            "stale",
            "child_install",
            json!({"factory":"worker","config":{"name":"stale"}})
        )
        .await
        .unwrap()["kind"],
        "refused"
    );
    let mut forged = child.clone();
    forged["grant"] = json!("0".repeat(32));
    assert_eq!(
        invoke("request", "child_dispose", forged).await.unwrap()["kind"],
        "refused"
    );
    assert_eq!(
        invoke("request", "child_ready", child.clone())
            .await
            .unwrap()["kind"],
        "success"
    );
    assert_eq!(
        invoke(
            "request",
            "value_provide",
            json!({"service":"settings","value":1})
        )
        .await
        .unwrap()["kind"],
        "refused"
    );
    assert_eq!(
        invoke("request", "child_dispose", child).await.unwrap()["kind"],
        "success"
    );
    bounded(rt.shutdown()).await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_child_reply_and_waiting_peer_death_leave_no_orphan_or_drain_deadlock() {
    for mode in ["reply_crash", "wait_crash"] {
        let rt = Runtime::new();
        let log = Arc::new(Mutex::new(Vec::new()));
        let provider = rt
            .install(peer(mode, log.clone()).plugin("peer", &probe_service()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (owner, handle) = probe_consumer(&rt).await;
        assert!(bounded(call(&handle,Endpoint::Ipc,"request",json!({"command":"child_install","payload":{"factory":"worker","config":{"name":"lost","pending":mode=="wait_crash"}}}))).await.is_err());
        bounded(owner.dispose()).await.unwrap();
        assert!(
            rt.snapshots()
                .iter()
                .filter(|s| s.name == "lost")
                .all(|s| s.retired && s.state == Lifecycle::Inactive)
        );
        let entries = log.lock().unwrap().clone();
        assert_eq!(
            entries.iter().filter(|v| v.ends_with(".open")).count(),
            entries.iter().filter(|v| v.ends_with(".close")).count()
        );
        assert!(bounded(rt.shutdown()).await.is_err());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_install_denies_before_factory_and_leaves_live_route_usable() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let broker = Service::<Broker<RemoteService>>::new("example.composition", "1")
            .with_descriptor(
                cordis_remote::Descriptor::new(
                    cordis_portable_example::lifecycle::ComposerClient::interface(),
                )
                .unwrap(),
            );
        let b = rt
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
        bounded(b.ready()).await.unwrap();
        let definitions = Arc::new(AtomicUsize::new(0));
        let mut spec = spec(backend, Endpoint::Ipc, Arc::default());
        spec.factories = vec![FactoryBinding::new("worker", {
            let definitions = definitions.clone();
            move |_| {
                definitions.fetch_add(1, Ordering::SeqCst);
                Ok(Plugin::new("forbidden", |_| async { Ok(()) }))
            }
        })];
        let candidate = Arc::new(Mutex::new(None));
        let p = rt
            .install(spec.broker_plugin("candidate", &broker, {
                let candidate = candidate.clone();
                move |c| {
                    *candidate.lock().unwrap() = Some(c);
                    Ok(())
                }
            }))
            .unwrap();
        bounded(p.ready()).await.unwrap();
        candidate.lock().unwrap().take().unwrap().promote().unwrap();
        let saved = Arc::new(Mutex::new(None));
        let c = rt
            .install(
                Plugin::new("caller", {
                    let broker = broker.clone();
                    let saved = saved.clone();
                    move |ctx| {
                        let broker = broker.clone();
                        let saved = saved.clone();
                        async move {
                            *saved.lock().unwrap() = Some(ctx.dependency(&broker)?);
                            Ok(())
                        }
                    }
                })
                .requires(&broker),
            )
            .unwrap();
        bounded(c.ready()).await.unwrap();
        let handle: ServiceHandle<Broker<RemoteService>> = saved.lock().unwrap().take().unwrap();
        let error = handle
            .route(|s, ctx| {
                Box::pin(async move {
                    s.invoke_json(&ctx, "install", json!({"name":"denied"}))
                        .await
                        .map_err(Into::into)
                })
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Broker"), "{error}");
        assert_eq!(definitions.load(Ordering::SeqCst), 0);
        assert_eq!(p.snapshot().state, Lifecycle::Active);
        bounded(rt.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_child_configuration_is_isolated_and_names_are_reserved_before_await() {
    for &backend in Backend::ALL {
        let rt = Runtime::new();
        let log = Arc::new(Mutex::new(Vec::new()));
        let provider = rt
            .install(spec(backend, Endpoint::Ipc, log.clone()).plugin("composer", &service()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (_, handle) = consumer(&rt).await;
        for config in [
            json!({}),
            json!({"name":17}),
            json!({"name":"invalid","unknown":true}),
        ] {
            assert!(
                call(&handle, Endpoint::Ipc, "install", config)
                    .await
                    .is_err()
            );
        }
        assert!(log.lock().unwrap().is_empty());
        for _ in 0..2 {
            let error = call(&handle, Endpoint::Ipc, "install", json!({"name":"reject"}))
                .await
                .unwrap_err();
            assert!(error.to_string().contains("worker_rejected"), "{error}");
        }
        let args = json!({"name":"one"});
        let (a, b) = tokio::join!(
            call(&handle, Endpoint::Ipc, "install", args.clone()),
            call(&handle, Endpoint::Ipc, "install", args)
        );
        assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
        let error = a.err().or_else(|| b.err()).unwrap();
        assert!(error.to_string().contains("duplicate_child"), "{error}");
        assert_eq!(*log.lock().unwrap(), ["one.open"]);
        call(&handle, Endpoint::Ipc, "release", json!("one"))
            .await
            .unwrap();
        assert!(
            call(&handle, Endpoint::Ipc, "install", json!({"name":"one"}))
                .await
                .unwrap_err()
                .to_string()
                .contains("duplicate_child")
        );
        bounded(rt.shutdown()).await.unwrap();
        assert_eq!(*log.lock().unwrap(), ["one.open", "one.close"]);
    }
}
