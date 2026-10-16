include!("support/context.rs");
use cordis_abi::{FactoryBinding, MiddlewareBinding, ProvisionBinding};

fn info() -> Service<cordis_abi::Bytes> {
    Service::new("example.info", "1")
}
struct FactoryState {
    calls: AtomicU64,
    entered: Semaphore,
    gate: Semaphore,
    cleaned: AtomicU64,
}
impl FactoryState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicU64::new(0),
            entered: Semaphore::new(0),
            gate: Semaphore::new(0),
            cleaned: AtomicU64::new(0),
        })
    }
}
fn composition_caps(state: &Arc<FactoryState>) -> Capabilities {
    let state = state.clone();
    Capabilities {
        factories: vec![FactoryBinding::new("worker", move |config| {
            let config = unwire_json(&config);
            state.calls.fetch_add(1, Ordering::Relaxed);
            if config["block"] == true {
                state.entered.add_permits(1);
                tokio::runtime::Handle::current()
                    .block_on(state.gate.acquire())
                    .unwrap()
                    .forget();
            }
            if config["reject"] == true {
                return Err(Error::Message("factory rejected".into()));
            }
            assert_ne!(config["panic"], true, "factory panic");
            let state = state.clone();
            let child_config = config.clone();
            let mut plugin = Plugin::new("factory child", move |ctx| {
                let state = state.clone();
                let config = child_config.clone();
                async move {
                    // Inherited from the ABI definition, without a child requires declaration.
                    let logger = ctx.dependency(&logger_service())?;
                    logger
                        .call(|logger, ctx| {
                            Box::pin(async move {
                                logger.records.lock().unwrap().push(Record {
                                    method: "child".into(),
                                    args: json!("active"),
                                    owner: ctx.owner(),
                                    caller: ctx.caller(),
                                    cleanup: ctx.is_cleanup(),
                                });
                                Ok(())
                            })
                        })
                        .await?;
                    ctx.defer("child cleanup", move |ctx| async move {
                        let logger = ctx.dependency(&logger_service())?;
                        logger
                            .call(|logger, ctx| {
                                Box::pin(async move {
                                    logger.records.lock().unwrap().push(Record {
                                        method: "child-cleanup".into(),
                                        args: Value::Null,
                                        owner: ctx.owner(),
                                        caller: ctx.caller(),
                                        cleanup: ctx.is_cleanup(),
                                    });
                                    Ok(())
                                })
                            })
                            .await?;
                        state.cleaned.fetch_add(1, Ordering::Relaxed);
                        if config["cleanup_fail"] == true {
                            return Err(Error::Message("child inverse failed".into()));
                        }
                        Ok(())
                    })?;
                    Ok(())
                }
            });
            if config["parent"] == true {
                plugin = plugin.requires(
                    &Service::<AbiService>::new("example.context", "1").with_descriptor(
                        cordis_contract::Descriptor::new(
                            cordis_contract::Interface::value_dispatch(true),
                        )
                        .unwrap(),
                    ),
                );
            }
            if config["missing"] == true {
                plugin = plugin.requires(&Service::<()>::new("missing", "1"));
            }
            Ok(plugin)
        })],
        provisions: vec![ProvisionBinding::new("info", &info())],
        ..caps()
    }
}
fn refused(error: AbiError, code: &str) {
    assert!(
        matches!(&error, AbiError::Refused { code: c, .. } if c == code),
        "{error:?}"
    );
}
async fn install(handle: &ServiceHandle<AbiService>, config: Value) -> cordis_abi::Result<Value> {
    invoke_json(handle, "install", json!({"config":config})).await
}

#[test]
fn independent_negotiation_and_exact_manifest_preflight() {
    for kind in 1..=7 {
        // SAFETY: repository fixtures deliberately vary metadata, with valid memory.
        #[allow(unsafe_code)]
        let result = unsafe {
            Library::load(artifacts::binary(&format!(
                "cordis_abi_bad_composition_{kind}"
            )))
        };
        assert!(result.is_err(), "kind {kind}");
    }
    let files = Files::new("cordis_abi_composition");
    let state = FactoryState::new();
    let library = files.load();
    assert!(library.supports_composition());
    assert!(!library.supports_managed());
    assert!(!library.supports_reactive());
    assert!(
        library
            .clone()
            .with_context(composition_caps(&state))
            .is_ok()
    );
    assert!(library.clone().with_context(caps()).is_err());
    let mut policy = composition_caps(&state);
    policy.factories.push(policy.factories[0].clone());
    assert!(library.clone().with_context(policy).is_err());
    let mut policy = composition_caps(&state);
    policy
        .provisions
        .push(ProvisionBinding::new("other", &info()));
    assert!(library.clone().with_context(policy).is_err());
    let mut policy = composition_caps(&state);
    policy.provisions = vec![ProvisionBinding::new(
        "info",
        &Service::new("example.context", "1").with_descriptor(
            cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true))
                .unwrap(),
        ),
    )];
    assert!(library.clone().with_context(policy).is_err());
    let mut policy = composition_caps(&state);
    policy.provisions = vec![ProvisionBinding::new(
        "info",
        &Service::new("example.info", "2"),
    )];
    assert!(library.with_context(policy).is_err());
    assert_eq!(files.count("create"), 0);
    assert_eq!(state.calls.load(Ordering::Relaxed), 0);
    assert!(
        !Files::new("cordis_abi_context")
            .load()
            .supports_composition()
    );
}

#[tokio::test]
async fn child_ownership_inheritance_and_independent_handles() {
    let files = Files::new("cordis_abi_composition");
    let state = FactoryState::new();
    let rig = rig(&files, cordis_wire::json!({}), composition_caps(&state)).await;
    let id = install(&rig.handle, cordis_wire::json!({})).await.unwrap();
    assert_eq!(
        invoke_json(&rig.handle, "ready", id.clone()).await.unwrap()["state"],
        "active"
    );
    let records = rig.logger.records("child");
    assert_eq!(records.len(), 1);
    assert_ne!(records[0].owner.fiber, rig.provider.id());
    assert_eq!(records[0].caller, Some(records[0].owner));
    assert_eq!(records[0].args, "active");
    assert!(!records[0].cleanup);
    refused(
        invoke_json(&rig.handle, "expired", Value::Null)
            .await
            .unwrap_err(),
        "closed",
    );
    let (a, b) = tokio::join!(
        invoke_json(&rig.handle, "dispose", id.clone()),
        invoke_json(&rig.handle, "dispose", id.clone())
    );
    a.unwrap();
    b.unwrap();
    assert_eq!(state.cleaned.load(Ordering::Relaxed), 1);
    assert_eq!(
        invoke_json(&rig.handle, "status", id).await.unwrap()["retired"],
        true
    );
    install(&rig.handle, cordis_wire::json!({})).await.unwrap();
    invoke_json(&rig.handle, "ready", json!(1)).await.unwrap();
    bounded(rig.client.dispose()).await.unwrap();
    assert_eq!(state.cleaned.load(Ordering::Relaxed), 2);
    assert!(
        rig.logger
            .records("child-cleanup")
            .iter()
            .all(|r| r.cleanup)
    );
    assert_eq!(files.count("deactivate"), 0);
    bounded(rig.rt.shutdown()).await.unwrap();
    assert_eq!(files.count("cleanup-install:cleanup_wait"), 1);
    drop(rig);
    files.wait("unload").await;
}

#[tokio::test]
async fn undeclared_aliases_and_exhaustion_do_not_execute_factory() {
    let files = Files::new("cordis_abi_composition");
    let state = FactoryState::new();
    let mut policy = composition_caps(&state);
    policy.composition.children = 1;
    let rig = rig(&files, cordis_wire::json!({}), policy).await;
    refused(
        invoke_json(&rig.handle, "install", json!({"factory":"unbound"}))
            .await
            .unwrap_err(),
        "factory",
    );
    refused(
        invoke_json(&rig.handle, "provide", json!({"alias":"unbound"}))
            .await
            .unwrap_err(),
        "provision",
    );
    for (config, code) in [
        (json!({"reject":true}), "factory_rejected"),
        (json!({"panic":true}), "factory_panicked"),
    ] {
        refused(install(&rig.handle, config).await.unwrap_err(), code);
    }
    let id = install(&rig.handle, cordis_wire::json!({})).await.unwrap();
    invoke_json(&rig.handle, "dispose", id).await.unwrap();
    refused(
        install(&rig.handle, cordis_wire::json!({}))
            .await
            .unwrap_err(),
        "overloaded",
    );
    assert_eq!(state.calls.load(Ordering::Relaxed), 3);
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn settled_reports_missing_dependencies_without_waiting_for_ready() {
    let files = Files::new("cordis_abi_composition");
    let state = FactoryState::new();
    let rig = rig(&files, cordis_wire::json!({}), composition_caps(&state)).await;
    let id = install(&rig.handle, json!({"missing":true})).await.unwrap();
    assert_eq!(
        bounded(invoke_json(&rig.handle, "settled", id.clone()))
            .await
            .unwrap()["state"],
        "inactive"
    );
    invoke_json(&rig.handle, "dispose", id.clone())
        .await
        .unwrap();
    assert!(
        invoke_json(&rig.handle, "ready", id)
            .await
            .unwrap_err()
            .to_string()
            .contains("Retired")
    );
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn publication_is_staged_and_consumers_drain_before_child_and_provider() {
    let files = Files::new("cordis_abi_composition");
    let state = FactoryState::new();
    let rt = Runtime::new();
    let logger = Logger::new();
    install_logger(&rt, logger.clone(), View::new()).await;
    let service = Service::<AbiService>::new("example.context", "1").with_descriptor(
        cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true)).unwrap(),
    );
    let provider = rt
        .install(
            files
                .load()
                .with_context(composition_caps(&state))
                .unwrap()
                .plugin(
                    "parent",
                    &service,
                    files.config(json!({"hold":true,"child":{}})),
                )
                .unwrap(),
        )
        .unwrap();
    bounded(logger.entered.acquire()).await.unwrap().forget();
    let entered = Arc::new(Semaphore::new(0));
    let gate = Arc::new(Semaphore::new(0));
    let consumer = rt
        .install(
            Plugin::new("info consumer", {
                let entered = entered.clone();
                let gate = gate.clone();
                move |ctx| {
                    let entered = entered.clone();
                    let gate = gate.clone();
                    async move {
                        let value = ctx.dependency(&info())?;
                        value
                            .call(|v, _| {
                                Box::pin(async move {
                                    assert_eq!(unwire_json(v), json!({"version":1.0}));
                                    Ok(())
                                })
                            })
                            .await?;
                        ctx.defer("consumer", move |ctx| async move {
                            entered.add_permits(1);
                            gate.acquire().await.unwrap().forget();
                            ctx.dependency(&info())?
                                .call(|v, _| {
                                    Box::pin(async move {
                                        assert_eq!(unwire_json(v)["version"], json!(1.0));
                                        Ok(())
                                    })
                                })
                                .await
                        })?;
                        Ok(())
                    }
                }
            })
            .requires(&info()),
        )
        .unwrap();
    assert_eq!(consumer.settled().await.unwrap().state, Lifecycle::Inactive);
    logger.gate.add_permits(1);
    provider.ready().await.unwrap();
    consumer.ready().await.unwrap();
    let retiring = tokio::spawn({
        let provider = provider.clone();
        async move { provider.dispose().await }
    });
    bounded(entered.acquire()).await.unwrap().forget();
    assert_eq!(files.count("deactivate"), 0);
    // Children and consumers may drain concurrently; the provider's own
    // inverses must wait for both groups.
    gate.add_permits(1);
    bounded(retiring).await.unwrap().unwrap();
    assert_eq!(state.cleaned.load(Ordering::Relaxed), 1);
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_initialization_recovers_children_and_rejects_duplicate_or_missing_provisions() {
    for config in [
        json!({"fail":true,"child":{}}),
        json!({"duplicate":true}),
        json!({"missing":true}),
    ] {
        let files = Files::new("cordis_abi_composition");
        let state = FactoryState::new();
        let rt = Runtime::new();
        install_logger(&rt, Logger::new(), View::new()).await;
        let provider = rt
            .install(
                files
                    .load()
                    .with_context(composition_caps(&state))
                    .unwrap()
                    .plugin(
                        "parent",
                        &Service::new("example.context", "1").with_descriptor(
                            cordis_contract::Descriptor::new(
                                cordis_contract::Interface::value_dispatch(true),
                            )
                            .unwrap(),
                        ),
                        files.config(config.clone()),
                    )
                    .unwrap(),
            )
            .unwrap();
        assert!(bounded(provider.ready()).await.is_err());
        assert_eq!(files.count("destroy"), 1);
        if config["child"].is_object() {
            assert_eq!(state.cleaned.load(Ordering::Relaxed), 1);
        }
        rt.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn parent_dependency_ready_detects_deadlock_and_recovers() {
    let files = Files::new("cordis_abi_composition");
    let state = FactoryState::new();
    let rt = Runtime::new();
    install_logger(&rt, Logger::new(), View::new()).await;
    let provider = rt
        .install(
            files
                .load()
                .with_context(composition_caps(&state))
                .unwrap()
                .plugin(
                    "parent",
                    &Service::new("example.context", "1").with_descriptor(
                        cordis_contract::Descriptor::new(
                            cordis_contract::Interface::value_dispatch(true),
                        )
                        .unwrap(),
                    ),
                    files.config(json!({"child":{"parent":true}})),
                )
                .unwrap(),
        )
        .unwrap();
    let error = bounded(provider.ready()).await.unwrap_err();
    assert!(error.to_string().contains("WouldDeadlock"), "{error:?}");
    assert_eq!(files.count("destroy"), 1);
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn foreign_or_active_context_cannot_replace_a_published_value() {
    let files = Files::new("cordis_abi_composition");
    let state = FactoryState::new();
    let rig = rig(&files, cordis_wire::json!({}), composition_caps(&state)).await;
    refused(
        invoke_json(&rig.handle, "provide", json!({"value":99}))
            .await
            .unwrap_err(),
        "closed",
    );
    let (_, handle) = client(&rig.rt, &info()).await;
    handle
        .call(|value, _| {
            Box::pin(async move {
                assert_eq!(unwire_json(value)["version"], json!(1.0));
                Ok(())
            })
        })
        .await
        .unwrap();
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn cancelled_waiter_does_not_drop_running_factory_or_late_child() {
    let files = Files::new("cordis_abi_composition");
    let state = FactoryState::new();
    let mut policy = composition_caps(&state);
    policy.composition.active_installs = 1;
    let rig = rig(&files, cordis_wire::json!({}), policy).await;
    let call = tokio::spawn({
        let handle = rig.handle.clone();
        async move { install(&handle, json!({"block":true})).await }
    });
    bounded(state.entered.acquire()).await.unwrap().forget();
    refused(
        install(&rig.handle, cordis_wire::json!({}))
            .await
            .unwrap_err(),
        "overloaded",
    );
    assert_eq!(state.calls.load(Ordering::Relaxed), 1);
    call.abort();
    let retiring = tokio::spawn({
        let client = rig.client.clone();
        async move { client.dispose().await }
    });
    bounded(async {
        while rig.client.snapshot().state != Lifecycle::Unloading {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!retiring.is_finished());
    assert_eq!(files.count("destroy"), 0);
    state.gate.add_permits(1);
    bounded(retiring).await.unwrap().unwrap();
    assert_eq!(
        invoke_json(&rig.handle, "echo", Value::Null)
            .await
            .unwrap_err(),
        AbiError::Closed
    );
    bounded(rig.rt.shutdown()).await.unwrap();
    drop(rig);
    files.wait("unload").await;
}

#[tokio::test]
async fn child_cleanup_failure_blocks_provider_destruction() {
    let files = Files::new("cordis_abi_composition");
    let state = FactoryState::new();
    let rig = rig(
        &files,
        json!({"child":{"cleanup_fail":true}}),
        composition_caps(&state),
    )
    .await;
    let error = bounded(rig.provider.dispose()).await.unwrap_err();
    assert!(
        error.to_string().contains("child inverse failed"),
        "{error:?}"
    );
    assert_eq!(files.count("deactivate"), 0);
    assert_eq!(files.count("destroy"), 0);
    assert_eq!(files.count("unload"), 0);
    assert!(rig.rt.shutdown().await.is_err());
}

#[tokio::test]
async fn composition_is_available_in_managed_and_reactive_callbacks() {
    let files = Files::new("cordis_abi_composition_reactive");
    let state = FactoryState::new();
    let mut policy = composition_caps(&state);
    policy.middleware = vec![MiddlewareBinding::new(
        "chain",
        &Middleware::new("example.chain", "1"),
    )];
    let rig = rig(&files, cordis_wire::json!({}), policy).await;
    let id = invoke_json(&rig.handle, "scope", Value::Null)
        .await
        .unwrap();
    assert_eq!(
        invoke_json(&rig.handle, "status", id).await.unwrap()["retired"],
        true
    );
    let id = invoke_json(&rig.handle, "terminal", Value::Null)
        .await
        .unwrap();
    assert_eq!(
        invoke_json(&rig.handle, "ready", id).await.unwrap()["state"],
        "active"
    );
    rig.client.dispose().await.unwrap();
    assert_eq!(state.cleaned.load(Ordering::Relaxed), 2);
    rig.rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn definition_realms_control_publication_and_inherited_child_dependencies() {
    let files = Files::new("cordis_abi_composition");
    let state = FactoryState::new();
    let rt = Runtime::new();
    let local = Logger::new();
    let global = Logger::new();
    let view = View::new().isolate(&logger_service()).isolate(&info());
    install_logger(&rt, local.clone(), view.clone()).await;
    install_logger(&rt, global.clone(), View::new()).await;
    let service = Service::<AbiService>::new("example.context", "1").with_descriptor(
        cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true)).unwrap(),
    );
    let provider = rt
        .install_in(
            view.clone(),
            files
                .load()
                .with_context(composition_caps(&state))
                .unwrap()
                .plugin("parent", &service, files.config(json!({"child":{}})))
                .unwrap(),
        )
        .unwrap();
    provider.ready().await.unwrap();
    assert_eq!(local.records("child").len(), 1);
    assert!(global.records("child").is_empty());
    let global_consumer = rt
        .install(Plugin::new("global", |_| async { Ok(()) }).requires(&info()))
        .unwrap();
    assert_eq!(
        global_consumer.settled().await.unwrap().state,
        Lifecycle::Inactive
    );
    let local_consumer = rt
        .install_in(
            view,
            Plugin::new("local", |ctx| async move {
                ctx.dependency(&info())?
                    .call(|v, _| {
                        Box::pin(async move {
                            assert_eq!(unwire_json(v)["version"], json!(1.0));
                            Ok(())
                        })
                    })
                    .await
            })
            .requires(&info()),
        )
        .unwrap();
    local_consumer.ready().await.unwrap();
    // Calls from a different realm cannot inherit the definition's logger.
    let (_, handle) = client(&rt, &service).await;
    let id = install(&handle, cordis_wire::json!({})).await.unwrap();
    assert!(invoke_json(&handle, "ready", id).await.is_err());
    rt.shutdown().await.unwrap();
    assert_eq!(local.records("child-cleanup").len(), 1);
    assert!(global.records("child-cleanup").is_empty());
}

#[tokio::test]
async fn broker_rejects_install_before_factory_and_extra_provisions_remain_exclusive() {
    let files = Files::new("cordis_abi_composition");
    let state = FactoryState::new();
    let rt = Runtime::new();
    install_logger(&rt, Logger::new(), View::new()).await;
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
    let library = files.load().with_context(composition_caps(&state)).unwrap();
    let make = || {
        let registrations = registrations.clone();
        library
            .broker_plugin(
                "candidate",
                &service,
                files.config(cordis_wire::json!({})),
                move |token| {
                    registrations.lock().unwrap().push(token);
                    Ok(())
                },
            )
            .unwrap()
    };
    let candidate = rt.install(make()).unwrap();
    candidate.ready().await.unwrap();
    registrations.lock().unwrap()[0].promote().unwrap();
    let (_, handle) = client(&rt, &service).await;
    let error = handle
        .route(|s, ctx| {
            Box::pin(async move {
                s.invoke_json(&ctx, "install", cordis_wire::json!({}))
                    .await
                    .map_err(Into::into)
            })
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("through a Broker"), "{error:?}");
    assert_eq!(state.calls.load(Ordering::Relaxed), 0);
    assert!(rt.install(make()).is_err());
    assert_eq!(files.count("create"), 1);
    rt.shutdown().await.unwrap();
}

#[cfg(feature = "loader")]
#[tokio::test]
async fn loader_preflights_composition_and_recovers_children() {
    use cordis_abi::loader::register_context_library;
    use cordis_loader::{Configuration, Entry, Loader, Registry};
    let files = Files::new("cordis_abi_composition");
    let state = FactoryState::new();
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
            composition_caps(&state),
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
                Entry::new("good", "library", files.config(json!({"child":{}}))).artifact(artifact),
            ],
        })
        .await
        .unwrap();
    bounded(loader.settled()).await.unwrap();
    let (_, handle) = client(&rt, &info()).await;
    handle
        .call(|v, _| {
            Box::pin(async move {
                assert_eq!(unwire_json(v)["version"], json!(1.0));
                Ok(())
            })
        })
        .await
        .unwrap();
    loader.apply(Configuration::default()).await.unwrap();
    assert_eq!(state.cleaned.load(Ordering::Relaxed), 1);
    assert_eq!(files.count("destroy"), 1);
    rt.shutdown().await.unwrap();
}
