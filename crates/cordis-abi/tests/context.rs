include!("support/context.rs");

#[test]
fn exact_manifest_and_capability_preflight_before_create() {
    let files = Files::new("cordis_abi_context");
    let library = files.load();
    assert!(library.supports_context());
    let service = Service::new("example.context", "1").with_descriptor(
        cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true)).unwrap(),
    );
    assert!(
        library
            .plugin("missing", &service, files.config(cordis_wire::json!({})))
            .is_err()
    );
    assert!(
        library
            .clone()
            .with_context(Capabilities::default())
            .is_err()
    );
    let mut duplicate = caps();
    duplicate
        .dependencies
        .push(duplicate.dependencies[0].clone());
    assert!(library.clone().with_context(duplicate).is_err());
    let mut wrong = caps();
    wrong.dependencies = vec![Dependency::new_json(
        &Service::<Logger>::new("example.logger", "2"),
        cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true)).unwrap(),
        |_, _, _, args| Box::pin(async move { Ok(args) }),
    )];
    assert!(library.with_context(wrong).is_err());
    assert_eq!(files.count("create"), 0);
    let basic = Files::new("cordis_abi_fixture").load();
    assert!(!basic.supports_context());
    assert!(basic.with_context(caps()).is_err());
}

#[test]
fn malformed_extension_table_or_manifest_is_rejected() {
    for version in 1..=4 {
        let path = artifacts::binary(&format!("cordis_abi_bad_context_{version}"));
        // SAFETY: malformed semantic fields only; all pointers stay valid.
        #[allow(unsafe_code)]
        let result = unsafe { Library::load(path) };
        assert!(result.is_err());
    }
}

#[tokio::test]
async fn retired_host_tokens_are_rejected_after_restart_without_pointer_access() {
    let files = Files::new("cordis_abi_context");
    let rig = rig(&files, cordis_wire::json!({}), caps()).await;
    invoke_json(&rig.handle, "save-global", Value::Null)
        .await
        .unwrap();
    bounded(rig.provider.restart()).await.unwrap();
    drop(rig.handle); // Release the old binding's final external instance reference.
    let (_, current) = client(
        &rig.rt,
        &Service::new("example.context", "1").with_descriptor(
            cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true))
                .unwrap(),
        ),
    )
    .await;
    assert!(
        matches!(invoke_json(&current, "old-host", Value::Null).await, Err(AbiError::Refused { code, .. }) if code == "closed")
    );
    assert!(rig.logger.records("old-host").is_empty());
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn cleanup_rejects_recursive_disposal_and_new_effect_registration() {
    let files = Files::new("cordis_abi_context");
    let rig = rig(&files, cordis_wire::json!({}), caps()).await;
    invoke_json(&rig.handle, "cleanup-guards", Value::Null)
        .await
        .unwrap();
    bounded(rig.client.dispose()).await.unwrap();
    assert_eq!(
        rig.logger.records("dispose_guard")[0].args,
        "cleanup_dispose"
    );
    assert_eq!(rig.logger.records("defer_guard")[0].args, "closed");
    bounded(rig.rt.shutdown()).await.unwrap();
    assert_eq!(files.count("destroy"), 1);
}

#[tokio::test]
async fn saturated_ordinary_callbacks_do_not_starve_inverse_callbacks() {
    let files = Files::new("cordis_abi_context");
    let mut capabilities = caps();
    capabilities.max_callbacks = 1;
    capabilities.max_contexts = 2; // held ordinary call + disposal caller fill it
    let rig = rig(&files, cordis_wire::json!({}), capabilities).await;
    let effect = invoke_json(&rig.handle, "subscribe", json!("manual"))
        .await
        .unwrap();
    let handle = rig.handle.clone();
    let call = tokio::spawn(async move { invoke_json(&handle, "hold", Value::Null).await });
    bounded(rig.logger.entered.acquire())
        .await
        .unwrap()
        .forget();
    assert!(
        matches!(invoke_json(&rig.handle, "write", Value::Null).await, Err(AbiError::Refused { code, .. }) if code == "overloaded")
    );
    bounded(invoke_json(&rig.handle, "dispose", effect))
        .await
        .unwrap();
    assert_eq!(files.count("cleanup:manual"), 1);
    rig.logger.gate.add_permits(1);
    bounded(call).await.unwrap().unwrap();
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn reverse_calls_preserve_owner_caller_and_reject_expired_or_undeclared_contexts() {
    let files = Files::new("cordis_abi_context");
    let rig = rig(&files, cordis_wire::json!({}), caps()).await;
    assert_eq!(
        invoke_json(&rig.handle, "write", json!("hello"))
            .await
            .unwrap(),
        "hello"
    );
    let record = &rig.logger.records("write")[0];
    assert_eq!(Some(record.owner), rig.client.snapshot().activation);
    assert_eq!(record.caller, rig.provider.snapshot().activation);
    invoke_json(&rig.handle, "save", Value::Null).await.unwrap();
    assert!(
        matches!(invoke_json(&rig.handle, "expired", Value::Null).await, Err(AbiError::Refused { code, .. }) if code == "closed")
    );
    assert!(
        matches!(invoke_json(&rig.handle, "undeclared", Value::Null).await, Err(AbiError::Refused { code, .. }) if code == "undeclared_dependency")
    );
    assert!(rig.logger.records("expired").is_empty());
    bounded(rig.rt.shutdown()).await.unwrap();
    drop(rig);
    files.wait("unload").await;
}

#[tokio::test]
async fn independent_disposal_is_cached_and_consumer_owns_lifo_cleanup() {
    let files = Files::new("cordis_abi_context");
    let rig = rig(&files, cordis_wire::json!({}), caps()).await;
    let manual = invoke_json(&rig.handle, "subscribe", json!("manual"))
        .await
        .unwrap();
    invoke_json(&rig.handle, "subscribe", json!("first"))
        .await
        .unwrap();
    invoke_json(&rig.handle, "subscribe", json!("last"))
        .await
        .unwrap();
    assert_eq!(rig.provider.effects().len(), 1);
    assert_eq!(rig.client.effects().len(), 3);
    let (a, b) = tokio::join!(
        invoke_json(&rig.handle, "dispose", manual.clone()),
        invoke_json(&rig.handle, "dispose", manual)
    );
    a.unwrap();
    b.unwrap();
    bounded(rig.client.dispose()).await.unwrap();
    let records = rig.logger.records("cleanup");
    assert_eq!(
        records.iter().map(|r| r.args.clone()).collect::<Vec<_>>(),
        vec![json!("v1:manual"), json!("v1:last"), json!("v1:first")]
    );
    assert!(records.iter().all(|r| r.cleanup));
    assert_eq!(files.count("cleanup:manual"), 1);
    assert_eq!(files.count("deactivate"), 0);
    bounded(rig.rt.shutdown()).await.unwrap();
    drop(rig);
    files.wait("unload").await;
}

#[tokio::test]
async fn reverse_callback_can_reenter_same_cpp_instance() {
    let files = Files::new("cordis_abi_context");
    let rig = rig(&files, cordis_wire::json!({}), caps()).await;
    *rig.logger.reenter.lock().unwrap() = Some(rig.handle.clone());
    assert_eq!(
        bounded(invoke_json(&rig.handle, "reenter", json!(42)))
            .await
            .unwrap(),
        json!(42.0)
    );
    *rig.logger.reenter.lock().unwrap() = None;
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn context_resources_reverse_call_during_acquire_invoke_and_cleanup() {
    let files = Files::new("cordis_abi_context");
    let rig = rig(&files, cordis_wire::json!({}), caps()).await;
    let resource = rig
        .handle
        .call(|s, ctx| {
            Box::pin(async move {
                s.acquire_json(&ctx, "counter", json!(7))
                    .await
                    .map_err(Into::into)
            })
        })
        .await
        .unwrap();
    assert_eq!(
        resource.invoke_json("get", json!(42)).await.unwrap(),
        json!(42.0)
    );
    resource.dispose().await.unwrap();
    resource.dispose().await.unwrap();
    assert_eq!(rig.logger.records("resource_release").len(), 1);
    assert!(rig.logger.records("resource_release")[0].cleanup);
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn cancelled_reverse_wait_drains_before_instance_cleanup() {
    let files = Files::new("cordis_abi_context");
    let rig = rig(&files, cordis_wire::json!({}), caps()).await;
    let handle = rig.handle.clone();
    let waiter = tokio::spawn(async move { invoke_json(&handle, "hold", json!(42)).await });
    bounded(rig.logger.entered.acquire())
        .await
        .unwrap()
        .forget();
    waiter.abort();
    let _ = waiter.await;
    let provider = rig.provider.clone();
    let stop = tokio::spawn(async move { provider.dispose().await });
    bounded(async {
        while rig.provider.snapshot().state != Lifecycle::Unloading {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!stop.is_finished());
    assert_eq!(files.count("deactivate"), 0);
    rig.logger.gate.add_permits(1);
    bounded(stop).await.unwrap().unwrap();
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn cancelled_manual_disposal_keeps_cleanup_and_its_callback_capacity() {
    let files = Files::new("cordis_abi_context");
    let mut capabilities = caps();
    capabilities.max_callbacks = 1;
    let rig = rig(&files, cordis_wire::json!({}), capabilities).await;
    let effect = invoke_json(&rig.handle, "subscribe", json!("hold"))
        .await
        .unwrap();
    let handle = rig.handle.clone();
    let id = effect.clone();
    let waiter = tokio::spawn(async move { invoke_json(&handle, "dispose", id).await });
    bounded(rig.logger.entered.acquire())
        .await
        .unwrap()
        .forget();
    waiter.abort();
    let _ = waiter.await;
    // One disposal waiter cannot occupy the capacity needed for inverse callbacks.
    rig.logger.gate.add_permits(1);
    bounded(rig.client.dispose()).await.unwrap();
    assert_eq!(files.count("cleanup:hold"), 1);
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn startup_failure_recovers_deferred_effect_before_deactivation() {
    let files = Files::new("cordis_abi_context");
    let rt = Runtime::new();
    let logger = Logger::new();
    install_logger(&rt, logger.clone(), View::new()).await;
    let library = files.load().with_context(caps()).unwrap();
    let service = Service::new("example.context", "1").with_descriptor(
        cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true)).unwrap(),
    );
    let provider = rt
        .install(
            library
                .plugin(
                    "fails",
                    &service,
                    files.config(json!({"startup_effect":true,"fail_activate":true})),
                )
                .unwrap(),
        )
        .unwrap();
    assert!(bounded(provider.ready()).await.is_err());
    let _ = bounded(provider.dispose()).await;
    assert_eq!(files.count("cleanup:startup"), 1);
    assert_eq!(files.count("deactivate"), 1);
    assert_eq!(files.count("destroy"), 1);
    assert!(logger.records("cleanup")[0].cleanup);
    let _ = bounded(rt.shutdown()).await;
    drop(provider);
    drop(rt);
    drop(library);
    files.wait("unload").await;
}

#[tokio::test]
async fn failed_inverse_caches_error_and_never_destroys_or_unloads_live_callbacks() {
    let files = Files::new("cordis_abi_context");
    let rig = rig(&files, json!({"fail_cleanup":true}), caps()).await;
    let effect = invoke_json(&rig.handle, "subscribe", json!("failure"))
        .await
        .unwrap();
    let first = invoke_json(&rig.handle, "dispose", effect.clone())
        .await
        .unwrap_err();
    let second = invoke_json(&rig.handle, "dispose", effect)
        .await
        .unwrap_err();
    assert_eq!(first, second);
    assert_eq!(files.count("cleanup:failure"), 1);
    assert!(bounded(rig.rt.shutdown()).await.is_err());
    drop(rig);
    assert_eq!(files.count("destroy"), 0);
    assert_eq!(files.count("unload"), 0);
}

#[tokio::test]
async fn bounded_effect_tombstones_reject_registration_without_stranding_cpp_callback() {
    let files = Files::new("cordis_abi_context");
    let mut capabilities = caps();
    capabilities.max_effects = 1;
    let rig = rig(&files, cordis_wire::json!({}), capabilities).await;
    let effect = invoke_json(&rig.handle, "subscribe", json!("first"))
        .await
        .unwrap();
    invoke_json(&rig.handle, "dispose", effect).await.unwrap();
    assert!(
        matches!(invoke_json(&rig.handle, "subscribe", json!("extra")).await, Err(AbiError::Refused { code, .. }) if code == "overloaded")
    );
    bounded(rig.rt.shutdown()).await.unwrap();
    assert_eq!(files.count("destroy"), 1);
}

#[tokio::test]
async fn broker_switch_retains_old_effect_code_and_definition_side_dependencies() {
    let old = Files::new("cordis_abi_context");
    let new = Files::new("cordis_abi_context_v2");
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
    bounded(broker.ready()).await.unwrap();
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let register = |files: &Files, name: &str| {
        let registrations = registrations.clone();
        files
            .load()
            .with_context(caps())
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
    bounded(v1.ready()).await.unwrap();
    registrations.lock().unwrap()[0].promote().unwrap();
    let (consumer, handle) = client(&rt, &service).await;
    let activation = consumer.snapshot().activation;
    handle
        .route(|s, ctx| {
            Box::pin(async move {
                s.invoke_json(&ctx, "subscribe", json!("old"))
                    .await
                    .map_err(Into::into)
            })
        })
        .await
        .unwrap();
    let v2 = rt.install(register(&new, "v2")).unwrap();
    bounded(v2.ready()).await.unwrap();
    registrations.lock().unwrap()[1].promote().unwrap();
    let version = handle
        .route(|s, ctx| {
            Box::pin(async move {
                s.invoke_json(&ctx, "version", Value::Null)
                    .await
                    .map_err(Into::into)
            })
        })
        .await
        .unwrap();
    assert_eq!(version, "v2");
    assert_eq!(consumer.snapshot().activation, activation);
    let retiring = v1.clone();
    let retirement = tokio::spawn(async move { retiring.dispose().await });
    bounded(async {
        while v1.snapshot().state != Lifecycle::Unloading {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!retirement.is_finished());
    assert_eq!(old.count("deactivate"), 0);
    assert_eq!(old.count("unload"), 0);
    bounded(consumer.dispose()).await.unwrap();
    bounded(retirement).await.unwrap().unwrap();
    assert_eq!(old_logger.records("cleanup")[0].args, "v1:old");
    assert!(old_logger.records("cleanup")[0].cleanup);
    assert!(new_logger.records("cleanup").is_empty());
    let (_, current) = client(&rt, &service).await;
    assert_eq!(
        current
            .route(|s, ctx| Box::pin(async move {
                s.invoke_json(&ctx, "version", Value::Null)
                    .await
                    .map_err(Into::into)
            }))
            .await
            .unwrap(),
        "v2"
    );
    // Registration and external service handles can also retain module Arcs.
    let retired = registrations.lock().unwrap().remove(0);
    drop(retired);
    drop(v1);
    old.wait("unload").await;
    bounded(rt.shutdown()).await.unwrap();
}

#[cfg(feature = "loader")]
#[tokio::test]
async fn loader_context_factories_preflight_dependencies_and_activate_pinned_artifacts() {
    use cordis_abi::loader::{register_broker_context_library, register_context_library};
    use cordis_loader::{Configuration, Entry, Loader, Registry};
    let files = Files::new("cordis_abi_context");
    let rt = Runtime::new();
    let logger = Logger::new();
    install_logger(&rt, logger.clone(), View::new()).await;
    let service = Service::<AbiService>::new("example.context", "1").with_descriptor(
        cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true)).unwrap(),
    );
    let broker_service = Service::<Broker<AbiService>>::new("example.context", "1")
        .with_descriptor(
            cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true))
                .unwrap(),
        );
    let options = artifacts::options(files.root.join("staged"));
    let artifact = artifacts::package(&files.binary);
    let mut registry = Registry::new();
    let candidates = Arc::new(Mutex::new(Vec::new()));
    // SAFETY: host-controlled staging and repository fixture library/dependencies.
    #[allow(unsafe_code)]
    unsafe {
        register_context_library(
            &mut registry,
            "library",
            service.clone(),
            options.clone(),
            caps(),
        )
        .unwrap();
        register_context_library(
            &mut registry,
            "missing",
            service.clone(),
            options.clone(),
            Capabilities::default(),
        )
        .unwrap();
        let candidates = candidates.clone();
        register_broker_context_library(
            &mut registry,
            "candidate",
            broker_service.clone(),
            options,
            caps(),
            move |candidate| {
                candidates.lock().unwrap().push(candidate);
                Ok(())
            },
        )
        .unwrap();
    }
    let loader = Loader::new(rt.clone(), registry);
    let entry = Entry::new("test", "library", files.config(cordis_wire::json!({})))
        .artifact(artifact.clone());
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
            entries: vec![entry],
        })
        .await
        .unwrap();
    bounded(loader.settled()).await.unwrap();
    let (_, handle) = client(&rt, &service).await;
    assert_eq!(
        invoke_json(&handle, "write", json!(42)).await.unwrap(),
        json!(42.0)
    );
    loader.apply(Configuration::default()).await.unwrap();
    let declaration = broker_service.clone();
    let broker = rt
        .install(
            Plugin::new("broker", move |ctx| {
                let service = declaration.clone();
                async move { ctx.provide_broker(&service) }
            })
            .provides(&broker_service),
        )
        .unwrap();
    bounded(broker.ready()).await.unwrap();
    loader
        .apply(Configuration {
            entries: vec![
                Entry::new(
                    "candidate",
                    "candidate",
                    files.config(cordis_wire::json!({})),
                )
                .artifact(artifact),
            ],
        })
        .await
        .unwrap();
    bounded(loader.settled()).await.unwrap();
    candidates.lock().unwrap()[0].promote().unwrap();
    let (_, handle) = client(&rt, &broker_service).await;
    assert_eq!(
        handle
            .route(|s, ctx| Box::pin(async move {
                s.invoke_json(&ctx, "write", json!(43))
                    .await
                    .map_err(Into::into)
            }))
            .await
            .unwrap(),
        json!(43.0)
    );
    bounded(rt.shutdown()).await.unwrap();
}
