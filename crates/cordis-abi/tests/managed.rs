include!("support/context.rs");

#[tokio::test]
async fn dynamic_intercepts_are_pinned_through_real_abi_callbacks_and_cleanup() {
    let files = Files::new("cordis_abi_managed");
    let rig = rig(&files, cordis_wire::json!({}), caps()).await;
    let service = Service::<AbiService>::new("example.context", "1").with_descriptor(
        cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true)).unwrap(),
    );
    let make = |revision| {
        let mut config = InterceptConfig::new(revision);
        config.set(&service, json!({"timeout":revision * 1000}));
        config
    };
    let source = Intercepts::new(make(1));
    let saved = Arc::new(Mutex::new(None));
    let client = rig
        .rt
        .install_in(View::new().with_intercepts(source.clone()), {
            let saved = saved.clone();
            let service = service.clone();
            Plugin::new("metadata client", move |ctx| {
                let saved = saved.clone();
                let service = service.clone();
                async move {
                    *saved.lock().unwrap() = Some(ctx.dependency(&service)?);
                    Ok(())
                }
            })
            .requires(
                &Service::<AbiService>::new("example.context", "1").with_descriptor(
                    cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(
                        true,
                    ))
                    .unwrap(),
                ),
            )
        })
        .unwrap();
    client.ready().await.unwrap();
    let handle = saved.lock().unwrap().take().unwrap();
    for (name, revision) in [("example.context", "2"), ("private.service", "1")] {
        let error = invoke_json(
            &handle,
            "metadata-for",
            json!({"name":name,"revision":revision}),
        )
        .await
        .unwrap_err();
        assert!(matches!(error,AbiError::Refused {code,..} if code=="undeclared_metadata"));
    }
    let before = invoke_json(&handle, "metadata-defer", Value::Null)
        .await
        .unwrap();
    assert_eq!(
        before,
        json!({"revision":"1","layers":[{"timeout":1000.0}]})
    );
    source.replace(make(3));
    assert_eq!(
        invoke_json(&handle, "metadata", cordis_wire::json!({}))
            .await
            .unwrap(),
        json!({"revision":"3","layers":[{"timeout":3000.0}]})
    );
    client.dispose().await.unwrap();
    assert_eq!(rig.logger.records("metadata-cleanup")[0].args, before);
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[test]
fn managed_handshake_rejects_version_size_missing_functions_and_missing_context() {
    for kind in 1..=4 {
        let path = artifacts::binary(&format!("cordis_abi_bad_managed_{kind}"));
        // SAFETY: trusted fixture changes semantic fields only, with valid memory.
        #[allow(unsafe_code)]
        let result = unsafe { Library::load(path) };
        assert!(result.is_err());
    }
}

#[tokio::test]
async fn direct_sequence_and_task_self_waits_are_rejected() {
    for sequence in [false, true] {
        let files = Files::new("cordis_abi_managed");
        let rig = rig(
            &files,
            json!({"self_sequence":sequence,"self_task":!sequence,"finite_task":true}),
            caps(),
        )
        .await;
        invoke_json(
            &rig.handle,
            if sequence {
                "sequence-start"
            } else {
                "task-start"
            },
            Value::Null,
        )
        .await
        .unwrap();
        bounded(rig.logger.entered.acquire())
            .await
            .unwrap()
            .forget();
        rig.logger.gate.add_permits(1);
        bounded(invoke_json(
            &rig.handle,
            if sequence {
                "sequence-ready"
            } else {
                "task-join"
            },
            Value::Null,
        ))
        .await
        .unwrap();
        assert_eq!(rig.logger.records("self_wait")[0].args, "self_wait");
        bounded(rig.rt.shutdown()).await.unwrap();
    }
}

#[tokio::test]
async fn full_task_capacity_can_cancel_and_drain_through_old_dependencies() {
    let files = Files::new("cordis_abi_managed");
    let mut capabilities = caps();
    capabilities.max_contexts = 2;
    capabilities.max_callbacks = 2;
    capabilities.managed.active_tasks = 2;
    let rig = rig(&files, cordis_wire::json!({}), capabilities).await;
    for _ in 0..2 {
        invoke_json(&rig.handle, "task-start", Value::Null)
            .await
            .unwrap();
    }
    assert!(
        matches!(invoke_json(&rig.handle,"task-start",Value::Null).await,Err(AbiError::Refused{code,..})if code=="overloaded")
    );
    bounded(rig.provider.dispose()).await.unwrap();
    assert_eq!(files.count("task-stop"), 2);
    assert_eq!(files.count("task-last-cleanup"), 2);
    assert_eq!(files.count("destroy"), 1);
    assert!(rig.logger.records("cleanup").iter().all(|r| r.cleanup));
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn failed_child_inverse_blocks_provider_and_retains_code() {
    let files = Files::new("cordis_abi_managed");
    let rig = rig(&files, json!({"fail_scope":true}), caps()).await;
    invoke_json(&rig.handle, "scope-create", Value::Null)
        .await
        .unwrap();
    invoke_json(&rig.handle, "scope-run", Value::Null)
        .await
        .unwrap();
    assert!(
        invoke_json(&rig.handle, "scope-dispose", Value::Null)
            .await
            .is_err()
    );
    assert!(
        invoke_json(&rig.handle, "scope-dispose", Value::Null)
            .await
            .is_err()
    );
    assert_eq!(files.count("scope-cleanup"), 1);
    assert!(bounded(rig.rt.shutdown()).await.is_err());
    drop(rig);
    assert_eq!(files.count("destroy"), 0);
    assert_eq!(files.count("unload"), 0);
}

#[tokio::test]
async fn sequence_collects_final_inverse_and_recovers_once_in_reverse_order() {
    let files = Files::new("cordis_abi_managed");
    assert!(files.load().supports_managed());
    assert!(!Files::new("cordis_abi_context").load().supports_managed());
    let rig = rig(&files, cordis_wire::json!({}), caps()).await;
    invoke_json(&rig.handle, "sequence-start", Value::Null)
        .await
        .unwrap();
    assert_eq!(
        invoke_json(&rig.handle, "sequence-ready", Value::Null)
            .await
            .unwrap()["completion"],
        "completed"
    );
    let status = invoke_json(&rig.handle, "sequence-status", Value::Null)
        .await
        .unwrap();
    assert_eq!(status["steps"].as_array().unwrap().len(), 2);
    assert!(
        matches!(invoke_json(&rig.handle,"expired",Value::Null).await,Err(AbiError::Refused{code,..})if code=="closed")
    );
    let (a, b) = tokio::join!(
        invoke_json(&rig.handle, "sequence-dispose", Value::Null),
        invoke_json(&rig.handle, "sequence-dispose", Value::Null)
    );
    a.unwrap();
    b.unwrap();
    assert_eq!(files.count("undo:1"), 1);
    assert_eq!(files.count("undo:2"), 1);
    assert_eq!(
        rig.logger
            .records("cleanup")
            .iter()
            .map(|r| r.args.clone())
            .collect::<Vec<_>>(),
        vec![json!("v1:undo:2"), json!("v1:undo:1")]
    );
    assert!(rig.logger.records("cleanup").iter().all(|r| r.cleanup));
    bounded(rig.rt.shutdown()).await.unwrap();
    drop(rig);
    files.wait("unload").await;
}

#[tokio::test]
async fn forward_failure_preserves_final_inverse_and_does_not_fault_provider() {
    let files = Files::new("cordis_abi_managed");
    let rig = rig(&files, json!({"fail_step":true}), caps()).await;
    invoke_json(&rig.handle, "sequence-start", Value::Null)
        .await
        .unwrap();
    assert!(
        matches!(invoke_json(&rig.handle,"sequence-ready",Value::Null).await,Err(AbiError::Rejected{code,..})if code=="step")
    );
    assert_eq!(files.count("undo:1"), 0);
    assert_eq!(rig.provider.snapshot().state, Lifecycle::Active);
    invoke_json(&rig.handle, "sequence-dispose", Value::Null)
        .await
        .unwrap();
    assert_eq!(files.count("undo:2"), 1);
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn cancelled_ready_and_dispose_waits_keep_late_step_but_stop_next_admission() {
    let files = Files::new("cordis_abi_managed");
    let mut capabilities = caps();
    capabilities.max_callbacks = 1;
    let rig = rig(&files, json!({"hold_step":true}), capabilities).await;
    invoke_json(&rig.handle, "sequence-start", Value::Null)
        .await
        .unwrap();
    bounded(rig.logger.entered.acquire())
        .await
        .unwrap()
        .forget();
    let handle = rig.handle.clone();
    let ready =
        tokio::spawn(async move { invoke_json(&handle, "sequence-ready", Value::Null).await });
    tokio::task::yield_now().await;
    ready.abort();
    let _ = ready.await;
    let handle = rig.handle.clone();
    let dispose =
        tokio::spawn(async move { invoke_json(&handle, "sequence-dispose", Value::Null).await });
    bounded(async {
        loop {
            if invoke_json(&rig.handle, "sequence-status", Value::Null)
                .await
                .unwrap()["state"]
                == "releasing"
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    dispose.abort();
    let _ = dispose.await;
    rig.logger.gate.add_permits(1);
    // The cancelled wait still occupies its admitted disposal slot until the
    // foreign method actually returns; observe that return before a new wait.
    files.wait("sequence-disposed").await;
    bounded(invoke_json(&rig.handle, "sequence-dispose", Value::Null))
        .await
        .unwrap();
    assert_eq!(files.count("step:1"), 1);
    assert_eq!(files.count("step:2"), 0);
    assert_eq!(files.count("undo:1"), 1);
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn failed_sequence_inverse_is_cached_and_keeps_earlier_steps_and_code() {
    let files = Files::new("cordis_abi_managed");
    let rig = rig(&files, json!({"fail_undo":true}), caps()).await;
    invoke_json(&rig.handle, "sequence-start", Value::Null)
        .await
        .unwrap();
    invoke_json(&rig.handle, "sequence-ready", Value::Null)
        .await
        .unwrap();
    let first = invoke_json(&rig.handle, "sequence-dispose", Value::Null)
        .await
        .unwrap_err();
    assert_eq!(
        invoke_json(&rig.handle, "sequence-dispose", Value::Null)
            .await
            .unwrap_err(),
        first
    );
    assert_eq!(files.count("undo:2"), 1);
    assert_eq!(files.count("undo:1"), 0);
    assert!(bounded(rig.rt.shutdown()).await.is_err());
    drop(rig);
    assert_eq!(files.count("destroy"), 0);
    assert_eq!(files.count("unload"), 0);
}

#[tokio::test]
async fn scopes_have_child_ownership_fresh_context_and_nested_cleanup() {
    let files = Files::new("cordis_abi_managed");
    let rig = rig(&files, cordis_wire::json!({}), caps()).await;
    invoke_json(&rig.handle, "scope-create", Value::Null)
        .await
        .unwrap();
    invoke_json(&rig.handle, "scope-ready", Value::Null)
        .await
        .unwrap();
    assert_eq!(
        invoke_json(&rig.handle, "scope-run", json!("nested"))
            .await
            .unwrap(),
        json!(42.0)
    );
    let record = &rig.logger.records("scoped")[0];
    assert_ne!(Some(record.owner), rig.client.snapshot().activation);
    assert_eq!(record.caller, Some(record.owner));
    assert!(
        matches!(invoke_json(&rig.handle,"expired",Value::Null).await,Err(AbiError::Refused{code,..})if code=="closed")
    );
    assert_eq!(
        invoke_json(&rig.handle, "scope-run", json!("self"))
            .await
            .unwrap(),
        "self_wait"
    );
    assert!(
        invoke_json(&rig.handle, "scope-run", json!("reject"))
            .await
            .is_err()
    );
    invoke_json(&rig.handle, "scope-dispose", Value::Null)
        .await
        .unwrap();
    invoke_json(&rig.handle, "scope-dispose", Value::Null)
        .await
        .unwrap();
    assert_eq!(
        rig.logger
            .records("cleanup")
            .iter()
            .map(|r| r.args.clone())
            .collect::<Vec<_>>(),
        vec![json!("nested"), json!("v1:scope"), json!("v1:scope")]
    );
    assert!(
        invoke_json(&rig.handle, "scope-run", Value::Null)
            .await
            .is_err()
    );
    assert_eq!(rig.client.snapshot().state, Lifecycle::Active);
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn cancelled_scope_run_still_registers_late_cleanup_before_parent_recovery() {
    let files = Files::new("cordis_abi_managed");
    let rig = rig(&files, cordis_wire::json!({}), caps()).await;
    invoke_json(&rig.handle, "scope-create", Value::Null)
        .await
        .unwrap();
    let handle = rig.handle.clone();
    let run = tokio::spawn(async move { invoke_json(&handle, "scope-run", json!("hold")).await });
    bounded(rig.logger.entered.acquire())
        .await
        .unwrap()
        .forget();
    run.abort();
    let _ = run.await;
    let owner = rig.client.clone();
    let closing = tokio::spawn(async move { owner.dispose().await });
    bounded(async {
        while rig.client.snapshot().state != Lifecycle::Unloading {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!closing.is_finished());
    rig.logger.gate.add_permits(1);
    bounded(closing).await.unwrap().unwrap();
    assert_eq!(files.count("scope-cleanup"), 1);
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn tasks_outlive_creating_call_cancel_on_owner_close_and_clean_late_effects() {
    let files = Files::new("cordis_abi_managed");
    let rig = rig(&files, cordis_wire::json!({}), caps()).await;
    let task = invoke_json(&rig.handle, "task-start", Value::Null)
        .await
        .unwrap();
    files.wait("task-start").await;
    assert_eq!(
        invoke_json(&rig.handle, "task-status", task.clone())
            .await
            .unwrap()["state"],
        "running"
    );
    assert!(
        matches!(invoke_json(&rig.handle,"cancelled",Value::Null).await,Err(AbiError::Refused{code,..})if code=="closed")
    );
    let handle = rig.handle.clone();
    let join = tokio::spawn(async move { invoke_json(&handle, "task-join", task).await });
    tokio::task::yield_now().await;
    join.abort();
    let _ = join.await;
    bounded(rig.client.dispose()).await.unwrap();
    assert_eq!(files.count("task-stop"), 1);
    assert_eq!(files.count("task-last-cleanup"), 1);
    assert_eq!(files.count("task-first-cleanup"), 1);
    assert_eq!(
        rig.logger
            .records("cleanup")
            .iter()
            .map(|r| r.args.clone())
            .collect::<Vec<_>>(),
        vec![json!("task-last"), json!("task-first")]
    );
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn completed_tasks_cache_join_and_expire_their_context() {
    let files = Files::new("cordis_abi_managed");
    let rig = rig(&files, json!({"finite_task":true}), caps()).await;
    let task = invoke_json(&rig.handle, "task-start", Value::Null)
        .await
        .unwrap();
    invoke_json(&rig.handle, "task-join", task.clone())
        .await
        .unwrap();
    invoke_json(&rig.handle, "task-join", task).await.unwrap();
    assert!(
        matches!(invoke_json(&rig.handle,"expired",Value::Null).await,Err(AbiError::Refused{code,..})if code=="closed")
    );
    assert_eq!(files.count("task-first-cleanup"), 0);
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn failed_task_join_retains_failed_responsibility_without_retry() {
    let files = Files::new("cordis_abi_managed");
    let rig = rig(&files, json!({"fail_task":true}), caps()).await;
    invoke_json(&rig.handle, "task-start", Value::Null)
        .await
        .unwrap();
    let first = invoke_json(&rig.handle, "task-join", Value::Null)
        .await
        .unwrap_err();
    assert_eq!(
        invoke_json(&rig.handle, "task-join", Value::Null)
            .await
            .unwrap_err(),
        first
    );
    assert!(bounded(rig.rt.shutdown()).await.is_err());
    drop(rig);
    assert_eq!(files.count("task-start"), 1);
    assert_eq!(files.count("destroy"), 0);
    assert_eq!(files.count("unload"), 0);
}

#[tokio::test]
async fn managed_admission_limits_leave_no_untracked_cpp_callbacks() {
    let files = Files::new("cordis_abi_managed");
    let mut capabilities = caps();
    capabilities.managed.sequences = 1;
    capabilities.managed.steps = 1;
    capabilities.managed.scopes = 1;
    capabilities.managed.tasks = 1;
    let rig = rig(&files, json!({"finite_task":true}), capabilities).await;
    invoke_json(&rig.handle, "sequence-start", Value::Null)
        .await
        .unwrap();
    assert!(
        invoke_json(&rig.handle, "sequence-ready", Value::Null)
            .await
            .is_err()
    );
    invoke_json(&rig.handle, "sequence-dispose", Value::Null)
        .await
        .unwrap();
    assert!(
        invoke_json(&rig.handle, "sequence-start", Value::Null)
            .await
            .is_err()
    );
    invoke_json(&rig.handle, "scope-create", Value::Null)
        .await
        .unwrap();
    assert!(
        invoke_json(&rig.handle, "scope-create", Value::Null)
            .await
            .is_err()
    );
    invoke_json(&rig.handle, "task-start", Value::Null)
        .await
        .unwrap();
    invoke_json(&rig.handle, "task-join", Value::Null)
        .await
        .unwrap();
    assert!(
        invoke_json(&rig.handle, "task-start", Value::Null)
            .await
            .is_err()
    );
    bounded(rig.rt.shutdown()).await.unwrap();
    assert_eq!(files.count("destroy"), 1);
}

#[tokio::test]
async fn startup_failure_stops_tasks_and_recovers_scopes_and_sequence() {
    let files = Files::new("cordis_abi_managed");
    let rt = Runtime::new();
    let logger = Logger::new();
    install_logger(&rt, logger, View::new()).await;
    let library = files.load().with_context(caps()).unwrap();
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
    assert_eq!(files.count("task-stop"), 1);
    assert_eq!(files.count("scope-cleanup"), 1);
    assert_eq!(files.count("undo:2"), 1);
    assert_eq!(files.count("destroy"), 1);
    let _ = bounded(rt.shutdown()).await;
}

#[tokio::test]
async fn unknown_managed_callbacks_never_authorize_destroy_or_unload() {
    for method in ["sequence-start", "scope-run", "task-start"] {
        let files = Files::new("cordis_abi_managed");
        let rig = rig(
            &files,
            json!({"unknown_step":true,"unknown_task":true}),
            caps(),
        )
        .await;
        if method == "scope-run" {
            invoke_json(&rig.handle, "scope-create", Value::Null)
                .await
                .unwrap();
        }
        let _ = invoke_json(&rig.handle, method, json!("unknown")).await;
        let _ = bounded(rig.rt.shutdown()).await;
        drop(rig);
        assert_eq!(files.count("destroy"), 0);
        assert_eq!(files.count("unload"), 0);
    }
}

#[tokio::test]
async fn broker_managed_effects_and_tasks_keep_old_code_and_old_definition_dependencies() {
    let old = Files::new("cordis_abi_managed");
    let new = Files::new("cordis_abi_managed_v2");
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
                s.invoke_json(&ctx, "sequence-start", Value::Null)
                    .await
                    .map_err(Into::into)
            })
        })
        .await
        .unwrap();
    handle
        .route(|s, ctx| {
            Box::pin(async move {
                s.invoke_json(&ctx, "sequence-ready", Value::Null)
                    .await
                    .map_err(Into::into)
            })
        })
        .await
        .unwrap();
    handle
        .route(|s, ctx| {
            Box::pin(async move {
                s.invoke_json(&ctx, "task-start", Value::Null)
                    .await
                    .map_err(Into::into)
            })
        })
        .await
        .unwrap();
    // Native Context still rejects child installation through dynamic Broker routing.
    assert!(
        handle
            .route(|s, ctx| Box::pin(async move {
                s.invoke_json(&ctx, "scope-create", Value::Null)
                    .await
                    .map_err(Into::into)
            }))
            .await
            .is_err()
    );
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
    assert!(
        old_logger
            .records("cleanup")
            .iter()
            .any(|r| r.args == "v1:undo:2")
    );
    assert_eq!(old_logger.records("task-stop")[0].args, "v1");
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
async fn loader_managed_factory_preserves_capabilities_and_recovers_real_sequences() {
    use cordis_abi::loader::{register_broker_context_library, register_context_library};
    use cordis_loader::{Configuration, Entry, Loader, Registry};
    let files = Files::new("cordis_abi_managed");
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
    invoke_json(&handle, "sequence-start", Value::Null)
        .await
        .unwrap();
    invoke_json(&handle, "sequence-ready", Value::Null)
        .await
        .unwrap();
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
                s.invoke_json(&ctx, "version", Value::Null)
                    .await
                    .map_err(Into::into)
            }))
            .await
            .unwrap(),
        "v1"
    );
    bounded(rt.shutdown()).await.unwrap();
}
