use cordis_portable_example::context::{ContextLogClient, data::google::protobuf::BytesValue};
use cordis_portable_example::context_fixture_interface;
use cordis_sdk_core::typed::Codec;
#[path = "support/context.rs"]
mod guest;
use cordis_sdk::*;
use cordis_wasm::{
    Capabilities, ContextComponent, ContextService, Dependency, Limits, Value, json,
};
use guest::bytes;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

struct App {
    rt: Runtime,
    logger: PluginHandle,
    provider: PluginHandle,
    client: PluginHandle,
    handle: ServiceHandle<ContextService>,
    saved: Arc<Mutex<Option<ServiceHandle<ContextService>>>>,
    log: Arc<Mutex<Vec<Value>>>,
}
async fn app() -> App {
    app_with(|_| {}).await
}
async fn app_with(configure: impl FnOnce(&mut Capabilities)) -> App {
    app_config(configure, json!({})).await
}
async fn app_config(configure: impl FnOnce(&mut Capabilities), config: Value) -> App {
    app_artifact(configure, config, false).await
}
async fn app_artifact(
    configure: impl FnOnce(&mut Capabilities),
    config: Value,
    normal: bool,
) -> App {
    let rt = Runtime::new();
    let logger = Service::<Mutex<Vec<Value>>>::new("example.context-log", "1");
    let log = Arc::new(Mutex::new(Vec::new()));
    let native = rt
        .install(
            Plugin::new("logger", {
                let logger = logger.clone();
                let log = log.clone();
                move |ctx| {
                    let logger = logger.clone();
                    let log = log.clone();
                    async move { ctx.provide(&logger, log) }
                }
            })
            .provides(&logger),
        )
        .unwrap();
    native.ready().await.unwrap();
    let mut caps = Capabilities {
        wasi: Some(cordis_wasm::Wasi::default()),
        ..Capabilities::default()
    };
    caps.dependencies.push(Dependency::new_json(
        &logger,
        cordis_contract::Descriptor::new(ContextLogClient::interface()).unwrap(),
        |log, _ctx, _method, args| {
            Box::pin(async move {
                log.lock().unwrap().push(args.clone());
                Ok(args)
            })
        },
    ));
    caps.events
        .insert("messages".into(), Event::new("messages", "1"));
    caps.middleware
        .insert("pipeline".into(), Middleware::new("pipeline", "1"));
    caps.factories.insert(
        "worker".into(),
        Arc::new(|config| {
            cordis_portable_example::context::ContextControls::worker()
                .build(cordis_sdk_core::Message::from_bytes(<cordis_portable_example::context::data::cordis::data::Unit as Codec>::contract().clone(),config).unwrap(), |_| Ok(Plugin::new("worker", |_| async { Ok(()) })))
                .map_err(|e| cordis_wasm::Error::from(Error::from(e)))
        }),
    );
    configure(&mut caps);
    let component = ContextComponent::from_bytes(
        if normal {
            guest::normal_bytes()
        } else {
            bytes()
        },
        Limits::default(),
        caps,
    )
    .unwrap();
    let service = Service::<ContextService>::new("example.context", "1").with_descriptor(
        cordis_contract::Descriptor::new(if normal {
            cordis_portable_example::context::ContextClient::interface()
        } else {
            context_fixture_interface()
        })
        .unwrap(),
    );
    let provider = rt
        .install(component.plugin("context", &service, config).unwrap())
        .unwrap();
    provider.ready().await.unwrap();
    let saved = Arc::new(Mutex::new(None));
    let client = rt
        .install(
            Plugin::new("client", {
                let saved = saved.clone();
                let service = service.clone();
                move |ctx| {
                    let saved = saved.clone();
                    let service = service.clone();
                    async move {
                        *saved.lock().unwrap() = Some(ctx.dependency(&service)?);
                        Ok(())
                    }
                }
            })
            .requires(&service),
        )
        .unwrap();
    client.ready().await.unwrap();
    let handle = saved.lock().unwrap().take().unwrap();
    App {
        rt,
        logger: native,
        provider,
        client,
        handle,
        saved,
        log,
    }
}

#[tokio::test]
async fn repeated_ownership_realm_and_restart_preserve_managed_dependencies_and_effects() {
    let mut app = app().await;
    tokio::time::timeout(Duration::from_secs(120), async {
        let mut owner = app
            .rt
            .install(Plugin::new("old group", |_| async { Ok(()) }))
            .unwrap();
        owner.ready().await.unwrap();
        app.rt
            .reparent(&[
                (app.logger.clone(), owner.snapshot().activation),
                (app.provider.clone(), owner.snapshot().activation),
                (app.client.clone(), owner.snapshot().activation),
            ])
            .unwrap();
        let baseline_effects = app.provider.effects().len();
        for round in 0..16 {
            let before = [
                app.logger.snapshot(),
                app.provider.snapshot(),
                app.client.snapshot(),
            ];
            let old_handle = app.handle.clone();
            let new_group = app
                .rt
                .install(Plugin::new(format!("owner-{round}"), |_| async { Ok(()) }))
                .unwrap();
            new_group.ready().await.unwrap();
            let service = Service::<()>::new("example.context", "1");
            let logger = Service::<()>::new("example.context-log", "1");
            let moved = View::new().isolate(&service).isolate(&logger);
            let tag = format!("before move {round}");
            app.call("subscribe", json!(tag)).await.unwrap();
            app.rt
                .reparent(&[
                    (app.logger.clone(), new_group.snapshot().activation),
                    (app.provider.clone(), new_group.snapshot().activation),
                    (app.client.clone(), new_group.snapshot().activation),
                ])
                .unwrap();
            owner.dispose().await.unwrap();
            let report = app
                .rt
                .reconfigure_realms(&[
                    (app.logger.clone(), moved.clone()),
                    (app.provider.clone(), moved.clone()),
                    (app.client.clone(), moved),
                ])
                .unwrap();
            assert!(report.withdrawn.is_empty());
            for (handle, old) in [&app.logger, &app.provider, &app.client]
                .into_iter()
                .zip(&before)
            {
                assert_eq!(handle.snapshot().activation, old.activation);
            }
            let message = format!("after move {round}");
            assert_eq!(
                app.call("echo", json!(message)).await.unwrap(),
                json!(message)
            );
            app.provider.restart().await.unwrap();
            app.client.ready().await.unwrap();
            app.handle = app.saved.lock().unwrap().take().unwrap();
            assert_eq!(app.logger.snapshot().activation, before[0].activation);
            assert_eq!(
                app.provider.snapshot().activation.unwrap().epoch,
                before[1].activation.unwrap().epoch + 1
            );
            assert_eq!(
                app.client.snapshot().activation.unwrap().epoch,
                before[2].activation.unwrap().epoch + 1
            );
            assert!(
                old_handle
                    .call(|s, ctx| Box::pin(async move {
                        Ok(s.invoke_json(&ctx, "echo", json!(null)).await?)
                    }))
                    .await
                    .is_err()
            );
            assert_eq!(
                app.log
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|v| **v == json!({"released":tag}))
                    .count(),
                1
            );
            assert!(app.client.effects().is_empty());
            assert_eq!(app.provider.effects().len(), baseline_effects);
            assert_eq!(
                app.rt
                    .snapshots()
                    .iter()
                    .filter(|s| s.state != Lifecycle::Inactive)
                    .count(),
                4
            );
            owner = new_group;
        }
        owner.dispose().await.unwrap();
        assert!(app.provider.snapshot().retired);
        assert!(app.client.snapshot().retired);
        app.rt.shutdown().await.unwrap();
        assert!(
            app.rt
                .snapshots()
                .iter()
                .all(|s| s.state == Lifecycle::Inactive)
        );
        assert!(app.rt.diagnostics().is_empty());
    })
    .await
    .expect("managed composition did not drain");
}
impl App {
    async fn call(&self, method: &str, args: Value) -> Result<Value> {
        let method = method.to_owned();
        self.handle
            .call(move |s, ctx| {
                Box::pin(
                    async move { s.invoke_json(&ctx, &method, args).await.map_err(Into::into) },
                )
            })
            .await
    }
}

#[tokio::test]
async fn dynamic_intercepts_are_pinned_through_real_wasm_callbacks_and_cleanup() {
    let app = app().await;
    let service = Service::<ContextService>::new("example.context", "1")
        .with_descriptor(cordis_contract::Descriptor::new(context_fixture_interface()).unwrap());
    let make = |revision| {
        let mut config = InterceptConfig::new(revision);
        config.set(&service, json!({"timeout":revision * 1000}));
        config
    };
    let source = Intercepts::new(make(1));
    let saved = Arc::new(Mutex::new(None));
    let client = app
        .rt
        .install_in(View::new().with_intercepts(source.clone()), {
            let saved = saved.clone();
            let declaration = service.clone();
            Plugin::new("metadata client", move |ctx| {
                let saved = saved.clone();
                let service = declaration.clone();
                async move {
                    *saved.lock().unwrap() = Some(ctx.dependency(&service)?);
                    Ok(())
                }
            })
            .requires(&service)
        })
        .unwrap();
    client.ready().await.unwrap();
    let handle = saved.lock().unwrap().take().unwrap();
    for (name, revision) in [("example.context", "2"), ("private.service", "1")] {
        let error = handle
            .call(move |s, ctx| {
                Box::pin(async move {
                    Ok(s.invoke_json(
                        &ctx,
                        "metadata-for",
                        json!({"name":name,"revision":revision}),
                    )
                    .await)
                })
            })
            .await
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(error,cordis_wasm::Error::Refused {code,..} if code=="undeclared_metadata")
        );
    }
    let before = handle
        .call(|s, ctx| {
            Box::pin(async move { Ok(s.invoke_json(&ctx, "metadata-defer", json!({})).await?) })
        })
        .await
        .unwrap();
    assert_eq!(
        before,
        json!({"revision":"1","layers":[{"timeout":1000.0}]})
    );
    source.replace(make(3));
    let after = handle
        .call(|s, ctx| {
            Box::pin(async move { Ok(s.invoke_json(&ctx, "metadata", json!({})).await?) })
        })
        .await
        .unwrap();
    assert_eq!(after, json!({"revision":"3","layers":[{"timeout":3000.0}]}));
    client.dispose().await.unwrap();
    assert!(
        app.log
            .lock()
            .unwrap()
            .contains(&json!({"metadata_cleanup":before}))
    );
    app.rt.shutdown().await.unwrap();
}
#[tokio::test]
async fn context_dependencies_effects_and_expiring_authority() {
    let app = app().await;
    assert_eq!(app.call("echo", json!(42)).await.unwrap(), json!(42.0));
    let info = app.call("info", json!({})).await.unwrap();
    assert_ne!(info["owner"], info["definition"]);
    app.call("subscribe", json!("first")).await.unwrap();
    app.call("subscribe", json!("second")).await.unwrap();
    app.call("release", json!({})).await.unwrap();
    app.call("release", json!({})).await.unwrap();
    app.call("stale", json!({})).await.unwrap();
    assert_eq!(app.client.effects().len(), 3);
    app.provider.dispose().await.unwrap();
    assert_eq!(
        *app.log.lock().unwrap(),
        vec![
            json!("activate"),
            json!(42.0),
            json!({"released":"second"}),
            json!({"released":"first"}),
            json!("deactivate")
        ]
    );
    app.rt.shutdown().await.unwrap();
}
#[tokio::test]
async fn scopes_sequences_and_authorized_child_factories() {
    let app = app().await;
    for method in ["scope", "sequence", "install"] {
        app.call(method, json!({})).await.unwrap();
    }
    app.rt.shutdown().await.unwrap();
}
#[tokio::test]
async fn same_instance_events_and_middleware_can_suspend_and_reenter() {
    let app = app().await;
    assert_eq!(
        app.call("events", json!({})).await.unwrap(),
        json!({"continued":{}})
    );
    assert_eq!(app.call("middleware", json!({})).await.unwrap(), "42");
    app.rt.shutdown().await.unwrap();
}
#[tokio::test]
async fn background_task_is_cancelled_before_provider_cleanup() {
    let app = app().await;
    app.call("task", json!({})).await.unwrap();
    app.provider.dispose().await.unwrap();
    assert_eq!(
        *app.log.lock().unwrap(),
        vec![
            json!("activate"),
            json!("task-stopped"),
            json!("deactivate")
        ]
    );
    app.rt.shutdown().await.unwrap();
}
#[test]
fn dependency_manifest_must_match_explicit_bindings() {
    let caps = Capabilities {
        wasi: Some(cordis_wasm::Wasi::default()),
        ..Capabilities::default()
    };
    assert!(matches!(
        ContextComponent::from_bytes(bytes(), Limits::default(), caps),
        Err(cordis_wasm::Error::Contract(_))
    ));
}

#[tokio::test]
async fn wasi_environment_is_explicit_and_default_files_network_are_denied() {
    let app = app_with(|caps| {
        caps.wasi
            .as_mut()
            .unwrap()
            .environment
            .insert("CORDIS_VISIBLE".into(), "yes".into());
    })
    .await;
    assert_eq!(
        app.call("wasi-env", json!({})).await.unwrap(),
        json!({"entries":[{"name":"CORDIS_VISIBLE","value":"yes"}]})
    );
    assert_eq!(
        app.call("wasi-random", json!({}))
            .await
            .unwrap()
            .as_str()
            .map(|v| BytesValue::decode(json!(v)).unwrap().value.len())
            .unwrap(),
        8
    );
    assert!(
        app.call("wasi-clock", json!({}))
            .await
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > 0
    );
    assert!(
        app.call("wasi-read", json!({"path":"value.txt"}))
            .await
            .is_err()
    );
    assert!(app.call("wasi-connect", json!({"port":80})).await.is_err());
    app.rt.shutdown().await.unwrap();
}
#[tokio::test]
async fn wasi_read_only_mount_confines_paths_and_read_write_is_opt_in() {
    let directory = std::env::temp_dir().join(format!("cordis-wasi-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("value.txt"), "hello").unwrap();
    let app = app_with(|caps| {
        caps.wasi
            .as_mut()
            .unwrap()
            .directories
            .push(cordis_wasm::Directory {
                host: directory.clone(),
                guest: "/data".into(),
                writable: false,
            });
    })
    .await;
    assert_eq!(
        app.call("wasi-read", json!({"path":"value.txt"}))
            .await
            .unwrap(),
        "aGVsbG8="
    );
    assert!(
        app.call(
            "wasi-write",
            json!({"path":"value.txt","data":"ZnJvbSB3YXNt"})
        )
        .await
        .is_err()
    );
    assert!(
        app.call("wasi-read", json!({"path":"../value.txt"}))
            .await
            .is_err()
    );
    assert!(
        app.call("wasi-read", json!({"path":"/etc/passwd"}))
            .await
            .is_err()
    );
    app.rt.shutdown().await.unwrap();
    let app = app_with(|caps| {
        caps.wasi
            .as_mut()
            .unwrap()
            .directories
            .push(cordis_wasm::Directory {
                host: directory.clone(),
                guest: "/data".into(),
                writable: true,
            });
    })
    .await;
    app.call("wasi-write", json!({"path":"value.txt","data":"AP9B"}))
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(directory.join("value.txt")).unwrap(),
        vec![0, 255, 65]
    );
    assert_eq!(
        app.call("wasi-read", json!({"path":"value.txt"}))
            .await
            .unwrap(),
        "AP9B"
    );
    app.rt.shutdown().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
#[tokio::test]
async fn wasi_tcp_connect_requires_exact_address_grant() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let app = app_with(|caps| {
        caps.wasi.as_mut().unwrap().tcp_connect.push(address);
    })
    .await;
    app.call("wasi-connect", json!({"port":address.port()}))
        .await
        .unwrap();
    assert!(
        app.call(
            "wasi-connect",
            json!({"port":address.port().wrapping_add(1)})
        )
        .await
        .is_err()
    );
    // Outbound connect needs a local ephemeral bind, but that must not grant
    // permission to listen on that local address.
    assert!(app.call("wasi-listen", json!({})).await.is_err());
    app.rt.shutdown().await.unwrap();
}
#[tokio::test]
async fn wasi_tcp_listen_requires_an_explicit_bind_grant() {
    let app = app_with(|caps| {
        caps.wasi
            .as_mut()
            .unwrap()
            .tcp_bind
            .push("0.0.0.0:0".parse().unwrap());
    })
    .await;
    app.call("wasi-listen", json!({})).await.unwrap();
    assert!(app.call("wasi-connect", json!({"port":80})).await.is_err());
    app.rt.shutdown().await.unwrap();
}
#[tokio::test]
async fn wasi_tcp_transfers_bytes_through_granted_socket() {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut request = [0; 5];
        stream.read_exact(&mut request).unwrap();
        assert_eq!(&request, b"hello");
        stream.write_all(b"world").unwrap();
    });
    let app = app_with(|caps| caps.wasi.as_mut().unwrap().tcp_connect.push(address)).await;
    assert_eq!(
        app.call("wasi-exchange", json!({"port":address.port()}))
            .await
            .unwrap(),
        "d29ybGQ="
    );
    server.join().unwrap();
    app.rt.shutdown().await.unwrap();
}
#[tokio::test]
async fn tasks_outlive_ordinary_call_deadlines() {
    let app = app_with(|caps| caps.call_timeout = std::time::Duration::from_millis(100)).await;
    app.call("task", json!({})).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    app.call("echo", json!(42)).await.unwrap();
    app.rt.shutdown().await.unwrap();
}
#[tokio::test]
async fn effect_admission_limit_does_not_fault_healthy_instance() {
    let app = app_with(|caps| caps.max_handles = 1).await;
    app.call("subscribe", json!("one")).await.unwrap();
    assert!(app.call("subscribe", json!("two")).await.is_err());
    assert_eq!(app.call("echo", json!(42)).await.unwrap(), json!(42.0));
    app.rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_registration_returns_reserved_capacity() {
    let app = app_with(|caps| caps.max_handles = 1).await;
    for _ in 0..3 {
        assert!(app.call("invalid-registration", json!({})).await.is_err());
    }
    app.call("subscribe", json!("one")).await.unwrap();
    app.rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn sequence_collects_final_inverse_before_completion_or_failure() {
    for method in ["sequence", "sequence-failure"] {
        let app = app().await;
        app.call(method, json!({})).await.unwrap();
        app.rt.shutdown().await.unwrap();
        assert_eq!(
            *app.log.lock().unwrap(),
            vec![
                json!("activate"),
                json!({"step":3.0}),
                json!({"step":2.0}),
                json!({"step":1.0}),
                json!("deactivate")
            ]
        );
    }
}

#[tokio::test]
async fn cancelled_wait_retains_native_call_and_late_inverse_before_provider_cleanup() {
    for inner in [false, true] {
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let resume = Arc::new(tokio::sync::Semaphore::new(0));
        let app = app_with(|caps| {
            let entered = entered.clone();
            let resume = resume.clone();
            caps.dependencies = vec![Dependency::new_json(
                &Service::<Mutex<Vec<Value>>>::new("example.context-log", "1"),
                cordis_contract::Descriptor::new(ContextLogClient::interface()).unwrap(),
                move |log, _, method, args| {
                    let entered = entered.clone();
                    let resume = resume.clone();
                    Box::pin(async move {
                        if method == "wait" {
                            entered.add_permits(1);
                            resume.acquire().await.unwrap().forget();
                        } else {
                            log.lock().unwrap().push(args.clone());
                        }
                        Ok(args)
                    })
                },
            )];
        })
        .await;
        let handle = app.handle.clone();
        let (cancel, cancelled) = tokio::sync::oneshot::channel();
        let waiter = tokio::spawn(async move {
            handle.call(move |s, ctx| Box::pin(async move {
                if inner {
                    tokio::select! {
                        _ = s.invoke_json(&ctx, "delayed-defer", json!({})) => panic!("dependency must stay blocked"),
                        _ = cancelled => {},
                    }
                    Ok(())
                } else {
                    s.invoke_json(&ctx, "delayed-defer", json!({})).await?;
                    Ok(())
                }
            })).await
        });
        tokio::time::timeout(Duration::from_secs(5), entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        if inner {
            cancel.send(()).unwrap();
            waiter.await.unwrap().unwrap();
        } else {
            waiter.abort();
            assert!(waiter.await.unwrap_err().is_cancelled());
        }
        let provider = app.provider.clone();
        let mut cleanup = tokio::spawn(async move { provider.dispose().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut cleanup)
                .await
                .is_err()
        );
        assert_eq!(*app.log.lock().unwrap(), vec![json!("activate")]);
        resume.add_permits(1);
        tokio::time::timeout(Duration::from_secs(5), cleanup)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            *app.log.lock().unwrap(),
            vec![
                json!("activate"),
                json!("late-cleanup"),
                json!("deactivate")
            ]
        );
        app.rt.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn failed_inverse_is_cached_and_blocks_provider_cleanup() {
    let app = app().await;
    app.call("failed-defer", json!({})).await.unwrap();
    let first = app.provider.dispose().await.unwrap_err();
    assert_eq!(first, app.provider.dispose().await.unwrap_err());
    assert_eq!(
        *app.log.lock().unwrap(),
        vec![json!("activate"), json!("cleanup-attempt")]
    );
    assert!(
        app.client
            .effects()
            .iter()
            .any(|e| matches!(e.status, EffectStatus::Failed(_)))
    );
    assert!(app.rt.shutdown().await.is_err());
}

#[tokio::test]
async fn traps_fuel_suspended_calls_and_blocking_wasi_keep_failed_responsibility() {
    for method in [
        "trap",
        "trap-after-request",
        "spin",
        "pending",
        "wasi-block",
    ] {
        let app = app_with(|caps| caps.call_timeout = Duration::from_secs(1)).await;
        let error = tokio::time::timeout(Duration::from_secs(5), app.call(method, json!({})))
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("Unknown"), "{method}: {error}");
        assert!(
            tokio::time::timeout(Duration::from_secs(5), app.provider.dispose())
                .await
                .unwrap()
                .is_err()
        );
        assert!(
            app.provider
                .effects()
                .iter()
                .any(|e| matches!(e.status, EffectStatus::Failed(_)))
        );
        assert!(app.rt.shutdown().await.is_err());
    }
}

#[tokio::test]
async fn restart_rejects_old_handles_and_recovers_old_effects() {
    let app = app().await;
    app.call("subscribe", json!("old")).await.unwrap();
    app.provider.restart().await.unwrap();
    app.client.ready().await.unwrap();
    assert!(app.call("echo", json!(42)).await.is_err());
    app.rt.shutdown().await.unwrap();
    assert_eq!(
        *app.log.lock().unwrap(),
        vec![
            json!("activate"),
            json!({"released":"old"}),
            json!("deactivate"),
            json!("activate"),
            json!("deactivate")
        ]
    );
}

#[tokio::test]
async fn value_provision_requires_explicit_binding_and_initialization() {
    let settings = Service::<cordis_wasm::Bytes>::new("example.settings", "1");
    let app = app_config(
        |caps| {
            caps.provisions.insert("settings".into(), settings.clone());
        },
        json!({"provide":{"answer":42}}),
    )
    .await;
    let client = app
        .rt
        .install(
            Plugin::new("settings-reader", {
                let settings = settings.clone();
                move |ctx| {
                    let settings = settings.clone();
                    async move {
                        ctx.dependency(&settings)?
                            .call(|value, _| {
                                Box::pin(async move {
                                    assert_eq!(
                                        cordis_sdk_core::Message::from_bytes(
                                            cordis_sdk_core::MessageContract::json_value(),
                                            value.clone()
                                        )
                                        .unwrap()
                                        .to_json()
                                        .unwrap(),
                                        json!({"answer":42.0})
                                    );
                                    Ok(())
                                })
                            })
                            .await
                    }
                }
            })
            .requires(&settings),
        )
        .unwrap();
    client.ready().await.unwrap();
    assert!(app.call("provide", json!(43)).await.is_err());
    assert!(app.call("install-denied", json!({})).await.is_err());
    assert_eq!(app.call("echo", json!(42)).await.unwrap(), json!(42.0));
    app.rt.shutdown().await.unwrap();
}

#[test]
fn wasi_imports_are_disabled_without_opt_in() {
    assert!(
        ContextComponent::from_bytes(bytes(), Limits::default(), Capabilities::default()).is_err()
    );
}

#[tokio::test]
async fn real_instance_must_preserve_preflight_manifest() {
    for environment in [
        "CORDIS_TEST_CONTRACT",
        "CORDIS_TEST_INTERFACE",
        "CORDIS_TEST_DEPENDENCY",
    ] {
        let logger = Service::<()>::new("example.context-log", "1");
        let caps = Capabilities {
            dependencies: vec![Dependency::new_json(
                &logger,
                cordis_contract::Descriptor::new(ContextLogClient::interface()).unwrap(),
                |_, _, _, _| Box::pin(async { Ok(json!({})) }),
            )],
            wasi: Some(cordis_wasm::Wasi {
                environment: [(environment.into(), "changed".into())].into(),
                ..Default::default()
            }),
            ..Capabilities::default()
        };
        let component = ContextComponent::from_bytes(bytes(), Limits::default(), caps).unwrap();
        let rt = Runtime::new();
        let native = rt
            .install(
                Plugin::new("logger", {
                    let logger = logger.clone();
                    move |ctx| {
                        let logger = logger.clone();
                        async move { ctx.provide(&logger, Arc::new(())) }
                    }
                })
                .provides(&logger),
            )
            .unwrap();
        native.ready().await.unwrap();
        let provider = rt
            .install(
                component
                    .plugin(
                        "changed",
                        &Service::new("example.context", "1").with_descriptor(
                            cordis_contract::Descriptor::new(context_fixture_interface()).unwrap(),
                        ),
                        json!({}),
                    )
                    .unwrap(),
            )
            .unwrap();
        assert!(
            provider
                .ready()
                .await
                .unwrap_err()
                .to_string()
                .contains("manifest changed")
        );
        assert!(provider.dispose().await.is_err());
        assert!(rt.shutdown().await.is_err());
    }
}

#[tokio::test]
async fn normal_context_has_generated_business_and_rejects_bad_inputs_before_side_effects() {
    let app = app_artifact(|_| {}, json!({}), true).await;
    for (method, args) in [
        ("subscribe", json!(42)),
        ("events", json!(42)),
        ("wasi-clock", Value::Null),
        ("wasi-connect", json!({"port":0})),
        ("wasi-connect", json!({"port":65536})),
        ("wasi-connect", json!({"port":-1})),
        ("wasi-connect", json!(80)),
        ("wasi-write", json!({"path":"file", "data":"%%%"})),
        ("wasi-read", json!({})),
        ("trap", json!({})),
        ("provide", json!(42)),
    ] {
        let result = app
            .handle
            .call(move |s, ctx| {
                Box::pin(async move { Ok(s.invoke_json(&ctx, method, args).await) })
            })
            .await
            .unwrap();
        assert!(
            matches!(result, Err(cordis_wasm::Error::InvalidInput(_))),
            "{method}: {result:?}"
        );
    }
    assert_eq!(*app.log.lock().unwrap(), vec![json!("activate")]);
    assert!(app.client.effects().is_empty());
    assert_eq!(app.call("echo", Value::Null).await.unwrap(), Value::Null);
    assert_eq!(app.call("middleware", json!({})).await.unwrap(), "42");
    assert_eq!(
        app.call("events", json!({})).await.unwrap(),
        json!({"continued":{}})
    );
    app.call("scope", json!({})).await.unwrap();
    app.call("install", json!({})).await.unwrap();
    app.rt.shutdown().await.unwrap();
}
