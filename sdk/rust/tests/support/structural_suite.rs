use cordis_runtime::{self as rt, Plugin, Runtime, Service};
use cordis_sdk_core::{Descriptor, Dispatch, Plugin as _, MessageContract, Value, json};
use probe::{ContractProbe, dependency_interface};
use std::sync::{Arc, Mutex};

fn descriptor() -> Descriptor {
    Descriptor::new(ContractProbe::interface()).unwrap()
}
fn observer() -> Service<Mutex<Vec<Value>>> {
    Service::new("test.observer", "1")
}
fn capabilities(log: &Service<Mutex<Vec<Value>>>, invalid_result: bool) -> backend::Capabilities {
    backend::Capabilities {
        dependencies: vec![backend::Dependency::new_json(
            log,
            Descriptor::new(dependency_interface()).unwrap(),
            move |log, _, method, value| {
                Box::pin(async move {
                    if method == "echo" {
                        log.lock().unwrap().push(json!("echo"));
                        if let Some(mode) = value.as_str().and_then(|v| v.parse::<i64>().ok()).filter(|v| *v < 0) {
                            return Err(match mode {
                                -1 => backend::Error::Rejected {
                                    code: "expected".into(),
                                    message: "declared".into(),
                                },
                                -2 => backend::Error::Rejected {
                                    code: "closed".into(),
                                    message: "undeclared business error".into(),
                                },
                                -3 => backend::Error::Refused {
                                    code: "closed".into(),
                                    message: "runtime refusal".into(),
                                },
                                _ => {
                                    backend::Error::Unknown("expected: uncertain execution".into())
                                }
                            });
                        }
                        Ok(if invalid_result {
                            json!("bad dependency result")
                        } else {
                            value
                        })
                    } else {
                        log.lock().unwrap().push(value);
                        Ok(json!({}))
                    }
                })
            },
        )],
        ..Default::default()
    }
}
async fn owner<T: Send + Sync + 'static>(
    runtime: &Runtime,
    service: &Service<T>,
) -> (rt::PluginHandle, rt::ServiceHandle<T>) {
    let saved = Arc::new(Mutex::new(None));
    let target = saved.clone();
    let binding = service.clone();
    let owner = runtime
        .install(
            Plugin::new("owner", move |ctx| {
                let saved = target.clone();
                let binding = binding.clone();
                async move {
                    *saved.lock().unwrap() = Some(ctx.dependency(&binding)?);
                    Ok(())
                }
            })
            .requires(service),
        )
        .unwrap();
    owner.ready().await.unwrap();
    let handle = saved.lock().unwrap().take().unwrap();
    (owner, handle)
}
struct Rig {
    runtime: Runtime,
    provider: rt::PluginHandle,
    owner: rt::PluginHandle,
    handle: rt::ServiceHandle<Foreign>,
    log: Arc<Mutex<Vec<Value>>>,
}
impl Rig {
    async fn new(invalid_result: bool) -> Self {
        let runtime = Runtime::new();
        let log = Arc::new(Mutex::new(Vec::new()));
        let logger = observer();
        let binding = logger.clone();
        let saved = log.clone();
        runtime
            .install(
                Plugin::new("observer", move |ctx| {
                    let binding = binding.clone();
                    let log = saved.clone();
                    async move { ctx.provide(&binding, log) }
                })
                .provides(&logger),
            )
            .unwrap()
            .ready()
            .await
            .unwrap();
        let component = load(capabilities(&logger, invalid_result)).unwrap();
        let service = Service::<Foreign>::new("test.structure", "1").with_descriptor(descriptor());
        let provider = runtime
            .install(component.plugin("probe", &service, Value::Null).unwrap())
            .unwrap();
        provider.ready().await.unwrap();
        let (owner, handle) = owner(&runtime, &service).await;
        Self {
            runtime,
            provider,
            owner,
            handle,
            log,
        }
    }
    async fn invoke_json(&self, method: &'static str, args: Value) -> backend::Result<Value> {
        self.handle
            .call(move |service, ctx| {
                Box::pin(async move { Ok(service.invoke_json(&ctx, method, args).await) })
            })
            .await?
    }
    async fn acquire(&self, kind: &'static str, args: Value) -> backend::Result<ForeignResource> {
        self.handle
            .call(move |service, ctx| {
                Box::pin(async move { Ok(service.acquire_json(&ctx, kind, args).await) })
            })
            .await?
    }
}
#[test]
fn missing_or_drifted_structure_is_refused_before_business_create() {
    let component = load(capabilities(&observer(), false)).unwrap();
    assert_eq!(component.descriptor(), &descriptor());
    let service = Service::<Foreign>::new("test.structure", "1");
    assert!(matches!(
        component.plugin("missing", &service, Value::Null),
        Err(backend::Error::Contract(_))
    ));
    let mut changed = ContractProbe::interface();
    let Dispatch::Named(methods) = &mut changed.methods else {
        unreachable!()
    };
    methods.get_mut("echo").unwrap().result = MessageContract::standard("google.protobuf.StringValue");
    let service = service.with_descriptor(Descriptor::new(changed).unwrap());
    let error = component
        .plugin("drift", &service, Value::Null)
        .err()
        .unwrap();
    assert!(
        matches!(error, backend::Error::Contract(ref reason) if reason.contains("echo.result") && reason.contains("google.protobuf.StringValue") && reason.contains("google.protobuf.Int64Value")),
        "{error}"
    );
    let broker = Service::<rt::Broker<Foreign>>::new("test.structure", "1");
    assert!(matches!(
        component.broker_plugin("broker", &broker, Value::Null, |_| Ok(())),
        Err(backend::Error::Contract(_))
    ));
}
#[test]
fn dependency_structure_is_checked_before_instance_creation() {
    let mut interface = dependency_interface();
    let Dispatch::Named(methods) = &mut interface.methods else {
        unreachable!()
    };
    methods.get_mut("echo").unwrap().args = MessageContract::standard("google.protobuf.StringValue");
    let caps = backend::Capabilities {
        dependencies: vec![backend::Dependency::new_json(
            &observer(),
            Descriptor::new(interface).unwrap(),
            |_, _, _, _| Box::pin(async { panic!("preflight must not invoke dependency") }),
        )],
        ..Default::default()
    };
    assert!(matches!(load(caps), Err(backend::Error::Contract(_))));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_inputs_do_not_enter_methods_or_factories_and_valid_calls_recover() {
    let rig = Rig::new(false).await;
    assert!(matches!(
        rig.invoke_json("echo", json!("bad")).await,
        Err(backend::Error::InvalidInput(_))
    ));
    assert!(matches!(
        rig.invoke_json("missing", Value::Null).await,
        Err(backend::Error::InvalidInput(_))
    ));
    assert_eq!(rig.invoke_json("calls", json!({})).await.unwrap(), json!("0"));
    assert_eq!(rig.invoke_json("echo", json!(42)).await.unwrap(), json!("42"));
    assert_eq!(rig.invoke_json("calls", json!({})).await.unwrap(), json!("1"));
    assert!(matches!(
        rig.acquire("counter", json!("bad")).await,
        Err(backend::Error::InvalidInput(_))
    ));
    assert!(matches!(
        rig.acquire("missing", json!(0)).await,
        Err(backend::Error::InvalidInput(_))
    ));
    assert_eq!(*rig.log.lock().unwrap(), vec![json!("activate")]);
    let resource = rig.acquire("counter", json!(40)).await.unwrap();
    assert!(matches!(
        resource.invoke_json("add", json!("bad")).await,
        Err(backend::Error::InvalidInput(_))
    ));
    assert!(matches!(
        resource.invoke_json("missing", Value::Null).await,
        Err(backend::Error::InvalidInput(_))
    ));
    assert_eq!(
        resource.invoke_json("get", json!({})).await.unwrap(),
        json!("40")
    );
    assert_eq!(resource.invoke_json("add", json!(2)).await.unwrap(), json!("42"));
    resource.dispose().await.unwrap();
    resource.dispose().await.unwrap();
    // An invalid reverse input is a known rejection; the native adapter is unreachable.
    assert!(matches!(
        rig.invoke_json("reverse-bad-args", json!({})).await,
        Err(backend::Error::Refused { .. } | backend::Error::InvalidInput(_))
    ));
    assert_eq!(rig.invoke_json("reverse", json!(42)).await.unwrap(), json!("42"));
    rig.owner.dispose().await.unwrap();
    rig.provider.dispose().await.unwrap();
    assert_eq!(
        *rig.log.lock().unwrap(),
        vec![
            json!("activate"),
            json!("acquire"),
            json!("release"),
            json!("echo"),
            json!("deactivate")
        ]
    );
    rig.runtime.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_success_result_preserves_unknown_instance_responsibility() {
    let rig = Rig::new(false).await;
    assert!(matches!(
        rig.invoke_json("bad", json!({})).await,
        Err(backend::Error::Unknown(_))
    ));
    assert!(rig.invoke_json("echo", json!(42)).await.is_err());
    assert!(rig.provider.dispose().await.is_err());
    assert!(matches!(
        rig.provider.effects()[0].status,
        rt::EffectStatus::Failed(_)
    ));
    assert!(rig.runtime.shutdown().await.is_err());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_resource_result_preserves_resource_and_provider_responsibility() {
    let rig = Rig::new(false).await;
    let resource = rig.acquire("counter", json!(40)).await.unwrap();
    assert!(matches!(
        resource.invoke_json("bad", json!({})).await,
        Err(backend::Error::Unknown(_))
    ));
    assert!(resource.dispose().await.is_err());
    assert!(rig.owner.dispose().await.is_err());
    assert!(matches!(
        resource.snapshot().status,
        rt::EffectStatus::Failed(_)
    ));
    assert!(rig.provider.dispose().await.is_err());
    assert!(!rig.log.lock().unwrap().contains(&json!("release")));
    assert!(rig.runtime.shutdown().await.is_err());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_reverse_result_faults_the_foreign_instance() {
    for method in ["reverse", "reverse-suppressed"] {
        let rig = Rig::new(true).await;
        assert!(matches!(
            rig.invoke_json(method, json!(42)).await,
            Err(backend::Error::Unknown(_))
        ));
        assert!(rig.provider.dispose().await.is_err());
        assert!(rig.runtime.shutdown().await.is_err());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declared_errors_and_runtime_refusals_preserve_service_availability() {
    let rig = Rig::new(false).await;
    let resource = rig.acquire("counter", json!(0)).await.unwrap();
    for mode in [-1, -3] {
        for outcome in [
            rig.invoke_json("error", json!(mode)).await,
            rig.invoke_json("reverse", json!(mode)).await,
            resource.invoke_json("error", json!(mode)).await,
            rig.acquire("counter", json!(mode))
                .await
                .map(|_| Value::Null),
        ] {
            match mode {
                -1 => assert!(
                    matches!(outcome, Err(backend::Error::Rejected {ref code, ..}) if code == "expected"),
                    "{outcome:?}"
                ),
                _ => assert!(
                    matches!(outcome, Err(backend::Error::Refused {ref code, ..}) if code == "closed"),
                    "{outcome:?}"
                ),
            }
        }
    }
    assert_eq!(rig.invoke_json("echo", json!(42)).await.unwrap(), json!("42"));
    resource.dispose().await.unwrap();
    rig.runtime.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn undeclared_and_unknown_errors_cannot_erase_instance_or_factory_responsibility() {
    for factory in [false, true] {
        for mode in [-2, -4] {
            let rig = Rig::new(false).await;
            let outcome = if factory {
                rig.acquire("counter", json!(mode))
                    .await
                    .map(|_| Value::Null)
            } else {
                rig.invoke_json("error", json!(mode)).await
            };
            assert!(
                matches!(outcome, Err(backend::Error::Unknown(_))),
                "{outcome:?}"
            );
            assert!(rig.owner.dispose().await.is_err() || !factory);
            assert!(rig.provider.dispose().await.is_err());
            if factory {
                // The failed consumer ticket blocks the provider before its
                // inverse starts; Owned here is required by the recovery barrier.
                assert!(
                    rig.owner
                        .effects()
                        .iter()
                        .any(|e| matches!(e.status, rt::EffectStatus::Failed(_)))
                );
                assert_ne!(rig.provider.effects()[0].status, rt::EffectStatus::Released);
            } else {
                assert!(matches!(
                    rig.provider.effects()[0].status,
                    rt::EffectStatus::Failed(_)
                ));
            }
            assert!(rig.runtime.shutdown().await.is_err());
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn undeclared_resource_error_keeps_the_inverse_obligation() {
    let rig = Rig::new(false).await;
    let resource = rig.acquire("counter", json!(0)).await.unwrap();
    assert!(matches!(
        resource.invoke_json("error", json!(-2)).await,
        Err(backend::Error::Unknown(_))
    ));
    assert!(resource.dispose().await.is_err());
    assert!(rig.owner.dispose().await.is_err());
    assert!(matches!(
        resource.snapshot().status,
        rt::EffectStatus::Failed(_)
    ));
    assert!(!rig.log.lock().unwrap().contains(&json!("release")));
    assert!(rig.runtime.shutdown().await.is_err());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catching_an_undeclared_reverse_error_cannot_clear_the_host_fault() {
    for (method, mode) in [
        ("reverse", -2),
        ("reverse-suppressed", -2),
        ("reverse", -4),
        ("reverse-suppressed", -4),
    ] {
        let rig = Rig::new(false).await;
        assert!(matches!(
            rig.invoke_json(method, json!(mode)).await,
            Err(backend::Error::Unknown(_))
        ));
        assert!(rig.provider.dispose().await.is_err());
        assert!(rig.runtime.shutdown().await.is_err());
    }
}
