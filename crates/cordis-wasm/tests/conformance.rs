mod support;
use cordis_sdk::*;
use cordis_wasm::{Component, Error as WasmError, Limits, Value, WasmResource, WasmService, json};
use std::sync::{Arc, Mutex};

struct App {
    rt: Runtime,
    provider: PluginHandle,
    client: PluginHandle,
    saved: Arc<Mutex<Option<ServiceHandle<WasmService>>>>,
}
impl App {
    async fn new(config: Value) -> Self {
        Self::with(support::component(), config).await
    }
    async fn with(component: Component, config: Value) -> Self {
        Self::with_descriptor(
            component,
            cordis_contract::portable::decode(
                &cordis_test_contracts::resources::rust_fixture::SERVICE
                    .parse()
                    .unwrap(),
            )
            .unwrap(),
            config,
        )
        .await
    }
    async fn with_descriptor(
        component: Component,
        descriptor: cordis_contract::Descriptor,
        config: Value,
    ) -> Self {
        let rt = Runtime::new();
        let service = Service::new("example.calculator", "1").with_descriptor(descriptor);
        let provider = rt
            .install(component.plugin("wasm", &service, config).unwrap())
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
        Self {
            rt,
            provider,
            client,
            saved,
        }
    }
    fn handle(&self) -> ServiceHandle<WasmService> {
        self.saved.lock().unwrap().as_ref().unwrap().clone()
    }
    async fn invoke_json(&self, method: &str, args: Value) -> cordis_wasm::Result<Value> {
        let method = method.to_owned();
        self.handle()
            .call(move |s, c| Box::pin(async move { Ok(s.invoke_json(&c, &method, args).await) }))
            .await
            .unwrap()
    }
    async fn counter(&self) -> cordis_wasm::Result<WasmResource> {
        self.handle()
            .call(|s, c| {
                Box::pin(
                    async move { Ok(s.acquire_json(&c, "counter", json!({"initial":40})).await) },
                )
            })
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn real_component_reuses_instance_and_owns_resources_at_call_site() {
    let app = App::new(json!({})).await;
    let value = json!({"中文":[null,true,42.0,"hello"]});
    assert_eq!(app.invoke_json("echo", value.clone()).await.unwrap(), value);
    assert_eq!(
        app.invoke_json("add", json!({"left":20,"right":22}))
            .await
            .unwrap(),
        json!("42")
    );
    let counter = app.counter().await.unwrap();
    assert_eq!(
        counter.invoke_json("add", json!(2)).await.unwrap(),
        json!("42")
    );
    assert_eq!(app.client.effects().len(), 1);
    assert_eq!(app.provider.effects().len(), 1);
    let stats = app.invoke_json("stats", json!({})).await.unwrap();
    assert_eq!(stats["calls"], "3");
    assert_eq!(stats["leases"], "1");
    counter.dispose().await.unwrap();
    counter.dispose().await.unwrap();
    assert_eq!(
        app.invoke_json("stats", json!({})).await.unwrap()["releases"],
        "1"
    );
    assert!(matches!(
        counter.invoke_json("get", json!({})).await,
        Err(WasmError::Closed)
    ));
    app.rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn normal_calculator_rejects_fault_hooks_and_bad_fields_without_mutating_state() {
    let descriptor = cordis_contract::portable::decode(
        &cordis_example_contracts::resources::calculator_wasm::SERVICE
            .parse()
            .unwrap(),
    )
    .unwrap();
    let component = support::normal_component();
    let fixture = Service::<WasmService>::new("example.calculator", "1").with_descriptor(
        cordis_contract::portable::decode(
            &cordis_test_contracts::resources::rust_fixture::SERVICE
                .parse()
                .unwrap(),
        )
        .unwrap(),
    );
    assert!(matches!(
        component.plugin("wrong-surface", &fixture, json!({})),
        Err(WasmError::Contract(_))
    ));
    let app = App::with_descriptor(component, descriptor, json!({})).await;
    for (method, args) in [
        ("add", json!({"left": 1})),
        ("add", json!({"left": "invalid", "right": 2})),
        ("add", json!({"left": 1, "right": 2, "extra": 3})),
        ("stats", json!({"extra":true})),
        ("trap", json!({})),
        ("spin", json!({})),
    ] {
        assert!(matches!(
            app.invoke_json(method, args).await,
            Err(WasmError::InvalidInput(_))
        ));
    }
    for args in [
        json!({"initial": []}),
        json!({"initial": "9223372036854775808"}),
        json!({"wait": "blocked"}),
    ] {
        let result = app
            .handle()
            .call(move |s, c| {
                Box::pin(async move { Ok(s.acquire_json(&c, "counter", args).await) })
            })
            .await
            .unwrap();
        assert!(matches!(result, Err(WasmError::InvalidInput(_))));
    }
    assert!(app.client.effects().is_empty());
    assert_eq!(
        app.invoke_json("stats", json!({})).await.unwrap(),
        json!({"calls":"1","leases":"0","releases":"0"})
    );
    let counter = app.counter().await.unwrap();
    for (method, args) in [
        ("add", json!("not an integer")),
        ("get", json!({"extra":true})),
        ("error", json!(-1)),
    ] {
        assert!(matches!(
            counter.invoke_json(method, args).await,
            Err(WasmError::InvalidInput(_))
        ));
    }
    assert!(
        matches!(counter.invoke_json("add", json!(i64::MAX.to_string())).await,
        Err(WasmError::Rejected {code, ..}) if code == "overflow")
    );
    assert_eq!(counter.invoke_json("get", json!({})).await.unwrap(), "40");
    assert_eq!(counter.invoke_json("add", json!(2)).await.unwrap(), "42");
    counter.dispose().await.unwrap();
    assert_eq!(
        app.invoke_json("stats", json!({})).await.unwrap(),
        json!({"calls":"2","leases":"0","releases":"1"})
    );
    app.rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn restart_replaces_instance_and_rejects_old_handles() {
    let app = App::new(json!({})).await;
    let old = app.handle();
    let counter = app.counter().await.unwrap();
    app.provider.restart().await.unwrap();
    app.client.ready().await.unwrap();
    assert!(matches!(
        old.call(|_, _| Box::pin(async { Ok(()) })).await,
        Err(Error::Closed)
    ));
    assert!(matches!(
        counter.invoke_json("get", json!({})).await,
        Err(WasmError::Closed)
    ));
    assert_eq!(
        app.invoke_json("stats", json!({})).await.unwrap(),
        json!({"calls":"1","leases":"0","releases":"0"})
    );
    app.rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn compiled_component_has_independent_stores() {
    let first = App::new(json!({})).await;
    let second = App::new(json!({})).await;
    let counter = first.counter().await.unwrap();
    counter.invoke_json("add", json!(5)).await.unwrap();
    assert_eq!(
        second.invoke_json("stats", json!({})).await.unwrap()["leases"],
        "0"
    );
    first.rt.shutdown().await.unwrap();
    assert_eq!(
        second
            .counter()
            .await
            .unwrap()
            .invoke_json("get", json!({}))
            .await
            .unwrap(),
        "40"
    );
    second.rt.shutdown().await.unwrap();
}

#[test]
fn validates_contract_artifact_and_imports_before_publication() {
    let component = support::component();
    let wrong = Service::<WasmService>::new("example.calculator", "2").with_descriptor(
        cordis_contract::portable::decode(
            &cordis_test_contracts::resources::rust_fixture::SERVICE
                .parse()
                .unwrap(),
        )
        .unwrap(),
    );
    assert!(matches!(
        component.plugin("bad", &wrong, Value::Null),
        Err(WasmError::Contract(_))
    ));
    assert!(Component::from_bytes(b"invalid wasm", Limits::default()).is_err());
    assert!(
        Component::from_bytes(
            support::bytes(),
            Limits {
                artifact_bytes: 8,
                ..Limits::default()
            }
        )
        .is_err()
    );
    let imports = wat::parse_str(r#"(component (import "ambient" (func)))"#).unwrap();
    assert!(Component::from_bytes(&imports, Limits::default()).is_err());
    let exhausted = Limits {
        fuel_per_call: 1,
        ..Limits::default()
    };
    assert!(Component::from_bytes(support::bytes(), exhausted).is_err());
    let memory = Limits {
        memory_bytes: 65536,
        ..Limits::default()
    };
    assert!(Component::from_bytes(support::bytes(), memory).is_err());
}

#[tokio::test]
async fn rejected_calls_and_invalid_input_do_not_fault_provider() {
    let app = App::new(json!({})).await;
    assert!(matches!(
        app.invoke_json("absent", Value::Null).await,
        Err(WasmError::InvalidInput(_))
    ));
    assert!(matches!(
        app.invoke_json("add", json!({"left":1.5,"right":2})).await,
        Err(WasmError::InvalidInput(_))
    ));
    assert!(
        matches!(app.invoke_json("add", json!({"left":9007199254740991i64,"right":1})).await, Err(WasmError::Rejected {code,..}) if code == "overflow")
    );
    assert_eq!(
        app.invoke_json("echo", json!(42)).await.unwrap(),
        json!(42.0)
    );
    app.rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn resource_limit_releases_capacity_after_confirmed_cleanup() {
    let component = Component::from_bytes(
        support::bytes(),
        Limits {
            resources: 1,
            ..Limits::default()
        },
    )
    .unwrap();
    let app = App::with(component, json!({})).await;
    let first = app.counter().await.unwrap();
    assert!(matches!(app.counter().await, Err(WasmError::Overloaded)));
    first.dispose().await.unwrap();
    app.counter().await.unwrap();
    app.rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejected_acquisition_leaves_no_failed_responsibility() {
    let app = App::new(json!({"fail":"acquire"})).await;
    assert!(matches!(
        app.counter().await,
        Err(WasmError::Rejected { .. })
    ));
    assert!(
        app.client
            .effects()
            .iter()
            .all(|e| e.status == EffectStatus::Released)
    );
    app.rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn unknown_acquisition_retains_failed_ticket() {
    let app = App::new(json!({"fail":"acquire","unknown":true})).await;
    assert!(matches!(app.counter().await, Err(WasmError::Unknown(_))));
    assert!(app.provider.dispose().await.is_err());
    assert!(
        app.client
            .effects()
            .iter()
            .any(|e| matches!(e.status, EffectStatus::Failed(_)))
    );
    assert!(app.rt.shutdown().await.is_err());
}

async fn fault(method: &str) {
    let app = App::new(json!({})).await;
    assert!(matches!(
        app.invoke_json(method, json!({})).await,
        Err(WasmError::Unknown(_))
    ));
    assert!(app.provider.dispose().await.is_err());
    assert!(
        app.provider
            .effects()
            .iter()
            .any(|e| matches!(e.status, EffectStatus::Failed(_)))
    );
    assert!(app.rt.shutdown().await.is_err());
}
#[tokio::test]
async fn trap_is_unknown() {
    fault("trap").await;
}
#[tokio::test]
async fn fuel_bounds_infinite_loop() {
    fault("spin").await;
}
#[tokio::test]
async fn memory_growth_is_bounded() {
    fault("memory").await;
}
#[tokio::test]
async fn malformed_lifted_value_faults_provider() {
    fault("invalid").await;
}

#[tokio::test]
async fn release_and_resource_destructor_failures_are_cached() {
    for stage in ["release", "lease-drop"] {
        let app = App::new(json!({"fail":stage})).await;
        let resource = app.counter().await.unwrap();
        let first = resource.dispose().await.unwrap_err();
        assert_eq!(first, resource.dispose().await.unwrap_err());
        assert!(matches!(
            resource.snapshot().status,
            EffectStatus::Failed(_)
        ));
        assert!(app.provider.dispose().await.is_err());
        assert!(app.rt.shutdown().await.is_err());
    }
}

#[tokio::test]
async fn deactivation_close_and_instance_destructor_failures_are_cached() {
    for stage in ["deactivate", "close", "instance-drop"] {
        let app = App::new(json!({"fail":stage})).await;
        let first = app.provider.dispose().await.unwrap_err();
        assert_eq!(first, app.provider.dispose().await.unwrap_err());
        assert!(matches!(
            app.provider.effects()[0].status,
            EffectStatus::Failed(_)
        ));
        assert!(app.rt.shutdown().await.is_err());
    }
}

#[tokio::test]
async fn failed_startup_recovers_known_instances_and_preserves_unknown() {
    for (stage, unknown) in [
        ("create", false),
        ("activate", false),
        ("create", true),
        ("activate", true),
    ] {
        let rt = Runtime::new();
        let service = Service::new("example.calculator", "1").with_descriptor(
            cordis_contract::portable::decode(
                &cordis_test_contracts::resources::rust_fixture::SERVICE
                    .parse()
                    .unwrap(),
            )
            .unwrap(),
        );
        let provider = rt
            .install(
                support::component()
                    .plugin("wasm", &service, json!({"fail":stage,"unknown":unknown}))
                    .unwrap(),
            )
            .unwrap();
        assert!(provider.ready().await.is_err());
        let _ = provider.dispose().await;
        if unknown {
            assert_eq!(provider.effects().len(), 1);
            assert!(matches!(
                provider.effects()[0].status,
                EffectStatus::Failed(_)
            ));
        } else {
            assert!(provider.effects().is_empty());
        }
        let _ = rt.shutdown().await;
    }
}

#[tokio::test]
async fn generated_calculator_preserves_full_i64_and_overflow_leaves_counter_unchanged() {
    let descriptor = cordis_contract::portable::decode(
        &cordis_example_contracts::resources::calculator_wasm::SERVICE
            .parse()
            .unwrap(),
    )
    .unwrap();
    let app = App::with_descriptor(support::normal_component(), descriptor, json!({})).await;
    for value in [i64::MIN, i64::MAX, 9007199254740992] {
        assert_eq!(
            app.invoke_json("add", json!({"left":value.to_string(),"right":"0"}))
                .await
                .unwrap(),
            value.to_string()
        );
    }
    let counter = app
        .handle()
        .call(|s, c| {
            Box::pin(async move {
                Ok(
                    s.acquire_json(&c, "counter", json!({"initial":i64::MAX.to_string()}))
                        .await,
                )
            })
        })
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(counter.invoke_json("add", json!("1")).await, Err(WasmError::Rejected { code, .. }) if code == "overflow")
    );
    assert_eq!(
        counter.invoke_json("get", json!({})).await.unwrap(),
        i64::MAX.to_string()
    );
    assert_eq!(
        counter
            .invoke_json("add", json!(i64::MIN.to_string()))
            .await
            .unwrap(),
        "-1"
    );
    counter.dispose().await.unwrap();
    app.rt.shutdown().await.unwrap();
}
