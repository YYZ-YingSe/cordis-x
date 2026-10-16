use cordis_abi::{AbiResource, AbiService, Error as AbiError, Library, Value, json};
use cordis_sdk::*;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[path = "support/artifacts.rs"]
mod artifacts;
use artifacts::binary;
fn load(path: &Path) -> cordis_abi::Result<Library> {
    // SAFETY: all binaries are compiled from this repository's trusted fixtures.
    // Malformed table/value fixtures only violate checked semantic fields, never
    // pointer validity, unwinding, or the underlying platform ABI.
    #[allow(unsafe_code)]
    unsafe {
        Library::load(path)
    }
}
struct Fixture {
    dir: PathBuf,
    binary: PathBuf,
    trace: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "cordis-abi-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let binary = dir.join(format!("fixture{}", std::env::consts::DLL_SUFFIX));
        std::fs::copy(crate::binary("cordis_abi_fixture"), &binary).unwrap();
        Self {
            trace: dir.join("trace"),
            dir,
            binary,
        }
    }
    fn config(&self, mut config: Value) -> Value {
        config["trace"] = json!(self.trace.to_str().unwrap());
        config
    }
    fn events(&self) -> Vec<String> {
        std::fs::read_to_string(&self.trace)
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }
    fn count(&self, event: &str) -> usize {
        self.events().iter().filter(|line| *line == event).count()
    }
    async fn wait(&self, event: &str) {
        bounded(async {
            while self.count(event) == 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await;
    }
    fn open(&self, name: &str) {
        std::fs::write(self.dir.join(name), b"open").unwrap();
    }
    fn gate(&self, name: &str) -> Value {
        json!(self.dir.join(name).to_str().unwrap())
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(15), future)
        .await
        .expect("ABI test timed out")
}
struct Rig {
    rt: Runtime,
    provider: PluginHandle,
    client: PluginHandle,
    handle: ServiceHandle<AbiService>,
}
async fn rig(library: &Library, config: Value) -> Rig {
    rig_with_descriptor(
        library,
        cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true)).unwrap(),
        config,
    )
    .await
}
async fn rig_with_descriptor(
    library: &Library,
    descriptor: cordis_contract::Descriptor,
    config: Value,
) -> Rig {
    let rt = Runtime::new();
    let service = Service::new("example.calculator", "1").with_descriptor(descriptor);
    let provider = rt
        .install(library.plugin("library", &service, config).unwrap())
        .unwrap();
    bounded(provider.ready()).await.unwrap();
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
    bounded(client.ready()).await.unwrap();
    let handle = saved.lock().unwrap().take().unwrap();
    Rig {
        rt,
        provider,
        client,
        handle,
    }
}
async fn invoke_json(
    handle: &ServiceHandle<AbiService>,
    method: &str,
    value: Value,
) -> cordis_abi::Result<Value> {
    let method = method.to_owned();
    handle
        .call(move |service, ctx| {
            Box::pin(async move { Ok(service.invoke_json(&ctx, &method, value).await) })
        })
        .await?
}
async fn acquire(
    handle: &ServiceHandle<AbiService>,
    kind: &str,
) -> cordis_abi::Result<AbiResource> {
    let kind = kind.to_owned();
    handle
        .call(move |service, ctx| {
            Box::pin(async move {
                Ok(service
                    .acquire_json(&ctx, &kind, json!({"initial":40}))
                    .await)
            })
        })
        .await?
}

#[test]
fn exact_contract_version_and_required_table_preflight() {
    let library = load(&binary("cordis_abi_calculator")).unwrap();
    assert!(library.supports_resources());
    for service in [
        Service::new("example.calculator", "2").with_descriptor(
            cordis_contract::portable::decode(
                &cordis_example_contracts::resources::calculator_abi::SERVICE
                    .parse()
                    .unwrap(),
            )
            .unwrap(),
        ),
        Service::new("other", "1"),
    ] {
        assert!(matches!(
            library.plugin("bad", &service, cordis_wire::json!({})),
            Err(AbiError::Contract(_))
        ));
    }
    for name in ["cordis_abi_bad_1", "cordis_abi_bad_2"] {
        assert!(load(&binary(name)).is_err());
    }
    assert!(load(&binary("missing")).is_err());
}

#[tokio::test]
async fn real_cpp_example_matches_process_value_and_counter_contract() {
    let library = load(&binary("cordis_abi_calculator")).unwrap();
    let rig = rig_with_descriptor(
        &library,
        cordis_contract::portable::decode(
            &cordis_example_contracts::resources::calculator_abi::SERVICE
                .parse()
                .unwrap(),
        )
        .unwrap(),
        cordis_wire::json!({}),
    )
    .await;
    let value = json!({"中文 🦀":[null,true,false,-9007199254740991f64,9007199254740991f64,{},[],"a\u{0000}b"]});
    assert_eq!(
        invoke_json(&rig.handle, "echo", value.clone())
            .await
            .unwrap(),
        value
    );
    assert_eq!(
        invoke_json(&rig.handle, "add", json!({"left":20,"right":22}))
            .await
            .unwrap(),
        "42"
    );
    let counter = acquire(&rig.handle, "counter").await.unwrap();
    assert_eq!(counter.invoke_json("add", json!(2)).await.unwrap(), "42");
    let (a, b) = tokio::join!(counter.dispose(), counter.dispose());
    a.unwrap();
    b.unwrap();
    assert!(matches!(
        counter.invoke_json("get", Value::Null).await,
        Err(AbiError::Closed)
    ));
    bounded(rig.rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn normal_calculator_checks_structure_without_exposing_fixture_operations() {
    let library = load(&binary("cordis_abi_calculator")).unwrap();
    let descriptor = cordis_contract::portable::decode(
        &cordis_example_contracts::resources::calculator_abi::SERVICE
            .parse()
            .unwrap(),
    )
    .unwrap();
    let rig = rig_with_descriptor(&library, descriptor, cordis_wire::json!({})).await;
    for (method, args) in [
        ("add", json!({"left": 1})),
        ("add", json!({"left": "bad", "right": 2})),
        ("add", json!({"left": 1, "right": 2, "extra": 3})),
        ("code-version", Value::Null),
        ("hold", json!(42)),
        ("metadata", cordis_wire::json!({})),
    ] {
        assert!(matches!(
            invoke_json(&rig.handle, method, args).await,
            Err(AbiError::InvalidInput(_))
        ));
    }
    for args in [
        json!({"initial": []}),
        json!({"initial": "bad"}),
        json!({"fail_close": true}),
    ] {
        let result = rig
            .handle
            .call(move |s, c| {
                Box::pin(async move { Ok(s.acquire_json(&c, "counter", args).await) })
            })
            .await
            .unwrap();
        assert!(matches!(result, Err(AbiError::InvalidInput(_))));
    }
    assert!(rig.client.effects().is_empty());
    let counter = acquire(&rig.handle, "counter").await.unwrap();
    for (method, args) in [
        ("add", json!("bad")),
        ("get", Value::Null),
        ("hold", json!(42)),
    ] {
        assert!(matches!(
            counter.invoke_json(method, args).await,
            Err(AbiError::InvalidInput(_))
        ));
    }
    assert!(
        matches!(counter.invoke_json("add", json!("9223372036854775807")).await,
        Err(AbiError::Rejected {code, ..}) if code == "overflow")
    );
    assert_eq!(counter.invoke_json("get", json!({})).await.unwrap(), "40");
    assert_eq!(counter.invoke_json("add", json!(2)).await.unwrap(), "42");
    rig.rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn multiple_instances_share_code_but_keep_separate_state_and_unload_last() {
    let fixture = Fixture::new();
    let library = load(&fixture.binary).unwrap();
    let first = rig(&library, fixture.config(json!({"offset":0}))).await;
    let second = rig(&library, fixture.config(json!({"offset":100}))).await;
    drop(library);
    assert_eq!(
        invoke_json(&first.handle, "add", json!({"left":20,"right":22}))
            .await
            .unwrap(),
        json!(42.0)
    );
    assert_eq!(
        invoke_json(&second.handle, "add", json!({"left":20,"right":22}))
            .await
            .unwrap(),
        json!(142.0)
    );
    first.rt.shutdown().await.unwrap();
    drop(first);
    assert_eq!(fixture.count("unload"), 0);
    second.rt.shutdown().await.unwrap();
    drop(second);
    fixture.wait("unload").await;
    assert_eq!(fixture.count("create"), 2);
    assert_eq!(fixture.count("destroy"), 2);
    assert_eq!(fixture.events().last().unwrap(), "unload");
}

#[tokio::test]
async fn cancelled_call_keeps_code_and_provider_alive_until_return() {
    let fixture = Fixture::new();
    let library = load(&fixture.binary).unwrap();
    let rig = rig(
        &library,
        fixture.config(json!({"invoke_gate":fixture.gate("invoke")})),
    )
    .await;
    let handle = rig.handle.clone();
    let waiter = tokio::spawn(async move { invoke_json(&handle, "hold", json!(42)).await });
    fixture.wait("invoke_gate-entered").await;
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
    assert_eq!(fixture.count("deactivate"), 0);
    fixture.open("invoke");
    bounded(stop).await.unwrap().unwrap();
    rig.rt.shutdown().await.unwrap();
    drop(rig);
    drop(library);
    fixture.wait("unload").await;
    assert_eq!(fixture.count("destroy"), 1);
}

#[tokio::test]
async fn cancelled_late_acquisition_is_released_before_provider_destruction() {
    let fixture = Fixture::new();
    let library = load(&fixture.binary).unwrap();
    let rig = rig(
        &library,
        fixture.config(json!({"acquire_gate":fixture.gate("acquire")})),
    )
    .await;
    let handle = rig.handle.clone();
    let waiter = tokio::spawn(async move { acquire(&handle, "counter").await });
    fixture.wait("acquire_gate-entered").await;
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
    assert_eq!(fixture.count("deactivate"), 0);
    assert!(!stop.is_finished());
    fixture.open("acquire");
    bounded(stop).await.unwrap().unwrap();
    rig.rt.shutdown().await.unwrap();
    drop(rig);
    drop(library);
    fixture.wait("unload").await;
    let events = fixture.events();
    assert_eq!(fixture.count("release"), 1);
    assert!(
        events.iter().position(|e| e == "release").unwrap()
            < events.iter().position(|e| e == "deactivate").unwrap()
    );
}

#[tokio::test]
async fn cancellation_inside_service_callback_keeps_adapter_operation_owned() {
    for acquiring in [false, true] {
        let fixture = Fixture::new();
        let library = load(&fixture.binary).unwrap();
        let rig = rig(
            &library,
            fixture.config(json!({
                "invoke_gate":fixture.gate("inner"),
                "acquire_gate":fixture.gate("inner")
            })),
        )
        .await;
        let handle = rig.handle.clone();
        let (cancel, cancelled) = tokio::sync::oneshot::channel();
        let callback = tokio::spawn(async move {
            handle
                .call(move |service, ctx| {
                    Box::pin(async move {
                        let operation = async {
                            if acquiring {
                                service
                                    .acquire_json(&ctx, "counter", json!({"initial":40}))
                                    .await
                                    .map(|_| ())
                            } else {
                                service
                                    .invoke_json(&ctx, "hold", Value::Null)
                                    .await
                                    .map(|_| ())
                            }
                        };
                        tokio::select! {
                            result = operation => result.map_err(Into::into),
                            _ = cancelled => Ok(()),
                        }
                    })
                })
                .await
        });
        fixture
            .wait(if acquiring {
                "acquire_gate-entered"
            } else {
                "invoke_gate-entered"
            })
            .await;
        cancel.send(()).unwrap();
        bounded(callback).await.unwrap().unwrap(); // Outer ServiceHandle lease is gone.
        let provider = rig.provider.clone();
        let stop = tokio::spawn(async move { provider.dispose().await });
        bounded(async {
            while rig.provider.snapshot().state != Lifecycle::Unloading {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(!stop.is_finished());
        assert_eq!(fixture.count("deactivate"), 0);
        fixture.open("inner");
        bounded(stop).await.unwrap().unwrap();
        assert_eq!(fixture.count("release"), usize::from(acquiring));
        rig.rt.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn manual_resource_disposal_drains_calls_and_shares_one_inverse() {
    let fixture = Fixture::new();
    let library = load(&fixture.binary).unwrap();
    let rig = rig(
        &library,
        fixture.config(json!({"resource_gate":fixture.gate("resource")})),
    )
    .await;
    let resource = acquire(&rig.handle, "counter").await.unwrap();
    let calling = resource.clone();
    let call =
        tokio::spawn(async move { calling.invoke_json("get", cordis_wire::json!({})).await });
    fixture.wait("resource_gate-entered").await;
    call.abort();
    let _ = call.await;
    let first = resource.clone();
    let first = tokio::spawn(async move { first.dispose().await });
    bounded(async {
        while resource.snapshot().status != EffectStatus::Releasing {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(fixture.count("release"), 0);
    first.abort();
    let _ = first.await;
    fixture.open("resource");
    bounded(resource.dispose()).await.unwrap();
    resource.dispose().await.unwrap();
    assert_eq!(fixture.count("release"), 1);
    assert_eq!(resource.snapshot().status, EffectStatus::Released);
    rig.rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn resources_belong_to_consumer_and_old_handles_close_after_restart() {
    let fixture = Fixture::new();
    let library = load(&fixture.binary).unwrap();
    let rig = rig(&library, fixture.config(cordis_wire::json!({}))).await;
    let resource = acquire(&rig.handle, "counter").await.unwrap();
    assert_eq!(rig.provider.effects().len(), 1);
    assert_eq!(rig.client.effects().len(), 1);
    bounded(rig.provider.restart()).await.unwrap();
    bounded(rig.client.ready()).await.unwrap();
    assert_eq!(fixture.count("create"), 2);
    assert_eq!(fixture.count("release"), 1);
    assert!(matches!(
        invoke_json(&rig.handle, "echo", Value::Null).await,
        Err(AbiError::Closed)
    ));
    assert!(matches!(
        resource.invoke_json("get", cordis_wire::json!({})).await,
        Err(AbiError::Closed)
    ));
    rig.rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn known_rejections_and_invalid_inputs_do_not_fault_provider() {
    let fixture = Fixture::new();
    let library = load(&fixture.binary).unwrap();
    let rig = rig(&library, fixture.config(cordis_wire::json!({}))).await;
    assert!(matches!(
        invoke_json(&rig.handle, "reject", Value::Null).await,
        Err(AbiError::Rejected { .. })
    ));
    assert!(matches!(
        acquire(&rig.handle, "reject").await,
        Err(AbiError::Rejected { .. })
    ));
    for value in [json!("x".repeat(1024 * 1024))] {
        assert!(matches!(
            invoke_json(&rig.handle, "echo", value).await,
            Err(AbiError::InvalidInput(_))
        ));
    }
    assert_eq!(fixture.count("invoke"), 1);
    assert_eq!(
        invoke_json(&rig.handle, "echo", json!(42)).await.unwrap(),
        json!(42.0)
    );
    rig.rt.shutdown().await.unwrap();
    assert_eq!(fixture.count("release"), 0);
}

#[tokio::test]
async fn initialization_failure_recovers_attempted_activation_once() {
    for config in [json!({"fail_activate":true}), json!({"reject_create":true})] {
        let fixture = Fixture::new();
        let library = load(&fixture.binary).unwrap();
        let rt = Runtime::new();
        let service = Service::<AbiService>::new("example.calculator", "1").with_descriptor(
            cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true))
                .unwrap(),
        );
        let provider = rt
            .install(
                library
                    .plugin("failing", &service, fixture.config(config.clone()))
                    .unwrap(),
            )
            .unwrap();
        assert!(bounded(provider.ready()).await.is_err());
        let _ = bounded(provider.dispose()).await;
        let _ = bounded(provider.dispose()).await;
        let _ = rt.shutdown().await;
        assert_eq!(
            fixture.count("deactivate"),
            usize::from(config["fail_activate"] == true)
        );
        assert_eq!(
            fixture.count("destroy"),
            usize::from(config["fail_activate"] == true)
        );
        drop(provider);
        drop(rt);
        drop(library);
        fixture.wait("unload").await;
    }
}

#[tokio::test]
async fn late_creation_during_retirement_is_cleaned_without_publication() {
    let fixture = Fixture::new();
    let library = load(&fixture.binary).unwrap();
    let rt = Runtime::new();
    let service = Service::<AbiService>::new("example.calculator", "1").with_descriptor(
        cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true)).unwrap(),
    );
    let provider = rt
        .install(
            library
                .plugin(
                    "late",
                    &service,
                    fixture.config(json!({"create_gate":fixture.gate("create")})),
                )
                .unwrap(),
        )
        .unwrap();
    fixture.wait("create_gate-entered").await;
    let stop = provider.clone();
    let stop = tokio::spawn(async move { stop.dispose().await });
    bounded(async {
        while provider.snapshot().state != Lifecycle::Unloading {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!stop.is_finished());
    fixture.open("create");
    bounded(stop).await.unwrap().unwrap();
    assert_eq!(fixture.count("destroy"), 1);
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_resource_cleanup_is_cached_and_blocks_destroy_and_unload() {
    let fixture = Fixture::new();
    let library = load(&fixture.binary).unwrap();
    let rig = rig(&library, fixture.config(json!({"fail_release":true}))).await;
    let resource = acquire(&rig.handle, "counter").await.unwrap();
    let first = resource.dispose().await.unwrap_err();
    let second = resource.dispose().await.unwrap_err();
    assert_eq!(first, second);
    assert!(matches!(
        resource.snapshot().status,
        EffectStatus::Failed(_)
    ));
    assert!(bounded(rig.provider.dispose()).await.is_err());
    assert!(rig.rt.shutdown().await.is_err());
    assert_eq!(fixture.count("release"), 1);
    assert_eq!(fixture.count("deactivate"), 0);
    assert_eq!(fixture.count("destroy"), 0);
    drop(resource);
    drop(rig);
    drop(library);
    assert_eq!(fixture.count("unload"), 0);
}

#[tokio::test]
async fn failed_deactivation_retains_instance_and_never_retries_inverse() {
    let fixture = Fixture::new();
    let library = load(&fixture.binary).unwrap();
    let rig = rig(&library, fixture.config(json!({"fail_deactivate":true}))).await;
    assert!(bounded(rig.provider.dispose()).await.is_err());
    assert!(bounded(rig.provider.dispose()).await.is_err());
    assert!(rig.rt.shutdown().await.is_err());
    drop(rig);
    drop(library);
    assert_eq!(fixture.count("deactivate"), 1);
    assert_eq!(fixture.count("destroy"), 0);
    assert_eq!(fixture.count("unload"), 0);
}

#[tokio::test]
async fn unknown_and_malformed_calls_free_results_but_keep_recovery_failed() {
    for method in ["throw", "invalid"] {
        let fixture = Fixture::new();
        let library = load(&fixture.binary).unwrap();
        let rig = rig(&library, fixture.config(cordis_wire::json!({}))).await;
        let before = fixture.count("result-free");
        assert!(matches!(
            invoke_json(&rig.handle, method, Value::Null).await,
            Err(AbiError::Unknown(_))
        ));
        assert_eq!(fixture.count("result-free"), before + 1);
        assert!(bounded(rig.provider.dispose()).await.is_err());
        let _ = rig.rt.shutdown().await;
        drop(rig);
        drop(library);
        assert_eq!(fixture.count("destroy"), 0);
        assert_eq!(fixture.count("unload"), 0);
    }
}

#[tokio::test]
async fn unknown_acquisition_keeps_a_failed_ticket() {
    let fixture = Fixture::new();
    let library = load(&fixture.binary).unwrap();
    let rig = rig(&library, fixture.config(cordis_wire::json!({}))).await;
    assert!(matches!(
        acquire(&rig.handle, "unknown").await,
        Err(AbiError::Unknown(_))
    ));
    assert!(bounded(rig.provider.dispose()).await.is_err());
    let _ = rig.rt.shutdown().await;
    assert!(
        rig.client
            .effects()
            .iter()
            .any(|effect| matches!(effect.status, EffectStatus::Failed(_)))
    );
    drop(rig);
    drop(library);
    assert_eq!(fixture.count("unload"), 0);
}

#[tokio::test]
async fn unknown_creation_cannot_be_reported_as_recovered() {
    let fixture = Fixture::new();
    let library = load(&fixture.binary).unwrap();
    let rt = Runtime::new();
    let service = Service::<AbiService>::new("example.calculator", "1").with_descriptor(
        cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true)).unwrap(),
    );
    let provider = rt
        .install(
            library
                .plugin(
                    "unknown",
                    &service,
                    fixture.config(json!({"unknown_create":true})),
                )
                .unwrap(),
        )
        .unwrap();
    assert!(bounded(provider.ready()).await.is_err());
    assert!(bounded(provider.dispose()).await.is_err());
    let _ = rt.shutdown().await;
    assert!(
        provider
            .effects()
            .iter()
            .any(|effect| matches!(effect.status, EffectStatus::Failed(_)))
    );
    drop(provider);
    drop(rt);
    drop(library);
    assert_eq!(fixture.count("unload"), 0);
}
