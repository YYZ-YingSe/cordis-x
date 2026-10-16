use super::*;
use crate::json;
use cordis_runtime::Runtime;
use wasmtime::component::Component;
use wasmtime::{Config, Engine};
#[cfg(unix)]
#[path = "../../tests/support/cpp_resources.rs"]
mod cpp_guest;
#[path = "../../tests/support/resources.rs"]
mod guest;
fn component(
    cpp: bool,
    counter: bool,
    limits: Limits,
    capabilities: Capabilities,
) -> ContextComponent {
    #[cfg(unix)]
    if cpp {
        let mut capabilities = capabilities;
        capabilities.wasi = Some(Wasi::default());
        return ContextComponent::from_bytes(
            cpp_guest::bytes(if counter {
                "cpp_resource_counter"
            } else {
                "cpp_resource_client"
            }),
            limits,
            capabilities,
        )
        .unwrap();
    }
    let _ = cpp;
    ContextComponent::from_bytes(
        guest::bytes(if counter {
            "wasm_resource_counter"
        } else {
            "wasm_resource_client"
        }),
        limits,
        capabilities,
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reverse_resource_grants_do_not_retain_component_stores_after_recovery() {
    for cpp in [false, true].into_iter().filter(|cpp| !cpp || cfg!(unix)) {
        store_recovery(cpp).await;
    }
}
async fn store_recovery(cpp: bool) {
    let runtime = Runtime::new();
    let logger = Service::<()>::new("example.counter-log", "1");
    let binding = logger.clone();
    runtime
        .install(
            Plugin::new("logger", move |ctx| {
                let binding = binding.clone();
                async move { ctx.provide(&binding, Arc::new(())) }
            })
            .provides(&logger),
        )
        .unwrap();
    let counters = Service::<ContextService>::new("example.counters", "1").with_descriptor(cordis_contract::Descriptor::new(<cordis_portable_example::resources::CounterPlugin as cordis_sdk_core::Plugin>::interface()).unwrap());
    let module = component(
        cpp,
        true,
        Limits::default(),
        Capabilities {
            dependencies: vec![Dependency::new_json(
                &logger,
                cordis_contract::Descriptor::new(
                    cordis_portable_example::resources::generated::CounterLogClient::interface(),
                )
                .unwrap(),
                |_, _, _, _| Box::pin(async { Ok(json!({})) }),
            )],
            ..Default::default()
        },
    );
    let provider = runtime
        .install(module.plugin("counters", &counters, json!({})).unwrap())
        .unwrap();
    provider.ready().await.unwrap();
    let client = Service::<ContextService>::new("example.counter-client", "1").with_descriptor(
        cordis_contract::Descriptor::new(
            cordis_portable_example::resources::generated::CounterHolderClient::interface(),
        )
        .unwrap(),
    );
    let module = component(
        cpp,
        false,
        Limits::default(),
        Capabilities {
            dependencies: vec![Dependency::wasm(&counters)],
            ..Default::default()
        },
    );
    let consumer = runtime
        .install(module.plugin("client", &client, json!({})).unwrap())
        .unwrap();
    consumer.ready().await.unwrap();
    let stores = Arc::new(Mutex::new(Vec::new()));
    let weak_stores = stores.clone();
    let a = counters.clone();
    let b = client.clone();
    let owner = runtime
        .install(
            Plugin::new("owner", move |ctx| {
                let (a, b, stores) = (a.clone(), b.clone(), weak_stores.clone());
                async move {
                    for service in [a, b.clone()] {
                        let weak = ctx
                            .dependency(&service)?
                            .call(|s, _| Box::pin(async move { Ok(Arc::downgrade(&s.instance)) }))
                            .await?;
                        stores.lock().unwrap().push(weak);
                    }
                    ctx.dependency(&b)?
                        .call(|s, ctx| {
                            Box::pin(async move {
                                s.invoke_json(&ctx, "acquire", crate::json!(40))
                                    .await
                                    .map_err(Into::into)
                            })
                        })
                        .await?;
                    Ok(())
                }
            })
            .requires(&counters)
            .requires(&client),
        )
        .unwrap();
    owner.ready().await.unwrap();
    assert_eq!(stores.lock().unwrap().len(), 2);
    for weak in stores.lock().unwrap().iter() {
        assert!(weak.upgrade().unwrap().worker.lock().unwrap().is_some());
    }
    runtime.shutdown().await.unwrap();
    for weak in stores.lock().unwrap().iter() {
        if let Some(instance) = weak.upgrade() {
            assert!(instance.worker.lock().unwrap().is_none());
            assert!(instance.guest.lock().unwrap().is_none());
        }
    }
    drop((owner, provider, consumer, runtime));
    for weak in stores.lock().unwrap().iter() {
        assert!(weak.upgrade().is_none());
    }
}

// Exercise guest admission independently of the host's call quota. All ordinary
// calls actually suspend, so this tests live coroutine capacity and cleanup.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_cleanup_capacity_survives_saturated_ordinary_calls() {
    use futures_util::{StreamExt, stream::FuturesUnordered};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Host {
        wasi: crate::wasi::State,
        gate: Arc<Semaphore>,
        entered: Arc<AtomicUsize>,
    }
    impl HasData for Host {
        type Data<'a> = &'a mut Host;
    }
    impl wit::Host for Host {}
    impl wasmtime_wasi::WasiView for Host {
        fn ctx(&mut self) -> wasmtime_wasi::WasiCtxView<'_> {
            self.wasi.view()
        }
    }
    impl bindings::cordis::plugin::host::Host for Host {}
    impl<T: Send> bindings::cordis::plugin::host::HostWithStore<T> for Host {
        async fn call(
            accessor: &Accessor<T, Self>,
            _: String,
            command: String,
            args: wit::Payload,
        ) -> std::result::Result<wit::Payload, wit::Failure> {
            let args = output(args, cordis_contract::Validation::Checked).unwrap();
            if command == "invoke" && args.control["method"] == "hold" {
                let gate = accessor.with(|mut access| {
                    access.get().entered.fetch_add(1, Ordering::SeqCst);
                    access.get().gate.clone()
                });
                gate.acquire().await.unwrap().forget();
            }
            Ok(input(&Payload::message(
                crate::json_body(&cordis_contract::MessageContract::json_value(), &json!({}))
                    .unwrap(),
            ))
            .unwrap())
        }
    }
    let mut config = Config::new();
    config
        .wasm_component_model(true)
        .concurrency_support(true)
        .wasm_exceptions(true);
    let engine = Engine::new(&config).unwrap();
    let component = Component::new(&engine, cpp_guest::bytes("cpp_resource_probe")).unwrap();
    let gate = Arc::new(Semaphore::new(0));
    let entered = Arc::new(AtomicUsize::new(0));
    let mut store = Store::new(
        &engine,
        Host {
            wasi: crate::wasi::State::new(&Wasi::default()).unwrap(),
            gate: gate.clone(),
            entered: entered.clone(),
        },
    );
    let mut linker = Linker::new(&engine);
    bindings::ManagedPlugin::add_to_linker::<_, Host>(&mut linker, |host| host).unwrap();
    wasmtime_wasi::p2::add_to_linker_async(&mut linker).unwrap();
    let plugin = bindings::ManagedPlugin::instantiate_async(&mut store, &component, &linker)
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        store.run_concurrent(async |accessor| {
            let guest = plugin.cordis_plugin_managed_service();
            for command in ["create", "activate"] {
                let control = if command == "create" {
                    cordis_component::Initialization::encode(
                        json!({}),
                        cordis_contract::Validation::Checked,
                    )
                } else {
                    json!({})
                };
                business(
                    guest
                        .call_call(
                            accessor,
                            "test".into(),
                            false,
                            command.into(),
                            input(&Payload::from(control)).unwrap(),
                        )
                        .await
                        .unwrap(),
                )
                .unwrap();
            }
            let mut calls = FuturesUnordered::new();
            for _ in 0..256 {
                calls.push(
                    guest.call_call(
                        accessor,
                        "test".into(),
                        false,
                        "invoke".into(),
                        input(&Payload::with_message(
                            json!({"method":"hold"}),
                            crate::json_body(
                                &cordis_contract::MessageContract::json_value(),
                                &Value::Null,
                            )
                            .unwrap(),
                        ))
                        .unwrap(),
                    ),
                );
            }
            let first = business(calls.next().await.unwrap().unwrap());
            assert!(matches!(first, Err(Error::Refused { code, .. }) if code == "overloaded"));
            assert!(entered.load(Ordering::SeqCst) > 0);
            business(
                guest
                    .call_call(
                        accessor,
                        "test".into(),
                        true,
                        "forget".into(),
                        input(&json!(0).into()).unwrap(),
                    )
                    .await
                    .unwrap(),
            )
            .unwrap();
            gate.add_permits(256);
            while let Some(result) = calls.next().await {
                match business(result.unwrap()) {
                    Ok(value) => assert_eq!(
                        crate::body_json(
                            &cordis_contract::MessageContract::json_value(),
                            &output(value, cordis_contract::Validation::Checked)
                                .unwrap()
                                .one()
                                .unwrap()
                        )
                        .unwrap(),
                        json!({})
                    ),
                    Err(Error::Refused { code, .. }) if code == "overloaded" => {}
                    other => panic!("unexpected completion: {other:?}"),
                }
            }
            for command in ["deactivate", "close"] {
                business(
                    guest
                        .call_call(
                            accessor,
                            "test".into(),
                            true,
                            command.into(),
                            input(&Payload::from(json!({}))).unwrap(),
                        )
                        .await
                        .unwrap(),
                )
                .unwrap();
            }
        }),
    )
    .await
    .unwrap()
    .unwrap();
}
