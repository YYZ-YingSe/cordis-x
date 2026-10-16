use cordis_portable_example::resources::{CounterClient, CounterPlugin};
use cordis_runtime::{self as rt, Plugin, Runtime, Service};
use cordis_sdk_core::{Value, json};
use cordis_sdk_runtime as native;
use cordis_wasm::{
    Capabilities, ContextComponent, ContextResource, ContextService, Dependency, Limits,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[path = "support/managed_resource_faults.rs"]
mod faults;

async fn bounded<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(20), f)
        .await
        .expect("resource scenario timed out")
}
#[cfg(unix)]
#[path = "support/cpp_resources.rs"]
mod cpp;
#[path = "support/resources.rs"]
mod guest;
fn component(name: &str, limits: Limits, capabilities: Capabilities) -> ContextComponent {
    // Reuse compiled code, exactly as production Compiler users do. Each case
    // still creates independent Stores, Runtime owners and capability bindings.
    static COMPILER: std::sync::OnceLock<cordis_wasm::Compiler> = std::sync::OnceLock::new();
    let compiler = COMPILER.get_or_init(cordis_wasm::Compiler::default);
    #[cfg(unix)]
    if name.starts_with("cpp_") {
        let mut capabilities = capabilities;
        capabilities.wasi = Some(cordis_wasm::Wasi::default());
        return compiler
            .context_component(cpp::bytes(name), limits, capabilities)
            .unwrap();
    }
    compiler
        .context_component(guest::bytes(name), limits, capabilities)
        .unwrap()
}
fn library(name: &str, capabilities: Capabilities) -> ContextComponent {
    component(name, Limits::default(), capabilities)
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
                let target = target.clone();
                let binding = binding.clone();
                async move {
                    *target.lock().unwrap() = Some(ctx.dependency(&binding)?);
                    Ok(())
                }
            })
            .requires(service),
        )
        .unwrap();
    bounded(owner.ready()).await.unwrap();
    let handle = saved.lock().unwrap().take().unwrap();
    (owner, handle)
}
enum Binding {
    Native(rt::ServiceHandle<native::Service>),
    Wasm(rt::ServiceHandle<ContextService>),
}
impl Binding {
    async fn invoke_json(&self, method: &'static str, args: Value) -> rt::Result<Value> {
        match self {
            Self::Native(h) => {
                h.call(move |s, ctx| {
                    Box::pin(async move {
                        s.invoke_json(&ctx, method, args)
                            .await
                            .map_err(|e| rt::Error::Message(e.to_string()))
                    })
                })
                .await
            }
            Self::Wasm(h) => {
                h.call(move |s, ctx| {
                    Box::pin(
                        async move { s.invoke_json(&ctx, method, args).await.map_err(Into::into) },
                    )
                })
                .await
            }
        }
    }
}
async fn scenario(wasm: Option<(bool, bool)>) -> Vec<Value> {
    let runtime = Runtime::new();
    let logs = Arc::new(Mutex::new(Vec::new()));
    let logger = Service::<Mutex<Vec<Value>>>::new("example.counter-log", "1");
    let binding = logger.clone();
    let records = logs.clone();
    runtime
        .install(
            Plugin::new("logger", move |ctx| {
                let binding = binding.clone();
                let records = records.clone();
                async move { ctx.provide(&binding, records) }
            })
            .provides(&logger),
        )
        .unwrap();
    let (first, a, b) = if let Some((cpp_provider, cpp_client)) = wasm {
        let counters = Service::<ContextService>::new("example.counters", "1").with_descriptor(cordis_contract::Descriptor::new(<cordis_portable_example::resources::CounterPlugin as cordis_sdk_core::Plugin>::interface()).unwrap());
        let lib = library(
            if cpp_provider {
                "cpp_resource_counter"
            } else {
                "wasm_resource_counter"
            },
            Capabilities {
                dependencies: vec![Dependency::new_json(
                    &logger,
                    cordis_contract::Descriptor::new(
                        cordis_portable_example::resources::generated::CounterLogClient::interface(
                        ),
                    )
                    .unwrap(),
                    |log, _, _, value| {
                        Box::pin(async move {
                            log.lock().unwrap().push(value);
                            Ok(json!({}))
                        })
                    },
                )],
                ..Default::default()
            },
        );
        let provider = runtime
            .install(lib.plugin("counters", &counters, json!({})).unwrap())
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let client = Service::<ContextService>::new("example.counter-client", "1").with_descriptor(
            cordis_contract::Descriptor::new(
                cordis_portable_example::resources::generated::CounterHolderClient::interface(),
            )
            .unwrap(),
        );
        let lib = library(
            if cpp_client {
                "cpp_resource_client"
            } else {
                "wasm_resource_client"
            },
            Capabilities {
                dependencies: vec![Dependency::wasm(&counters)],
                ..Default::default()
            },
        );
        let plugin = runtime
            .install(lib.plugin("client", &client, json!({})).unwrap())
            .unwrap();
        bounded(plugin.ready()).await.unwrap();
        let (first, a) = owner(&runtime, &client).await;
        let (_, b) = owner(&runtime, &client).await;
        (first, Binding::Wasm(a), Binding::Wasm(b))
    } else {
        let counters = Service::<native::Service>::new("example.counters", "1").with_descriptor(
            cordis_sdk_core::Descriptor::new(
                <CounterPlugin as cordis_sdk_core::Plugin>::interface(),
            )
            .unwrap(),
        );
        runtime
            .install(
                native::plugin::<CounterPlugin>(
                    "counters",
                    &counters,
                    json!({}),
                    native::Capabilities {
                        dependencies: vec![native::Dependency::new(
                            &logger,
                            cordis_sdk_core::Descriptor::new(
                                cordis_portable_example::resources::generated::CounterLogClient::interface(),
                            )
                            .unwrap(),
                            |log, _, _, value| {
                                Box::pin(async move {
                                    log.lock().unwrap().push(value.to_json().map_err(cordis_sdk_core::typed::violation)?);
                                    cordis_sdk_core::Message::new(cordis_portable_example::resources::generated::data::google::protobuf::Empty {}).map_err(cordis_sdk_core::typed::violation)
                                })
                            },
                        )],
                        ..Default::default()
                    },
                )
                .unwrap(),
            )
            .unwrap();
        let client = Service::<native::Service>::new("example.counter-client", "1");
        runtime
            .install(
                native::plugin::<CounterClient>(
                    "client",
                    &client,
                    json!({}),
                    native::Capabilities {
                        dependencies: vec![native::Dependency::portable(&counters).unwrap()],
                        ..Default::default()
                    },
                )
                .unwrap(),
            )
            .unwrap();
        let (first, a) = owner(&runtime, &client).await;
        let (_, b) = owner(&runtime, &client).await;
        (first, Binding::Native(a), Binding::Native(b))
    };
    if wasm.is_some_and(|(_, cpp)| cpp) {
        assert_eq!(
            bounded(a.invoke_json("scoped", json!(5))).await.unwrap(),
            json!("5")
        );
        assert_eq!(*logs.lock().unwrap(), vec![json!({"counter_closed":"5"})]);
        logs.lock().unwrap().clear();
    }
    // Scope must transport the canonical representation without narrowing int64.
    for value in ["9223372036854775807", "-9223372036854775808"] {
        assert_eq!(
            bounded(a.invoke_json("scoped", json!(value)))
                .await
                .unwrap(),
            json!(value)
        );
    }
    bounded(a.invoke_json("acquire", json!("9223372036854775807")))
        .await
        .unwrap();
    assert!(bounded(a.invoke_json("add", json!(1))).await.is_err());
    assert_eq!(
        bounded(a.invoke_json("get", json!({}))).await.unwrap(),
        json!("9223372036854775807")
    );
    bounded(a.invoke_json("release", json!({}))).await.unwrap();
    logs.lock().unwrap().clear();
    bounded(a.invoke_json("acquire", json!(40))).await.unwrap();
    assert_eq!(
        bounded(b.invoke_json("add", json!(2))).await.unwrap(),
        json!("42")
    );
    bounded(first.dispose()).await.unwrap();
    assert_eq!(*logs.lock().unwrap(), vec![json!({"counter_closed":"42"})]);
    assert!(bounded(b.invoke_json("get", json!({}))).await.is_err());
    bounded(b.invoke_json("release", json!({}))).await.unwrap();
    bounded(b.invoke_json("acquire", json!(7))).await.unwrap();
    bounded(b.invoke_json("release", json!({}))).await.unwrap();
    bounded(b.invoke_json("release", json!({}))).await.unwrap();
    bounded(b.invoke_json("acquire", json!(9))).await.unwrap();
    bounded(runtime.shutdown()).await.unwrap();
    let result = logs.lock().unwrap().clone();
    assert_eq!(
        result,
        vec![
            json!({"counter_closed":"42"}),
            json!({"counter_closed":"7"}),
            json!({"counter_closed":"9"})
        ]
    );
    result
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_counter_business_matches_native_through_two_managed_components() {
    assert_eq!(scenario(None).await, scenario(Some((false, false))).await);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_resources_compose_with_cpp_and_rust_through_real_components() {
    let expected = scenario(None).await;
    for pair in [(true, true), (true, false), (false, true)] {
        eprintln!("C++ component provider={} client={}", pair.0, pair.1);
        assert_eq!(scenario(Some(pair)).await, expected);
    }
}
