//! Real consumer/provider combinations share one binding and one resource owner.
#![allow(unsafe_code)]
use cordis_host::{Body, Dependency, HostBindings};
use cordis_portable_example::resources::{
    CounterClient, CounterPlugin, generated::CounterLogClient,
};
use cordis_runtime as rt;
use cordis_sdk_core::{self as sdk, json};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
#[path = "support/resources.rs"]
mod guest;

#[derive(Clone, Copy, Debug)]
enum Backend {
    Native,
    Process,
    Abi,
    Wasm,
}
const BACKENDS: [Backend; 4] = [
    Backend::Native,
    Backend::Process,
    Backend::Abi,
    Backend::Wasm,
];
fn artifacts() -> &'static PathBuf {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let target = cordis_test_contracts::native::cargo_target();
        for (package, examples) in [
            (
                "cordis-sdk-process",
                vec!["resource_plugin", "process_workflows"],
            ),
            (
                "cordis-sdk-abi",
                vec!["resource_counter", "resource_client", "abi_workflows"],
            ),
        ] {
            let mut command = std::process::Command::new(env!("CARGO"));
            command
                .current_dir(&root)
                .args([
                    "build",
                    "--offline",
                    "--locked",
                    "-p",
                    package,
                    "--target-dir",
                ])
                .arg(&target);
            for example in examples {
                command.args(["--example", example]);
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "{command:?}\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        target.join("debug/examples")
    })
}
fn prepare<P: sdk::Plugin>(
    backend: Backend,
    entry: Entry,
    bindings: HostBindings,
    http: bool,
) -> (rt::Plugin, Dependency) {
    let client = matches!(entry, Entry::Client);
    let contract = P::manifest().provides;
    let descriptor = sdk::Descriptor::new(P::interface()).unwrap();
    let name = format!("{backend:?}-{}", contract.name);
    let (plugin, endpoint) = match backend {
        Backend::Native => {
            let service =
                rt::Service::<cordis_sdk_runtime::Service>::new(contract.name, contract.revision)
                    .with_descriptor(descriptor);
            (
                cordis_sdk_runtime::plugin::<P>(name, &service, json!({}), bindings).unwrap(),
                Dependency::new(&service).unwrap(),
            )
        }
        Backend::Process => {
            let service =
                rt::Service::<cordis_remote::RemoteService>::new(contract.name, contract.revision)
                    .with_descriptor(descriptor);
            let mut spec = cordis_remote::ProcessSpec::new(artifacts().join(format!(
                "{}{}",
                if matches!(entry, Entry::Workflow) {
                    "process_workflows"
                } else {
                    "resource_plugin"
                },
                std::env::consts::EXE_SUFFIX
            )));
            spec.bindings = bindings;
            spec.endpoint = if http {
                cordis_remote::Endpoint::Http
            } else {
                cordis_remote::Endpoint::Ipc
            };
            if !http {
                spec.args.push("--stdio-only".into());
            }
            if client {
                spec.args.push("--client".into());
            }
            (
                spec.plugin(name, &service),
                Dependency::new(&service).unwrap(),
            )
        }
        Backend::Abi => {
            let service =
                rt::Service::<cordis_abi::AbiService>::new(contract.name, contract.revision)
                    .with_descriptor(descriptor);
            let file = if matches!(entry, Entry::Workflow) {
                "abi_workflows"
            } else if client {
                "resource_client"
            } else {
                "resource_counter"
            };
            // SAFETY: these libraries are trusted fixtures built from this workspace.
            let library = unsafe {
                cordis_abi::Library::load(artifacts().join(format!(
                    "{}{file}{}",
                    std::env::consts::DLL_PREFIX,
                    std::env::consts::DLL_SUFFIX
                )))
            }
            .unwrap()
            .with_context(cordis_abi::Capabilities {
                bindings,
                ..Default::default()
            })
            .unwrap();
            (
                library.plugin(name, &service, json!({})).unwrap(),
                Dependency::new(&service).unwrap(),
            )
        }
        Backend::Wasm => {
            static COMPILER: OnceLock<cordis_wasm::Compiler> = OnceLock::new();
            let service =
                rt::Service::<cordis_wasm::ContextService>::new(contract.name, contract.revision)
                    .with_descriptor(descriptor);
            let module = COMPILER
                .get_or_init(Default::default)
                .context_component(
                    guest::bytes(if matches!(entry, Entry::Workflow) {
                        "wasm_workflows"
                    } else if client {
                        "wasm_resource_client"
                    } else {
                        "wasm_resource_counter"
                    }),
                    cordis_wasm::Limits::default(),
                    cordis_wasm::Capabilities {
                        bindings,
                        ..Default::default()
                    },
                )
                .unwrap();
            (
                module.plugin(name, &service, json!({})).unwrap(),
                Dependency::new(&service).unwrap(),
            )
        }
    };
    (plugin, endpoint)
}
#[derive(Clone, Copy)]
enum Entry {
    Counter,
    Client,
    Workflow,
}
async fn install<P: sdk::Plugin>(
    runtime: &rt::Runtime,
    backend: Backend,
    client: bool,
    bindings: HostBindings,
    http: bool,
) -> Dependency {
    let (plugin, endpoint) = prepare::<P>(
        backend,
        if client {
            Entry::Client
        } else {
            Entry::Counter
        },
        bindings,
        http,
    );
    runtime.install(plugin).unwrap().ready().await.unwrap();
    endpoint
}
async fn call(
    endpoint: &Dependency,
    ctx: &rt::Context,
    method: &str,
    value: sdk::Value,
) -> cordis_host::Result<sdk::Value> {
    let signature =
        cordis_contract::portable::method(&endpoint.descriptor().interface().methods, method)
            .unwrap();
    let input = sdk::Message::from_json(signature.args.clone(), &value).unwrap();
    endpoint
        .invoke(ctx.clone(), method.into(), Body::from(input))
        .await?
        .into_message(&signature.result)
        .unwrap()
        .to_json()
        .map_err(cordis_host::unknown)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_consumer_provider_pairs_keep_resource_ownership_and_cleanup() {
    // Build once outside scenario timeouts; later cases reuse actual code artifacts.
    artifacts();
    guest::bytes("wasm_resource_counter");
    for provider in BACKENDS {
        for consumer in BACKENDS {
            for http in [false, true] {
                if http
                    && !matches!(provider, Backend::Process)
                    && !matches!(consumer, Backend::Process)
                {
                    continue;
                }
                eprintln!("unified resource pair: {consumer:?} -> {provider:?}, http={http}");
                tokio::time::timeout(Duration::from_secs(30), async {
                    let runtime = rt::Runtime::new();
                    let logs = Arc::new(Mutex::new(Vec::new()));
                    let logger = rt::Service::<Mutex<Vec<sdk::Value>>>::new("example.counter-log", "1");
                    let provided = logger.clone();
                    let records = logs.clone();
                    runtime.install(rt::Plugin::new("logger", move |ctx| {
                        let provided = provided.clone(); let records = records.clone();
                        async move { ctx.provide(&provided, records) }
                    }).provides(&logger)).unwrap().ready().await.unwrap();
                    let log = Dependency::json(&logger, sdk::Descriptor::new(CounterLogClient::interface()).unwrap(), |log, _, _, value| Box::pin(async move {
                        log.lock().unwrap().push(value);
                        Ok::<_, sdk::Failure>(json!({}))
                    }));
                    let counters = install::<CounterPlugin>(&runtime, provider, false, HostBindings { dependencies: vec![log], ..Default::default() }, http).await;
                    let client = install::<CounterClient>(&runtime, consumer, true, HostBindings { dependencies: vec![counters], ..Default::default() }, http).await;
                    let saved = Arc::new(Mutex::new(None)); let target = saved.clone();
                    let owner = runtime.install(client.declare(rt::Plugin::new("owner", move |ctx| {
                        *target.lock().unwrap() = Some(ctx.snapshot()); async { Ok(()) }
                    }))).unwrap();
                    owner.ready().await.unwrap();
                    let ctx = saved.lock().unwrap().take().unwrap();
                    call(&client, &ctx, "acquire", json!("9223372036854775807")).await.unwrap();
                    assert!(matches!(call(&client, &ctx, "add", json!(1)).await, Err(sdk::Failure::Rejected(e)) if e.code == "overflow"));
                    call(&client, &ctx, "release", json!({})).await.unwrap();
                    assert_eq!(call(&client, &ctx, "scoped", json!(7)).await.unwrap(), json!("7"));
                    logs.lock().unwrap().clear();
                    call(&client, &ctx, "acquire", json!(40)).await.unwrap();
                    assert_eq!(call(&client, &ctx, "add", json!(2)).await.unwrap(), json!("42"));
                    assert_eq!(call(&client, &ctx, "get", json!({})).await.unwrap(), json!("42"));
                    owner.dispose().await.unwrap();
                    assert_eq!(*logs.lock().unwrap(), vec![json!({"counter_closed":"42"})]);
                    assert!(call(&client, &ctx, "get", json!({})).await.is_err());
                    runtime.shutdown().await.unwrap();
                    assert_eq!(logs.lock().unwrap().len(), 1, "resource inverse executed once");
                }).await.expect("cross-backend scenario timed out");
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_control_pairs_share_channels_and_factories_can_install_other_backends() {
    use cordis_portable_example::workflows::{
        Plugin as Workflow, generated::data::workflows::Settings,
    };
    use sdk::typed::Codec;
    artifacts();
    guest::bytes("wasm_workflows");
    for listener in BACKENDS {
        for emitter in BACKENDS {
            eprintln!("unified controls: {emitter:?} -> {listener:?}");
            tokio::time::timeout(Duration::from_secs(30), async {
                let runtime = rt::Runtime::new();
                let logger = rt::Service::<()>::new("example.counter-log", "1");
                let provided = logger.clone();
                runtime
                    .install(
                        rt::Plugin::new("logger", move |ctx| {
                            let provided = provided.clone();
                            async move { ctx.provide(&provided, Arc::new(())) }
                        })
                        .provides(&logger),
                    )
                    .unwrap()
                    .ready()
                    .await
                    .unwrap();
                let log = Dependency::json(
                    &logger,
                    sdk::Descriptor::new(CounterLogClient::interface()).unwrap(),
                    |_, _, _, _| Box::pin(async { Ok::<_, sdk::Failure>(json!({})) }),
                );
                let tick = rt::Event::<Body, Body>::new("example.tick", "1");
                let chain = rt::Middleware::<Body, Body>::new("example.request", "1");
                let settings = rt::Service::<Body>::new("example.settings", "1");
                let mut nodes = Vec::new();
                for (index, backend) in [listener, emitter].into_iter().enumerate() {
                    let realm = cordis_contract::RealmId(index as u64 + 1);
                    let child_log = log.clone();
                    let bindings = HostBindings {
                        events: vec![cordis_host::EventBinding::new("tick", &tick)],
                        middleware: vec![cordis_host::MiddlewareBinding::new("request", &chain)],
                        provisions: vec![cordis_host::ProvisionBinding::new("settings", &settings)],
                        factories: vec![cordis_host::FactoryBinding::new(
                            "worker",
                            move |_config| {
                                // The requested child uses the opposite node's execution adapter.
                                // Only its authorized factory knows how to prepare that definition.
                                Ok(prepare::<CounterPlugin>(
                                    listener,
                                    Entry::Counter,
                                    HostBindings {
                                        dependencies: vec![child_log.clone()],
                                        ..Default::default()
                                    },
                                    false,
                                )
                                .0)
                            },
                        )],
                        ..Default::default()
                    };
                    let (plugin, endpoint) =
                        prepare::<Workflow>(backend, Entry::Workflow, bindings, false);
                    let view = rt::View::new()
                        .in_realm(endpoint.declaration(), realm)
                        .in_realm(&settings, realm);
                    let provider = runtime.install_in(view.clone(), plugin).unwrap();
                    provider.ready().await.unwrap();
                    let saved = Arc::new(Mutex::new(None));
                    let target = saved.clone();
                    let setting = settings.clone();
                    let owner = runtime
                        .install_in(
                            view,
                            endpoint
                                .declare(rt::Plugin::new("control-owner", move |ctx| {
                                    let target = target.clone();
                                    let setting = setting.clone();
                                    async move {
                                        let value = ctx
                                            .dependency(&setting)?
                                            .with(|value, _| Ok(value.clone()))?;
                                        assert_eq!(
                                            value
                                                .into_message(Settings::contract())
                                                .unwrap()
                                                .into_typed::<Settings>()
                                                .unwrap()
                                                .answer,
                                            Some(42)
                                        );
                                        *target.lock().unwrap() = Some(ctx.snapshot());
                                        Ok(())
                                    }
                                }))
                                .requires(&settings),
                        )
                        .unwrap();
                    owner.ready().await.unwrap();
                    let ctx = saved.lock().unwrap().take().unwrap();
                    nodes.push((provider, owner, endpoint, ctx));
                }
                let (_, owner, endpoint, ctx) = &nodes[1];
                assert_eq!(
                    call(endpoint, ctx, "parallel", json!({"value":"5"}))
                        .await
                        .unwrap(),
                    json!({"values":[{"stopped":{"value":"6"}},{"stopped":{"value":"6"}}]})
                );
                assert_eq!(
                    call(endpoint, ctx, "serial", json!({"value":"5"}))
                        .await
                        .unwrap(),
                    json!({"stopped":{"value":"6"}})
                );
                assert_eq!(
                    call(endpoint, ctx, "run", json!({"value":"5"}))
                        .await
                        .unwrap(),
                    json!({"value":"20"})
                );
                assert_eq!(
                    call(endpoint, ctx, "run", json!({"value":"0"}))
                        .await
                        .unwrap(),
                    json!({"value":"100"})
                );
                call(
                    endpoint,
                    ctx,
                    "spawn_worker",
                    json!({"label":"cross-backend-child"}),
                )
                .await
                .unwrap();
                owner.dispose().await.unwrap();
                // Retiring the listener removes only its own subscriptions.
                nodes[0].1.dispose().await.unwrap();
                nodes[0].0.dispose().await.unwrap();
                let (_, _, endpoint, _) = &nodes[1];
                let saved = Arc::new(Mutex::new(None));
                let target = saved.clone();
                let owner = runtime
                    .install_in(
                        rt::View::new()
                            .in_realm(endpoint.declaration(), cordis_contract::RealmId(2)),
                        endpoint.declare(rt::Plugin::new("after-retire", move |ctx| {
                            *target.lock().unwrap() = Some(ctx.snapshot());
                            async { Ok(()) }
                        })),
                    )
                    .unwrap();
                owner.ready().await.unwrap();
                let ctx = saved.lock().unwrap().take().unwrap();
                assert_eq!(
                    call(endpoint, &ctx, "parallel", json!({"value":"5"}))
                        .await
                        .unwrap(),
                    json!({"values":[{"stopped":{"value":"6"}}]})
                );
                runtime.shutdown().await.unwrap();
            })
            .await
            .expect("cross-backend control scenario timed out");
        }
    }
}
