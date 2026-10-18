//! The same C++ business source runs behind ABI and Session adapters.
#![cfg(unix)]
use cordis_abi::{AbiService, Capabilities, Library, Value, json};
use cordis_remote::{Endpoint, ProcessSpec, RemoteService};
use cordis_sdk::*;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

fn artifacts(process: bool) -> &'static Path {
    static ABI: OnceLock<PathBuf> = OnceLock::new();
    static PROCESS: OnceLock<PathBuf> = OnceLock::new();
    let cache = if process { &PROCESS } else { &ABI };
    cache.get_or_init(|| {
        cordis_test_contracts::native::build(&[if process {
            "cordis_portable_process"
        } else {
            "cordis_portable_abi"
        }])
    })
}

trait Portable: Send + Sync + 'static {
    fn invoke_json(
        &self,
        ctx: Context,
        method: &'static str,
        args: Value,
    ) -> BoxFuture<'_, Result<Value>>;
    fn counter(&self, ctx: Context) -> BoxFuture<'_, Result<Value>>;
}
macro_rules! portable {
    ($service:ty) => {
        impl Portable for $service {
            fn invoke_json(&self, ctx: Context, method: &'static str, args: Value) -> BoxFuture<'_, Result<Value>> {
                Box::pin(async move { self.invoke_json(&ctx, method, args).await.map_err(Into::into) })
            }
            fn counter(&self, ctx: Context) -> BoxFuture<'_, Result<Value>> {
                Box::pin(async move {
                    let resource = self.acquire_json(&ctx, "counter", json!({"initial":40})).await?;
                    let value = resource.invoke_json("add", json!(2)).await?;
                    resource.dispose().await?;
                    assert!(resource.invoke_json("get", cordis_wire::json!({})).await.is_err());
                    Ok(value)
                })
            }
        }
    };
}
portable!(AbiService);
portable!(RemoteService);

type Logs = Mutex<Vec<Value>>;
fn logger() -> Service<Logs> {
    Service::new("example.portable-log", "1")
}

async fn scenario<S: Portable>(service: Service<S>, provider: Plugin) {
    let rt = Runtime::new();
    let records = Arc::new(Mutex::new(Vec::new()));
    rt.install(
        Plugin::new("logger", {
            let records = records.clone();
            move |ctx| {
                let records = records.clone();
                async move { ctx.provide(&logger(), records) }
            }
        })
        .provides(&logger()),
    )
    .unwrap()
    .ready()
    .await
    .unwrap();
    let provider = rt.install(provider).unwrap();
    provider.ready().await.unwrap();
    let saved = Arc::new(Mutex::new(None));
    let mut config = InterceptConfig::new(7);
    config.set(&service, json!({"timeout":1000.0}));
    let consumer = rt
        .install_in(
            View::new().with_intercepts(Intercepts::new(config)),
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
    consumer.ready().await.unwrap();
    let handle = saved.lock().unwrap().take().unwrap();
    for (method, args, expected) in [
        ("count", json!({}), json!("1")),
        ("count", json!({}), json!("2")),
        ("save-context", json!({}), json!({})),
        ("expired", json!({}), json!(true)),
        (
            "echo",
            json!({"unicode":"通用 SDK","values":[null,true,42.0]}),
            json!({"unicode":"通用 SDK","values":[null,true,42.0]}),
        ),
        (
            "metadata",
            cordis_wire::json!({}),
            json!({"revision":"7","layers":[{"timeout":1000.0}]}),
        ),
        ("log", json!("portable call"), json!("portable call")),
        ("defer", json!({}), json!({})),
        ("scope", json!({}), json!("42")),
        ("sequence", json!({}), json!({})),
        ("event", json!({}), json!({"values":[{"stopped":"42"}]})),
        ("middleware", json!({}), json!({"wrapped":"42"})),
        ("child", json!({}), json!({})),
        ("task", json!({}), json!({})),
    ] {
        assert_eq!(
            handle
                .call(move |service, ctx| service.invoke_json(ctx, method, args))
                .await
                .unwrap(),
            expected
        );
    }
    assert_eq!(
        handle
            .call(|service, ctx| service.counter(ctx))
            .await
            .unwrap(),
        json!("42")
    );
    assert!(
        handle
            .call(|service, ctx| service.invoke_json(ctx, "missing", json!({})))
            .await
            .is_err()
    );
    assert!(records.lock().unwrap().contains(&json!("scope-cleanup")));
    assert!(records.lock().unwrap().contains(&json!("final-inverse")));
    consumer.dispose().await.unwrap();
    assert!(
        handle
            .call(|service, ctx| service.invoke_json(ctx, "echo", json!({})))
            .await
            .is_err()
    );
    rt.shutdown().await.unwrap();
    assert!(records.lock().unwrap().contains(&json!("task-stopped")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(unix)]
async fn cpp_service_runs_through_async_abi() {
    let build = artifacts(false);
    // SAFETY: built immediately above from the repository's common SDK example.
    #[allow(unsafe_code)]
    let library = unsafe {
        Library::load(build.join(format!(
            "{}cordis_portable_abi{}",
            std::env::consts::DLL_PREFIX,
            std::env::consts::DLL_SUFFIX
        )))
    }
    .unwrap();
    let library = library
        .with_context(Capabilities {
            dependencies: vec![cordis_abi::Dependency::new_json(
                &logger(),
                cordis_contract::Descriptor::new(
                    cordis_contract::portable::decode(
                        &cordis_example_contracts::resources::portable::PORTABLELOG
                            .parse::<Value>()
                            .unwrap(),
                    )
                    .unwrap()
                    .interface()
                    .clone(),
                )
                .unwrap(),
                |logs, _, _, args| {
                    Box::pin(async move {
                        logs.lock().unwrap().push(args.clone());
                        Ok(args)
                    })
                },
            )],
            events: vec![cordis_abi::EventBinding::new(
                "message",
                &Event::new("example.message", "1"),
            )],
            middleware: vec![cordis_abi::MiddlewareBinding::new(
                "chain",
                &Middleware::new("example.chain", "1"),
            )],
            factories: vec![cordis_abi::FactoryBinding::new("worker", |_| {
                Ok(Plugin::new("child", |_| async { Ok(()) }))
            })],
            ..Default::default()
        })
        .unwrap();
    let service = Service::<AbiService>::new("example.portable", "1").with_descriptor(
        cordis_contract::Descriptor::new(
            cordis_contract::portable::decode(
                &cordis_example_contracts::resources::portable::PORTABLECPP
                    .parse::<Value>()
                    .unwrap(),
            )
            .unwrap()
            .interface()
            .clone(),
        )
        .unwrap(),
    );
    let plugin = library
        .plugin("abi", &service, cordis_wire::json!({}))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), scenario(service, plugin))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_service_runs_through_ipc_http() {
    let build = artifacts(true);
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        {
            let mut spec = ProcessSpec::new(build.join("cordis_portable_process")).requires(
                cordis_remote::Dependency::new_json(
                    &logger(),
                    cordis_remote::Descriptor::new(
                        cordis_contract::portable::decode(
                            &cordis_example_contracts::resources::portable::PORTABLELOG
                                .parse::<Value>()
                                .unwrap(),
                        )
                        .unwrap()
                        .interface()
                        .clone(),
                    )
                    .unwrap(),
                    |logs, _, _, args| {
                        Box::pin(async move {
                            logs.lock().unwrap().push(args.clone());
                            Ok(args)
                        })
                    },
                ),
            );
            spec = spec
                .event(cordis_remote::EventBinding::new(
                    "message",
                    &Event::new("example.message", "1"),
                ))
                .middleware(cordis_remote::MiddlewareBinding::new(
                    "chain",
                    &Middleware::new("example.chain", "1"),
                ))
                .factory(cordis_remote::FactoryBinding::new("worker", |_| {
                    Ok(Plugin::new("child", |_| async { Ok(()) }))
                }));
            spec.endpoint = endpoint;
            if matches!(endpoint, Endpoint::Ipc) {
                spec.args.push("--stdio-only".into());
            }
            let service = Service::<RemoteService>::new("example.portable", "1").with_descriptor(
                cordis_remote::Descriptor::new(
                    cordis_contract::portable::decode(
                        &cordis_example_contracts::resources::portable::PORTABLECPP
                            .parse::<Value>()
                            .unwrap(),
                    )
                    .unwrap()
                    .interface()
                    .clone(),
                )
                .unwrap(),
            );
            let plugin = spec.plugin("process", &service);
            tokio::time::timeout(Duration::from_secs(30), scenario(service, plugin))
                .await
                .unwrap();
        }
    }
}
