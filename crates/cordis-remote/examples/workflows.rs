//! Typed control business is shared across languages; the host chooses transport and grants.
use cordis_portable_example::workflows::generated::{WorkflowClient, WorkflowControls};
use cordis_remote::{
    Endpoint, EventBinding, FactoryBinding, MiddlewareBinding, ProcessSpec, ProvisionBinding,
    RemoteService, json,
};
use cordis_sdk::*;
use std::{
    path::Path,
    sync::{Arc, Mutex},
};
#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut spec = match args.first().map(String::as_str).unwrap_or("ts") {
        "ts" => {
            let mut spec = ProcessSpec::new("node");
            spec.args.push(
                root.join("sdk/typescript/examples/workflows.ts")
                    .display()
                    .to_string(),
            );
            spec
        }
        "cpp" => ProcessSpec::new(root.join("target/cpp/cordis_workflows")),
        "rust" => ProcessSpec::new(root.join("target/debug/examples/workflows")),
        _ => {
            return Err(Error::Message(
                "usage: workflows [ts|cpp|rust] [ipc|http]".into(),
            ));
        }
    };
    spec.endpoint = match args.get(1).map(String::as_str).unwrap_or("ipc") {
        "ipc" => Endpoint::Ipc,
        "http" => Endpoint::Http,
        _ => return Err(Error::Message("invalid endpoint".into())),
    };
    if matches!(spec.endpoint, Endpoint::Ipc) {
        spec.args.push("--stdio-only".into());
    }
    let settings = Service::<cordis_remote::Bytes>::new("example.settings", "1");
    spec = spec
        .event(EventBinding::new("tick", &Event::new("example.tick", "1")))
        .middleware(MiddlewareBinding::new(
            "request",
            &Middleware::new("example.request", "1"),
        ))
        .provision(ProvisionBinding::new("settings", &settings))
        .factory(FactoryBinding::new("worker", |config| {
            WorkflowControls::worker()
                .build(cordis_sdk_core::Message::from_bytes(<cordis_portable_example::workflows::generated::data::workflows::WorkerConfig as cordis_sdk_core::typed::Codec>::contract().clone(), config).map_err(Error::Message)?, |config| {
                    let name = config.label.unwrap();
                    Ok(Plugin::new(name.clone(), move |ctx| {
                        let name = name.clone();
                        async move {
                            println!("worker {name} started, owner={:?}", ctx.owner());
                            ctx.defer("worker", move |_| async move {
                                println!("worker {name} stopped");
                                Ok(())
                            })?;
                            Ok(())
                        }
                    }))
                })
                .map_err(Into::into)
        }));
    let service = Service::<RemoteService>::new("example.workflow", "1")
        .with_descriptor(cordis_sdk_core::Descriptor::new(WorkflowClient::interface()).unwrap());
    let runtime = Runtime::new();
    let provider = runtime.install(spec.plugin("workflow", &service))?;
    provider.ready().await?;
    let saved = Arc::new(Mutex::new(None));
    let owner = runtime.install(
        Plugin::new("application", {
            let saved = saved.clone();
            let service = service.clone();
            let settings = settings.clone();
            move |ctx| {
                let saved = saved.clone();
                let service = service.clone();
                let settings = settings.clone();
                async move {
                    let settings = WorkflowControls::settings()
                        .decode(cordis_sdk_core::Message::from_bytes(<cordis_portable_example::workflows::generated::data::workflows::Settings as cordis_sdk_core::typed::Codec>::contract().clone(),ctx.dependency(&settings)?.with(|v, _| Ok(v.clone()))?).map_err(Error::Message)?)?;
                    println!("settings.answer = {}", settings.answer.unwrap());
                    *saved.lock().unwrap() = Some(ctx.dependency(&service)?);
                    Ok(())
                }
            }
        })
        .requires(&service)
        .requires(&settings),
    )?;
    owner.ready().await?;
    let handle: ServiceHandle<RemoteService> = saved.lock().unwrap().take().unwrap();
    for (method, args) in [
        ("run", json!({"value":"5"})),
        ("serial", json!({"value":"5"})),
        ("parallel", json!({"value":"-1"})),
        ("spawn_worker", json!({"label":"application-worker"})),
    ] {
        let value = handle
            .call(move |service, ctx| {
                Box::pin(async move {
                    service
                        .invoke_json(&ctx, method, args)
                        .await
                        .map_err(Into::into)
                })
            })
            .await?;
        println!("{method}: {value}");
    }
    owner.dispose().await?;
    provider.dispose().await?;
    runtime.shutdown().await
}
