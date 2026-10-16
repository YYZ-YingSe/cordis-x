use cordis_remote::{Endpoint, FactoryBinding, ProcessSpec, ProvisionBinding, RemoteService, json};
use cordis_sdk::*;
use std::sync::{Arc, Mutex};
#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let mut spec = match args.get(1).map(String::as_str).unwrap_or("ts") {
        "ts" => {
            let mut s = ProcessSpec::new("node");
            s.args.push("sdk/typescript/examples/composition.ts".into());
            s
        }
        "cpp" => ProcessSpec::new("target/cpp/cordis_composition"),
        _ => {
            return Err(Error::Message(
                "usage: composition [ts|cpp] [ipc|http]".into(),
            ));
        }
    };
    spec.endpoint = match args.get(2).map(String::as_str).unwrap_or("ipc") {
        "ipc" => Endpoint::Ipc,
        "http" => Endpoint::Http,
        _ => return Err(Error::Message("invalid endpoint".into())),
    };
    if matches!(spec.endpoint, Endpoint::Ipc) {
        spec.args.push("--stdio-only".into());
    }
    let rt = Runtime::new();
    let settings = Service::<cordis_remote::Bytes>::new("example.settings", "1");
    let service = Service::<RemoteService>::new("example.composition", "1").with_descriptor(
        cordis_remote::Descriptor::new(
            cordis_portable_example::lifecycle::ComposerClient::interface(),
        )
        .unwrap(),
    );
    spec = spec
        .factory(FactoryBinding::new("worker", |config| {
            cordis_portable_example::lifecycle::ComposerControls::worker()
                .build(cordis_sdk_core::Message::from_bytes(<cordis_portable_example::lifecycle::data::lifecycle::WorkerConfig as cordis_sdk_core::typed::Codec>::contract().clone(), config).map_err(Error::Message)?, |config| {
                    // This factory constructs a definition. Resource acquisition belongs in
                    // its initializer, so the Runtime can own and unwind it.
                    let name = config.name.expect("validated required name");
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
                .map_err(Error::Portable)
        }))
        .provision(ProvisionBinding::new("settings", &settings));
    let provider = rt.install(spec.plugin("composer", &service))?;
    provider.ready().await?;
    let saved = Arc::new(Mutex::new(None));
    let owner = rt.install(
        Plugin::new("application", {
            let saved = saved.clone();
            let service = service.clone();
            let settings = settings.clone();
            move |ctx| {
                let saved = saved.clone();
                let service = service.clone();
                let settings = settings.clone();
                async move {
                    println!(
                        "published settings bytes: {:?}",
                        ctx.dependency(&settings)?.with(|v, _| Ok(v.clone()))?
                    );
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
    let state = handle
        .call(|s, ctx| {
            Box::pin(async move {
                s.invoke_json(&ctx, "install", json!({"name":"application-worker"}))
                    .await
                    .map_err(Into::into)
            })
        })
        .await?;
    println!("child: {state}");
    owner.dispose().await?; // automatically retires its installed child
    rt.shutdown().await
}
