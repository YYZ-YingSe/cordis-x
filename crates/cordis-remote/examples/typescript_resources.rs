//! Two TypeScript processes using public Context/ResourceHandle and Runtime ownership.
use cordis_remote::{Dependency, Endpoint, ProcessSpec, RemoteService, Value, json};
use cordis_runtime::{Plugin, Runtime, Service};
use std::{path::Path, sync::Arc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = match std::env::args().nth(1).as_deref() {
        None | Some("ipc") => Endpoint::Ipc,
        Some("http") => Endpoint::Http,
        _ => return Err("expected ipc or http".into()),
    };
    let process = |client: bool| {
        let mut spec = ProcessSpec::new("node");
        spec.args.push(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../sdk/typescript/examples/resources/process.ts")
                .display()
                .to_string(),
        );
        spec.endpoint = endpoint;
        if matches!(endpoint, Endpoint::Ipc) {
            spec.args.push("--stdio-only".into());
        }
        if client {
            spec.args.push("--client".into());
        }
        spec
    };
    let runtime = Runtime::new();
    let logger = Service::<()>::new("example.logger", "1");
    let binding = logger.clone();
    runtime.install(
        Plugin::new("logger", move |ctx| {
            let binding = binding.clone();
            async move { ctx.provide(&binding, Arc::new(())) }
        })
        .provides(&logger),
    )?;
    let counters = Service::<RemoteService>::new("example.counters", "1").with_descriptor(
        cordis_remote::Descriptor::new(
            serde_json::from_str(cordis_example_contracts::resources::examples::COUNTER).unwrap(),
        )
        .unwrap(),
    );
    runtime
        .install(
            process(false)
                .requires(Dependency::new_json(
                    &logger,
                    cordis_remote::Descriptor::new(cordis_remote::Interface::value_dispatch(true))
                        .unwrap(),
                    |_, _, _, value| {
                        Box::pin(async move {
                            println!("cleanup: {value}");
                            Ok(Value::Null)
                        })
                    },
                ))
                .plugin("counters", &counters),
        )?
        .ready()
        .await?;
    let client = Service::<RemoteService>::new("example.counter-client", "1").with_descriptor(
        cordis_remote::Descriptor::new(cordis_remote::Interface::value_dispatch(true)).unwrap(),
    );
    runtime
        .install(
            process(true)
                .requires(Dependency::remote(&counters))
                .plugin("client", &client),
        )?
        .ready()
        .await?;
    let binding = client.clone();
    let caller = runtime.install(
        Plugin::new("caller", move |ctx| {
            let service = binding.clone();
            async move {
                let handle = ctx.dependency(&service)?;
                for (method, args) in [
                    ("acquire", json!(40)),
                    ("add", json!(2)),
                    ("dispose", Value::Null),
                    ("acquire", json!(7)),
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
                Ok(())
            }
        })
        .requires(&client),
    )?;
    caller.ready().await?;
    // The second counter remains owned by this caller after the callback returns.
    caller.dispose().await?;
    runtime.shutdown().await?;
    Ok(())
}
