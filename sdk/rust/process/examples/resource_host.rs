//! Two Rust plugin processes reuse the same counter business as native Runtime.
use cordis_remote::{Dependency, Endpoint, ProcessSpec, RemoteService, Value, json};
use cordis_runtime::{Plugin, Runtime, Service};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let executable = args
        .get(1)
        .map(String::as_str)
        .unwrap_or("target/debug/examples/resource_plugin");
    let endpoint = match args.get(2).map(String::as_str).unwrap_or("ipc") {
        "ipc" => Endpoint::Ipc,
        "http" => Endpoint::Http,
        _ => return Err("endpoint must be ipc or http".into()),
    };
    let mut spec = ProcessSpec::new(executable);
    spec.endpoint = endpoint;
    if matches!(endpoint, Endpoint::Ipc) {
        spec.args.push("--stdio-only".into());
    }

    let runtime = Runtime::new();
    let logger = Service::<()>::new("example.logger", "1");
    let log_binding = logger.clone();
    runtime
        .install(
            Plugin::new("logger", move |ctx| {
                let logger = log_binding.clone();
                async move { ctx.provide(&logger, Arc::new(())) }
            })
            .provides(&logger),
        )?
        .ready()
        .await?;

    let counters = Service::<RemoteService>::new("example.counters", "1").with_descriptor(
        cordis_remote::Descriptor::new(
            serde_json::from_str(cordis_example_contracts::resources::examples::COUNTER).unwrap(),
        )
        .unwrap(),
    );
    runtime
        .install(
            spec.clone()
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
    spec.args.push("--client".into());
    runtime
        .install(
            spec.requires(Dependency::remote(&counters))
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
    // The second counter remains owned by caller, across both process boundaries.
    caller.dispose().await?;
    runtime.shutdown().await?;
    Ok(())
}
