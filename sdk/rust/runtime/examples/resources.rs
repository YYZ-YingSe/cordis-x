use cordis_portable_example::resources::{CounterClient, CounterPlugin};
use cordis_runtime::{Plugin, Runtime, Service};
use cordis_sdk_core::{Descriptor, Plugin as _, json};
use cordis_sdk_runtime::{Capabilities, Dependency, Service as PortableService};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = Runtime::new();
    let logger = Service::<()>::new("example.counter-log", "1");
    let log_binding = logger.clone();
    runtime.install(
        Plugin::new("logger", move |ctx| {
            let logger = log_binding.clone();
            async move { ctx.provide(&logger, Arc::new(())) }
        })
        .provides(&logger),
    )?;
    let counters = Service::<PortableService>::new("example.counters", "1")
        .with_descriptor(Descriptor::new(CounterPlugin::interface())?);
    runtime.install(cordis_sdk_runtime::plugin::<CounterPlugin>(
        "counters",
        &counters,
        json!({}),
        Capabilities {
            dependencies: vec![Dependency::new(
                &logger,
                Descriptor::new(
                    cordis_portable_example::resources::generated::CounterLogClient::interface(),
                )?,
                |_, _, _, value| {
                    Box::pin(async move {
                        println!("cleanup: {}", value.to_json().map_err(cordis_sdk_core::typed::violation)?);
                        cordis_sdk_core::Message::new(cordis_portable_example::resources::generated::data::cordis::data::Unit {}).map_err(cordis_sdk_core::typed::violation)
                    })
                },
            )],
            ..Default::default()
        },
    )?)?;
    let client = Service::<PortableService>::new("example.counter-client", "1");
    runtime.install(cordis_sdk_runtime::plugin::<CounterClient>(
        "client",
        &client,
        json!({}),
        Capabilities {
            dependencies: vec![Dependency::portable(&counters)?],
            ..Default::default()
        },
    )?)?;
    let binding = client.clone();
    let caller = runtime.install(
        Plugin::new("caller", move |ctx| {
            let service = binding.clone();
            async move {
                let handle = ctx.dependency(&service)?;
                for (method, args) in [
                    ("acquire", json!(40)),
                    ("add", json!(2)),
                    ("release", json!({})),
                    ("acquire", json!(7)),
                ] {
                    let value = handle
                        .call(move |service, ctx| {
                            Box::pin(async move {
                                service
                                    .invoke_json(&ctx, method, args)
                                    .await
                                    .map_err(|e| cordis_runtime::Error::Message(e.to_string()))
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
    // The second counter belongs to caller and is automatically closed here.
    caller.dispose().await?;
    runtime.shutdown().await?;
    Ok(())
}
