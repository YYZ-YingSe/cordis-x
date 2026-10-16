use cordis_abi::{AbiService as PortableService, Capabilities, Dependency, Library};
use cordis_runtime::{Plugin, Runtime, Service};
use cordis_sdk_core::json;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "target/debug/examples".into());
    let load = |name: &str| -> Result<Library, Box<dyn std::error::Error>> {
        let path = std::path::Path::new(&directory).join(format!(
            "{}{name}{}",
            std::env::consts::DLL_PREFIX,
            std::env::consts::DLL_SUFFIX
        ));
        // SAFETY: this example accepts locally built, trusted Rust plugin libraries.
        #[allow(unsafe_code)]
        unsafe {
            Ok(Library::load(path)?)
        }
    };
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
    let counters = Service::<PortableService>::new("example.counters", "1").with_descriptor(
        cordis_sdk_core::Descriptor::new(
            cordis_portable_example::resources::generated::CountersClient::interface(),
        )
        .unwrap(),
    );
    let provider = load("resource_counter")?.with_context(Capabilities {
        dependencies: vec![Dependency::new_json(
            &logger,
            cordis_sdk_core::Descriptor::new(
                cordis_portable_example::resources::generated::CounterLogClient::interface(),
            )
            .unwrap(),
            |_, _, _, value| {
                Box::pin(async move {
                    println!("cleanup: {value}");
                    Ok(json!({}))
                })
            },
        )],
        ..Default::default()
    })?;
    runtime
        .install(provider.plugin("counters", &counters, json!({}))?)?
        .ready()
        .await?;
    let client = Service::<PortableService>::new("example.counter-client", "1").with_descriptor(
        cordis_sdk_core::Descriptor::new(
            cordis_portable_example::resources::generated::CounterHolderClient::interface(),
        )
        .unwrap(),
    );
    let consumer = load("resource_client")?.with_context(Capabilities {
        dependencies: vec![Dependency::abi(&counters)],
        ..Default::default()
    })?;
    runtime
        .install(consumer.plugin("client", &client, json!({}))?)?
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
