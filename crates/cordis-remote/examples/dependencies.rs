use cordis_remote::{Dependency, Endpoint, ProcessSpec, RemoteService, json};
use cordis_sdk::*;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

struct Logger {
    generation: usize,
}
fn logger() -> Service<Logger> {
    Service::new("example.logger", "1")
}
fn pricing() -> Service<RemoteService> {
    Service::new("example.pricing", "1").with_descriptor(
        cordis_contract::portable::decode(
            &serde_json::from_str(cordis_example_contracts::resources::pricing::PRICING).unwrap(),
        )
        .unwrap(),
    )
}
fn orders() -> Service<RemoteService> {
    Service::new("example.orders", "1").with_descriptor(
        cordis_contract::portable::decode(
            &serde_json::from_str(cordis_example_contracts::resources::orders::ORDERS).unwrap(),
        )
        .unwrap(),
    )
}

#[tokio::main]
async fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let endpoint = match std::env::args().nth(1).as_deref() {
        None | Some("ipc") => Endpoint::Ipc,
        Some("http") => Endpoint::Http,
        _ => return Err(Error::Message("expected ipc or http".into())),
    };
    let runtime = Runtime::new();
    let generations = Arc::new(AtomicUsize::new(0));
    let log = runtime.install(
        Plugin::new("logger", move |ctx| {
            let generation = generations.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                ctx.defer("logger", move |_| async move {
                    println!("logger {generation}: closed");
                    Ok(())
                })?;
                ctx.provide(&logger(), Arc::new(Logger { generation }))
            }
        })
        .provides(&logger()),
    )?;

    let mut cpp =
        ProcessSpec::new(root.join("target/cpp/cordis_pricing")).requires(Dependency::new_json(
            &logger(),
            cordis_contract::portable::decode(
                &serde_json::from_str(cordis_example_contracts::resources::logger::LOGGER).unwrap(),
            )
            .unwrap(),
            |logger, ctx, method, args| {
                Box::pin(async move {
                    if method != "write" {
                        return Err(cordis_remote::Error::Refused {
                            code: "unknown_method".into(),
                            message: method,
                        });
                    }
                    println!("logger {}: {args}", logger.generation);
                    Ok(json!({
                        "generation": logger.generation,
                        "owner": ctx.owner().fiber.0,
                        "caller": ctx.caller().unwrap().fiber.0,
                    }))
                })
            },
        ));
    cpp.endpoint = endpoint;
    let price = runtime.install(cpp.plugin("pricing", &pricing()))?;
    let mut ts = ProcessSpec::new("node").requires(Dependency::remote(&pricing()));
    ts.args = vec![
        root.join("sdk/typescript/examples/orders.ts")
            .display()
            .to_string(),
    ];
    ts.endpoint = endpoint;
    let order = runtime.install(ts.plugin("orders", &orders()))?;

    // Observe backend startup errors directly, before waiting on their consumers.
    log.ready().await?;
    price.ready().await?;
    order.ready().await?;
    let app = runtime.install(
        Plugin::new("app", |ctx| async move {
            let value = ctx
                .dependency(&orders())?
                .call(|service, ctx| {
                    Box::pin(async move {
                        service
                            .invoke_json(&ctx, "submit", json!({"quantity": 3, "unit_price": 1400}))
                            .await
                            .map_err(Into::into)
                    })
                })
                .await?;
            println!("order result: {value}");
            Ok(())
        })
        .requires(&orders()),
    )?;
    app.ready().await?;
    println!("-- restart logger; dependent processes clean up and restart --");
    log.restart().await?;
    price.ready().await?;
    order.ready().await?;
    app.ready().await?;
    runtime.shutdown().await
}
