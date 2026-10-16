use cordis_sdk::*;
use cordis_wasm::{Component, Limits, WasmService, json};
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .ok_or_else(|| Error::Message("usage: component <calculator component.wasm>".into()))?;
    let library = Component::load(path, Limits::default())?;
    let runtime = Runtime::new();
    let calculator = Service::<WasmService>::new("example.calculator", "1").with_descriptor(
        cordis_contract::portable::decode(
            &cordis_example_contracts::resources::calculator_wasm::SERVICE
                .parse()
                .unwrap(),
        )
        .unwrap(),
    );
    let provider = runtime.install(library.plugin("calculator", &calculator, json!({}))?)?;
    let saved = Arc::new(Mutex::new(None));
    let client = runtime.install(
        Plugin::new("client", {
            let calculator = calculator.clone();
            let saved = saved.clone();
            move |ctx| {
                let calculator = calculator.clone();
                let saved = saved.clone();
                async move {
                    *saved.lock().unwrap() = Some(ctx.dependency(&calculator)?);
                    ctx.defer("client cleanup", move |ctx| async move {
                        let value = ctx
                            .dependency(&calculator)?
                            .call(|service, ctx| {
                                Box::pin(async move {
                                    service
                                        .invoke_json(&ctx, "add", json!({"left":20,"right":22}))
                                        .await
                                        .map_err(Into::into)
                                })
                            })
                            .await?;
                        println!("consumer cleanup used live component: {value}");
                        Ok(())
                    })?;
                    Ok(())
                }
            }
        })
        .requires(&calculator),
    )?;
    provider.ready().await?;
    client.ready().await?;
    let handle: ServiceHandle<WasmService> = saved.lock().unwrap().take().unwrap();
    let sum = handle
        .call(|service, ctx| {
            Box::pin(async move {
                service
                    .invoke_json(&ctx, "add", json!({"left":20,"right":22}))
                    .await
                    .map_err(Into::into)
            })
        })
        .await?;
    println!("Wasm add: {sum}");
    let counter = handle
        .call(|service, ctx| {
            Box::pin(async move {
                service
                    .acquire_json(&ctx, "counter", json!({"initial":40}))
                    .await
                    .map_err(Into::into)
            })
        })
        .await?;
    println!(
        "caller-owned counter: {}",
        counter.invoke_json("add", json!(2)).await?
    );
    provider.restart().await?;
    client.ready().await?;
    assert!(matches!(
        counter.invoke_json("get", json!({})).await,
        Err(cordis_wasm::Error::Closed)
    ));
    println!("old instance recovered before restart; old resource rejected");
    runtime.shutdown().await
}
