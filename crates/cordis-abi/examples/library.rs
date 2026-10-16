use cordis_abi::{AbiService, Library, json};
use cordis_sdk::*;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::args().nth(1).ok_or_else(|| {
        Error::Message("usage: library <trusted calculator dynamic library>".into())
    })?;
    // SAFETY: build sdk/cpp/examples/abi/calculator.cpp from this repository. Do not
    // use this example to load arbitrary/untrusted native binaries.
    #[allow(unsafe_code)]
    let library = unsafe { Library::load(path)? };
    let runtime = Runtime::new();
    let calculator = Service::<AbiService>::new("example.calculator", "1").with_descriptor(
        cordis_contract::portable::decode(
            &cordis_example_contracts::resources::calculator_abi::SERVICE
                .parse()
                .unwrap(),
        )
        .unwrap(),
    );
    let provider =
        runtime.install(library.plugin("calculator", &calculator, cordis_wire::json!({}))?)?;
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
                        println!("consumer cleanup used live library: {value}");
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
    let handle: ServiceHandle<AbiService> = saved.lock().unwrap().take().unwrap();
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
    println!("C ABI add: {sum}");
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
        counter.invoke_json("get", json!(null)).await,
        Err(cordis_abi::Error::Closed)
    ));
    println!("old instance recovered before restart; old resource rejected");
    runtime.shutdown().await
}
