use cordis_remote::{Endpoint, ProcessSpec, RemoteService, json};
use cordis_sdk::*;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let executable = args.get(1).cloned().unwrap_or_else(|| {
        std::path::Path::new(cordis_example_contracts::NATIVE_BIN)
            .join("cordis_calculator")
            .to_string_lossy()
            .into_owned()
    });
    let endpoint = match args.get(2).map(String::as_str).unwrap_or("ipc") {
        "ipc" => Endpoint::Ipc,
        "http" => Endpoint::Http,
        _ => return Err(Error::Message("endpoint must be ipc or http".into())),
    };
    let runtime = Runtime::new();
    let calculator = Service::<RemoteService>::new("example.calculator", "1").with_descriptor(
        cordis_contract::portable::decode(
            &cordis_example_contracts::resources::calculator_process::SERVICE
                .parse()
                .unwrap(),
        )
        .unwrap(),
    );
    let saved = Arc::new(Mutex::new(None));
    let client = runtime.install(
        Plugin::new("client", {
            let saved = saved.clone();
            let calculator = calculator.clone();
            move |ctx| {
                let saved = saved.clone();
                let calculator = calculator.clone();
                async move {
                    *saved.lock().unwrap() = Some(ctx.dependency(&calculator)?);
                    ctx.defer("client cleanup", move |ctx| async move {
                        let result = ctx
                            .dependency(&calculator)?
                            .call(|remote, ctx| {
                                Box::pin(async move {
                                    remote
                                        .invoke_json(&ctx, "add", json!({"left":20,"right":22}))
                                        .await
                                        .map_err(Into::into)
                                })
                            })
                            .await?;
                        println!("consumer cleanup used live process: {result}");
                        Ok(())
                    })?;
                    Ok(())
                }
            }
        })
        .requires(&calculator),
    )?;
    let mut spec = ProcessSpec::new(executable);
    spec.endpoint = endpoint;
    let provider = runtime.install(spec.plugin("cpp-calculator", &calculator))?;
    // Observe provider failure directly instead of indefinitely waiting on a missing dependency.
    provider.ready().await?;
    client.ready().await?;
    let remote: ServiceHandle<RemoteService> = saved.lock().unwrap().take().unwrap();
    let value = remote
        .call(|service, ctx| {
            Box::pin(async move {
                service
                    .invoke_json(
                        &ctx,
                        "echo",
                        json!({"hello":"中文", "values":[1,true,null]}),
                    )
                    .await
                    .map_err(Into::into)
            })
        })
        .await?;
    println!("{endpoint:?} / protobuf: {value}");
    let counter = remote
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
        "caller-owned remote counter: {}",
        counter.invoke_json("add", json!(2)).await?
    );
    provider.restart().await?;
    client.ready().await?;
    println!(
        "process restarted; consumer epoch = {}",
        client.snapshot().activation.unwrap().epoch
    );
    assert!(matches!(
        counter.invoke_json("get", json!(null)).await,
        Err(cordis_remote::Error::Closed)
    ));
    println!("old resource released before restart; stale handle rejected");
    runtime.shutdown().await
}
