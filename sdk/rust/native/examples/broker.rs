use cordis_sdk::*;
use std::sync::{Arc, Mutex};

fn service() -> Service<Broker<u32>> {
    Service::new("example.version", "1")
}

async fn implementation(
    runtime: &Runtime,
    version: u32,
) -> Result<(PluginHandle, BrokerRegistration<u32>)> {
    let saved = Arc::new(Mutex::new(None));
    let handle = runtime.install(
        Plugin::new(format!("v{version}"), {
            let saved = saved.clone();
            move |ctx| {
                let saved = saved.clone();
                async move {
                    ctx.defer("implementation", move |_| async move {
                        println!("v{version}: cleaned");
                        Ok(())
                    })?;
                    *saved.lock().unwrap() =
                        Some(ctx.dependency(&service())?.register(Arc::new(version))?);
                    Ok(())
                }
            }
        })
        .requires(&service()),
    )?;
    handle.ready().await?;
    let registration = saved.lock().unwrap().take().unwrap();
    Ok((handle, registration))
}

#[tokio::main]
async fn main() -> Result<()> {
    let runtime = Runtime::new();
    let broker = runtime.install(
        Plugin::new(
            "broker",
            |ctx| async move { ctx.provide_broker(&service()) },
        )
        .provides(&service()),
    )?;
    broker.ready().await?;
    let saved = Arc::new(Mutex::new(None));
    let consumer = runtime.install(
        Plugin::new("consumer", {
            let saved = saved.clone();
            move |ctx| {
                let saved = saved.clone();
                async move {
                    *saved.lock().unwrap() = Some(ctx.dependency(&service())?);
                    Ok(())
                }
            }
        })
        .requires(&service()),
    )?;
    let activation = consumer.ready().await?.activation;
    let client: ServiceHandle<Broker<u32>> = saved.lock().unwrap().take().unwrap();
    let (v1, first) = implementation(&runtime, 1).await?;
    first.promote()?;
    let transaction = client
        .route(|version, ctx| {
            let version = *version;
            Box::pin(async move {
                ctx.acquire(
                    "transaction",
                    async move { Ok(version) },
                    |version, _| async move {
                        println!("transaction on v{version}: released");
                        Ok(())
                    },
                )
                .await
            })
        })
        .await?;
    let (_, second) = implementation(&runtime, 2).await?;
    second.promote()?;
    assert_eq!(
        client.route(|v, _| Box::pin(async move { Ok(*v) })).await?,
        2
    );
    println!(
        "new requests: v2; existing transaction: v{}",
        transaction.with(|v| *v)?
    );
    let retiring = tokio::spawn({
        let v1 = v1.clone();
        async move { v1.dispose().await }
    });
    while v1.snapshot().state != Lifecycle::Unloading {
        tokio::task::yield_now().await;
    }
    assert!(!retiring.is_finished());
    assert_eq!(
        transaction
            .call(|v, _| Box::pin(async move { Ok(*v) }))
            .await?,
        1
    );
    transaction.dispose().await?;
    retiring
        .await
        .map_err(|error| Error::Message(error.to_string()))??;
    assert_eq!(consumer.snapshot().activation, activation);
    println!("v1 retired; consumer did not restart");
    runtime.shutdown().await
}
