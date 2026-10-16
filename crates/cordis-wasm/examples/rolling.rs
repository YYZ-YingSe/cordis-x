use cordis_loader::{ArtifactRef, Configuration, Entry, Loader, Registry};
use cordis_sdk::*;
use cordis_wasm::{Limits, Value, WasmService, json, loader::register_broker_component};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let paths: Vec<_> = std::env::args().skip(1).collect();
    if paths.len() != 2 {
        return Err("usage: rolling <v1.component.wasm> <v2.component.wasm>".into());
    }
    let limits = Limits::default();
    let v1 = ArtifactRef::from_file(&paths[0], limits.artifact_bytes)?;
    let v2 = ArtifactRef::from_file(&paths[1], limits.artifact_bytes)?;
    let runtime = Runtime::new();
    let service = Service::<Broker<WasmService>>::new("example.calculator", "1").with_descriptor(
        cordis_contract::portable::decode(
            &cordis_example_contracts::resources::calculator_wasm::SERVICE
                .parse()
                .unwrap(),
        )
        .unwrap(),
    );
    let broker = runtime.install(
        Plugin::new("stable broker", {
            let service = service.clone();
            move |ctx| {
                let service = service.clone();
                async move { ctx.provide_broker(&service) }
            }
        })
        .provides(&service),
    )?;
    broker.ready().await?;
    let saved = Arc::new(Mutex::new(None));
    let consumer = runtime.install(
        Plugin::new("client", {
            let service = service.clone();
            let saved = saved.clone();
            move |ctx| {
                let service = service.clone();
                let saved = saved.clone();
                async move {
                    *saved.lock().unwrap() = Some(ctx.dependency(&service)?);
                    Ok(())
                }
            }
        })
        .requires(&service),
    )?;
    let consumer_activation = consumer.ready().await?.activation;
    let handle: ServiceHandle<Broker<WasmService>> = saved.lock().unwrap().take().unwrap();

    let candidates = Arc::new(Mutex::new(BTreeMap::new()));
    let mut registry = Registry::new();
    register_broker_component(
        &mut registry,
        "calculator",
        service,
        cordis_wasm::Compiler::default(),
        limits,
        {
            let candidates = candidates.clone();
            move |candidate| {
                candidates
                    .lock()
                    .unwrap()
                    .insert(candidate.activation().ok_or(Error::Closed)?, candidate);
                Ok(())
            }
        },
    )?;
    let loader = Loader::new(runtime.clone(), registry);
    let old = Entry::new("v1", "calculator", json!({})).artifact(v1);
    let new = Entry::new("v2", "calculator", json!({})).artifact(v2);
    loader
        .apply(Configuration {
            entries: vec![old.clone()],
        })
        .await?;
    loader.settled().await?;
    let promote = |id: &str| -> Result<()> {
        let activation = loader
            .snapshot()
            .entries
            .iter()
            .find(|e| e.id == id)
            .and_then(|e| e.instance.as_ref())
            .and_then(|s| s.activation)
            .ok_or(Error::Closed)?;
        candidates.lock().unwrap()[&activation].promote()
    };
    promote("v1")?;
    let resource = handle
        .route(|s, ctx| {
            Box::pin(async move {
                s.acquire_json(&ctx, "counter", json!({"initial": 42}))
                    .await
                    .map_err(Into::into)
            })
        })
        .await?;
    println!("v1 active; acquired counter from v1");

    // Two independent Entries coexist. Replacing one Entry uses stop/start handoff.
    loader
        .apply(Configuration {
            entries: vec![old, new.clone()],
        })
        .await?;
    loader.settled().await?;
    assert_eq!(version(&handle).await?, json!("v1"));
    promote("v2")?;
    assert_eq!(version(&handle).await?, json!("v2"));
    println!("new requests use v2; consumer activation unchanged");

    let retiring = tokio::spawn({
        let loader = loader.clone();
        async move { loader.apply(Configuration { entries: vec![new] }).await }
    });
    // apply performs cold artifact compilation before it starts retirement.
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        loop {
            let closing = loader.snapshot().entries.iter().any(|e| {
                e.id == "v1"
                    && e.instance
                        .as_ref()
                        .is_some_and(|s| s.state == Lifecycle::Unloading)
            });
            if closing {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(!retiring.is_finished());
    assert_eq!(resource.invoke_json("add", json!(1)).await?, json!("43"));
    assert_eq!(version(&handle).await?, json!("v2"));
    println!("v1 draining; its existing counter still returns 43");
    resource.dispose().await?;
    retiring.await??;
    assert_eq!(consumer.snapshot().activation, consumer_activation);
    assert!(resource.invoke_json("get", json!({})).await.is_err());
    println!("counter released; v1 retired; v2 continues serving");
    runtime.shutdown().await?;
    Ok(())
}

async fn version(handle: &ServiceHandle<Broker<WasmService>>) -> Result<Value> {
    handle
        .route(|s, ctx| {
            Box::pin(async move {
                s.invoke_json(&ctx, "code-version", json!({}))
                    .await
                    .map_err(Into::into)
            })
        })
        .await
}
