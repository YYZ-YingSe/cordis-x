//! cargo run -p cordis-remote --example intercepts -- ts http
use cordis_loader::{Configuration, Entry, Loader, Registry};
use cordis_remote::{Endpoint, ProcessSpec, RemoteService, Value, json};
use cordis_sdk::*;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let typescript = args.first().map(String::as_str).unwrap_or("ts") == "ts";
    let mut spec = ProcessSpec::new(if typescript {
        PathBuf::from("node")
    } else {
        std::path::Path::new(cordis_example_contracts::NATIVE_BIN).join("cordis_calculator")
    });
    if typescript {
        spec.args = vec![
            root.join("sdk/typescript/examples/calculator.ts")
                .display()
                .to_string(),
        ];
    }
    spec.endpoint = if args.get(1).map(String::as_str) == Some("http") {
        Endpoint::Http
    } else {
        Endpoint::Ipc
    };
    let service = Service::<RemoteService>::new("example.calculator", "1").with_descriptor(
        cordis_contract::portable::decode(
            &cordis_example_contracts::resources::calculator_process::SERVICE
                .parse()
                .unwrap(),
        )
        .unwrap(),
    );
    let rt = Runtime::new();
    rt.install(spec.plugin("calculator", &service))?
        .ready()
        .await?;
    let mut registry = Registry::new();
    registry.register_portable_intercept(&service, |layers| {
        let mut merged = json!({});
        for layer in layers {
            let object = layer
                .as_object()
                .ok_or_else(|| Error::Message("expected configuration object".into()))?;
            for (key, value) in object {
                merged[key] = value.clone();
            }
        }
        if merged["timeout_ms"].as_u64().is_none_or(|n| n == 0) {
            return Err(Error::Message("timeout_ms must be positive".into()));
        }
        Ok(merged)
    })?;
    let saved = Arc::new(Mutex::new(None));
    registry.register("client", {
        let saved = saved.clone();
        let service = service.clone();
        move |_| {
            let saved = saved.clone();
            let service = service.clone();
            let dependency = service.clone();
            Ok(Plugin::new("client", move |ctx| {
                let saved = saved.clone();
                let service = service.clone();
                async move {
                    *saved.lock().unwrap() = Some(ctx.dependency(&service)?);
                    Ok(())
                }
            })
            .requires(&dependency))
        }
    })?;
    let loader = Loader::new(rt.clone(), registry);
    let config = |timeout| Configuration {
        entries: vec![
            Entry::new("client", "client", Value::Null)
                .intercept(&service, json!({"timeout_ms":timeout})),
        ],
    };
    loader.apply(config(1000)).await?;
    loader.settled().await?;
    let handle = saved.lock().unwrap().clone().unwrap();
    let resource = handle
        .call(|s, ctx| {
            Box::pin(async move { Ok(s.acquire_json(&ctx, "counter", json!({})).await?) })
        })
        .await?;
    println!("before: {}", read(&handle).await?);
    let report = loader.apply(config(3000)).await?;
    println!(
        "updated without restart: {:?}",
        report.plan.update_intercepts
    );
    println!("new call: {}", read(&handle).await?);
    println!(
        "existing resource: {}",
        resource
            .invoke_json("metadata", serde_json::json!({}))
            .await?
    );
    resource.dispose().await?;
    rt.shutdown().await?;
    Ok(())
}
async fn read(handle: &ServiceHandle<RemoteService>) -> Result<Value> {
    handle
        .call(|s, ctx| {
            Box::pin(async move {
                Ok(s.invoke_json(&ctx, "metadata", serde_json::json!({}))
                    .await?)
            })
        })
        .await
}
