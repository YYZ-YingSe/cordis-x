#[path = "support/update_arguments.rs"]
mod arguments;
use cordis_loader::{ArtifactRef, Configuration, Entry, Loader, Registry};
use cordis_remote::{RemoteService, json, loader::register_process};
use cordis_sdk::*;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let (paths, options, spec) = arguments::parse()?;
    let v1 = ArtifactRef::from_file(&paths[0], options.package.max_manifest_bytes)?;
    let v2 = ArtifactRef::from_file(&paths[1], options.package.max_manifest_bytes)?;
    assert_ne!(v1.sha256, v2.sha256, "build two different releases");
    let runtime = Runtime::new();
    let service = Service::<RemoteService>::new("example.calculator", "1").with_descriptor(
        cordis_contract::portable::decode(
            &cordis_example_contracts::resources::calculator_process::SERVICE
                .parse()
                .unwrap(),
        )
        .unwrap(),
    );
    let saved = Arc::new(Mutex::new(None));
    let mut registry = Registry::new();
    register_process(
        &mut registry,
        "calculator",
        service.clone(),
        options,
        move |_| Ok(spec.clone()),
    )?;
    registry.register("client", {
        let service = service.clone();
        let saved = saved.clone();
        move |_| {
            let service = service.clone();
            let saved = saved.clone();
            Ok(Plugin::new("client", {
                let service = service.clone();
                move |ctx| {
                    let service = service.clone();
                    let saved = saved.clone();
                    async move {
                        *saved.lock().unwrap() = Some(ctx.dependency(&service)?);
                        ctx.defer("use old code during recovery", move |cleanup| async move {
                            let version = code_version(&cleanup.dependency(&service)?).await?;
                            println!("consumer cleanup still uses {version}");
                            Ok(())
                        })?;
                        Ok(())
                    }
                }
            })
            .requires(&service))
        }
    })?;
    let loader = Loader::new(runtime.clone(), registry);
    let configuration = |artifact| Configuration {
        entries: vec![
            Entry::new("calculator", "calculator", json!({})).artifact(artifact),
            Entry::new("client", "client", json!(null)),
        ],
    };
    loader.apply(configuration(v1)).await?;
    loader.settled().await?;
    let old: ServiceHandle<RemoteService> = saved.lock().unwrap().as_ref().unwrap().clone();
    assert_eq!(code_version(&old).await?, json!("v1"));
    let resource = old
        .call(|s, ctx| {
            Box::pin(async move {
                s.acquire_json(&ctx, "counter", json!({"initial":42}))
                    .await
                    .map_err(Into::into)
            })
        })
        .await?;

    let mut bad = v2.clone();
    bad.sha256 = "0".repeat(64);
    assert!(loader.apply(configuration(bad)).await.is_err());
    assert_eq!(code_version(&old).await?, json!("v1"));
    println!("bad candidate rejected; v1 remains active");

    let next = configuration(v2);
    let plan = loader.plan(next.clone()).await?;
    println!("prepare {}", plan.artifacts["calculator"].sha256);
    loader.apply(next).await?;
    let state = loader.settled().await?;
    let current = saved.lock().unwrap().as_ref().unwrap().clone();
    assert_eq!(code_version(&current).await?, json!("v2"));
    assert!(code_version(&old).await.is_err());
    assert!(resource.invoke_json("get", json!(null)).await.is_err());
    let entry = state.entries.iter().find(|e| e.id == "calculator").unwrap();
    println!(
        "v2 active: {}; old handles rejected",
        entry
            .actual
            .as_ref()
            .unwrap()
            .artifact
            .as_ref()
            .unwrap()
            .sha256
    );
    runtime.shutdown().await?;
    Ok(())
}
async fn code_version(handle: &ServiceHandle<RemoteService>) -> Result<cordis_remote::Value> {
    handle
        .call(|s, ctx| {
            Box::pin(async move {
                s.invoke_json(&ctx, "code-version", json!({}))
                    .await
                    .map_err(Into::into)
            })
        })
        .await
}
