#[path = "support/update_arguments.rs"]
mod arguments;
use cordis_loader::{
    ArtifactRef, Configuration, ConfigurationChanges, Entry, Registry,
    rolling::{Candidates, DeploymentPhase, Readiness, ReadinessPolicy, RollingDeployment},
};
use cordis_remote::{RemoteService, Value, json, loader::register_broker_process};
use cordis_sdk::*;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let (paths, options, spec) = arguments::parse()?;
    let v1 = ArtifactRef::from_file(&paths[0], options.package.max_manifest_bytes)?;
    let v2 = ArtifactRef::from_file(&paths[1], options.package.max_manifest_bytes)?;
    let runtime = Runtime::new();
    let service = Service::<Broker<RemoteService>>::new("example.calculator", "1").with_descriptor(
        cordis_contract::portable::decode(
            &cordis_example_contracts::resources::calculator_process::SERVICE
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
    let handle: ServiceHandle<Broker<RemoteService>> = saved.lock().unwrap().take().unwrap();

    let candidates = Candidates::default();
    let mut registry = Registry::new();
    register_broker_process(
        &mut registry,
        "calculator",
        service,
        options,
        move |_| Ok(spec.clone()),
        {
            let candidates = candidates.clone();
            move |candidate| candidates.register(candidate)
        },
    )?;
    let deployment = RollingDeployment::new(
        runtime.clone(),
        registry,
        candidates,
        handle.clone(),
        ReadinessPolicy::default(),
        |service, ctx| {
            Box::pin(async move {
                // Business readiness is an actual call on the unpublished candidate.
                let result = service
                    .invoke_json(&ctx, "echo", json!({"message":"readiness"}))
                    .await?;
                if result != json!({"message":"readiness"}) {
                    return Err(Error::Message("candidate echo failed".into()));
                }
                Ok(Readiness::Ready)
            })
        },
    )?;
    let old = Entry::new("calculator", "calculator", json!({})).artifact(v1);
    let new = Entry::new("calculator", "calculator", json!({})).artifact(v2);
    let initial = deployment
        .apply(Configuration { entries: vec![old] })
        .await?;
    let old_id = initial.deployment.current.as_ref().unwrap().id.clone();
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

    let retiring = tokio::spawn({
        let deployment = deployment.clone();
        async move { deployment.apply(Configuration { entries: vec![new] }).await }
    });
    let mut changes = deployment.subscribe();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let snapshot = deployment.snapshot();
            if snapshot.deployment.phase == DeploymentPhase::Draining
                && snapshot.loader.entries.iter().any(|e| {
                    e.id == old_id
                        && e.instance
                            .as_ref()
                            .is_some_and(|s| s.state == Lifecycle::Unloading)
                })
            {
                break;
            }
            changes.changed().await?;
        }
        Ok::<_, cordis_loader::Error>(())
    })
    .await??;
    println!("candidate passed business readiness; new requests use v2");
    assert!(!retiring.is_finished());
    assert_eq!(resource.invoke_json("add", json!(1)).await?, json!("43"));
    assert_eq!(
        resource
            .invoke_json("code-version", serde_json::json!({}))
            .await?,
        json!("v1")
    );
    assert_eq!(version(&handle).await?, json!("v2"));
    println!("v1 draining; its existing counter still returns 43");
    resource.dispose().await?;
    retiring.await??;
    assert_eq!(consumer.snapshot().activation, consumer_activation);
    assert!(
        resource
            .invoke_json("get", serde_json::json!({}))
            .await
            .is_err()
    );
    // Finished resource handles can still retain snapshots; the candidate table is weak.
    drop(resource);
    println!("counter released; v1 retired; v2 continues serving");
    deployment.apply(Configuration::default()).await?;
    runtime.shutdown().await?;
    Ok(())
}

async fn version(handle: &ServiceHandle<Broker<RemoteService>>) -> Result<Value> {
    handle
        .route(|s, ctx| {
            Box::pin(async move {
                s.invoke_json(&ctx, "code-version", serde_json::json!({}))
                    .await
                    .map_err(Into::into)
            })
        })
        .await
}
