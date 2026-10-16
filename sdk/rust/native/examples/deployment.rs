//! Save deployment.json to stage, probe, promote and drain a version automatically.
use cordis_loader::{
    Configuration, Registry,
    rolling::{Candidates, Readiness, ReadinessPolicy, RollingDeployment},
    watch::{ConfigWatcher, WatchOptions},
};
use cordis_sdk::*;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;

struct Version {
    number: u64,
    ready_at: Instant,
    healthy: bool,
}
fn service() -> Service<Broker<Version>> {
    Service::new("example.deployment", "1")
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: deployment <configuration.json>")?;
    let runtime = Runtime::new();
    let broker = runtime.install(
        Plugin::new("stable broker", |ctx| async move {
            ctx.provide_broker(&service())
        })
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
    let consumer_activation = consumer.ready().await?.activation;
    let client: ServiceHandle<Broker<Version>> = saved.lock().unwrap().take().unwrap();
    let candidates = Candidates::default();
    let mut registry = Registry::new();
    registry.register("version", {
        let candidates = candidates.clone();
        move |config| {
            let number = config["version"]
                .as_u64()
                .ok_or_else(|| Error::Message("version must be u64".into()))?;
            let warmup = config["warmup_ms"]
                .as_u64()
                .ok_or_else(|| Error::Message("warmup_ms must be u64".into()))?;
            if warmup > 60_000 {
                return Err(Error::Message("example warmup exceeds 60 seconds".into()));
            }
            let healthy = config["healthy"]
                .as_bool()
                .ok_or_else(|| Error::Message("healthy must be bool".into()))?;
            let candidates = candidates.clone();
            Ok(Plugin::new("version", move |ctx| {
                let candidates = candidates.clone();
                async move {
                    println!("v{number}: initialized; business warmup {warmup} ms");
                    ctx.defer("version", move |_| async move {
                        println!("v{number}: cleaned");
                        Ok(())
                    })?;
                    candidates.register(ctx.dependency(&service())?.register(Arc::new(
                        Version {
                            number,
                            ready_at: Instant::now() + Duration::from_millis(warmup),
                            healthy,
                        },
                    ))?)
                }
            })
            .requires(&service()))
        }
    })?;
    let deployment = RollingDeployment::new(
        runtime.clone(),
        registry,
        candidates,
        client.clone(),
        ReadinessPolicy {
            timeout: Duration::from_secs(3),
            interval: Duration::from_millis(100),
            consecutive_successes: 2,
        },
        |version, _| {
            Box::pin(async move {
                if !version.healthy {
                    return Err(Error::Message(format!(
                        "v{} rejected by business check",
                        version.number
                    )));
                }
                Ok(if Instant::now() >= version.ready_at {
                    Readiness::Ready
                } else {
                    Readiness::NotReady
                })
            })
        },
    )?;
    let watcher = ConfigWatcher::start(deployment.clone(), &path, WatchOptions::default())?;
    println!(
        "Watching {}; save to deploy, Ctrl-C to drain and stop.",
        watcher.path().display()
    );
    let mut updates = watcher.subscribe();
    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);
    let result: std::result::Result<(), Box<dyn std::error::Error>> = loop {
        tokio::select! {
            result = &mut interrupt => break result.map_err(Into::into),
            result = updates.changed() => {
                if let Err(error) = result { break Err(error.into()); }
                let status = updates.borrow_and_update().clone();
                let state = &status.target.deployment;
                println!("deployment {}: {:?}; current={:?}, candidate={:?}", state.revision, state.phase,
                    state.current.as_ref().map(|g| (&g.id, g.activation)), state.candidate.as_ref().map(|g| (&g.id, g.activation)));
                if let Some(error) = &status.error { eprintln!("file: {error}"); }
                if let Some(error) = &status.backend_error { eprintln!("notifications: {error}"); }
                if let Some(error) = &state.error { eprintln!("deployment: {error}"); }
                if let Some(error) = &state.cleanup_error { eprintln!("retained cleanup responsibility: {error}"); }
                match client.route(|v, _| Box::pin(async move { Ok(v.number) })).await {
                    Ok(version) => println!("traffic: v{version}"), Err(error) => println!("traffic unavailable: {error}"),
                }
                assert_eq!(consumer.snapshot().activation, consumer_activation);
            }
        }
    };
    let stopped = watcher.stop().await;
    let cleared = deployment.apply(Configuration::default()).await;
    let shutdown = runtime.shutdown().await;
    result?;
    stopped?;
    cleared?;
    shutdown?;
    println!("deployment stopped; all generations cleaned; consumer activation was preserved");
    Ok(())
}
