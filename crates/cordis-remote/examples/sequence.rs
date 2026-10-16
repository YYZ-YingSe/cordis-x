use cordis_portable_example::lifecycle::{LifecycleLogClient, SequencesClient};
use cordis_remote::{Dependency, Endpoint, ProcessSpec, RemoteService, Value, json};
use cordis_sdk::*;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;

struct Logger {
    entered: Semaphore,
    release: Semaphore,
}
fn logger() -> Service<Logger> {
    Service::new("example.lifecycle-log", "1")
}
fn pipeline() -> Service<RemoteService> {
    Service::new("example.pipeline", "1")
        .with_descriptor(cordis_remote::Descriptor::new(SequencesClient::interface()).unwrap())
}
async fn invoke_json(
    handle: &ServiceHandle<RemoteService>,
    method: &'static str,
    name: &'static str,
) -> Result<Value> {
    handle
        .call(move |service, ctx| {
            Box::pin(async move {
                service
                    .invoke_json(&ctx, method, json!(name))
                    .await
                    .map_err(Into::into)
            })
        })
        .await
}
async fn caller(rt: &Runtime) -> Result<(PluginHandle, ServiceHandle<RemoteService>)> {
    let saved = Arc::new(Mutex::new(None));
    let plugin = rt.install(
        Plugin::new("caller", {
            let saved = saved.clone();
            move |ctx| {
                let saved = saved.clone();
                async move {
                    *saved.lock().unwrap() = Some(ctx.dependency(&pipeline())?);
                    Ok(())
                }
            }
        })
        .requires(&pipeline()),
    )?;
    plugin.ready().await?;
    let handle = saved.lock().unwrap().take().unwrap();
    Ok((plugin, handle))
}
#[tokio::main]
async fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut spec = match std::env::args().nth(1).as_deref() {
        None | Some("cpp") => ProcessSpec::new(root.join("target/cpp/cordis_sequence")),
        Some("ts") => {
            let mut spec = ProcessSpec::new("node");
            spec.args.push(
                root.join("sdk/typescript/examples/sequence.ts")
                    .display()
                    .to_string(),
            );
            spec
        }
        _ => return Err(Error::Message("expected cpp or ts".into())),
    };
    spec.endpoint = match std::env::args().nth(2).as_deref() {
        None | Some("ipc") => {
            spec.args.push("--stdio-only".into());
            Endpoint::Ipc
        }
        Some("http") => Endpoint::Http,
        _ => return Err(Error::Message("expected ipc or http".into())),
    };
    spec = spec.requires(Dependency::new_json(
        &logger(),
        cordis_remote::Descriptor::new(LifecycleLogClient::interface()).unwrap(),
        |log, ctx, _, args| {
            Box::pin(async move {
                println!("owner {}: {args}", ctx.owner().fiber.0);
                if args == "stopped/step:2" {
                    log.entered.add_permits(1);
                    log.release.acquire().await.unwrap().forget();
                }
                Ok(json!({}))
            })
        },
    ));
    let rt = Runtime::new();
    let log = Arc::new(Logger {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    rt.install(
        Plugin::new("logger", {
            let log = log.clone();
            move |ctx| {
                let log = log.clone();
                async move {
                    ctx.defer("logger", |_| async {
                        println!("logger closed");
                        Ok(())
                    })?;
                    ctx.provide(&logger(), log)
                }
            }
        })
        .provides(&logger()),
    )?;
    let provider = rt.install(spec.plugin("pipeline", &pipeline()))?;
    provider.ready().await?;
    let (_, observer) = caller(&rt).await?;
    let (complete, handle) = caller(&rt).await?;
    invoke_json(&handle, "start", "complete").await?;
    println!(
        "ready: {}",
        invoke_json(&observer, "ready", "complete").await?
    );
    println!(
        "buffers: {}",
        invoke_json(&observer, "count", "complete").await?
    );
    invoke_json(&observer, "release", "complete").await?;
    println!(
        "after manual disposal: {}",
        invoke_json(&observer, "count", "complete").await?
    );
    complete.dispose().await?;
    let (partial, handle) = caller(&rt).await?;
    invoke_json(&handle, "start", "stopped").await?;
    log.entered.acquire().await.unwrap().forget();
    let stopping = partial.clone();
    let disposed = tokio::spawn(async move { stopping.dispose().await });
    while partial.snapshot().state != Lifecycle::Unloading {
        tokio::task::yield_now().await;
    }
    println!("-- caller unloading during step 2; let its admitted step finish --");
    log.release.add_permits(1);
    disposed
        .await
        .map_err(|e| Error::Message(e.to_string()))??;
    println!(
        "ready after unloading: {}",
        invoke_json(&observer, "ready", "stopped").await?
    );
    println!(
        "remaining buffers: {}",
        invoke_json(&observer, "count", "stopped").await?
    );
    rt.shutdown().await
}
