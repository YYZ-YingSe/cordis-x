use cordis_portable_example::lifecycle::{HeartbeatsClient, LifecycleLogClient};
use cordis_remote::{Dependency, Endpoint, ProcessSpec, RemoteService, Value, json};
use cordis_sdk::*;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

struct Logger;
fn logger() -> Service<Logger> {
    Service::new("example.lifecycle-log", "1")
}
fn tasks() -> Service<RemoteService> {
    Service::new("example.tasks", "1")
        .with_descriptor(cordis_remote::Descriptor::new(HeartbeatsClient::interface()).unwrap())
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
                    *saved.lock().unwrap() = Some(ctx.dependency(&tasks())?);
                    Ok(())
                }
            }
        })
        .requires(&tasks()),
    )?;
    plugin.ready().await?;
    let handle = saved.lock().unwrap().take().unwrap();
    Ok((plugin, handle))
}
#[tokio::main]
async fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut spec = match std::env::args().nth(1).as_deref() {
        None | Some("cpp") => ProcessSpec::new(
            std::path::Path::new(cordis_example_contracts::NATIVE_BIN).join("cordis_tasks"),
        ),
        Some("ts") => {
            let mut spec = ProcessSpec::new("node");
            spec.args.push(
                root.join("sdk/typescript/examples/tasks.ts")
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
        |_, ctx, _, args| {
            Box::pin(async move {
                println!("owner {}: {args}", ctx.owner().fiber.0);
                Ok(json!({}))
            })
        },
    ));
    let rt = Runtime::new();
    let log = Arc::new(Logger);
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
    let provider = rt.install(spec.plugin("tasks", &tasks()))?;
    provider.ready().await?;
    let (_, observer) = caller(&rt).await?;
    let (owner, handle) = caller(&rt).await?;
    invoke_json(&handle, "open", "heartbeat").await?;
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    println!("-- closing caller: signal task, join it, then clean connection --");
    owner.dispose().await?;
    invoke_json(&observer, "join", "heartbeat").await?;
    println!(
        "task status: {}",
        invoke_json(&observer, "status", "heartbeat").await?
    );
    rt.shutdown().await
}
