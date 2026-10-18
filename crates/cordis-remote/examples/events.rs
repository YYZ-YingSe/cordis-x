use cordis_portable_example::lifecycle::{EventsClient, LifecycleLogClient};
use cordis_remote::{Dependency, Endpoint, EventBinding, ProcessSpec, RemoteService, Value, json};
use cordis_sdk::*;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;

struct Logger {
    entered: Semaphore,
}
fn logger() -> Service<Logger> {
    Service::new("example.lifecycle-log", "1")
}
fn service(name: &str) -> Service<RemoteService> {
    Service::new(name, "1")
        .with_descriptor(cordis_remote::Descriptor::new(EventsClient::interface()).unwrap())
}
async fn invoke_json(
    handle: &ServiceHandle<RemoteService>,
    method: &'static str,
    args: Value,
) -> Result<Value> {
    handle
        .call(move |remote, ctx| {
            Box::pin(async move {
                remote
                    .invoke_json(&ctx, method, args)
                    .await
                    .map_err(Into::into)
            })
        })
        .await
}
#[tokio::main]
async fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let rt = Runtime::new();
    let log = Arc::new(Logger {
        entered: Semaphore::new(0),
    });
    rt.install(
        Plugin::new("logger", {
            let log = log.clone();
            move |ctx| {
                let log = log.clone();
                async move { ctx.provide(&logger(), log) }
            }
        })
        .provides(&logger()),
    )?;
    let tick = Event::<cordis_remote::Bytes, cordis_remote::Bytes>::new("example.events.tick", "1");
    let dependency = Dependency::new_json(
        &logger(),
        cordis_remote::Descriptor::new(LifecycleLogClient::interface()).unwrap(),
        |log, ctx, _, args| {
            Box::pin(async move {
                println!("owner {}: {args}", ctx.owner().fiber.0);
                if args == "subscriber.enter" {
                    log.entered.add_permits(1);
                }
                Ok(json!({}))
            })
        },
    );
    // The same contract is isolated into separate service realms for each host.
    let remote = service("example.events");
    let mut subscriber = ProcessSpec::new("node");
    subscriber.args = vec![
        root.join("sdk/typescript/examples/events.ts")
            .display()
            .to_string(),
        "--listen".into(),
    ];
    subscriber.endpoint = if std::env::args().nth(1).as_deref() == Some("http") {
        Endpoint::Http
    } else {
        subscriber.args.push("--stdio-only".into());
        Endpoint::Ipc
    };
    let listener = rt.install_in(
        View::new().in_realm(&remote, RealmId(1)),
        subscriber
            .requires(dependency.clone())
            .event(EventBinding::new("tick", &tick))
            .plugin("TS listener", &remote),
    )?;
    listener.ready().await?;
    let mut publisher = ProcessSpec::new(
        std::path::Path::new(cordis_example_contracts::NATIVE_BIN).join("cordis_events"),
    );
    publisher.args.push("--stdio-only".into());
    let provider = rt.install(
        publisher
            .requires(dependency)
            .event(EventBinding::new("tick", &tick))
            .plugin("C++ publisher", &remote),
    )?;
    provider.ready().await?;
    let saved = Arc::new(Mutex::new(None));
    let owner = rt.install(
        Plugin::new("heartbeat owner", {
            let saved = saved.clone();
            let remote = remote.clone();
            move |ctx| {
                let saved = saved.clone();
                let remote = remote.clone();
                async move {
                    *saved.lock().unwrap() = Some(ctx.dependency(&remote)?);
                    Ok(())
                }
            }
        })
        .requires(&remote),
    )?;
    owner.ready().await?;
    let handle = saved.lock().unwrap().take().unwrap();
    invoke_json(&handle, "start", json!({})).await?;
    log.entered.acquire_many(2).await.unwrap().forget();
    println!("-- unload the TypeScript listener; C++ publisher remains active --");
    listener.dispose().await?;
    assert_eq!(
        invoke_json(&handle, "parallel", json!({"value":"0"})).await?,
        json!({})
    );
    println!("new dispatch has no listeners; stopping heartbeat owner");
    owner.dispose().await?;
    rt.shutdown().await
}
