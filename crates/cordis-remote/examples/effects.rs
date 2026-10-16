use cordis_portable_example::lifecycle::{AuditedSubscriptionsClient, LifecycleLogClient};
use cordis_remote::{Dependency, Endpoint, ProcessSpec, RemoteService, Value, json};
use cordis_sdk::*;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

fn subscriptions() -> Service<RemoteService> {
    Service::new("example.audited-subscriptions", "1").with_descriptor(
        cordis_remote::Descriptor::new(AuditedSubscriptionsClient::interface()).unwrap(),
    )
}
fn logger() -> Service<()> {
    Service::new("example.lifecycle-log", "1")
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
    let mut spec = match std::env::args().nth(1).as_deref() {
        None | Some("cpp") => ProcessSpec::new(root.join("target/cpp/cordis_subscriptions")),
        Some("ts") => {
            let mut spec = ProcessSpec::new("node");
            spec.args.push(
                root.join("sdk/typescript/examples/subscriptions.ts")
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
    spec.args.push("--logger".into());
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
    rt.install(
        Plugin::new("logger", |ctx| async move {
            ctx.defer("logger", |_| async {
                println!("logger closed");
                Ok(())
            })?;
            ctx.provide(&logger(), Arc::new(()))
        })
        .provides(&logger()),
    )?;
    let provider = rt.install(spec.plugin("subscriptions", &subscriptions()))?;
    provider.ready().await?;
    let saved = Arc::new(Mutex::new(None));
    let observer = rt.install(
        Plugin::new("observer", {
            let saved = saved.clone();
            move |ctx| {
                let saved = saved.clone();
                async move {
                    *saved.lock().unwrap() = Some(ctx.dependency(&subscriptions())?);
                    Ok(())
                }
            }
        })
        .requires(&subscriptions()),
    )?;
    observer.ready().await?;
    let observer = saved.lock().unwrap().take().unwrap();
    let caller = rt.install(
        Plugin::new("caller", |ctx| async move {
            let service = ctx.dependency(&subscriptions())?;
            invoke_json(&service, "subscribe", json!("automatic")).await?;
            invoke_json(&service, "subscribe", json!("manual")).await?;
            Ok(())
        })
        .requires(&subscriptions()),
    )?;
    caller.ready().await?;
    println!(
        "listeners after calls returned: {}",
        invoke_json(&observer, "publish", json!("hello")).await?
    );
    invoke_json(&observer, "unsubscribe", json!("manual")).await?;
    println!(
        "listeners after manual dispose: {}",
        invoke_json(&observer, "count", json!({})).await?
    );
    caller.dispose().await?;
    println!(
        "listeners after caller disposal: {}",
        invoke_json(&observer, "publish", json!("no listeners")).await?
    );
    rt.shutdown().await
}
