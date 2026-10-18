use cordis_portable_example::lifecycle::{LifecycleLogClient, PipelineClient};
use cordis_remote::{Dependency, Endpoint, MiddlewareBinding, ProcessSpec, RemoteService, json};
use cordis_sdk::*;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

#[tokio::main]
async fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let rt = Runtime::new();
    let logger = Service::<()>::new("example.lifecycle-log", "1");
    rt.install(
        Plugin::new("logger", {
            let logger = logger.clone();
            move |ctx| {
                let logger = logger.clone();
                async move { ctx.provide(&logger, Arc::new(())) }
            }
        })
        .provides(&logger),
    )?;
    let log = Dependency::new_json(
        &logger,
        cordis_remote::Descriptor::new(LifecycleLogClient::interface()).unwrap(),
        |_, ctx, _, args| {
            Box::pin(async move {
                println!("owner {}: {args}", ctx.owner().fiber.0);
                Ok(json!({}))
            })
        },
    );
    let pipeline = Middleware::<cordis_remote::Bytes, cordis_remote::Bytes>::new(
        "example.middleware.request",
        "1",
    );
    let remote = Service::<RemoteService>::new("example.middleware", "1")
        .with_descriptor(cordis_remote::Descriptor::new(PipelineClient::interface()).unwrap());
    let mut cpp = ProcessSpec::new(
        std::path::Path::new(cordis_example_contracts::NATIVE_BIN).join("cordis_middleware"),
    );
    cpp.args = vec!["--stdio-only".into(), "--listen".into()];
    let timer = rt.install_in(
        View::new().in_realm(&remote, RealmId(1)),
        cpp.requires(log.clone())
            .middleware(MiddlewareBinding::new("request", &pipeline))
            .plugin("C++ timer", &remote),
    )?;
    timer.ready().await?;
    let mut ts = ProcessSpec::new("node");
    ts.args.push(
        root.join("sdk/typescript/examples/middleware.ts")
            .display()
            .to_string(),
    );
    let endpoint = if std::env::args().nth(1).as_deref() == Some("http") {
        Endpoint::Http
    } else {
        ts.args.push("--stdio-only".into());
        Endpoint::Ipc
    };
    ts.endpoint = endpoint;
    rt.install(
        ts.requires(log)
            .middleware(MiddlewareBinding::new("request", &pipeline))
            .plugin("TS processor", &remote),
    )?;
    let saved = Arc::new(Mutex::new(None));
    let owner = rt.install(
        Plugin::new("client", {
            let saved = saved.clone();
            let remote = remote.clone();
            move |ctx| {
                let (saved, remote) = (saved.clone(), remote.clone());
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
    let result = handle
        .call(|remote, ctx| {
            Box::pin(async move {
                remote
                    .invoke_json(&ctx, "run", json!(10))
                    .await
                    .map_err(Into::into)
            })
        })
        .await?;
    assert_eq!(result, json!("25"));
    println!("C++ before -> TS terminal -> C++ after: {result}");
    timer.dispose().await?;
    let result = handle
        .call(|remote, ctx| {
            Box::pin(async move {
                remote
                    .invoke_json(&ctx, "run", json!(10))
                    .await
                    .map_err(Into::into)
            })
        })
        .await?;
    assert_eq!(result, json!("20"));
    println!("C++ middleware removed; TS continues: {result}");
    owner.dispose().await?;
    rt.shutdown().await
}
