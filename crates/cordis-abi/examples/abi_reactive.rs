use cordis_abi::{
    AbiService, Capabilities, Dependency, EventBinding, Library, MiddlewareBinding, Value, json,
};
use cordis_portable_example::lifecycle::{LifecycleLogClient, ReactiveClient};
use cordis_sdk::*;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::args().nth(1).ok_or_else(|| {
        Error::Message("usage: abi_reactive <trusted reactive example library>".into())
    })?;
    let runtime = Runtime::new();
    let logger = Service::<()>::new("example.lifecycle-log", "1");
    let declaration = logger.clone();
    let log_plugin = runtime.install(
        Plugin::new("Rust logger", move |ctx| {
            let logger = declaration.clone();
            async move { ctx.provide(&logger, Arc::new(())) }
        })
        .provides(&logger),
    )?;
    log_plugin.ready().await?;
    let dependency = Dependency::new_json(
        &logger,
        cordis_contract::Descriptor::new(LifecycleLogClient::interface()).unwrap(),
        |_, ctx, method, args| {
            Box::pin(async move {
                if method != "write" {
                    return Err(cordis_abi::Error::Rejected {
                        code: "method".into(),
                        message: method,
                    });
                }
                println!("Rust logger (cleanup={}): {args}", ctx.is_cleanup());
                Ok(json!({}))
            })
        },
    );
    // SAFETY: compile sdk/cpp/examples/abi/reactive.cpp from this repository; this
    // in-process example assumes trusted native code and system dependencies.
    #[allow(unsafe_code)]
    let library = unsafe { Library::load(path)? }.with_context(Capabilities {
        dependencies: vec![dependency],
        events: vec![EventBinding::new(
            "notice",
            &Event::new("example.reactive.notice", "1"),
        )],
        middleware: vec![MiddlewareBinding::new(
            "calculate",
            &Middleware::new("example.reactive.calculate", "1"),
        )],
        ..Default::default()
    })?;
    let notifications = Service::<AbiService>::new("example.reactive", "1")
        .with_descriptor(cordis_contract::Descriptor::new(ReactiveClient::interface()).unwrap());
    let provider = runtime.install(library.plugin(
        "C++ pipeline",
        &notifications,
        cordis_wire::json!({}),
    )?)?;
    provider.ready().await?;
    let saved = Arc::new(Mutex::new(None));
    let consumer = runtime.install(
        Plugin::new("consumer", {
            let service = notifications.clone();
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
        .requires(&notifications),
    )?;
    consumer.ready().await?;
    let handle: ServiceHandle<AbiService> = saved.lock().unwrap().take().unwrap();
    call(&handle, "subscribe", json!({})).await?;
    assert_eq!(call(&handle, "process", json!(3)).await?, json!("28"));
    println!("with subscriber: (3 + 10 + 1) * 2 = 28");
    consumer.dispose().await?;
    // A fresh consumer can keep using the provider after the previous consumer's
    // event listener and middleware have both been removed.
    let current = runtime.install(
        Plugin::new("next consumer", {
            let service = notifications.clone();
            move |ctx| {
                let service = service.clone();
                async move {
                    let handle = ctx.dependency(&service)?;
                    assert_eq!(call(&handle, "process", json!(3)).await?, json!("4"));
                    println!("after subscriber disposal: 3 + 1 = 4");
                    Ok(())
                }
            }
        })
        .requires(&notifications),
    )?;
    current.ready().await?;
    runtime.shutdown().await
}

async fn call(handle: &ServiceHandle<AbiService>, method: &str, args: Value) -> Result<Value> {
    let method = method.to_owned();
    handle
        .call(move |service, ctx| {
            Box::pin(async move {
                service
                    .invoke_json(&ctx, &method, args)
                    .await
                    .map_err(Into::into)
            })
        })
        .await
}
