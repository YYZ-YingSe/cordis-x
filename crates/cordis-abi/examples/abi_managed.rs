use cordis_abi::{AbiService, Capabilities, Dependency, Library, Value};
use cordis_sdk::*;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::args().nth(1).ok_or_else(|| {
        Error::Message("usage: abi_managed <trusted managed example library>".into())
    })?;
    let runtime = Runtime::new();
    let logger = Service::<()>::new("example.logger", "1");
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
        cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true)).unwrap(),
        |_, ctx, method, args| {
            Box::pin(async move {
                if method != "write" {
                    return Err(cordis_abi::Error::Rejected {
                        code: "method".into(),
                        message: method,
                    });
                }
                println!("Rust logger (cleanup={}): {args}", ctx.is_cleanup());
                Ok(args)
            })
        },
    );
    // SAFETY: compile sdk/cpp/examples/abi/managed.cpp from this repository; this
    // in-process example assumes trusted native code and system dependencies.
    #[allow(unsafe_code)]
    let library = unsafe { Library::load(path)? }.with_context(Capabilities {
        dependencies: vec![dependency],
        ..Default::default()
    })?;
    let notifications = Service::<AbiService>::new("example.managed", "1").with_descriptor(
        cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true)).unwrap(),
    );
    let provider = runtime.install(library.plugin(
        "C++ connections",
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
    let manual = call(&handle, "open", Value::Null).await?;
    call(&handle, "open", Value::Null).await?;
    println!("closing one connection; its worker stops before subscriptions are removed");
    call(&handle, "close", manual.clone()).await?;
    call(&handle, "close", manual).await?;
    println!("disposing consumer: its other connection closes automatically");
    consumer.dispose().await?;
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
