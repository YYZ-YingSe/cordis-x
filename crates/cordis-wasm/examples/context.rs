use cordis_host::Dependency;
use cordis_portable_example::context::{ContextClient, ContextLogClient};
use cordis_sdk::*;
use cordis_wasm::{Capabilities, ContextComponent, ContextService, Limits, Wasi, json};
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .ok_or_else(|| Error::Message("usage: context <context component.wasm>".into()))?;
    let runtime = Runtime::new();
    let logger = Service::<()>::new("example.context-log", "1");
    let native = runtime.install(
        Plugin::new("logger", {
            let logger = logger.clone();
            move |ctx| {
                let logger = logger.clone();
                async move { ctx.provide(&logger, Arc::new(())) }
            }
        })
        .provides(&logger),
    )?;
    native.ready().await?;

    let mut capabilities = Capabilities {
        bindings: cordis_host::HostBindings {
            dependencies: vec![Dependency::json(
                &logger,
                cordis_contract::Descriptor::new(ContextLogClient::interface()).unwrap(),
                |_, ctx, method, args| {
                    Box::pin(async move {
                        println!("native {method} (cleanup={}): {args}", ctx.is_cleanup());
                        Ok::<_, cordis_wasm::Error>(args)
                    })
                },
            )],
            ..Default::default()
        },
        // Opt in to clocks/random and this environment entry. Directories,
        // network, inherited environment and standard input stay closed.
        wasi: Some(Wasi {
            environment: [("APP_MODE".into(), "demo".into())].into(),
            ..Wasi::default()
        }),
        ..Capabilities::default()
    };
    capabilities
        .bindings
        .events
        .push(cordis_host::EventBinding::new(
            "messages",
            &Event::new("messages", "1"),
        ));
    capabilities
        .bindings
        .middleware
        .push(cordis_host::MiddlewareBinding::new(
            "pipeline",
            &Middleware::new("pipeline", "1"),
        ));
    capabilities.bindings.factories.push(cordis_host::FactoryBinding::new(
        "worker",
        |config| {
            cordis_portable_example::context::ContextControls::worker()
                .build(cordis_sdk_core::Message::from_bytes(<cordis_portable_example::context::data::cordis::data::Unit as cordis_sdk_core::typed::Codec>::contract().clone(),config.into_bytes().map_err(Error::Message)?).map_err(|e|cordis_wasm::Error::from(Error::Message(e)))?, |_| Ok(Plugin::new("worker", |_| async { Ok(()) })))
                .map_err(Error::from)
        },
    ));
    let component = ContextComponent::load(path, Limits::default(), capabilities)?;
    let service = Service::<ContextService>::new("example.context", "1")
        .with_descriptor(cordis_contract::Descriptor::new(ContextClient::interface()).unwrap());
    let provider = runtime.install(component.plugin("wasm", &service, json!({}))?)?;
    provider.ready().await?;

    let saved = Arc::new(Mutex::new(None));
    let client = runtime.install(
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
    client.ready().await?;
    // Ordinary event/middleware registrations become visible when their owner
    // is ready, so demonstrate dispatch after the client's initialization.
    let handle: ServiceHandle<ContextService> = saved.lock().unwrap().take().unwrap();
    for (method, args) in [
        ("echo", json!(42)),
        ("subscribe", json!("first")),
        ("subscribe", json!("second")),
        ("scope", json!({})),
        ("sequence", json!({})),
        ("events", json!({})),
        ("middleware", json!({})),
        ("install", json!({})),
        ("wasi-env", json!({})),
        ("task", json!({})),
    ] {
        let value = handle
            .call(move |s, ctx| {
                Box::pin(async move { s.invoke_json(&ctx, method, args).await.map_err(Into::into) })
            })
            .await?;
        match method {
            "echo" => assert_eq!(value, json!(42.0)),
            "middleware" => assert_eq!(value, json!("42")),
            "events" => assert_eq!(value, json!({"continued":{}})),
            "wasi-env" => assert_eq!(
                value,
                json!({"entries":[{"name":"APP_MODE","value":"demo"}]})
            ),
            _ => {}
        }
        println!("Wasm {method}: {value}");
    }
    // Wait for the client's task and reverse subscriptions before deactivation.
    provider.dispose().await?;
    runtime.shutdown().await
}
