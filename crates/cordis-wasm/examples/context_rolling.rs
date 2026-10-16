use cordis_portable_example::context::{ContextClient, ContextLogClient};
use cordis_sdk::*;
use cordis_wasm::{Capabilities, ContextComponent, ContextService, Dependency, Limits, Wasi, json};
use std::sync::{Arc, Mutex};
#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 3 {
        return Err(Error::Message(
            "usage: context_rolling <context-v1.component.wasm> <context-v2.component.wasm>".into(),
        ));
    }
    let rt = Runtime::new();
    let logger = Service::<()>::new("example.context-log", "1");
    let p = rt.install(
        Plugin::new("logger", {
            let logger = logger.clone();
            move |ctx| {
                let logger = logger.clone();
                async move { ctx.provide(&logger, Arc::new(())) }
            }
        })
        .provides(&logger),
    )?;
    p.ready().await?;
    let broker = Service::<Broker<ContextService>>::new("example.context", "1")
        .with_descriptor(cordis_contract::Descriptor::new(ContextClient::interface()).unwrap());
    let p = rt.install(
        Plugin::new("broker", {
            let broker = broker.clone();
            move |ctx| {
                let broker = broker.clone();
                async move { ctx.provide_broker(&broker) }
            }
        })
        .provides(&broker),
    )?;
    p.ready().await?;
    let saved = Arc::new(Mutex::new(None));
    let client = rt.install(
        Plugin::new("client", {
            let saved = saved.clone();
            let broker = broker.clone();
            move |ctx| {
                let saved = saved.clone();
                let broker = broker.clone();
                async move {
                    *saved.lock().unwrap() = Some(ctx.dependency(&broker)?);
                    Ok(())
                }
            }
        })
        .requires(&broker),
    )?;
    client.ready().await?;
    let handle: ServiceHandle<Broker<ContextService>> = saved.lock().unwrap().take().unwrap();
    let activation = client.snapshot().activation;
    let caps = Capabilities {
        wasi: Some(Wasi::default()),
        dependencies: vec![Dependency::new_json(
            &logger,
            cordis_contract::Descriptor::new(ContextLogClient::interface()).unwrap(),
            |_, ctx, _, args| {
                Box::pin(async move {
                    println!("logger cleanup={}: {args}", ctx.is_cleanup());
                    Ok(args)
                })
            },
        )],
        ..Capabilities::default()
    };
    let mut old: Option<PluginHandle> = None;
    for path in &args[1..] {
        let component = ContextComponent::load(path, Limits::default(), caps.clone())?;
        let candidate = Arc::new(Mutex::new(None));
        let p = rt.install(component.broker_plugin(path, &broker, json!({}), {
            let candidate = candidate.clone();
            move |registration| {
                *candidate.lock().unwrap() = Some(registration);
                Ok(())
            }
        })?)?;
        p.ready().await?;
        candidate.lock().unwrap().take().unwrap().promote()?;
        let version = handle
            .route(|s, ctx| {
                Box::pin(async move {
                    s.invoke_json(&ctx, "code-version", cordis_wasm::json!({}))
                        .await
                        .map_err(Into::into)
                })
            })
            .await?;
        println!("selected {version}, consumer activation={activation:?}");
        assert_eq!(client.snapshot().activation, activation);
        if let Some(previous) = old.replace(p) {
            previous.dispose().await?;
        }
    }
    rt.shutdown().await
}
