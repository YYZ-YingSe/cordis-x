use cordis_abi::{AbiService, Capabilities, FactoryBinding, Library, ProvisionBinding, json};
use cordis_sdk::*;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::args().nth(1).ok_or_else(|| {
        Error::Message("usage: abi_composition <trusted composition example library>".into())
    })?;
    let runtime = Runtime::new();
    let info = Service::<cordis_abi::Bytes>::new("example.info", "1");
    let cleaned = Arc::new(Mutex::new(Vec::new()));
    let factory = FactoryBinding::new("worker", {
        let cleaned = cleaned.clone();
        move |config| {
            cordis_portable_example::lifecycle::ApplicationControls::worker()
                .build(cordis_sdk_core::Message::from_bytes(<cordis_portable_example::lifecycle::data::lifecycle::WorkerConfig as cordis_sdk_core::typed::Codec>::contract().clone(),config).map_err(Error::Message)?, |config| {
                    let label = config.name.expect("validated required name");
                    let cleaned = cleaned.clone();
                    // Only build a definition here. Resources belong to its initializer.
                    Ok(Plugin::new(format!("worker: {label}"), move |ctx| {
                        let label = label.clone();
                        let cleaned = cleaned.clone();
                        async move {
                            println!("worker started: {label}");
                            ctx.defer("worker cleanup", move |_| async move {
                                println!("worker stopped: {label}");
                                cleaned.lock().unwrap().push(label);
                                Ok(())
                            })?;
                            Ok(())
                        }
                    }))
                })
                .map_err(Error::Portable)
        }
    });
    // SAFETY: build sdk/cpp/examples/abi/composition.cpp from this repository;
    // the binary and its native dependencies are trusted in-process code.
    #[allow(unsafe_code)]
    let library = unsafe { Library::load(path)? }.with_context(Capabilities {
        factories: vec![factory],
        provisions: vec![ProvisionBinding::new("info", &info)],
        ..Default::default()
    })?;
    let service = Service::<AbiService>::new("example.application", "1").with_descriptor(
        cordis_contract::Descriptor::new(
            cordis_portable_example::lifecycle::ApplicationClient::interface(),
        )
        .unwrap(),
    );
    let provider =
        runtime.install(library.plugin("application", &service, cordis_wire::json!({}))?)?;
    provider.ready().await?;
    let consumer = runtime.install(
        Plugin::new("consumer", {
            let service = service.clone();
            let info = info.clone();
            move |ctx| {
                let service = service.clone();
                let info = info.clone();
                async move {
                    ctx.dependency(&info)?
                        .call(|value, _| {
                            Box::pin(async move {
                                let value=cordis_sdk_core::Message::from_bytes(<cordis_portable_example::lifecycle::data::lifecycle::ApplicationInfo as cordis_sdk_core::typed::Codec>::contract().clone(),value.clone()).map_err(Error::Message)?.to_json().map_err(Error::Message)?;
                                assert_eq!(value["version"], "1");
                                println!("published info: {value}");
                                Ok(())
                            })
                        })
                        .await?;
                    ctx.dependency(&service)?
                        .call(|service, ctx| {
                            Box::pin(async move {
                                let status = service
                                    .invoke_json(&ctx, "start", json!({"name":"consumer"}))
                                    .await?;
                                assert_eq!(status["state"], "active");
                                Ok(())
                            })
                        })
                        .await
                }
            }
        })
        .requires(&service)
        .requires(&info),
    )?;
    consumer.ready().await?;
    consumer.dispose().await?;
    assert_eq!(*cleaned.lock().unwrap(), ["consumer"]);
    provider.dispose().await?;
    assert_eq!(*cleaned.lock().unwrap(), ["consumer", "application"]);
    runtime.shutdown().await
}
