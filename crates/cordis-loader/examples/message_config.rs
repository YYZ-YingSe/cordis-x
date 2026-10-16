//! JSON configuration bound to a single protobuf message at source registration.
use cordis_loader::{Configuration, Loader, Registry};
use cordis_runtime::{Plugin, Runtime};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Runtime::new()?.block_on(async {
        let description: serde_json::Value =
            serde_json::from_str(cordis_example_contracts::resources::configuration::DESCRIPTOR)?;
        let schema = serde_json::from_value(description["Profile"]["config"].clone())?;
        let mut registry = Registry::new();
        registry.register_message("example.profile", schema, |config| {
            let config = config.clone();
            Ok(Plugin::new("profile", move |_| {
                let config = config.clone();
                async move {
                    println!("validated profile configuration: {config}");
                    Ok(())
                }
            }))
        })?;
        let runtime = Runtime::new();
        let loader = Loader::new(runtime.clone(), registry);
        loader
            .apply(Configuration::from_json(
                cordis_example_contracts::resources::PROFILE_APP,
            )?)
            .await?;
        loader.settled().await?;
        runtime.shutdown().await?;
        Ok(())
    })
}
