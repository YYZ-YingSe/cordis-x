use cordis_remote::{Endpoint, ProcessSpec, RemoteService, json};
use cordis_sdk::*;

#[tokio::main]
async fn main() -> Result<()> {
    let runtime = Runtime::new();
    let adder = Service::<RemoteService>::new("example.adder", "1").with_descriptor(
        cordis_contract::portable::decode(
            &serde_json::from_str(cordis_example_contracts::resources::examples::ADDER).unwrap(),
        )
        .unwrap(),
    );

    // Cordis starts the executable and manages its lifetime.
    let mut spec = ProcessSpec::new("target/cpp/cordis_adder");
    spec.endpoint = match std::env::args().nth(1).as_deref() {
        None | Some("ipc") => Endpoint::Ipc,
        Some("http") => Endpoint::Http,
        _ => return Err(Error::Message("expected ipc or http".into())),
    };
    let provider = runtime.install(spec.plugin("adder-process", &adder))?;
    provider.ready().await?;

    // This plugin consumes the service declared by the C++ program.
    let dependency = adder.clone();
    let client = runtime.install(
        Plugin::new("client", move |ctx| {
            let adder = adder.clone();
            async move {
                let result = ctx
                    .dependency(&adder)?
                    .call(|remote, ctx| {
                        Box::pin(async move {
                            remote
                                .invoke_json(&ctx, "add", json!({"left": 20, "right": 22}))
                                .await
                                .map_err(Into::into)
                        })
                    })
                    .await?;
                println!("20 + 22 = {result}");
                Ok(())
            }
        })
        .requires(&dependency),
    )?;
    client.ready().await?;
    runtime.shutdown().await
}
