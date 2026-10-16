use super::*;
use cordis_loader::{Configuration, Entry, Loader, Registry};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dynamic_intercepts_cross_both_sdks_endpoints_and_pin_resources() {
    for &backend in Backend::ALL {
        for endpoint in [Endpoint::Ipc, Endpoint::Http] {
            let rt = Runtime::new();
            let saved = Arc::new(Mutex::new(None));
            let mut spec = backend.spec("calculator");
            spec.endpoint = endpoint;

            let provider = rt
                .install(spec.plugin("calculator", &calculator()))
                .unwrap();
            bounded(provider.ready()).await.unwrap();
            let mut registry = Registry::new();
            registry
                .register_portable_intercept(&calculator(), |layers| {
                    Ok(layers.last().unwrap().clone())
                })
                .unwrap();
            registry
                .register("client", {
                    let saved = saved.clone();
                    move |_| {
                        let saved = saved.clone();
                        Ok(Plugin::new("client", move |ctx| {
                            let saved = saved.clone();
                            async move {
                                *saved.lock().unwrap() = Some(ctx.dependency(&calculator())?);
                                Ok(())
                            }
                        })
                        .requires(&calculator()))
                    }
                })
                .unwrap();
            let loader = Loader::new(rt.clone(), registry);
            let config = |timeout| Configuration {
                entries: vec![
                    Entry::new("client", "client", json!(null))
                        .intercept(&calculator(), json!({"timeout":timeout})),
                ],
            };
            loader.apply(config(1000)).await.unwrap();
            let before = loader.settled().await.unwrap();
            let handle = saved.lock().unwrap().clone().unwrap();
            let expected = json!({"revision":"1","layers":[{"timeout":1000.0}]});
            for (name, revision) in [("example.calculator", "2"), ("private.service", "1")] {
                let error = call(
                    &handle,
                    endpoint,
                    "metadata-for",
                    json!({"name":name,"revision":revision}),
                )
                .await
                .unwrap_err();
                assert!(error.to_string().contains("undeclared_metadata"));
            }
            assert_eq!(
                call(&handle, endpoint, "metadata", json!({}))
                    .await
                    .unwrap(),
                expected
            );
            let resource = handle
                .call(|remote, ctx| {
                    Box::pin(
                        async move { Ok(remote.acquire_json(&ctx, "counter", json!({})).await?) },
                    )
                })
                .await
                .unwrap();
            let report = loader.apply(config(3000)).await.unwrap();
            assert!(report.plan.retire.is_empty());
            assert_eq!(report.plan.update_intercepts, ["client"]);
            let after = loader.settled().await.unwrap();
            assert_eq!(
                before.entries[0].instance.as_ref().unwrap().activation,
                after.entries[0].instance.as_ref().unwrap().activation
            );
            assert_eq!(
                call(&handle, endpoint, "metadata", json!({}))
                    .await
                    .unwrap(),
                json!({"revision":"2","layers":[{"timeout":3000.0}]})
            );
            assert_eq!(
                resource.invoke_json("metadata", json!({})).await.unwrap(),
                expected
            );
            resource.dispose().await.unwrap();
            bounded(rt.shutdown()).await.unwrap();
        }
    }
}
