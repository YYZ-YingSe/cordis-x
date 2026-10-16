use super::*;
use cordis_loader::{Configuration, Entry, Loader, Registry};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declarative_loader_reconfigures_one_binary_across_endpoints() {
    let executable = binary();
    let trace = executable.parent().unwrap().join(format!(
        "loader-{}-{}.log",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_file(&trace);
    let rt = Runtime::new();
    let saved: Arc<Mutex<Vec<ServiceHandle<RemoteService>>>> = Arc::default();
    let mut registry = Registry::new();
    registry
        .register("calculator-process", {
            let trace = trace.clone();
            move |config| {
                let mut spec = ProcessSpec::new(&executable);
                spec.timeout = Duration::from_secs(5);
                spec.args = vec![
                    "--test-hooks".into(),
                    "--trace".into(),
                    trace.display().to_string(),
                ];
                spec.endpoint = match config["endpoint"].as_str() {
                    Some("ipc") => Endpoint::Ipc,
                    Some("http") => Endpoint::Http,
                    _ => return Err(Error::Message("invalid endpoint".into())),
                };
                Ok(spec.plugin("calculator", &test_calculator()))
            }
        })
        .unwrap();
    registry
        .register("native-consumer", {
            let saved = saved.clone();
            move |_| {
                let saved = saved.clone();
                Ok(Plugin::new("native consumer", move |ctx| {
                    let saved = saved.clone();
                    async move {
                        saved
                            .lock()
                            .unwrap()
                            .push(ctx.dependency(&test_calculator())?);
                        ctx.defer("consumer", |ctx| async move {
                            ctx.dependency(&test_calculator())?
                                .call(|remote, ctx| {
                                    Box::pin(async move {
                                        assert_eq!(
                                            remote
                                                .invoke_json(
                                                    &ctx,
                                                    "add",
                                                    json!({"left":20,"right":22})
                                                )
                                                .await?,
                                            json!("42")
                                        );
                                        Ok(())
                                    })
                                })
                                .await
                        })?;
                        Ok(())
                    }
                })
                .requires(&test_calculator()))
            }
        })
        .unwrap();
    let loader = Loader::new(rt.clone(), registry);
    let target = |endpoint| Configuration {
        entries: vec![
            Entry::new("consumer", "native-consumer", json!(null)),
            Entry::new(
                "calculator",
                "calculator-process",
                json!({"endpoint":endpoint}),
            ),
        ],
    };
    let mut previous = None;
    for endpoint in ["ipc", "http", "ipc", "http"] {
        bounded(loader.apply(target(endpoint))).await.unwrap();
        let state = bounded(loader.settled()).await.unwrap();
        let consumer = state
            .entries
            .iter()
            .find(|e| e.id == "consumer")
            .unwrap()
            .instance
            .as_ref()
            .unwrap();
        if let Some(old) = previous {
            assert_eq!(consumer.id, old);
        }
        previous = Some(consumer.id);
        let handle = saved.lock().unwrap().last().unwrap().clone();
        let result = handle
            .call(|remote, ctx| {
                Box::pin(async move {
                    remote
                        .invoke_json(&ctx, "add", json!({"left":20,"right":22}))
                        .await
                        .map_err(Into::into)
                })
            })
            .await
            .unwrap();
        assert_eq!(result, json!("42"));
        let stable = consumer.activation;
        let same = bounded(loader.apply(target(endpoint))).await.unwrap();
        assert!(same.plan.retire.is_empty());
        assert!(same.plan.install.is_empty());
        assert_eq!(
            loader
                .settled()
                .await
                .unwrap()
                .entries
                .iter()
                .find(|e| e.id == "consumer")
                .unwrap()
                .instance
                .as_ref()
                .unwrap()
                .activation,
            stable
        );
    }
    let old = saved.lock().unwrap()[0].clone();
    assert!(matches!(
        call(&old, Endpoint::Ipc, "echo", json!(null)).await,
        Err(Error::Closed)
    ));
    bounded(loader.apply(Configuration::default()))
        .await
        .unwrap();
    bounded(rt.shutdown()).await.unwrap();
    assert_eq!(
        std::fs::read_to_string(trace).unwrap(),
        "activated\ndeactivated\n".repeat(4)
    );
}
