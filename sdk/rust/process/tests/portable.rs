use cordis_remote::{
    Dependency, Endpoint, EventBinding, FactoryBinding, MiddlewareBinding, ProcessSpec,
    RemoteService as PortableService,
};
use cordis_runtime::*;
use cordis_sdk_core::{Value, json};
use std::sync::{Arc, Mutex};

async fn scenario(endpoint: Endpoint) {
    let rt = Runtime::new();
    let logs = Arc::new(Mutex::new(Vec::<Value>::new()));
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let resume = Arc::new(tokio::sync::Semaphore::new(0));
    let dependency_entered = entered.clone();
    let dependency_resume = resume.clone();
    let logger = Service::<Mutex<Vec<Value>>>::new("example.portable-log", "1");
    let binding = logger.clone();
    let records = logs.clone();
    rt.install(
        Plugin::new("logger", move |ctx| {
            let binding = binding.clone();
            let records = records.clone();
            async move { ctx.provide(&binding, records) }
        })
        .provides(&logger),
    )
    .unwrap()
    .ready()
    .await
    .unwrap();
    let mut spec = ProcessSpec::new(artifact())
        .requires(Dependency::new_json(
            &logger,
            cordis_remote::Descriptor::new(
                cordis_sdk_core::contract::decode(
                    &cordis_example_contracts::resources::portable::PORTABLELOG
                        .parse::<Value>()
                        .unwrap(),
                )
                .unwrap()
                .interface()
                .clone(),
            )
            .unwrap(),
            move |logger, _, _, value| {
                let entered = dependency_entered.clone();
                let resume = dependency_resume.clone();
                Box::pin(async move {
                    if value == json!({"reject":true}) {
                        return Err(cordis_remote::Error::Remote {
                            code: "logger_rejected".into(),
                            message: "business error".into(),
                        });
                    }
                    if value["blocked"] == true {
                        entered.add_permits(1);
                        resume.acquire().await.unwrap().forget();
                    }
                    logger.lock().unwrap().push(value.clone());
                    Ok(value)
                })
            },
        ))
        .event(EventBinding::new(
            "message",
            &Event::new("example.message", "1"),
        ))
        .middleware(MiddlewareBinding::new(
            "chain",
            &Middleware::new("example.chain", "1"),
        ))
        .factory(FactoryBinding::new("worker", |_| {
            Ok(Plugin::new("child", |_| async { Ok(()) }))
        }));
    spec.endpoint = endpoint;
    if matches!(endpoint, Endpoint::Ipc) {
        spec.args.push("--stdio-only".into());
    }
    let service = Service::<PortableService>::new("example.portable-rust", "1").with_descriptor(
        cordis_remote::Descriptor::new(
            cordis_sdk_core::contract::decode(
                &cordis_example_contracts::resources::portable::PORTABLERUST
                    .parse::<Value>()
                    .unwrap(),
            )
            .unwrap()
            .interface()
            .clone(),
        )
        .unwrap(),
    );
    let provider = rt.install(spec.plugin("portable", &service)).unwrap();
    provider.ready().await.unwrap();
    let saved = Arc::new(Mutex::new(None));
    let target = saved.clone();
    let binding = service.clone();
    let consumer = rt
        .install(
            Plugin::new("consumer", move |ctx| {
                let target = target.clone();
                let binding = binding.clone();
                async move {
                    *target.lock().unwrap() = Some(ctx.dependency(&binding)?);
                    Ok(())
                }
            })
            .requires(&service),
        )
        .unwrap();
    consumer.ready().await.unwrap();
    let handle = saved.lock().unwrap().take().unwrap();
    for (method, args, expected) in [
        ("resource-support", json!({}), json!("unsupported_resource")),
        ("echo", json!({"text":"中文"}), json!({"text":"中文"})),
        ("count", json!({}), json!("1")),
        ("count", json!({}), json!("2")),
        ("save-context", json!({}), json!({})),
        ("expired", json!({}), json!(true)),
        ("defer", json!("manual"), json!({})),
        ("release", json!({}), json!({})),
        ("scope", json!({}), json!("42")),
        ("sequence", json!({}), json!({})),
        ("event", json!({}), json!({"values":[{"stopped":"42"}]})),
        ("middleware", json!({}), json!({"wrapped":"42"})),
        ("child", json!({}), json!({})),
        ("task", json!({}), json!({})),
        ("defer", json!("automatic"), json!({})),
    ] {
        let actual = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            handle.call(move |service, ctx| {
                Box::pin(async move {
                    service
                        .invoke_json(&ctx, method, args)
                        .await
                        .map_err(|e| Error::Message(e.to_string()))
                })
            }),
        )
        .await
        .expect(method)
        .unwrap();
        assert_eq!(actual, expected, "{method}");
    }
    assert!(
        handle
            .call(|service, ctx| Box::pin(async move {
                service
                    .invoke_json(&ctx, "echo", json!({"reject":true}))
                    .await
                    .map_err(|e| Error::Message(e.to_string()))
            }))
            .await
            .is_err()
    );
    assert_eq!(
        handle
            .call(|service, ctx| Box::pin(async move {
                service
                    .invoke_json(&ctx, "count", json!({}))
                    .await
                    .map_err(|e| Error::Message(e.to_string()))
            }))
            .await
            .unwrap(),
        json!("3")
    );
    // Exercise the real stdio buffers across their capacity in both directions,
    // including reverse calls; the same scenario also covers HTTP management.
    let large = json!({"text":"中文".repeat(32 * 1024)});
    let expected = large.clone();
    assert_eq!(
        handle
            .call(move |service, ctx| Box::pin(async move {
                Ok(service.invoke_json(&ctx, "echo", large).await?)
            }))
            .await
            .unwrap(),
        expected,
    );
    let mut blocked = tokio::task::JoinSet::new();
    for slot in 0..8 {
        let handle = handle.clone();
        blocked.spawn(async move {
            let value = json!({"blocked":true,"slot":slot.to_string()});
            let expected = value.clone();
            let result = handle
                .call(move |service, ctx| {
                    Box::pin(async move { Ok(service.invoke_json(&ctx, "echo", value).await?) })
                })
                .await
                .unwrap();
            assert_eq!(result, expected);
        });
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        entered.acquire_many(8).await.unwrap().forget();
        // Every reverse call remains suspended until after this foreground call.
        let count = handle
            .call(|service, ctx| {
                Box::pin(async move { Ok(service.invoke_json(&ctx, "count", json!({})).await?) })
            })
            .await
            .unwrap();
        assert_eq!(count, json!("4"));
        resume.add_permits(8);
        while let Some(result) = blocked.join_next().await {
            result.unwrap();
        }
    })
    .await
    .expect("suspended dependencies must not block foreground calls");
    consumer.dispose().await.unwrap();
    assert!(
        handle
            .call(|_, _| Box::pin(async { Ok(()) }))
            .await
            .is_err()
    );
    rt.shutdown().await.unwrap();
    let logs = logs.lock().unwrap();
    assert!(logs.contains(&json!("scope-cleanup")));
    assert!(logs.contains(&json!("final-inverse")));
    assert!(logs.contains(&json!("task-stopped")));
    assert_eq!(
        logs.iter()
            .filter(|v| **v == json!({"released":"manual"}))
            .count(),
        1
    );
    assert_eq!(
        logs.iter()
            .filter(|v| **v == json!({"released":"automatic"}))
            .count(),
        1
    );
    assert_eq!(logs.last(), Some(&json!("deactivate")));
}

fn artifact() -> &'static std::path::Path {
    static BUILD: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
    BUILD.get_or_init(|| {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let target = cordis_test_contracts::native::cargo_target();
        let output = std::process::Command::new("cargo")
            .current_dir(&root)
            .args([
                "build",
                "-p",
                "cordis-sdk-process",
                "--example",
                "portable",
                "--offline",
                "--target-dir",
            ])
            .arg(&target)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        target
            .join("debug/examples")
            .join(format!("portable{}", std::env::consts::EXE_SUFFIX))
    })
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_rust_plugin_runs_through_ipc_and_http_protobuf() {
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        eprintln!("testing {endpoint:?}");
        tokio::time::timeout(std::time::Duration::from_secs(60), scenario(endpoint))
            .await
            .unwrap();
    }
}
