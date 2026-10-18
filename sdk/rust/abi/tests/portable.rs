use cordis_abi::{
    AbiService as PortableService, Capabilities, Dependency, EventBinding, FactoryBinding, Library,
    MiddlewareBinding,
};
use cordis_runtime::*;
use cordis_sdk_core::{Value, json};
use std::sync::{Arc, Mutex};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn portable_rust_cdylib_runs_reentrant_callbacks() {
    let rt = Runtime::new();
    let logs = Arc::new(Mutex::new(Vec::<Value>::new()));
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
    let caps = Capabilities {
        dependencies: vec![Dependency::new_json(
            &logger,
            cordis_sdk_core::Descriptor::new(
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
            |logger, _, _, value| {
                Box::pin(async move {
                    if value == json!({"reject":true}) {
                        return Err(cordis_abi::Error::Rejected {
                            code: "logger_rejected".into(),
                            message: "business error".into(),
                        });
                    }
                    logger.lock().unwrap().push(value.clone());
                    Ok(value)
                })
            },
        )],
        events: vec![EventBinding::new(
            "message",
            &Event::new("example.message", "1"),
        )],
        middleware: vec![MiddlewareBinding::new(
            "chain",
            &Middleware::new("example.chain", "1"),
        )],
        factories: vec![FactoryBinding::new("worker", |_| {
            Ok(Plugin::new("child", |_| async { Ok(()) }))
        })],
        ..Default::default()
    };
    let service = Service::<PortableService>::new("example.portable-rust", "1").with_descriptor(
        cordis_sdk_core::Descriptor::new(
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
    // SAFETY: compiled here from the repository's trusted Rust example.
    #[allow(unsafe_code)]
    let library = unsafe { Library::load(artifact()) }
        .unwrap()
        .with_context(caps)
        .unwrap();
    let provider = rt
        .install(library.plugin("portable", &service, json!({})).unwrap())
        .unwrap();
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
                "cordis-portable-abi",
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
        target.join("debug").join(format!(
            "{}cordis_portable_abi{}",
            std::env::consts::DLL_PREFIX,
            std::env::consts::DLL_SUFFIX
        ))
    })
}
