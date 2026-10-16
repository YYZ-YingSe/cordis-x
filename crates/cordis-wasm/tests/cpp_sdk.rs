#![cfg(unix)]
use cordis_runtime::*;
use cordis_wasm::{
    Capabilities, ContextComponent, ContextService as PortableService, Dependency, Limits, Wasi,
};
use cordis_wasm::{Value, json};
use std::sync::{Arc, Mutex};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_portable_component_runs_reentrant_callbacks() {
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
            cordis_contract::Descriptor::new(
                cordis_contract::portable::decode(
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
                    logger.lock().unwrap().push(value.clone());
                    Ok(value)
                })
            },
        )],
        events: [("message".into(), Event::new("example.message", "1"))].into(),
        middleware: [("chain".into(), Middleware::new("example.chain", "1"))].into(),
        factories: [(
            "worker".into(),
            Arc::new(|_| Ok(Plugin::new("child", |_| async { Ok(()) })))
                as Arc<dyn Fn(cordis_wasm::Bytes) -> cordis_wasm::Result<Plugin> + Send + Sync>,
        )]
        .into(),
        wasi: Some(Wasi::default()),
        ..Default::default()
    };
    let service = Service::<PortableService>::new("example.portable", "1").with_descriptor(
        cordis_contract::Descriptor::new(
            cordis_contract::portable::decode(
                &cordis_example_contracts::resources::portable::PORTABLECPP
                    .parse::<Value>()
                    .unwrap(),
            )
            .unwrap()
            .interface()
            .clone(),
        )
        .unwrap(),
    );
    let module = ContextComponent::load(
        artifact(),
        Limits {
            fuel_per_call: 100_000_000,
            ..Limits::default()
        },
        caps,
    )
    .unwrap();
    let provider = rt
        .install(module.plugin("portable", &service, json!({})).unwrap())
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
    handle
        .call(|service, ctx| {
            Box::pin(async move {
                let counter = service
                    .acquire_json(&ctx, "counter", json!({"initial":40}))
                    .await?;
                assert_eq!(counter.invoke_json("add", json!(2)).await?, json!("42"));
                counter.dispose().await?;
                counter.dispose().await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    for (method, args, expected) in [
        ("count", json!({}), json!("1")),
        ("count", json!({}), json!("2")),
        ("save-context", json!({}), json!({})),
        ("expired", json!({}), json!(true)),
        (
            "echo",
            json!({"text":"中文","floats":[1.5,-1e100]}),
            json!({"text":"中文","floats":[1.5,-1e100]}),
        ),
        ("scope", json!({}), json!("42")),
        ("sequence", json!({}), json!({})),
        ("event", json!({}), json!({"values":[{"stopped":"42"}]})),
        ("middleware", json!({}), json!({"wrapped":"42"})),
        ("child", json!({}), json!({})),
        ("task", json!({}), json!({})),
        ("defer", json!({}), json!({})),
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
        .unwrap_or_else(|e| panic!("{method}: {e:?}"));
        assert_eq!(actual, expected, "{method}");
    }

    assert!(
        handle
            .call(|service, ctx| Box::pin(async move {
                service
                    .invoke_json(&ctx, "missing", json!({}))
                    .await
                    .map_err(Into::into)
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
                    .map_err(Into::into)
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
    assert!(logs.contains(&json!("portable cleanup")));
}
fn artifact() -> &'static std::path::Path {
    static BUILD: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
    BUILD.get_or_init(|| {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let output = root.join("target/cpp-component/portable.wasm");
        let build = std::process::Command::new(
            std::env::var_os("PYTHON")
                .unwrap_or_else(|| if cfg!(windows) { "python" } else { "python3" }.into()),
        )
        .current_dir(&root)
        .args([
            "sdk/cpp/tools/build-component.py",
            "--sdk-source",
            "sdk/cpp",
            "--include",
            "examples/generated/cpp",
            "--proto-path",
            "examples/resources/contracts",
            "--core",
            "--proto",
            "portable.proto",
            "sdk/cpp/examples/portable/component.cpp",
        ])
        .arg(&output)
        .output()
        .unwrap();
        assert!(
            build.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&build.stdout),
            String::from_utf8_lossy(&build.stderr)
        );
        let core = std::fs::read(&output).unwrap();
        let component = wit_component::ComponentEncoder::default()
            .module(&core)
            .unwrap()
            .adapter(
                "wasi_snapshot_preview1",
                wasi_preview1_component_adapter_provider::WASI_SNAPSHOT_PREVIEW1_REACTOR_ADAPTER,
            )
            .unwrap()
            .validate(true)
            .encode()
            .unwrap();
        std::fs::write(&output, component).unwrap();
        output
    })
}
