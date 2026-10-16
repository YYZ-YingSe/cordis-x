#[path = "support/context.rs"]
mod context_guest;
mod support;
use cordis_runtime::Service;
use cordis_wasm::{Capabilities, Compiler, Dependency, Limits};

#[cfg(feature = "loader")]
#[tokio::test]
async fn loader_preview_reuses_compilation_but_never_authorizes_changed_files() {
    use cordis_loader::{ArtifactRef, Configuration, Entry, Loader, Registry};
    let path =
        std::env::temp_dir().join(format!("cordis-cached-preview-{}.wasm", std::process::id()));
    let bytes = support::bytes();
    std::fs::write(&path, bytes).unwrap();
    let reference = ArtifactRef::from_bytes(path.to_str().unwrap(), bytes);
    let compiler = Compiler::default();
    let service = Service::<cordis_wasm::WasmService>::new("example.calculator", "1")
        .with_descriptor(
            cordis_contract::portable::decode(
                &cordis_test_contracts::resources::rust_fixture::SERVICE
                    .parse()
                    .unwrap(),
            )
            .unwrap(),
        );
    let mut registry = Registry::new();
    cordis_wasm::loader::register_component(
        &mut registry,
        "code",
        service,
        compiler.clone(),
        Limits::default(),
    )
    .unwrap();
    let rt = cordis_runtime::Runtime::new();
    let loader = Loader::new(rt.clone(), registry);
    let config = Configuration {
        entries: vec![Entry::new("a", "code", cordis_wasm::json!({})).artifact(reference)],
    };
    loader.plan(config.clone()).await.unwrap();
    assert_eq!(compiler.stats().compilations, 1);
    std::fs::write(&path, b"changed").unwrap();
    assert!(loader.apply(config.clone()).await.is_err());
    assert!(rt.snapshots().is_empty());
    std::fs::write(&path, bytes).unwrap();
    loader.apply(config.clone()).await.unwrap();
    loader.settled().await.unwrap();
    assert_eq!(compiler.stats().compilations, 1);
    assert_eq!(compiler.stats().hits, 1);
    assert_eq!(compiler.stats().preflights, 2);
    let unchanged = loader.apply(config).await.unwrap();
    assert_eq!(unchanged.preparation.definitions_built, 0);
    assert_eq!(compiler.stats().preflights, 2);
    assert_eq!(compiler.stats().hits, 1);
    rt.shutdown().await.unwrap();
    std::fs::remove_file(path).unwrap();
}

async fn install(
    rt: &cordis_runtime::Runtime,
    component: &cordis_wasm::Component,
) -> cordis_runtime::ServiceHandle<cordis_wasm::WasmService> {
    use cordis_runtime::Plugin;
    use std::sync::{Arc, Mutex};
    let service =
        Service::new("example.calculator", "1").with_descriptor(component.descriptor().clone());
    let provider = rt
        .install(
            component
                .plugin("wasm", &service, cordis_wasm::json!({}))
                .unwrap(),
        )
        .unwrap();
    let saved = Arc::new(Mutex::new(None));
    let client = rt
        .install(
            Plugin::new("client", {
                let saved = saved.clone();
                let service = service.clone();
                move |ctx| {
                    let saved = saved.clone();
                    let service = service.clone();
                    async move {
                        *saved.lock().unwrap() = Some(ctx.dependency(&service)?);
                        Ok(())
                    }
                }
            })
            .requires(&service),
        )
        .unwrap();
    provider.ready().await.unwrap();
    client.ready().await.unwrap();
    saved.lock().unwrap().take().unwrap()
}

#[tokio::test]
async fn cache_eviction_and_one_runtime_shutdown_do_not_revoke_another_instances_resources() {
    use cordis_wasm::json;
    let compiler = Compiler::default();
    let first = compiler
        .component(support::bytes(), Limits::default())
        .unwrap();
    let second = compiler
        .component(support::bytes(), Limits::default())
        .unwrap();
    let a = cordis_runtime::Runtime::new();
    let b = cordis_runtime::Runtime::new();
    let a_handle = install(&a, &first).await;
    let b_handle = install(&b, &second).await;
    let counter_a = a_handle
        .call(|s, ctx| {
            Box::pin(async move {
                s.acquire_json(&ctx, "counter", json!({"initial":10}))
                    .await
                    .map_err(Into::into)
            })
        })
        .await
        .unwrap();
    let counter_b = b_handle
        .call(|s, ctx| {
            Box::pin(async move {
                s.acquire_json(&ctx, "counter", json!({"initial":20}))
                    .await
                    .map_err(Into::into)
            })
        })
        .await
        .unwrap();
    compiler.clear();
    drop(first);
    drop(second);
    a.shutdown().await.unwrap();
    assert!(counter_a.invoke_json("get", json!({})).await.is_err());
    assert_eq!(
        counter_b.invoke_json("add", json!(2)).await.unwrap(),
        json!("22")
    );
    assert_eq!(compiler.stats().compilations, 1);
    b.shutdown().await.unwrap();
}

#[test]
fn cached_code_still_checks_each_memory_budget_and_preflights_each_definition() {
    let compiler = Compiler::default();
    let bytes = support::bytes();
    let first = compiler.component(bytes, Limits::default()).unwrap();
    assert!(
        compiler
            .component(
                bytes,
                Limits {
                    memory_bytes: 1,
                    ..Limits::default()
                }
            )
            .is_err()
    );
    assert!(
        compiler
            .component(
                bytes,
                Limits {
                    fuel_per_call: 1,
                    ..Limits::default()
                }
            )
            .is_err()
    );
    let second = compiler.component(bytes, Limits::default()).unwrap();
    assert_eq!(first.contract(), second.contract());
    assert_eq!(compiler.stats().compilations, 1);
    assert_eq!(compiler.stats().hits, 3);
    assert_eq!(compiler.stats().preflights, 4);
    let hits = compiler.stats().hits;
    assert!(
        compiler
            .component(
                bytes,
                Limits {
                    artifact_bytes: 1,
                    ..Limits::default()
                }
            )
            .is_err()
    );
    assert_eq!(compiler.stats().hits, hits);
}

#[test]
fn cached_context_code_never_caches_import_or_dependency_authority() {
    let compiler = Compiler::default();
    let bytes = context_guest::bytes();
    let logger = Service::<()>::new("example.context-log", "1");
    let permitted = Capabilities {
        wasi: Some(Default::default()),
        dependencies: vec![Dependency::new_json(
            &logger,
            cordis_contract::Descriptor::new(
                cordis_portable_example::context::ContextLogClient::interface(),
            )
            .unwrap(),
            |_, _, _, args| Box::pin(async move { Ok(args) }),
        )],
        ..Default::default()
    };
    // A failure to bind imports is not a negative cache entry or permission grant.
    assert!(
        compiler
            .context_component(bytes, Limits::default(), Capabilities::default())
            .is_err()
    );
    let module = compiler
        .context_component(bytes, Limits::default(), permitted.clone())
        .unwrap();
    let mut missing_dependency = permitted.clone();
    missing_dependency.dependencies.clear();
    assert!(
        compiler
            .context_component(bytes, Limits::default(), missing_dependency)
            .is_err()
    );
    let mut missing_wasi = permitted;
    missing_wasi.wasi = None;
    assert!(
        compiler
            .context_component(bytes, Limits::default(), missing_wasi)
            .is_err()
    );
    assert_eq!(compiler.stats().compilations, 1);
    assert_eq!(compiler.stats().hits, 3);
    assert_eq!(compiler.stats().preflights, 4);
    compiler.clear();
    assert_eq!(compiler.stats().resident_entries, 0);
    assert_eq!(module.contract().name.as_ref(), "example.context");
}
