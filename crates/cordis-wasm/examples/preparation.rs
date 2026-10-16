//! Comparable before/after preparation measurements; wall times are not assertions.
use cordis_loader::{ArtifactRef, Configuration, Entry, Loader, Registry};
use cordis_runtime::{Plugin, Runtime, Service, ServiceHandle};
use cordis_wasm::{Limits, WasmService, json, loader::register_component};
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: preparation <calculator.component.wasm>")?;
    let limits = Limits::default();
    let mut report = json!({"profile":"dev (debug=1, incremental=false)", "samples":{}});
    let start = Instant::now();
    let artifact = ArtifactRef::from_file(&path, limits.artifact_bytes)?;
    report["read_hash_ms"] = json!(start.elapsed().as_secs_f64() * 1000.0);
    report["sha256"] = json!(artifact.sha256);
    report["artifact_bytes"] = json!(std::fs::metadata(&path)?.len());
    let runtime = Runtime::new();
    let service = Service::<WasmService>::new("example.calculator", "1").with_descriptor(
        cordis_contract::portable::decode(
            &cordis_example_contracts::resources::calculator_wasm::SERVICE.parse()?,
        )
        .map_err(std::io::Error::other)?,
    );
    let saved = Arc::new(Mutex::new(None));
    let mut registry = Registry::new();
    let compiler = cordis_wasm::Compiler::default();
    register_component(
        &mut registry,
        "calculator",
        service.clone(),
        compiler.clone(),
        limits,
    )?;
    registry.register("client", {
        let saved = saved.clone();
        move |_| {
            let service = service.clone();
            let saved = saved.clone();
            Ok(Plugin::new("client", {
                let service = service.clone();
                move |ctx| {
                    let service = service.clone();
                    let saved = saved.clone();
                    async move {
                        *saved.lock().unwrap() = Some(ctx.dependency(&service)?);
                        Ok(())
                    }
                }
            })
            .requires(&service))
        }
    })?;
    let loader = Loader::new(runtime.clone(), registry);
    let config = Configuration {
        entries: vec![
            Entry::new("calculator", "calculator", json!({})).artifact(artifact),
            Entry::new("client", "client", json!(null)),
        ],
    };
    let start = Instant::now();
    loader.plan(config.clone()).await?;
    report["cold_plan_ms"] = json!(start.elapsed().as_secs_f64() * 1000.0);
    let start = Instant::now();
    loader.apply(config.clone()).await?;
    report["apply_after_plan_ms"] = json!(start.elapsed().as_secs_f64() * 1000.0);
    let start = Instant::now();
    loader.settled().await?;
    report["settle_after_apply_ms"] = json!(start.elapsed().as_secs_f64() * 1000.0);
    let mut noops = Vec::new();
    for _ in 0..5 {
        let start = Instant::now();
        let result = loader.apply(config.clone()).await?;
        report["unchanged_preparation"] = json!({
            "definitions_built":result.preparation.definitions_built,
            "definitions_retained":result.preparation.definitions_retained,
            "resolve_ms":result.preparation.resolve.as_secs_f64()*1000.0,
            "verify_retained_ms":result.preparation.verify_retained.as_secs_f64()*1000.0,
            "build_ms":result.preparation.build.as_secs_f64()*1000.0,
            "validate_graph_ms":result.preparation.validate_graph.as_secs_f64()*1000.0,
        });
        noops.push(start.elapsed().as_secs_f64() * 1000.0);
        assert!(result.plan.install.is_empty() && result.plan.retire.is_empty());
    }
    report["samples"]["unchanged_apply_ms"] = json!(noops);
    let handle: ServiceHandle<WasmService> = saved.lock().unwrap().take().unwrap();
    let mut calls = Vec::new();
    for _ in 0..200 {
        let start = Instant::now();
        let value = handle
            .call(|s, ctx| {
                Box::pin(async move {
                    s.invoke_json(&ctx, "add", json!({"left":20, "right":22}))
                        .await
                        .map_err(Into::into)
                })
            })
            .await?;
        calls.push(start.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(value, json!("42"));
    }
    calls.sort_by(f64::total_cmp);
    let stats = compiler.stats();
    report["compiler"] = json!({
        "hits":stats.hits, "misses":stats.misses, "compilations":stats.compilations,
        "compile_ms":stats.compile_time.as_secs_f64() * 1000.0,
        "preflights":stats.preflights, "preflight_ms":stats.preflight_time.as_secs_f64() * 1000.0,
        "resident_entries":stats.resident_entries, "resident_image_bytes":stats.resident_image_bytes,
    });
    report["call_p50_ms"] = json!(calls[99]);
    report["call_p99_ms"] = json!(calls[197]);
    report["call_samples"] = json!(calls.len());
    let start = Instant::now();
    runtime.shutdown().await?;
    report["drain_shutdown_ms"] = json!(start.elapsed().as_secs_f64() * 1000.0);
    println!("{report:#}");
    Ok(())
}
