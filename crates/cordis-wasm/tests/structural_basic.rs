use cordis_runtime::{self as rt, Plugin, Runtime, Service};
use cordis_wasm::{Component, Error, Limits, Value, WasmService, json};
use std::sync::{Arc, Mutex, OnceLock};
fn component() -> Component {
    static COMPONENT: OnceLock<Component> = OnceLock::new();
    COMPONENT
        .get_or_init(|| {
            use std::{path::Path, process::Command};
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
            let target = root.join("target/wasm-structural-guest");
            let mut command = Command::new(env!("CARGO"));
            command
                .current_dir(&root)
                .args([
                    "build",
                    "-p",
                    "cordis-wasm-calculator",
                    "--target",
                    "wasm32-unknown-unknown",
                    "--release",
                    "--features",
                    "structural-fixture",
                    "--locked",
                    "--offline",
                    "--target-dir",
                ])
                .arg(&target);
            if let Some(rustc) = std::env::var_os("CORDIS_WASM_RUSTC") {
                command.env("RUSTC", rustc);
            }
            command.env_remove("CARGO_ENCODED_RUSTFLAGS");
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let core = std::fs::read(
                target.join("wasm32-unknown-unknown/release/cordis_wasm_calculator.wasm"),
            )
            .unwrap();
            let bytes = wit_component::ComponentEncoder::default()
                .module(&core)
                .unwrap()
                .validate(true)
                .encode()
                .unwrap();
            Component::from_bytes(&bytes, Limits::default()).unwrap()
        })
        .clone()
}
async fn rig() -> (Runtime, rt::PluginHandle, rt::ServiceHandle<WasmService>) {
    let runtime = Runtime::new();
    let component = component();
    let service =
        Service::new("example.calculator", "1").with_descriptor(component.descriptor().clone());
    let provider = runtime
        .install(component.plugin("calculator", &service, json!({})).unwrap())
        .unwrap();
    provider.ready().await.unwrap();
    let saved = Arc::new(Mutex::new(None));
    let target = saved.clone();
    let binding = service.clone();
    runtime
        .install(
            Plugin::new("owner", move |ctx| {
                let target = target.clone();
                let binding = binding.clone();
                async move {
                    *target.lock().unwrap() = Some(ctx.dependency(&binding)?);
                    Ok(())
                }
            })
            .requires(&service),
        )
        .unwrap()
        .ready()
        .await
        .unwrap();
    let handle = saved.lock().unwrap().take().unwrap();
    (runtime, provider, handle)
}
#[test]
fn basic_component_requires_exact_structure_before_create() {
    let component = component();
    let service = Service::new("example.calculator", "1");
    assert!(matches!(
        component.plugin("missing", &service, json!({})),
        Err(Error::Contract(_))
    ));
    let wrong = service.with_descriptor(
        cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true)).unwrap(),
    );
    assert!(matches!(
        component.plugin("drift", &wrong, json!({})),
        Err(Error::Contract(_))
    ));
}
#[tokio::test]
async fn basic_component_checks_method_factory_and_resource_arguments_before_business() {
    let (runtime, _, handle) = rig().await;
    handle
        .call(|s, ctx| {
            Box::pin(async move {
                assert!(matches!(
                    s.invoke_json(&ctx, "echo", json!("bad")).await,
                    Err(Error::InvalidInput(_))
                ));
                assert_eq!(
                    s.invoke_json(&ctx, "echo", json!(42)).await.unwrap(),
                    json!("42")
                );
                assert!(matches!(
                    s.acquire_json(&ctx, "counter", json!({"initial":"bad"}))
                        .await,
                    Err(Error::InvalidInput(_))
                ));
                assert!(matches!(
                    s.acquire_json(&ctx, "missing", json!({})).await,
                    Err(Error::InvalidInput(_))
                ));
                let r = s
                    .acquire_json(&ctx, "counter", json!({"initial":40}))
                    .await
                    .unwrap();
                assert!(matches!(
                    r.invoke_json("add", json!("bad")).await,
                    Err(Error::InvalidInput(_))
                ));
                assert!(matches!(
                    r.invoke_json("missing", Value::Null).await,
                    Err(Error::InvalidInput(_))
                ));
                assert_eq!(r.invoke_json("get", json!({})).await.unwrap(), json!("40"));
                assert_eq!(r.invoke_json("add", json!(2)).await.unwrap(), json!("42"));
                Ok(())
            })
        })
        .await
        .unwrap();
    runtime.shutdown().await.unwrap();
}
#[tokio::test]
async fn basic_component_treats_wrong_declared_result_as_unknown_not_recovered() {
    let (runtime, provider, handle) = rig().await;
    let outcome = handle
        .call(|s, ctx| Box::pin(async move { Ok(s.invoke_json(&ctx, "stats", json!({})).await) }))
        .await
        .unwrap();
    assert!(matches!(outcome, Err(Error::Unknown(_))));
    assert!(provider.dispose().await.is_err());
    assert!(matches!(
        provider.effects()[0].status,
        rt::EffectStatus::Failed(_)
    ));
    assert!(runtime.shutdown().await.is_err());
}

#[tokio::test]
async fn basic_wit_keeps_business_rejection_and_runtime_refusal_distinct() {
    let (runtime, _, handle) = rig().await;
    handle.call(|s, ctx| Box::pin(async move {
        let resource = s.acquire_json(&ctx, "counter", json!({})).await.unwrap();
        for mode in [-1, -3] {
            for result in [
                s.invoke_json(&ctx, "error", json!(mode)).await,
                resource.invoke_json("error", json!(mode)).await,
                s.acquire_json(&ctx, "counter", json!({"initial": mode})).await.map(|_| Value::Null),
            ] {
                if mode == -1 { assert!(matches!(result, Err(Error::Rejected {ref code, ..}) if code == "expected"), "{result:?}"); }
                else { assert!(matches!(result, Err(Error::Refused {ref code, ..}) if code == "closed"), "{result:?}"); }
            }
        }
        assert_eq!(s.invoke_json(&ctx, "echo", json!(42)).await.unwrap(), json!("42"));
        resource.dispose().await.unwrap();
        Ok(())
    })).await.unwrap();
    runtime.shutdown().await.unwrap();
}
#[tokio::test]
async fn basic_wit_preserves_unknown_responsibility_for_undeclared_or_uncertain_errors() {
    for mode in [-2, -4] {
        for action in ["method", "factory", "resource"] {
            let (runtime, provider, handle) = rig().await;
            let result = handle
                .call(move |s, ctx| {
                    Box::pin(async move {
                        Ok(match action {
                            "method" => s.invoke_json(&ctx, "error", json!(mode)).await,
                            "factory" => s
                                .acquire_json(&ctx, "counter", json!({"initial":mode}))
                                .await
                                .map(|_| Value::Null),
                            _ => {
                                let r = s.acquire_json(&ctx, "counter", json!({})).await.unwrap();
                                let result = r.invoke_json("error", json!(mode)).await;
                                assert!(r.dispose().await.is_err());
                                result
                            }
                        })
                    })
                })
                .await
                .unwrap();
            assert!(
                matches!(result, Err(Error::Unknown(_))),
                "{action}: {result:?}"
            );
            assert!(provider.dispose().await.is_err());
            assert!(runtime.shutdown().await.is_err());
        }
    }
}
