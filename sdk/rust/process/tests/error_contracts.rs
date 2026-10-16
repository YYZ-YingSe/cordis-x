use cordis_remote::{Dependency, Endpoint, Error, ProcessSpec, RemoteService, Value, json};
use cordis_runtime::{self as rt, Plugin, Runtime, Service};
use cordis_sdk_core::{Descriptor, Plugin as _};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};
#[allow(dead_code)]
#[path = "../../tests/support/structural_probe.rs"]
mod probe;
fn artifact() -> &'static Path {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    PATH.get_or_init(|| {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let target = root.join("target/rust-process-sdk");
        let output = std::process::Command::new(env!("CARGO"))
            .current_dir(&root)
            .args([
                "build",
                "-p",
                "cordis-sdk-process",
                "--example",
                "process_structural_probe",
                "--locked",
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
        target.join("debug/examples/process_structural_probe")
    })
}
async fn rig(endpoint: Endpoint) -> (Runtime, rt::PluginHandle, rt::ServiceHandle<RemoteService>) {
    let runtime = Runtime::new();
    let observer = Service::<()>::new("test.observer", "1");
    let binding = observer.clone();
    runtime
        .install(
            Plugin::new("observer", move |ctx| {
                let binding = binding.clone();
                async move { ctx.provide(&binding, Arc::new(())) }
            })
            .provides(&observer),
        )
        .unwrap()
        .ready()
        .await
        .unwrap();
    let dependency = Dependency::new_json(
        &observer,
        Descriptor::new(probe::dependency_interface()).unwrap(),
        |_, _, method, value| {
            Box::pin(async move {
                if method != "echo" {
                    return Ok(json!({}));
                }
                match value.as_str().unwrap().parse::<i64>().unwrap() {
                    -1 => Err(Error::Remote {
                        code: "expected".into(),
                        message: "declared".into(),
                    }),
                    -2 => Err(Error::Remote {
                        code: "closed".into(),
                        message: "undeclared business error".into(),
                    }),
                    -4 => Err(Error::OutcomeUnknown(
                        "expected: uncertain native execution".into(),
                    )),
                    -3 => Err(Error::Refused {
                        code: "closed".into(),
                        message: "runtime refusal".into(),
                    }),
                    _ => Ok(value),
                }
            })
        },
    );
    let mut spec = ProcessSpec::new(artifact()).requires(dependency);
    spec.endpoint = endpoint;

    let service = Service::<RemoteService>::new("test.structure", "1")
        .with_descriptor(Descriptor::new(probe::ContractProbe::interface()).unwrap());
    let provider = runtime.install(spec.plugin("probe", &service)).unwrap();
    provider.ready().await.unwrap();
    let saved = Arc::new(Mutex::new(None));
    let target = saved.clone();
    let binding = service.clone();
    runtime
        .install(
            Plugin::new("owner", move |ctx| {
                let binding = binding.clone();
                let target = target.clone();
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
async fn invoke_json(
    handle: &rt::ServiceHandle<RemoteService>,
    method: &'static str,
    mode: i64,
) -> cordis_remote::Result<Value> {
    handle
        .call(move |service, ctx| {
            Box::pin(async move { Ok(service.invoke_json(&ctx, method, json!(mode)).await) })
        })
        .await
        .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declared_business_errors_and_refusals_roundtrip_over_both_endpoints() {
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        let (runtime, _, handle) = rig(endpoint).await;
        let resource = handle
            .call(|s, ctx| {
                Box::pin(async move {
                    s.acquire_json(&ctx, "counter", json!(0))
                        .await
                        .map_err(Into::into)
                })
            })
            .await
            .unwrap();
        for mode in [-1, -3] {
            let factory = handle
                .call(move |s, ctx| {
                    Box::pin(async move {
                        Ok(s.acquire_json(&ctx, "counter", json!(mode))
                            .await
                            .map(|_| Value::Null))
                    })
                })
                .await
                .unwrap();
            for result in [
                invoke_json(&handle, "error", mode).await,
                invoke_json(&handle, "reverse", mode).await,
                resource.invoke_json("error", json!(mode)).await,
                factory,
            ] {
                if mode == -1 {
                    assert!(
                        matches!(result, Err(Error::Remote {ref code, ..}) if code == "expected"),
                        "{result:?}"
                    );
                } else {
                    assert!(
                        matches!(result, Err(Error::Refused {ref code, ..}) if code == "closed"),
                        "{result:?}"
                    );
                }
            }
        }
        assert_eq!(invoke_json(&handle, "echo", 42).await.unwrap(), json!("42"));
        resource.dispose().await.unwrap();
        runtime.shutdown().await.unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn undeclared_method_resource_factory_and_caught_reverse_errors_fail_the_session() {
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        for (action, mode) in [
            ("error", -2),
            ("factory", -2),
            ("resource", -2),
            ("reverse-suppressed", -2),
            ("error", -4),
            ("factory", -4),
            ("resource", -4),
            ("reverse-suppressed", -4),
        ] {
            let (runtime, provider, handle) = rig(endpoint).await;
            let result = match action {
                "factory" => handle
                    .call(move |s, ctx| {
                        Box::pin(async move {
                            Ok(s.acquire_json(&ctx, "counter", json!(mode))
                                .await
                                .map(|_| Value::Null))
                        })
                    })
                    .await
                    .unwrap(),
                "resource" => {
                    let resource = handle
                        .call(|s, ctx| {
                            Box::pin(async move {
                                s.acquire_json(&ctx, "counter", json!(0))
                                    .await
                                    .map_err(Into::into)
                            })
                        })
                        .await
                        .unwrap();
                    let result = resource.invoke_json("error", json!(mode)).await;
                    assert!(resource.dispose().await.is_err());
                    result
                }
                _ => invoke_json(&handle, action, mode).await,
            };
            assert!(
                matches!(
                    result,
                    Err(Error::Protocol(_)
                        | Error::HostLost(_)
                        | Error::Io(_)
                        | Error::OutcomeUnknown(_))
                ),
                "{action}: {result:?}"
            );
            assert!(provider.dispose().await.is_err());
            assert!(runtime.shutdown().await.is_err());
        }
    }
}
