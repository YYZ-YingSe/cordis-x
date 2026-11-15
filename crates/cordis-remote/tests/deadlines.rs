use cordis_remote::{Endpoint, ProcessSpec, RemoteService, json};
use cordis_sdk::{Plugin, PluginHandle, Runtime, Service, ServiceHandle};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

fn spec() -> ProcessSpec {
    let mut spec = ProcessSpec::new("node");
    spec.args.push(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../sdk/typescript/test/fixtures/deadlines.ts")
            .display()
            .to_string(),
    );
    spec
}

async fn setup(spec: ProcessSpec) -> (Runtime, PluginHandle, ServiceHandle<RemoteService>) {
    let runtime = Runtime::new();
    let service = Service::new("test.deadlines", "1").with_descriptor(
        cordis_remote::Descriptor::new(cordis_remote::Interface::value_dispatch(false)).unwrap(),
    );
    let provider = runtime.install(spec.plugin("deadlines", &service)).unwrap();
    provider.ready().await.unwrap();
    let saved = Arc::new(Mutex::new(None));
    let capture = saved.clone();
    let declaration = service.clone();
    let consumer = runtime
        .install(
            Plugin::new("caller", move |context| {
                let saved = capture.clone();
                let service = service.clone();
                async move {
                    *saved.lock().unwrap() = Some(context.dependency(&service)?);
                    Ok(())
                }
            })
            .requires(&declaration),
        )
        .unwrap();
    consumer.ready().await.unwrap();
    let handle = saved.lock().unwrap().take().unwrap();
    (runtime, provider, handle)
}

async fn call(
    handle: &ServiceHandle<RemoteService>,
    endpoint: Endpoint,
    method: &str,
) -> cordis_sdk::Result<cordis_remote::Value> {
    let method = method.to_owned();
    handle
        .call(move |remote, context| {
            Box::pin(async move {
                remote
                    .invoke_json_via(&context, endpoint, &method, json!(null))
                    .await
                    .map_err(Into::into)
            })
        })
        .await
}

async fn entered(handle: &ServiceHandle<RemoteService>) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while call(handle, Endpoint::Ipc, "status").await.unwrap() != json!(true) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_calls_have_no_default_deadline_on_either_endpoint() {
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        let spec = spec();
        assert_eq!(spec.execution_timeout, None);
        let (runtime, _, handle) = setup(spec).await;
        let waiting = tokio::spawn({
            let handle = handle.clone();
            async move { call(&handle, endpoint, "hold").await }
        });
        entered(&handle).await;
        tokio::time::sleep(Duration::from_secs(11)).await;
        assert!(!waiting.is_finished());
        call(&handle, Endpoint::Ipc, "release").await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_execution_deadlines_preserve_unknown_outcomes() {
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        let mut spec = spec();
        spec.execution_timeout = Some(Duration::from_millis(100));
        let (runtime, _, handle) = setup(spec).await;
        let result = tokio::time::timeout(Duration::from_secs(5), call(&handle, endpoint, "hold"))
            .await
            .unwrap();
        let error = result.unwrap_err();
        assert!(
            error.to_string().to_lowercase().contains("unknown"),
            "{error}"
        );
        assert!(runtime.shutdown().await.is_err());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unbounded_calls_still_have_a_stop_deadline_and_failed_cleanup() {
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        let mut spec = spec();
        spec.stop_timeout = Duration::from_millis(100);
        let (runtime, provider, handle) = setup(spec).await;
        let waiting = tokio::spawn({
            let handle = handle.clone();
            async move { call(&handle, endpoint, "hold").await }
        });
        entered(&handle).await;
        assert!(
            tokio::time::timeout(Duration::from_secs(5), provider.dispose())
                .await
                .unwrap()
                .is_err()
        );
        assert!(matches!(
            provider.snapshot().outcome,
            Some(cordis_sdk::Outcome::CleanupFailed(_))
        ));
        assert!(
            tokio::time::timeout(Duration::from_secs(5), waiting)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        assert!(runtime.shutdown().await.is_err());
    }
}

#[tokio::test]
async fn invalid_frame_capacities_are_rejected_before_process_launch() {
    for capacity in [0, i32::MAX as usize + 1] {
        let mut spec = ProcessSpec::new("nonexistent-capacity-test-executable");
        spec.frame_capacity = capacity;
        let service = Service::new("test.capacity", "1").with_descriptor(
            cordis_remote::Descriptor::new(cordis_remote::Interface::value_dispatch(false))
                .unwrap(),
        );
        let runtime = Runtime::new();
        let provider = runtime.install(spec.plugin("invalid", &service)).unwrap();
        let error = provider.ready().await.unwrap_err();
        assert!(error.to_string().contains("frame_capacity"), "{error}");
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn invalid_deadlines_are_rejected_before_process_launch() {
    for timeout in [Duration::ZERO, Duration::MAX] {
        for setting in 0..3 {
            let mut spec = ProcessSpec::new("nonexistent-deadline-test-executable");
            match setting {
                0 => spec.initialization_timeout = timeout,
                1 => spec.execution_timeout = Some(timeout),
                _ => spec.stop_timeout = timeout,
            }
            let service = Service::new("test.deadlines", "1").with_descriptor(
                cordis_remote::Descriptor::new(cordis_remote::Interface::value_dispatch(false))
                    .unwrap(),
            );
            let runtime = Runtime::new();
            let provider = runtime.install(spec.plugin("invalid", &service)).unwrap();
            let error = provider.ready().await.unwrap_err();
            assert!(
                error.to_string().contains("positive and representable"),
                "{error}"
            );
            runtime.shutdown().await.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_the_waiter_does_not_start_the_stop_deadline() {
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        let mut spec = spec();
        spec.stop_timeout = Duration::from_millis(100);
        let (runtime, _, handle) = setup(spec).await;
        let waiting = tokio::spawn({
            let handle = handle.clone();
            async move { call(&handle, endpoint, "hold").await }
        });
        entered(&handle).await;
        waiting.abort();
        assert!(waiting.await.unwrap_err().is_cancelled());
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            call(&handle, Endpoint::Ipc, "status").await.unwrap(),
            json!(true)
        );
        call(&handle, Endpoint::Ipc, "release").await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), runtime.shutdown())
            .await
            .unwrap()
            .unwrap();
    }
}
