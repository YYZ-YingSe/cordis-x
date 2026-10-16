use super::*;

use std::sync::OnceLock;

static BINARY: OnceLock<PathBuf> = OnceLock::new();
pub(super) fn binary() -> PathBuf {
    BINARY
        .get_or_init(|| {
            let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
            let build = root.join("target/cpp-conformance");
            for args in [
                vec![
                    "-DCORDIS_BUILD_ABI=OFF".into(),
                    "-DCORDIS_BUILD_EXAMPLES=ON".into(),
                    "-DBUILD_TESTING=ON".into(),
                    "-S".into(),
                    root.join("sdk/cpp").display().to_string(),
                    "-B".into(),
                    build.display().to_string(),
                ],
                vec![
                    "--build".into(),
                    build.display().to_string(),
                    "-j".into(),
                    "2".into(),
                ],
            ] {
                let output = std::process::Command::new("cmake")
                    .args(args)
                    .output()
                    .expect("CMake and a C++20 compiler are required for process conformance");
                assert!(
                    output.status.success(),
                    "C++ build failed:\n{}\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            build.join("cordis_calculator")
        })
        .clone()
}

pub(super) fn test_calculator() -> Service<RemoteService> {
    Service::new("example.calculator", "1").with_descriptor(
        cordis_contract::portable::decode(
            &cordis_test_contracts::resources::remote_fixture::SERVICE
                .parse()
                .unwrap(),
        )
        .unwrap(),
    )
}

struct Setup {
    runtime: Runtime,
    provider: PluginHandle,
    consumer: PluginHandle,
    saved: Arc<Mutex<Option<ServiceHandle<RemoteService>>>>,
    trace: PathBuf,
}
impl Setup {
    fn handle(&self) -> ServiceHandle<RemoteService> {
        self.saved.lock().unwrap().as_ref().unwrap().clone()
    }
    async fn new(endpoint: Endpoint, timeout: Duration, cleanup: bool) -> Self {
        let executable = binary();
        let trace = executable.parent().unwrap().join(format!(
            "trace-{}-{}.log",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&trace);
        let mut spec = ProcessSpec::new(executable);

        spec.endpoint = endpoint;
        spec.timeout = timeout;
        spec.args = vec![
            "--test-hooks".into(),
            "--trace".into(),
            trace.display().to_string(),
        ];
        let runtime = Runtime::new();
        let saved = Arc::new(Mutex::new(None));
        let consumer = runtime
            .install(
                Plugin::new("consumer", {
                    let saved = saved.clone();
                    move |ctx| {
                        let saved = saved.clone();
                        async move {
                            *saved.lock().unwrap() = Some(ctx.dependency(&test_calculator())?);
                            if cleanup {
                                ctx.defer("remote cleanup", move |ctx| async move {
                                    assert_eq!(
                                        call(
                                            &ctx.dependency(&test_calculator())?,
                                            endpoint,
                                            "add",
                                            json!({"left":20,"right":22})
                                        )
                                        .await?,
                                        json!("42")
                                    );
                                    Ok(())
                                })?;
                            }
                            Ok(())
                        }
                    }
                })
                .requires(&test_calculator()),
            )
            .unwrap();
        let provider = runtime
            .install(spec.plugin("cpp", &test_calculator()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        bounded(consumer.ready()).await.unwrap();
        Self {
            runtime,
            provider,
            consumer,
            saved,
            trace,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_cpp_binary_supports_both_endpoints_with_restart() {
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        let setup = Setup::new(endpoint, Duration::from_secs(5), true).await;
        let handle = setup.handle();
        for endpoint in [Endpoint::Ipc, Endpoint::Http] {
            let input = json!({"unicode":"中文 🦀", "values":[null,true,false,-9_007_199_254_740_991f64,9_007_199_254_740_991f64,{"nested":[1.0,2.0]}]});
            assert_eq!(
                bounded(call(&handle, endpoint, "echo", input.clone()))
                    .await
                    .unwrap(),
                input
            );
            assert_eq!(
                call(&handle, endpoint, "add", json!({"left":20,"right":22}))
                    .await
                    .unwrap(),
                json!("42")
            );
            assert!(
                call(&handle, endpoint, "unknown", json!(null))
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("InvalidInput")
            );
            // google.protobuf.Value explicitly uses binary64 numbers.
            assert_eq!(
                call(&handle, endpoint, "echo", json!(u64::MAX))
                    .await
                    .unwrap(),
                json!(u64::MAX as f64)
            );
            assert_eq!(
                setup.provider.snapshot().state,
                Lifecycle::Active,
                "invalid local input must not kill the provider"
            );
        }
        let epoch = setup.consumer.snapshot().activation.unwrap().epoch;
        bounded(setup.provider.restart()).await.unwrap();
        bounded(setup.consumer.ready()).await.unwrap();
        assert!(setup.consumer.snapshot().activation.unwrap().epoch > epoch);
        assert!(matches!(
            call(&handle, endpoint, "echo", json!(null)).await,
            Err(Error::Closed)
        ));
        bounded(setup.runtime.shutdown()).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(&setup.trace).unwrap(),
            "activated\ndeactivated\nactivated\ndeactivated\n"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_call_wait_keeps_process_alive_through_consumer_cleanup() {
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        let setup = Setup::new(endpoint, Duration::from_secs(5), true).await;
        let handle = setup.handle();
        let waiting = tokio::spawn({
            let handle = handle.clone();
            async move { call(&handle, endpoint, "hold", json!("work")).await }
        });
        bounded(async {
            loop {
                let status = call(&handle, Endpoint::Http, "status", json!({}))
                    .await
                    .unwrap();
                if status["waiting"] == json!(["work"]) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        // An already-admitted second invocation retains continuation permission during closing.
        let release = Arc::new(Semaphore::new(0));
        let admitted = Arc::new(Semaphore::new(0));
        let releaser = tokio::spawn({
            let handle = handle.clone();
            let release = release.clone();
            let admitted = admitted.clone();
            async move {
                handle
                    .call(move |remote, ctx| {
                        Box::pin(async move {
                            admitted.add_permits(1);
                            release.acquire().await.unwrap().forget();
                            remote
                                .invoke_json_via(&ctx, Endpoint::Ipc, "release", json!("work"))
                                .await
                                .map_err(Into::into)
                        })
                    })
                    .await
            }
        });
        bounded(admitted.acquire()).await.unwrap().forget();
        waiting.abort();
        let _ = waiting.await;
        let disposing = tokio::spawn({
            let provider = setup.provider.clone();
            async move { provider.dispose().await }
        });
        bounded(async {
            while setup.consumer.snapshot().state != Lifecycle::Unloading {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(!disposing.is_finished());
        assert!(
            !std::fs::read_to_string(&setup.trace)
                .unwrap()
                .contains("deactivated")
        );
        release.add_permits(1);
        bounded(releaser).await.unwrap().unwrap();
        bounded(disposing).await.unwrap().unwrap();
        assert_eq!(
            std::fs::read_to_string(&setup.trace).unwrap(),
            "activated\nhold:work\nreleased:work\ndeactivated\n"
        );
        bounded(setup.runtime.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn process_crash_withdraws_provider_and_preserves_unconfirmed_cleanup() {
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        let setup = Setup::new(endpoint, Duration::from_secs(5), false).await;
        assert!(
            bounded(call(&setup.handle(), endpoint, "crash", json!({})))
                .await
                .is_err()
        );
        bounded(async {
            loop {
                if matches!(
                    setup.provider.snapshot().outcome,
                    Some(Outcome::CleanupFailed(_))
                ) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert_eq!(setup.provider.snapshot().state, Lifecycle::Unloading);
        assert_eq!(setup.consumer.snapshot().state, Lifecycle::Inactive);
        assert!(bounded(setup.runtime.shutdown()).await.is_err());
        assert_eq!(
            std::fs::read_to_string(&setup.trace).unwrap(),
            "activated\n"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unconfirmed_timeout_terminates_owned_process_without_claiming_clean_recovery() {
    let setup = Setup::new(Endpoint::Ipc, Duration::from_millis(200), false).await;
    let error = bounded(call(
        &setup.handle(),
        Endpoint::Ipc,
        "hold",
        json!("timeout"),
    ))
    .await
    .unwrap_err();
    assert!(error.to_string().contains("OutcomeUnknown"));
    assert!(bounded(setup.provider.settled()).await.is_err());
    assert!(bounded(setup.runtime.shutdown()).await.is_err());
    assert!(
        !std::fs::read_to_string(&setup.trace)
            .unwrap()
            .contains("deactivated")
    );
}

#[tokio::test]
async fn incompatible_contract_fails_before_activation_and_process_is_reaped() {
    let runtime = Runtime::new();
    let wrong = Service::<RemoteService>::new("example.calculator", "2").with_descriptor(
        cordis_contract::portable::decode(
            &cordis_example_contracts::resources::calculator_process::SERVICE
                .parse()
                .unwrap(),
        )
        .unwrap(),
    );
    let provider = runtime
        .install(ProcessSpec::new(binary()).plugin("wrong", &wrong))
        .unwrap();
    assert!(matches!(
        bounded(provider.ready()).await,
        Err(Error::InitializationFailed(_, _))
    ));
    assert_eq!(provider.snapshot().state, Lifecycle::Inactive);
    bounded(runtime.shutdown()).await.unwrap();
}

#[tokio::test]
async fn ipc_only_host_does_not_require_an_http_endpoint() {
    let runtime = Runtime::new();
    let mut spec = ProcessSpec::new(binary());
    spec.args.push("--stdio-only".into());
    let provider = runtime
        .install(spec.plugin("ipc-only", &calculator()))
        .unwrap();
    bounded(provider.ready()).await.unwrap();
    bounded(runtime.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_inside_service_callback_also_retains_remote_operation_leases() {
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        let setup = Setup::new(endpoint, Duration::from_secs(5), true).await;
        let handle = setup.handle();
        let cancel = Arc::new(Semaphore::new(0));
        let outer = tokio::spawn({
            let handle = handle.clone();
            let cancel = cancel.clone();
            async move {
                handle.call(move |remote, ctx| Box::pin(async move {
                tokio::select! {
                    result = remote.invoke_json_via(&ctx, endpoint, "hold", json!("inner")) => result.map(|_| ()).map_err(Into::into),
                    permit = cancel.acquire() => { permit.unwrap().forget(); Ok(()) }
                }
            })).await
            }
        });
        bounded(async {
            loop {
                if call(&handle, Endpoint::Ipc, "status", json!({}))
                    .await
                    .unwrap()["waiting"]
                    == json!(["inner"])
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        cancel.add_permits(1);
        bounded(outer).await.unwrap().unwrap();
        let release = Arc::new(Semaphore::new(0));
        let admitted = Arc::new(Semaphore::new(0));
        let releaser = tokio::spawn({
            let handle = handle.clone();
            let release = release.clone();
            let admitted = admitted.clone();
            async move {
                handle
                    .call(move |remote, ctx| {
                        Box::pin(async move {
                            admitted.add_permits(1);
                            release.acquire().await.unwrap().forget();
                            remote
                                .invoke_json_via(&ctx, Endpoint::Http, "release", json!("inner"))
                                .await
                                .map_err(Into::into)
                        })
                    })
                    .await
            }
        });
        bounded(admitted.acquire()).await.unwrap().forget();
        let dispose = tokio::spawn({
            let provider = setup.provider.clone();
            async move { provider.dispose().await }
        });
        bounded(async {
            while setup.consumer.snapshot().state != Lifecycle::Unloading {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(!dispose.is_finished());
        release.add_permits(1);
        bounded(releaser).await.unwrap().unwrap();
        bounded(dispose).await.unwrap().unwrap();
        assert_eq!(
            std::fs::read_to_string(&setup.trace).unwrap(),
            "activated\nhold:inner\nreleased:inner\ndeactivated\n"
        );
        bounded(setup.runtime.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_resources_belong_to_independent_callers_and_close_before_provider() {
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        let setup = Setup::new(endpoint, Duration::from_secs(5), false).await;
        let other = Arc::new(Mutex::new(None));
        let consumer = setup
            .runtime
            .install(
                Plugin::new("other owner", {
                    let other = other.clone();
                    move |ctx| {
                        let other = other.clone();
                        async move {
                            *other.lock().unwrap() = Some(ctx.dependency(&test_calculator())?);
                            Ok(())
                        }
                    }
                })
                .requires(&test_calculator()),
            )
            .unwrap();
        bounded(consumer.ready()).await.unwrap();
        let other_handle = other.lock().unwrap().take().unwrap();
        let a = acquire(&setup.handle(), endpoint, json!({"name":"a","initial":40}))
            .await
            .unwrap();
        let b = acquire(&other_handle, endpoint, json!({"name":"b","initial":7}))
            .await
            .unwrap();
        assert_eq!(a.invoke_json("add", json!(2)).await.unwrap(), "42");
        assert_eq!(setup.consumer.effects().len(), 1);
        assert_eq!(consumer.effects().len(), 1);
        bounded(setup.consumer.dispose()).await.unwrap();
        assert!(matches!(
            a.invoke_json("get", json!({})).await,
            Err(cordis_remote::Error::Closed)
        ));
        assert_eq!(b.invoke_json("get", json!({})).await.unwrap(), "7");
        assert_eq!(setup.provider.snapshot().state, Lifecycle::Active);
        bounded(setup.provider.dispose()).await.unwrap();
        assert!(matches!(
            b.invoke_json("get", json!({})).await,
            Err(cordis_remote::Error::Closed)
        ));
        assert_eq!(
            std::fs::read_to_string(&setup.trace).unwrap(),
            "activated\nresource_acquired:a\nresource_acquired:b\nresource_closed:a\nresource_closed:b\ndeactivated\n"
        );
        bounded(setup.runtime.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_remote_disposal_waits_for_calls_and_runs_once() {
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        let setup = Setup::new(endpoint, Duration::from_secs(5), false).await;
        let resource = acquire(&setup.handle(), endpoint, json!({"name":"held"}))
            .await
            .unwrap();
        let waiting = tokio::spawn({
            let resource = resource.clone();
            async move { resource.invoke_json("hold", json!("resource-call")).await }
        });
        bounded(async {
            loop {
                if call(&setup.handle(), Endpoint::Ipc, "status", json!({}))
                    .await
                    .unwrap()["waiting"]
                    == json!(["resource-call"])
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        waiting.abort();
        let _ = waiting.await;
        let first = tokio::spawn({
            let resource = resource.clone();
            async move { resource.dispose().await }
        });
        bounded(async {
            while resource.snapshot().status != EffectStatus::Releasing {
                tokio::task::yield_now().await;
            }
        })
        .await;
        let second = tokio::spawn({
            let resource = resource.clone();
            async move { resource.dispose().await }
        });
        assert!(!first.is_finished());
        call(
            &setup.handle(),
            Endpoint::Http,
            "release",
            json!("resource-call"),
        )
        .await
        .unwrap();
        bounded(first).await.unwrap().unwrap();
        bounded(second).await.unwrap().unwrap();
        assert_eq!(resource.snapshot().status, EffectStatus::Released);
        assert!(matches!(
            resource.invoke_json("get", json!({})).await,
            Err(cordis_remote::Error::Closed)
        ));
        bounded(setup.runtime.shutdown()).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(&setup.trace)
                .unwrap()
                .matches("resource_closed:held")
                .count(),
            1
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_remote_acquisition_still_closes_its_late_resource() {
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        let setup = Setup::new(endpoint, Duration::from_secs(5), false).await;
        let wait = tokio::spawn({
            let handle = setup.handle();
            async move { acquire(&handle, endpoint, json!({"name":"late","wait":"acquire"})).await }
        });
        bounded(async {
            loop {
                if call(&setup.handle(), Endpoint::Ipc, "status", json!({}))
                    .await
                    .unwrap()["waiting"]
                    == json!(["acquire"])
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        wait.abort();
        let _ = wait.await;
        call(&setup.handle(), Endpoint::Http, "release", json!("acquire"))
            .await
            .unwrap();
        bounded(setup.runtime.shutdown()).await.unwrap();
        let trace = std::fs::read_to_string(&setup.trace).unwrap();
        assert!(trace.ends_with("resource_acquired:late\nresource_closed:late\ndeactivated\n"));
    }
}

#[tokio::test]
async fn remote_release_failure_is_not_retried_and_blocks_provider_cleanup() {
    let setup = Setup::new(Endpoint::Http, Duration::from_secs(5), false).await;
    let resource = acquire(
        &setup.handle(),
        Endpoint::Http,
        json!({"name":"failure","fail_close":true}),
    )
    .await
    .unwrap();
    assert!(bounded(resource.dispose()).await.is_err());
    assert!(bounded(resource.dispose()).await.is_err());
    assert!(matches!(
        bounded(setup.provider.dispose()).await,
        Err(Error::CleanupBlocked(_, _))
    ));
    assert!(matches!(
        resource.snapshot().status,
        EffectStatus::Failed(_)
    ));
    let trace = std::fs::read_to_string(&setup.trace).unwrap();
    assert_eq!(trace.matches("resource_closed:failure").count(), 1);
    assert!(!trace.contains("deactivated"));
    assert!(bounded(setup.runtime.shutdown()).await.is_err());
}

#[tokio::test]
async fn unknown_remote_acquisition_remains_in_the_callers_effect_ledger() {
    let setup = Setup::new(Endpoint::Ipc, Duration::from_millis(200), false).await;
    assert!(
        bounded(acquire(
            &setup.handle(),
            Endpoint::Ipc,
            json!({"wait":"unknown"})
        ))
        .await
        .is_err()
    );
    bounded(async {
        loop {
            if setup
                .consumer
                .effects()
                .iter()
                .any(|e| matches!(e.status, EffectStatus::Failed(_)))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(setup.consumer.snapshot().state, Lifecycle::Unloading);
    assert!(matches!(
        bounded(setup.provider.dispose()).await,
        Err(Error::CleanupBlocked(_, _))
    ));
    assert!(bounded(setup.runtime.shutdown()).await.is_err());
}

#[tokio::test]
async fn cpp_resource_grants_prevent_cross_owner_access_and_release_is_idempotent() {
    let mut process = RawProcess::sdk(Backend::Cpp).await;
    let owner = "0123456789abcdef0123456789abcdef";
    let request = process.request(
        "resource_acquire",
        json!({"kind":"counter","args":{"initial":42},"owner":owner}),
    );
    let value = process.exchange(request).await;
    let id = value["outcome"]["value"]["resource"].clone();
    let request = process.request(
        "resource_call",
        json!({"resource":id,"owner":"fedcba9876543210fedcba9876543210","method":"get","args":{}}),
    );
    assert_eq!(
        process.exchange(request).await["outcome"]["error"]["code"],
        "resource_owner_mismatch"
    );
    let request = process.request(
        "resource_call",
        json!({"resource":id,"owner":owner,"method":"get","args":{}}),
    );
    assert_eq!(process.exchange(request).await["outcome"]["value"], "42");
    for _ in 0..2 {
        let request = process.request("resource_release", json!({"resource":id,"owner":owner}));
        assert_eq!(
            process.exchange(request).await["outcome"]["kind"],
            "success"
        );
    }
    process.finish().await;
}

#[tokio::test]
async fn cpp_stdio_backpressure_keeps_input_and_other_calls_moving() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut process = RawProcess::sdk(Backend::Cpp).await;
    let payload = json!("x".repeat(256 * 1024));
    let mut operations = std::collections::BTreeSet::new();
    // Deliberately don't read stdout until all these large inputs have
    // arrived. A blocking writer on the business executor would deadlock.
    bounded(async {
        for _ in 0..12 {
            let request = process.request("call", json!({"method":"echo", "args":payload}));
            operations.insert(request["operation"].clone().as_str().unwrap().to_owned());
            let bytes = process.encode(&request);
            process.input.write_u32(bytes.len() as u32).await.unwrap();

            process.input.write_all(&bytes).await.unwrap();
        }
        process.input.flush().await.unwrap();
    })
    .await;
    for _ in 0..12 {
        let size = bounded(process.output.read_u32()).await.unwrap();
        assert!(size <= cordis_wire::MAX_MESSAGE as u32);

        let mut bytes = vec![0; size as usize];
        bounded(process.output.read_exact(&mut bytes))
            .await
            .unwrap();
        let response = process.decode(bytes);
        assert!(operations.remove(response["operation"].as_str().unwrap()));
        assert_eq!(response["outcome"]["kind"], "success");
        assert_eq!(response["outcome"]["value"], payload);
    }
    assert!(operations.is_empty());
    process.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_http_io_timeout_does_not_shorten_an_admitted_business_call() {
    let setup = Setup::new(Endpoint::Http, Duration::from_secs(12), false).await;
    let waiting = tokio::spawn({
        let handle = setup.handle();
        async move { call(&handle, Endpoint::Http, "hold", json!("slow-http")).await }
    });
    bounded(async {
        loop {
            if std::fs::read_to_string(&setup.trace)
                .unwrap_or_default()
                .contains("hold:slow-http")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    // Read/write protection is five seconds; the host granted this business
    // operation twelve seconds. Keep the call suspended past the I/O deadline.
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert!(!waiting.is_finished());
    call(
        &setup.handle(),
        Endpoint::Ipc,
        "release",
        json!("slow-http"),
    )
    .await
    .unwrap();
    bounded(waiting).await.unwrap().unwrap();
    bounded(setup.runtime.shutdown()).await.unwrap();
}

#[tokio::test]
async fn cpp_streaming_frames_preserve_boundaries_and_allow_queued_calls_to_progress() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut process = RawProcess::sdk(Backend::Cpp).await;
    let mut packets = Vec::new();
    let mut expected = std::collections::BTreeMap::new();
    for index in 0..48 {
        let value = json!({"index":index,"text":"中文 🦀"});
        let request = process.request("call", json!({"method":"echo","args":value}));
        expected.insert(request["operation"].as_str().unwrap().to_owned(), value);
        let payload = process.encode(&request);
        let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
        frame.extend(payload);
        packets.push(frame);
    }
    // Split both the first frame's header and its multibyte body, then
    // concatenate a burst of complete frames without waiting for replies.
    for byte in &packets[0][..4] {
        process.input.write_all(&[*byte]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let split = packets[0]
        .windows("中".len())
        .position(|bytes| bytes == "中".as_bytes())
        .unwrap()
        + 1;
    process
        .input
        .write_all(&packets[0][4..split])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1)).await;
    let rest: Vec<_> = packets[0][split..]
        .iter()
        .chain(packets[1..].iter().flatten())
        .copied()
        .collect();
    bounded(process.input.write_all(&rest)).await.unwrap();
    for _ in 0..48 {
        let length = bounded(process.output.read_u32()).await.unwrap();
        assert!(length <= cordis_wire::MAX_MESSAGE as u32);

        let mut bytes = vec![0; length as usize];
        bounded(process.output.read_exact(&mut bytes))
            .await
            .unwrap();
        let response = process.decode(bytes);
        let value = expected
            .remove(response["operation"].as_str().unwrap())
            .unwrap();
        assert_eq!(response["outcome"]["kind"], "success", "{response}");
        assert_eq!(response["outcome"]["value"], unwire_json(wire_json(value)));
    }
    assert!(expected.is_empty());
    process.finish().await;
}

#[test]
fn cpp_effect_handles_survive_context_expiry_and_cache_cleanup_failures() {
    let result = std::process::Command::new(binary().with_file_name("cordis_effects_test"))
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}
