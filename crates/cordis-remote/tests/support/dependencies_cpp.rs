use super::*;

fn pricing() -> Service<RemoteService> {
    Service::new("example.pricing", "1").with_descriptor(descriptor(
        cordis_example_contracts::resources::pricing::PRICING,
    ))
}

fn orders() -> Service<RemoteService> {
    Service::new("example.orders", "1").with_descriptor(descriptor(
        cordis_example_contracts::resources::orders::ORDERS,
    ))
}

fn log_dependency() -> Dependency {
    Dependency::new_json(&logger(), logger_descriptor(), write_log)
}

fn price_spec(endpoint: Endpoint) -> ProcessSpec {
    let mut spec =
        ProcessSpec::new(binary().with_file_name("cordis_pricing")).requires(log_dependency());

    spec.endpoint = endpoint;
    if matches!(endpoint, Endpoint::Ipc) {
        spec.args = vec!["--stdio-only".into()];
    }
    spec
}

async fn rejects_bad_quotes(
    handle: &ServiceHandle<RemoteService>,
    endpoint: Endpoint,
    method: &str,
    state: &LogState,
) {
    let before = state.events.lock().unwrap().clone();
    for args in [
        Value::Null,
        json!({"quantity": 1}),
        json!({"quantity": "bad", "unit_price": 2}),
        json!({"quantity": 1, "unit_price": null}),
        json!({"quantity": 1, "unit_price": 2, "discount": 1}),
    ] {
        assert!(matches!(
            checked_call(handle, endpoint, method, args).await,
            Err(cordis_remote::Error::InvalidInput(_))
        ));
    }
    for args in [
        json!({"quantity": -1, "unit_price": 2}),
        json!({"quantity": 1_000_001, "unit_price": 2}),
        json!({"quantity": 1, "unit_price": -1}),
        json!({"quantity": 1, "unit_price": 1_000_001}),
    ] {
        assert!(matches!(
            checked_call(handle, endpoint, method, args).await,
            Err(cordis_remote::Error::Remote { code, .. }) if code == "invalid_argument"
        ));
    }
    assert_eq!(*state.events.lock().unwrap(), before);
}

async fn checked_call(
    handle: &ServiceHandle<RemoteService>,
    endpoint: Endpoint,
    method: &str,
    args: Value,
) -> cordis_remote::Result<Value> {
    let method = method.to_owned();
    bounded(handle.call(move |remote, ctx| {
        Box::pin(async move { Ok(remote.invoke_json_via(&ctx, endpoint, &method, args).await) })
    }))
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adder_checks_fields_and_declared_overflow_across_endpoints() {
    let adder = Service::<RemoteService>::new("example.adder", "1").with_descriptor(descriptor(
        cordis_example_contracts::resources::examples::ADDER,
    ));

    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        let rt = Runtime::new();
        let mut spec = ProcessSpec::new(binary().with_file_name("cordis_adder"));

        spec.endpoint = endpoint;
        let provider = rt.install(spec.plugin("adder", &adder)).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (_, saved) = consumer(&rt, adder.clone()).await;
        let handle = get(&saved);
        for args in [
            Value::Null,
            json!({"left": 1}),
            json!({"left": true, "right": 2}),
            json!({"left": 1, "right": 2, "extra": 3}),
        ] {
            assert!(matches!(
                checked_call(&handle, endpoint, "add", args).await,
                Err(cordis_remote::Error::InvalidInput(_))
            ));
        }
        for args in [
            json!({"left": "9223372036854775807", "right": "1"}),
            json!({"left": "-9223372036854775808", "right": "-1"}),
        ] {
            assert!(matches!(
                checked_call(&handle, endpoint, "add", args).await,
                Err(cordis_remote::Error::Remote { code, .. }) if code == "overflow"
            ));
        }
        assert_eq!(
            checked_call(&handle, endpoint, "add", json!({"left": 20, "right": 22}))
                .await
                .unwrap(),
            json!({"value":"42"})
        );
        bounded(rt.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pricing_rejects_bad_requests_before_log_effects_across_endpoints() {
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        let rt = Runtime::new();
        let state = LogState::new(false);
        rt.install(log_plugin(state.clone())).unwrap();
        let provider = rt
            .install(price_spec(endpoint).plugin("pricing", &pricing()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (_, saved) = consumer(&rt, pricing()).await;
        let handle = get(&saved);
        rejects_bad_quotes(&handle, endpoint, "quote", &state).await;
        let before = state.events.lock().unwrap().clone();
        assert!(matches!(
            checked_call(&handle, endpoint, "flush", json!(null)).await,
            Err(cordis_remote::Error::InvalidInput(_))
        ));
        assert_eq!(*state.events.lock().unwrap(), before);
        for (quantity, unit_price, total) in
            [(0, 2, 0_i64), (1_000_000, 1_000_000, 1_000_000_000_000)]
        {
            let result = checked_call(
                &handle,
                endpoint,
                "quote",
                json!({"quantity": quantity, "unit_price": unit_price}),
            )
            .await
            .unwrap();
            assert_eq!(result["total"], (total).to_string());
            assert_eq!(result["receipt"]["generation"], (1).to_string());
        }
        bounded(rt.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_dependencies_preserve_definition_use_site_and_cleanup_across_endpoints() {
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        let rt = Runtime::new();
        let state = LogState::new(false);
        // Provider's logger is isolated. The outer consumer has no logger dependency.
        let view = View::new().isolate(&logger());
        let provider = rt
            .install_in(
                view.clone(),
                price_spec(endpoint).plugin("pricing", &pricing()),
            )
            .unwrap();
        assert_eq!(
            bounded(provider.settled()).await.unwrap().state,
            Lifecycle::Inactive
        );
        let log = rt.install_in(view, log_plugin(state.clone())).unwrap();
        bounded(provider.ready()).await.unwrap();
        let (client, saved) = consumer(&rt, pricing()).await;
        let old = get(&saved);
        let result = bounded(call(
            &old,
            endpoint,
            "quote",
            json!({"quantity":3,"unit_price":1400}),
        ))
        .await
        .unwrap();
        assert_eq!(result["total"], (4200).to_string());
        assert_eq!(
            result["receipt"]["owner"],
            (client.snapshot().id.0).to_string()
        );
        assert_eq!(
            result["receipt"]["caller"],
            (provider.snapshot().id.0).to_string()
        );
        assert_eq!(result["receipt"]["generation"], (1).to_string());
        let error = call(&old, endpoint, "missing", Value::Null)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("InvalidInput"));
        bounded(log.restart()).await.unwrap();
        bounded(provider.ready()).await.unwrap();
        bounded(client.ready()).await.unwrap();
        assert!(matches!(
            call(&old, endpoint, "flush", serde_json::json!({})).await,
            Err(Error::Closed)
        ));
        let result = call(&get(&saved), endpoint, "flush", serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(result["generation"], (2).to_string());
        ordered(
            &state.events.lock().unwrap(),
            1,
            &[
                "pricing.started",
                "pricing.quoted",
                "effect.released",
                "pricing.stopped",
                "logger.closed",
            ],
        );
        bounded(rt.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typescript_to_cpp_to_native_dependencies_restart_and_cleanup_in_order() {
    for endpoint in [Endpoint::Ipc, Endpoint::Http] {
        let rt = Runtime::new();
        let state = LogState::new(false);
        let log = rt.install(log_plugin(state.clone())).unwrap();
        // Deliberately use MessagePack on C++ while TypeScript uses JSON.
        let price = rt
            .install(price_spec(endpoint).plugin("pricing", &pricing()))
            .unwrap();
        let mut spec = ProcessSpec::new("node").requires(Dependency::remote(&pricing()));
        spec.endpoint = endpoint;
        spec.args = vec![
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../sdk/typescript/examples/orders.ts")
                .display()
                .to_string(),
        ];
        let order = rt.install(spec.plugin("orders", &orders())).unwrap();
        bounded(price.ready()).await.unwrap();
        bounded(order.ready()).await.unwrap();
        let (client, saved) = consumer(&rt, orders()).await;
        let before = order.snapshot().activation.unwrap();
        rejects_bad_quotes(&get(&saved), endpoint, "submit", &state).await;
        let result = call(
            &get(&saved),
            endpoint,
            "submit",
            json!({"quantity":3,"unit_price":1400}),
        )
        .await
        .unwrap();
        assert_eq!(result["status"], "priced");
        assert_eq!(result["quote"]["total"], (4200).to_string());
        assert_eq!(
            result["quote"]["receipt"]["owner"],
            (client.snapshot().id.0).to_string()
        );
        assert_eq!(
            result["quote"]["receipt"]["caller"],
            (price.snapshot().id.0).to_string()
        );
        bounded(log.restart()).await.unwrap();
        bounded(price.ready()).await.unwrap();
        bounded(order.ready()).await.unwrap();
        bounded(client.ready()).await.unwrap();
        assert_ne!(before, order.snapshot().activation.unwrap());
        ordered(
            &state.events.lock().unwrap(),
            1,
            &[
                "effect.released",
                "pricing.flushed",
                "pricing.stopped",
                "logger.closed",
            ],
        );
        bounded(rt.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_reverse_call_wait_retains_caller_effects_and_native_dependency() {
    let rt = Runtime::new();
    let state = LogState::new(true);
    let log = rt.install(log_plugin(state.clone())).unwrap();
    let provider = rt
        .install(price_spec(Endpoint::Http).plugin("pricing", &pricing()))
        .unwrap();
    bounded(provider.ready()).await.unwrap();
    let (client, saved) = consumer(&rt, pricing()).await;
    let handle = get(&saved);
    let waiting = tokio::spawn(async move {
        call(
            &handle,
            Endpoint::Http,
            "quote",
            json!({"quantity":1,"unit_price":1}),
        )
        .await
    });
    bounded(state.entered.acquire()).await.unwrap().forget();
    waiting.abort();
    let _ = waiting.await;
    let mut restarting = tokio::spawn(async move { log.restart().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(40), &mut restarting)
            .await
            .is_err()
    );
    assert!(
        !state
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|(_, event)| event == "logger.closed")
    );
    state.block.store(false, Ordering::SeqCst);
    state.release.add_permits(1);
    bounded(restarting).await.unwrap().unwrap();
    bounded(provider.ready()).await.unwrap();
    bounded(client.ready()).await.unwrap();
    ordered(
        &state.events.lock().unwrap(),
        1,
        &[
            "pricing.quoted",
            "effect.released",
            "pricing.stopped",
            "logger.closed",
        ],
    );
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dependency_declaration_mismatch_fails_before_business_activation() {
    let rt = Runtime::new();
    let provider = rt
        .install(
            ProcessSpec::new(binary().with_file_name("cordis_pricing"))
                .plugin("unbound", &pricing()),
        )
        .unwrap();
    assert!(bounded(provider.ready()).await.is_err());
    assert!(format!("{:?}", provider.snapshot().outcome).contains("contract_mismatch"));
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pricing_dependency_shape_drift_is_rejected_before_activation() {
    let rt = Runtime::new();
    let state = LogState::new(false);
    rt.install(log_plugin(state.clone())).unwrap();
    let mut interface = logger_descriptor().interface().clone();
    let cordis_remote::Dispatch::Named(methods) = &mut interface.methods else {
        unreachable!()
    };
    methods.get_mut("write").unwrap().result =
        cordis_remote::MessageContract::standard("google.protobuf.StringValue");
    let mut spec = price_spec(Endpoint::Ipc);
    spec.dependencies = vec![Dependency::new_json(
        &logger(),
        cordis_remote::Descriptor::new(interface).unwrap(),
        write_log,
    )];
    let provider = rt.install(spec.plugin("pricing", &pricing())).unwrap();
    assert!(bounded(provider.ready()).await.is_err());
    assert!(format!("{:?}", provider.snapshot().outcome).contains("result"));
    assert!(state.events.lock().unwrap().is_empty());
    bounded(rt.shutdown()).await.unwrap();
}

#[test]
fn cpp_dependency_context_rejects_expired_calls_and_invalid_replies() {
    let output = std::process::Command::new(binary().with_file_name("cordis_context_test"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
