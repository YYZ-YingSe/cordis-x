use super::*;
use cordis_remote::{Dependency, Value};
use std::sync::atomic::AtomicBool;

struct NativeLog {
    generation: usize,
    open: AtomicBool,
    state: Arc<LogState>,
}
struct LogState {
    generation: AtomicUsize,
    events: Mutex<Vec<(usize, String)>>,
    block: AtomicBool,
    entered: Semaphore,
    release: Semaphore,
}
impl LogState {
    fn new(block: bool) -> Arc<Self> {
        Arc::new(Self {
            generation: AtomicUsize::new(0),
            events: Mutex::default(),
            block: AtomicBool::new(block),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        })
    }
}
fn logger() -> Service<NativeLog> {
    Service::new("example.logger", "1")
}
fn descriptor(source: &str) -> cordis_remote::Descriptor {
    cordis_contract::portable::decode(&serde_json::from_str(source).unwrap()).unwrap()
}
fn logger_descriptor() -> cordis_remote::Descriptor {
    descriptor(cordis_example_contracts::resources::logger::LOGGER)
}
fn log_plugin(state: Arc<LogState>) -> Plugin {
    Plugin::new("logger", move |ctx| {
        let generation = state.generation.fetch_add(1, Ordering::SeqCst) + 1;
        let log = Arc::new(NativeLog {
            generation,
            open: AtomicBool::new(true),
            state: state.clone(),
        });
        async move {
            let cleanup = log.clone();
            ctx.defer("native logger", move |_| async move {
                cleanup.open.store(false, Ordering::SeqCst);
                cleanup
                    .state
                    .events
                    .lock()
                    .unwrap()
                    .push((generation, "logger.closed".into()));
                Ok(())
            })?;
            ctx.provide(&logger(), log)
        }
    })
    .provides(&logger())
}
fn write_log(
    log: &NativeLog,
    ctx: Context,
    method: String,
    args: Value,
) -> cordis_sdk::BoxFuture<'_, cordis_remote::Result<Value>> {
    Box::pin(async move {
        if method != "write" {
            return Err(cordis_remote::Error::Refused {
                code: "unknown_method".into(),
                message: method,
            });
        }
        assert!(
            log.open.load(Ordering::SeqCst),
            "logger closed before dependency callback"
        );
        let event = args.as_str().unwrap().to_owned();
        if event == "pricing.quoted" || event == "block" {
            log.state.entered.add_permits(1);
            if log.state.block.load(Ordering::SeqCst) {
                log.state.release.acquire().await.unwrap().forget();
            }
            let state = log.state.clone();
            let generation = log.generation;
            // Effects created by a nested native dependency must belong to the outer caller.
            ctx.defer("dependency contribution", move |_| async move {
                state
                    .events
                    .lock()
                    .unwrap()
                    .push((generation, "effect.released".into()));
                Ok(())
            })
            .map_err(|e| cordis_remote::Error::Resource(e.to_string()))?;
        }
        assert!(
            log.open.load(Ordering::SeqCst),
            "logger closed during dependency callback"
        );
        log.state
            .events
            .lock()
            .unwrap()
            .push((log.generation, event));
        Ok(
            json!({"generation":log.generation,"owner":ctx.owner().fiber.0,"caller":ctx.caller().unwrap().fiber.0}),
        )
    })
}
async fn consumer(
    runtime: &Runtime,
    service: Service<RemoteService>,
) -> (
    PluginHandle,
    Arc<Mutex<Option<ServiceHandle<RemoteService>>>>,
) {
    let saved = Arc::new(Mutex::new(None));
    let handle = runtime
        .install(
            Plugin::new("caller", {
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
    bounded(handle.ready()).await.unwrap();
    (handle, saved)
}
fn get(saved: &Arc<Mutex<Option<ServiceHandle<RemoteService>>>>) -> ServiceHandle<RemoteService> {
    saved.lock().unwrap().as_ref().unwrap().clone()
}

fn ordered(events: &[(usize, String)], generation: usize, names: &[&str]) {
    let filtered: Vec<_> = events
        .iter()
        .filter(|(g, _)| *g == generation)
        .map(|(_, name)| name.as_str())
        .collect();
    let mut offset = 0;
    for name in names {
        offset += filtered[offset..]
            .iter()
            .position(|event| event == name)
            .unwrap_or_else(|| panic!("missing {name} in order: {filtered:?}"))
            + 1;
    }
}

fn probe() -> Service<RemoteService> {
    Service::new("test.dependency-probe", "1").with_descriptor(
        cordis_remote::Descriptor::new(cordis_remote::Interface::value_dispatch(false)).unwrap(),
    )
}
fn fault_spec(timeout: Duration) -> ProcessSpec {
    let mut interface = logger_descriptor().interface().clone();
    let cordis_remote::Dispatch::Named(methods) = &mut interface.methods else {
        unreachable!()
    };
    methods
        .get_mut("write")
        .unwrap()
        .errors
        .insert("expected".into());
    let dependency = Dependency::new_json(
        &logger(),
        cordis_remote::Descriptor::new(interface).unwrap(),
        |log, ctx, method, args| {
            Box::pin(async move {
                if args == "reject" {
                    return Err(cordis_remote::Error::Remote {
                        code: "expected".into(),
                        message: "probe rejection".into(),
                    });
                }
                write_log(log, ctx, method, args).await
            })
        },
    );
    let mut spec = python_spec().requires(dependency);
    spec.args.extend([PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/dependency_peer.py")
        .display()
        .to_string()]);
    spec.timeout = timeout;
    spec
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reverse_requests_reject_expired_contexts_undeclared_services_and_preserve_business_errors()
{
    let rt = Runtime::new();
    let state = LogState::new(false);
    rt.install(log_plugin(state.clone())).unwrap();
    let provider = rt
        .install(fault_spec(Duration::from_secs(5)).plugin("peer", &probe()))
        .unwrap();
    bounded(provider.ready()).await.unwrap();
    let (_, saved) = consumer(&rt, probe()).await;
    let handle = get(&saved);
    call(&handle, Endpoint::Ipc, "cache", Value::Null)
        .await
        .unwrap();
    for (method, code) in [
        ("stale", "closed"),
        ("undeclared", "undeclared_dependency"),
        ("business_error", "expected"),
    ] {
        let result = call(&handle, Endpoint::Ipc, method, Value::Null)
            .await
            .unwrap();
        assert_eq!(
            result["kind"],
            if method == "business_error" {
                "failure"
            } else {
                "refused"
            }
        );
        assert_eq!(result["error"]["code"], code);
    }
    assert!(state.events.lock().unwrap().is_empty());
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_reverse_identity_or_duplicate_operation_faults_the_process() {
    for method in ["bad_session", "bad_epoch", "bad_frame", "duplicate"] {
        let rt = Runtime::new();
        let state = LogState::new(false);
        rt.install(log_plugin(state.clone())).unwrap();
        let provider = rt
            .install(fault_spec(Duration::from_secs(5)).plugin("peer", &probe()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (_, saved) = consumer(&rt, probe()).await;
        let handle = get(&saved);
        if method == "duplicate" {
            call(&handle, Endpoint::Ipc, "probe", Value::Null)
                .await
                .unwrap();
        }
        assert!(
            bounded(call(&handle, Endpoint::Ipc, method, Value::Null))
                .await
                .is_err()
        );
        assert!(bounded(provider.settled()).await.is_err());
        assert!(bounded(rt.shutdown()).await.is_err());
        let probes = state
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, event)| event == "probe")
            .count();
        assert_eq!(probes, usize::from(method == "duplicate"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reverse_call_retains_native_work_on_crash_timeout_and_premature_parent_response() {
    for mode in ["crash", "timeout", "early_return"] {
        let rt = Runtime::new();
        let state = LogState::new(true);
        rt.install(log_plugin(state.clone())).unwrap();
        let deadline = if mode == "timeout" {
            Duration::from_millis(300)
        } else {
            Duration::from_secs(5)
        };
        let provider = rt
            .install(fault_spec(deadline).plugin("peer", &probe()))
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let (client, saved) = consumer(&rt, probe()).await;
        let handle = get(&saved);
        let work =
            tokio::spawn(async move { call(&handle, Endpoint::Ipc, "block", Value::Null).await });
        bounded(state.entered.acquire()).await.unwrap().forget();
        let mut crashed = None;
        if mode == "crash" {
            let handle = get(&saved);
            crashed = Some(tokio::spawn(async move {
                call(&handle, Endpoint::Ipc, "crash", serde_json::json!({})).await
            }));
        }
        if mode == "early_return" {
            call(&get(&saved), Endpoint::Ipc, "early_return", Value::Null)
                .await
                .unwrap();
        }
        let mut disposing = tokio::spawn(async move { client.dispose().await });
        // The timeout case must pass the peer deadline while native work is still blocked.
        let delay = if mode == "timeout" {
            Duration::from_millis(400)
        } else {
            Duration::from_millis(40)
        };
        assert!(tokio::time::timeout(delay, &mut disposing).await.is_err());
        if mode != "early_return" {
            bounded(async {
                while provider.snapshot().state == Lifecycle::Active {
                    tokio::task::yield_now().await;
                }
            })
            .await;
        }
        assert!(
            !state
                .events
                .lock()
                .unwrap()
                .iter()
                .any(|(_, event)| event == "logger.closed" || event == "effect.released")
        );
        state.release.add_permits(1);
        let result = bounded(work).await.unwrap();
        assert_eq!(result.is_ok(), mode == "early_return");
        if let Some(crashed) = crashed {
            assert!(bounded(crashed).await.unwrap().is_err());
        }
        bounded(disposing).await.unwrap().unwrap();
        ordered(
            &state.events.lock().unwrap(),
            1,
            &["block", "effect.released"],
        );
        if mode == "early_return" {
            bounded(rt.shutdown()).await.unwrap();
        } else {
            assert!(bounded(provider.settled()).await.is_err());
            assert!(bounded(rt.shutdown()).await.is_err());
            assert!(
                !state
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(_, event)| event == "logger.closed")
            );
        }
    }
}

#[cfg(unix)]
#[path = "dependencies_cpp.rs"]
mod cpp;
