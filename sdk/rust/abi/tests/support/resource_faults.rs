use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Semaphore;

struct Control {
    reenter: Mutex<Option<rt::ServiceHandle<AbiService>>>,
    attempts: AtomicUsize,
    releases: AtomicUsize,
    deactivations: AtomicUsize,
    acquire_entered: Semaphore,
    acquire_finish: Semaphore,
    call_entered: Semaphore,
    call_finish: Semaphore,
    release_entered: Semaphore,
    release_finish: Semaphore,
}
impl Control {
    fn new() -> Self {
        Self {
            reenter: Mutex::new(None),
            attempts: AtomicUsize::new(0),
            releases: AtomicUsize::new(0),
            deactivations: AtomicUsize::new(0),
            acquire_entered: Semaphore::new(0),
            acquire_finish: Semaphore::new(0),
            call_entered: Semaphore::new(0),
            call_finish: Semaphore::new(0),
            release_entered: Semaphore::new(0),
            release_finish: Semaphore::new(0),
        }
    }
    async fn invoke_json(
        &self,
        ctx: rt::Context,
        method: String,
        args: Value,
    ) -> cordis_abi::Result<Value> {
        match method.as_str() {
            "acquire" => {
                assert!(!ctx.is_cleanup());
                self.attempts.fetch_add(1, Ordering::SeqCst);
                if args["wait_acquire"] == true {
                    self.acquire_entered.add_permits(1);
                    self.acquire_finish.acquire().await.unwrap().forget();
                }
            }
            "call" => {
                if args["reenter"] == true {
                    let handle = self.reenter.lock().unwrap().take().expect("reentry target");
                    return call(&handle, "reentered", Value::Null).await;
                }
                if args["wait_call"] == true {
                    self.call_entered.add_permits(1);
                    self.call_finish.acquire().await.unwrap().forget();
                }
            }
            "release" => {
                assert!(ctx.is_cleanup());
                self.releases.fetch_add(1, Ordering::SeqCst);
                if args["wait_release"] == true {
                    self.release_entered.add_permits(1);
                    self.release_finish.acquire().await.unwrap().forget();
                }
            }
            "deactivate" => {
                self.deactivations.fetch_add(1, Ordering::SeqCst);
            }
            "must-not-run" => panic!("expired resource Context reached dependency"),
            _ => {}
        }
        Ok(args)
    }
}
struct Fixture {
    runtime: Runtime,
    control: Arc<Control>,
    provider: rt::PluginHandle,
    owner: rt::PluginHandle,
    handle: rt::ServiceHandle<AbiService>,
    reverse: bool,
}
#[derive(Clone)]
enum Lease {
    Export(Box<AbiResource>),
    Reverse(Box<rt::ServiceHandle<AbiService>>),
}
impl Lease {
    async fn invoke_json(&self, method: &'static str) -> cordis_abi::Result<Value> {
        match self {
            Self::Export(r) => r.invoke_json(method, Value::Null).await,
            Self::Reverse(h) => call(h, method, Value::Null).await,
        }
    }
    async fn dispose(&self) -> cordis_abi::Result<()> {
        match self {
            Self::Export(r) => r.dispose().await,
            Self::Reverse(h) => call(h, "dispose", Value::Null).await.map(|_| ()),
        }
    }
}
async fn call(
    h: &rt::ServiceHandle<AbiService>,
    method: &'static str,
    args: Value,
) -> cordis_abi::Result<Value> {
    h.call(move |s, ctx| Box::pin(async move { Ok(s.invoke_json(&ctx, method, args).await) }))
        .await
        .map_err(|e| cordis_abi::Error::Runtime(e.to_string()))?
}
async fn acquire(
    h: &rt::ServiceHandle<AbiService>,
    reverse: bool,
    args: Value,
) -> cordis_abi::Result<Lease> {
    if reverse {
        call(h, "acquire", args).await?;
        Ok(Lease::Reverse(Box::new(h.clone())))
    } else {
        h.call(move |s, ctx| {
            Box::pin(async move { Ok(s.acquire_json(&ctx, "counter", args).await) })
        })
        .await
        .map_err(|e| cordis_abi::Error::Runtime(e.to_string()))?
        .map(|resource| Lease::Export(Box::new(resource)))
    }
}
impl Fixture {
    async fn new(reverse: bool, cpp: bool) -> Self {
        let runtime = Runtime::new();
        let control = Arc::new(Control::new());
        let logger = Service::<Control>::new("example.logger", "1");
        let binding = logger.clone();
        let records = control.clone();
        runtime
            .install(
                Plugin::new("logger", move |ctx| {
                    let binding = binding.clone();
                    let records = records.clone();
                    async move { ctx.provide(&binding, records) }
                })
                .provides(&logger),
            )
            .unwrap();
        let counters = Service::<AbiService>::new("example.counters", "1").with_descriptor(
            cordis_sdk_core::Descriptor::new(cordis_sdk_core::Interface::value_dispatch(true))
                .unwrap(),
        );
        let lib = library(
            "abi_resource_probe",
            Capabilities {
                dependencies: vec![Dependency::new_json(
                    &logger,
                    cordis_sdk_core::Descriptor::new(cordis_sdk_core::Interface::value_dispatch(
                        true,
                    ))
                    .unwrap(),
                    |control, ctx, method, args| Box::pin(control.invoke_json(ctx, method, args)),
                )],
                ..Default::default()
            },
        );
        let provider = runtime
            .install(lib.plugin("probe", &counters, Value::Null).unwrap())
            .unwrap();
        bounded(provider.ready()).await.unwrap();
        let service = if reverse {
            let client = Service::<AbiService>::new("example.counter-client", "1").with_descriptor(
                cordis_sdk_core::Descriptor::new(cordis_sdk_core::Interface::value_dispatch(true))
                    .unwrap(),
            );
            let lib = library(
                if cpp {
                    "cordis_resource_probe_client"
                } else {
                    "abi_resource_probe_client"
                },
                Capabilities {
                    dependencies: vec![Dependency::abi(&counters)],
                    ..Default::default()
                },
            );
            let plugin = runtime
                .install(lib.plugin("client", &client, Value::Null).unwrap())
                .unwrap();
            bounded(plugin.ready()).await.unwrap();
            client
        } else {
            counters
        };
        let (owner, handle) = owner(&runtime, &service).await;
        Self {
            runtime,
            control,
            provider,
            owner,
            handle,
            reverse,
        }
    }
    async fn acquire(&self, args: Value) -> cordis_abi::Result<Lease> {
        bounded(acquire(&self.handle, self.reverse, args)).await
    }
}
async fn wait(s: &Semaphore) {
    bounded(s.acquire()).await.unwrap().forget();
}
async fn unloading(h: &rt::PluginHandle) {
    bounded(async {
        while h.snapshot().state != cordis_kernel::Lifecycle::Unloading {
            tokio::task::yield_now().await;
        }
    })
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn release_keeps_its_capacity_when_all_business_slots_are_busy() {
    let f = Fixture::new(false, false).await;
    let quiet = f.acquire(Value::Null).await.unwrap();
    let busy = f.acquire(json!({"wait_call":true})).await.unwrap();
    let mut calls = Vec::new();
    for _ in 0..16 {
        let resource = busy.clone();
        calls.push(tokio::spawn(
            async move { resource.invoke_json("get").await },
        ));
    }
    for _ in 0..16 {
        wait(&f.control.call_entered).await;
    }
    bounded(quiet.dispose()).await.unwrap();
    assert_eq!(f.control.releases.load(Ordering::SeqCst), 1);
    f.control.call_finish.add_permits(16);
    for call in calls {
        bounded(call).await.unwrap().unwrap();
    }
    bounded(f.runtime.shutdown()).await.unwrap();
    assert_eq!(f.control.releases.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_export_and_reverse_acquisitions_remain_owned_after_waiter_cancellation() {
    for (reverse, cpp) in [(false, false), (true, false), (true, true)] {
        let f = Fixture::new(reverse, cpp).await;
        let h = f.handle.clone();
        let acquiring =
            tokio::spawn(async move { acquire(&h, reverse, json!({"wait_acquire":true})).await });
        wait(&f.control.acquire_entered).await;
        acquiring.abort();
        let _ = acquiring.await;
        let owner = f.owner.clone();
        let stopping = tokio::spawn(async move { owner.dispose().await });
        unloading(&f.owner).await;
        assert!(!stopping.is_finished());
        assert_eq!(f.control.releases.load(Ordering::SeqCst), 0);
        f.control.acquire_finish.add_permits(1);
        bounded(stopping).await.unwrap().unwrap();
        assert_eq!(f.control.releases.load(Ordering::SeqCst), 1);
        bounded(f.runtime.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resource_disposal_drains_calls_and_caches_one_close_across_cancelled_waits() {
    for (reverse, cpp) in [(false, false), (true, false), (true, true)] {
        let f = Fixture::new(reverse, cpp).await;
        let resource = f
            .acquire(json!({"wait_call":true,"wait_release":true}))
            .await
            .unwrap();
        assert_eq!(resource.invoke_json("expired").await.unwrap(), json!(true));
        let running = tokio::spawn({
            let r = resource.clone();
            async move { r.invoke_json("get").await }
        });
        wait(&f.control.call_entered).await;
        running.abort();
        let _ = running.await;
        let first = tokio::spawn({
            let r = resource.clone();
            async move { r.dispose().await }
        });
        // Releasing cannot reach business close while the admitted method waits.
        tokio::task::yield_now().await;
        assert_eq!(f.control.releases.load(Ordering::SeqCst), 0);
        f.control.call_finish.add_permits(1);
        wait(&f.control.release_entered).await;
        first.abort();
        let _ = first.await;
        assert!(resource.invoke_json("get").await.is_err());
        let second = tokio::spawn({
            let r = resource.clone();
            async move { r.dispose().await }
        });
        f.control.release_finish.add_permits(1);
        bounded(second).await.unwrap().unwrap();
        resource.dispose().await.unwrap();
        bounded(f.runtime.shutdown()).await.unwrap();
        assert_eq!(f.control.releases.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_or_panicking_release_keeps_responsibility_without_retry() {
    for (reverse, cpp) in [(false, false), (true, false), (true, true)] {
        for failure in ["fail_release", "panic_release"] {
            let f = Fixture::new(reverse, cpp).await;
            let resource = f.acquire(json!({failure:true})).await.unwrap();
            assert!(bounded(resource.dispose()).await.is_err());
            assert!(bounded(resource.dispose()).await.is_err());
            assert!(bounded(f.runtime.shutdown()).await.is_err());
            assert_eq!(f.control.releases.load(Ordering::SeqCst), 1);
            assert_eq!(f.control.deactivations.load(Ordering::SeqCst), 0);
            assert_eq!(
                f.provider.snapshot().state,
                cordis_kernel::Lifecycle::Unloading
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_or_panicking_acquisition_cannot_be_reported_as_recovered() {
    for (reverse, cpp) in [(false, false), (true, false), (true, true)] {
        for failure in ["unknown", "panic_acquire"] {
            let f = Fixture::new(reverse, cpp).await;
            assert!(f.acquire(json!({failure:true})).await.is_err());
            assert!(bounded(f.runtime.shutdown()).await.is_err());
            assert!(
                f.owner
                    .effects()
                    .iter()
                    .any(|e| matches!(e.status, rt::EffectStatus::Failed(_)))
            );
            assert_eq!(f.control.releases.load(Ordering::SeqCst), 0);
            assert_eq!(f.control.deactivations.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejected_acquisition_has_no_surviving_resource_and_instance_remains_usable() {
    for (reverse, cpp) in [(false, false), (true, false), (true, true)] {
        let f = Fixture::new(reverse, cpp).await;
        assert!(f.acquire(json!({"reject":true})).await.is_err());
        let resource = f.acquire(Value::Null).await.unwrap();
        resource.dispose().await.unwrap();
        bounded(f.runtime.shutdown()).await.unwrap();
        assert_eq!(f.control.attempts.load(Ordering::SeqCst), 2);
        assert_eq!(f.control.releases.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reverse_resource_quota_rejects_before_executing_another_factory() {
    for cpp in [false, true] {
        let f = Fixture::new(true, cpp).await;
        for _ in 0..4096 {
            f.acquire(Value::Null).await.unwrap();
        }
        assert!(f.acquire(Value::Null).await.is_err());
        assert_eq!(f.control.attempts.load(Ordering::SeqCst), 4096);
        bounded(f.runtime.shutdown()).await.unwrap();
        assert_eq!(f.control.releases.load(Ordering::SeqCst), 4096);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resource_method_can_reenter_the_same_library_through_its_native_dependency() {
    for (reverse, cpp) in [(false, false), (true, false), (true, true)] {
        let f = Fixture::new(reverse, cpp).await;
        let (_, handle) = owner(
            &f.runtime,
            &Service::<AbiService>::new("example.counters", "1").with_descriptor(
                cordis_sdk_core::Descriptor::new(cordis_sdk_core::Interface::value_dispatch(true))
                    .unwrap(),
            ),
        )
        .await;
        *f.control.reenter.lock().unwrap() = Some(handle);
        let resource = f.acquire(json!({"reenter":true})).await.unwrap();
        assert_eq!(
            bounded(resource.invoke_json("get")).await.unwrap().as_f64(),
            Some(42.0)
        );
        bounded(f.runtime.shutdown()).await.unwrap();
        assert_eq!(f.control.releases.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_recovers_exported_resource_before_destroying_instance_and_expires_handles() {
    let f = Fixture::new(false, false).await;
    let resource = f.acquire(Value::Null).await.unwrap();
    bounded(f.provider.restart()).await.unwrap();
    assert_eq!(f.control.releases.load(Ordering::SeqCst), 1);
    assert_eq!(f.control.deactivations.load(Ordering::SeqCst), 1);
    assert!(bounded(resource.invoke_json("get")).await.is_err());
    bounded(f.runtime.shutdown()).await.unwrap();
}
