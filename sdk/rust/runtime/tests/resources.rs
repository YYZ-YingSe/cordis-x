use cordis_runtime as rt;
use cordis_sdk_core::{self as core, *};
use cordis_sdk_runtime::{Capabilities, Dependency, Resource as NativeResource, Service};
use std::{
    cell::RefCell,
    future::Future,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("resource did not settle")
}
async fn wait(semaphore: &Semaphore) {
    bounded(semaphore.acquire()).await.unwrap().forget();
}
fn contract(name: &str) -> Contract {
    Contract {
        name: name.into(),
        revision: "1".into(),
    }
}
fn native(e: Failure) -> rt::Error {
    rt::Error::Message(e.to_string())
}

struct Control {
    log: Mutex<Vec<String>>,
    attempts: AtomicUsize,
    releases: AtomicUsize,
    acquiring: Semaphore,
    acquired: Semaphore,
    calling: Semaphore,
    called: Semaphore,
    releasing: Semaphore,
    released: Semaphore,
}
impl Control {
    fn new() -> Self {
        Self {
            log: Mutex::default(),
            attempts: AtomicUsize::new(0),
            releases: AtomicUsize::new(0),
            acquiring: Semaphore::new(0),
            acquired: Semaphore::new(0),
            calling: Semaphore::new(0),
            called: Semaphore::new(0),
            releasing: Semaphore::new(0),
            released: Semaphore::new(0),
        }
    }
    async fn invoke(&self, ctx: rt::Context, method: String, args: Value) -> Result<Value> {
        self.log.lock().unwrap().push(method.clone());
        match method.as_str() {
            "acquire" => {
                assert!(!ctx.is_cleanup());
                self.attempts.fetch_add(1, Ordering::SeqCst);
                if args["wait_acquire"] == true {
                    self.acquiring.add_permits(1);
                    self.acquired.acquire().await.unwrap().forget();
                }
            }
            "call" if args["wait_call"] == true => {
                self.calling.add_permits(1);
                self.called.acquire().await.unwrap().forget();
            }
            "release" => {
                assert!(ctx.is_cleanup());
                self.releases.fetch_add(1, Ordering::SeqCst);
                if args["wait_release"] == true {
                    self.releasing.add_permits(1);
                    self.released.acquire().await.unwrap().forget();
                }
                if args["fail_release"] == true {
                    return Err(error("close_failed", "retain responsibility"));
                }
            }
            _ => {}
        }
        Ok(args)
    }
}

struct Probe;
struct Object {
    args: Value,
    expired: Context,
}
impl Plugin for Probe {
    fn dependency_interfaces() -> std::collections::BTreeMap<String, Interface> {
        [("control".into(), Interface::value_dispatch(true))].into()
    }
    fn interface() -> cordis_sdk_core::Interface {
        cordis_sdk_core::Interface::value_dispatch(true)
    }
    fn manifest() -> PluginManifest {
        PluginManifest {
            provides: contract("probe"),
            requires: vec![contract("control")],
        }
    }
    fn create(_: Value) -> Result<Self> {
        Ok(Self)
    }
    fn activate(self: Rc<Self>, ctx: Context) -> cordis_sdk_core::LocalFuture<()> {
        Box::pin(async move {
            self.call_probe(ctx, "activate".into(), Value::Null)
                .await
                .map(|_| ())
        })
    }
    fn deactivate(self: Rc<Self>, ctx: Context) -> cordis_sdk_core::LocalFuture<()> {
        Box::pin(async move {
            self.call_probe(ctx, "deactivate".into(), Value::Null)
                .await
                .map(|_| ())
        })
    }
    fn invoke(
        self: Rc<Self>,
        ctx: Context,
        method: String,
        args: cordis_sdk_core::Message,
    ) -> cordis_sdk_core::message::FutureResult {
        Box::pin(async move {
            let interface = Self::interface();
            let signature = cordis_sdk_core::contract::method(&interface.methods, &method)
                .map_err(|e| cordis_sdk_core::refused("unknown_method", e))?;
            let args = test_json::input(args)?;
            let output = self
                .call_probe(
                    ctx,
                    "invoke".into(),
                    cordis_sdk_core::json!({"method":method,"args":args}),
                )
                .await?;
            cordis_sdk_core::Message::from_json(signature.result.clone(), &output)
                .map_err(cordis_sdk_core::typed::violation)
        })
    }
    fn acquire(
        self: Rc<Self>,
        ctx: Context,
        kind: String,
        args: cordis_sdk_core::Message,
    ) -> LocalFuture<Rc<dyn ResourceObject>> {
        Box::pin(async move {
            let args = test_json::input(args)?;
            test_json::invoke(&ctx, "control", "acquire", args.clone()).await?;
            match kind.as_str() {
                "rejected" => return Err(error("rejected_factory", "nothing acquired")),
                "unknown" => {
                    return Err(Failure::Unknown(ErrorInfo {
                        code: "unknown_factory".into(),
                        message: "uncertain acquisition".into(),
                    }));
                }
                "panic" => panic!("uncertain acquisition"),
                _ => {}
            }
            Ok(Rc::new(Object { args, expired: ctx }) as Rc<dyn ResourceObject>)
        })
    }
}
impl Probe {
    fn call_probe(self: Rc<Self>, ctx: Context, command: String, _: Value) -> FutureResult {
        Box::pin(async move { test_json::invoke(&ctx, "control", &command, Value::Null).await })
    }
}

impl ResourceObject for Object {
    fn invoke(
        self: Rc<Self>,
        ctx: Context,
        method: String,
        args: cordis_sdk_core::Message,
    ) -> cordis_sdk_core::message::FutureResult {
        Box::pin(async move {
            let output = self
                .invoke_probe(ctx, method, test_json::input(args)?)
                .await?;
            test_json::message(output)
        })
    }
    fn close(self: Rc<Self>, ctx: Context) -> LocalFuture<()> {
        Box::pin(async move {
            test_json::invoke(&ctx, "control", "release", self.args.clone()).await?;
            Ok(())
        })
    }
}
impl Object {
    fn invoke_probe(self: Rc<Self>, ctx: Context, method: String, _: Value) -> FutureResult {
        Box::pin(async move {
            if method == "expired" {
                return Ok(json!(
                    test_json::invoke(&self.expired, "control", "must-not-run", Value::Null)
                        .await
                        .is_err()
                ));
            }
            test_json::invoke(&ctx, "control", "call", self.args.clone()).await
        })
    }
}

struct Fixture {
    runtime: rt::Runtime,
    control: Arc<Control>,
    provider: rt::PluginHandle,
    owner: rt::PluginHandle,
    handle: rt::ServiceHandle<Service>,
}
async fn owner(
    runtime: &rt::Runtime,
    service: &rt::Service<Service>,
) -> (rt::PluginHandle, rt::ServiceHandle<Service>) {
    let saved = Arc::new(Mutex::new(None));
    let target = saved.clone();
    let service = service.clone();
    let binding = service.clone();
    let owner = runtime
        .install(
            rt::Plugin::new("owner", move |ctx| {
                let service = binding.clone();
                let target = target.clone();
                async move {
                    *target.lock().unwrap() = Some(ctx.dependency(&service)?);
                    Ok(())
                }
            })
            .requires(&service),
        )
        .unwrap();
    bounded(owner.ready()).await.unwrap();
    let handle = saved.lock().unwrap().take().unwrap();
    (owner, handle)
}
impl Fixture {
    async fn new() -> Self {
        let runtime = rt::Runtime::new();
        let control = Arc::new(Control::new());
        let logger = rt::Service::<Control>::new("control", "1");
        let binding = logger.clone();
        let state = control.clone();
        runtime
            .install(
                rt::Plugin::new("control", move |ctx| {
                    let binding = binding.clone();
                    let state = state.clone();
                    async move { ctx.provide(&binding, state) }
                })
                .provides(&logger),
            )
            .unwrap();
        let service = rt::Service::<Service>::new("probe", "1")
            .with_descriptor(Descriptor::new(Probe::interface()).unwrap());
        let provider = runtime
            .install(
                cordis_sdk_runtime::plugin::<Probe>(
                    "probe",
                    &service,
                    Value::Null,
                    Capabilities {
                        dependencies: vec![Dependency::new(
                            &logger,
                            Descriptor::new(Interface::value_dispatch(true)).unwrap(),
                            |control, ctx, method, args| {
                                Box::pin(async move {
                                    test_json::message(
                                        control
                                            .invoke(ctx, method, test_json::input(args)?)
                                            .await?,
                                    )
                                })
                            },
                        )],
                        ..Default::default()
                    },
                )
                .unwrap(),
            )
            .unwrap();
        let (owner, handle) = owner(&runtime, &service).await;
        Self {
            runtime,
            control,
            provider,
            owner,
            handle,
        }
    }
    async fn acquire(&self, kind: &str, args: Value) -> Result<NativeResource> {
        acquire(&self.handle, kind, args).await
    }
}
async fn acquire(
    handle: &rt::ServiceHandle<Service>,
    kind: &str,
    args: Value,
) -> Result<NativeResource> {
    let kind = kind.to_owned();
    bounded(handle.call(move |service, ctx| {
        Box::pin(async move { Ok(service.acquire_json(&ctx, &kind, args).await) })
    }))
    .await
    .unwrap()
}
async fn unloading(handle: &rt::PluginHandle) {
    bounded(async {
        while handle.snapshot().state != cordis_kernel::Lifecycle::Unloading {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resources_belong_to_caller_and_restart_invalidates_old_handles() {
    let f = Fixture::new().await;
    let manual = f.acquire("counter", json!({"value":42.0})).await.unwrap();
    assert_eq!(
        manual
            .invoke_json("get", cordis_sdk_core::json!({}))
            .await
            .unwrap(),
        json!({"value":42.0})
    );
    assert_eq!(
        manual.invoke_json("expired", Value::Null).await.unwrap(),
        json!(true)
    );
    manual.dispose().await.unwrap();
    manual.dispose().await.unwrap();
    assert!(
        manual
            .invoke_json("get", cordis_sdk_core::json!({}))
            .await
            .is_err()
    );
    let automatic = f.acquire("counter", Value::Null).await.unwrap();
    assert!(
        f.owner
            .effects()
            .iter()
            .any(|e| e.label == "portable resource: counter")
    );
    assert!(
        !f.provider
            .effects()
            .iter()
            .any(|e| e.label == "portable resource: counter")
    );
    bounded(f.provider.restart()).await.unwrap();
    assert!(
        automatic
            .invoke_json("get", cordis_sdk_core::json!({}))
            .await
            .is_err()
    );
    assert_eq!(f.control.releases.load(Ordering::SeqCst), 2);
    let log = f.control.log.lock().unwrap().clone();
    assert!(
        log.iter().rposition(|v| v == "release").unwrap()
            < log.iter().position(|v| v == "deactivate").unwrap()
    );
    bounded(f.runtime.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_acquisition_wait_keeps_late_resource_owned_during_shutdown() {
    let f = Fixture::new().await;
    let handle = f.handle.clone();
    let acquiring =
        tokio::spawn(
            async move { acquire(&handle, "counter", json!({"wait_acquire":true})).await },
        );
    wait(&f.control.acquiring).await;
    acquiring.abort();
    let _ = acquiring.await;
    let owner = f.owner.clone();
    let disposing = tokio::spawn(async move { owner.dispose().await });
    unloading(&f.owner).await;
    assert!(!disposing.is_finished());
    assert_eq!(f.control.releases.load(Ordering::SeqCst), 0);
    f.control.acquired.add_permits(1);
    bounded(disposing).await.unwrap().unwrap();
    assert_eq!(f.control.releases.load(Ordering::SeqCst), 1);
    bounded(f.runtime.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disposal_drains_cancelled_calls_and_concurrent_waiters_share_one_close() {
    let f = Fixture::new().await;
    let resource = f
        .acquire("counter", json!({"wait_call":true,"wait_release":true}))
        .await
        .unwrap();
    let calling = tokio::spawn({
        let resource = resource.clone();
        async move {
            resource
                .invoke_json("get", cordis_sdk_core::json!({}))
                .await
        }
    });
    wait(&f.control.calling).await;
    calling.abort();
    let _ = calling.await;
    let first = tokio::spawn({
        let resource = resource.clone();
        async move { resource.dispose().await }
    });
    bounded(async {
        while resource.snapshot().status != rt::EffectStatus::Releasing {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(f.control.releases.load(Ordering::SeqCst), 0);
    assert!(!first.is_finished());
    f.control.called.add_permits(1);
    wait(&f.control.releasing).await;
    first.abort();
    let _ = first.await;
    let second = tokio::spawn({
        let resource = resource.clone();
        async move { resource.dispose().await }
    });
    assert!(
        resource
            .invoke_json("get", cordis_sdk_core::json!({}))
            .await
            .is_err()
    );
    f.control.released.add_permits(1);
    bounded(second).await.unwrap().unwrap();
    resource.dispose().await.unwrap();
    bounded(f.runtime.shutdown()).await.unwrap();
    assert_eq!(f.control.releases.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_close_is_cached_and_blocks_instance_and_dependency_cleanup() {
    let f = Fixture::new().await;
    let resource = f
        .acquire("counter", json!({"fail_release":true}))
        .await
        .unwrap();
    let failure = resource.dispose().await.unwrap_err();
    assert_eq!(resource.dispose().await.unwrap_err(), failure);
    assert!(
        resource
            .invoke_json("get", cordis_sdk_core::json!({}))
            .await
            .is_err()
    );
    assert!(bounded(f.runtime.shutdown()).await.is_err());
    assert_eq!(f.control.releases.load(Ordering::SeqCst), 1);
    assert!(matches!(
        resource.snapshot().status,
        rt::EffectStatus::Failed(_)
    ));
    assert_eq!(
        f.provider.snapshot().state,
        cordis_kernel::Lifecycle::Unloading
    );
    assert!(
        !f.control
            .log
            .lock()
            .unwrap()
            .iter()
            .any(|v| v == "deactivate")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_or_panicked_acquisition_keeps_an_observable_failed_ticket() {
    for kind in ["unknown", "panic"] {
        let f = Fixture::new().await;
        assert!(matches!(
            f.acquire(kind, Value::Null).await,
            Err(Failure::Unknown(_))
        ));
        assert!(bounded(f.runtime.shutdown()).await.is_err());
        assert!(
            f.owner
                .effects()
                .iter()
                .any(|e| matches!(e.status, rt::EffectStatus::Failed(_)))
        );
        assert!(
            !f.control
                .log
                .lock()
                .unwrap()
                .iter()
                .any(|v| v == "deactivate")
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capacity_is_reserved_before_factory_and_rejected_acquisitions_release_slots() {
    let f = Fixture::new().await;
    for _ in 0..3 {
        assert!(matches!(
            f.acquire("rejected", Value::Null).await,
            Err(Failure::Rejected(_))
        ));
    }
    let mut resources = Vec::new();
    for _ in 0..4096 {
        resources.push(f.acquire("counter", Value::Null).await.unwrap());
    }
    let attempts = f.control.attempts.load(Ordering::SeqCst);
    assert!(
        matches!(f.acquire("counter", Value::Null).await, Err(Failure::Refused(info)) if info.code == "overloaded")
    );
    assert_eq!(f.control.attempts.load(Ordering::SeqCst), attempts);
    resources.pop().unwrap().dispose().await.unwrap();
    f.acquire("counter", Value::Null).await.unwrap();
    bounded(f.runtime.shutdown()).await.unwrap();
    assert_eq!(f.control.releases.load(Ordering::SeqCst), 4097);
}

// Exercise the public handle through the adapter, including callback expiry
// and a resource whose owner is a child Scope rather than the plugin instance.
struct ScopeClient(RefCell<Option<(core::Resource, Context)>>);
impl Plugin for ScopeClient {
    fn dependency_interfaces() -> std::collections::BTreeMap<String, Interface> {
        [("probe".into(), Probe::interface())].into()
    }
    fn interface() -> cordis_sdk_core::Interface {
        cordis_sdk_core::Interface::value_dispatch(true)
    }
    fn manifest() -> PluginManifest {
        PluginManifest {
            provides: contract("scope-client"),
            requires: vec![contract("probe")],
        }
    }
    fn create(_: Value) -> Result<Self> {
        Ok(Self(RefCell::default()))
    }
    fn activate(self: Rc<Self>, ctx: Context) -> cordis_sdk_core::LocalFuture<()> {
        Box::pin(async move {
            self.call_probe(ctx, "activate".into(), Value::Null)
                .await
                .map(|_| ())
        })
    }
    fn deactivate(self: Rc<Self>, ctx: Context) -> cordis_sdk_core::LocalFuture<()> {
        Box::pin(async move {
            self.call_probe(ctx, "deactivate".into(), Value::Null)
                .await
                .map(|_| ())
        })
    }
    fn invoke(
        self: Rc<Self>,
        ctx: Context,
        method: String,
        args: cordis_sdk_core::Message,
    ) -> cordis_sdk_core::message::FutureResult {
        Box::pin(async move {
            let interface = Self::interface();
            let signature = cordis_sdk_core::contract::method(&interface.methods, &method)
                .map_err(|e| cordis_sdk_core::refused("unknown_method", e))?;
            let args = test_json::input(args)?;
            let output = self
                .call_probe(
                    ctx,
                    "invoke".into(),
                    cordis_sdk_core::json!({"method":method,"args":args}),
                )
                .await?;
            cordis_sdk_core::Message::from_json(signature.result.clone(), &output)
                .map_err(cordis_sdk_core::typed::violation)
        })
    }
}
impl ScopeClient {
    fn call_probe(self: Rc<Self>, ctx: Context, command: String, _: Value) -> FutureResult {
        Box::pin(async move {
            if command == "activate" || command == "deactivate" {
                return Ok(Value::Null);
            }
            let saved = self.0.borrow().clone();
            if let Some((resource, old)) = saved {
                assert!(
                    test_json::resource(&resource, &old, "get", cordis_sdk_core::json!({}))
                        .await
                        .is_err()
                );
                assert!(
                    test_json::resource(&resource, &ctx, "get", cordis_sdk_core::json!({}))
                        .await
                        .is_err()
                );
                assert!(
                    test_json::acquire(&old, "probe", "counter", Value::Null)
                        .await
                        .is_err()
                );
                return Ok(json!("closed"));
            }
            let scope = ctx.scope("resource owner").await?;
            let this = self.clone();
            scope
                .run(&ctx, move |child| {
                    let this = this.clone();
                    async move {
                        let resource =
                            test_json::acquire(&child, "probe", "counter", json!({"nested":true}))
                                .await?;
                        assert_eq!(
                            test_json::resource(
                                &resource,
                                &child,
                                "get",
                                cordis_sdk_core::json!({})
                            )
                            .await?,
                            json!({"nested":true})
                        );
                        *this.0.borrow_mut() = Some((resource, child.clone()));
                        child
                            .defer("cleanup rejects new resource", |cleanup| async move {
                                assert!(
                                    test_json::acquire(&cleanup, "probe", "counter", Value::Null)
                                        .await
                                        .is_err()
                                );
                                Ok(())
                            })
                            .await?;
                        Ok(Value::Null)
                    }
                })
                .await?;
            let (resource, expired) = self.0.borrow().clone().unwrap();
            assert!(
                test_json::resource(&resource, &expired, "get", cordis_sdk_core::json!({}))
                    .await
                    .is_err()
            );
            assert_eq!(
                test_json::resource(&resource, &ctx, "get", cordis_sdk_core::json!({})).await?,
                json!({"nested":true})
            );
            scope.dispose(&ctx).await?;
            Ok(json!("released"))
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_counter_example_keeps_the_original_caller_as_resource_owner() {
    use cordis_portable_example::resources::{CounterClient, CounterPlugin};
    let runtime = rt::Runtime::new();
    let logs = Arc::new(Mutex::new(Vec::<Value>::new()));
    let logger = rt::Service::<Mutex<Vec<Value>>>::new("example.counter-log", "1");
    let binding = logger.clone();
    let records = logs.clone();
    runtime
        .install(
            rt::Plugin::new("logger", move |ctx| {
                let binding = binding.clone();
                let records = records.clone();
                async move { ctx.provide(&binding, records) }
            })
            .provides(&logger),
        )
        .unwrap();
    let counters = rt::Service::<Service>::new("example.counters", "1")
        .with_descriptor(Descriptor::new(CounterPlugin::interface()).unwrap());
    runtime
        .install(
            cordis_sdk_runtime::plugin::<CounterPlugin>(
                "counters",
                &counters,
                json!({}),
                Capabilities {
                    dependencies: vec![Dependency::new(
                        &logger,
                        Descriptor::new(cordis_portable_example::resources::generated::CounterLogClient::interface()).unwrap(),
                        |logs, _, _, value| {
                            Box::pin(async move {
                                logs.lock().unwrap().push(test_json::input(value)?);
                                cordis_sdk_core::Message::from_json(
                                    cordis_portable_example::resources::generated::CounterLogClient::interface().methods.get("write").unwrap().result.clone(), &json!({})
                                ).map_err(cordis_sdk_core::typed::violation)
                            })
                        },
                    )],
                    ..Default::default()
                },
            )
            .unwrap(),
        )
        .unwrap();
    let service = rt::Service::<Service>::new("example.counter-client", "1");
    runtime
        .install(
            cordis_sdk_runtime::plugin::<CounterClient>(
                "client",
                &service,
                json!({}),
                Capabilities {
                    dependencies: vec![Dependency::portable(&counters).unwrap()],
                    ..Default::default()
                },
            )
            .unwrap(),
        )
        .unwrap();
    let (first, a) = owner(&runtime, &service).await;
    let (_, b) = owner(&runtime, &service).await;
    async fn invoke(
        handle: &rt::ServiceHandle<Service>,
        method: &'static str,
        args: Value,
    ) -> Result<Value> {
        bounded(handle.call(move |s, ctx| {
            Box::pin(async move { Ok(s.invoke_json(&ctx, method, args).await) })
        }))
        .await
        .unwrap()
    }
    invoke(&a, "acquire", json!(40)).await.unwrap();
    assert_eq!(invoke(&b, "add", json!(2)).await.unwrap(), json!("42"));
    bounded(first.dispose()).await.unwrap();
    assert_eq!(*logs.lock().unwrap(), vec![json!({"counter_closed":"42"})]);
    assert!(invoke(&b, "get", cordis_sdk_core::json!({})).await.is_err());
    invoke(&b, "acquire", json!(7)).await.unwrap();
    invoke(&b, "release", json!({})).await.unwrap();
    invoke(&b, "release", json!({})).await.unwrap();
    bounded(runtime.shutdown()).await.unwrap();
    assert_eq!(
        *logs.lock().unwrap(),
        vec![
            json!({"counter_closed":"42"}),
            json!({"counter_closed":"7"})
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_resource_handles_respect_child_scope_and_callback_authority() {
    let f = Fixture::new().await;
    let service = rt::Service::<Service>::new("scope-client", "1");
    f.runtime
        .install(
            cordis_sdk_runtime::plugin::<ScopeClient>(
                "scope-client",
                &service,
                Value::Null,
                Capabilities {
                    dependencies: vec![
                        Dependency::portable(
                            &rt::Service::new("probe", "1")
                                .with_descriptor(Descriptor::new(Probe::interface()).unwrap()),
                        )
                        .unwrap(),
                    ],
                    ..Default::default()
                },
            )
            .unwrap(),
        )
        .unwrap();
    let (_, handle) = owner(&f.runtime, &service).await;
    for expected in ["released", "closed"] {
        let value = bounded(handle.call(|s, ctx| {
            Box::pin(async move {
                s.invoke_json(&ctx, "run", Value::Null)
                    .await
                    .map_err(native)
            })
        }))
        .await
        .unwrap();
        assert_eq!(value, json!(expected));
    }
    assert_eq!(f.control.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(f.control.releases.load(Ordering::SeqCst), 1);
    bounded(f.runtime.shutdown()).await.unwrap();
}

#[path = "../../tests/support/json.rs"]
#[allow(dead_code)]
mod test_json;
