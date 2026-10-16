use super::*;
use cordis_sdk_core::{Context, FutureResult, LocalFuture, PluginManifest, ResourceObject};
use std::{rc::Rc, sync::Mutex, time::Duration};

struct Version(Value);
impl Plugin for Version {
    fn interface() -> cordis_sdk_core::Interface {
        cordis_sdk_core::Interface::value_dispatch(true)
    }
    fn manifest() -> PluginManifest {
        PluginManifest {
            provides: cordis_sdk_core::Contract {
                name: "version".into(),
                revision: "1".into(),
            },
            requires: vec![],
        }
    }
    fn create(value: Value) -> Result<Self> {
        Ok(Self(value))
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
        _: Context,
        _: String,
        _: cordis_sdk_core::Message,
    ) -> LocalFuture<Rc<dyn ResourceObject>> {
        Box::pin(async move { Ok(Rc::new(Self(self.0.clone())) as Rc<dyn ResourceObject>) })
    }
}
impl Version {
    fn call_probe(self: Rc<Self>, _: Context, _: String, _: Value) -> FutureResult {
        Box::pin(async { Ok(Value::Null) })
    }
}

impl ResourceObject for Version {
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
    fn close(self: Rc<Self>, _: Context) -> LocalFuture<()> {
        Box::pin(async { Ok(()) })
    }
}
impl Version {
    fn invoke_probe(self: Rc<Self>, _: Context, _: String, _: Value) -> FutureResult {
        Box::pin(async move { Ok(self.0.clone()) })
    }
}

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("Broker did not settle")
}

async fn candidate(
    runtime: &rt::Runtime,
    broker: &rt::Service<rt::Broker<Service>>,
    version: i32,
) -> (rt::PluginHandle, rt::BrokerRegistration<Service>) {
    let saved = Arc::new(Mutex::new(None));
    let target = saved.clone();
    let binding = broker.clone();
    let candidate = runtime
        .install(
            rt::Plugin::new(format!("v{version}"), move |ctx| {
                let binding = binding.clone();
                let target = target.clone();
                async move {
                    // Install the same local executor under a real Broker candidate's
                    // Context, so resource tickets must retain its dynamic lease.
                    let client =
                        Client::start::<Version>(json!(version), Arc::new(Capabilities::default()))
                            .await
                            .map_err(native_error)?;
                    let cleanup = client.clone();
                    ctx.defer("portable instance", move |ctx| async move {
                        cleanup.lifecycle(&ctx, false).await.map_err(native_error)?;
                        cleanup.close().await.map_err(native_error)
                    })?;
                    client.lifecycle(&ctx, true).await.map_err(native_error)?;
                    *target.lock().unwrap() =
                        Some(ctx.dependency(&binding)?.register(Arc::new(Service {
                            client,
                            descriptor: Arc::new(
                                cordis_contract::Descriptor::new(Version::interface()).unwrap(),
                            ),
                        }))?);
                    Ok(())
                }
            })
            .requires(broker),
        )
        .unwrap();
    bounded(candidate.ready()).await.unwrap();
    let registration = saved.lock().unwrap().take().unwrap();
    (candidate, registration)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_switch_keeps_old_portable_resources_and_their_instance_alive() {
    let runtime = rt::Runtime::new();
    let broker = rt::Service::<rt::Broker<Service>>::new("broker", "1");
    let binding = broker.clone();
    runtime
        .install(
            rt::Plugin::new("broker", move |ctx| {
                let binding = binding.clone();
                async move { ctx.provide_broker(&binding) }
            })
            .provides(&broker),
        )
        .unwrap();
    let (old, first) = candidate(&runtime, &broker, 1).await;
    first.promote().unwrap();
    let saved = Arc::new(Mutex::new(None));
    let target = saved.clone();
    let binding = broker.clone();
    let owner = runtime
        .install(
            rt::Plugin::new("owner", move |ctx| {
                let target = target.clone();
                let binding = binding.clone();
                async move {
                    *target.lock().unwrap() = Some(ctx.dependency(&binding)?);
                    Ok(())
                }
            })
            .requires(&broker),
        )
        .unwrap();
    bounded(owner.ready()).await.unwrap();
    let activation = owner.snapshot().activation;
    let handle = saved.lock().unwrap().take().unwrap();
    async fn acquire(handle: &rt::ServiceHandle<rt::Broker<Service>>) -> Resource {
        bounded(handle.route(|service, ctx| {
            Box::pin(async move {
                service
                    .acquire_json(&ctx, "version", Value::Null)
                    .await
                    .map_err(native_error)
            })
        }))
        .await
        .unwrap()
    }
    let previous = acquire(&handle).await;
    let (_, second) = candidate(&runtime, &broker, 2).await;
    second.promote().unwrap();
    let current = acquire(&handle).await;
    let retiring = tokio::spawn({
        let old = old.clone();
        async move { old.dispose().await }
    });
    bounded(async {
        while old.snapshot().state != cordis_kernel::Lifecycle::Unloading {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!retiring.is_finished());
    assert_eq!(
        previous
            .invoke_json("get", cordis_sdk_core::json!({}))
            .await
            .unwrap(),
        json!(1.0)
    );
    assert_eq!(
        current
            .invoke_json("get", cordis_sdk_core::json!({}))
            .await
            .unwrap(),
        json!(2.0)
    );
    previous.dispose().await.unwrap();
    bounded(retiring).await.unwrap().unwrap();
    assert!(
        previous
            .invoke_json("get", cordis_sdk_core::json!({}))
            .await
            .is_err()
    );
    assert_eq!(owner.snapshot().activation, activation);
    bounded(runtime.shutdown()).await.unwrap();
    assert!(
        current
            .invoke_json("get", cordis_sdk_core::json!({}))
            .await
            .is_err()
    );
}

#[path = "../../../tests/support/json.rs"]
#[allow(dead_code)]
mod test_json;
