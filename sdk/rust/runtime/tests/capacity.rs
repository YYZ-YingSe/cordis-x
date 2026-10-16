use cordis_runtime::{Plugin as NativePlugin, Runtime, Service};
use cordis_sdk_core::*;
use cordis_sdk_runtime::{Capabilities, Service as PortableService};
use std::{cell::Cell, rc::Rc};

struct CapacityProbe;
impl Plugin for CapacityProbe {
    fn interface() -> cordis_sdk_core::Interface {
        cordis_sdk_core::Interface::value_dispatch(true)
    }
    fn manifest() -> PluginManifest {
        PluginManifest {
            provides: Contract {
                name: "example.capacity".into(),
                revision: "1".into(),
            },
            requires: vec![],
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
}
impl CapacityProbe {
    fn call_probe(self: Rc<Self>, ctx: Context, command: String, _: Value) -> FutureResult {
        Box::pin(async move {
            if command != "invoke" {
                return Ok(Value::Null);
            }
            // Leave room for the producer, but not for its returned inverse.
            for _ in 0..4095 {
                ctx.defer("occupied", |_| async { Ok(()) }).await?;
            }
            let acquired = Rc::new(Cell::new(0));
            let started = acquired.clone();
            let sequence = ctx
                .effect("must reject before acquiring", move |_| {
                    started.set(started.get() + 1);
                    async { Ok(Step::yield_inverse(|_| async { Ok(()) }).finish()) }
                })
                .await?;
            let failed = sequence.ready(&ctx).await.is_err();
            sequence.dispose(&ctx).await?;
            Ok(json!({"failed":failed,"acquired":acquired.get()}))
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_callback_table_rejects_sequence_before_acquisition() {
    let rt = Runtime::new();
    let service = Service::<PortableService>::new("example.capacity", "1");
    rt.install(
        cordis_sdk_runtime::plugin::<CapacityProbe>(
            "capacity",
            &service,
            Value::Null,
            Capabilities::default(),
        )
        .unwrap(),
    )
    .unwrap()
    .ready()
    .await
    .unwrap();
    let binding = service.clone();
    let consumer = rt
        .install(
            NativePlugin::new("consumer", move |ctx| {
                let service = binding.clone();
                async move {
                    let value = ctx
                        .dependency(&service)?
                        .call(|service, ctx| {
                            Box::pin(async move {
                                service
                                    .invoke_json(&ctx, "fill", Value::Null)
                                    .await
                                    .map_err(|e| cordis_runtime::Error::Message(e.to_string()))
                            })
                        })
                        .await?;
                    assert_eq!(value, json!({"failed":true,"acquired":0.0}));
                    Ok(())
                }
            })
            .requires(&service),
        )
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(30), consumer.ready())
        .await
        .unwrap()
        .unwrap();
    rt.shutdown().await.unwrap();
}

#[path = "../../tests/support/json.rs"]
#[allow(dead_code)]
mod test_json;
