//! Explicit fault artifact. Ordinary calls still execute the generated business Server.
use crate::service::{self, log};
use cordis_guest::adapter::ComponentContextExt;
use cordis_portable_example::context::{
    self as generated, ContextProbeDependencies as Dependencies, ContextProbeExport,
    ContextProbeServer,
    data::{
        component_example::ProbeConfig,
        google::protobuf::{Empty, Value as Dynamic},
    },
};
use cordis_sdk_core::{
    Context, Interface, LocalFuture, Plugin, PluginManifest, Result, Value, error, json,
    typed::{Codec, violation},
};
use std::{collections::BTreeMap, rc::Rc};
fn probe_interface() -> &'static Interface {
    static PROBES: std::sync::LazyLock<Interface> =
        std::sync::LazyLock::new(generated::ContextProbeClient::interface);
    &PROBES
}
fn changed(name: &str) -> bool {
    wasi::cli::environment::get_environment()
        .iter()
        .any(|(key, _)| key == name)
}
pub struct Example {
    business: Rc<service::Example>,
    probes: Rc<ContextProbeExport<Probes>>,
}
impl Plugin for Example {
    fn interface() -> Interface {
        let mut interface = cordis_portable_example::context_fixture_interface();
        if changed("CORDIS_TEST_INTERFACE") {
            interface.capabilities.insert("different-interface".into());
        }
        interface
    }
    fn manifest() -> PluginManifest {
        if changed("CORDIS_TEST_CONTRACT") {
            return PluginManifest {
                provides: cordis_sdk_core::Contract {
                    name: "changed".into(),
                    revision: "1".into(),
                },
                requires: vec![],
            };
        }
        service::Example::manifest()
    }
    fn dependency_interfaces() -> BTreeMap<String, Interface> {
        if changed("CORDIS_TEST_CONTRACT") {
            return BTreeMap::new();
        }
        let mut dependencies = service::Example::dependency_interfaces();
        if changed("CORDIS_TEST_DEPENDENCY") {
            dependencies
                .get_mut("example.context-log")
                .unwrap()
                .capabilities
                .insert("different-dependency".into());
        }
        dependencies
    }
    fn bindings() -> cordis_sdk_core::Bindings {
        service::Example::bindings()
    }
    fn create(config: Value) -> Result<Self> {
        Ok(Self {
            business: Rc::new(service::Example::create(json!({}))?),
            probes: Rc::new(ContextProbeExport::create(config)?),
        })
    }
    fn activate(self: Rc<Self>, ctx: Context) -> LocalFuture<()> {
        Box::pin(async move {
            self.probes.clone().activate(ctx.clone()).await?;
            self.business.clone().activate(ctx).await
        })
    }
    fn deactivate(self: Rc<Self>, ctx: Context) -> LocalFuture<()> {
        self.business.clone().deactivate(ctx)
    }
    fn invoke(
        self: Rc<Self>,
        ctx: Context,
        method: String,
        args: cordis_sdk_core::Message,
    ) -> cordis_sdk_core::message::FutureResult {
        if probe_interface().methods.get(&method).is_some() {
            self.probes.clone().invoke(ctx, method, args)
        } else {
            self.business.clone().invoke(ctx, method, args)
        }
    }
}
pub struct Probes {
    config: ProbeConfig,
}
impl ContextProbeServer for Probes {
    fn create(config: ProbeConfig) -> Result<Self> {
        Ok(Self { config })
    }
    fn activate(self: Rc<Self>, ctx: Context) -> LocalFuture<()> {
        Box::pin(async move {
            if let Some(value) = &self.config.provide {
                ctx.provide(
                    "settings",
                    cordis_sdk_core::Message::new(value.clone()).map_err(violation)?,
                )
                .await?;
            }
            Ok(())
        })
    }
    fn stale(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(async move {
            let stale = ctx.clone();
            ctx.defer("stale", move |_| {
                let stale = stale.clone();
                async move {
                    assert!(stale.info().await.is_err());
                    Ok(())
                }
            })
            .await?;
            Ok(Empty {})
        })
    }
    fn sequence_failure(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(async move { service::sequence(&ctx, true).await })
    }
    fn provide(self: Rc<Self>, ctx: Context, args: Dynamic) -> LocalFuture<Empty> {
        Box::pin(async move {
            ctx.provide(
                "settings",
                cordis_sdk_core::Message::new(args).map_err(violation)?,
            )
            .await?;
            Ok(Empty {})
        })
    }
    fn install_denied(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(async move {
            ctx.install(
                "not-granted",
                cordis_sdk_core::Message::new(Empty {}).map_err(violation)?,
            )
            .await?;
            Ok(Empty {})
        })
    }
    fn delayed_defer(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(async move {
            Dependencies::logger().wait(&ctx, Empty {}).await?;
            ctx.defer("late", |cleanup| async move {
                log(&cleanup, json!("late-cleanup")).await?;
                Ok(())
            })
            .await?;
            Ok(Empty {})
        })
    }
    fn failed_defer(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(async move {
            ctx.defer("failed inverse", |cleanup| async move {
                log(&cleanup, json!("cleanup-attempt")).await?;
                Err(error("cleanup_failed", "cannot undo"))
            })
            .await?;
            Ok(Empty {})
        })
    }
    fn invalid_registration(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(async move {
            ctx.request("defer", json!({"label":"missing callback"}))
                .await?;
            Ok(Empty {})
        })
    }
    fn spin(self: Rc<Self>, _: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(async {
            loop {
                std::hint::spin_loop();
            }
        })
    }
    fn pending(self: Rc<Self>, _: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(std::future::pending())
    }
    fn trap(self: Rc<Self>, _: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(async { panic!("guest trap") })
    }
    fn trap_after_request(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(async move {
            use std::{future::Future, task::Poll};
            let mut request = std::pin::pin!(ctx.cancelled());
            std::future::poll_fn(|cx| {
                assert!(request.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            panic!("trap with an admitted, unpolled host request");
        })
    }
    fn wasi_block(self: Rc<Self>, _: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(async {
            wasi::clocks::monotonic_clock::subscribe_duration(60_000_000_000).block();
            Ok(Empty {})
        })
    }
}
