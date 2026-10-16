//! One generated business implementation for native, process, ABI and component entries.
pub mod adder;
pub mod forwarding;
pub mod resources;
pub mod workflows;
pub use cordis_example_contracts::context;
pub use cordis_example_contracts::examples as generated;
pub use cordis_example_contracts::lifecycle;
pub use cordis_example_contracts::portable;

/// Explicit test surface: ordinary generated Context plus named fault probes.
/// Normal component artifacts export only `context::ContextClient::interface()`.
pub fn context_fixture_interface() -> cordis_sdk_core::Interface {
    let mut interface = context::ContextClient::interface();
    let cordis_sdk_core::Dispatch::Named(methods) = &mut interface.methods else {
        unreachable!()
    };
    let cordis_sdk_core::Dispatch::Named(probes) = context::ContextProbeClient::interface().methods
    else {
        unreachable!()
    };
    for (name, method) in probes {
        assert!(
            methods.insert(name, method).is_none(),
            "probe shadows business"
        );
    }
    interface
}
use cordis_sdk_core::{
    Context, Effect, LocalFuture, Result, Step, error, json,
    typed::{Codec, Flow, violation},
};
use portable::{
    PortableRustControls as Controls, PortableRustDependencies as Dependencies, PortableRustServer,
    data::{
        cordis::data::Unit,
        google::protobuf::{BoolValue, Empty, Int64Value, StringValue, UInt64Value, Value},
        portable::{Decision, Decisions, Metadata, Wrapped, decision::State},
    },
};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

pub type Example = portable::PortableRustExport<Business>;
pub struct Business {
    calls: Cell<u64>,
    saved: RefCell<Option<Context>>,
    effects: RefCell<Vec<Effect>>,
}
async fn log(ctx: &Context, args: cordis_sdk_core::Value) -> Result<cordis_sdk_core::Value> {
    Dependencies::logger()
        .write(ctx, Value::decode(args).map_err(violation)?)
        .await?
        .encode()
        .map_err(violation)
}
impl Business {
    fn contribution(self: Rc<Self>, ctx: Context, args: Value, fail: bool) -> LocalFuture<Empty> {
        Box::pin(async move {
            let args = args.encode().map_err(violation)?;
            let effect = ctx
                .defer("subscription", move |cleanup| {
                    let args = args.clone();
                    async move {
                        log(&cleanup, json!({"released": args})).await?;
                        if fail {
                            return Err(error("cleanup_failed", "retained responsibility"));
                        }
                        Ok(())
                    }
                })
                .await?;
            self.effects.borrow_mut().push(effect);
            Ok(Empty {})
        })
    }
}
impl PortableRustServer for Business {
    fn create(_: Unit) -> Result<Self> {
        Ok(Self {
            calls: Cell::new(0),
            saved: RefCell::default(),
            effects: RefCell::default(),
        })
    }
    fn activate(self: Rc<Self>, ctx: Context) -> LocalFuture<()> {
        Box::pin(async move {
            log(&ctx, json!("activate")).await?;
            Ok(())
        })
    }
    fn deactivate(self: Rc<Self>, ctx: Context) -> LocalFuture<()> {
        Box::pin(async move {
            log(&ctx, json!("deactivate")).await?;
            Ok(())
        })
    }
    fn resource_support(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<StringValue> {
        Box::pin(async move {
            // Deliberately exercise the actual absence of resource authority on this dependency.
            match ctx
                .acquire(
                    "example.portable-log",
                    "unbound",
                    cordis_sdk_core::Message::new(portable::data::cordis::data::Unit {})
                        .map_err(|e| cordis_sdk_core::refused("invalid_argument", e))?,
                )
                .await
            {
                Err(
                    cordis_sdk_core::Failure::Rejected(e) | cordis_sdk_core::Failure::Refused(e),
                ) => Ok(StringValue { value: e.code }),
                _ => Err(violation("unbound resource must be refused")),
            }
        })
    }
    fn echo(self: Rc<Self>, ctx: Context, args: Value) -> LocalFuture<Value> {
        Box::pin(async move { Dependencies::logger().write(&ctx, args).await })
    }
    fn count(self: Rc<Self>, _: Context, _: Empty) -> LocalFuture<UInt64Value> {
        Box::pin(async move {
            let value = self
                .calls
                .get()
                .checked_add(1)
                .ok_or_else(|| error("overflow", "call count overflow"))?;
            self.calls.set(value);
            Ok(UInt64Value { value })
        })
    }
    fn metadata(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Metadata> {
        Box::pin(async move {
            Metadata::decode(ctx.metadata("example.portable-rust", "1").await?).map_err(violation)
        })
    }
    fn save_context(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(async move {
            *self.saved.borrow_mut() = Some(ctx);
            Ok(Empty {})
        })
    }
    fn expired(self: Rc<Self>, _: Context, _: Empty) -> LocalFuture<BoolValue> {
        Box::pin(async move {
            let saved = self
                .saved
                .borrow()
                .clone()
                .ok_or_else(|| error("empty", "no saved Context"))?;
            Ok(BoolValue {
                value: log(&saved, json!("must not run")).await.is_err(),
            })
        })
    }
    fn defer(self: Rc<Self>, ctx: Context, args: Value) -> LocalFuture<Empty> {
        self.contribution(ctx, args, false)
    }
    fn failed_cleanup(self: Rc<Self>, ctx: Context, args: Value) -> LocalFuture<Empty> {
        self.contribution(ctx, args, true)
    }
    fn release(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(async move {
            let effect = self
                .effects
                .borrow()
                .last()
                .cloned()
                .ok_or_else(|| error("empty", "no effect"))?;
            effect.dispose(&ctx).await?;
            Ok(Empty {})
        })
    }
    fn scope(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Int64Value> {
        Box::pin(async move {
            let scope = ctx.scope("child scope").await?;
            let result = scope
                .run(&ctx, |ctx| async move {
                    ctx.defer("scoped", |ctx| async move {
                        log(&ctx, json!("scope-cleanup")).await?;
                        Ok(())
                    })
                    .await?;
                    log(&ctx, json!(42)).await
                })
                .await?;
            scope.dispose(&ctx).await?;
            Int64Value::decode(result).map_err(violation)
        })
    }
    fn sequence(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(async move {
            let sequence = ctx
                .effect("sequence", |_| async move {
                    Ok(Step::yield_inverse(|ctx| async move {
                        log(&ctx, json!("final-inverse")).await?;
                        Ok(())
                    })
                    .finish())
                })
                .await?;
            sequence.ready(&ctx).await?;
            sequence.dispose(&ctx).await?;
            Ok(Empty {})
        })
    }
    fn task(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(async move {
            ctx.spawn("worker", |ctx| async move {
                ctx.cancelled().await?;
                log(&ctx, json!("task-stopped")).await?;
                Ok(())
            })
            .await?;
            Ok(Empty {})
        })
    }
    fn event(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Decisions> {
        Box::pin(async move {
            let effect = Controls::message()
                .on(&ctx, |ctx, args| async move {
                    log(&ctx, json!(args.value)).await?;
                    Ok(Flow::Break(args))
                })
                .await?;
            let result = Controls::message()
                .parallel(&ctx, Int64Value { value: 42 })
                .await?;
            effect.dispose(&ctx).await?;
            Ok(Decisions {
                values: result
                    .into_iter()
                    .map(|flow| Decision {
                        state: Some(match flow {
                            Flow::Continue => State::Continued(Empty {}),
                            Flow::Break(value) => State::Stopped(value),
                        }),
                    })
                    .collect(),
            })
        })
    }
    fn middleware(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Wrapped> {
        Box::pin(async move {
            let effect = Controls::chain()
                .register(
                    &ctx,
                    |ctx, args, next| async move { next.run(&ctx, args).await },
                )
                .await?;
            let result = Controls::chain()
                .run(&ctx, Int64Value { value: 42 }, |_, args| async move {
                    Ok(Wrapped {
                        wrapped: Some(args.value),
                    })
                })
                .await?;
            effect.dispose(&ctx).await?;
            Ok(result)
        })
    }
    fn child(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(async move {
            let child = Controls::worker().install(&ctx, Unit {}).await?;
            child.ready(&ctx).await?;
            child.dispose(&ctx).await?;
            Ok(Empty {})
        })
    }
}
