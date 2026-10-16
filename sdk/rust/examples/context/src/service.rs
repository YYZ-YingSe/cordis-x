//! Fixed business surface; dispatch, input/output validation and controls are generated.
use cordis_portable_example::context::{
    ContextControls as Controls, ContextDependencies as Dependencies, ContextExport, ContextServer,
    data::{
        component_example::*,
        cordis::data::Unit,
        google::protobuf::{BytesValue, Empty, Int64Value, StringValue, UInt64Value, Value},
    },
};
use cordis_sdk_core::{
    Context, Effect, LocalFuture, Result, Step, error, json,
    typed::{Codec, Flow, violation},
};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

pub type Example = ContextExport<Business>;
#[derive(Default)]
pub struct Business {
    effects: RefCell<Vec<Effect>>,
}
pub(crate) async fn log(
    ctx: &Context,
    args: cordis_sdk_core::Value,
) -> Result<cordis_sdk_core::Value> {
    Dependencies::logger()
        .write(ctx, Value::decode(args).map_err(violation)?)
        .await?
        .encode()
        .map_err(violation)
}
pub(crate) async fn sequence(ctx: &Context, fail: bool) -> Result<Empty> {
    let count = Cell::new(0);
    let sequence = ctx
        .effect("steps", move |_| {
            let i = count.get() + 1;
            count.set(i);
            async move {
                let inverse = Step::yield_inverse(move |cleanup| async move {
                    log(&cleanup, json!({"step":i})).await?;
                    Ok(())
                });
                Ok(if i == 3 {
                    if fail {
                        inverse.fail("last step failed")
                    } else {
                        inverse.finish()
                    }
                } else {
                    inverse
                })
            }
        })
        .await?;
    let result = sequence.ready(ctx).await;
    if fail {
        assert!(result.is_err());
    } else {
        result?;
    }
    sequence.dispose(ctx).await?;
    Ok(Empty {})
}
impl ContextServer for Business {
    fn create(_: Unit) -> Result<Self> {
        Ok(Self::default())
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
    fn code_version(self: Rc<Self>, _: Context, _: Empty) -> LocalFuture<StringValue> {
        Box::pin(async {
            Ok(StringValue {
                value: if cfg!(feature = "version-v2") {
                    "v2"
                } else {
                    "v1"
                }
                .into(),
            })
        })
    }
    fn echo(self: Rc<Self>, ctx: Context, args: Value) -> LocalFuture<Value> {
        Box::pin(async move { Dependencies::logger().write(&ctx, args).await })
    }
    fn info(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<ContextInfo> {
        Box::pin(async move { ContextInfo::decode(ctx.info().await?).map_err(violation) })
    }
    fn metadata_for(self: Rc<Self>, ctx: Context, args: ContractIdentity) -> LocalFuture<Metadata> {
        Box::pin(async move {
            Metadata::decode(
                ctx.metadata(&args.name.unwrap(), &args.revision.unwrap())
                    .await?,
            )
            .map_err(violation)
        })
    }
    fn metadata(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Metadata> {
        Box::pin(async move {
            Metadata::decode(ctx.metadata("example.context", "1").await?).map_err(violation)
        })
    }
    fn metadata_defer(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Metadata> {
        Box::pin(async move {
            let before = ctx.metadata("example.context", "1").await?;
            let expected = before.clone();
            ctx.defer("metadata", move |cleanup| {
                let expected = expected.clone();
                async move {
                    let after = cleanup.metadata("example.context", "1").await?;
                    if after != expected {
                        return Err(error("metadata", "cleanup configuration changed"));
                    }
                    log(&cleanup, json!({"metadata_cleanup":after})).await?;
                    Ok(())
                }
            })
            .await?;
            Metadata::decode(before).map_err(violation)
        })
    }
    fn subscribe(self: Rc<Self>, ctx: Context, args: StringValue) -> LocalFuture<UInt64Value> {
        Box::pin(async move {
            let effect = ctx
                .defer("subscription", move |cleanup| {
                    let name = args.value.clone();
                    async move {
                        log(&cleanup, json!({"released":name})).await?;
                        Ok(())
                    }
                })
                .await?;
            self.effects.borrow_mut().push(effect);
            Ok(UInt64Value {
                value: self.effects.borrow().len() as u64,
            })
        })
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
    fn scope(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(async move {
            let scope = ctx.scope("example child").await?;
            scope
                .run(
                    &ctx,
                    |child| async move { log(&child, json!("child")).await },
                )
                .await?;
            scope.dispose(&ctx).await?;
            Ok(Empty {})
        })
    }
    fn task(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(async move {
            ctx.spawn("worker", |task| async move {
                task.cancelled().await?;
                log(&task, json!("task-stopped")).await?;
                Ok(())
            })
            .await?;
            Ok(Empty {})
        })
    }
    fn events(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Decision> {
        Box::pin(async move {
            let e = Controls::messages()
                .on(&ctx, |listener, args| async move {
                    log(&listener, args.encode().map_err(violation)?).await?;
                    Ok(Flow::Continue)
                })
                .await?;
            let result = Controls::messages()
                .serial(&ctx, Int64Value { value: 42 })
                .await?;
            e.dispose(&ctx).await?;
            Ok(Decision {
                state: Some(match result {
                    Flow::Continue => decision::State::Continued(Empty {}),
                    Flow::Break(value) => decision::State::Stopped(value),
                }),
            })
        })
    }
    fn middleware(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Int64Value> {
        Box::pin(async move {
            let e = Controls::pipeline()
                .register(&ctx, |frame, args, next| async move {
                    let value = args
                        .value
                        .checked_add(1)
                        .ok_or_else(|| error("overflow", "integer overflow"))?;
                    next.run(&frame, Int64Value { value }).await
                })
                .await?;
            let value = Controls::pipeline()
                .run(
                    &ctx,
                    Int64Value { value: 41 },
                    |_, args| async move { Ok(args) },
                )
                .await?;
            e.dispose(&ctx).await?;
            Ok(value)
        })
    }
    fn sequence(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(async move { sequence(&ctx, false).await })
    }
    fn install(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(async move {
            let child = Controls::worker().install(&ctx, Unit {}).await?;
            child.ready(&ctx).await?;
            child.dispose(&ctx).await?;
            Ok(Empty {})
        })
    }
    fn wasi_env(self: Rc<Self>, _: Context, _: Empty) -> LocalFuture<Environment> {
        Box::pin(async {
            Ok(Environment {
                entries: wasi::cli::environment::get_environment()
                    .into_iter()
                    .map(|(name, value)| EnvironmentEntry {
                        name: Some(name),
                        value: Some(value),
                    })
                    .collect(),
            })
        })
    }
    fn wasi_random(self: Rc<Self>, _: Context, _: Empty) -> LocalFuture<BytesValue> {
        Box::pin(async {
            Ok(BytesValue {
                value: wasi::random::random::get_random_bytes(8).into(),
            })
        })
    }
    fn wasi_clock(self: Rc<Self>, _: Context, _: Empty) -> LocalFuture<UInt64Value> {
        Box::pin(async {
            Ok(UInt64Value {
                value: wasi::clocks::wall_clock::now().seconds,
            })
        })
    }
    fn wasi_read(self: Rc<Self>, _: Context, args: FilePath) -> LocalFuture<BytesValue> {
        Box::pin(async move { super::io::read(args) })
    }
    fn wasi_write(self: Rc<Self>, _: Context, args: FileWrite) -> LocalFuture<Empty> {
        Box::pin(async move { super::io::write(args) })
    }
    fn wasi_listen(self: Rc<Self>, _: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(async { super::io::listen() })
    }
    fn wasi_connect(self: Rc<Self>, _: Context, args: Port) -> LocalFuture<Empty> {
        Box::pin(async move {
            super::io::connect(args)?;
            Ok(Empty {})
        })
    }
    fn wasi_exchange(self: Rc<Self>, _: Context, args: Port) -> LocalFuture<BytesValue> {
        Box::pin(async move { super::io::exchange(args) })
    }
}
