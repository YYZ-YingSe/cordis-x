//! Business logic implementing a generated interface. No Value dispatch or protocol IDs.
pub use cordis_example_contracts::workflows as generated;
use cordis_sdk_core::{self as sdk, typed::Flow};
use generated::data::{
    cordis::data::{Int64, Unit},
    workflows::{Decision, Decisions, Settings, Tick, WorkerConfig, decision},
};
use generated::{WorkflowControls as Controls, WorkflowServer};
use std::rc::Rc;
pub struct Workflow;
pub type Plugin = generated::WorkflowExport<Workflow>;
fn number(value: i64) -> Int64 {
    Int64 { value: Some(value) }
}
fn add(a: i64, b: i64) -> sdk::Result<i64> {
    a.checked_add(b)
        .ok_or_else(|| sdk::error("overflow", "int64 overflow"))
}
fn decision(flow: Flow<Int64>) -> Decision {
    Decision {
        state: Some(match flow {
            Flow::Continue => decision::State::Continued(Unit {}),
            Flow::Break(value) => decision::State::Stopped(value),
        }),
    }
}
impl WorkflowServer for Workflow {
    fn create(_: Unit) -> sdk::Result<Self> {
        Ok(Self)
    }
    fn activate(self: Rc<Self>, ctx: sdk::Context) -> sdk::LocalFuture<()> {
        Box::pin(async move {
            Controls::tick()
                .on(&ctx, |_, args| async move {
                    let n = args.value.unwrap();
                    Ok(if n < 0 {
                        Flow::Continue
                    } else {
                        Flow::Break(number(add(n, 1)?))
                    })
                })
                .await?;
            Controls::request()
                .register(&ctx, |ctx, args, next| async move {
                    let n = args.value.unwrap();
                    if n == 0 {
                        return Ok(number(100));
                    }
                    let result = next
                        .run(
                            &ctx,
                            Tick {
                                value: Some(add(n, 1)?),
                            },
                        )
                        .await?;
                    Ok(number(add(result.value.unwrap(), 3)?))
                })
                .await?;
            Controls::settings()
                .provide(&ctx, Settings { answer: Some(42) })
                .await
        })
    }
    fn run(self: Rc<Self>, ctx: sdk::Context, args: Tick) -> sdk::LocalFuture<Int64> {
        Box::pin(async move {
            Controls::request()
                .run(&ctx, args, |_, args| async move {
                    let n = args.value.unwrap();
                    Ok(number(add(n, n)?))
                })
                .await
        })
    }
    fn serial(self: Rc<Self>, ctx: sdk::Context, args: Tick) -> sdk::LocalFuture<Decision> {
        Box::pin(async move { Ok(decision(Controls::tick().serial(&ctx, args).await?)) })
    }
    fn parallel(self: Rc<Self>, ctx: sdk::Context, args: Tick) -> sdk::LocalFuture<Decisions> {
        Box::pin(async move {
            Ok(Decisions {
                values: Controls::tick()
                    .parallel(&ctx, args)
                    .await?
                    .into_iter()
                    .map(decision)
                    .collect(),
            })
        })
    }
    fn spawn_worker(
        self: Rc<Self>,
        ctx: sdk::Context,
        config: WorkerConfig,
    ) -> sdk::LocalFuture<Unit> {
        Box::pin(async move {
            let child = Controls::worker().install(&ctx, config).await?;
            child.ready(&ctx).await?;
            Ok(Unit {})
        })
    }
}
