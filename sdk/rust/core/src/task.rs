use crate::{
    Context, Result, Value,
    backend::{Callback, Handle, Observation, Operation},
};
use std::{future::Future, rc::Rc};

#[derive(Clone)]
pub struct Task(Handle);
impl Task {
    pub async fn join(&self, ctx: &Context) -> Result<()> {
        ctx.observe(Observation::TaskJoin {
            task: self.0.clone(),
        })
        .await
        .map(|_| ())
    }
}
impl Context {
    /// Observe cooperative cancellation from a managed task callback. Dropping
    /// this future withdraws only the observation; it does not stop the task.
    /// Callback shutdown settles any foreign acknowledgement before releasing
    /// callback storage, without waiting for an unused cancellation signal.
    pub async fn cancelled(&self) -> Result<()> {
        self.observe(Observation::Cancellation).await.map(|_| ())
    }
    /// Wait for at most 60,000 milliseconds, or return early on owner cancellation.
    /// A losing select branch releases its timer rather than delaying callback
    /// completion. Already admitted calls and resource operations still drain
    /// independently, even when their response futures are dropped.
    pub async fn sleep(&self, milliseconds: u64) -> Result<()> {
        self.observe(Observation::Delay { milliseconds })
            .await
            .map(|_| ())
    }
    pub async fn spawn<F, Fut>(&self, label: &str, task: F) -> Result<Task>
    where
        F: Fn(Context) -> Fut + 'static,
        Fut: Future<Output = Result<()>> + 'static,
    {
        let task: Callback = Rc::new(move |ctx, _| {
            let future = task(ctx);
            Box::pin(async move {
                future.await?;
                Ok(Value::Null)
            })
        });
        self.submit(Operation::Spawn {
            label: label.into(),
            task,
        })
        .await?
        .handle()
        .map(Task)
    }
}
