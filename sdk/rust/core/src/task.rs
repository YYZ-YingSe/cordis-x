use crate::{
    Context, Result, Value,
    backend::{Callback, Handle, Operation},
};
use std::{future::Future, rc::Rc};

#[derive(Clone)]
pub struct Task(Handle);
impl Task {
    pub async fn join(&self, ctx: &Context) -> Result<()> {
        ctx.value(Operation::TaskJoin {
            task: self.0.clone(),
        })
        .await
        .map(|_| ())
    }
}
impl Context {
    pub async fn cancelled(&self) -> Result<()> {
        self.value(Operation::Cancelled).await.map(|_| ())
    }
    pub async fn sleep(&self, milliseconds: u64) -> Result<()> {
        self.value(Operation::Sleep { milliseconds })
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
