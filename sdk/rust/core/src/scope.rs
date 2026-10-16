use crate::{
    Context, Result, Value,
    backend::{Callback, Handle, Operation},
};
use std::{future::Future, rc::Rc};

#[derive(Clone)]
pub struct Scope(Handle);
impl Scope {
    pub async fn run<F, Fut>(&self, ctx: &Context, run: F) -> Result<Value>
    where
        F: Fn(Context) -> Fut + 'static,
        Fut: Future<Output = Result<Value>> + 'static,
    {
        let run: Callback = Rc::new(move |ctx, _| Box::pin(run(ctx)));
        ctx.value(Operation::ScopeRun {
            scope: self.0.clone(),
            run,
        })
        .await
    }
    pub async fn dispose(&self, ctx: &Context) -> Result<()> {
        ctx.value(Operation::ScopeDispose {
            scope: self.0.clone(),
        })
        .await
        .map(|_| ())
    }
}
impl Context {
    pub async fn scope(&self, label: &str) -> Result<Scope> {
        self.submit(Operation::Scope {
            label: label.into(),
        })
        .await?
        .handle()
        .map(Scope)
    }
}
