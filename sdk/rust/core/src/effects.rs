use crate::{
    Context, Result, Value,
    backend::{Callback, Handle, Operation},
};
use std::{future::Future, rc::Rc};

#[derive(Clone)]
pub struct Effect(pub(crate) Handle);
impl Effect {
    pub async fn dispose(&self, ctx: &Context) -> Result<()> {
        ctx.value(Operation::Dispose {
            effect: self.0.clone(),
        })
        .await
        .map(|_| ())
    }
}

pub struct Step {
    inverse: Option<Callback>,
    complete: bool,
    failure: Option<String>,
}
impl Step {
    pub fn complete() -> Self {
        Self {
            inverse: None,
            complete: true,
            failure: None,
        }
    }
    pub fn yield_inverse<F, Fut>(cleanup: F) -> Self
    where
        F: Fn(Context) -> Fut + 'static,
        Fut: Future<Output = Result<()>> + 'static,
    {
        let inverse: Callback = Rc::new(move |ctx, _| {
            let future = cleanup(ctx);
            Box::pin(async move {
                future.await?;
                Ok(Value::Null)
            })
        });
        Self {
            inverse: Some(inverse),
            complete: false,
            failure: None,
        }
    }
    pub fn finish(mut self) -> Self {
        self.complete = true;
        self
    }
    pub fn fail(mut self, message: impl Into<String>) -> Self {
        self.failure = Some(message.into());
        self
    }
    /// Adapter consumes the step and registers any inverse before returning it.
    pub fn into_parts(self) -> (Option<Callback>, bool, Option<String>) {
        (self.inverse, self.complete, self.failure)
    }
}

#[derive(Clone)]
pub struct Sequence(Handle);
impl Sequence {
    pub async fn ready(&self, ctx: &Context) -> Result<Value> {
        ctx.value(Operation::SequenceReady {
            sequence: self.0.clone(),
        })
        .await
    }
    pub async fn dispose(&self, ctx: &Context) -> Result<()> {
        ctx.value(Operation::SequenceDispose {
            sequence: self.0.clone(),
        })
        .await
        .map(|_| ())
    }
}
impl Context {
    pub async fn defer<F, Fut>(&self, label: &str, cleanup: F) -> Result<Effect>
    where
        F: Fn(Context) -> Fut + 'static,
        Fut: Future<Output = Result<()>> + 'static,
    {
        let cleanup: Callback = Rc::new(move |ctx, _| {
            let future = cleanup(ctx);
            Box::pin(async move {
                future.await?;
                Ok(Value::Null)
            })
        });
        self.submit(Operation::Defer {
            label: label.into(),
            cleanup,
        })
        .await?
        .handle()
        .map(Effect)
    }
    pub async fn effect<F, Fut>(&self, label: &str, next: F) -> Result<Sequence>
    where
        F: Fn(Context) -> Fut + 'static,
        Fut: Future<Output = Result<Step>> + 'static,
    {
        let next = Rc::new(move |ctx| Box::pin(next(ctx)) as crate::LocalFuture<Step>);
        self.submit(Operation::Effect {
            label: label.into(),
            next,
        })
        .await?
        .handle()
        .map(Sequence)
    }
}
