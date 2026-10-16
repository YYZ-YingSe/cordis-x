//! Typed projections of existing control capabilities; no new lifecycle state.
use super::{Codec, violation};
use crate::message::{argument, input, output, result};
use crate::{Child, Context, Effect, MessageContract, Method, Result, Value};
use std::{collections::BTreeSet, future::Future, marker::PhantomData, rc::Rc};

/// Continue has no payload. A Break payload is checked using the event result schema.
#[derive(Clone, Debug, PartialEq)]
pub enum Flow<T> {
    Continue,
    Break(T),
}
fn decode_flow<T: Codec>(value: Flow<crate::Message>, shape: &MessageContract) -> Result<Flow<T>> {
    match value {
        Flow::Continue => Ok(Flow::Continue),
        Flow::Break(value) => result(value, shape).map(Flow::Break),
    }
}

#[derive(Clone, Copy)]
pub struct Event<A, R> {
    alias: &'static str,
    signature: &'static Method,
    marker: PhantomData<fn(A) -> R>,
}
impl<A: Codec + 'static, R: Codec + 'static> Event<A, R> {
    pub const fn new(alias: &'static str, signature: &'static Method) -> Self {
        Self {
            alias,
            signature,
            marker: PhantomData,
        }
    }
    pub async fn on<F, Fut>(&self, ctx: &Context, handler: F) -> Result<Effect>
    where
        F: Fn(Context, A) -> Fut + 'static,
        Fut: Future<Output = Result<Flow<R>>> + 'static,
    {
        let signature = self.signature;
        let handler = Rc::new(handler);
        ctx.on(self.alias, signature, move |ctx, value| {
            let handler = handler.clone();
            async move {
                let args = input(value, &signature.args)?;
                let flow = handler(ctx, args)
                    .await
                    .map_err(|e| e.check_declared(&signature.errors, "listener"))?;
                match flow {
                    Flow::Continue => Ok(Flow::Continue),
                    Flow::Break(value) => Ok(Flow::Break(output(value, &signature.result)?)),
                }
            }
        })
        .await
    }
    async fn dispatch(&self, ctx: &Context, args: A, serial: bool) -> Result<Vec<Flow<R>>> {
        let args = argument(args, &self.signature.args.with_validation(ctx.validation()))?;
        let value = ctx
            .emit(self.alias, self.signature, args, serial)
            .await
            .map_err(|e| e.check_declared(&self.signature.errors, "event"))?;
        value
            .into_iter()
            .map(|v| decode_flow(v, &self.signature.result.with_validation(ctx.validation())))
            .collect()
    }
    pub async fn parallel(&self, ctx: &Context, args: A) -> Result<Vec<Flow<R>>> {
        self.dispatch(ctx, args, false).await
    }
    pub async fn serial(&self, ctx: &Context, args: A) -> Result<Flow<R>> {
        let mut stopped = None;
        for flow in self.dispatch(ctx, args, true).await? {
            if stopped.is_some() {
                return Err(violation("serial event continued after Break"));
            }
            if let Flow::Break(value) = flow {
                stopped = Some(value);
            }
        }
        Ok(stopped.map_or(Flow::Continue, Flow::Break))
    }
}

pub struct Next<A, R> {
    next: crate::Next,
    signature: &'static Method,
    marker: PhantomData<fn(A) -> R>,
}
impl<A, R> Clone for Next<A, R> {
    fn clone(&self) -> Self {
        Self {
            next: self.next.clone(),
            signature: self.signature,
            marker: PhantomData,
        }
    }
}
impl<A: Codec, R: Codec> Next<A, R> {
    pub async fn run(&self, ctx: &Context, args: A) -> Result<R> {
        let args = argument(args, &self.signature.args.with_validation(ctx.validation()))?;
        let value = self
            .next
            .run(ctx, args, &self.signature.result)
            .await
            .map_err(|e| e.check_declared(&self.signature.errors, "next"))?;
        result(
            value,
            &self.signature.result.with_validation(ctx.validation()),
        )
    }
}
#[derive(Clone, Copy)]
pub struct Middleware<A, R> {
    alias: &'static str,
    signature: &'static Method,
    marker: PhantomData<fn(A) -> R>,
}
impl<A: Codec + 'static, R: Codec + 'static> Middleware<A, R> {
    pub const fn new(alias: &'static str, signature: &'static Method) -> Self {
        Self {
            alias,
            signature,
            marker: PhantomData,
        }
    }
    pub async fn register<F, Fut>(&self, ctx: &Context, handler: F) -> Result<Effect>
    where
        F: Fn(Context, A, Next<A, R>) -> Fut + 'static,
        Fut: Future<Output = Result<R>> + 'static,
    {
        let signature = self.signature;
        let handler = Rc::new(handler);
        ctx.middleware(self.alias, signature, move |ctx, value, next| {
            let handler = handler.clone();
            async move {
                let args = input(value, &signature.args)?;
                let next = Next {
                    next,
                    signature,
                    marker: PhantomData,
                };
                let value = handler(ctx, args, next)
                    .await
                    .map_err(|e| e.check_declared(&signature.errors, "middleware"))?;
                output(value, &signature.result)
            }
        })
        .await
    }
    /// A terminal is explicit because input and output need not have the same type.
    pub async fn run<F, Fut>(&self, ctx: &Context, args: A, terminal: F) -> Result<R>
    where
        F: Fn(Context, A) -> Fut + 'static,
        Fut: Future<Output = Result<R>> + 'static,
    {
        let signature = self.signature;
        let args = argument(args, &signature.args.with_validation(ctx.validation()))?;
        let terminal = Rc::new(terminal);
        let value = ctx
            .run(self.alias, signature, args, move |ctx, value| {
                let terminal = terminal.clone();
                async move {
                    let args = input(value, &signature.args)?;
                    let value = terminal(ctx, args)
                        .await
                        .map_err(|e| e.check_declared(&signature.errors, "terminal"))?;
                    output(value, &signature.result)
                }
            })
            .await
            .map_err(|e| e.check_declared(&signature.errors, "middleware"))?;
        result(value, &signature.result.with_validation(ctx.validation()))
    }
}

#[derive(Clone, Copy)]
pub struct Factory<C> {
    validation: crate::Validation,
    alias: &'static str,
    config: &'static MessageContract,
    errors: &'static BTreeSet<String>,
    marker: PhantomData<fn(C)>,
}
impl<C: Codec> Factory<C> {
    pub const fn new(
        alias: &'static str,
        config: &'static MessageContract,
        errors: &'static BTreeSet<String>,
    ) -> Self {
        Self {
            validation: crate::Validation::Checked,
            alias,
            config,
            errors,
            marker: PhantomData,
        }
    }
    pub const fn with_validation(mut self, validation: crate::Validation) -> Self {
        self.validation = validation;
        self
    }
    pub async fn install(&self, ctx: &Context, config: C) -> Result<Child> {
        ctx.install(
            self.alias,
            crate::message::argument(config, &self.config.with_validation(ctx.validation()))?,
        )
        .await
        .map_err(|e| e.check_declared(self.errors, "factory"))
    }
    /// Host-side factory adapter: reject malformed config before business creates anything.
    pub fn build<T>(
        &self,
        config: crate::Message,
        factory: impl FnOnce(C) -> Result<T>,
    ) -> Result<T> {
        factory(crate::message::input(
            config,
            &self.config.with_validation(self.validation),
        )?)
        .map_err(|e| e.check_declared(self.errors, "factory"))
    }
    /// Explicit external JSON ingress; native factories pass Message directly.
    pub fn build_json<T>(&self, config: Value, factory: impl FnOnce(C) -> Result<T>) -> Result<T> {
        let config = crate::Message::from_json(self.config.clone(), &config)
            .map_err(|e| crate::refused("invalid_argument", e))?;
        self.build(config, factory)
    }
}
#[derive(Clone, Copy)]
pub struct Provision<T> {
    validation: crate::Validation,
    alias: &'static str,
    shape: &'static MessageContract,
    marker: PhantomData<fn(T)>,
}
impl<T: Codec> Provision<T> {
    pub const fn new(alias: &'static str, shape: &'static MessageContract) -> Self {
        Self {
            validation: crate::Validation::Checked,
            alias,
            shape,
            marker: PhantomData,
        }
    }
    pub const fn with_validation(mut self, validation: crate::Validation) -> Self {
        self.validation = validation;
        self
    }
    pub async fn provide(&self, ctx: &Context, value: T) -> Result<()> {
        ctx.provide(
            self.alias,
            crate::message::argument(value, &self.shape.with_validation(ctx.validation()))?,
        )
        .await
    }
    /// Decode a value obtained through an already-authorized service dependency.
    pub fn decode(&self, value: crate::Message) -> Result<T> {
        crate::message::result(value, &self.shape.with_validation(self.validation))
    }
    /// Explicit external JSON result boundary.
    pub fn decode_json(&self, value: Value) -> Result<T> {
        let value = crate::Message::from_json(self.shape.clone(), &value).map_err(violation)?;
        self.decode(value)
    }
}
