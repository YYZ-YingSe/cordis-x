use crate::{
    Context, Effect, Message, MessageContract, Method, Result,
    backend::{Handle, Listener, MessageCallback, Operation},
    typed::Flow,
};
use std::{future::Future, rc::Rc};

#[derive(Clone)]
pub struct Next(Handle);
impl Next {
    pub fn from_handle(handle: Handle) -> Self {
        Self(handle)
    }
    pub async fn run(
        &self,
        ctx: &Context,
        args: Message,
        result: &MessageContract,
    ) -> Result<Message> {
        ctx.message(Operation::Next {
            next: self.0.clone(),
            args,
            result: result.clone(),
        })
        .await
    }
}
impl Context {
    pub async fn on<F, Fut>(&self, event: &str, signature: &Method, handler: F) -> Result<Effect>
    where
        F: Fn(Context, Message) -> Fut + 'static,
        Fut: Future<Output = Result<Flow<Message>>> + 'static,
    {
        let handler: Listener = Rc::new(move |ctx, args| Box::pin(handler(ctx, args)));
        self.submit(Operation::On {
            event: event.into(),
            signature: signature.clone(),
            handler,
        })
        .await?
        .handle()
        .map(Effect)
    }
    pub async fn emit(
        &self,
        event: &str,
        signature: &Method,
        args: Message,
        serial: bool,
    ) -> Result<Vec<Flow<Message>>> {
        args.check(&signature.args)
            .map_err(|e| crate::refused("invalid_argument", e))?;
        self.submit(Operation::Emit {
            event: event.into(),
            signature: signature.clone(),
            args,
            serial,
        })
        .await?
        .flows()
    }
    pub async fn middleware<F, Fut>(
        &self,
        name: &str,
        signature: &Method,
        handler: F,
    ) -> Result<Effect>
    where
        F: Fn(Context, Message, Next) -> Fut + 'static,
        Fut: Future<Output = Result<Message>> + 'static,
    {
        let handler = Rc::new(move |ctx, args, next| {
            Box::pin(handler(ctx, args, next)) as crate::message::FutureResult
        });
        self.submit(Operation::Middleware {
            name: name.into(),
            signature: signature.clone(),
            handler,
        })
        .await?
        .handle()
        .map(Effect)
    }
    pub async fn run<F, Fut>(
        &self,
        name: &str,
        signature: &Method,
        args: Message,
        terminal: F,
    ) -> Result<Message>
    where
        F: Fn(Context, Message) -> Fut + 'static,
        Fut: Future<Output = Result<Message>> + 'static,
    {
        args.check(&signature.args)
            .map_err(|e| crate::refused("invalid_argument", e))?;
        let terminal: MessageCallback = Rc::new(move |ctx, args| Box::pin(terminal(ctx, args)));
        self.message(Operation::Run {
            name: name.into(),
            signature: signature.clone(),
            args,
            terminal,
        })
        .await
    }
}
