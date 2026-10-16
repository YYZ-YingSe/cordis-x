use crate::{
    Context, LocalFuture, Message, MessageContract, Result,
    backend::{Handle, Operation},
};
use std::rc::Rc;

/// Plugin-owned implementation of a caller-owned resource. The adapter retains
/// this object until close succeeds; dropping it is not asynchronous cleanup.
pub trait ResourceObject: 'static {
    fn typed_dispatch(&self) -> bool {
        false
    }
    fn invoke(
        self: Rc<Self>,
        ctx: Context,
        method: String,
        args: Message,
    ) -> crate::message::FutureResult;
    /// A failure retains the cleanup obligation and must not be retried blindly.
    fn close(self: Rc<Self>, ctx: Context) -> LocalFuture<()>;
}

/// An opaque resource capability. Cloning or using it from a later callback
/// does not transfer ownership away from the scope that acquired it.
#[derive(Clone)]
pub struct Resource(Handle);
impl Resource {
    pub async fn invoke(
        &self,
        ctx: &Context,
        method: &str,
        args: Message,
        result: &MessageContract,
    ) -> Result<Message> {
        ctx.message(Operation::ResourceInvoke {
            resource: self.0.clone(),
            method: method.into(),
            args,
            result: result.clone(),
        })
        .await
    }

    /// Close admission, drain accepted calls, and share one cleanup result with
    /// concurrent disposal and automatic scope cleanup.
    pub async fn dispose(&self, ctx: &Context) -> Result<()> {
        ctx.value(Operation::ResourceDispose {
            resource: self.0.clone(),
        })
        .await
        .map(|_| ())
    }
}

impl Context {
    /// Acquire from an explicitly bound dependency. Unsupported adapters must
    /// reject before executing the resource factory.
    pub async fn acquire(&self, dependency: &str, kind: &str, args: Message) -> Result<Resource> {
        self.submit(Operation::Acquire {
            dependency: dependency.into(),
            kind: kind.into(),
            args,
        })
        .await?
        .handle()
        .map(Resource)
    }
}
