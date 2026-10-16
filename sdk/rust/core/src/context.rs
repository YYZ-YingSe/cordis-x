use crate::{
    FutureResult, LocalFuture, Message, MessageContract, Value,
    backend::{Backend, Operation, Reply},
};
use std::rc::Rc;

/// Callback-scoped authority. Its backend validates lifetime and permissions.
#[derive(Clone)]
pub struct Context {
    backend: Rc<dyn Backend>,
}
impl Context {
    /// Adapter entry point; ordinary business code receives its Context.
    pub fn from_backend(backend: Rc<dyn Backend>) -> Self {
        Self { backend }
    }

    /// Allows explicitly imported adapter extensions without making them part
    /// of the portable business interface.
    pub fn validation(&self) -> crate::Validation {
        self.backend.validation()
    }

    pub fn backend(&self) -> &dyn Backend {
        &*self.backend
    }

    pub(crate) fn submit(&self, operation: Operation) -> LocalFuture<Reply> {
        self.backend
            .submit(operation.with_validation(self.backend.validation()))
    }
    pub(crate) fn value(&self, operation: Operation) -> FutureResult {
        let reply = self.submit(operation);
        Box::pin(async move { reply.await?.value() })
    }
    pub(crate) fn message(&self, operation: Operation) -> crate::message::FutureResult {
        let reply = self.submit(operation);
        Box::pin(async move { reply.await?.message() })
    }
    pub async fn info(&self) -> crate::Result<Value> {
        self.value(Operation::Info).await
    }
    /// Use-side configuration pinned to this callback, not service discovery.
    pub async fn metadata(&self, name: &str, revision: &str) -> crate::Result<Value> {
        self.value(Operation::Metadata {
            name: name.into(),
            revision: revision.into(),
        })
        .await
    }
    pub async fn invoke(
        &self,
        dependency: &str,
        method: &str,
        args: Message,
        result: &MessageContract,
    ) -> crate::Result<Message> {
        self.message(Operation::Invoke {
            dependency: dependency.into(),
            method: method.into(),
            args,
            result: result.clone(),
        })
        .await
    }
}
