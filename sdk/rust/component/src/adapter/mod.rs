//! Component-specific mapping and escape hatch. Ordinary plugins use the
//! portable methods on Context, not raw component command strings.
mod callbacks;
mod host;
mod operations;

pub(crate) use callbacks::Callbacks;
use cordis_sdk_core::{
    Context, FutureResult, LocalFuture, Value,
    backend::{Backend, Operation, Reply},
    refused,
};
use std::{any::Any, rc::Rc};

#[derive(Clone)]
pub(crate) struct ComponentBackend {
    token: String,
    callbacks: Callbacks,
}
impl ComponentBackend {
    pub(crate) fn context(token: String, callbacks: Callbacks) -> Context {
        Context::from_backend(Rc::new(Self { token, callbacks }))
    }
}
impl Backend for ComponentBackend {
    fn validation(&self) -> cordis_sdk_core::Validation {
        self.callbacks.validation()
    }
    fn submit(&self, operation: Operation) -> LocalFuture<Reply> {
        let backend = self.clone();
        Box::pin(async move { backend.execute(operation).await })
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Explicit access to the component profile's raw request API.
/// Kept for advanced users and protocol conformance fixtures.
pub trait ComponentContextExt {
    fn request(&self, command: &str, args: Value) -> FutureResult;
}
impl ComponentContextExt for Context {
    fn request(&self, command: &str, args: Value) -> FutureResult {
        let Some(backend) = self.backend().as_any().downcast_ref::<ComponentBackend>() else {
            return Box::pin(async {
                Err(refused(
                    "unsupported_backend",
                    "Context is not a component Context",
                ))
            });
        };
        let backend = backend.clone();
        let command = command.to_owned();
        Box::pin(async move { backend.request(&command, args).await })
    }
}
