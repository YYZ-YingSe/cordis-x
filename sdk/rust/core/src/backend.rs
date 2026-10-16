//! Semantic adapter interface. Implementations own authority validation and
//! callback storage; the core does not implement another lifecycle machine.
use crate::{
    Context, FutureResult, LocalFuture, Message, MessageContract, Next, Result, Step, Value,
    refused,
};
use std::{any::Any, rc::Rc};

pub type Callback = Rc<dyn Fn(Context, Value) -> FutureResult>;
pub type MessageCallback = Rc<dyn Fn(Context, Message) -> crate::message::FutureResult>;
pub type Listener = Rc<dyn Fn(Context, Message) -> LocalFuture<crate::typed::Flow<Message>>>;
pub type Producer = Rc<dyn Fn(Context) -> LocalFuture<Step>>;
pub type Middleware = Rc<dyn Fn(Context, Message, Next) -> crate::message::FutureResult>;

/// Opaque adapter-owned capability. There is no required wire representation.
#[derive(Clone)]
pub struct Handle(Rc<dyn Any>);
impl Handle {
    pub fn new<T: Any>(value: T) -> Self {
        Self(Rc::new(value))
    }
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.0.downcast_ref()
    }
}

pub enum Reply {
    Value(Value),
    Message(Message),
    Flows(Vec<crate::typed::Flow<Message>>),
    Handle(Handle),
}
impl Reply {
    pub fn flows(self) -> Result<Vec<crate::typed::Flow<Message>>> {
        match self {
            Self::Flows(flows) => Ok(flows),
            _ => Err(refused("invalid_reply", "expected event results")),
        }
    }
    pub fn value(self) -> Result<Value> {
        match self {
            Self::Value(value) => Ok(value),
            _ => Err(refused("invalid_reply", "expected value")),
        }
    }
    pub fn handle(self) -> Result<Handle> {
        match self {
            Self::Handle(handle) => Ok(handle),
            _ => Err(refused("invalid_reply", "expected capability")),
        }
    }
    pub fn message(self) -> Result<Message> {
        match self {
            Self::Message(message) => Ok(message),
            _ => Err(refused("invalid_reply", "expected message")),
        }
    }
}

/// Operations contain semantic arguments and real callbacks, never command
/// strings, callback IDs, Session envelopes or serialized payloads.
pub enum Operation {
    Info,
    Metadata {
        name: String,
        revision: String,
    },
    Invoke {
        dependency: String,
        method: String,
        args: Message,
        result: MessageContract,
    },
    Acquire {
        dependency: String,
        kind: String,
        args: Message,
    },
    ResourceInvoke {
        resource: Handle,
        method: String,
        args: Message,
        result: MessageContract,
    },
    ResourceDispose {
        resource: Handle,
    },
    Defer {
        label: String,
        cleanup: Callback,
    },
    Effect {
        label: String,
        next: Producer,
    },
    Scope {
        label: String,
    },
    Cancelled,
    Sleep {
        milliseconds: u64,
    },
    Spawn {
        label: String,
        task: Callback,
    },
    On {
        event: String,
        signature: crate::Method,
        handler: Listener,
    },
    Emit {
        event: String,
        args: Message,
        signature: crate::Method,
        serial: bool,
    },
    Middleware {
        name: String,
        signature: crate::Method,
        handler: Middleware,
    },
    Run {
        name: String,
        args: Message,
        signature: crate::Method,
        terminal: MessageCallback,
    },
    Install {
        factory: String,
        config: Message,
    },
    Provide {
        service: String,
        value: Message,
    },
    Dispose {
        effect: Handle,
    },
    ScopeRun {
        scope: Handle,
        run: Callback,
    },
    ScopeDispose {
        scope: Handle,
    },
    TaskJoin {
        task: Handle,
    },
    Next {
        next: Handle,
        args: Message,
        result: MessageContract,
    },
    SequenceReady {
        sequence: Handle,
    },
    SequenceDispose {
        sequence: Handle,
    },
    ChildReady {
        child: Handle,
    },
    ChildDispose {
        child: Handle,
    },
}

/// One callback's authority. Cloning the owning Context must not extend it.
pub trait Backend: Any {
    fn validation(&self) -> crate::Validation {
        crate::Validation::Checked
    }
    fn submit(&self, operation: Operation) -> LocalFuture<Reply>;
    fn as_any(&self) -> &dyn Any;
}
impl Operation {
    /// Bind message policy once per semantic operation, outside field loops.
    pub(crate) fn with_validation(mut self, policy: crate::Validation) -> Self {
        fn checked(value: Message, policy: crate::Validation) -> crate::Result<Message> {
            value.validate(policy).map_err(crate::typed::violation)?;
            Ok(value.with_validation(policy))
        }
        fn callback(f: MessageCallback, policy: crate::Validation) -> MessageCallback {
            Rc::new(move |ctx, value| {
                let future = f(ctx, value.with_validation(policy));
                Box::pin(async move { checked(future.await?, policy) })
            })
        }
        match &mut self {
            Self::Invoke { args, result, .. }
            | Self::ResourceInvoke { args, result, .. }
            | Self::Next { args, result, .. } => {
                *args = args.clone().with_validation(policy);
                *result = result.with_validation(policy);
            }
            Self::Acquire { args, .. } => *args = args.clone().with_validation(policy),
            Self::Install { config, .. } => *config = config.clone().with_validation(policy),
            Self::Provide { value, .. } => *value = value.clone().with_validation(policy),
            Self::On { signature, .. } | Self::Middleware { signature, .. } => {
                *signature = signature.with_validation(policy)
            }
            Self::Emit {
                args, signature, ..
            }
            | Self::Run {
                args, signature, ..
            } => {
                *args = args.clone().with_validation(policy);
                *signature = signature.with_validation(policy);
            }
            _ => (),
        }
        match &mut self {
            Self::On { handler, .. } => {
                let original = handler.clone();
                *handler = Rc::new(move |ctx, value| {
                    let future = original(ctx, value.with_validation(policy));
                    Box::pin(async move {
                        match future.await? {
                            crate::typed::Flow::Continue => Ok(crate::typed::Flow::Continue),
                            crate::typed::Flow::Break(value) => {
                                checked(value, policy).map(crate::typed::Flow::Break)
                            }
                        }
                    })
                });
            }
            Self::Middleware { handler, .. } => {
                let original = handler.clone();
                *handler = Rc::new(move |ctx, value, next| {
                    let future = original(ctx, value.with_validation(policy), next);
                    Box::pin(async move { checked(future.await?, policy) })
                });
            }
            Self::Run { terminal, .. } => *terminal = callback(terminal.clone(), policy),
            _ => (),
        }
        self
    }
}
