//! Portable Rust plugin API, independent of execution backend and encoding.
//!
//! Backends implement [`backend::Backend`]; applications use [`Context`] and
//! [`Plugin`]. Generated method/resource calls carry owned [`Message`] objects;
//! JSON is an explicit configuration or external gateway representation.
pub mod backend;
pub mod composition;
pub mod context;
pub mod effects;
pub mod error;
pub mod events;
pub mod json;
pub mod message;
pub mod plugin;
pub mod resource;
pub mod scope;
pub mod task;
pub mod typed;
pub use message::Message;

pub use composition::Child;
pub use context::Context;
pub use cordis_contract::{
    Control as ControlContract, Descriptor, Dispatch, Endpoint, Interface, MessageContract, Method,
    Resource as ResourceContract, Validation,
};
pub use effects::{Effect, Sequence, Step};
pub use error::{ErrorInfo, Failure, Result, error, refused};
pub use events::Next;
pub use plugin::{Bindings, Contract, FutureResult, LocalFuture, Plugin, PluginManifest};
pub use resource::{Resource, ResourceObject};
pub use scope::Scope;
pub use serde_json::{Value, json};
pub use task::Task;

pub use cordis_contract::portable as contract;
