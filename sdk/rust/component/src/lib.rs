//! Component execution adapter for the portable Rust plugin API.
//!
//! Business APIs come from `cordis-sdk-core`. Only this crate imports WIT and
//! implements native async component calls, callback storage and resource ownership.
pub mod adapter;
#[doc(hidden)]
pub mod bindings;
mod conversion;
mod exports;
mod runtime;

pub use adapter::ComponentContextExt;
pub use cordis_sdk_core::{
    Bindings, Child, Context, Contract, Effect, ErrorInfo, Failure, FutureResult, LocalFuture,
    Next, Plugin, PluginManifest, Resource, ResourceObject, Result, Scope, Sequence, Step, Task,
    Value, error, json, refused,
};

/// Implementation details used by export! in downstream crates.
#[doc(hidden)]
pub mod __private {
    pub use crate::conversion::describe;
    pub use crate::runtime::{Instance, call};
}
