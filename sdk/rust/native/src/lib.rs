//! Typed native Rust SDK. No serialization is introduced on native calls.
//!
//! Modules group the public API by responsibility. Root re-exports preserve
//! existing `use cordis_sdk::*` callers; implementation stays in the Runtime.
pub mod context;
pub mod effects;
pub mod events;
pub mod prelude;
pub mod runtime;
pub mod service;

pub use context::{Context, ContractId, InterceptConfig, Intercepts, RealmId, View};
pub use cordis_runtime::{BoxFuture, Error, Result};
pub use effects::{
    EffectCompletion, EffectHandle, EffectSequence, EffectSnapshot, EffectStatus, EffectStep,
    Inverse, ScopeHandle, StepCompletion, TaskHandle, WeakEffectHandle, WeakEffectSequence,
    WeakScopeHandle,
};
pub use events::{Break, Continue, Dispatch, Event, Middleware, Next};
pub use runtime::{
    Activation, FiberId, Lifecycle, Outcome, OwnershipChange, Plugin, PluginHandle, RealmChange,
    RealmReport, Runtime, Snapshot, WeakPluginHandle,
};
pub use service::{
    Broker, BrokerRegistration, Resource, Service, ServiceHandle, WeakBrokerRegistration,
};
