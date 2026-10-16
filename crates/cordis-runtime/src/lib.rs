//! Native executor for the protocol-independent Cordis kernel.
mod broker;
mod context;
mod effects;
mod error;
mod events;
mod gate;
mod retention;
mod runtime;
mod scope;
mod sequence;
mod service;
mod view;

pub use broker::{Broker, BrokerRegistration, WeakBrokerRegistration};
pub use context::{Context, TaskHandle};
pub use effects::{EffectHandle, EffectSnapshot, EffectStatus, WeakEffectHandle};
pub use error::{Error, Result};
pub use events::{Break, Continue, Dispatch, Event, Middleware, Next};
pub use runtime::{
    OwnershipChange, Plugin, PluginHandle, RealmChange, RealmReport, Runtime, WeakPluginHandle,
};
pub use scope::{ScopeHandle, WeakScopeHandle};
pub use sequence::{
    EffectCompletion, EffectSequence, EffectStep, Inverse, StepCompletion, WeakEffectSequence,
};
pub use service::{Resource, Service, ServiceHandle, WeakResource};
pub use view::{InterceptConfig, Intercepts, View};

use futures_util::FutureExt;
use std::{future::Future, panic::AssertUnwindSafe, pin::Pin};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

async fn protected<T>(future: impl Future<Output = Result<T>>) -> Result<T> {
    AssertUnwindSafe(future)
        .catch_unwind()
        .await
        .unwrap_or_else(|payload| {
            let message = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "non-string panic".into());
            Err(Error::Panic(message))
        })
}
