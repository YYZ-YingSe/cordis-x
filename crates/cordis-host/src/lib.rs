//! Host-selected bindings to real Runtime services. Execution adapters implement
//! endpoints; application bindings do not select a transport or own a second runtime.
mod binding;
mod body;
pub use binding::{
    EventBinding, Factory, FactoryBinding, HostBindings, MiddlewareBinding, ProvisionBinding,
};
mod endpoint;
mod resource;
pub use body::Body;
pub use cordis_contract::{Failure, refused};
pub use endpoint::{Dependency, Provider};
pub use resource::{Resource, ResourceProvider, WeakResource, WeakResourceProvider};
pub type Result<T> = std::result::Result<T, Failure>;

pub fn unknown(error: impl ToString) -> Failure {
    Failure::Unknown(cordis_contract::ErrorInfo {
        code: "outcome_unknown".into(),
        message: error.to_string(),
    })
}

/// The service callback returns its Failure inside Ok. Closed/Retired on the
/// outer Runtime result therefore mean admission failed before that callback.
/// A panic or lost executor completion still has an unknown business outcome.
pub(crate) fn service_failure(error: cordis_runtime::Error) -> Failure {
    match error {
        cordis_runtime::Error::Portable(failure) => failure,
        cordis_runtime::Error::Closed => refused("closed", "service admission is closed"),
        error @ cordis_runtime::Error::Retired(_) => refused("retired", error),
        other => unknown(other),
    }
}
