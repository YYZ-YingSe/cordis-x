//! Optional process/session and HTTP adapters. No language-to-transport mapping.
#[cfg(feature = "loader")]
mod artifact;
mod composition;
mod dependency;
mod dependency_resources;
pub use composition::{CompositionLimits, FactoryBinding, ProvisionBinding};
#[cfg(feature = "loader")]
pub mod loader;
#[cfg(feature = "loader")]
pub use artifact::{ArtifactOptions, Launch, ProcessArtifact};
mod effects;
mod events;
mod middleware;
pub use events::EventBinding;
pub use middleware::MiddlewareBinding;
mod error;
mod frame;
mod http;
mod process;
mod resource;
mod scope;
mod sequence;
mod session;
mod task;
pub use cordis_contract::{Descriptor, Dispatch, Interface, MessageContract, Method};
pub use cordis_wire::{Bytes, Payload, Value, json};
pub use dependency::Dependency;
pub use error::{Error, Result};
pub use process::{Endpoint, ProcessSpec, RemoteService};
pub use resource::RemoteResource;

pub(crate) fn json_body(shape: &MessageContract, value: &Value) -> Result<Bytes> {
    use prost::Message;
    let value = shape.parse_json(value).map_err(Error::InvalidInput)?;
    if value.encoded_len() > cordis_wire::MAX_MESSAGE {
        return Err(Error::InvalidInput("message too large".into()));
    }
    Ok(value.encode_to_vec().into())
}
pub(crate) fn body_json(shape: &MessageContract, bytes: &Bytes) -> Result<Value> {
    let message = shape.parse_binary(bytes).map_err(Error::Protocol)?;
    serde_json::to_value(&message).map_err(|e| Error::Protocol(e.to_string()))
}
