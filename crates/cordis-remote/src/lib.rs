//! Optional process/session and HTTP adapters. No language-to-transport mapping.
#[cfg(feature = "loader")]
mod artifact;
mod composition;
mod dependency;
mod dependency_resources;
mod host;
mod host_failure;
mod references;
pub use composition::CompositionLimits;
#[cfg(feature = "loader")]
pub mod loader;
#[cfg(feature = "loader")]
pub use artifact::{ArtifactOptions, Launch, ProcessArtifact};
mod effects;
mod events;
mod middleware;

mod error;
mod frame;
mod http;
mod plugin;
mod process;
mod resource;
mod scope;
mod sequence;
mod session;
mod task;
mod transport;
pub use cordis_contract::{Descriptor, Dispatch, Interface, MessageContract, Method};
pub use cordis_wire::{Bytes, Payload, Value, json};

pub use error::{Error, Result};
pub use plugin::{Endpoint, RemoteService, SessionSpec};
pub use process::ProcessSpec;
pub use resource::RemoteResource;
pub use transport::{Connected, Connector, Peer};

pub(crate) fn json_body(shape: &MessageContract, value: &Value) -> Result<Bytes> {
    use prost::Message;
    let value = shape.parse_json(value).map_err(Error::InvalidInput)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(value.encoded_len())
        .map_err(|error| Error::InvalidInput(error.to_string()))?;
    value.encode_raw(&mut bytes);
    Ok(bytes.into())
}
pub(crate) fn body_json(shape: &MessageContract, bytes: &Bytes) -> Result<Value> {
    let message = shape.parse_binary(bytes).map_err(Error::Protocol)?;
    serde_json::to_value(&message).map_err(|e| Error::Protocol(e.to_string()))
}
