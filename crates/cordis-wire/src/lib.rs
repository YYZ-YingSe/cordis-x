//! Protobuf execution frames shared by IPC, HTTP, ABI and components.
pub mod binary;
pub mod local;
pub use binary::{CONTENT_TYPE, Payload};
pub use cordis_contract::Validation;
pub use prost::bytes::Bytes;
use serde::{Deserialize, Serialize};
pub use serde_json::{Value, json};
use std::fmt;

pub const MAX_MESSAGE: usize = 1024 * 1024;
pub use cordis_contract::portable::{MAX_DEPTH, MAX_INTEGER};
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(pub String);
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;
pub(crate) fn invalid(message: impl Into<String>) -> Error {
    Error(message.into())
}
pub fn validate(value: &Value, depth: usize) -> Result<()> {
    cordis_contract::portable::validate(value, depth).map_err(invalid)
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub incarnation: String,
    pub fiber: String,
    pub epoch: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RemoteError {
    pub code: String,
    pub message: String,
}
#[derive(Debug, Clone, PartialEq)]
pub struct Request {
    pub session: String,
    pub activation: Identity,
    pub operation: String,
    pub command: String,
    pub payload: Payload,
}
#[derive(Debug, Clone, PartialEq)]
pub struct Response {
    pub session: String,
    pub activation: Identity,
    pub operation: String,
    pub outcome: Outcome,
}
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Success { value: Payload },
    Failure { error: RemoteError },
    Refused { error: RemoteError },
    Unknown { error: RemoteError },
}
#[derive(Debug, Clone, PartialEq)]
pub enum Incoming {
    Request(Request),
    Response(Response),
}
impl Request {
    pub fn encode(&self) -> Result<Bytes> {
        binary::Frame {
            session: self.session.clone(),
            activation: self.activation.clone(),
            operation: self.operation.clone(),
            direction: binary::Direction::Request {
                command: self.command.clone(),
                payload: self.payload.clone(),
            },
        }
        .encode()
    }
    pub fn response(&self, outcome: Outcome) -> Response {
        Response {
            session: self.session.clone(),
            activation: self.activation.clone(),
            operation: self.operation.clone(),
            outcome,
        }
    }
    pub fn check(&self, response: &Response) -> Result<()> {
        if response.session != self.session
            || response.activation != self.activation
            || response.operation != self.operation
        {
            return Err(invalid("response session/activation/operation mismatch"));
        }
        Ok(())
    }
}
impl Response {
    pub fn encode(&self) -> Result<Bytes> {
        binary::Frame {
            session: self.session.clone(),
            activation: self.activation.clone(),
            operation: self.operation.clone(),
            direction: binary::Direction::Response(match &self.outcome {
                Outcome::Success { value } => binary::Outcome::Success(value.clone()),
                Outcome::Failure { error } => binary::Outcome::Rejected(error.clone()),
                Outcome::Refused { error } => binary::Outcome::Refused(error.clone()),
                Outcome::Unknown { error } => binary::Outcome::Unknown(error.clone()),
            }),
        }
        .encode()
    }
    pub fn is_success(&self) -> bool {
        matches!(self.outcome, Outcome::Success { .. })
    }
}
pub fn decode(bytes: Bytes) -> Result<Incoming> {
    decode_with(bytes, cordis_contract::Validation::Checked)
}
pub fn decode_with(bytes: Bytes, validation: cordis_contract::Validation) -> Result<Incoming> {
    let frame = binary::Frame::decode_with(bytes, validation)?;
    Ok(match frame.direction {
        binary::Direction::Request { command, payload } => Incoming::Request(Request {
            session: frame.session,
            activation: frame.activation,
            operation: frame.operation,
            command,
            payload,
        }),
        binary::Direction::Response(outcome) => Incoming::Response(Response {
            session: frame.session,
            activation: frame.activation,
            operation: frame.operation,
            outcome: match outcome {
                binary::Outcome::Success(value) => Outcome::Success { value },
                binary::Outcome::Rejected(error) => Outcome::Failure { error },
                binary::Outcome::Refused(error) => Outcome::Refused { error },
                binary::Outcome::Unknown(error) => Outcome::Unknown { error },
            },
        }),
    })
}

/// Host-owned instance settings. This envelope is accepted only at creation,
/// never as a per-message claim about completed validation.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Initialization {
    pub config: Value,
    pub validation: cordis_contract::Validation,
}
impl Initialization {
    pub fn encode(config: Value, validation: cordis_contract::Validation) -> Value {
        json!({"config": config, "validation": validation})
    }
    pub fn decode(value: Value) -> Result<Self> {
        serde_json::from_value(value).map_err(|e| invalid(e.to_string()))
    }
}
