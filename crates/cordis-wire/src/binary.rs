//! The common protobuf carrier. Business bodies remain opaque owned bytes;
//! the receiving operation supplies their contract before entering business.
use crate::{Error, MAX_MESSAGE, Result, Value, invalid};
use cordis_contract::Validation;
use prost::{Message, bytes::Bytes};
use prost_reflect::DescriptorPool;
use std::sync::OnceLock;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/cordis.transport.rs"));
}

pub const MAX_PAYLOAD_MESSAGES: usize = 4096;
pub const CONTENT_TYPE: &str = "application/protobuf";

#[derive(Clone, Debug, PartialEq)]
pub struct Payload {
    pub control: Value,
    pub messages: Vec<Bytes>,
}
impl From<Value> for Payload {
    fn from(control: Value) -> Self {
        Self {
            control,
            messages: Vec::new(),
        }
    }
}
impl Payload {
    pub fn message(bytes: Bytes) -> Self {
        Self {
            control: Value::Null,
            messages: vec![bytes],
        }
    }
    pub fn with_message(control: Value, bytes: Bytes) -> Self {
        Self {
            control,
            messages: vec![bytes],
        }
    }
    pub fn one(&self) -> Result<Bytes> {
        match self.messages.as_slice() {
            [bytes] => Ok(bytes.clone()),
            _ => Err(invalid("operation requires exactly one business message")),
        }
    }
    pub fn control_only(self) -> Result<Value> {
        if !self.messages.is_empty() {
            return Err(invalid("unexpected business message"));
        }
        Ok(self.control)
    }
    pub fn encode(&self) -> Result<Bytes> {
        self.encode_to_vec().map(Bytes::from)
    }
    /// Encode directly into the owned allocation required by a WIT byte list.
    pub fn encode_to_vec(&self) -> Result<Vec<u8>> {
        bounded_vec(&self.to_proto()?)
    }
    pub fn decode(bytes: Bytes) -> Result<Self> {
        Self::decode_with(bytes, Validation::Checked)
    }
    pub fn decode_with(bytes: Bytes, validation: Validation) -> Result<Self> {
        check(&bytes, "Payload", validation)?;
        Self::from_proto(proto::Payload::decode(bytes).map_err(wire_error)?)
    }
    fn to_proto(&self) -> Result<proto::Payload> {
        validate_control(&self.control)?;
        if self.messages.len() > MAX_PAYLOAD_MESSAGES {
            return Err(invalid("too many business messages"));
        }
        let mut remaining = MAX_MESSAGE;
        for bytes in &self.messages {
            remaining = remaining
                .checked_sub(bytes.len())
                .ok_or_else(|| invalid("message too large"))?;
        }
        Ok(proto::Payload {
            control: Some(to_proto(&self.control)),
            messages: self.messages.clone(),
        })
    }
    fn from_proto(value: proto::Payload) -> Result<Self> {
        if value.messages.len() > MAX_PAYLOAD_MESSAGES {
            return Err(invalid("too many business messages"));
        }
        let control = from_proto(
            value
                .control
                .ok_or_else(|| invalid("missing control value"))?,
        )?;
        validate_control(&control)?;
        Ok(Self {
            control,
            messages: value.messages,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Frame {
    pub session: String,
    pub activation: crate::Identity,
    pub operation: String,
    pub direction: Direction,
}
#[derive(Clone, Debug, PartialEq)]
pub enum Direction {
    Request { command: String, payload: Payload },
    Response(Outcome),
}
#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    Success(Payload),
    Rejected(crate::RemoteError),
    Refused(crate::RemoteError),
    Unknown(crate::RemoteError),
}
impl Frame {
    pub fn encode(&self) -> Result<Bytes> {
        use proto::{frame::Direction as D, response::Outcome as O};
        let direction = match &self.direction {
            Direction::Request { command, payload } => D::Request(proto::Request {
                command: command.clone(),
                payload: Some(payload.to_proto()?),
            }),
            Direction::Response(outcome) => D::Response(proto::Response {
                outcome: Some(match outcome {
                    Outcome::Success(value) => O::Success(value.to_proto()?),
                    Outcome::Rejected(e) => O::Rejected(error_to_proto(e)),
                    Outcome::Refused(e) => O::Refused(error_to_proto(e)),
                    Outcome::Unknown(e) => O::Unknown(error_to_proto(e)),
                }),
            }),
        };
        bounded(&proto::Frame {
            session: self.session.clone(),
            activation: Some(proto::Identity {
                incarnation: self.activation.incarnation.clone(),
                fiber: self.activation.fiber.clone(),
                epoch: self.activation.epoch.clone(),
            }),
            operation: self.operation.clone(),
            direction: Some(direction),
        })
    }
    pub fn decode(bytes: Bytes) -> Result<Self> {
        Self::decode_with(bytes, Validation::Checked)
    }
    pub fn decode_with(bytes: Bytes, validation: Validation) -> Result<Self> {
        use proto::{frame::Direction as D, response::Outcome as O};
        check(&bytes, "Frame", validation)?;
        let frame = proto::Frame::decode(bytes).map_err(wire_error)?;
        let activation = frame
            .activation
            .ok_or_else(|| invalid("missing activation"))?;
        let direction = match frame
            .direction
            .ok_or_else(|| invalid("missing frame direction"))?
        {
            D::Request(request) => Direction::Request {
                command: request.command,
                payload: Payload::from_proto(
                    request.payload.ok_or_else(|| invalid("missing payload"))?,
                )?,
            },
            D::Response(response) => Direction::Response(
                match response.outcome.ok_or_else(|| invalid("missing outcome"))? {
                    O::Success(value) => Outcome::Success(Payload::from_proto(value)?),
                    O::Rejected(e) => Outcome::Rejected(error_from_proto(e)),
                    O::Refused(e) => Outcome::Refused(error_from_proto(e)),
                    O::Unknown(e) => Outcome::Unknown(error_from_proto(e)),
                },
            ),
        };
        Ok(Self {
            session: frame.session,
            activation: crate::Identity {
                incarnation: activation.incarnation,
                fiber: activation.fiber,
                epoch: activation.epoch,
            },
            operation: frame.operation,
            direction,
        })
    }
}
fn error_to_proto(e: &crate::RemoteError) -> proto::Error {
    proto::Error {
        code: e.code.clone(),
        message: e.message.clone(),
    }
}
fn error_from_proto(e: proto::Error) -> crate::RemoteError {
    crate::RemoteError {
        code: e.code,
        message: e.message,
    }
}
fn wire_error(e: impl std::fmt::Display) -> Error {
    invalid(e.to_string())
}
fn bounded(value: &impl Message) -> Result<Bytes> {
    bounded_vec(value).map(Bytes::from)
}
fn bounded_vec(value: &impl Message) -> Result<Vec<u8>> {
    let length = value.encoded_len();
    if length > MAX_MESSAGE {
        return Err(invalid("message too large"));
    }
    let mut bytes = Vec::with_capacity(length);
    value.encode_raw(&mut bytes);
    Ok(bytes)
}
fn check(bytes: &Bytes, name: &str, validation: Validation) -> Result<()> {
    if bytes.len() > MAX_MESSAGE {
        return Err(invalid("message too large"));
    }
    if !validation.audits() {
        return Ok(());
    }
    static DESCRIPTORS: OnceLock<DescriptorPool> = OnceLock::new();
    let pool = DESCRIPTORS.get_or_init(|| {
        DescriptorPool::decode(
            include_bytes!(concat!(env!("OUT_DIR"), "/transport.bin")).as_slice(),
        )
        .expect("generated transport descriptor")
    });
    let descriptor = pool
        .get_message_by_name(&format!("cordis.transport.{name}"))
        .expect("transport message");
    cordis_contract::protobuf::audit_binary(bytes, &descriptor).map_err(invalid)
}

/// Metadata limits leave headroom for the WKT and frame wrappers in standard
/// protobuf decoders. They do not constrain opaque typed business bodies.
pub const MAX_CONTROL_DEPTH: usize = 16;
pub const MAX_CONTROL_NODES: usize = 8_192;
pub fn validate_control(value: &Value) -> Result<()> {
    fn walk(value: &Value, depth: usize, nodes: &mut usize, bytes: &mut usize) -> Result<()> {
        if depth > MAX_CONTROL_DEPTH {
            return Err(invalid("control nesting too deep"));
        }
        *nodes = nodes
            .checked_sub(1)
            .ok_or_else(|| invalid("control node limit exceeded"))?;
        *bytes = bytes
            .checked_sub(2)
            .ok_or_else(|| invalid("control byte limit exceeded"))?;
        match value {
            Value::Number(v) => {
                let n = v
                    .as_f64()
                    .ok_or_else(|| invalid("invalid control number"))?;
                if !n.is_finite() || (n.fract() == 0.0 && n.abs() > crate::MAX_INTEGER as f64) {
                    return Err(invalid(
                        "control integers outside safe range require a decimal string",
                    ));
                }
            }
            Value::String(s) => {
                *bytes = bytes
                    .checked_sub(s.len())
                    .ok_or_else(|| invalid("control byte limit exceeded"))?
            }
            Value::Array(values) => {
                for value in values {
                    walk(value, depth + 1, nodes, bytes)?;
                }
            }
            Value::Object(values) => {
                for (key, value) in values {
                    *bytes = bytes
                        .checked_sub(key.len())
                        .ok_or_else(|| invalid("control byte limit exceeded"))?;
                    walk(value, depth + 1, nodes, bytes)?;
                }
            }
            _ => (),
        }
        Ok(())
    }
    let mut nodes = MAX_CONTROL_NODES;
    let mut bytes = MAX_MESSAGE;
    walk(value, 0, &mut nodes, &mut bytes)
}

fn to_proto(value: &Value) -> prost_types::Value {
    use prost_types::value::Kind;
    let kind = match value {
        Value::Null => Kind::NullValue(0),
        Value::Bool(v) => Kind::BoolValue(*v),
        Value::Number(v) => Kind::NumberValue(v.as_f64().expect("finite JSON number")),
        Value::String(v) => Kind::StringValue(v.clone()),
        Value::Array(v) => Kind::ListValue(prost_types::ListValue {
            values: v.iter().map(to_proto).collect(),
        }),
        Value::Object(v) => Kind::StructValue(prost_types::Struct {
            fields: v.iter().map(|(k, v)| (k.clone(), to_proto(v))).collect(),
        }),
    };
    prost_types::Value { kind: Some(kind) }
}
fn from_proto(value: prost_types::Value) -> Result<Value> {
    use prost_types::value::Kind;
    Ok(
        match value
            .kind
            .ok_or_else(|| invalid("missing control value branch"))?
        {
            Kind::NullValue(0) => Value::Null,
            Kind::NullValue(_) => return Err(invalid("invalid null enum")),
            Kind::BoolValue(v) => Value::Bool(v),
            Kind::NumberValue(v) => {
                if !v.is_finite() {
                    return Err(invalid("non-finite control number"));
                }
                // Preserve portable integer semantics; full-width IDs use strings.
                if v.fract() == 0.0 && v.abs() <= crate::MAX_INTEGER as f64 {
                    Value::from(v as i64)
                } else {
                    Value::from(v)
                }
            }
            Kind::StringValue(v) => Value::String(v),
            Kind::ListValue(v) => Value::Array(
                v.values
                    .into_iter()
                    .map(from_proto)
                    .collect::<Result<_>>()?,
            ),
            Kind::StructValue(v) => Value::Object(
                v.fields
                    .into_iter()
                    .map(|(k, v)| Ok((k, from_proto(v)?)))
                    .collect::<Result<_>>()?,
            ),
        },
    )
}
