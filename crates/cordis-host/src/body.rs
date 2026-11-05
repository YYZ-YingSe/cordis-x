use cordis_contract::MessageContract;
use cordis_sdk_core::Message;
use cordis_wire::{Payload, local::Envelope};

/// A host-local boundary carrier, not a second business message or wire format.
/// Native objects remain native; encoded snapshots retain their local receipts.
#[derive(Clone)]
pub struct Body(Repr);
#[derive(Clone)]
enum Repr {
    Native(Message),
    Encoded(Envelope),
}
impl From<Message> for Body {
    fn from(value: Message) -> Self {
        Self(Repr::Native(value))
    }
}
impl From<Envelope> for Body {
    fn from(value: Envelope) -> Self {
        Self(Repr::Encoded(value))
    }
}
impl Body {
    pub fn into_message(self, shape: &MessageContract) -> Result<Message, String> {
        match self.0 {
            Repr::Native(value) => value.bind(shape),
            Repr::Encoded(value) => value.into_message(shape),
        }
    }
    pub fn into_envelope(self) -> Result<Envelope, String> {
        match self.0 {
            Repr::Encoded(value) => Ok(value),
            Repr::Native(value) => Envelope::from_message(value),
        }
    }
}

impl Body {
    pub fn from_bytes(bytes: cordis_wire::Bytes) -> Self {
        Envelope::from(Payload::message(bytes)).into()
    }
    pub fn into_bytes(self) -> Result<cordis_wire::Bytes, String> {
        self.into_envelope()?.one().map_err(|e| e.to_string())
    }
}
impl From<cordis_wire::Bytes> for Body {
    fn from(bytes: cordis_wire::Bytes) -> Self {
        Self::from_bytes(bytes)
    }
}
impl std::fmt::Debug for Body {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Repr::Native(value) => f.debug_tuple("Native").field(value).finish(),
            Repr::Encoded(_) => f.write_str("Encoded(..)"),
        }
    }
}
