//! The common protobuf carrier for the WIT byte-list boundary.
//! WIT owns canonical-ABI list allocation/freeing; the receiver validates the
//! frame and independently checks every business body against its contract.
pub use cordis_wire::{Bytes, Error, Initialization, Payload, Result, Validation, Value, json};
pub const WIT: &str = include_str!(env!("CORDIS_PLUGIN_WIT"));
pub const MAX_BYTES: usize = cordis_wire::MAX_MESSAGE;
pub fn encode(payload: &Payload) -> Result<Vec<u8>> {
    payload.encode_to_vec()
}
pub fn decode(bytes: Vec<u8>) -> Result<Payload> {
    decode_with(bytes, Validation::Checked)
}
pub fn decode_with(bytes: Vec<u8>, validation: Validation) -> Result<Payload> {
    Payload::decode_with(bytes.into(), validation)
}
