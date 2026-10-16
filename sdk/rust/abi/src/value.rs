//! Owned protobuf buffers at the native C boundary.
#![allow(unsafe_code)]
use super::*;
use crate::raw::{CordisAbiBuffer as RawBuffer, CordisAbiSlice as Slice};
use std::{slice, str};
pub fn slice(value: &str) -> Slice {
    Slice {
        data: value.as_ptr(),
        len: value.len(),
    }
}
#[derive(Default)]
pub struct Input {
    bytes: Bytes,
}
impl Input {
    pub fn build(&mut self, value: &Payload) -> Result<RawBuffer> {
        self.bytes = value.encode().map_err(|e| refused("invalid_payload", e))?;
        Ok(RawBuffer {
            data: self.bytes.as_ptr(),
            len: self.bytes.len(),
        })
    }
}
fn invalid_value() -> Failure {
    unknown("invalid ABI buffer")
}
pub struct Budget(usize);
impl Budget {
    pub fn new() -> Self {
        Self(cordis_wire::MAX_MESSAGE)
    }
    fn take(&mut self, count: usize) -> Result<()> {
        self.0 = self.0.checked_sub(count).ok_or_else(invalid_value)?;
        Ok(())
    }
}
unsafe fn read_slice<'a>(data: *const u8, len: usize) -> Result<&'a [u8]> {
    if len == 0 {
        return Ok(&[]);
    }
    if data.is_null() || len > isize::MAX as usize {
        return Err(invalid_value());
    }
    // SAFETY: trusted caller guarantees readable storage for the borrowed length.
    Ok(unsafe { slice::from_raw_parts(data, len) })
}
pub unsafe fn read_text(raw: Slice, budget: &mut Budget) -> Result<String> {
    budget.take(raw.len)?;
    let bytes = unsafe { read_slice(raw.data, raw.len)? };
    str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| invalid_value())
}
pub unsafe fn read_payload(
    raw: &RawBuffer,
    budget: &mut Budget,
    validation: cordis_sdk_core::Validation,
) -> Result<Payload> {
    budget.take(raw.len)?;
    // Copy while borrowed input or result storage is live. Decoded message fields
    // share this owned allocation and can safely outlive submission/release.
    let bytes = Bytes::copy_from_slice(unsafe { read_slice(raw.data, raw.len)? });
    Payload::decode_with(bytes, validation).map_err(unknown)
}
