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
pub unsafe fn read_text(raw: Slice) -> Result<String> {
    let bytes = unsafe { read_slice(raw.data, raw.len)? };
    let text = str::from_utf8(bytes).map_err(|_| invalid_value())?;
    let mut owned = String::new();
    owned
        .try_reserve_exact(text.len())
        .map_err(|_| invalid_value())?;
    owned.push_str(text);
    Ok(owned)
}
pub unsafe fn read_payload(
    raw: &RawBuffer,
    validation: cordis_sdk_core::Validation,
) -> Result<Payload> {
    // Copy while borrowed input or result storage is live. Decoded message fields
    // share this owned allocation and can safely outlive submission/release.
    let borrowed = unsafe { read_slice(raw.data, raw.len)? };
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(borrowed.len())
        .map_err(|_| invalid_value())?;
    bytes.extend_from_slice(borrowed);
    Payload::decode_with(bytes.into(), validation).map_err(unknown)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copied_large_inputs_outlive_caller_storage() {
        for validation in [
            cordis_sdk_core::Validation::Contract,
            cordis_sdk_core::Validation::Checked,
            cordis_sdk_core::Validation::Audit,
        ] {
            let expected = Payload::message(vec![173; 3 * 1024 * 1024].into());
            let mut bytes = expected.encode().unwrap().to_vec();
            let raw = RawBuffer {
                data: bytes.as_ptr(),
                len: bytes.len(),
            };
            let copied = unsafe { read_payload(&raw, validation) }.unwrap();
            bytes.fill(0);
            assert_eq!(copied, expected);
        }
        let mut text = "x".repeat(2 * 1024 * 1024);
        let copied = unsafe { read_text(slice(&text)) }.unwrap();
        text.clear();
        assert_eq!(copied.len(), 2 * 1024 * 1024);
    }

    #[test]
    fn invalid_slices_fail_before_foreign_memory_is_read() {
        for raw in [
            RawBuffer {
                data: std::ptr::null(),
                len: 1,
            },
            RawBuffer {
                data: std::ptr::dangling(),
                len: isize::MAX as usize + 1,
            },
        ] {
            assert!(unsafe { read_payload(&raw, cordis_sdk_core::Validation::Checked) }.is_err());
        }
        let raw = Slice {
            data: std::ptr::null(),
            len: 0,
        };
        assert_eq!(unsafe { read_text(raw) }.unwrap(), "");
    }
}
