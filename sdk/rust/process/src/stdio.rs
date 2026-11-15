use super::*;
use cordis_wire::{Bytes, Incoming};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
pub const BUFFER_CAPACITY: usize = 8 * 1024;
pub async fn read(
    reader: &mut (impl AsyncRead + Unpin),
    validation: cordis_sdk_core::Validation,
    capacity: usize,
) -> Result<Incoming> {
    let length = reader.read_u32().await.map_err(unknown)? as usize;
    if !(1..=capacity).contains(&length) {
        return Err(refused("protocol", "invalid frame length"));
    }
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length).map_err(unknown)?;
    bytes.resize(length, 0);
    reader.read_exact(&mut bytes).await.map_err(unknown)?;
    cordis_wire::decode_with(bytes.into(), validation).map_err(unknown)
}
pub async fn write(writer: &mut (impl AsyncWrite + Unpin), bytes: &Bytes) -> Result<()> {
    if bytes.is_empty() || bytes.len() > u32::MAX as usize {
        return Err(unknown("invalid frame length"));
    }
    writer
        .write_u32(bytes.len() as u32)
        .await
        .map_err(unknown)?;
    writer.write_all(bytes).await.map_err(unknown)?;
    writer.flush().await.map_err(unknown)
}
