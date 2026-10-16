use crate::{Error, Result};
use cordis_wire::{Bytes, Incoming, MAX_MESSAGE};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub(crate) const BUFFER_CAPACITY: usize = 8 * 1024;
pub(crate) async fn read(
    reader: &mut (impl AsyncRead + Unpin),
    validation: cordis_contract::Validation,
) -> Result<Incoming> {
    let first = match reader.read_u8().await {
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Err(Error::Closed),
        other => other?,
    };
    let mut rest = [0; 3];
    reader.read_exact(&mut rest).await?;
    let length = u32::from_be_bytes([first, rest[0], rest[1], rest[2]]) as usize;
    if !(1..=MAX_MESSAGE).contains(&length) {
        return Err(Error::Protocol("invalid frame length".into()));
    }
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes).await?;
    Ok(cordis_wire::decode_with(bytes.into(), validation)?)
}
pub(crate) async fn write(writer: &mut (impl AsyncWrite + Unpin), bytes: &Bytes) -> Result<()> {
    if bytes.is_empty() || bytes.len() > MAX_MESSAGE {
        return Err(Error::Protocol("invalid frame length".into()));
    }
    writer.write_u32(bytes.len() as u32).await?;
    writer.write_all(bytes).await?;
    writer.flush().await?;
    Ok(())
}
