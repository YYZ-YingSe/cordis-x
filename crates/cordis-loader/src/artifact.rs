use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{io::Read, path::Path};

/// A code identity, separate from configuration revisions and service contracts.
/// The registered adapter interprets `location` and must verify the exact bytes
/// it prepares. The built-in file helpers never infer identity from mtime.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    pub location: String,
    pub sha256: String,
}
impl ArtifactRef {
    pub fn from_bytes(location: impl Into<String>, bytes: &[u8]) -> Self {
        Self {
            location: location.into(),
            sha256: format!("{:x}", Sha256::digest(bytes)),
        }
    }

    /// Inspect a local file to construct a desired reference. Preparation will
    /// read it again and reject changes instead of silently selecting new code.
    pub fn from_file(path: impl AsRef<Path>, max_bytes: usize) -> Result<Self> {
        let path = path.as_ref();
        let location = path
            .to_str()
            .ok_or_else(|| Error::Artifact("artifact path must be UTF-8".into()))?;
        let bytes = read_file(location, max_bytes)?;
        Ok(Self::from_bytes(location, &bytes))
    }

    pub fn validate(&self) -> Result<()> {
        if self.location.trim().is_empty()
            || self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        {
            return Err(Error::Artifact(
                "artifact requires a location and 64 lowercase SHA-256 digits".into(),
            ));
        }
        Ok(())
    }

    pub fn verify(&self, bytes: &[u8]) -> Result<()> {
        self.validate()?;
        let actual = format!("{:x}", Sha256::digest(bytes));
        if self.sha256 != actual {
            return Err(Error::Artifact(format!(
                "{}: SHA-256 mismatch: expected {}, got {actual}",
                self.location, self.sha256
            )));
        }
        Ok(())
    }

    /// Read once, bound the input, and verify that same buffer. An adapter must
    /// prepare from this buffer, not reopen the mutable path during activation.
    pub fn read_file(&self, max_bytes: usize) -> Result<Vec<u8>> {
        self.validate()?;
        let bytes = read_file(&self.location, max_bytes)?;
        self.verify(&bytes)?;
        Ok(bytes)
    }
}
pub(crate) fn read_file(path: &str, max_bytes: usize) -> Result<Vec<u8>> {
    if max_bytes == 0 {
        return Err(Error::Artifact(
            "artifact size limit must be positive".into(),
        ));
    }
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .and_then(|file| {
            file.take(max_bytes.saturating_add(1) as u64)
                .read_to_end(&mut bytes)
        })
        .map_err(|error| Error::Artifact(format!("{path}: {error}")))?;
    if bytes.len() > max_bytes {
        return Err(Error::Artifact(format!(
            "{path}: artifact exceeds {max_bytes} bytes"
        )));
    }
    Ok(bytes)
}
