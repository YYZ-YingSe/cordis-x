//! Explicit build tools. Resolving tools never installs or changes their environment.
use crate::{Diagnostic, Result};
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct GeneratorTools {
    pub protoc: PathBuf,
    pub protoc_gen_es: PathBuf,
}

impl Default for GeneratorTools {
    fn default() -> Self {
        Self {
            protoc: std::env::var_os("PROTOC")
                .map(PathBuf::from)
                .unwrap_or_else(|| "protoc".into()),
            protoc_gen_es: std::env::var_os("CORDIS_PROTOC_GEN_ES")
                .map(PathBuf::from)
                .unwrap_or_else(|| "protoc-gen-es".into()),
        }
    }
}

impl GeneratorTools {
    pub(crate) fn es_plugin(&self) -> Result<PathBuf> {
        let path = &self.protoc_gen_es;
        if path.components().count() > 1 || path.is_absolute() {
            return std::fs::canonicalize(path)
                .map_err(|e| Diagnostic::new(path.display().to_string(), e));
        }
        for directory in std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()) {
            let candidate = directory.join(path);
            if candidate.is_file() {
                return std::fs::canonicalize(candidate)
                    .map_err(|e| Diagnostic::new("protoc-gen-es", e));
            }
            #[cfg(windows)]
            for extension in ["exe", "cmd", "bat"] {
                let candidate = candidate.with_extension(extension);
                if candidate.is_file() {
                    return std::fs::canonicalize(candidate)
                        .map_err(|e| Diagnostic::new("protoc-gen-es", e));
                }
            }
        }
        Err(Diagnostic::new(
            "protoc-gen-es",
            format!(
                "{} not found on PATH; provide --protoc-gen-es or CORDIS_PROTOC_GEN_ES",
                path.display()
            ),
        ))
    }
}
