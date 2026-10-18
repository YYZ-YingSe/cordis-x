//! Shared generated example contracts, accessed by module name.
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

/// Native example location selected by this repository's build configuration.
pub const NATIVE_BIN: &str = env!("CORDIS_NATIVE_BIN");
