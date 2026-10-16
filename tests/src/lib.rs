//! Shared generated test contracts and fixtures, accessed by module name.
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
pub const PYTHON_RUNNER: &str = env!("CORDIS_PYTHON_RUNNER");
