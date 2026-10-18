//! Real C++ artifacts shared by the Rust/process and Rust/ABI resource suites.
use std::{
    path::{Path, PathBuf},
    sync::OnceLock,
};

pub fn build() -> &'static Path {
    static BUILD: OnceLock<PathBuf> = OnceLock::new();
    BUILD.get_or_init(|| {
        let mut targets = vec![
            "cordis_resource_counter",
            "cordis_resource_client",
            "cordis_resource_probe_client",
        ];
        if cfg!(unix) {
            targets.push("cordis_resources_process");
        }
        cordis_test_contracts::native::build(&targets)
    })
}
