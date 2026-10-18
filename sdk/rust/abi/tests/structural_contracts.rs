use backend::{AbiResource as ForeignResource, AbiService as Foreign};
use cordis_abi as backend;
#[allow(dead_code)]
#[path = "../../tests/support/structural_probe.rs"]
mod probe;
include!("../../tests/support/structural_suite.rs");
fn load(capabilities: backend::Capabilities) -> backend::Result<backend::Library> {
    use std::{
        path::{Path, PathBuf},
        process::Command,
        sync::OnceLock,
    };
    static BINARY: OnceLock<PathBuf> = OnceLock::new();
    let path = BINARY.get_or_init(|| {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let target = cordis_test_contracts::native::cargo_target();
        let output = Command::new(env!("CARGO"))
            .current_dir(&root)
            .args([
                "build",
                "-p",
                "cordis-sdk-abi",
                "--example",
                "abi_structural_probe",
                "--locked",
                "--offline",
                "--target-dir",
            ])
            .arg(&target)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        target.join("debug/examples").join(format!(
            "{}abi_structural_probe{}",
            std::env::consts::DLL_PREFIX,
            std::env::consts::DLL_SUFFIX
        ))
    });
    // SAFETY: this library is built above from the repository-owned fixture.
    #[allow(unsafe_code)]
    let library = unsafe { backend::Library::load(path) }?;
    library.with_context(capabilities)
}
