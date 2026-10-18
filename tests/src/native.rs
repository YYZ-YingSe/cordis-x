//! Repository native fixtures. CMake owns freshness; the build script owns paths and locking.
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::OnceLock,
};

pub fn build(targets: &[&str]) -> PathBuf {
    let python = std::env::var_os("PYTHON")
        .unwrap_or_else(|| if cfg!(windows) { "python" } else { "python3" }.into());
    let mut command = Command::new(python);
    command.args([env!("CORDIS_TEST_BUILD_SCRIPT"), "--native"]);
    if !targets.is_empty() {
        command.arg("--target").args(targets);
    }
    let output = command
        .output()
        .expect("Python, CMake and a C++20 compiler are required");
    assert!(
        output.status.success(),
        "{command:?} failed ({}):\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    PathBuf::from(env!("CORDIS_NATIVE_BIN"))
}

pub fn all() -> &'static Path {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| build(&[]))
}

/// Native Rust plugins use Cargo's shared dependency cache too. Feature variants
/// are tracked by Cargo; these fixtures do not require separate target trees.
pub fn cargo_target() -> PathBuf {
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target"));
    Path::new(env!("CORDIS_WORKSPACE_ROOT")).join(target)
}
