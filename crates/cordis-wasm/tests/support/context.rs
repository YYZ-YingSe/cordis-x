use std::{path::Path, process::Command, sync::OnceLock};

pub fn bytes() -> &'static [u8] {
    static BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    BYTES.get_or_init(|| build(false, true))
}
#[allow(dead_code)]
pub fn v2_bytes() -> &'static [u8] {
    static BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    BYTES.get_or_init(|| build(true, true))
}
#[allow(dead_code)]
pub fn normal_bytes() -> &'static [u8] {
    static BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    BYTES.get_or_init(|| build(false, false))
}
fn build(v2: bool, fixture: bool) -> Vec<u8> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let target = root.join(if !fixture {
        "target/wasm-context-normal"
    } else if v2 {
        "target/wasm-context-guest-v2"
    } else {
        "target/wasm-context-guest"
    });
    let mut command = Command::new(env!("CARGO"));
    command
        .current_dir(&root)
        .args([
            "build",
            "-p",
            "cordis-wasm-context-example",
            "--target",
            "wasm32-unknown-unknown",
            "--release",
            "--offline",
            "--locked",
        ])
        .arg("--target-dir")
        .arg(&target);
    if fixture {
        command.args([
            "--features",
            if v2 {
                "test-fixture,version-v2"
            } else {
                "test-fixture"
            },
        ]);
    }
    if let Some(rustc) = std::env::var_os("CORDIS_WASM_RUSTC") {
        command.env("RUSTC", rustc);
    }
    command.env_remove("CARGO_ENCODED_RUSTFLAGS");
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let core = std::fs::read(
        target.join("wasm32-unknown-unknown/release/cordis_wasm_context_example.wasm"),
    )
    .unwrap();
    wit_component::ComponentEncoder::default()
        .module(&core)
        .unwrap()
        .validate(true)
        .encode()
        .unwrap()
}
