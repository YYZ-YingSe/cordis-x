use cordis_wasm::{Component, Limits};
use std::{path::Path, process::Command, sync::OnceLock};

pub fn core_bytes() -> &'static [u8] {
    static BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    BYTES.get_or_init(|| build_core(true))
}
#[allow(dead_code)] // The post-return suite uses the core bytes directly.
pub fn component() -> Component {
    static COMPONENT: OnceLock<Component> = OnceLock::new();
    COMPONENT
        .get_or_init(|| Component::from_bytes(bytes(), Limits::default()).unwrap())
        .clone()
}

#[allow(dead_code)]
pub fn bytes() -> &'static [u8] {
    static BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    BYTES.get_or_init(|| package(core_bytes()))
}
pub fn package(module: &[u8]) -> Vec<u8> {
    wit_component::ComponentEncoder::default()
        .module(module)
        .unwrap()
        .validate(true)
        .encode()
        .unwrap()
}

fn build_core(fixture: bool) -> Vec<u8> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let target = root.join(if fixture {
        "target/wasm-guest"
    } else {
        "target/wasm-calculator-normal"
    });
    let mut command = Command::new(env!("CARGO"));
    command
        .current_dir(&root)
        .args([
            "build",
            "-p",
            "cordis-wasm-calculator",
            "--target",
            "wasm32-unknown-unknown",
            "--release",
            "--offline",
            "--locked",
        ])
        .arg("--target-dir")
        .arg(&target);
    if fixture {
        command.args(["--features", "test-fixture"]);
    }
    // A separate guest compiler is useful on Homebrew installations without
    // a compatible wasm standard library. CI uses rustup's normal target.
    if let Some(rustc) = std::env::var_os("CORDIS_WASM_RUSTC") {
        command.env("RUSTC", rustc);
    }
    command.env_remove("CARGO_ENCODED_RUSTFLAGS");
    let output = command.output().expect("start guest compiler");
    assert!(
        output.status.success(),
        "guest build failed (install wasm32-unknown-unknown):\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    std::fs::read(target.join("wasm32-unknown-unknown/release/cordis_wasm_calculator.wasm"))
        .unwrap()
}
#[allow(dead_code)] // Only normal-interface tests need this separate artifact.
pub fn normal_component() -> Component {
    static COMPONENT: OnceLock<Component> = OnceLock::new();
    COMPONENT
        .get_or_init(|| {
            Component::from_bytes(&package(&build_core(false)), Limits::default()).unwrap()
        })
        .clone()
}
