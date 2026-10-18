#[path = "../tools/codegen/rust_bindings.rs"]
mod bindings;

fn main() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    println!(
        "cargo:rustc-env=CORDIS_WORKSPACE_ROOT={}",
        root.parent().unwrap().display()
    );
    println!(
        "cargo:rustc-env=CORDIS_TEST_BUILD_SCRIPT={}",
        root.join("build.py").display()
    );
    println!(
        "cargo:rustc-env=CORDIS_NATIVE_BIN={}",
        root.join("../build/native/bin").display()
    );
    println!(
        "cargo:rustc-env=CORDIS_PYTHON_RUNNER={}",
        root.join("run-peer.py").display()
    );
    bindings::generate(&root.join("generated"), &root.join("resources"));
}
