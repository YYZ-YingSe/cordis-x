#[path = "../tools/codegen/rust_bindings.rs"]
mod bindings;

fn main() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    println!(
        "cargo:rustc-env=CORDIS_NATIVE_BIN={}",
        root.join("../build/native/bin").display()
    );
    bindings::generate(&root.join("generated"), &root.join("resources"));
}
