fn main() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = root.join("../../protocols/generated/runtime/resources/schema.base64");
    println!("cargo:rerun-if-changed={}", path.display());
    println!("cargo:rustc-env=CORDIS_RUNTIME_SCHEMA={}", path.display());
}
