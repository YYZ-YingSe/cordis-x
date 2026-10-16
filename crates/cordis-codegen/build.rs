fn main() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = root.join("../../protocols/protobuf/cordis/options.proto");
    println!("cargo:rerun-if-changed={}", path.display());
    println!("cargo:rustc-env=CORDIS_OPTIONS_PROTO={}", path.display());
    let path = root.join("../../protocols/protobuf/cordis/types.proto");
    println!("cargo:rerun-if-changed={}", path.display());
    println!("cargo:rustc-env=CORDIS_TYPES_PROTO={}", path.display());
}
