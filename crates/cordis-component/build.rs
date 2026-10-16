fn main() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = root.join("../../protocols/component/wit/plugin.wit");
    println!("cargo:rerun-if-changed={}", path.display());
    println!("cargo:rustc-env=CORDIS_PLUGIN_WIT={}", path.display());
}
