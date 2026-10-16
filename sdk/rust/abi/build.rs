fn main() {
    let headers =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../protocols/abi/include");
    let bindings = bindgen::Builder::default()
        .header(headers.join("cordis/abi_resources.h").to_string_lossy())
        .clang_arg(format!("-I{}", headers.display()))
        .allowlist_type("Cordis.*")
        .allowlist_var("CORDIS_.*")
        .blocklist_function(".*")
        .derive_default(true)
        .layout_tests(false)
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("generate Cordis ABI bindings from canonical headers");
    bindings
        .write_to_file(
            std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap()).join("abi.rs"),
        )
        .unwrap();
}
