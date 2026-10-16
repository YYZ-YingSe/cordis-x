fn main() {
    let wit =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../protocols/component/wit");
    println!("cargo:rerun-if-changed={}", wit.display());
    let output = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    std::fs::write(
        output.join("plugin_bindings.rs"),
        format!(r#"wasmtime::component::bindgen!({{ path: {wit:?}, world: "plugin",  }});"#),
    )
    .unwrap();
    std::fs::write(output.join("managed_bindings.rs"), format!(r#"wasmtime::component::bindgen!({{ path: {wit:?}, world: "managed-plugin", exports: {{ default: async }}, }});"#)).unwrap();
}
