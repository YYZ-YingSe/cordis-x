fn main() {
    let wit = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../../protocols/component/wit");
    println!("cargo:rerun-if-changed={}", wit.display());
    let output = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    std::fs::write(
        output.join("plugin_bindings.rs"),
        format!(r#"wit_bindgen::generate!({{ path: {wit:?}, world: "plugin", generate_all, }});"#),
    )
    .unwrap();
}
