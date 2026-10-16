//! Build-time module wiring. Paths are supplied by the owning crate's build.rs.
use std::{fs, path::Path};

pub fn generate(generated: &Path, resources: &Path) {
    println!("cargo:rerun-if-changed={}", generated.display());
    println!("cargo:rerun-if-changed={}", resources.display());
    let mut entries = fs::read_dir(generated)
        .expect("generate repository contracts first: npm run generate")
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.join(".cordis-generated.json").is_file())
        .collect::<Vec<_>>();
    entries.sort();
    let mut modules = String::new();
    let mut data = String::from("pub mod resources {\n");
    for directory in entries {
        let name = directory.file_name().unwrap().to_str().unwrap();
        let module = directory.join("rust/mod.rs");
        if module.is_file() {
            modules += &format!("#[path = {module:?}]\npub mod {name};\n");
        }
        data += &format!("pub mod {name} {{\n");
        for (name, file) in [
            ("DESCRIPTOR", "descriptor.json"),
            ("SCHEMA", "schema.base64"),
        ] {
            let path = directory.join("resources").join(file);
            data += &format!("pub const {name}: &str = include_str!({path:?});\n");
        }
        let mut interfaces = fs::read_dir(directory.join("resources/interfaces"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        interfaces.sort();
        for path in interfaces {
            let name = path
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .to_ascii_uppercase();
            data += &format!("pub const {name}: &str = include_str!({path:?});\n");
        }
        data += "}\n";
    }
    let config = resources.join("config/profile-app.json");
    if config.is_file() {
        data += &format!("pub const PROFILE_APP: &str = include_str!({config:?});\n");
    }
    data += "}\n";
    // Compiler tests need the source inputs, not a second hand-maintained path.
    let contracts = resources.join("contracts");
    modules += &format!("pub const CONTRACT_SOURCES: &str = {contracts:?};\n");
    let output = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    fs::write(output.join("bindings.rs"), modules + &data).unwrap();
}
