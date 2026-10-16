use std::{collections::BTreeMap, path::Path, process::Command, sync::OnceLock};

pub fn bytes(name: &str) -> &'static [u8] {
    static BUILD: OnceLock<BTreeMap<String, Vec<u8>>> = OnceLock::new();
    BUILD
        .get_or_init(|| {
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
            let target = root.join("target/wasm-resource-guest");
            let names = [
                "wasm_resource_counter",
                "wasm_structural_probe",
                "wasm_resource_client",
                "wasm_resource_probe",
                "wasm_resource_probe_client",
                "wasm_resource_protocol_probe",
            ];
            let mut command = Command::new(env!("CARGO"));
            command.current_dir(&root).args([
                "build",
                "-p",
                "cordis-guest",
                "--target",
                "wasm32-unknown-unknown",
                "--release",
                "--offline",
                "--locked",
            ]);
            for name in names {
                command.args(["--example", name]);
            }
            command.arg("--target-dir").arg(&target);
            if let Some(rustc) = std::env::var_os("CORDIS_WASM_RUSTC") {
                command.env("RUSTC", rustc);
            }
            command.env_remove("CARGO_ENCODED_RUSTFLAGS");
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            names
                .into_iter()
                .map(|name| {
                    let core = std::fs::read(target.join(format!(
                        "wasm32-unknown-unknown/release/examples/{name}.wasm"
                    )))
                    .unwrap();
                    let bytes = wit_component::ComponentEncoder::default()
                        .module(&core)
                        .unwrap()
                        .validate(true)
                        .encode()
                        .unwrap();
                    (name.to_owned(), bytes)
                })
                .collect()
        })
        .get(name)
        .expect("resource fixture")
        .as_slice()
}
