use std::{collections::BTreeMap, path::Path, process::Command, sync::OnceLock};

pub fn bytes(name: &str) -> &'static [u8] {
    static BUILD: OnceLock<BTreeMap<String, Vec<u8>>> = OnceLock::new();
    BUILD.get_or_init(|| {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        [
            ("cpp_resource_counter", "sdk/cpp/examples/resources/component_counter.cpp"),
            ("cpp_resource_client", "sdk/cpp/examples/resources/component_client.cpp"),
            ("cpp_resource_probe_client", "sdk/cpp/tests/component/resource_probe_client.cpp"),
            ("cpp_resource_probe", "sdk/cpp/tests/component/resource_probe.cpp"),
        ].into_iter().map(|(name, source)| {
            let output = root.join(format!("target/cpp-resource-components/{name}.wasm"));
            let built = Command::new("python3").current_dir(&root)
                .args(["sdk/cpp/tools/build-component.py", "--sdk-source", "sdk/cpp", "--include", "examples/generated/cpp", "--proto-path", "examples/resources/contracts", "--core", "--proto", "fixtures/counters.proto", source])
                .arg(&output).output().unwrap();
            assert!(built.status.success(), "{}\n{}", String::from_utf8_lossy(&built.stdout), String::from_utf8_lossy(&built.stderr));
            let core = std::fs::read(output).unwrap();
            let component = wit_component::ComponentEncoder::default().module(&core).unwrap()
                .adapter("wasi_snapshot_preview1", wasi_preview1_component_adapter_provider::WASI_SNAPSHOT_PREVIEW1_REACTOR_ADAPTER).unwrap()
                .validate(true).encode().unwrap();
            (name.to_owned(), component)
        }).collect()
    }).get(name).expect("C++ resource fixture").as_slice()
}
