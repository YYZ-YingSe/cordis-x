fn main() {
    let root = "../../protocols/protobuf";
    println!("cargo:rerun-if-changed={root}/transport.proto");
    prost_build::Config::new()
        .bytes([".cordis.transport.Payload.messages"])
        .file_descriptor_set_path(
            std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap()).join("transport.bin"),
        )
        .compile_protos(&[format!("{root}/transport.proto")], &[root])
        .expect("generate the current protobuf transport");
}
