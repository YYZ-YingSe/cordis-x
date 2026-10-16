fn main() {
    let options = cordis_sdk_process::Options {
        config: cordis_sdk_core::json!({}),
        http: !std::env::args().any(|a| a == "--stdio-only"),
    };
    if let Err(error) = cordis_sdk_process::run::<cordis_portable_example::Example>(options) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
