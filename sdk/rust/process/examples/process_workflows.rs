fn main() -> cordis_sdk_core::Result<()> {
    cordis_sdk_process::run::<cordis_portable_example::workflows::Plugin>(
        cordis_sdk_process::Options {
            config: cordis_sdk_core::json!({}),
            http: !std::env::args().any(|arg| arg == "--stdio-only"),
        },
    )
}
