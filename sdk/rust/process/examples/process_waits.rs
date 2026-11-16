fn main() -> cordis_sdk_core::Result<()> {
    cordis_sdk_process::run::<cordis_test_contracts::waits::Probe>(cordis_sdk_process::Options {
        config: cordis_sdk_core::json!({}),
        http: true,
    })
}
