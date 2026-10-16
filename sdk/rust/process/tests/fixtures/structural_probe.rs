include!("../../../tests/support/structural_probe.rs");
fn main() {
    cordis_sdk_process::run::<ContractProbe>(cordis_sdk_process::Options::default()).unwrap();
}
