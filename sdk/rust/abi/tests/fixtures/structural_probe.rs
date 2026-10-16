#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
include!("../../../tests/support/structural_probe.rs");
cordis_sdk_abi::export!(crate::ContractProbe);
