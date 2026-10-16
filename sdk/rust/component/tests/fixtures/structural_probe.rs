#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
include!("../../../tests/support/structural_probe.rs");
cordis_guest::export!(crate::ContractProbe);
