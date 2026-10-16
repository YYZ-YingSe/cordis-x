#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
include!("../../../tests/support/resource_probe_client.rs");
cordis_guest::export!(crate::ProbeClient);
