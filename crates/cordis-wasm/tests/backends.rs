#[path = "../../cordis-abi/tests/support/backends.rs"]
mod common;
mod support;
use common::{Calculator, Counter};
use cordis_abi::{Value, json};
use cordis_sdk::*;
use cordis_wasm::{WasmResource, WasmService};
common::adapter!(WasmService, WasmResource);
async fn compare(endpoint: Option<cordis_remote::Endpoint>) {
    let expected = common::compare(endpoint).await;
    let service = Service::new("example.calculator", "1").with_descriptor(
        cordis_contract::portable::decode(
            &cordis_example_contracts::resources::calculator_wasm::SERVICE
                .parse()
                .unwrap(),
        )
        .unwrap(),
    );
    let plugin = support::normal_component()
        .plugin("wasm", &service, json!({}))
        .unwrap();
    assert_eq!(common::scenario(service, plugin).await, expected);
}
#[tokio::test]
async fn native_abi_wasm_share_behavior_and_cleanup_trace() {
    compare(None).await;
}
#[tokio::test]
async fn native_abi_ipc_wasm_share_behavior_and_cleanup_trace() {
    compare(Some(cordis_remote::Endpoint::Ipc)).await;
}
#[tokio::test]
async fn native_abi_http_wasm_share_behavior_and_cleanup_trace() {
    compare(Some(cordis_remote::Endpoint::Http)).await;
}
