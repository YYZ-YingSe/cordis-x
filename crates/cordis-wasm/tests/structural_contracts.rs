use backend::{ContextResource as ForeignResource, ContextService as Foreign};
use cordis_wasm as backend;
#[path = "support/resources.rs"]
mod artifacts;
#[allow(dead_code)]
#[path = "../../../sdk/rust/tests/support/structural_probe.rs"]
mod probe;
include!("../../../sdk/rust/tests/support/structural_suite.rs");
fn load(capabilities: backend::Capabilities) -> backend::Result<backend::ContextComponent> {
    backend::ContextComponent::from_bytes(
        artifacts::bytes("wasm_structural_probe"),
        backend::Limits::default(),
        capabilities,
    )
}
