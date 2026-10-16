//! Generated calculator business over the synchronous, resource-owning WIT profile.
#[cfg(not(any(feature = "test-fixture", feature = "structural-fixture")))]
mod adapter;
#[cfg(any(feature = "test-fixture", feature = "structural-fixture"))]
mod raw_fixture;
#[cfg(not(any(feature = "test-fixture", feature = "structural-fixture")))]
mod service;
pub use cordis_example_contracts::calculator_wasm as generated;
