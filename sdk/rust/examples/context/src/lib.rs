//! Component entry; the normal artifact exports only generated business methods.
#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code, unused_imports))]
#[cfg(feature = "test-fixture")]
mod fixture;
mod io;
mod service;
#[cfg(feature = "test-fixture")]
use fixture::Example;
#[cfg(not(feature = "test-fixture"))]
use service::Example;
cordis_guest::export!(Example);
