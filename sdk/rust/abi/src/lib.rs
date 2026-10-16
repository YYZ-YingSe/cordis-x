//! Export a portable Rust plugin as a trusted native C ABI library.
//! Business futures run on a dedicated local executor; blocking host calls run
//! outside it so that Scope, events and middleware can reenter the plugin.
mod backend;
mod callbacks;
#[doc(hidden)]
pub mod ffi;
mod resources;
mod value;
mod worker;
#[doc(hidden)]
#[allow(
    unsafe_code,
    non_camel_case_types,
    non_snake_case,
    non_upper_case_globals,
    clippy::all
)]
pub mod raw {
    include!(concat!(env!("OUT_DIR"), "/abi.rs"));
}

use cordis_sdk_core::{ErrorInfo, Failure, Plugin, Result, Value, json, refused};
use cordis_wire::{Bytes, Payload};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    rc::Rc,
};
fn unknown(message: impl std::fmt::Display) -> Failure {
    Failure::Unknown(ErrorInfo {
        code: "abi".into(),
        message: message.to_string(),
    })
}

/// Define the C entry tables for a `cdylib`. The source of the ABI layout
/// is `protocols/abi/include/cordis`, consumed by bindgen at build time.
#[macro_export]
macro_rules! export {
    ($plugin:ty) => {
        #[allow(unsafe_code)]
        mod __cordis_abi_exports {
            use $crate::raw::*;
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn cordis_abi_v1(
                v: u32,
                out: *mut CordisAbiApi,
                size: usize,
            ) -> u32 {
                unsafe { $crate::ffi::entry::<$plugin>(v, out, size) }
            }
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn cordis_abi_context_v1(
                v: u32,
                out: *mut CordisAbiContextApi,
                size: usize,
            ) -> u32 {
                unsafe { $crate::ffi::context_entry::<$plugin>(v, out, size) }
            }
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn cordis_abi_managed_v1(
                v: u32,
                out: *mut CordisAbiManagedApi,
                size: usize,
            ) -> u32 {
                unsafe { $crate::ffi::managed_entry::<$plugin>(v, out, size) }
            }
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn cordis_abi_reactive_v1(
                v: u32,
                out: *mut CordisAbiReactiveApi,
                size: usize,
            ) -> u32 {
                unsafe { $crate::ffi::reactive_entry::<$plugin>(v, out, size) }
            }
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn cordis_abi_composition_v1(
                v: u32,
                out: *mut CordisAbiCompositionApi,
                size: usize,
            ) -> u32 {
                unsafe { $crate::ffi::composition_entry::<$plugin>(v, out, size) }
            }
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn cordis_abi_resources_v1(
                v: u32,
                out: *mut CordisAbiResourceApi,
                size: usize,
            ) -> u32 {
                unsafe { $crate::ffi::resources_entry::<$plugin>(v, out, size) }
            }
        }
    };
}
