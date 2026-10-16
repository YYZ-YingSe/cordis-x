//! Session/HTTP adapter for portable Rust plugins. Endpoints and codecs are
//! negotiated independently; normal calls reuse the process and plugin.
mod backend;
mod callbacks;
mod channel;
mod host;
mod http;
mod resources;
mod stdio;

use cordis_sdk_core::{ErrorInfo, Failure, Plugin, Result, Value, json, refused};
use cordis_wire::{Outcome, Payload, RemoteError};
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
};

#[derive(Clone)]
pub struct Options {
    pub http: bool,
    pub config: Value,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            http: true,
            config: Value::Null,
        }
    }
}
/// Own a standalone plugin process. stdout is reserved for Session frames;
/// business logging must use stderr. `http=false` disables the HTTP listener.
pub fn run<P: Plugin>(options: Options) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(unknown)?;
    let result =
        runtime.block_on(tokio::task::LocalSet::new().run_until(host::serve::<P>(options)));
    // Tokio's stdio reader cannot cancel an outstanding OS read. This entry is
    // for a standalone process; do not wait for that read during graceful exit.
    runtime.shutdown_background();
    result
}
fn unknown(message: impl std::fmt::Display) -> Failure {
    Failure::Unknown(ErrorInfo {
        code: "process".into(),
        message: message.to_string(),
    })
}
fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key]
        .as_str()
        .ok_or_else(|| refused("invalid_argument", format!("missing {key}")))
}
fn outcome(result: Result<Payload>) -> Outcome {
    let error = |e: ErrorInfo| RemoteError {
        code: e.code,
        message: e.message,
    };
    match result {
        Ok(value) => Outcome::Success { value },
        Err(Failure::Refused(e)) => Outcome::Refused { error: error(e) },
        Err(Failure::Rejected(e)) => Outcome::Failure { error: error(e) },
        Err(Failure::Unknown(e)) => Outcome::Unknown { error: error(e) },
    }
}

fn checked_outcome(result: Result<Value>) -> Result<Value> {
    match result {
        Err(e @ Failure::Unknown(_)) => Err(e),
        Ok(value) => Ok(json!({"kind":"success","value":value})),
        Err(Failure::Refused(e)) => {
            Ok(json!({"kind":"refused","error":{"code":e.code,"message":e.message}}))
        }
        Err(Failure::Rejected(e)) => {
            Ok(json!({"kind":"failure","error":{"code":e.code,"message":e.message}}))
        }
    }
}
