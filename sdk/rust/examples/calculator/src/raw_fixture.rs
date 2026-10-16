//! Malformed-value, trap and destructor probes. This artifact intentionally bypasses generated result validation.
#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
#[allow(unsafe_code)]
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/plugin_bindings.rs"));
}

use bindings::cordis::plugin::types::{Contract, ErrorInfo, Failure, Payload as WitValue};
use bindings::exports::cordis::plugin::service::{
    Guest, GuestInstance, GuestLease, Instance, Lease,
};
use cordis_component::{Payload, Value, json};
use std::{cell::Cell, rc::Rc};

#[cfg(feature = "structural-fixture")]
#[path = "fixtures.rs"]
mod fixtures;

fn interface() -> cordis_contract::Descriptor {
    #[cfg(feature = "structural-fixture")]
    return cordis_contract::Descriptor::new(fixtures::interface()).unwrap();
    #[cfg(all(not(feature = "structural-fixture"), feature = "test-fixture"))]
    let source = cordis_test_contracts::resources::rust_fixture::SERVICE;
    #[cfg(not(feature = "structural-fixture"))]
    cordis_contract::portable::decode(&source.parse::<Value>().unwrap()).unwrap()
}

struct Calculator;
struct State {
    active: Cell<bool>,
    closed: Cell<bool>,
    leases: Cell<u32>,
    calls: Cell<u32>,
    releases: Cell<u32>,
    config: Value,
}
struct CalculatorInstance(Rc<State>);
struct Counter {
    state: Rc<State>,
    value: Cell<i64>,
    released: Cell<bool>,
}
type Result<T> = std::result::Result<T, Failure>;
fn rejected(code: &str, message: impl ToString) -> Failure {
    Failure::Rejected(ErrorInfo {
        code: code.into(),
        message: message.to_string(),
    })
}
fn refused(code: &str, message: impl ToString) -> Failure {
    Failure::Refused(ErrorInfo {
        code: code.into(),
        message: message.to_string(),
    })
}
fn decode(value: WitValue) -> Result<Value> {
    cordis_component::decode(value)
        .and_then(Payload::control_only)
        .map_err(|e| refused("invalid_payload", e))
}
fn encode(value: Value) -> Result<WitValue> {
    cordis_component::encode(&Payload::from(value)).map_err(|e| {
        Failure::Unknown(ErrorInfo {
            code: "invalid_payload".into(),
            message: e.to_string(),
        })
    })
}
fn decode_body(value: WitValue, shape: &cordis_contract::MessageContract) -> Result<Value> {
    let bytes = cordis_component::decode(value)
        .and_then(|p| p.one())
        .map_err(|e| refused("invalid_payload", e))?;
    cordis_sdk_core::Message::from_bytes(shape.clone(), bytes)
        .and_then(|m| m.to_json())
        .map_err(|e| refused("invalid_argument", e))
}
fn encode_body(value: Value, shape: &cordis_contract::MessageContract) -> Result<WitValue> {
    let body = cordis_sdk_core::Message::from_json(shape.clone(), &value)
        .and_then(|m| m.encode(cordis_component::MAX_BYTES))
        .map_err(|e| {
            Failure::Unknown(ErrorInfo {
                code: "invalid_result".into(),
                message: e.to_string(),
            })
        })?;
    cordis_component::encode(&Payload::message(body)).map_err(|e| {
        Failure::Unknown(ErrorInfo {
            code: "invalid_result".into(),
            message: e.to_string(),
        })
    })
}
fn integer(value: &Value) -> Result<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
        .ok_or_else(|| rejected("invalid_arguments", "expected integer"))
}
fn add(left: i64, right: i64) -> Result<i64> {
    left.checked_add(right)
        .filter(|n| n.unsigned_abs() <= 9_007_199_254_740_991_i64 as u64)
        .ok_or_else(|| rejected("overflow", "integer overflow"))
}
impl State {
    fn fault(&self, stage: &str) -> Result<()> {
        if cfg!(feature = "test-fixture") && self.config["fail"] == stage {
            let info = ErrorInfo {
                code: "fixture".into(),
                message: stage.into(),
            };
            return Err(if self.config["unknown"] == true {
                Failure::Unknown(info)
            } else {
                Failure::Rejected(info)
            });
        }
        Ok(())
    }
    fn ready(&self) -> Result<()> {
        if !self.active.get() || self.closed.get() {
            return Err(refused("closed", "inactive instance"));
        }
        Ok(())
    }
}
impl Guest for Calculator {
    type Instance = CalculatorInstance;
    type Lease = Counter;
    fn describe() -> Contract {
        Contract {
            name: "example.calculator".into(),
            descriptor: encode(cordis_contract::portable::encode(&interface())).unwrap(),
            revision: "1".into(),
        }
    }
    fn create(config: WitValue) -> Result<Instance> {
        let state = Rc::new(State {
            config: cordis_component::Initialization::decode(decode(config)?)
                .map_err(|e| refused("invalid_config", e))?
                .config,
            active: Cell::new(false),
            closed: Cell::new(false),
            leases: Cell::new(0),
            calls: Cell::new(0),
            releases: Cell::new(0),
        });
        state.fault("create")?;
        Ok(Instance::new(CalculatorInstance(state)))
    }
}
impl GuestInstance for CalculatorInstance {
    fn activate(&self) -> Result<()> {
        self.0.fault("activate")?;
        self.0.active.set(true);
        Ok(())
    }
    fn invoke(&self, method: String, args: WitValue) -> Result<WitValue> {
        self.0.ready()?;
        self.0.fault("invoke")?;
        let descriptor = interface();
        let signature = cordis_contract::portable::method(&descriptor.interface().methods, &method)
            .map_err(|e| refused("unknown_method", e))?;
        let args = decode_body(args, &signature.args)?;
        let encode = |value| encode_body(value, &signature.result);
        self.0.calls.set(self.0.calls.get() + 1);
        #[cfg(feature = "test-fixture")]
        match method.as_str() {
            "trap" => panic!("fixture trap"),
            "spin" => loop {
                std::hint::spin_loop();
            },
            "invalid" => return Ok(vec![255]),
            "memory" => {
                #[cfg(target_arch = "wasm32")]
                {
                    let _ = core::arch::wasm32::memory_grow::<0>(1024);
                }
                return encode(Value::Null);
            }
            _ => {}
        }
        match method.as_str() {
            // A compiled release identity, deliberately independent of config
            // and of the unchanged example.calculator@1 service contract.
            "code-version" => encode(json!(if cfg!(feature = "version-v2") {
                "v2"
            } else {
                "v1"
            })),
            #[cfg(feature = "structural-fixture")]
            "error" => Err(fixtures::failure(integer(&args)?)),
            "echo" => encode(args),
            "add" => encode(json!(add(
                integer(&args["left"])?,
                integer(&args["right"])?
            )?)),
            "stats" => encode(
                json!({"calls":self.0.calls.get(), "leases":self.0.leases.get(), "releases":self.0.releases.get()}),
            ),
            _ => Err(refused("unknown_method", method)),
        }
    }
    fn acquire(&self, kind: String, args: WitValue) -> Result<Lease> {
        self.0.ready()?;
        self.0.fault("acquire")?;
        if kind != "counter" {
            return Err(refused("unknown_resource_kind", kind));
        }
        let descriptor = interface();
        let resource = cordis_contract::portable::resource(&descriptor, &kind)
            .map_err(|e| refused("unknown_resource_kind", e))?;
        let args = decode_body(args, &resource.args)?;
        let value = if args["initial"].is_null() {
            0
        } else {
            integer(&args["initial"])?
        };
        #[cfg(feature = "structural-fixture")]
        if value < 0 {
            return Err(fixtures::failure(value));
        }
        self.0.leases.set(self.0.leases.get() + 1);
        Ok(Lease::new(Counter {
            state: self.0.clone(),
            value: Cell::new(value),
            released: Cell::new(false),
        }))
    }
    fn deactivate(&self) -> Result<()> {
        self.0.fault("deactivate")?;
        if self.0.leases.get() != 0 {
            return Err(rejected("live_resources", "leases remain"));
        }
        self.0.active.set(false);
        Ok(())
    }
    fn close(&self) -> Result<()> {
        self.0.fault("close")?;
        if self.0.active.get() {
            return Err(rejected("active", "deactivate first"));
        }
        self.0.closed.set(true);
        Ok(())
    }
}
impl GuestLease for Counter {
    fn invoke(&self, method: String, args: WitValue) -> Result<WitValue> {
        self.state.ready()?;
        if self.released.get() {
            return Err(refused("closed", "released counter"));
        }
        let descriptor = interface();
        let resource = cordis_contract::portable::resource(&descriptor, "counter")
            .map_err(|e| refused("unknown_resource_kind", e))?;
        let signature = cordis_contract::portable::method(&resource.methods, &method)
            .map_err(|e| refused("unknown_method", e))?;
        let args = decode_body(args, &signature.args)?;
        let encode = |value| encode_body(value, &signature.result);
        match method.as_str() {
            #[cfg(feature = "structural-fixture")]
            "error" => return Err(fixtures::failure(integer(&args)?)),
            "get" => {}
            "add" => self.value.set(add(self.value.get(), integer(&args)?)?),
            _ => return Err(refused("unknown_method", method)),
        }
        encode(json!(self.value.get()))
    }
    fn release(&self) -> Result<()> {
        self.state.ready()?;
        self.state.fault("release")?;
        if self.released.replace(true) {
            return Err(rejected("duplicate_release", "already released"));
        }
        self.state.leases.set(self.state.leases.get() - 1);
        self.state.releases.set(self.state.releases.get() + 1);
        Ok(())
    }
}
impl Drop for Counter {
    fn drop(&mut self) {
        if cfg!(feature = "test-fixture") && self.state.config["fail"] == "lease-drop" {
            panic!("lease destructor trap");
        }
    }
}
impl Drop for CalculatorInstance {
    fn drop(&mut self) {
        if cfg!(feature = "test-fixture") && self.0.config["fail"] == "instance-drop" {
            panic!("instance destructor trap");
        }
    }
}
#[allow(unsafe_code)]
#[cfg(target_arch = "wasm32")]
mod exports {
    use super::{Calculator, bindings};
    bindings::export!(Calculator with_types_in bindings);
}
