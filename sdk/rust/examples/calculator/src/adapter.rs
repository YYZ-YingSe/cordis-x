//! This example uses the synchronous WIT profile: no imports grant a Context.
//! Its generated business futures must finish on first poll. An unexpected
//! suspension is a profile violation, never a successful cancellation.
#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
#[allow(unsafe_code)]
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/plugin_bindings.rs"));
}
use crate::service::Example;
use bindings::cordis::plugin::types::{Contract, ErrorInfo, Failure, Payload as WitPayload};
use bindings::exports::cordis::plugin::service::{
    Guest, GuestInstance, GuestLease, Instance, Lease,
};
use cordis_component::Payload;
use cordis_sdk_core::{Context, LocalFuture, Message, Plugin, ResourceObject, Result, refused};
use std::{
    any::Any,
    cell::{Cell, RefCell},
    rc::Rc,
    task::{Context as TaskContext, Poll, Waker},
};

struct NoImports;
impl cordis_sdk_core::backend::Backend for NoImports {
    fn validation(&self) -> cordis_sdk_core::Validation {
        cordis_sdk_core::Validation::Contract
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn submit(
        &self,
        _: cordis_sdk_core::backend::Operation,
    ) -> LocalFuture<cordis_sdk_core::backend::Reply> {
        Box::pin(async {
            Err(refused(
                "unsupported_context",
                "this WIT world grants no host Context",
            ))
        })
    }
}
fn context() -> Context {
    Context::from_backend(Rc::new(NoImports))
}
fn failure(error: cordis_sdk_core::Failure) -> Failure {
    let info = |e: cordis_sdk_core::ErrorInfo| ErrorInfo {
        code: e.code,
        message: e.message,
    };
    match error {
        cordis_sdk_core::Failure::Rejected(e) => Failure::Rejected(info(e)),
        cordis_sdk_core::Failure::Refused(e) => Failure::Refused(info(e)),
        cordis_sdk_core::Failure::Unknown(e) => Failure::Unknown(info(e)),
    }
}
fn decode(value: WitPayload, validation: cordis_sdk_core::Validation) -> Result<Payload> {
    cordis_component::decode_with(value, validation).map_err(|e| refused("invalid_payload", e))
}
fn encode(value: impl Into<Payload>) -> Result<WitPayload> {
    cordis_component::encode(&value.into()).map_err(cordis_sdk_core::typed::violation)
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Created,
    Active,
    Inactive,
    Closed,
}
struct State {
    validation: cordis_sdk_core::Validation,
    interface: Rc<cordis_sdk_core::Interface>,
    plugin: Rc<Example>,
    phase: Cell<Phase>,
    leases: Cell<usize>,
    // Retain a suspended operation and its captures if business violates the
    // synchronous profile. The host receives Unknown and keeps responsibility.
    suspended: RefCell<Option<Box<dyn Any>>>,
}
impl State {
    fn complete<T: 'static>(&self, mut future: LocalFuture<T>) -> Result<T> {
        if self.suspended.borrow().is_some() {
            return Err(cordis_sdk_core::typed::violation(
                "synchronous instance has an unresolved operation",
            ));
        }
        match future
            .as_mut()
            .poll(&mut TaskContext::from_waker(Waker::noop()))
        {
            Poll::Ready(result) => result,
            Poll::Pending => {
                *self.suspended.borrow_mut() = Some(Box::new(future));
                Err(cordis_sdk_core::typed::violation(
                    "business suspended in a synchronous WIT world",
                ))
            }
        }
    }
    fn ready(&self) -> Result<()> {
        if self.phase.get() != Phase::Active {
            return Err(refused("closed", "inactive instance"));
        }
        Ok(())
    }
    fn lifecycle(&self, activate: bool) -> Result<()> {
        self.complete(if activate {
            self.plugin.clone().activate(context())
        } else {
            self.plugin.clone().deactivate(context())
        })
    }
}
struct Calculator;
struct CalculatorInstance(Rc<State>);
struct Counter {
    state: Rc<State>,
    contract: cordis_sdk_core::ResourceContract,
    object: Rc<dyn ResourceObject>,
    release: RefCell<Option<Result<()>>>,
}
impl Guest for Calculator {
    type Instance = CalculatorInstance;
    type Lease = Counter;
    fn describe() -> Contract {
        let manifest = Example::manifest();
        Contract {
            name: manifest.provides.name,
            revision: manifest.provides.revision,
            descriptor: encode(cordis_contract::portable::encode(
                &cordis_contract::Descriptor::new(Example::interface()).unwrap(),
            ))
            .unwrap(),
        }
    }
    fn create(config: WitPayload) -> std::result::Result<Instance, Failure> {
        let settings = cordis_component::Initialization::decode(
            decode(config, cordis_sdk_core::Validation::Audit)
                .and_then(|p| p.control_only().map_err(|e| refused("invalid_config", e)))
                .map_err(failure)?,
        )
        .map_err(|e| failure(refused("invalid_config", e)))?;
        let mut interface = Example::interface();
        interface.set_validation(settings.validation);
        let plugin = Example::create(settings.config).map_err(failure)?;
        Ok(Instance::new(CalculatorInstance(Rc::new(State {
            interface: Rc::new(interface),
            validation: settings.validation,
            plugin: Rc::new(plugin),
            phase: Cell::new(Phase::Created),
            leases: Cell::new(0),
            suspended: RefCell::new(None),
        }))))
    }
}
impl GuestInstance for CalculatorInstance {
    fn activate(&self) -> std::result::Result<(), Failure> {
        if self.0.phase.get() != Phase::Created {
            return Err(failure(refused("closed", "instance already activated")));
        }
        self.0.lifecycle(true).map_err(failure)?;
        self.0.phase.set(Phase::Active);
        Ok(())
    }
    fn invoke(&self, method: String, args: WitPayload) -> std::result::Result<WitPayload, Failure> {
        self.0.ready().map_err(failure)?;
        let args = decode(args, self.0.validation).map_err(failure)?;
        let plugin = self.0.plugin.clone();
        let interface = self.0.interface.clone();
        self.0
            .complete(Box::pin(async move {
                let signature = cordis_sdk_core::contract::method(&interface.methods, &method)
                    .map_err(|e| refused("invalid_argument", e))?;
                let input = Message::from_bytes(
                    signature.args.clone(),
                    args.one().map_err(|e| refused("invalid_argument", e))?,
                )
                .map_err(|e| refused("invalid_argument", e))?;
                let output = plugin
                    .invoke(context(), method, input)
                    .await
                    .map_err(|e| e.check_declared(&signature.errors, "method result"))?;
                output
                    .check(&signature.result)
                    .map_err(cordis_sdk_core::typed::violation)?;
                output
                    .encode(cordis_component::MAX_BYTES)
                    .map(Payload::message)
                    .map_err(cordis_sdk_core::typed::violation)
            }))
            .and_then(encode)
            .map_err(failure)
    }
    fn acquire(&self, kind: String, args: WitPayload) -> std::result::Result<Lease, Failure> {
        self.0.ready().map_err(failure)?;
        let count = self
            .0
            .leases
            .get()
            .checked_add(1)
            .ok_or_else(|| failure(refused("capacity", "too many leases")))?;
        let args = decode(args, self.0.validation).map_err(failure)?;
        let contract = self
            .0
            .interface
            .resources
            .get(&kind)
            .cloned()
            .ok_or_else(|| failure(refused("unknown_resource", &kind)))?;
        let args = Message::from_bytes(
            contract.args.clone(),
            args.one()
                .map_err(|e| failure(refused("invalid_argument", e)))?,
        )
        .map_err(|e| failure(refused("invalid_argument", e)))?;
        let object = self
            .0
            .complete(self.0.plugin.clone().acquire(context(), kind, args))
            .map_err(failure)?;
        self.0.leases.set(count);
        Ok(Lease::new(Counter {
            state: self.0.clone(),
            contract,
            object,
            release: RefCell::new(None),
        }))
    }
    fn deactivate(&self) -> std::result::Result<(), Failure> {
        if self.0.phase.get() == Phase::Closed {
            return Err(failure(refused("closed", "closed instance")));
        }
        if self.0.leases.get() != 0 {
            return Err(failure(refused("live_resources", "leases remain")));
        }
        if self.0.phase.get() == Phase::Inactive {
            return Ok(());
        }
        self.0.lifecycle(false).map_err(failure)?;
        self.0.phase.set(Phase::Inactive);
        Ok(())
    }
    fn close(&self) -> std::result::Result<(), Failure> {
        if self.0.phase.get() == Phase::Active || self.0.leases.get() != 0 {
            return Err(failure(refused("active", "deactivate and release first")));
        }
        if self.0.suspended.borrow().is_some() {
            return Err(failure(cordis_sdk_core::typed::violation(
                "unresolved operation",
            )));
        }
        self.0.phase.set(Phase::Closed);
        Ok(())
    }
}
impl GuestLease for Counter {
    fn invoke(&self, method: String, args: WitPayload) -> std::result::Result<WitPayload, Failure> {
        self.state.ready().map_err(failure)?;
        if self.release.borrow().is_some() {
            return Err(failure(refused("closed", "release has begun")));
        }
        let args = decode(args, self.state.validation).map_err(failure)?;
        let signature = cordis_sdk_core::contract::method(&self.contract.methods, &method)
            .map_err(|e| failure(refused("unknown_method", e)))?;
        let args = Message::from_bytes(
            signature.args.clone(),
            args.one()
                .map_err(|e| failure(refused("invalid_argument", e)))?,
        )
        .map_err(|e| failure(refused("invalid_argument", e)))?;
        self.state
            .complete(self.object.clone().invoke(context(), method, args))
            .map_err(|e| e.check_declared(&signature.errors, "resource result"))
            .and_then(|value| {
                value
                    .check(&signature.result)
                    .map_err(cordis_sdk_core::typed::violation)?;
                value
                    .encode(cordis_component::MAX_BYTES)
                    .map(Payload::message)
                    .map_err(cordis_sdk_core::typed::violation)
            })
            .and_then(encode)
            .map_err(failure)
    }
    fn release(&self) -> std::result::Result<(), Failure> {
        if let Some(result) = &*self.release.borrow() {
            return result.clone().map_err(failure);
        }
        self.state.ready().map_err(failure)?;
        let result = self.state.complete(self.object.clone().close(context()));
        if result.is_ok() {
            self.state.leases.set(self.state.leases.get() - 1);
        }
        *self.release.borrow_mut() = Some(result.clone());
        result.map_err(failure)
    }
}
#[allow(unsafe_code)]
#[cfg(target_arch = "wasm32")]
mod exports {
    use super::{Calculator, bindings};
    bindings::export!(Calculator with_types_in bindings);
}

#[cfg(test)]
mod tests {
    use super::*;
    use cordis_sdk_core::json;
    fn state() -> Rc<State> {
        Rc::new(State {
            interface: Rc::new(Example::interface()),
            validation: cordis_sdk_core::Validation::Checked,
            plugin: Rc::new(Example::create(json!({})).unwrap()),
            phase: Cell::new(Phase::Active),
            leases: Cell::new(0),
            suspended: RefCell::new(None),
        })
    }
    struct Object {
        closes: Rc<Cell<usize>>,
        fail: bool,
    }
    impl ResourceObject for Object {
        fn invoke(self: Rc<Self>, _: Context, _: String, args: Message) -> LocalFuture<Message> {
            Box::pin(async { Ok(args) })
        }
        fn close(self: Rc<Self>, _: Context) -> LocalFuture<()> {
            Box::pin(async move {
                self.closes.set(self.closes.get() + 1);
                if self.fail {
                    Err(cordis_sdk_core::error("cleanup_failed", "retained"))
                } else {
                    Ok(())
                }
            })
        }
    }
    #[test]
    fn release_result_is_cached_and_only_success_removes_responsibility() {
        for fail in [false, true] {
            let state = state();
            state.leases.set(1);
            let closes = Rc::new(Cell::new(0));
            let counter = Counter {
                state: state.clone(),
                contract: Example::interface()
                    .resources
                    .get("counter")
                    .unwrap()
                    .clone(),
                object: Rc::new(Object {
                    closes: closes.clone(),
                    fail,
                }),
                release: RefCell::new(None),
            };
            assert_eq!(counter.release().is_err(), fail);
            assert_eq!(counter.release().is_err(), fail);
            assert_eq!(closes.get(), 1);
            assert_eq!(state.leases.get(), usize::from(fail));
            assert!(
                counter
                    .invoke("get".into(), encode(json!({})).unwrap())
                    .is_err()
            );
            assert_eq!(CalculatorInstance(state).deactivate().is_err(), fail);
        }
    }
    #[test]
    fn dropping_a_lease_does_not_forge_successful_cleanup() {
        let state = state();
        state.leases.set(1);
        let closes = Rc::new(Cell::new(0));
        drop(Counter {
            state: state.clone(),
            contract: Example::interface()
                .resources
                .get("counter")
                .unwrap()
                .clone(),
            object: Rc::new(Object {
                closes: closes.clone(),
                fail: false,
            }),
            release: RefCell::new(None),
        });
        assert_eq!(closes.get(), 0);
        assert_eq!(state.leases.get(), 1);
        assert!(CalculatorInstance(state).deactivate().is_err());
    }
    #[test]
    fn unexpected_suspension_retains_captures_and_blocks_close() {
        struct Capture(Rc<Cell<usize>>);
        impl Drop for Capture {
            fn drop(&mut self) {
                self.0.set(self.0.get() + 1);
            }
        }
        let drops = Rc::new(Cell::new(0));
        let capture = Capture(drops.clone());
        let state = state();
        let pending: LocalFuture<()> = Box::pin(async move {
            let _capture = capture;
            std::future::pending().await
        });
        assert!(matches!(
            state.complete(pending),
            Err(cordis_sdk_core::Failure::Unknown(_))
        ));
        assert_eq!(drops.get(), 0);
        assert!(state.complete(Box::pin(async { Ok(()) })).is_err());
        let instance = CalculatorInstance(state);
        assert!(instance.deactivate().is_err());
        assert!(instance.close().is_err());
        drop(instance);
        assert_eq!(drops.get(), 1);
    }
}
