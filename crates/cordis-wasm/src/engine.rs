use crate::{
    Compiler, Error, Result, bindings,
    compiler::{CompiledCode, Profile},
};
use bindings::cordis::plugin::types as wit;
use cordis_component::{Bytes, Payload, Value};
use cordis_contract::{ContractId, Descriptor, portable as contract};
use std::sync::Arc;
use wasmtime::component::{HasSelf, Linker, ResourceAny};
use wasmtime::{Store, StoreLimits, StoreLimitsBuilder};

/// Per-store execution limits. The host imports no WASI or ambient capabilities.
#[derive(Clone, Debug)]
pub struct Limits {
    pub memory_bytes: usize,
    pub fuel_per_call: u64,
    pub resources: usize,
    pub pending_calls: usize,
    pub artifact_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            memory_bytes: 16 * 1024 * 1024,
            fuel_per_call: 50_000_000,
            resources: 1024,
            pending_calls: 16,
            artifact_bytes: 16 * 1024 * 1024,
        }
    }
}
#[derive(Clone)]
pub(crate) struct Module {
    pub validation: cordis_contract::Validation,
    code: Arc<CompiledCode>,
    pub limits: Limits,
    pub contract: ContractId,
    descriptor: Option<Descriptor>,
}
pub(crate) struct Data(StoreLimits);
impl wit::Host for Data {}

pub(crate) fn unknown(error: impl std::fmt::Display) -> Error {
    Error::Unknown(error.to_string())
}
pub(crate) fn input(value: &Payload) -> Result<wit::Payload> {
    cordis_component::encode(value).map_err(|e| Error::InvalidInput(e.to_string()))
}
fn output(value: wit::Payload, validation: cordis_contract::Validation) -> Result<Payload> {
    cordis_component::decode_with(value, validation).map_err(unknown)
}

fn result<T>(value: wasmtime::Result<std::result::Result<T, wit::Failure>>) -> Result<T> {
    value
        .map_err(|e| unknown(format!("{e:#}")))?
        .map_err(|e| match e {
            wit::Failure::Refused(e) => Error::Refused {
                code: e.code,
                message: e.message,
            },
            wit::Failure::Rejected(e) => Error::Rejected {
                code: e.code,
                message: e.message,
            },
            wit::Failure::Unknown(e) => unknown(format!("{}: {}", e.code, e.message)),
        })
}
impl Module {
    pub fn compile(compiler: &Compiler, bytes: &[u8], limits: Limits) -> Result<Self> {
        if bytes.len() > limits.artifact_bytes
            || limits.pending_calls == 0
            || limits.pending_calls > 1024
            || limits.memory_bytes == 0
            || limits.fuel_per_call == 0
            || limits.resources == 0
        {
            return Err(Error::InvalidInput(
                "invalid limits or component too large".into(),
            ));
        }
        let code = compiler.compile(Profile::Basic, bytes)?;
        compiler.preflight(|| {
            let mut module = Self {
                validation: cordis_contract::Validation::Checked,
                code,
                limits,
                contract: ContractId::new("", ""),
                descriptor: None,
            };
            let mut probe = module.instantiate()?;
            let (id, descriptor) = probe.describe()?;
            module.contract = id;
            module.descriptor = Some(descriptor.with_validation(module.validation));
            Ok(module)
        })
    }
    pub fn set_validation(&mut self, validation: cordis_contract::Validation) {
        self.validation = validation;
        self.descriptor = self
            .descriptor
            .take()
            .map(|d| d.with_validation(validation));
    }
    pub fn descriptor(&self) -> &Descriptor {
        self.descriptor.as_ref().expect("preflight completed")
    }
    pub fn instantiate(&self) -> Result<Guest> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(self.limits.memory_bytes)
            .table_elements(4096)
            .instances(8)
            .tables(8)
            .memories(1)
            .trap_on_grow_failure(true)
            .build();
        let mut store = Store::new(&self.code.engine, Data(limits));
        store.limiter(|data| &mut data.0);
        store.set_fuel(self.limits.fuel_per_call).map_err(unknown)?;
        let mut linker = Linker::new(&self.code.engine);
        bindings::Plugin::add_to_linker::<_, HasSelf<Data>>(&mut linker, |data| data)
            .map_err(unknown)?;
        let bindings = bindings::Plugin::instantiate(&mut store, &self.code.component, &linker)
            .map_err(|e| unknown(format!("{e:#}")))?;
        Ok(Guest {
            store,
            bindings,
            fuel: self.limits.fuel_per_call,
            validation: self.validation,
        })
    }
}

pub(crate) struct Guest {
    store: Store<Data>,
    bindings: bindings::Plugin,
    fuel: u64,
    validation: cordis_contract::Validation,
}
#[derive(Clone, Copy)]
pub(crate) enum Op {
    Activate,
    Invoke,
    Acquire,
    ResourceCall(ResourceAny),
}
pub(crate) enum Reply {
    Empty,
    Value(Bytes),
    Resource(ResourceAny),
}
impl Guest {
    fn budget(&mut self) -> Result<()> {
        self.store.set_fuel(self.fuel).map_err(unknown)
    }
    pub fn describe(&mut self) -> Result<(ContractId, Descriptor)> {
        self.budget()?;
        let contract = self
            .bindings
            .cordis_plugin_service()
            .call_describe(&mut self.store)
            .map_err(unknown)?;
        if contract.name.is_empty()
            || contract.name.len() > 4096
            || contract.revision.is_empty()
            || contract.revision.len() > 4096
        {
            return Err(Error::Contract("invalid component contract".into()));
        }
        let descriptor = contract::decode(
            &output(contract.descriptor, cordis_contract::Validation::Audit)?
                .control_only()
                .map_err(unknown)?,
        )
        .map_err(Error::Contract)?;
        Ok((
            ContractId::new(contract.name, contract.revision),
            descriptor,
        ))
    }
    pub fn create(&mut self, config: &Value) -> Result<ResourceAny> {
        let config = input(&config.clone().into())?;
        self.budget()?;
        result(
            self.bindings
                .cordis_plugin_service()
                .call_create(&mut self.store, &config),
        )
    }
    pub fn execute(
        &mut self,
        id: ResourceAny,
        op: Op,
        method: &str,
        args: &Payload,
    ) -> Result<Reply> {
        let args = input(args)?;
        self.budget()?;
        let api = self.bindings.cordis_plugin_service();
        match op {
            Op::Activate => {
                result(api.instance().call_activate(&mut self.store, id)).map(|()| Reply::Empty)
            }
            Op::Invoke => output(
                result(
                    api.instance()
                        .call_invoke(&mut self.store, id, method, &args),
                )?,
                self.validation,
            )
            .and_then(|value| value.one().map_err(unknown))
            .map(Reply::Value),
            Op::Acquire => result(
                api.instance()
                    .call_acquire(&mut self.store, id, method, &args),
            )
            .map(Reply::Resource),
            Op::ResourceCall(resource) => output(
                result(
                    api.lease()
                        .call_invoke(&mut self.store, resource, method, &args),
                )?,
                self.validation,
            )
            .and_then(|value| value.one().map_err(unknown))
            .map(Reply::Value),
        }
    }
    pub fn release(&mut self, id: ResourceAny) -> Result<()> {
        self.budget()?;
        result(
            self.bindings
                .cordis_plugin_service()
                .lease()
                .call_release(&mut self.store, id),
        )?;
        self.budget()?;
        id.resource_drop(&mut self.store).map_err(unknown)
    }
    pub fn close(&mut self, id: ResourceAny, activated: bool) -> Result<()> {
        if activated {
            self.budget()?;
            result(
                self.bindings
                    .cordis_plugin_service()
                    .instance()
                    .call_deactivate(&mut self.store, id),
            )?;
        }
        self.budget()?;
        result(
            self.bindings
                .cordis_plugin_service()
                .instance()
                .call_close(&mut self.store, id),
        )?;
        self.budget()?;
        id.resource_drop(&mut self.store).map_err(unknown)
    }
}

#[cfg(test)]
mod measurements {
    use super::*;
    use std::time::Instant;

    /// Stage boundaries unavailable through the public Runtime API (which spawns
    /// activation before apply returns). This deliberately bypasses scheduling
    /// only for measurements; integration tests cover the owned lifecycle.
    #[test]
    #[ignore = "manual stage measurement: set CORDIS_PREPARATION_WASM and CORDIS_PREPARATION_REPORT"]
    fn preparation_stages() {
        let bytes = std::fs::read(std::env::var("CORDIS_PREPARATION_WASM").unwrap()).unwrap();
        let compiler = Compiler::default();
        let module = Module::compile(&compiler, &bytes, Limits::default()).unwrap();
        let mut instantiate = Vec::new();
        let mut create = Vec::new();
        let mut activate = Vec::new();
        let mut invoke = Vec::new();
        let mut close = Vec::new();
        let signature =
            cordis_contract::portable::method(&module.descriptor().interface().methods, "add")
                .unwrap()
                .clone();
        let args =
            crate::json_body(&signature.args, &crate::json!({"left":20,"right":22})).unwrap();
        for _ in 0..20 {
            let start = Instant::now();
            let mut guest = module.instantiate().unwrap();
            instantiate.push(start.elapsed().as_secs_f64() * 1000.0);
            let start = Instant::now();
            let id = guest.create(&crate::json!({})).unwrap();
            create.push(start.elapsed().as_secs_f64() * 1000.0);
            let start = Instant::now();
            guest
                .execute(id, Op::Activate, "", &Value::Null.into())
                .unwrap();
            activate.push(start.elapsed().as_secs_f64() * 1000.0);
            for _ in 0..10 {
                let start = Instant::now();
                let value = guest
                    .execute(id, Op::Invoke, "add", &Payload::message(args.clone()))
                    .unwrap();
                invoke.push(start.elapsed().as_secs_f64() * 1000.0);
                assert!(
                    matches!(value, Reply::Value(value) if crate::body_json(&signature.result,&value).unwrap() == crate::json!("42"))
                );
            }
            let start = Instant::now();
            guest.close(id, true).unwrap();
            close.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        let stats = compiler.stats();
        let report = crate::json!({
            "compile_ms":stats.compile_time.as_secs_f64() * 1000.0,
            "preflight_ms":stats.preflight_time.as_secs_f64() * 1000.0,
            "instantiate_ms":instantiate, "create_ms":create, "activate_ms":activate,
            "invoke_ms":invoke, "deactivate_close_ms":close,
        });
        std::fs::write(
            std::env::var("CORDIS_PREPARATION_REPORT").unwrap(),
            format!("{report:#}\n"),
        )
        .unwrap();
    }
}
