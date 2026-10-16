//! One resolved operation/type plan shared by all emitters.
pub mod cpp;
mod plan;
pub mod rust;
pub mod typescript;
use crate::{Module, Result, output::Files};
pub use plan::{Control, Operation, Plan, Resource, Service, Shape};

pub trait Backend {
    fn name(&self) -> &'static str;
    fn generate(&self, module: &Module, plan: &Plan) -> Result<Files>;
}
pub fn generate(
    module: &Module,
    backends: &[&dyn Backend],
    tools: &crate::GeneratorTools,
) -> Result<Files> {
    let plan = if backends.is_empty() {
        None
    } else {
        Some(Plan::new(module)?)
    };
    let mut files = crate::protobuf::generate(module, backends, tools)?;
    for (name, service) in module.services() {
        files.add(
            format!(
                "resources/interfaces/{}.json",
                crate::symbols::ident(name, crate::symbols::Case::Type)
            ),
            serde_json::to_string_pretty(&cordis_contract::portable::encode(&service.descriptor))
                .unwrap()
                + "\n",
        )?;
    }
    files.add("resources/descriptor.json",serde_json::to_string_pretty(&module.services().iter().map(|(name,s)|
        (name,serde_json::json!({"contract":{"name":s.name,"revision":s.revision},
        "interface":cordis_contract::portable::encode(&s.descriptor),"config":s.config,"dependencies":s.dependencies,"bindings":s.bindings()})))
        .collect::<std::collections::BTreeMap<_,_>>()).unwrap()+"\n")?;
    for backend in backends {
        files.extend(backend.generate(module, plan.as_ref().unwrap())?)?;
    }
    Ok(files)
}
