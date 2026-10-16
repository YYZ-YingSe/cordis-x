use crate::{
    Diagnostic, Module, Result,
    symbols::{Case, Scope, ident},
};
use cordis_contract::{Dispatch, MessageContract};
use heck::ToUpperCamelCase;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct Shape {
    pub rust: String,
    pub cpp: String,
    pub ts: String,
    pub schema: String,
    pub file: String,
    pub name: String,
}
pub struct Operation {
    pub wire: Option<String>,
    pub name: String,
    pub prefix: String,
    pub args: Shape,
    pub result: Shape,
    pub signature: cordis_contract::Method,
}
pub struct Resource {
    pub wire: Option<String>,
    pub name: String,
    pub prefix: String,
    pub args: Shape,
    pub contract: cordis_contract::Resource,
    pub methods: Vec<Operation>,
}
pub struct Service {
    pub key: String,
    pub prefix: String,
    pub config: Shape,
    pub methods: Vec<Operation>,
    pub resources: Vec<Resource>,
    pub controls: Vec<Control>,
}
pub struct Control {
    pub wire: String,
    pub name: String,
    pub prefix: String,
    pub args: Shape,
    pub result: Option<Shape>,
    pub contract: cordis_contract::Control,
}
pub struct Plan {
    pub services: Vec<Service>,
    pub messages: Vec<Shape>,
}
fn message_shape(module: &Module, name: &str) -> Shape {
    let m = module.message(name);
    let file = m.parent_file();
    let package = file.package_name();
    let local = if package.is_empty() {
        name
    } else {
        &name[package.len() + 1..]
    };
    let mut parts: Vec<_> = local.split('.').collect();
    let mut leaf = parts.pop().unwrap().to_upper_camel_case();
    if leaf == "Self" {
        leaf.push('_');
    }
    let mut rust = if let Some(path) = module.rust_module(package) {
        vec![path]
    } else {
        let mut rust = vec!["data".to_owned()];
        rust.extend(
            prost_build::Module::from_protobuf_package_name(package)
                .parts()
                .map(str::to_owned),
        );
        rust
    };
    rust.extend(
        prost_build::Module::from_protobuf_package_name(&parts.join("."))
            .parts()
            .map(str::to_owned),
    );
    rust.push(leaf);
    let alias = format!(
        "pb_{}",
        file.name()
            .trim_end_matches(".proto")
            .replace(['/', '-', '.'], "_")
    );
    let ts_name = local.replace('.', "_");
    Shape {
        rust: rust.join("::"),
        cpp: format!("::{}", name.replace('.', "::")),
        ts: format!("{alias}.{ts_name}"),
        schema: format!("{alias}.{ts_name}Schema"),
        file: file.name().into(),
        name: name.into(),
    }
}
fn shape(module: &Module, t: &MessageContract) -> Shape {
    message_shape(module, t.message())
}
fn methods(
    module: &Module,
    dispatch: &Dispatch<cordis_contract::Method>,
    prefix: &str,
    scope: &mut Scope,
) -> Result<Vec<Operation>> {
    let Dispatch::Named(methods) = dispatch else {
        unreachable!()
    };
    methods
        .iter()
        .map(|(wire, m)| {
            let name = scope.insert(wire, Case::Member, prefix)?;
            let prefix = format!("{prefix}{}", ident(wire, Case::Type));
            check_error_names(&m.errors, &prefix)?;
            Ok(Operation {
                wire: Some(wire.clone()),
                name,
                prefix,
                args: shape(module, &m.args),
                result: shape(module, &m.result),
                signature: m.clone(),
            })
        })
        .collect()
}
impl Plan {
    pub fn new(module: &Module) -> Result<Self> {
        let mut services = Vec::new();
        let mut globals = Scope::default();
        for (key, service) in module.services() {
            let prefix = globals.insert(key, Case::Type, key)?;
            for suffix in [
                "Client",
                "Server",
                "Export",
                "Model",
                "Dependencies",
                "Controls",
            ] {
                globals.insert(&format!("{prefix}{suffix}"), Case::Type, key)?;
            }
            let mut scope = Scope::default();
            for name in [
                "activate",
                "deactivate",
                "create",
                "close",
                "dispose",
                "interface",
                "descriptor",
                "dependencies",
            ] {
                scope.reserve(name);
            }
            let operations = methods(
                module,
                &service.descriptor.interface().methods,
                &prefix,
                &mut scope,
            )?;
            let Dispatch::Named(factories) = &service.descriptor.interface().resources else {
                unreachable!()
            };
            let mut resources = Vec::new();
            for (wire, r) in factories {
                let name = scope.insert(wire, Case::Member, key)?;
                let rp = format!("{prefix}{}", ident(wire, Case::Type));
                for suffix in ["Client", "Server", "Export", "Error"] {
                    globals.insert(&format!("{rp}{suffix}"), Case::Type, key)?;
                }
                let mut scope = Scope::default();
                scope.reserve("close");
                scope.reserve("dispose");
                check_error_names(&r.errors, &rp)?;
                resources.push(Resource {
                    wire: Some(wire.clone()),
                    name,
                    prefix: rp.clone(),
                    args: shape(module, &r.args),
                    contract: r.clone(),
                    methods: methods(module, &r.methods, &rp, &mut scope)?,
                });
            }
            let mut aliases = Scope::default();
            for alias in service.dependencies.keys() {
                aliases.insert(alias, Case::Member, key)?;
            }
            let mut controls = Vec::new();
            for (wire, control) in &service.descriptor.interface().controls {
                use cordis_contract::Control as C;
                let name = scope.insert(wire, Case::Member, key)?;
                let prefix = format!("{prefix}{}", ident(wire, Case::Type));
                let (args, result, errors) = match control {
                    C::Event { listener, .. }
                    | C::Middleware {
                        handler: listener, ..
                    } => (
                        &listener.args,
                        Some(&listener.result),
                        Some(&listener.errors),
                    ),
                    C::Factory { config, errors } => (config, None, Some(errors)),
                    C::Provision { value, .. } => (value, None, None),
                };
                if let Some(errors) = errors {
                    check_error_names(errors, &prefix)?;
                }
                controls.push(Control {
                    wire: wire.clone(),
                    name,
                    prefix,
                    args: shape(module, args),
                    result: result.map(|r| shape(module, r)),
                    contract: control.clone(),
                });
            }
            services.push(Service {
                key: key.clone(),
                prefix,
                config: shape(module, &service.config),
                methods: operations,
                resources,
                controls,
            });
        }
        let messages: Vec<Shape> = module
            .pool
            .all_messages()
            .filter(|m| {
                m.parent_file().name() != "google/protobuf/descriptor.proto"
                    && !m.full_name().starts_with("cordis.schema.")
                    && !m.is_map_entry()
                    && !matches!(m.full_name(), "cordis.data.Lease" | "cordis.data.Child")
            })
            .map(|m| message_shape(module, m.full_name()))
            .collect();
        let mut names = std::collections::BTreeSet::new();
        for message in &messages {
            if !names.insert(&message.rust) {
                return Err(Diagnostic::new(
                    &message.name,
                    "protobuf names collide in Rust",
                ));
            }
        }
        Ok(Self { services, messages })
    }
}

#[derive(Default)]
pub struct Writer {
    text: String,
    depth: usize,
}
impl Writer {
    pub fn line(&mut self, line: impl AsRef<str>) {
        self.text.push_str(&"    ".repeat(self.depth));
        self.text.push_str(line.as_ref());
        self.text.push('\n');
    }
    pub fn open(&mut self, line: impl AsRef<str>) {
        self.line(line);
        self.depth += 1;
    }
    pub fn close(&mut self, line: impl AsRef<str>) {
        self.depth -= 1;
        self.line(line);
    }
    pub fn finish(self) -> String {
        self.text
    }
}
pub fn dependencies(module: &Module, service: &Service) -> BTreeMap<String, serde_json::Value> {
    module.services()[&service.key]
        .dependencies
        .values()
        .map(|key| {
            let dep = &module.services()[key];
            (
                dep.name.clone(),
                cordis_contract::portable::encode(&dep.descriptor),
            )
        })
        .collect()
}
pub fn check_error_names(errors: &std::collections::BTreeSet<String>, path: &str) -> Result<()> {
    let mut scope = Scope::default();
    let mut variants = Scope::default();
    for e in errors {
        if e != "*" {
            scope.insert(e, Case::Member, path)?;
            variants.insert(e, Case::Type, path)?;
        }
    }
    if errors.contains("*") && errors.len() != 1 {
        return Err(Diagnostic::new(
            path,
            "wildcard errors must be declared alone",
        ));
    }
    Ok(())
}
