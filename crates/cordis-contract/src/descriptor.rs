//! Structural contracts. Canonical identity does not depend on a wire codec.
use crate::MessageContract;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Method {
    pub args: MessageContract,
    pub result: MessageContract,
    pub errors: BTreeSet<String>,
}
impl Method {
    pub fn new(args: MessageContract, result: MessageContract) -> Self {
        Self {
            args,
            result,
            errors: BTreeSet::new(),
        }
    }
}
/// Named operations or an explicitly declared dynamic dispatcher.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "mode",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Dispatch<T> {
    Named(BTreeMap<String, T>),
    Dynamic(T),
}
impl<T> Dispatch<T> {
    pub fn get(&self, name: &str) -> Option<&T> {
        match self {
            Self::Named(entries) => entries.get(name),
            Self::Dynamic(value) => Some(value),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resource {
    pub args: MessageContract,
    pub methods: Dispatch<Method>,
    pub errors: BTreeSet<String>,
    /// Current resource semantics: caller ownership, drain before one inverse.
    pub ownership: String,
    pub release: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    pub name: String,
    pub revision: String,
}
/// Typed control operations are distinct from ordinary request/reply methods.
/// Handles and continuation authority stay in the selected runtime adapter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Control {
    Event {
        contract: Endpoint,
        listener: Method,
    },
    Middleware {
        contract: Endpoint,
        handler: Method,
    },
    Factory {
        config: MessageContract,
        errors: BTreeSet<String>,
    },
    Provision {
        contract: Endpoint,
        value: MessageContract,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Interface {
    pub methods: Dispatch<Method>,
    pub resources: Dispatch<Resource>,
    pub controls: BTreeMap<String, Control>,
    pub capabilities: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Descriptor(Interface);
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Difference {
    pub path: String,
    pub message: String,
}
impl fmt::Display for Difference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path, self.message)
    }
}
impl std::error::Error for Difference {}
fn fail(path: &str, message: impl Into<String>) -> Difference {
    Difference {
        path: path.into(),
        message: message.into(),
    }
}
fn name(value: &str, path: &str) -> Result<(), Difference> {
    if value.is_empty() || value.len() > 1024 {
        return Err(fail(path, "identifier must contain 1..1024 UTF-8 bytes"));
    }
    Ok(())
}
impl Descriptor {
    pub fn with_validation(mut self, validation: crate::Validation) -> Self {
        self.0.set_validation(validation);
        self
    }
    pub fn new(interface: Interface) -> Result<Self, Difference> {
        let mut budget = 4096;
        interface.check(&mut budget)?;
        Ok(Self(interface))
    }
    pub fn interface(&self) -> &Interface {
        &self.0
    }
    /// Canonical length-prefixed tree, independent of JSON object ordering or codec bytes.
    pub fn digest(&self) -> String {
        let mut bytes = Vec::new();
        self.0.encode(&mut bytes);
        Sha256::digest(bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }
    pub fn compare(&self, actual: &Self) -> Result<(), Difference> {
        compare_dispatch(
            &self.0.methods,
            &actual.0.methods,
            "methods",
            compare_method,
        )?;
        compare_dispatch(
            &self.0.resources,
            &actual.0.resources,
            "resources",
            |a, b, path| {
                compare_type(&a.args, &b.args, &format!("{path}.args"))?;
                compare_dispatch(
                    &a.methods,
                    &b.methods,
                    &format!("{path}.methods"),
                    compare_method,
                )?;
                if a.errors != b.errors || a.ownership != b.ownership || a.release != b.release {
                    return Err(fail(path, "resource errors or lifecycle rules differ"));
                }
                Ok(())
            },
        )?;
        if self.0.capabilities != actual.0.capabilities {
            return Err(fail("capabilities", "required capabilities differ"));
        }
        if self.0.controls != actual.0.controls {
            return Err(fail("controls", "control contracts differ"));
        }
        Ok(())
    }
}
impl Method {
    pub fn with_validation(&self, validation: crate::Validation) -> Self {
        Self {
            args: self.args.with_validation(validation),
            result: self.result.with_validation(validation),
            errors: self.errors.clone(),
        }
    }
}
impl Resource {
    pub fn with_validation(&self, validation: crate::Validation) -> Self {
        let mut result = self.clone();
        result.args = result.args.with_validation(validation);
        result
            .methods
            .for_each_mut(|method| *method = method.with_validation(validation));
        result
    }
}
impl<T> Dispatch<T> {
    fn for_each_mut(&mut self, mut f: impl FnMut(&mut T)) {
        match self {
            Self::Dynamic(value) => f(value),
            Self::Named(values) => values.values_mut().for_each(f),
        }
    }
}
impl Interface {
    pub fn set_validation(&mut self, validation: crate::Validation) {
        self.methods
            .for_each_mut(|method| *method = method.with_validation(validation));
        self.resources
            .for_each_mut(|resource| *resource = resource.with_validation(validation));
        for control in self.controls.values_mut() {
            match control {
                Control::Event { listener, .. } => *listener = listener.with_validation(validation),
                Control::Middleware { handler, .. } => {
                    *handler = handler.with_validation(validation)
                }
                Control::Factory { config, .. } => *config = config.with_validation(validation),
                Control::Provision { value, .. } => *value = value.with_validation(validation),
            }
        }
    }
}
fn compare_dispatch<T>(
    a: &Dispatch<T>,
    b: &Dispatch<T>,
    path: &str,
    compare: impl Fn(&T, &T, &str) -> Result<(), Difference>,
) -> Result<(), Difference> {
    match (a, b) {
        (Dispatch::Dynamic(a), Dispatch::Dynamic(b)) => compare(a, b, path),
        (Dispatch::Named(a), Dispatch::Named(b)) => {
            if a.keys().ne(b.keys()) {
                return Err(fail(path, "declared operation identities differ"));
            }
            for (key, a) in a {
                compare(a, &b[key], &format!("{path}.{key}"))?;
            }
            Ok(())
        }
        _ => Err(fail(
            path,
            "named and dynamic dispatch are different interfaces",
        )),
    }
}
fn compare_method(a: &Method, b: &Method, path: &str) -> Result<(), Difference> {
    compare_type(&a.args, &b.args, &format!("{path}.args"))?;
    compare_type(&a.result, &b.result, &format!("{path}.result"))?;
    if a.errors != b.errors {
        return Err(fail(&format!("{path}.errors"), "declared errors differ"));
    }
    Ok(())
}
fn compare_type(a: &MessageContract, b: &MessageContract, path: &str) -> Result<(), Difference> {
    if a == b {
        Ok(())
    } else {
        Err(fail(
            path,
            format!(
                "message definitions differ: {} / {}",
                a.message(),
                b.message()
            ),
        ))
    }
}
fn take(budget: &mut usize, depth: usize, path: &str) -> Result<(), Difference> {
    if *budget == 0 || depth > 32 {
        return Err(fail(path, "descriptor exceeds node/depth limit"));
    }
    *budget -= 1;
    Ok(())
}
impl MessageContract {
    fn check(&self, budget: &mut usize, depth: usize, path: &str) -> Result<(), Difference> {
        take(budget, depth, path)?;
        self.resolve().map_err(|e| fail(path, e))?;
        Ok(())
    }
    fn encode(&self, out: &mut Vec<u8>) {
        string(out, self.message());
        string(out, self.descriptor());
    }
}
fn check_dispatch<T>(
    value: &Dispatch<T>,
    budget: &mut usize,
    path: &str,
    check: impl Fn(&T, &mut usize, &str) -> Result<(), Difference>,
) -> Result<(), Difference> {
    take(budget, 0, path)?;
    match value {
        Dispatch::Dynamic(value) => check(value, budget, path),
        Dispatch::Named(entries) => {
            for (key, entry) in entries {
                name(key, path)?;
                check(entry, budget, &format!("{path}.{key}"))?;
            }
            Ok(())
        }
    }
}
fn check_names(
    values: &BTreeSet<String>,
    budget: &mut usize,
    path: &str,
) -> Result<(), Difference> {
    for value in values {
        take(budget, 0, path)?;
        name(value, path)?;
    }
    Ok(())
}
fn check_method(value: &Method, budget: &mut usize, path: &str) -> Result<(), Difference> {
    take(budget, 0, path)?;
    value.args.check(budget, 0, &format!("{path}.args"))?;
    value.result.check(budget, 0, &format!("{path}.result"))?;
    check_names(&value.errors, budget, &format!("{path}.errors"))
}
impl Interface {
    /// Declare a method-name/Value dispatcher explicitly. It is not a missing-schema default.
    pub fn value_dispatch(resources: bool) -> Self {
        let method = Method {
            args: MessageContract::json_value(),
            result: MessageContract::json_value(),
            errors: BTreeSet::from(["*".into()]),
        };
        Self {
            methods: Dispatch::Dynamic(method.clone()),
            resources: if resources {
                Dispatch::Dynamic(Resource {
                    args: MessageContract::json_value(),
                    methods: Dispatch::Dynamic(method),
                    errors: BTreeSet::from(["*".into()]),
                    ownership: "caller".into(),
                    release: "drain_once".into(),
                })
            } else {
                Dispatch::Named(BTreeMap::new())
            },
            capabilities: BTreeSet::new(),
            controls: BTreeMap::new(),
        }
    }
    fn check(&self, budget: &mut usize) -> Result<(), Difference> {
        check_dispatch(&self.methods, budget, "methods", check_method)?;
        check_dispatch(&self.resources, budget, "resources", |r, budget, path| {
            if r.ownership != "caller" || r.release != "drain_once" {
                return Err(fail(path, "unsupported resource lifecycle"));
            }
            r.args.check(budget, 0, &format!("{path}.args"))?;
            check_names(&r.errors, budget, &format!("{path}.errors"))?;
            check_dispatch(&r.methods, budget, &format!("{path}.methods"), check_method)
        })?;
        check_names(&self.capabilities, budget, "capabilities")?;
        for (alias, control) in &self.controls {
            name(alias, "controls")?;
            take(budget, 0, alias)?;
            let endpoint = match control {
                Control::Event { contract, listener } => {
                    check_method(listener, budget, alias)?;
                    Some(contract)
                }
                Control::Middleware { contract, handler } => {
                    check_method(handler, budget, alias)?;
                    Some(contract)
                }
                Control::Factory { config, errors } => {
                    config.check(budget, 0, alias)?;
                    check_names(errors, budget, alias)?;
                    None
                }
                Control::Provision { contract, value } => {
                    value.check(budget, 0, alias)?;
                    Some(contract)
                }
            };
            if let Some(endpoint) = endpoint {
                name(&endpoint.name, alias)?;
                name(&endpoint.revision, alias)?;
            }
        }
        Ok(())
    }
    fn encode(&self, out: &mut Vec<u8>) {
        encode_dispatch(out, &self.methods, encode_method);
        encode_dispatch(out, &self.resources, |out, resource| {
            resource.args.encode(out);
            encode_dispatch(out, &resource.methods, encode_method);
            names(out, &resource.errors);
            string(out, &resource.ownership);
            string(out, &resource.release);
        });
        names(out, &self.capabilities);
        // BTreeMap, fixed struct fields and sorted error sets give a stable
        // current representation; equality above compares the full values.
        string(
            out,
            &serde_json::to_string(&self.controls).expect("control descriptors"),
        );
    }
}
fn size(out: &mut Vec<u8>, size: usize) {
    out.extend_from_slice(&(size as u64).to_be_bytes());
}
fn string(out: &mut Vec<u8>, value: &str) {
    size(out, value.len());
    out.extend_from_slice(value.as_bytes());
}
fn names(out: &mut Vec<u8>, values: &BTreeSet<String>) {
    size(out, values.len());
    for value in values {
        string(out, value);
    }
}
fn encode_method(out: &mut Vec<u8>, value: &Method) {
    value.args.encode(out);
    value.result.encode(out);
    names(out, &value.errors);
}
fn encode_dispatch<T>(out: &mut Vec<u8>, value: &Dispatch<T>, encode: impl Fn(&mut Vec<u8>, &T)) {
    match value {
        Dispatch::Dynamic(value) => {
            out.push(0);
            encode(out, value);
        }
        Dispatch::Named(entries) => {
            out.push(1);
            size(out, entries.len());
            for (key, value) in entries {
                string(out, key);
                encode(out, value);
            }
        }
    }
}
