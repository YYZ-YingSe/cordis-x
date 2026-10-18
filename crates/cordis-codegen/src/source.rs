use crate::{Diagnostic, Module, Result, model::Service};
use cordis_contract::{
    Control, Descriptor, Dispatch, Endpoint, Interface, Method, Resource,
    protobuf::{self, option},
};
use heck::ToSnakeCase;
use prost_reflect::{DynamicMessage, MethodDescriptor, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    process::Command,
};

fn text(message: &DynamicMessage, name: &str) -> String {
    match message.get_field_by_name(name).as_deref() {
        Some(Value::String(v)) => v.clone(),
        _ => String::new(),
    }
}
fn errors(method: &MethodDescriptor) -> Result<BTreeSet<String>> {
    let values = match option(&method.options(), "cordis.schema.errors") {
        Some(Value::List(v)) => v,
        _ => vec![],
    };
    let mut result = BTreeSet::new();
    for v in values {
        let Value::String(v) = v else {
            return Err(Diagnostic::new(method.full_name(), "invalid errors"));
        };
        if v.is_empty() || !result.insert(v) {
            return Err(Diagnostic::new(
                method.full_name(),
                "duplicate or empty business error",
            ));
        }
    }
    Ok(result)
}
fn method(module: &Module, method: &MethodDescriptor) -> Result<Method> {
    if method.is_client_streaming() || method.is_server_streaming() {
        return Err(Diagnostic::new(
            method.full_name(),
            "streaming needs a separately defined lifetime/backpressure contract",
        ));
    }
    for m in [method.input(), method.output()] {
        if matches!(m.full_name(), "cordis.data.Lease" | "cordis.data.Child") {
            return Err(Diagnostic::new(
                method.full_name(),
                "Lease/Child are capability result markers for acquires/installs, not message data",
            ));
        }
    }
    Ok(Method {
        args: module.shape(method.input().full_name()),
        result: module.shape(method.output().full_name()),
        errors: errors(method)?,
    })
}
fn endpoint(value: Value, path: &str) -> Result<Endpoint> {
    let Value::Message(value) = value else {
        return Err(Diagnostic::new(path, "expected control routing identity"));
    };
    let endpoint = Endpoint {
        name: text(&value, "name"),
        revision: text(&value, "revision"),
    };
    if endpoint.name.is_empty() || endpoint.revision.is_empty() {
        return Err(Diagnostic::new(
            path,
            "control identity requires name and revision",
        ));
    }
    Ok(endpoint)
}
fn control(module: &Module, rpc: &MethodDescriptor) -> Result<Option<Control>> {
    let kinds: Vec<_> = ["event", "middleware", "installs", "provision", "acquires"]
        .into_iter()
        .filter_map(|kind| {
            option(&rpc.options(), &format!("cordis.schema.{kind}")).map(|value| (kind, value))
        })
        .collect();
    if kinds.len() > 1 {
        return Err(Diagnostic::new(
            rpc.full_name(),
            "an operation has exactly one control kind",
        ));
    }
    let Some((kind, value)) = kinds.into_iter().next() else {
        return Ok(None);
    };
    if kind == "acquires" {
        return Ok(None);
    }
    if rpc.is_client_streaming() || rpc.is_server_streaming() {
        return Err(Diagnostic::new(
            rpc.full_name(),
            "control declarations cannot be streaming RPCs",
        ));
    }
    let result = match kind {
        "event" => Control::Event {
            contract: endpoint(value, rpc.full_name())?,
            listener: method(module, rpc)?,
        },
        "middleware" => Control::Middleware {
            contract: endpoint(value, rpc.full_name())?,
            handler: method(module, rpc)?,
        },
        "installs" => {
            if value != Value::Bool(true) || rpc.output().full_name() != "cordis.data.Child" {
                return Err(Diagnostic::new(
                    rpc.full_name(),
                    "installs=true requires a Child result",
                ));
            }
            Control::Factory {
                config: module.shape(rpc.input().full_name()),
                errors: errors(rpc)?,
            }
        }
        "provision" => {
            if rpc.output().full_name() != "cordis.data.Unit" || !errors(rpc)?.is_empty() {
                return Err(Diagnostic::new(
                    rpc.full_name(),
                    "provision returns Unit and has runtime failures only",
                ));
            }
            Control::Provision {
                contract: endpoint(value, rpc.full_name())?,
                value: module.shape(rpc.input().full_name()),
            }
        }
        _ => unreachable!(),
    };
    Ok(Some(result))
}
fn wire_name(method: &MethodDescriptor) -> String {
    match option(&method.options(), "cordis.schema.wire_name") {
        Some(Value::String(name)) => name,
        _ => method.name().to_snake_case(),
    }
}
pub fn compile(
    input: &Path,
    includes: &[PathBuf],
    tools: &crate::GeneratorTools,
) -> Result<Module> {
    let input =
        dunce::canonicalize(input).map_err(|e| Diagnostic::new(input.display().to_string(), e))?;
    let mut dirs = includes
        .iter()
        .map(|p| dunce::canonicalize(p).map_err(|e| Diagnostic::new(p.display().to_string(), e)))
        .collect::<Result<Vec<_>>>()?;
    if dirs.is_empty() {
        dirs.push(input.parent().unwrap().to_owned());
    }
    let relative = dirs
        .iter()
        .find_map(|p| input.strip_prefix(p).ok())
        .ok_or_else(|| {
            Diagnostic::new(
                "proto-path",
                "input must be inside a proto include directory",
            )
        })?
        .to_owned();
    let relative = crate::paths::relative_name(&relative)?;
    let temp = tempfile::tempdir().map_err(|e| Diagnostic::new("temporary directory", e))?;
    let resources = tempfile::tempdir().map_err(|e| Diagnostic::new("protocol resources", e))?;
    let cordis = resources.path().join("cordis");
    std::fs::create_dir(&cordis).map_err(|e| Diagnostic::new("protocol resources", e))?;
    for (name, contents) in [
        ("options.proto", include_str!(env!("CORDIS_OPTIONS_PROTO"))),
        ("types.proto", include_str!(env!("CORDIS_TYPES_PROTO"))),
    ] {
        std::fs::write(cordis.join(name), contents)
            .map_err(|e| Diagnostic::new("protocol resources", e))?;
    }
    dirs.push(resources.path().to_owned());
    let descriptor_path = temp.path().join("schema.pb");
    let mut command = Command::new(&tools.protoc);
    for path in &dirs {
        command.arg("-I").arg(path);
    }
    let output = command
        .arg("--include_imports")
        .arg(format!(
            "--descriptor_set_out={}",
            descriptor_path.display()
        ))
        .arg(&relative)
        .output()
        .map_err(|e| {
            Diagnostic::new(
                tools.protoc.display().to_string(),
                format!("{e}; provide --protoc or PROTOC"),
            )
        })?;
    if !output.status.success() {
        return Err(Diagnostic::new(
            "protoc",
            String::from_utf8_lossy(&output.stderr),
        ));
    }
    let descriptor =
        std::fs::read(descriptor_path).map_err(|e| Diagnostic::new("descriptor", e))?;
    let pool = prost_reflect::DescriptorPool::decode(descriptor.as_slice())
        .map_err(|e| Diagnostic::new("descriptor", e))?;
    protobuf::check_schema(&pool).map_err(|e| Diagnostic::new("schema", e))?;
    let main = pool
        .get_file_by_name(&relative)
        .ok_or_else(|| Diagnostic::new("input", "missing descriptor"))?;
    let package = main.package_name().to_owned();
    if package.is_empty() {
        return Err(Diagnostic::new(
            "package",
            "an explicit package is required",
        ));
    }
    let files = pool
        .files()
        .filter(|f| !f.name().starts_with("google/protobuf/"))
        .map(|f| f.name().to_owned())
        .collect();
    let mut module = Module {
        _resources: resources,
        name: package.replace('.', "_"),
        services: BTreeMap::new(),
        pool,
        descriptor,
        includes: dirs,
        files,
        rust_packages: BTreeMap::new(),
    };
    let mut identities = BTreeSet::new();
    let mut dependencies = BTreeMap::new();
    let mut control_identities = BTreeMap::new();
    for service in module.pool.services() {
        let Some(Value::Message(contract)) = option(&service.options(), "cordis.schema.contract")
        else {
            continue;
        };
        if protobuf::flag(&service.options(), "cordis.schema.resource") {
            return Err(Diagnostic::new(
                service.full_name(),
                "a resource service cannot also provide a contract",
            ));
        }
        let name = text(&contract, "name");
        let revision = text(&contract, "revision");
        let config = text(&contract, "config");
        if name.is_empty()
            || revision.is_empty()
            || !identities.insert((name.clone(), revision.clone()))
        {
            return Err(Diagnostic::new(
                service.full_name(),
                "missing or duplicate contract identity",
            ));
        }
        let config = module.pool.get_message_by_name(&config).ok_or_else(|| {
            Diagnostic::new(service.full_name(), "config must name a protobuf message")
        })?;
        module
            .shape(config.full_name())
            .resolve()
            .map_err(|e| Diagnostic::new(service.full_name(), e))?;
        let mut methods = BTreeMap::new();
        let mut resources = BTreeMap::new();
        let mut controls = BTreeMap::new();
        let mut operation_names = BTreeSet::new();
        for rpc in service.methods() {
            let wire = wire_name(&rpc);
            if !operation_names.insert(wire.clone()) {
                return Err(Diagnostic::new(rpc.full_name(), "operation name collision"));
            }
            if let Some(control) = control(&module, &rpc)? {
                let identity = match &control {
                    Control::Event { contract, .. } => Some(("event", contract)),
                    Control::Middleware { contract, .. } => Some(("middleware", contract)),
                    Control::Provision { contract, .. } => Some(("provision", contract)),
                    Control::Factory { .. } => None,
                };
                if let Some((kind, endpoint)) = identity {
                    let key = (kind, endpoint.name.clone(), endpoint.revision.clone());
                    if control_identities
                        .get(&key)
                        .is_some_and(|previous| previous != &control)
                    {
                        return Err(Diagnostic::new(
                            rpc.full_name(),
                            "one control routing identity must have one structural contract",
                        ));
                    }
                    control_identities.insert(key, control.clone());
                }
                controls.insert(wire, control);
                continue;
            }
            match option(&rpc.options(), "cordis.schema.acquires") {
                Some(Value::String(resource)) => {
                    if rpc.output().full_name() != "cordis.data.Lease"
                        || rpc.is_client_streaming()
                        || rpc.is_server_streaming()
                    {
                        return Err(Diagnostic::new(
                            rpc.full_name(),
                            "acquires requires a unary Lease result",
                        ));
                    }
                    let resource = module.pool.get_service_by_name(&resource).ok_or_else(|| {
                        Diagnostic::new(rpc.full_name(), "unknown resource service")
                    })?;
                    if !protobuf::flag(&resource.options(), "cordis.schema.resource") {
                        return Err(Diagnostic::new(
                            resource.full_name(),
                            "resource service must be explicitly marked",
                        ));
                    }
                    let mut operations = BTreeMap::new();
                    for rpc in resource.methods() {
                        if control(&module, &rpc)?.is_some() {
                            return Err(Diagnostic::new(
                                rpc.full_name(),
                                "controls belong to a plugin contract, not a resource",
                            ));
                        }
                        if option(&rpc.options(), "cordis.schema.acquires").is_some() {
                            return Err(Diagnostic::new(
                                rpc.full_name(),
                                "nested resource factories are not a data operation",
                            ));
                        }
                        if operations
                            .insert(wire_name(&rpc), method(&module, &rpc)?)
                            .is_some()
                        {
                            return Err(Diagnostic::new(
                                rpc.full_name(),
                                "operation name collision",
                            ));
                        }
                    }
                    if resources
                        .insert(
                            wire,
                            Resource {
                                args: module.shape(rpc.input().full_name()),
                                methods: Dispatch::Named(operations),
                                errors: errors(&rpc)?,
                                ownership: "caller".into(),
                                release: "drain_once".into(),
                            },
                        )
                        .is_some()
                    {
                        return Err(Diagnostic::new(rpc.full_name(), "factory name collision"));
                    }
                }
                None => {
                    if methods.insert(wire, method(&module, &rpc)?).is_some() {
                        return Err(Diagnostic::new(rpc.full_name(), "operation name collision"));
                    }
                }
                _ => {
                    return Err(Diagnostic::new(
                        rpc.full_name(),
                        "invalid acquires annotation",
                    ));
                }
            }
        }
        let mut capabilities = BTreeSet::new();
        if let Some(Value::List(values)) = contract.get_field_by_name("capabilities").as_deref() {
            for value in values {
                let Value::String(value) = value else {
                    return Err(Diagnostic::new(service.full_name(), "invalid capability"));
                };
                if !capabilities.insert(value.clone()) {
                    return Err(Diagnostic::new(service.full_name(), "duplicate capability"));
                }
            }
        }
        let interface = Descriptor::new(Interface {
            methods: Dispatch::Named(methods),
            resources: Dispatch::Named(resources),
            controls,
            capabilities,
        })
        .map_err(|e| Diagnostic::new(service.full_name(), e))?;
        let mut deps = BTreeMap::new();
        if let Some(Value::List(values)) = contract.get_field_by_name("dependencies").as_deref() {
            for value in values {
                let Value::Message(value) = value else {
                    unreachable!()
                };
                if deps
                    .insert(text(value, "alias"), text(value, "service"))
                    .is_some()
                {
                    return Err(Diagnostic::new(
                        service.full_name(),
                        "duplicate dependency alias",
                    ));
                }
            }
        }
        dependencies.insert(service.name().to_owned(), deps);
        if module
            .services
            .insert(
                service.name().to_owned(),
                Service {
                    name,
                    revision,
                    descriptor: interface,
                    config: module.shape(config.full_name()),
                    dependencies: BTreeMap::new(),
                },
            )
            .is_some()
        {
            return Err(Diagnostic::new(
                service.full_name(),
                "duplicate local service symbol",
            ));
        }
    }
    for (key, deps) in dependencies {
        let mut resolved = BTreeMap::new();
        let mut names = BTreeSet::new();
        for (alias, target) in deps {
            let target = module
                .pool
                .get_service_by_name(&target)
                .ok_or_else(|| Diagnostic::new(&alias, "unknown dependency service"))?;
            if option(&target.options(), "cordis.schema.contract").is_none() {
                return Err(Diagnostic::new(
                    &alias,
                    "dependency must provide an explicit contract",
                ));
            }
            let service = module.services.get(target.name()).ok_or_else(|| {
                Diagnostic::new(&alias, "dependency must provide an explicit contract")
            })?;
            if alias.is_empty() || !names.insert(&service.name) {
                return Err(Diagnostic::new(
                    &alias,
                    "empty alias or repeated dependency contract",
                ));
            }
            resolved.insert(alias, target.name().to_owned());
        }
        module.services.get_mut(&key).unwrap().dependencies = resolved;
    }
    if module.services.is_empty() {
        return Err(Diagnostic::new(
            "services",
            "no Cordis contract declarations",
        ));
    }
    Ok(module)
}
