use super::{
    Backend, Operation, Plan, Service, Shape,
    plan::{Writer, dependencies},
};
use crate::{
    Module, Result,
    output::Files,
    symbols::{Case, cpp as lit, ident},
};
pub struct Cpp;
fn text(s: &str) -> String {
    // Separate std::string expressions avoid the compiler's single-literal
    // limit. Adjacent C++ literals would be concatenated before that check.
    let mut chunks = Vec::new();
    let mut start = 0;
    for (end, _) in s.char_indices() {
        if end - start >= 8_000 {
            chunks.push(format!(
                "std::string({}, {})",
                lit(&s[start..end]),
                end - start
            ));
            start = end;
        }
    }
    chunks.push(format!(
        "std::string({}, {})",
        lit(&s[start..]),
        s.len() - start
    ));
    chunks.join(" + ")
}
fn value<T: serde::Serialize>(v: &T) -> String {
    format!(
        "cordis::Value::parse({})",
        text(&serde_json::to_string(v).unwrap())
    )
}
fn ty(s: &Shape) -> String {
    s.cpp.clone()
}

fn error_type(w: &mut Writer, prefix: &str, errors: &std::collections::BTreeSet<String>) {
    if errors.is_empty() {
        return;
    }
    if errors.contains("*") {
        w.line(format!("struct {prefix}Error {{ std::string code; }};"));
        w.line(format!("inline cordis::RpcError failure({prefix}Error error, std::string message) {{ return {{std::move(error.code), std::move(message)}}; }}"));
    } else {
        w.line(format!(
            "enum class {prefix}Error {{ {} }};",
            errors
                .iter()
                .map(|e| ident(e, Case::Type))
                .collect::<Vec<_>>()
                .join(", ")
        ));
        w.open(format!(
            "inline cordis::RpcError failure({prefix}Error error, std::string message) {{"
        ));
        w.open("switch (error) {");
        for e in errors {
            w.line(format!(
                "case {prefix}Error::{}: return {{{}, std::move(message)}};",
                ident(e, Case::Type),
                text(e)
            ));
        }
        w.close("}");
        w.line("throw std::invalid_argument(\"invalid generated error code\");");
        w.close("}");
    }
}
fn signature(w: &mut Writer, o: &Operation) {
    w.line(format!(
        "virtual cordis::Async<{}> {}(cordis::Context ctx, {}{} args) = 0;",
        ty(&o.result),
        o.name,
        if o.wire.is_none() {
            "std::string method, "
        } else {
            ""
        },
        ty(&o.args)
    ));
}
fn operation_descriptor(w: &mut Writer, o: &Operation) {
    w.line(format!("inline const cordis::Value& {}Signature() {{ static const auto value = {}; return value; }}",o.prefix,value(&o.signature)));
    error_type(w, &o.prefix, &o.signature.errors);
}
fn client_method(w: &mut Writer, o: &Operation, resource: bool) {
    w.open(format!(
        "cordis::Async<{}> {}(cordis::Context ctx, {}{} args) const {{",
        ty(&o.result),
        o.name,
        if o.wire.is_none() {
            "std::string method, "
        } else {
            ""
        },
        ty(&o.args)
    ));
    let method = o
        .wire
        .as_deref()
        .map(text)
        .unwrap_or("std::move(method)".into());
    let p = &o.prefix;
    w.line(format!("return cordis::typed::{}<{}, {}>({}, {}, std::move(args), {p}Signature().at(\"args\"), {p}Signature().at(\"result\"), {p}Signature().at(\"errors\"));",
        if resource{"resource_call"}else{"invoke"},ty(&o.args),ty(&o.result),
        if resource{"handle_, std::move(ctx)"}else{"std::move(ctx), dependency_"},method));
    w.close("}");
}
fn dispatch(w: &mut Writer, methods: &[Operation], prefix: &str) {
    w.open(format!("static cordis::Async<cordis::Message> invoke_impl(std::shared_ptr<{prefix}Server> server, cordis::Context ctx, std::string method, cordis::Message args) {{"));
    if methods.is_empty() {
        w.line("(void)server; (void)ctx; (void)args;");
    }
    for o in methods {
        if let Some(method) = &o.wire {
            w.open(format!("if (method == {}) {{", text(method)));
        } else {
            w.open("{");
        }
        let p = &o.prefix;
        w.line("const auto validation = ctx.validation();");
        w.line(format!(
            "auto decoded = cordis::typed::input<{}>(args, {p}Signature().at(\"args\"), validation);",
            ty(&o.args)
        ));
        w.line(format!("auto result = co_await cordis::typed::checked({p}Signature().at(\"errors\"), [&] {{ return server->{}(std::move(ctx), {}std::move(decoded)); }});",o.name,if o.wire.is_none(){"std::move(method), "}else{""}));
        w.line(format!(
            "co_return cordis::typed::output(std::move(result), {p}Signature().at(\"result\"), validation);"
        ));
        w.close("}");
    }
    if !methods.iter().any(|o| o.wire.is_none()) {
        if methods.is_empty() {
            w.line("co_await corral::noop();");
        }
        w.line("throw cordis::Refusal(\"unknown_method\", method);");
    }
    w.close("}");
    w.line("cordis::Async<cordis::Message> invoke(cordis::Context ctx, std::string method, cordis::Message args) override { return invoke_impl(server_, std::move(ctx), std::move(method), std::move(args)); }");
}
fn service(w: &mut Writer, module: &Module, s: &Service) {
    let p = &s.prefix;
    let model = &module.services()[&s.key];
    controls(w, s);
    for r in &s.resources {
        let rp = &r.prefix;
        w.line(format!("inline const cordis::Value& {rp}Signature() {{ static const auto value = {}; return value; }}",value(&r.contract)));
        error_type(w, rp, &r.contract.errors);
        for o in &r.methods {
            operation_descriptor(w, o);
        }
        w.open(format!("struct {rp}Server {{"));
        w.line(format!("virtual ~{rp}Server() = default;"));
        for o in &r.methods {
            signature(w, o);
        }
        w.line("virtual cordis::Async<void> close(cordis::Context ctx) = 0;");
        w.close("};");
        w.open(format!(
            "class {rp}Export final : public cordis::Resource {{"
        ));
        w.line(format!("std::shared_ptr<{rp}Server> server_;"));
        w.line("public:");
        w.line(format!("explicit {rp}Export(std::shared_ptr<{rp}Server> server) : server_(std::move(server)) {{ if (!server_) throw std::runtime_error(\"null resource implementation\"); }}"));
        dispatch(w, &r.methods, rp);
        w.line("cordis::Async<void> close(cordis::Context ctx) override { return cordis::typed::close(server_, std::move(ctx)); }");
        w.close("};");
        w.open(format!("class {rp}Client {{"));
        w.line("cordis::ResourceHandle handle_;");
        w.line("public:");
        w.line(format!(
            "explicit {rp}Client(cordis::ResourceHandle handle) : handle_(std::move(handle)) {{}}"
        ));
        for o in &r.methods {
            client_method(w, o, true);
        }
        w.line("cordis::Async<void> dispose(cordis::Context ctx) const { return handle_.dispose(std::move(ctx)); }");
        w.close("};");
    }
    for o in &s.methods {
        operation_descriptor(w, o);
    }
    w.open(format!("struct {p}Server {{"));
    w.line(format!("virtual ~{p}Server() = default;"));
    w.line("virtual cordis::Async<void> activate(cordis::Context) { co_return; }");
    w.line("virtual cordis::Async<void> deactivate(cordis::Context) { co_return; }");
    for o in &s.methods {
        signature(w, o);
    }
    for r in &s.resources {
        w.line(format!("virtual cordis::Async<std::shared_ptr<{}Server>> {}(cordis::Context ctx, {}{} args) = 0;",r.prefix,r.name,if r.wire.is_none(){"std::string kind, "}else{""},ty(&r.args)));
    }
    w.close("};");
    w.open(format!("class {p}Export final : public cordis::Service {{"));
    w.line(format!("std::shared_ptr<{p}Server> server_;"));
    w.line("public:");
    w.line("bool typed_dispatch() const override { return true; }");
    w.line(format!("explicit {p}Export(std::shared_ptr<{p}Server> server) : server_(std::move(server)) {{ if (!server_) throw std::runtime_error(\"null service implementation\"); }}"));
    w.line("cordis::Async<void> activate(cordis::Context ctx) override { return cordis::typed::activate(server_, std::move(ctx)); }");
    w.line("cordis::Async<void> deactivate(cordis::Context ctx) override { return cordis::typed::deactivate(server_, std::move(ctx)); }");
    dispatch(w, &s.methods, p);
    if !s.resources.is_empty() {
        w.open(format!("static cordis::Async<std::shared_ptr<cordis::Resource>> acquire_impl(std::shared_ptr<{p}Server> server, cordis::Context ctx, std::string kind, cordis::Message args) {{"));
        for r in &s.resources {
            let rp = &r.prefix;
            if let Some(kind) = &r.wire {
                w.open(format!("if (kind == {}) {{", text(kind)));
            } else {
                w.open("{");
            }
            w.line(format!(
                "auto decoded = cordis::typed::input<{}>(args, {rp}Signature().at(\"args\"), ctx.validation());",
                ty(&r.args)
            ));
            w.line(format!("auto object = co_await cordis::typed::checked({rp}Signature().at(\"errors\"), [&] {{ return server->{}(std::move(ctx), {}std::move(decoded)); }});",r.name,if r.wire.is_none(){"std::move(kind), "}else{""}));
            w.line(format!(
                "co_return std::make_shared<{rp}Export>(std::move(object));"
            ));
            w.close("}");
        }
        if !s.resources.iter().any(|r| r.wire.is_none()) {
            w.line("throw cordis::Refusal(\"unknown_resource_kind\", kind);");
        }
        w.close("}");
        w.line("cordis::Async<std::shared_ptr<cordis::Resource>> acquire(cordis::Context ctx, std::string kind, cordis::Message args) override { return acquire_impl(server_, std::move(ctx), std::move(kind), std::move(args)); }");
    }
    w.close("};");
    w.open(format!("class {p}Client {{"));
    w.line(format!("std::string dependency_ = {};", text(&model.name)));
    w.line("public:");
    w.line(format!(
        "static cordis::Value interface() {{ return {}; }}",
        value(&cordis_contract::portable::encode(&model.descriptor))
    ));
    for o in &s.methods {
        client_method(w, o, false);
    }
    for r in &s.resources {
        let rp = &r.prefix;
        w.open(format!(
            "cordis::Async<{rp}Client> {}(cordis::Context ctx, {}{} args) const {{",
            r.name,
            if r.wire.is_none() {
                "std::string kind, "
            } else {
                ""
            },
            ty(&r.args)
        ));
        w.line(format!("return cordis::typed::acquire<{rp}Client>(std::move(ctx), dependency_, {}, std::move(args), {rp}Signature().at(\"args\"), {rp}Signature().at(\"errors\"));",r.wire.as_deref().map(text).unwrap_or("std::move(kind)".into())));
        w.close("}");
    }
    w.close("};");
    w.open(format!("template <class T> struct {p}Model {{"));
    w.line(format!(
        "static std::string contract_name() {{ return {}; }}",
        text(&model.name)
    ));
    w.line(format!(
        "static std::string contract_revision() {{ return {}; }}",
        text(&model.revision)
    ));
    w.line(format!(
        "static cordis::Value interface() {{ return {p}Client::interface(); }}"
    ));
    let bindings = model.bindings();
    w.line(format!(
        "static cordis::Value events() {{ return {}; }}",
        value(&bindings["events"])
    ));
    w.line(format!(
        "static cordis::Value middleware() {{ return {}; }}",
        value(&bindings["middleware"])
    ));
    w.line(format!("static cordis::Value composition() {{ return {}; }}", value(&serde_json::json!({"factories":bindings["factories"],"provisions":bindings["provisions"]}))));
    let deps: std::collections::BTreeMap<_, _> = model
        .dependencies
        .values()
        .map(|key| {
            let d = &module.services()[key];
            (d.name.clone(), d.revision.clone())
        })
        .collect();
    w.line(format!(
        "static cordis::Value dependencies() {{ return {}; }}",
        value(&deps)
    ));
    w.line(format!(
        "static cordis::Value dependency_interfaces() {{ return {}; }}",
        value(&dependencies(module, s))
    ));
    w.line(format!("static std::shared_ptr<cordis::Service> create(cordis::Value config) {{ return std::make_shared<{p}Export>(T::create(cordis::typed::json_input<{}>(config, {}))); }}",ty(&s.config),value(&model.config)));
    w.close("};");
}
fn controls(w: &mut Writer, service: &Service) {
    use cordis_contract::Control;
    for control in &service.controls {
        let signature = match &control.contract {
            Control::Event { listener, .. }
            | Control::Middleware {
                handler: listener, ..
            } => {
                error_type(w, &control.prefix, &listener.errors);
                value(listener)
            }
            Control::Factory { errors, .. } => {
                error_type(w, &control.prefix, errors);
                value(&control.contract)
            }
            Control::Provision { value: shape, .. } => value(shape),
        };
        w.line(format!("inline const cordis::Value& {}Signature() {{ static const auto value = {signature}; return value; }}",control.prefix));
    }
    w.open(format!("struct {}Controls {{", service.prefix));
    for control in &service.controls {
        let (kind, types) = match &control.contract {
            Control::Event { .. } => (
                "Event",
                format!(
                    "{}, {}",
                    ty(&control.args),
                    ty(control.result.as_ref().unwrap())
                ),
            ),
            Control::Middleware { .. } => (
                "Middleware",
                format!(
                    "{}, {}",
                    ty(&control.args),
                    ty(control.result.as_ref().unwrap())
                ),
            ),
            Control::Factory { .. } => ("Factory", ty(&control.args)),
            Control::Provision { .. } => ("Provision", ty(&control.args)),
        };
        let signature = format!("{}Signature()", control.prefix);
        let args = if kind == "Factory" {
            format!("{signature}.at(\"config\"), {signature}.at(\"errors\")")
        } else {
            signature
        };
        w.line(format!(
            "static cordis::typed::{kind}<{types}> {}() {{ return {{ {}, {args} }}; }}",
            control.name,
            text(&control.wire)
        ));
    }
    w.close("};");
}
impl Backend for Cpp {
    fn name(&self) -> &'static str {
        "cpp"
    }
    fn generate(&self, module: &Module, plan: &Plan) -> Result<Files> {
        let mut w = Writer::default();
        let ns = ident(module.name(), Case::Member);
        w.line("// @generated by cordis-codegen. Edit the contract source and regenerate.");
        w.line("#pragma once\n#include <cordis/core/typed.hpp>");
        for file in &module.files {
            w.line(format!(
                "#include \"proto/{}.pb.h\"",
                file.trim_end_matches(".proto")
            ));
        }
        w.open(format!("namespace {ns} {{"));
        for s in &plan.services {
            service(&mut w, module, s);
        }
        for s in &plan.services {
            w.open(format!("struct {}Dependencies {{", s.prefix));
            for (alias, target) in &module.services()[&s.key].dependencies {
                w.line(format!(
                    "static {}Client {}() {{ return {{}}; }}",
                    ident(target, Case::Type),
                    ident(alias, Case::Member)
                ));
            }
            w.close("};");
        }
        w.close("}");
        let mut files = Files::default();
        files.add("cpp/module.hpp", w.finish())?;
        Ok(files)
    }
}
