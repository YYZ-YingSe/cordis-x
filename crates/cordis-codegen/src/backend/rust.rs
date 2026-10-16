use super::{
    Backend, Operation, Plan, Service, Shape,
    plan::{Writer, dependencies},
};
use crate::{
    Diagnostic, Module, Result,
    output::Files,
    symbols::{Case, ident},
};

pub struct Rust;
fn lit(value: &str) -> String {
    // quote uses Rust's literal escaping, including NUL and Unicode.
    quote::quote!(#value).to_string()
}
fn ty(s: &Shape) -> String {
    s.rust.clone()
}

fn shape(t: &cordis_contract::MessageContract) -> String {
    format!(
        "sdk::typed::shape({})",
        lit(&serde_json::to_string(t).unwrap())
    )
}
fn sig(prefix: &str) -> String {
    format!("{}_signature()", ident(prefix, Case::Member))
}
fn signature_literal(w: &mut Writer, prefix: &str, kind: &str, text: String) {
    let ty = if kind == "method" {
        "sdk::Method"
    } else {
        "sdk::ResourceContract"
    };
    w.line(format!("fn {} -> &'static {ty} {{ static VALUE: std::sync::LazyLock<{ty}> = std::sync::LazyLock::new(||sdk::typed::{kind}({})); &VALUE }}",sig(prefix),lit(&text)));
}
fn error_type(w: &mut Writer, prefix: &str, e: &std::collections::BTreeSet<String>) {
    if e.is_empty() {
        return;
    }
    if e.contains("*") {
        w.line(format!(
            "#[derive(Clone, Debug, PartialEq, Eq)] pub struct {prefix}Error(pub String);"
        ));
        w.open(format!("impl {prefix}Error {{"));
        w.line("pub fn failure(self, message: impl ToString) -> sdk::Failure { sdk::error(&self.0, message) }");
        w.close("}");
    } else {
        w.line("#[derive(Clone, Copy, Debug, PartialEq, Eq)]");
        w.open(format!("pub enum {prefix}Error {{"));
        for e in e {
            w.line(format!("{},", ident(e, Case::Type)));
        }
        w.close("}");
        w.open(format!("impl {prefix}Error {{"));
        w.open("pub fn code(self) -> &'static str { match self {");
        for e in e {
            w.line(format!("Self::{} => {},", ident(e, Case::Type), lit(e)));
        }
        w.close("} }");
        w.line("pub fn failure(self, message: impl ToString) -> sdk::Failure { sdk::error(self.code(), message) }");
        w.close("}");
    }
}
fn provider_method(w: &mut Writer, o: &Operation) {
    w.line(format!(
        "fn {}(self: Rc<Self>, ctx: sdk::Context, {}args: {}) -> sdk::LocalFuture<{}>;",
        o.name,
        if o.wire.is_none() {
            "method: String, "
        } else {
            ""
        },
        ty(&o.args),
        ty(&o.result)
    ));
}
fn client_method(w: &mut Writer, o: &Operation, resource: bool) {
    w.open(format!(
        "pub async fn {}(&self, ctx: &sdk::Context, {}args: {}) -> sdk::Result<{}> {{",
        o.name,
        if o.wire.is_none() {
            "method: &str, "
        } else {
            ""
        },
        ty(&o.args),
        ty(&o.result)
    ));
    let sig = sig(&o.prefix);
    w.line(format!(
        "let args = sdk::message::argument(args, &{sig}.args.with_validation(ctx.validation()))?;"
    ));
    let method = o.wire.as_deref().map(lit).unwrap_or("method".into());
    let call = if resource {
        format!("self.handle.invoke(ctx, {method}, args, &{sig}.result)")
    } else {
        format!("ctx.invoke(&self.dependency, {method}, args, &{sig}.result)")
    };
    w.line(format!(
        "let value = {call}.await.map_err(|e| e.check_declared(&{sig}.errors, \"result\"))?;"
    ));
    w.line(format!(
        "sdk::message::result(value, &{sig}.result.with_validation(ctx.validation()))"
    ));
    w.close("}");
}
fn dispatch(w: &mut Writer, methods: &[Operation]) {
    w.open("Box::pin(async move {");
    dispatch_body(w, methods);
    w.close("})");
}
fn dispatch_body(w: &mut Writer, methods: &[Operation]) {
    if methods.is_empty() {
        w.line("let _ = (args, ctx);");
    }
    for o in methods {
        let sig = sig(&o.prefix);
        if let Some(name) = &o.wire {
            w.open(format!("if method == {} {{", lit(name)));
        } else {
            w.open("{");
        }
        w.line("let validation = ctx.validation();");
        w.line(format!("{}sdk::message::output(self.0.clone().{}(ctx, {}sdk::message::input::<{}>(args, &{sig}.args.with_validation(validation))?).await.map_err(|e| e.check_declared(&{sig}.errors, \"result\"))?, &{sig}.result.with_validation(validation)){}",if o.wire.is_none(){""}else{"return "},o.name,
            if o.wire.is_none(){"method, "}else{""},ty(&o.args),if o.wire.is_none(){""}else{";"}));
        w.close("}");
    }
    if !methods.iter().any(|o| o.wire.is_none()) {
        w.line("Err(sdk::refused(\"unknown_method\", method))");
    }
}
fn service(w: &mut Writer, module: &Module, s: &Service) {
    let p = &s.prefix;
    let model = &module.services()[&s.key];
    controls(w, s);
    for r in &s.resources {
        let rp = &r.prefix;
        signature_literal(
            w,
            rp,
            "resource",
            serde_json::to_string(&r.contract).unwrap(),
        );
        error_type(w, rp, &r.contract.errors);
        for o in &r.methods {
            signature_literal(
                w,
                &o.prefix,
                "method",
                serde_json::to_string(&o.signature).unwrap(),
            );
            error_type(w, &o.prefix, &o.signature.errors);
        }
        w.open(format!("pub trait {rp}Server: 'static {{"));
        for o in &r.methods {
            provider_method(w, o);
        }
        w.line("fn close(self: Rc<Self>, ctx: sdk::Context) -> sdk::LocalFuture<()>;");
        w.close("}");
        w.line(format!("pub struct {rp}Export(pub Rc<dyn {rp}Server>);"));
        w.open(format!("impl sdk::ResourceObject for {rp}Export {{"));
        w.line("fn typed_dispatch(&self) -> bool { true }");
        w.open("fn invoke(self: Rc<Self>, ctx: sdk::Context, method: String, args: sdk::Message) -> sdk::message::FutureResult {");
        dispatch(w, &r.methods);
        w.close("}");
        w.line("fn close(self: Rc<Self>, ctx: sdk::Context) -> sdk::LocalFuture<()> { self.0.clone().close(ctx) }");
        w.close("}");
        w.line(format!(
            "#[derive(Clone)] pub struct {rp}Client {{ handle: sdk::Resource }}"
        ));
        w.open(format!("impl {rp}Client {{"));
        for o in &r.methods {
            client_method(w, o, true);
        }
        w.line("pub async fn dispose(&self, ctx: &sdk::Context) -> sdk::Result<()> { self.handle.dispose(ctx).await }");
        w.close("}");
    }
    for o in &s.methods {
        signature_literal(
            w,
            &o.prefix,
            "method",
            serde_json::to_string(&o.signature).unwrap(),
        );
        error_type(w, &o.prefix, &o.signature.errors);
    }
    w.open(format!("pub trait {p}Server: Sized + 'static {{"));
    w.line(format!(
        "fn create(config: {}) -> sdk::Result<Self>;",
        ty(&s.config)
    ));
    w.line("fn activate(self: Rc<Self>, _ctx: sdk::Context) -> sdk::LocalFuture<()> { Box::pin(async { Ok(()) }) }");
    w.line("fn deactivate(self: Rc<Self>, _ctx: sdk::Context) -> sdk::LocalFuture<()> { Box::pin(async { Ok(()) }) }");
    for o in &s.methods {
        provider_method(w, o);
    }
    for r in &s.resources {
        w.line(format!("fn {}(self: Rc<Self>, ctx: sdk::Context, {}args: {}) -> sdk::LocalFuture<Rc<dyn {}Server>>;",r.name,
            if r.wire.is_none(){"kind: String, "}else{""},ty(&r.args),r.prefix));
    }
    w.close("}");
    w.line(format!("pub struct {p}Export<T: {p}Server>(pub Rc<T>);"));
    w.open(format!(
        "impl<T: {p}Server> sdk::Plugin for {p}Export<T> {{"
    ));
    w.line("const TYPED_DISPATCH: bool = true;");
    w.open("fn manifest() -> sdk::PluginManifest {");
    let contract = |name: &str, rev: &str| {
        format!(
            "sdk::Contract {{ name: {}.into(), revision: {}.into() }}",
            lit(name),
            lit(rev)
        )
    };
    w.line(format!(
        "sdk::PluginManifest {{ provides: {}, requires: vec![{}] }}",
        contract(&model.name, &model.revision),
        model
            .dependencies
            .values()
            .map(|k| {
                let d = &module.services()[k];
                contract(&d.name, &d.revision)
            })
            .collect::<Vec<_>>()
            .join(",")
    ));
    w.close("}");
    w.line(format!(
        "fn interface() -> sdk::Interface {{ {p}Client::interface() }}"
    ));
    w.line(format!(
        "fn bindings() -> sdk::Bindings {{ sdk::typed::bindings({}) }}",
        lit(&model.bindings().to_string())
    ));
    w.open("fn dependency_interfaces() -> std::collections::BTreeMap<String,sdk::Interface> {");
    w.line(format!(
        "std::collections::BTreeMap::from([{}])",
        dependencies(module, s)
            .into_iter()
            .map(|(k, v)| format!(
                "({}.into(), sdk::typed::interface({}))",
                lit(&k),
                lit(&v.to_string())
            ))
            .collect::<Vec<_>>()
            .join(",")
    ));
    w.close("}");
    w.line(format!("fn create(config: sdk::Value) -> sdk::Result<Self> {{ Ok(Self(Rc::new(T::create(sdk::json::input(config, &{})?)?))) }}",shape(&model.config)));
    w.line("fn activate(self: Rc<Self>, ctx: sdk::Context) -> sdk::LocalFuture<()> { self.0.clone().activate(ctx) }");
    w.line("fn deactivate(self: Rc<Self>, ctx: sdk::Context) -> sdk::LocalFuture<()> { self.0.clone().deactivate(ctx) }");
    w.open("fn invoke(self: Rc<Self>, ctx: sdk::Context, method: String, args: sdk::Message) -> sdk::message::FutureResult {");
    w.open("Box::pin(async move {");
    dispatch_body(w, &s.methods);
    w.close("})");
    w.close("}");
    if !s.resources.is_empty() {
        w.open("fn acquire(self: Rc<Self>, ctx: sdk::Context, kind: String, args: sdk::Message) -> sdk::LocalFuture<Rc<dyn sdk::ResourceObject>> {");
        w.open("Box::pin(async move {");
        for r in &s.resources {
            let sig = sig(&r.prefix);
            if let Some(kind) = &r.wire {
                w.open(format!("if kind == {} {{", lit(kind)));
            } else {
                w.open("{");
            }
            w.line("let validation = ctx.validation();");
            w.line(format!("let object = self.0.clone().{}(ctx, {}sdk::message::input::<{}>(args, &{sig}.args.with_validation(validation))?).await.map_err(|e| e.check_declared(&{sig}.errors, \"acquire\"))?;",r.name,
                if r.wire.is_none(){"kind, "}else{""},ty(&r.args)));
            w.line(format!(
                "{}Ok(Rc::new({}Export(object)) as Rc<dyn sdk::ResourceObject>){}",
                if r.wire.is_none() { "" } else { "return " },
                r.prefix,
                if r.wire.is_none() { "" } else { ";" }
            ));
            w.close("}");
        }
        if !s.resources.iter().any(|r| r.wire.is_none()) {
            w.line("Err(sdk::refused(\"unknown_resource_kind\", kind))");
        }
        w.close("})");
        w.close("}");
    }
    w.close("}");
    w.line(format!(
        "#[derive(Clone)] pub struct {p}Client {{ dependency: String }}"
    ));
    w.line(format!("impl Default for {p}Client {{ fn default() -> Self {{ Self {{ dependency: {}.into() }} }} }}",lit(&model.name)));
    w.open(format!("impl {p}Client {{"));
    w.line(format!(
        "pub fn interface() -> sdk::Interface {{ sdk::typed::interface({}) }}",
        lit(&cordis_contract::portable::encode(&model.descriptor).to_string())
    ));
    for o in &s.methods {
        client_method(w, o, false);
    }
    for r in &s.resources {
        let sig = sig(&r.prefix);
        w.open(format!(
            "pub async fn {}(&self, ctx: &sdk::Context, {}args: {}) -> sdk::Result<{}Client> {{",
            r.name,
            if r.wire.is_none() { "kind: &str, " } else { "" },
            ty(&r.args),
            r.prefix
        ));
        w.line(format!(
            "let args = sdk::message::argument(args, &{sig}.args.with_validation(ctx.validation()))?;"
        ));
        w.line(format!("let handle = ctx.acquire(&self.dependency, {}, args).await.map_err(|e| e.check_declared(&{sig}.errors, \"acquire\"))?;",r.wire.as_deref().map(lit).unwrap_or("kind".into())));
        w.line(format!("Ok({}Client {{ handle }})", r.prefix));
        w.close("}");
    }
    w.close("}");
    w.line(format!("pub struct {p}Dependencies;"));
    w.open(format!("impl {p}Dependencies {{"));
    for (alias, target) in &model.dependencies {
        let tp = ident(target, Case::Type);
        w.line(format!(
            "pub fn {}() -> {tp}Client {{ {tp}Client::default() }}",
            ident(alias, Case::Member)
        ));
    }
    w.close("}");
}
fn controls(w: &mut Writer, service: &Service) {
    use cordis_contract::Control;
    for control in &service.controls {
        let (value, kind) = match &control.contract {
            Control::Event { listener, .. }
            | Control::Middleware {
                handler: listener, ..
            } => (serde_json::to_string(listener).unwrap(), "method"),
            Control::Factory { .. } => {
                (serde_json::to_string(&control.contract).unwrap(), "control")
            }
            Control::Provision { value, .. } => (serde_json::to_string(value).unwrap(), "shape"),
        };
        if kind == "control" {
            w.line(format!("fn {} -> &'static sdk::ControlContract {{ static VALUE: std::sync::LazyLock<sdk::ControlContract> = std::sync::LazyLock::new(||sdk::typed::control({})); &VALUE }}",sig(&control.prefix),lit(&value)));
        } else if kind == "shape" {
            w.line(format!("fn {} -> &'static sdk::MessageContract {{ static VALUE: std::sync::LazyLock<sdk::MessageContract> = std::sync::LazyLock::new(||sdk::typed::shape({})); &VALUE }}",sig(&control.prefix),lit(&value)));
        } else {
            signature_literal(w, &control.prefix, "method", value);
        }
        match &control.contract {
            Control::Event { listener, .. }
            | Control::Middleware {
                handler: listener, ..
            } => error_type(w, &control.prefix, &listener.errors),
            Control::Factory { errors, .. } => error_type(w, &control.prefix, errors),
            Control::Provision { .. } => {}
        }
    }
    w.line(format!("pub struct {}Controls;", service.prefix));
    w.open(format!("impl {}Controls {{", service.prefix));
    for control in &service.controls {
        let signature = sig(&control.prefix);
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
        w.open(format!(
            "pub fn {}() -> sdk::typed::{kind}<{types}> {{",
            control.name
        ));
        if kind == "Factory" {
            w.line(format!("let sdk::ControlContract::Factory {{ config, errors }} = {signature} else {{ unreachable!() }};"));
            w.line(format!(
                "sdk::typed::Factory::new({}, config, errors)",
                lit(&control.wire)
            ));
        } else {
            w.line(format!(
                "sdk::typed::{kind}::new({}, {signature})",
                lit(&control.wire)
            ));
        }
        w.close("}");
    }
    w.close("}");
}
impl Backend for Rust {
    fn name(&self) -> &'static str {
        "rust"
    }
    fn generate(&self, module: &Module, plan: &Plan) -> Result<Files> {
        let mut w = Writer::default();
        w.line("// @generated by cordis-codegen. Edit the contract source and regenerate.");
        w.line("use cordis_sdk_core as sdk; use std::rc::Rc;");
        w.line("// Preserve the standard generator's schema-derived enum variant names.");
        w.line("#[allow(clippy::enum_variant_names)] pub mod data { include!(\"proto/_messages.rs\"); }");
        for m in &plan.messages {
            if module
                .rust_module(module.message(&m.name).parent_file().package_name())
                .is_some()
            {
                continue;
            }
            w.open(format!("impl sdk::typed::Codec for {} {{", m.rust));
            let constructor = shape(&module.shape(&m.name));
            w.line(format!("fn contract()->&'static sdk::MessageContract{{ static VALUE:std::sync::LazyLock<sdk::MessageContract>=std::sync::LazyLock::new(||{constructor}); &VALUE }}"));
            w.close("}");
        }
        for s in &plan.services {
            service(&mut w, module, s);
        }
        let source = w.finish();
        let syntax = syn::parse_file(&source)
            .map_err(|e| Diagnostic::new("rust", format!("emitter produced invalid Rust: {e}")))?;
        let mut files = Files::default();
        files.add(
            "rust/mod.rs",
            String::from(
                "// @generated by cordis-codegen. Edit the contract source and regenerate.\n",
            ) + &prettyplease::unparse(&syntax),
        )?;
        Ok(files)
    }
}
