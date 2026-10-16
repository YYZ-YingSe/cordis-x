use super::{
    Backend, Operation, Plan, Service, Shape,
    plan::{Writer, dependencies},
};
use crate::{
    Module, Result,
    output::Files,
    symbols::{Case, ident, json as lit},
};
pub struct Typescript {
    pub sdk: String,
}
fn value<T: serde::Serialize>(v: &T) -> String {
    format!("JSON.parse({})", lit(&serde_json::to_string(v).unwrap()))
}
fn ty(s: &Shape) -> String {
    s.ts.clone()
}

fn codec(s: &Shape) -> String {
    format!("sdk.typed.protobuf({})", s.schema)
}

fn error_type(w: &mut Writer, p: &str, errors: &std::collections::BTreeSet<String>) {
    if errors.is_empty() {
        return;
    }
    let codes = if errors.contains("*") {
        "string".into()
    } else {
        errors
            .iter()
            .map(|e| lit(e))
            .collect::<Vec<_>>()
            .join(" | ")
    };
    w.line(format!("export class {p}Error extends sdk.RpcError {{ constructor(code: {codes}, message: string) {{ super(code, message); }} }}"));
}
fn operation_descriptor(w: &mut Writer, o: &Operation) {
    w.line(format!(
        "const {}Signature: sdk.MethodContract = {};",
        o.prefix,
        value(&o.signature)
    ));
    error_type(w, &o.prefix, &o.signature.errors);
}
fn signature(w: &mut Writer, o: &Operation) {
    w.line(format!(
        "{}(ctx: sdk.Context, {}args: {}): Promise<{}>;",
        o.name,
        if o.wire.is_none() {
            "method: string, "
        } else {
            ""
        },
        ty(&o.args),
        ty(&o.result)
    ));
}
fn client_method(w: &mut Writer, o: &Operation, resource: bool) {
    let p = &o.prefix;
    w.open(format!(
        "async {}(ctx: sdk.Context, {}args: {}): Promise<{}> {{",
        o.name,
        if o.wire.is_none() {
            "method: string, "
        } else {
            ""
        },
        ty(&o.args),
        ty(&o.result)
    ));
    w.line(format!(
        "const encoded = sdk.typed.argument(args, {p}Signature.args, {}, ctx.validation);",
        codec(&o.args)
    ));
    let method = o.wire.as_deref().map(lit).unwrap_or("method".into());
    let call = if resource {
        format!("this.handle.invoke(ctx, {method}, encoded)")
    } else {
        format!("ctx.invoke(this.dependency, {method}, encoded)")
    };
    w.line(format!(
        "const result = await sdk.typed.checked({p}Signature.errors, async () => {call});"
    ));
    w.line(format!(
        "return sdk.typed.result(result, {p}Signature.result, {}, ctx.validation);",
        codec(&o.result)
    ));
    w.close("}");
}
fn dispatch(w: &mut Writer, methods: &[Operation], server: &str) {
    for o in methods {
        let p = &o.prefix;
        if let Some(m) = &o.wire {
            w.open(format!("if (method === {}) {{", lit(m)));
        } else {
            w.open("{");
        }
        w.line(format!(
            "const decoded = sdk.typed.input(args, {p}Signature.args, {}, ctx.validation);",
            codec(&o.args)
        ));
        w.line(format!("const result = await sdk.typed.checked({p}Signature.errors, async () => {server}.{}(ctx, {}decoded));",o.name,if o.wire.is_none(){"method, "}else{""}));
        w.line(format!(
            "return sdk.typed.output(result, {p}Signature.result, {}, ctx.validation);",
            codec(&o.result)
        ));
        w.close("}");
    }
    if !methods.iter().any(|o| o.wire.is_none()) {
        w.line("throw new sdk.Refusal('unknown_method', method);");
    }
}
fn service(w: &mut Writer, module: &Module, s: &Service) {
    let p = &s.prefix;
    let model = &module.services()[&s.key];
    controls(w, s);
    for r in &s.resources {
        let rp = &r.prefix;
        w.line(format!(
            "const {rp}Signature: sdk.ResourceContract = {};",
            value(&r.contract)
        ));
        error_type(w, rp, &r.contract.errors);
        for o in &r.methods {
            operation_descriptor(w, o);
        }
        w.open(format!("export interface {rp}Server {{"));
        for o in &r.methods {
            signature(w, o);
        }
        w.line("close(ctx: sdk.Context): Promise<void>;");
        w.close("}");
        w.open(format!(
            "export class {rp}Export implements sdk.Resource {{"
        ));
        w.line(format!("private readonly server: {rp}Server;"));
        w.line(format!("constructor(server: {rp}Server) {{ if (!server) throw new Error('null resource implementation'); this.server = server; }}"));
        w.open("async invoke(method: string, args: sdk.Message, ctx?: sdk.Context): Promise<sdk.Message> {");
        w.line("if (!ctx) throw new sdk.Refusal('missing_context', 'resource call requires a current Context');");
        dispatch(w, &r.methods, "this.server");
        w.close("}");
        w.line("async close(ctx?: sdk.Context): Promise<void> { if (!ctx) throw new sdk.Refusal('missing_context', 'resource cleanup requires a current Context'); await this.server.close(ctx); }");
        w.close("}");
        w.open(format!("export class {rp}Client {{"));
        w.line("private readonly handle: sdk.ResourceHandle;");
        w.line("constructor(handle: sdk.ResourceHandle) { this.handle = handle; }");
        for o in &r.methods {
            client_method(w, o, true);
        }
        w.line(
            "async dispose(ctx: sdk.Context): Promise<void> { await this.handle.dispose(ctx); }",
        );
        w.close("}");
    }
    for o in &s.methods {
        operation_descriptor(w, o);
    }
    w.open(format!("export interface {p}Server {{"));
    w.line("activate?(ctx: sdk.Context): Promise<void>;");
    w.line("deactivate?(ctx: sdk.Context): Promise<void>;");
    for o in &s.methods {
        signature(w, o);
    }
    for r in &s.resources {
        w.line(format!(
            "{}(ctx: sdk.Context, {}args: {}): Promise<{}Server>;",
            r.name,
            if r.wire.is_none() {
                "kind: string, "
            } else {
                ""
            },
            ty(&r.args),
            r.prefix
        ));
    }
    w.close("}");
    w.open(format!("export function {p}Export(create: (config: {}) => {p}Server | Promise<{p}Server>, config: sdk.Value): sdk.Service {{",ty(&s.config)));
    w.line(format!(
        "const decoded = sdk.typed.jsonInput(config, {}, {});",
        value(&model.config),
        codec(&s.config)
    ));
    w.line(format!("let server: {p}Server | undefined;"));
    w.line(format!("const current = (): {p}Server => {{ if (!server) throw new sdk.Refusal('inactive', 'service has not been activated'); return server; }};"));
    w.open("return {");
    w.line("typedDispatch: true,");
    w.line(format!("interface: {p}Client.interface(),"));
    let bindings = model.bindings();
    w.line(format!("events: {},", value(&bindings["events"])));
    w.line(format!("middleware: {},", value(&bindings["middleware"])));
    w.line(format!("composition: {},", value(&serde_json::json!({"factories":bindings["factories"], "provisions":bindings["provisions"]}))));
    w.line(format!(
        "contract: {},",
        value(&serde_json::json!({"name":model.name,"revision":model.revision}))
    ));
    let deps: std::collections::BTreeMap<_, _> = model
        .dependencies
        .values()
        .map(|key| {
            let d = &module.services()[key];
            (&d.name, &d.revision)
        })
        .collect();
    w.line(format!("dependencies: {},", value(&deps)));
    w.line(format!(
        "dependencyInterfaces: {},",
        value(&dependencies(module, s))
    ));
    w.line("async activate(ctx) { if (server) throw new sdk.Refusal('already_active', 'service is already active'); server = await create(decoded); if (!server) throw new Error('null service implementation'); await server.activate?.(ctx); },");
    w.line("async deactivate(ctx) { await server?.deactivate?.(ctx); },");
    w.open("async invoke(method, args, ctx) {");
    w.line("const server = current();");
    dispatch(w, &s.methods, "server");
    w.close("},");
    if !s.resources.is_empty() {
        w.open("async acquire(kind, args, ctx) {");
        w.line("const server = current();");
        for r in &s.resources {
            let rp = &r.prefix;
            if let Some(k) = &r.wire {
                w.open(format!("if (kind === {}) {{", lit(k)));
            } else {
                w.open("{");
            }
            w.line(format!(
                "const decoded = sdk.typed.input(args, {rp}Signature.args, {}, ctx.validation);",
                codec(&r.args)
            ));
            w.line(format!("const object = await sdk.typed.checked({rp}Signature.errors, async () => server.{}(ctx, {}decoded));",r.name,if r.wire.is_none(){"kind, "}else{""}));
            w.line(format!("return new {rp}Export(object);"));
            w.close("}");
        }
        if !s.resources.iter().any(|r| r.wire.is_none()) {
            w.line("throw new sdk.Refusal('unknown_resource_kind', kind);");
        }
        w.close("},");
    }
    w.close("};");
    w.close("}");
    w.open(format!("export class {p}Client {{"));
    w.line(format!(
        "private readonly dependency = {};",
        lit(&model.name)
    ));
    w.line(format!(
        "static interface(): sdk.Interface {{ return {}; }}",
        value(&cordis_contract::portable::encode(&model.descriptor))
    ));
    for o in &s.methods {
        client_method(w, o, false);
    }
    for r in &s.resources {
        let rp = &r.prefix;
        w.open(format!(
            "async {}(ctx: sdk.Context, {}args: {}): Promise<{rp}Client> {{",
            r.name,
            if r.wire.is_none() {
                "kind: string, "
            } else {
                ""
            },
            ty(&r.args)
        ));
        w.line(format!(
            "const encoded = sdk.typed.argument(args, {rp}Signature.args, {}, ctx.validation);",
            codec(&r.args)
        ));
        w.line(format!("const handle = await sdk.typed.checked({rp}Signature.errors, async () => ctx.acquire(this.dependency, {}, encoded));",r.wire.as_deref().map(lit).unwrap_or("kind".into())));
        w.line(format!("return new {rp}Client(handle);"));
        w.close("}");
    }
    w.close("}");
    w.open(format!("export class {p}Dependencies {{"));
    for (alias, target) in &model.dependencies {
        let tp = ident(target, Case::Type);
        w.line(format!(
            "static {}(): {tp}Client {{ return new {tp}Client(); }}",
            ident(alias, Case::Member)
        ));
    }
    w.close("}");
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
        w.line(format!("const {}Signature = {signature};", control.prefix));
    }
    w.open(format!("export class {}Controls {{", service.prefix));
    for control in &service.controls {
        let p = &control.prefix;
        let alias = lit(&control.wire);
        let codec = codec(&control.args);
        let expression = match &control.contract {
            Control::Event { .. } | Control::Middleware { .. } => {
                let kind = if matches!(&control.contract, Control::Event { .. }) {
                    "Event"
                } else {
                    "Middleware"
                };
                format!(
                    "new sdk.typed.{kind}({alias}, {p}Signature, {codec}, {})",
                    self::codec(control.result.as_ref().unwrap())
                )
            }
            Control::Factory { .. } => format!(
                "new sdk.typed.Factory({alias}, {p}Signature.config, {p}Signature.errors, {codec})"
            ),
            Control::Provision { .. } => {
                format!("new sdk.typed.Provision({alias}, {p}Signature, {codec})")
            }
        };
        w.line(format!(
            "static {}() {{ return {expression}; }}",
            control.name
        ));
    }
    w.close("}");
}
impl Backend for Typescript {
    fn name(&self) -> &'static str {
        "typescript"
    }
    fn generate(&self, module: &Module, plan: &Plan) -> Result<Files> {
        let mut w = Writer::default();
        w.line("// @generated by cordis-codegen. Edit the contract source and regenerate.");
        w.line(format!("import * as sdk from {};", lit(&self.sdk)));
        let mut standard = std::collections::BTreeSet::new();
        for m in &plan.messages {
            if m.file.starts_with("google/protobuf/") && standard.insert(&m.file) {
                let alias = m.ts.split('.').next().unwrap();
                w.line(format!(
                    "import * as {alias} from '@bufbuild/protobuf/wkt';"
                ));
            }
        }
        for file in &module.files {
            let stem = file.trim_end_matches(".proto");
            let alias = format!("pb_{}", stem.replace(['/', '-', '.'], "_"));
            w.line(format!(
                "import * as {alias} from \"./proto/{stem}_pb.ts\";"
            ));
        }
        for s in &plan.services {
            service(&mut w, module, s);
        }
        let mut files = Files::default();
        files.add("typescript/module.ts", w.finish())?;
        Ok(files)
    }
}
