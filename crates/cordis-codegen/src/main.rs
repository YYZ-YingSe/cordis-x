use cordis_codegen::{
    Diagnostic, Result,
    backend::{self, Backend, cpp::Cpp, rust::Rust, typescript::Typescript},
};
use std::path::PathBuf;
fn run() -> Result<()> {
    let usage = "usage: cordis-codegen generate --input FILE --out DIRECTORY [--proto-path DIR] [--languages cpp,rust,typescript] [--ts-sdk IMPORT] [--rust-package PROTO_PACKAGE=RUST_MODULE] [--protoc FILE] [--protoc-gen-es FILE]";
    let mut args = std::env::args().skip(1);
    let command = args.next().ok_or_else(|| Diagnostic::new("cli", usage))?;
    if command == "--help" {
        println!("{usage}");
        return Ok(());
    }
    if command != "generate" {
        return Err(Diagnostic::new("cli", usage));
    }
    let mut tools = cordis_codegen::GeneratorTools::default();
    let mut includes = Vec::new();
    let mut input = None;
    let mut out = None;
    let mut languages = "cpp,rust,typescript".to_string();
    let mut ts_sdk = "@cordis-x/sdk".to_string();
    let mut rust_packages = Vec::new();
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| Diagnostic::new("cli", format!("missing value for {flag}")))?;
        match flag.as_str() {
            "--protoc" => tools.protoc = value.into(),
            "--protoc-gen-es" => tools.protoc_gen_es = value.into(),
            "--input" => input = Some(value),
            "--proto-path" => includes.push(PathBuf::from(value)),
            "--out" => out = Some(PathBuf::from(value)),
            "--languages" => languages = value,
            "--ts-sdk" => ts_sdk = value,
            "--rust-package" => rust_packages.push(value),
            _ => return Err(Diagnostic::new("cli", format!("unknown argument {flag}"))),
        }
    }
    let input = input.ok_or_else(|| Diagnostic::new("cli", "--input is required"))?;
    let out = out.ok_or_else(|| Diagnostic::new("cli", "--out is required"))?;
    let mut module = cordis_codegen::compile(std::path::Path::new(&input), &includes, &tools)?;
    for mapping in rust_packages {
        let (package, path) = mapping.split_once('=').ok_or_else(|| {
            Diagnostic::new("--rust-package", "expected PROTO_PACKAGE=RUST_MODULE")
        })?;
        module.rust_package(package, path)?;
    }
    let cpp = Cpp;
    let rust = Rust;
    let ts = Typescript { sdk: ts_sdk };
    let mut backends: Vec<&dyn Backend> = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for name in languages.split(',').filter(|name| !name.is_empty()) {
        if !seen.insert(name) {
            return Err(Diagnostic::new("cli", "duplicate language"));
        }
        backends.push(match name {
            "cpp" => &cpp,
            "rust" => &rust,
            "typescript" => &ts,
            _ => {
                return Err(Diagnostic::new(
                    "cli",
                    format!("unsupported language {name:?}"),
                ));
            }
        });
    }
    backend::generate(&module, &backends, &tools)?.write(&out)?;
    println!("generated {}: {}", module.name(), out.display());
    Ok(())
}
fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
