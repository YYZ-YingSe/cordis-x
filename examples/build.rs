#[path = "../tools/codegen/rust_bindings.rs"]
mod bindings;

fn main() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    bindings::generate(&root.join("generated"), &root.join("resources"));
}
