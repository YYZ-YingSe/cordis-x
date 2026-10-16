//! Package a wit-bindgen core module as a validated Component Model binary.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 2 && !(args.len() == 3 && args[2] == "--wasi-preview1") {
        return Err(
            "usage: cordis-component-pack <guest core.wasm> <output component.wasm> [--wasi-preview1]".into(),
        );
    }
    let module = std::fs::read(&args[0])?;
    let mut encoder = wit_component::ComponentEncoder::default();
    encoder.module(&module)?;
    if args.len() == 3 {
        encoder.adapter(
            "wasi_snapshot_preview1",
            wasi_preview1_component_adapter_provider::WASI_SNAPSHOT_PREVIEW1_REACTOR_ADAPTER,
        )?;
    }
    let component = encoder.validate(true).encode()?;
    std::fs::write(&args[1], component)?;
    Ok(())
}
