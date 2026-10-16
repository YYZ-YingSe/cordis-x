#[path = "../../../../sdk/rust/package_policy.rs"]
mod package_policy;
use cordis_remote::{ArtifactOptions, Endpoint, Launch, ProcessSpec};
pub fn parse() -> Result<(Vec<String>, ArtifactOptions, ProcessSpec), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if !(2..=4).contains(&args.len()) {
        return Err(
            "usage: <v1 artifact> <v2 artifact> [ipc|http] [absolute script interpreter]".into(),
        );
    }
    // The artifact determines the executable; this template selects its protocol.
    let mut spec = ProcessSpec::new("");
    spec.endpoint = match args.get(2).map(String::as_str) {
        None | Some("ipc") => Endpoint::Ipc,
        Some("http") => Endpoint::Http,
        _ => return Err("expected ipc or http".into()),
    };
    if matches!(spec.endpoint, Endpoint::Ipc) {
        spec.args.push("--stdio-only".into());
    }
    let mut options = ArtifactOptions {
        package: package_policy::example_policy(std::env::temp_dir()),
        ..Default::default()
    };
    if let Some(interpreter) = args.get(3) {
        let version = std::process::Command::new(interpreter)
            .args(["-p", "process.version"])
            .output()?;
        if !version.status.success() {
            return Err("example requires a Node interpreter".into());
        }
        options.package.host_dependencies.insert(
            "interpreter".into(),
            format!("node:{}", String::from_utf8(version.stdout)?.trim()),
        );
        options.launch = Launch::Interpreter {
            executable: interpreter.into(),
            args: vec![],
        };
    }
    Ok((args, options, spec))
}
