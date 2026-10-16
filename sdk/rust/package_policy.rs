//! Explicit environment for repository examples/tests, never policy read from a plugin.
use cordis_loader::package::PackageOptions;
use std::path::PathBuf;
#[allow(dead_code)]
pub fn example_policy(staging_dir: PathBuf) -> PackageOptions {
    let mut policy = PackageOptions {
        staging_dir,
        ..Default::default()
    };
    let input = std::env::var("CORDIS_EXAMPLE_HOST_LIBRARIES").unwrap_or_else(|_| "{}".into());
    let libraries: cordis_loader::Value = input.parse()
        .expect("CORDIS_EXAMPLE_HOST_LIBRARIES must be a JSON object mapping import names to host library paths");
    for (name, path) in libraries
        .as_object()
        .expect("host libraries must be an object")
    {
        let path = PathBuf::from(path.as_str().expect("host library path must be a string"));
        let identity = cordis_loader::ArtifactRef::from_file(&path, policy.max_bytes)
            .unwrap_or_else(|error| panic!("host library {}: {error}", path.display()))
            .sha256;
        policy.host_dependencies.insert(name.clone(), identity);
    }
    policy
}
