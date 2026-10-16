use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::OnceLock,
};

fn artifacts() -> &'static Path {
    static BUILD: OnceLock<PathBuf> = OnceLock::new();
    BUILD.get_or_init(|| {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let build = root.join("target/abi-conformance");
        for args in [
            vec![
                "-DCORDIS_BUILD_PROCESS=OFF".into(),
                "-DCORDIS_BUILD_EXAMPLES=ON".into(),
                "-DBUILD_TESTING=ON".into(),
                "-S".into(),
                root.join("sdk/cpp").into_os_string(),
                "-B".into(),
                build.clone().into_os_string(),
            ],
            vec![
                "--build".into(),
                build.clone().into_os_string(),
                "--config".into(),
                "Debug".into(),
                "-j".into(),
                "2".into(),
            ],
        ] {
            let output = Command::new("cmake")
                .args(args)
                .output()
                .expect("CMake and C++20 compiler required for ABI tests");
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        build
    })
}
pub fn binary(name: &str) -> PathBuf {
    let file = format!(
        "{}{name}{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    );
    let path = artifacts().join(&file);
    if path.exists() {
        path
    } else {
        artifacts().join("Debug").join(file)
    }
}

#[cfg(feature = "loader")]
#[path = "../../../../sdk/rust/package_policy.rs"]
mod package_policy;
#[cfg(feature = "loader")]
#[allow(dead_code)]
pub fn options(staging: PathBuf) -> cordis_abi::ArtifactOptions {
    cordis_abi::ArtifactOptions {
        package: package_policy::example_policy(staging),
    }
}
#[cfg(feature = "loader")]
#[allow(dead_code)]
pub fn package(path: &Path) -> cordis_loader::ArtifactRef {
    use cordis_loader::package::{FileKind, PackageManifest};
    let root = path.parent().unwrap();
    let name = path.file_name().unwrap().to_str().unwrap().to_owned();
    let policy = package_policy::example_policy(root.join("staged"));
    PackageManifest::from_files(root, name.clone(), vec![(name, FileKind::Library)], &policy)
        .unwrap()
        .write(root)
        .unwrap()
}
