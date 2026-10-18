use std::path::{Path, PathBuf};

pub fn binary(name: &str) -> PathBuf {
    let file = format!(
        "{}{name}{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    );
    cordis_test_contracts::native::all().join(file)
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
