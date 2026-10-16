//! Manifest builder for an explicit installed native package directory.
use cordis_loader::package::{FileKind, NativeImage, PackageManifest, PackageOptions};
use std::path::PathBuf;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = PathBuf::from(
        args.next()
            .ok_or("usage: cordis-package <directory> <entry> [--host <import>=<file>]...")?,
    );
    let entry = args.next().ok_or("missing package entry")?;
    let mut policy = PackageOptions::default();
    while let Some(flag) = args.next() {
        if flag != "--host" {
            return Err("expected --host <import>=<host-library>".into());
        }
        let value = args.next().ok_or("missing host dependency")?;
        let (name, path) = value.split_once('=').ok_or("expected <import>=<file>")?;
        let id = cordis_loader::ArtifactRef::from_file(path, policy.max_bytes)?.sha256;
        policy.host_dependencies.insert(name.into(), id);
    }
    let mut files = Vec::new();
    // Installed native targets are siblings. No recursive filesystem dependency
    // discovery; assets and script packages can use PackageManifest directly.
    for member in std::fs::read_dir(&root)? {
        let member = member?;
        let name = member
            .file_name()
            .into_string()
            .map_err(|_| "non-UTF8 member")?;
        if name == cordis_loader::package::MANIFEST {
            continue;
        }
        if !member.file_type()?.is_file() {
            return Err(format!("not a regular package member: {name}").into());
        }
        let image = NativeImage::parse(&std::fs::read(member.path())?)?;
        if name == entry && !matches!(image.kind, FileKind::Library | FileKind::Executable) {
            return Err("invalid native entry".into());
        }
        files.push((name, image.kind));
    }
    let manifest = PackageManifest::from_files(&root, entry, files, &policy)?;
    let reference = manifest.write(&root)?;
    policy.verify(&reference)?;
    println!("{}", serde_json::to_string_pretty(&reference)?);
    Ok(())
}
