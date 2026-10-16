//! Immutable code packages. All I/O and native metadata work happens at preparation.
//! Native execution and platform loader side effects remain the adapter's contract.
mod native;
use crate::{ArtifactRef, Error, Result};
pub use native::NativeImage;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

pub const HOST_TARGET: &str = env!("CORDIS_PACKAGE_TARGET");
pub const MANIFEST: &str = "cordis-package.json";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileKind {
    Library,
    Executable,
    Data,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageFile {
    pub path: String,
    pub sha256: String,
    pub kind: FileKind,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageManifest {
    pub target: String,
    pub entry: String,
    pub files: Vec<PackageFile>,
    pub host_dependencies: BTreeMap<String, String>,
}
/// Immutable host policy. External identities are deployment assertions, not
/// permissions granted by a package. Hosts must keep their resolution stable.
#[derive(Clone, Debug)]
pub struct PackageOptions {
    pub staging_dir: PathBuf,
    pub max_bytes: usize,
    pub max_manifest_bytes: usize,
    pub max_files: usize,
    pub host_dependencies: BTreeMap<String, String>,
}
impl Default for PackageOptions {
    fn default() -> Self {
        Self {
            staging_dir: std::env::temp_dir(),
            max_bytes: 64 * 1024 * 1024,
            max_manifest_bytes: 1024 * 1024,
            max_files: 1024,
            host_dependencies: native::system_dependencies(),
        }
    }
}
impl PackageOptions {
    pub fn verify(&self, artifact: &ArtifactRef) -> Result<()> {
        VerifiedPackage::read(artifact, self).map(|_| ())
    }
}

/// Build tooling creates this manifest from explicit files; runtime never guesses
/// an import closure from the filesystem. The builder must also inspect native
/// imports and classify every external against its independently chosen policy.
impl PackageManifest {
    pub fn write(&self, root: &Path) -> Result<ArtifactRef> {
        let bytes = serde_json::to_vec_pretty(self).map_err(err)?;
        let path = root.join(MANIFEST);
        fs::write(&path, &bytes).map_err(err)?;
        Ok(ArtifactRef::from_bytes(path_string(&path)?, &bytes))
    }
    pub fn from_files(
        root: &Path,
        entry: String,
        files: Vec<(String, FileKind)>,
        options: &PackageOptions,
    ) -> Result<Self> {
        if files.len() > options.max_files {
            return Err(err("oversized package file list"));
        }
        let mut remaining = options.max_bytes;
        let mut members = Vec::new();
        let mut imports = BTreeSet::new();
        let mut private = BTreeSet::new();
        for (path, kind) in files {
            validate_path(&path)?;
            let bytes =
                crate::artifact::read_file(&path_string(&root.join(&path))?, remaining.max(1))?;
            remaining = remaining
                .checked_sub(bytes.len())
                .ok_or_else(|| err("package exceeds byte limit"))?;
            if kind != FileKind::Data {
                let image = NativeImage::parse(&bytes)?;
                imports.extend(image.imports);
                if kind == FileKind::Library {
                    private.insert(path.rsplit('/').next().unwrap().to_ascii_lowercase());
                }
            }
            members.push(PackageFile {
                path,
                sha256: digest(&bytes),
                kind,
            });
        }
        members.sort_by(|a, b| a.path.cmp(&b.path));
        let mut external = BTreeMap::new();
        for import in imports {
            if private.contains(&native::private_name(&import).to_ascii_lowercase()) {
                continue;
            }
            let identity = options
                .host_dependencies
                .get(&import)
                .ok_or_else(|| err(format!("undeclared host dependency: {import}")))?;
            external.insert(import, identity.clone());
        }
        Ok(Self {
            target: HOST_TARGET.into(),
            entry,
            files: members,
            host_dependencies: external,
        })
    }
}

struct VerifiedPackage {
    manifest: PackageManifest,
    files: Vec<Vec<u8>>,
    images: BTreeMap<String, NativeImage>,
}
impl VerifiedPackage {
    fn read(reference: &ArtifactRef, options: &PackageOptions) -> Result<Self> {
        if options.max_bytes == 0 {
            return Err(err("package byte limit must be positive"));
        }
        let manifest_bytes = reference.read_file(options.max_manifest_bytes)?;
        let manifest: PackageManifest = serde_json::from_slice(&manifest_bytes).map_err(err)?;
        if manifest.target != HOST_TARGET {
            return Err(err("package target does not match host"));
        }
        if manifest.files.is_empty() || manifest.files.len() > options.max_files {
            return Err(err("empty or oversized package file list"));
        }
        validate_path(&manifest.entry)?;
        let mut names = BTreeSet::new();
        let mut spellings = BTreeMap::new();
        for file in &manifest.files {
            validate_path(&file.path)?;
            if !names.insert(file.path.clone()) {
                return Err(err("duplicate package path"));
            }
            for end in file
                .path
                .match_indices('/')
                .map(|(i, _)| i)
                .chain(std::iter::once(file.path.len()))
            {
                let spelling = &file.path[..end];
                if let Some(previous) = spellings.insert(spelling.to_ascii_lowercase(), spelling)
                    && previous != spelling
                {
                    return Err(err("case-aliased package path"));
                }
            }
        }
        if !names.contains(&manifest.entry) {
            return Err(err("package entry is missing"));
        }
        for name in &names {
            for (i, _) in name.match_indices('/') {
                if names.contains(&name[..i]) {
                    return Err(err("package file is also a directory"));
                }
            }
        }
        for (name, identity) in &manifest.host_dependencies {
            if identity.is_empty() || options.host_dependencies.get(name) != Some(identity) {
                return Err(err(format!(
                    "host dependency identity is not authorized: {name}"
                )));
            }
        }
        let source = Path::new(&reference.location)
            .parent()
            .unwrap_or(Path::new("."));
        let root = fs::canonicalize(source).map_err(err)?;
        let mut total = 0usize;
        let mut files = Vec::new();
        let mut images = BTreeMap::new();
        for file in &manifest.files {
            // Reject symlinks in every component; no source symlink is copied into
            // the private snapshot. Source races still cannot change accepted bytes
            // without a digest mismatch. The staging parent is host-controlled.
            let mut path = root.clone();
            for part in file.path.split('/') {
                path.push(part);
                if fs::symlink_metadata(&path)
                    .map_err(err)?
                    .file_type()
                    .is_symlink()
                {
                    return Err(err("symlink in package source"));
                }
            }
            if !fs::metadata(&path).map_err(err)?.is_file() {
                return Err(err("package member is not a regular file"));
            }
            let remaining = options
                .max_bytes
                .checked_sub(total)
                .ok_or_else(|| err("package exceeds byte limit"))?;
            let bytes = ArtifactRef {
                location: path_string(&path)?,
                sha256: file.sha256.clone(),
            }
            .read_file(remaining.max(1))?;
            total = total
                .checked_add(bytes.len())
                .ok_or_else(|| err("package size overflow"))?;
            if total > options.max_bytes {
                return Err(err("package exceeds byte limit"));
            }
            if file.kind != FileKind::Data {
                let image = NativeImage::parse(&bytes)?;
                if image.kind != file.kind {
                    return Err(err(format!(
                        "native kind differs from manifest: {}",
                        file.path
                    )));
                }
                images.insert(file.path.clone(), image);
            }
            files.push(bytes);
        }
        native::verify_links(&manifest, &images)?;
        Ok(Self {
            manifest,
            files,
            images,
        })
    }
}

/// One private snapshot, shared by definitions, modules, sessions and failure
/// records. No reference to mutable source paths is used after construction.
#[derive(Clone, Debug)]
pub struct PreparedPackage(Arc<PackageStorage>);
#[derive(Debug)]
struct PackageStorage {
    root: tempfile::TempDir,
    manifest: PackageManifest,
    native_images: BTreeMap<String, NativeImage>,
}
impl PreparedPackage {
    pub fn prepare(reference: &ArtifactRef, options: &PackageOptions) -> Result<Self> {
        let verified = VerifiedPackage::read(reference, options)?;
        fs::create_dir_all(&options.staging_dir).map_err(err)?;
        let staging = fs::canonicalize(&options.staging_dir).map_err(err)?;
        let mut builder = tempfile::Builder::new();
        builder.prefix("cordis-package-");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            builder.permissions(fs::Permissions::from_mode(0o700));
        }
        let root = builder.tempdir_in(staging).map_err(err)?;
        let storage = PackageStorage {
            root,
            manifest: verified.manifest,
            native_images: verified.images,
        };
        for (member, bytes) in storage.manifest.files.iter().zip(&verified.files) {
            let path = storage.root.path().join(&member.path);
            fs::create_dir_all(path.parent().unwrap()).map_err(err)?;
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(err)?;
            file.write_all(bytes).map_err(err)?;
            file.sync_all().map_err(err)?;
            let mut permissions = file.metadata().map_err(err)?.permissions();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                permissions.set_mode(if member.kind == FileKind::Executable {
                    0o500
                } else {
                    0o400
                });
            }
            #[cfg(not(unix))]
            permissions.set_readonly(true);
            file.set_permissions(permissions).map_err(err)?;
        }
        Ok(Self(Arc::new(storage)))
    }
    pub fn entry(&self) -> PathBuf {
        self.0.root.path().join(&self.0.manifest.entry)
    }
    pub fn root(&self) -> &Path {
        self.0.root.path()
    }
    pub fn manifest(&self) -> &PackageManifest {
        &self.0.manifest
    }
    pub fn entry_kind(&self) -> FileKind {
        self.0
            .manifest
            .files
            .iter()
            .find(|f| f.path == self.0.manifest.entry)
            .unwrap()
            .kind
    }
    pub fn native_image(&self, path: &str) -> Option<&NativeImage> {
        self.0.native_images.get(path)
    }
    /// Static private import graph. File kind, spelling and resolution were
    /// checked against the same buffers that produced these images.
    pub fn private_imports(&self, path: &str) -> Vec<String> {
        self.0
            .native_images
            .get(path)
            .into_iter()
            .flat_map(|image| &image.imports)
            .filter_map(|import| {
                self.0
                    .manifest
                    .files
                    .iter()
                    .find(|member| {
                        member.kind == FileKind::Library
                            && member
                                .path
                                .rsplit('/')
                                .next()
                                .unwrap()
                                .eq_ignore_ascii_case(native::private_name(import))
                    })
                    .map(|member| member.path.clone())
            })
            .collect()
    }
}
impl Drop for PackageStorage {
    fn drop(&mut self) {
        #[cfg(windows)]
        for member in &self.manifest.files {
            let path = self.root.path().join(&member.path);
            if let Ok(metadata) = fs::metadata(&path) {
                let mut permissions = metadata.permissions();
                permissions.set_readonly(false);
                let _ = fs::set_permissions(path, permissions);
            }
        }
        // TempDir cleans only this owned snapshot. A retained PreparedPackage
        // cannot reach this point, including when failure deliberately leaks it.
    }
}
pub fn digest(bytes: &[u8]) -> String {
    ArtifactRef::from_bytes("", bytes).sha256
}
fn path_string(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| err("package path must be UTF-8"))
}
fn err(error: impl std::fmt::Display) -> Error {
    Error::Artifact(error.to_string())
}
fn validate_path(path: &str) -> Result<()> {
    if path.is_empty() || path.len() > 180 {
        return Err(err("invalid package path length"));
    }
    for part in path.split('/') {
        let base = part.split('.').next().unwrap().to_ascii_uppercase();
        if part.is_empty()
            || part == "."
            || part == ".."
            || part.ends_with('.')
            || !part
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"._-@".contains(&c))
            || matches!(base.as_str(), "CON" | "PRN" | "AUX" | "NUL")
            || (base.len() == 4
                && (base.starts_with("COM") || base.starts_with("LPT"))
                && base.as_bytes()[3].is_ascii_digit())
        {
            return Err(err("package paths must be portable relative file paths"));
        }
    }
    Ok(())
}
