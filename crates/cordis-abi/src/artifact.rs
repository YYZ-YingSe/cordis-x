use crate::{Error, Result};
use cordis_loader::{
    ArtifactRef,
    package::{FileKind, PackageOptions, PreparedPackage},
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock, Weak},
};

#[derive(Clone, Debug, Default)]
pub struct ArtifactOptions {
    pub package: PackageOptions,
}

// A private link identity has one content meaning while in use. Identical
// dependencies share the actual loaded handle AND its original package files.
// This cache holds weak references only; it owns no lifetime independently.
struct Dependency {
    library: OnceLock<std::result::Result<libloading::Library, String>>,
    _dependencies: Vec<Arc<Dependency>>,
    _package: PreparedPackage,
    path: PathBuf,
    digest: String,
}
type Dependencies = BTreeMap<String, Weak<Dependency>>;
fn dependencies() -> &'static Mutex<Dependencies> {
    static DEPENDENCIES: OnceLock<Mutex<Dependencies>> = OnceLock::new();
    DEPENDENCIES.get_or_init(Mutex::default)
}
pub(crate) struct StagedArtifact {
    // Native handles drop before the owning package snapshots.
    _dependencies: Vec<Arc<Dependency>>,
    package: PreparedPackage,
}
impl StagedArtifact {
    /// Caller accepts native constructors and the host loader environment.
    #[allow(unsafe_code)]
    pub unsafe fn prepare(reference: &ArtifactRef, options: &ArtifactOptions) -> Result<Self> {
        let package = PreparedPackage::prepare(reference, &options.package).map_err(load_error)?;
        if package.entry_kind() != FileKind::Library {
            return Err(Error::Load(
                "ABI package entry must be a native library".into(),
            ));
        }
        // Resolve the whole private DAG before running any native constructor.
        // Every shared dependency owns its children; an implicit platform import
        // can therefore never outlive the child handle/identity reservation.
        let mut pending: BTreeMap<_, _> = package
            .manifest()
            .files
            .iter()
            .filter(|file| file.kind == FileKind::Library && file.path != package.manifest().entry)
            .map(|file| (file.path.clone(), package.private_imports(&file.path)))
            .collect();
        let mut order = Vec::new();
        while !pending.is_empty() {
            let next = pending.iter().find(|(_, deps)| deps.iter().all(|dep| order.contains(dep)))
                .map(|(path, _)| path.clone())
                .ok_or_else(|| Error::Load("ABI private dependency cycles or imports of the entry library are not supported".into()))?;
            pending.remove(&next);
            order.push(next);
        }
        let mut shared = dependencies().lock().unwrap();
        shared.retain(|_, value| value.strong_count() != 0);
        let mut loaded = Vec::new();
        let mut by_path = BTreeMap::new();
        for path in order {
            let file = package
                .manifest()
                .files
                .iter()
                .find(|f| f.path == path)
                .unwrap();
            let image = package.native_image(&path).unwrap();
            let mut names = vec![file.path.rsplit('/').next().unwrap().to_ascii_lowercase()];
            if let Some(identity) = &image.identity {
                names.push(identity.clone());
            }
            let existing = names
                .iter()
                .filter_map(|name| shared.get(name).and_then(Weak::upgrade))
                .next();
            for name in &names {
                if let Some(existing) = shared.get(name).and_then(Weak::upgrade)
                    && existing.digest != file.sha256
                {
                    return Err(Error::Load(format!(
                        "native link identity {name} is still in use by different code; rebuild with a unique dependency name"
                    )));
                }
            }
            let dependency = existing.unwrap_or_else(|| {
                Arc::new(Dependency {
                    library: OnceLock::new(),
                    _package: package.clone(),
                    _dependencies: package
                        .private_imports(&path)
                        .iter()
                        .map(|dep| Arc::clone(&by_path[dep]))
                        .collect(),
                    path: package.root().join(&file.path),
                    digest: file.sha256.clone(),
                })
            });
            for name in names {
                shared.insert(name, Arc::downgrade(&dependency));
            }
            by_path.insert(path, dependency.clone());
            loaded.push(dependency);
        }
        drop(shared);
        // No registry lock is held while native constructors execute.
        for dependency in &loaded {
            dependency
                .library
                .get_or_init(|| {
                    // SAFETY: same caller contract as the primary ABI module.
                    unsafe { crate::ffi::open_library(&dependency.path) }.map_err(|e| e.to_string())
                })
                .as_ref()
                .map_err(load_error)?;
        }
        Ok(Self {
            package,
            _dependencies: loaded,
        })
    }
    pub fn path(&self) -> PathBuf {
        self.package.entry()
    }
}
fn load_error(error: impl std::fmt::Display) -> Error {
    Error::Load(error.to_string())
}
