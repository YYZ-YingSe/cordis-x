use cordis_loader::{
    ArtifactRef, Configuration, Entry, Loader, Registry,
    package::{
        FileKind, HOST_TARGET, PackageFile, PackageManifest, PackageOptions, PreparedPackage,
        digest,
    },
};
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
struct Files(PathBuf);
impl Files {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "cordis-package-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        Self(root)
    }
    fn options(&self) -> PackageOptions {
        PackageOptions {
            staging_dir: self.0.join("staged"),
            ..Default::default()
        }
    }
    fn manifest(&self) -> PackageManifest {
        let files = [
            ("entry.mjs", b"entry".as_slice()),
            ("lib/helper.mjs", b"helper".as_slice()),
        ];
        for (name, bytes) in files {
            let path = self.0.join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, bytes).unwrap();
        }
        PackageManifest {
            target: HOST_TARGET.into(),
            entry: "entry.mjs".into(),
            files: files
                .into_iter()
                .map(|(path, bytes)| PackageFile {
                    path: path.into(),
                    sha256: digest(bytes),
                    kind: FileKind::Data,
                })
                .collect(),
            host_dependencies: Default::default(),
        }
    }
    fn staged(&self) -> usize {
        std::fs::read_dir(self.0.join("staged"))
            .map(|r| r.count())
            .unwrap_or(0)
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[test]
fn immutable_snapshot_owns_all_members_until_last_reference() {
    let files = Files::new();
    let manifest = files.manifest();
    let reference = manifest.write(&files.0).unwrap();
    let prepared = PreparedPackage::prepare(&reference, &files.options()).unwrap();
    let root = prepared.root().to_owned();
    std::fs::write(files.0.join("lib/helper.mjs"), b"changed").unwrap();
    assert!(files.options().verify(&reference).is_err());
    assert_eq!(
        std::fs::read(root.join("lib/helper.mjs")).unwrap(),
        b"helper"
    );
    assert!(
        std::fs::metadata(prepared.entry())
            .unwrap()
            .permissions()
            .readonly()
    );
    let held = prepared.clone();
    drop(prepared);
    assert!(root.exists());
    drop(held);
    assert!(!root.exists());
}
#[test]
fn helper_change_changes_package_identity_without_changing_entry() {
    let files = Files::new();
    let mut manifest = files.manifest();
    let first = manifest.write(&files.0).unwrap();
    let entry_digest = manifest.files[0].sha256.clone();
    std::fs::write(files.0.join("lib/helper.mjs"), b"next helper").unwrap();
    manifest.files[1].sha256 = digest(b"next helper");
    let next = manifest.write(&files.0).unwrap();
    assert_ne!(first.sha256, next.sha256);
    assert_eq!(entry_digest, manifest.files[0].sha256);
    assert!(files.options().verify(&first).is_err());
    files.options().verify(&next).unwrap();
}
#[test]
fn paths_collisions_targets_and_host_assertions_fail_before_staging() {
    let files = Files::new();
    let valid = files.manifest();
    let reject = |manifest: &PackageManifest| {
        let reference = manifest.write(&files.0).unwrap();
        assert!(PreparedPackage::prepare(&reference, &files.options()).is_err());
        assert_eq!(files.staged(), 0);
    };
    for path in [
        "../outside",
        "/absolute",
        "C:/absolute",
        "a\\b",
        "a//b",
        "CON",
        "a.",
        "a/../b",
    ] {
        let mut m = valid.clone();
        m.entry = path.into();
        m.files[0].path = path.into();
        reject(&m);
    }
    for path in [
        "entry.mjs",
        "ENTRY.mjs",
        "entry.mjs/child",
        "Lib/another.mjs",
    ] {
        let mut m = valid.clone();
        m.files.push(PackageFile {
            path: path.into(),
            sha256: digest(b""),
            kind: FileKind::Data,
        });
        reject(&m);
    }
    let mut m = valid.clone();
    m.entry = "missing".into();
    reject(&m);
    let mut m = valid.clone();
    m.target = "wrong-target".into();
    reject(&m);
    let mut m = valid.clone();
    m.host_dependencies
        .insert("untrusted-runtime".into(), "self-approved".into());
    reject(&m);
    let mut m = valid;
    m.files[1].kind = FileKind::Library;
    reject(&m);
}
#[test]
fn aggregate_limits_missing_files_and_hashes_leave_no_snapshot() {
    let files = Files::new();
    let manifest = files.manifest();
    let reference = manifest.write(&files.0).unwrap();
    for (bytes, entries, metadata) in [(10, 10, 10000), (100, 1, 10000), (100, 10, 1)] {
        let options = PackageOptions {
            max_bytes: bytes,
            max_files: entries,
            max_manifest_bytes: metadata,
            ..files.options()
        };
        assert!(PreparedPackage::prepare(&reference, &options).is_err());
        assert_eq!(files.staged(), 0);
    }
    std::fs::remove_file(files.0.join("lib/helper.mjs")).unwrap();
    assert!(files.options().verify(&reference).is_err());
    assert_eq!(files.staged(), 0);
}
#[cfg(unix)]
#[test]
fn source_symlinks_are_not_followed() {
    let files = Files::new();
    let manifest = files.manifest();
    let reference = manifest.write(&files.0).unwrap();
    std::fs::rename(files.0.join("lib"), files.0.join("elsewhere")).unwrap();
    std::os::unix::fs::symlink("elsewhere", files.0.join("lib")).unwrap();
    assert!(files.options().verify(&reference).is_err());
    assert_eq!(files.staged(), 0);
}
#[tokio::test]
async fn loader_verifies_retained_members_and_plans_dependency_only_changes() {
    let files = Files::new();
    let mut manifest = files.manifest();
    let old = manifest.write(&files.0).unwrap();
    let runtime = cordis_runtime::Runtime::new();
    let mut registry = Registry::new();
    let held = Arc::new(Mutex::new(Vec::new()));
    let verify = files.options();
    let prepare = verify.clone();
    registry
        .register_artifact(
            "package",
            move |artifact| {
                verify
                    .verify(artifact)
                    .map_err(|e| cordis_runtime::Error::Message(e.to_string()))
            },
            {
                let held = held.clone();
                move |artifact, _| {
                    let code = PreparedPackage::prepare(artifact, &prepare)
                        .map_err(|e| cordis_runtime::Error::Message(e.to_string()))?;
                    held.lock().unwrap().push(code);
                    Ok(cordis_runtime::Plugin::new("package", |_| async { Ok(()) }))
                }
            },
        )
        .unwrap();
    let loader = Loader::new(runtime.clone(), registry);
    let config = |artifact: ArtifactRef| Configuration {
        entries: vec![Entry::new("app", "package", serde_json::Value::Null).artifact(artifact)],
    };
    loader.apply(config(old.clone())).await.unwrap();
    loader.settled().await.unwrap();
    let before = loader.snapshot();
    std::fs::write(files.0.join("lib/helper.mjs"), b"next").unwrap();
    assert!(loader.apply(config(old)).await.is_err());
    assert_eq!(loader.snapshot().desired_revision, before.desired_revision);
    manifest.files[1].sha256 = digest(b"next");
    let new = manifest.write(&files.0).unwrap();
    let plan = loader.plan(config(new.clone())).await.unwrap();
    assert_eq!(plan.retire, vec!["app"]);
    assert_eq!(plan.install, vec!["app"]);
    loader.apply(config(new)).await.unwrap();
    loader.settled().await.unwrap();
    runtime.shutdown().await.unwrap();
}
