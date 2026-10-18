use crate::{Diagnostic, Result};
use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
};

#[derive(Default)]
pub struct Files(BTreeMap<String, String>);
impl Files {
    pub fn add(&mut self, name: impl Into<String>, text: String) -> Result<()> {
        let name = name.into();
        let path = Path::new(&name);
        if path
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
            || name.contains('\\')
            || name.is_empty()
        {
            return Err(Diagnostic::new(
                &name,
                "output must be a relative normal path",
            ));
        }
        if self.0.contains_key(&name) {
            return Err(Diagnostic::new(name, "duplicate generated file"));
        }
        self.0.insert(name, text);
        Ok(())
    }
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }
    pub fn extend(&mut self, other: Files) -> Result<()> {
        for (path, text) in other.0 {
            self.add(path, text)?;
        }
        Ok(())
    }
    /// Dedicated output directories are replaced as a set. No user-authored files are removed.
    pub fn write(&self, directory: &Path) -> Result<()> {
        let manifest = ".cordis-generated.json";
        let io = |e| Diagnostic::new(directory.display().to_string(), e);
        if directory.ancestors().any(Path::is_symlink) {
            return Err(io("refusing symlinked output path".into()));
        }
        if directory.exists() {
            if directory.join(manifest).is_symlink() {
                return Err(io("refusing symlinked output manifest".into()));
            }
            let old: Vec<String> = serde_json::from_str(
                &std::fs::read_to_string(directory.join(manifest))
                    .map_err(|_| io("existing directory is not owned by codegen".into()))?,
            )
            .map_err(|e| io(e.to_string()))?;
            let mut actual = Vec::new();
            walk(directory, directory, &mut actual)?;
            let expected: std::collections::BTreeSet<_> =
                old.iter().cloned().chain([manifest.into()]).collect();
            if actual.iter().any(|p| !expected.contains(p)) {
                return Err(io("output directory contains unmanaged files".into()));
            }
        }
        let parent = directory
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        std::fs::create_dir_all(parent).map_err(|e| io(e.to_string()))?;
        let stage = parent.join(format!(".cordis-stage-{}", std::process::id()));
        let backup = parent.join(format!(".cordis-backup-{}", std::process::id()));
        if stage.symlink_metadata().is_ok() || backup.symlink_metadata().is_ok() {
            return Err(io("staging directory already exists".into()));
        }
        std::fs::create_dir(&stage).map_err(|e| io(e.to_string()))?;
        let result = (|| {
            for (name, text) in self.iter() {
                let path = stage.join(name);
                std::fs::create_dir_all(path.parent().unwrap()).map_err(|e| io(e.to_string()))?;
                std::fs::write(path, text).map_err(|e| io(e.to_string()))?;
            }
            std::fs::write(
                stage.join(manifest),
                serde_json::to_string_pretty(&self.0.keys().collect::<Vec<_>>()).unwrap() + "\n",
            )
            .map_err(|e| io(e.to_string()))?;
            if directory.exists() {
                std::fs::rename(directory, &backup).map_err(|e| io(e.to_string()))?;
            }
            if let Err(e) = std::fs::rename(&stage, directory) {
                if backup.exists() {
                    let _ = std::fs::rename(&backup, directory);
                }
                return Err(io(e.to_string()));
            }
            if backup.exists() {
                std::fs::remove_dir_all(&backup).map_err(|e| io(e.to_string()))?;
            }
            Ok(())
        })();
        if stage.exists() {
            let _ = std::fs::remove_dir_all(stage);
        }
        result
    }
}
fn walk(root: &Path, at: &Path, files: &mut Vec<String>) -> Result<()> {
    for entry in std::fs::read_dir(at).map_err(|e| Diagnostic::new(at.display().to_string(), e))? {
        let path: PathBuf = entry
            .map_err(|e| Diagnostic::new(at.display().to_string(), e))?
            .path();
        if path.is_symlink() {
            return Err(Diagnostic::new(
                path.display().to_string(),
                "output contains a symlink",
            ));
        }
        if path.is_dir() {
            walk(root, &path, files)?;
        } else {
            files.push(crate::paths::relative_name(
                path.strip_prefix(root).unwrap(),
            )?);
        }
    }
    Ok(())
}
