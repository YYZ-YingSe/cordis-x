use crate::{Diagnostic, Result};
use std::path::{Component, Path};

/// Protobuf file identities and the generated manifest use '/' on every host.
pub(crate) fn relative_name(path: &Path) -> Result<String> {
    path.components()
        .map(|component| match component {
            Component::Normal(name) => name.to_str().ok_or_else(|| {
                Diagnostic::new(path.display().to_string(), "file names must be UTF-8")
            }),
            _ => Err(Diagnostic::new(
                path.display().to_string(),
                "expected a relative normal path",
            )),
        })
        .collect::<Result<Vec<_>>>()
        .map(|parts| parts.join("/"))
}
