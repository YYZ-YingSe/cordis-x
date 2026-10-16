use cordis_contract::{Descriptor, MessageContract};
use prost::Message;
use prost_reflect::{DescriptorPool, MessageDescriptor};
use std::{collections::BTreeMap, path::PathBuf};

/// Checked protobuf model. Data declarations belong to protoc, not Cordis emitters.
#[derive(Debug)]
pub struct Module {
    pub(crate) _resources: tempfile::TempDir,
    pub(crate) name: String,
    pub(crate) services: BTreeMap<String, Service>,
    pub(crate) pool: DescriptorPool,
    pub(crate) descriptor: Vec<u8>,
    pub(crate) includes: Vec<PathBuf>,
    pub(crate) files: Vec<String>,
    pub(crate) rust_packages: BTreeMap<String, String>,
}
impl Module {
    /// Reuse a package generated once in a shared Rust DTO crate. This is an
    /// explicit build-time type mapping, never a runtime encoding fallback.
    pub fn rust_package(&mut self, package: &str, path: &str) -> crate::Result<()> {
        if !self.pool.files().any(|file| file.package_name() == package)
            || package.is_empty()
            || package.starts_with('.')
        {
            return Err(crate::Diagnostic::new(
                package,
                "expected an existing protobuf package",
            ));
        }
        let syntax = syn::parse_str::<syn::Path>(path)
            .map_err(|e| crate::Diagnostic::new("Rust package path", e))?;
        if syntax.segments.is_empty() || syntax.segments.iter().any(|s| !s.arguments.is_empty()) {
            return Err(crate::Diagnostic::new(
                path,
                "expected a Rust module path without type arguments",
            ));
        }
        if self.rust_packages.contains_key(package) {
            return Err(crate::Diagnostic::new(
                package,
                "duplicate Rust package mapping",
            ));
        }
        self.rust_packages.insert(package.into(), path.into());
        Ok(())
    }
    pub(crate) fn rust_module(&self, package: &str) -> Option<String> {
        let (prefix, path) = self
            .rust_packages
            .iter()
            .filter(|(prefix, _)| {
                package == prefix.as_str()
                    || package
                        .strip_prefix(prefix.as_str())
                        .is_some_and(|tail| tail.starts_with('.'))
            })
            .max_by_key(|(prefix, _)| prefix.len())?;
        let tail = package[prefix.len()..].trim_start_matches('.');
        let suffix = prost_build::Module::from_protobuf_package_name(tail)
            .parts()
            .collect::<Vec<_>>()
            .join("::");
        Some(if suffix.is_empty() {
            path.clone()
        } else {
            format!("{path}::{suffix}")
        })
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn services(&self) -> &BTreeMap<String, Service> {
        &self.services
    }
    pub fn pool(&self) -> &DescriptorPool {
        &self.pool
    }
    pub fn shape(&self, message: &str) -> MessageContract {
        if message.starts_with("google.protobuf.") {
            MessageContract::standard(message)
        } else {
            // A consumer importing a message must describe that same message
            // independently of its own unrelated services and source files.
            fn visit(
                file: prost_reflect::FileDescriptor,
                seen: &mut std::collections::BTreeSet<String>,
                files: &mut Vec<Vec<u8>>,
            ) {
                if !seen.insert(file.name().to_owned())
                    || file.name().starts_with("google/protobuf/")
                {
                    return;
                }
                let mut dependencies: Vec<_> = file.dependencies().collect();
                dependencies.sort_by(|a, b| a.name().cmp(b.name()));
                for dependency in dependencies {
                    visit(dependency, seen, files);
                }
                files.push(file.encode_to_vec());
            }
            // FileDescriptorSet.file is length-delimited; prost retains the raw
            // FileDescriptorProto bytes, including Cordis extension options.
            #[derive(prost::Message)]
            struct Files {
                #[prost(bytes = "vec", repeated, tag = "1")]
                file: Vec<Vec<u8>>,
            }
            let mut files = Vec::new();
            visit(
                self.message(message).parent_file(),
                &mut Default::default(),
                &mut files,
            );
            MessageContract::new(message, &Files { file: files }.encode_to_vec())
                .expect("validated compiler descriptor")
        }
    }
    pub fn message(&self, name: &str) -> MessageDescriptor {
        self.pool
            .get_message_by_name(name)
            .expect("checked message")
    }
}
#[derive(Debug)]
pub struct Service {
    pub name: String,
    pub revision: String,
    pub descriptor: Descriptor,
    pub config: MessageContract,
    pub dependencies: BTreeMap<String, String>,
}
impl Service {
    /// The current runtime binding manifest, generated from the same controls
    /// as the typed API. Host-selected grants still determine authorization.
    pub fn bindings(&self) -> serde_json::Value {
        use cordis_contract::Control;
        let mut events = BTreeMap::new();
        let mut middleware = BTreeMap::new();
        let mut factories = Vec::new();
        let mut provisions = BTreeMap::new();
        for (alias, control) in &self.descriptor.interface().controls {
            match control {
                Control::Event { contract, .. } => {
                    events.insert(alias, contract);
                }
                Control::Middleware { contract, .. } => {
                    middleware.insert(alias, contract);
                }
                Control::Factory { .. } => factories.push(alias),
                Control::Provision { contract, .. } => {
                    provisions.insert(alias, contract);
                }
            }
        }
        serde_json::json!({"events":events,"middleware":middleware,"factories":factories,"provisions":provisions})
    }
}
