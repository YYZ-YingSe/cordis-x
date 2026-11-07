//! Explicit implementation bindings reference a checked public service. The
//! interface descriptor is never changed by a provider's configuration type or
//! private dependencies, and conflicting declarations are rejected at generation.
use crate::{Diagnostic, Module, Result, model::Implementation};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    config: String,
    #[serde(default, deserialize_with = "unique_map")]
    dependencies: BTreeMap<String, String>,
}
impl Module {
    /// Bind constructors using fully qualified protobuf service/message names.
    /// A package may keep a small implementation declaration next to its service
    /// or supply a separate document; there must be exactly one source for each
    /// selected implementation. No configuration is silently overridden.
    pub fn bind_implementations(&mut self, text: &str) -> Result<()> {
        let mut decoder = serde_json::Deserializer::from_str(text);
        let inputs: BTreeMap<String, Input> =
            unique_map(&mut decoder).map_err(|e| Diagnostic::new("implementation", e))?;
        decoder
            .end()
            .map_err(|e| Diagnostic::new("implementation", e))?;
        let mut prepared = BTreeMap::new();
        for (name, input) in inputs {
            let service = self
                .pool
                .get_service_by_name(&name)
                .ok_or_else(|| Diagnostic::new(&name, "unknown implementation service"))?;
            let key = service.name().to_owned();
            let declaration = self.services.get(&key).ok_or_else(|| {
                Diagnostic::new(&name, "implementation requires a public contract")
            })?;
            if declaration.implementation.is_some() {
                return Err(Diagnostic::new(
                    &name,
                    "duplicate implementation configuration; keep its declaration in one location",
                ));
            }
            let config = self
                .pool
                .get_message_by_name(&input.config)
                .ok_or_else(|| {
                    Diagnostic::new(&input.config, "configuration must name a protobuf message")
                })?;
            let config = self.shape(config.full_name());
            config.resolve().map_err(|e| Diagnostic::new(&name, e))?;
            let mut dependencies = BTreeMap::new();
            let mut identities = BTreeSet::new();
            for (alias, target) in input.dependencies {
                let dependency = self
                    .pool
                    .get_service_by_name(&target)
                    .ok_or_else(|| Diagnostic::new(&target, "unknown implementation dependency"))?;
                let contract = self.services.get(dependency.name()).ok_or_else(|| {
                    Diagnostic::new(&target, "dependency requires a public contract")
                })?;
                if alias.is_empty() || !identities.insert(contract.name.clone()) {
                    return Err(Diagnostic::new(
                        &alias,
                        "empty alias or repeated dependency contract",
                    ));
                }
                dependencies.insert(alias, dependency.name().into());
            }
            prepared.insert(
                key,
                Implementation {
                    config,
                    dependencies,
                },
            );
        }
        for (key, binding) in prepared {
            self.services
                .get_mut(&key)
                .expect("checked service")
                .implementation = Some(binding);
        }
        Ok(())
    }
}

/// A declaration file has one author for each service and dependency alias.
/// Reject duplicate JSON keys instead of silently retaining the final value.
fn unique_map<'de, D, T>(decoder: D) -> std::result::Result<BTreeMap<String, T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Visitor<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for Visitor<T> {
        type Value = BTreeMap<String, T>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("an object with unique declaration keys")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut values: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut result = BTreeMap::new();
            while let Some((key, value)) = values.next_entry::<String, T>()? {
                if result.insert(key.clone(), value).is_some() {
                    return Err(serde::de::Error::custom(format!(
                        "duplicate declaration {key}"
                    )));
                }
            }
            Ok(result)
        }
    }
    decoder.deserialize_map(Visitor(std::marker::PhantomData))
}
