use crate::{ArtifactRef, Error, Result};
use cordis_runtime::Service;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const GROUP: &str = "cordis:group";

/// Deployment input budgets, independent of any individual invocation frame.
#[derive(Clone, Debug)]
pub struct ConfigurationLimits {
    pub bytes: usize,
    pub nodes: usize,
    pub depth: usize,
    pub entries: usize,
}
impl Default for ConfigurationLimits {
    fn default() -> Self {
        Self {
            bytes: 16 * 1024 * 1024,
            nodes: 262_144,
            depth: 64,
            entries: 16_384,
        }
    }
}

/// JSON is a configuration representation, independent of any invocation codec.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    pub entries: Vec<Entry>,
}
impl Configuration {
    pub fn from_json(input: &str) -> Result<Self> {
        Self::from_json_with_limits(input, &ConfigurationLimits::default())
    }
    pub fn from_json_with_limits(input: &str, limits: &ConfigurationLimits) -> Result<Self> {
        if input.len() > limits.bytes {
            return Err(Error::InvalidConfig(
                "configuration exceeds size limit".into(),
            ));
        }
        let value: Value =
            serde_json::from_str(input).map_err(|error| Error::InvalidConfig(error.to_string()))?;
        let mut remaining = limits.nodes;
        let mut pending = vec![(&value, 0)];
        while let Some((value, depth)) = pending.pop() {
            if depth > limits.depth || remaining == 0 {
                return Err(Error::InvalidConfig(
                    "configuration depth/node limit exceeded".into(),
                ));
            }
            remaining -= 1;
            match value {
                Value::Array(values) => pending.extend(values.iter().map(|v| (v, depth + 1))),
                Value::Object(values) => pending.extend(values.values().map(|v| (v, depth + 1))),
                _ => (),
            }
        }
        let config: Self =
            serde_json::from_value(value).map_err(|e| Error::InvalidConfig(e.to_string()))?;
        let mut pending: Vec<_> = config.entries.iter().collect();
        let mut entries = 0;
        while let Some(entry) = pending.pop() {
            entries += 1;
            if entries > limits.entries {
                return Err(Error::InvalidConfig(
                    "configuration entry limit exceeded".into(),
                ));
            }
            pending.extend(&entry.children);
        }
        Ok(config)
    }
}

/// IDs are unique across one loader tree and survive reordering and group moves.
/// `source` names a registered factory, or the built-in `cordis:group`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    pub id: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<ArtifactRef>,
    #[serde(default = "enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub config: Value,
    #[serde(default)]
    pub isolate: Vec<Isolation>,
    #[serde(default)]
    pub intercept: Vec<Intercept>,
    #[serde(default)]
    pub children: Vec<Entry>,
}
fn enabled() -> bool {
    true
}
impl Entry {
    pub fn new(id: impl Into<String>, source: impl Into<String>, config: Value) -> Self {
        Self {
            id: id.into(),
            source: source.into(),
            artifact: None,
            config,
            enabled: true,
            isolate: Vec::new(),
            intercept: Vec::new(),
            children: Vec::new(),
        }
    }
    pub fn group(id: impl Into<String>, children: Vec<Entry>) -> Self {
        Self {
            children,
            ..Self::new(id, GROUP, Value::Null)
        }
    }
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
    pub fn artifact(mut self, artifact: ArtifactRef) -> Self {
        self.artifact = Some(artifact);
        self
    }
    pub fn isolate<T: ?Sized>(mut self, service: &Service<T>, realm: Realm) -> Self {
        self.isolate.push(Isolation {
            name: service.contract().name.to_string(),
            revision: service.contract().revision.to_string(),
            realm,
        });
        self
    }
    pub fn intercept<T: ?Sized>(mut self, service: &Service<T>, config: Value) -> Self {
        self.intercept.push(Intercept {
            name: service.contract().name.to_string(),
            revision: service.contract().revision.to_string(),
            config,
        });
        self
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Intercept {
    pub name: String,
    pub revision: String,
    pub config: Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Isolation {
    pub name: String,
    pub revision: String,
    pub realm: Realm,
}

/// Local realms follow an entry ID. Shared names are scoped to this Loader.
/// Root explicitly clears a realm inherited from a group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Realm {
    Local,
    Shared(String),
    Root,
}
