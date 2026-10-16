use crate::{ArtifactRef, Error, Result, config::GROUP};
use cordis_contract::ContractId;
use cordis_runtime::{InterceptConfig, Plugin, Service};
use serde_json::Value;
use std::{
    any::Any,
    collections::BTreeMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

type Factory = dyn Fn(&Value) -> cordis_runtime::Result<Plugin> + Send + Sync;
type ArtifactFactory = dyn Fn(&ArtifactRef, &Value) -> cordis_runtime::Result<Plugin> + Send + Sync;
type ArtifactVerifier = dyn Fn(&ArtifactRef) -> cordis_runtime::Result<()> + Send + Sync;
type InterceptFactory =
    dyn Fn(&[Value], &mut InterceptConfig) -> cordis_runtime::Result<()> + Send + Sync;
#[derive(Clone)]
enum Source {
    Static(Arc<Factory>),
    Artifact {
        verify: Arc<ArtifactVerifier>,
        build: Arc<ArtifactFactory>,
    },
}

/// Factories validate configuration and construct definitions. They must not start
/// processes, acquire resources, or otherwise perform plugin effects in preflight.
/// Put that work in the returned Plugin's initializer. The registry is immutable
/// after moving it into a Loader; runtime backends remain ordinary plugin factories.
/// Native dynamic-library adapters necessarily run platform constructors/metadata
/// entry points while loading: hosts must explicitly accept their trust contract.
/// Such loader side effects are outside Runtime's business-effect recovery ledger.
/// Definitions must be stable functions of their explicit configuration and pinned
/// code under the registry's immutable host policy. Retained definitions are not
/// reconstructed; live policy changes must be represented by a changed definition.
#[derive(Clone, Default)]
pub struct Registry {
    factories: BTreeMap<String, Source>,
    intercepts: BTreeMap<ContractId, Arc<InterceptFactory>>,
}
impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Portable variant: validate the merged value before changing live state.
    pub fn register_portable_intercept<T: ?Sized, F>(
        &mut self,
        service: &Service<T>,
        merge: F,
    ) -> Result<()>
    where
        F: Fn(&[Value]) -> cordis_runtime::Result<Value> + Send + Sync + 'static,
    {
        if service.contract().name.len() > 1024 || service.contract().revision.len() > 1024 {
            return Err(Error::InvalidConfig(
                "portable intercept contract exceeds 1024 bytes".into(),
            ));
        }
        self.register_intercept(service, move |layers| {
            let value = merge(layers)?;
            let snapshot = serde_json::json!({"revision":"18446744073709551615", "layers":[value]});
            cordis_wire::Payload::from(snapshot)
                .encode()
                .map_err(|e| cordis_runtime::Error::Message(e.to_string()))?;
            Ok(value)
        })
    }

    /// Declare dynamic use-config support for an exact service contract.
    /// Validate and merge ancestor-to-child layers without business effects.
    /// Return a native typed value, or cordis_wire::Value for portable SDKs.
    pub fn register_intercept<T: ?Sized, M, F>(
        &mut self,
        service: &Service<T>,
        merge: F,
    ) -> Result<()>
    where
        M: Any + Send + Sync,
        F: Fn(&[Value]) -> cordis_runtime::Result<M> + Send + Sync + 'static,
    {
        let contract = service.contract().clone();
        if contract.name.trim().is_empty()
            || contract.revision.trim().is_empty()
            || self.intercepts.contains_key(&contract)
        {
            return Err(Error::InvalidConfig(format!(
                "invalid or duplicate intercept contract: {contract}"
            )));
        }
        let service = Service::<()>::new(contract.name.clone(), contract.revision.clone());
        self.intercepts.insert(
            contract,
            Arc::new(move |layers, config| {
                config.set(&service, merge(layers)?);
                Ok(())
            }),
        );
        Ok(())
    }

    pub(crate) fn intercept_config(
        &self,
        id: &str,
        layers: &BTreeMap<ContractId, Vec<Value>>,
    ) -> Result<InterceptConfig> {
        let mut config = InterceptConfig::default();
        for (contract, values) in layers {
            let merge = self.intercepts.get(contract).ok_or_else(|| {
                Error::InvalidConfig(format!(
                    "entry {id}: dynamic intercept is not declared for {contract}"
                ))
            })?;
            catch_unwind(AssertUnwindSafe(|| merge(values, &mut config)))
                .map_err(|_| Error::Factory {
                    entry: id.into(),
                    message: format!("intercept validator panicked: {contract}"),
                })?
                .map_err(|error| Error::Factory {
                    entry: id.into(),
                    message: format!("intercept {contract}: {error}"),
                })?;
        }
        Ok(config)
    }

    /// Bind a source to its current protobuf config message. Validation happens
    /// during preflight, before the factory can run or live state can change.
    /// There is no format detection: this source always accepts ProtoJSON.
    pub fn register_message<F>(
        &mut self,
        source: impl Into<String>,
        config: cordis_contract::MessageContract,
        factory: F,
    ) -> Result<()>
    where
        F: Fn(&Value) -> cordis_runtime::Result<Plugin> + Send + Sync + 'static,
    {
        config.resolve().map_err(Error::InvalidConfig)?;
        self.register(source, move |value| {
            let message = config
                .parse_json(value)
                .map_err(cordis_runtime::Error::Message)?;
            let normalized = serde_json::to_value(message)
                .map_err(|e| cordis_runtime::Error::Message(e.to_string()))?;
            factory(&normalized)
        })
    }

    /// An artifact source with a fixed protobuf configuration contract.
    pub fn register_message_artifact<V, F>(
        &mut self,
        source: impl Into<String>,
        config: cordis_contract::MessageContract,
        verify: V,
        factory: F,
    ) -> Result<()>
    where
        V: Fn(&ArtifactRef) -> cordis_runtime::Result<()> + Send + Sync + 'static,
        F: Fn(&ArtifactRef, &Value) -> cordis_runtime::Result<Plugin> + Send + Sync + 'static,
    {
        config.resolve().map_err(Error::InvalidConfig)?;
        self.register_artifact(source, verify, move |artifact, value| {
            let message = config
                .parse_json(value)
                .map_err(cordis_runtime::Error::Message)?;
            let normalized = serde_json::to_value(message)
                .map_err(|e| cordis_runtime::Error::Message(e.to_string()))?;
            factory(artifact, &normalized)
        })
    }

    pub fn register<F>(&mut self, source: impl Into<String>, factory: F) -> Result<()>
    where
        F: Fn(&Value) -> cordis_runtime::Result<Plugin> + Send + Sync + 'static,
    {
        self.insert(source.into(), Source::Static(Arc::new(factory)))
    }

    /// Prepare and pin a specific artifact before any live instance retires.
    /// The adapter verifies content identity and available static metadata, then
    /// returns a definition that owns the prepared code across every activation.
    /// Execution-only checks (such as process hello/contract negotiation) run in
    /// initialization before publication, not in preflight. A digest cannot prove
    /// those checks will succeed. Do not start business effects here. Calls run
    /// on blocking workers.
    /// `verify` rechecks the identity of a retained definition's source without
    /// loading code or constructing a Plugin. It must cover the same complete
    /// content identity as `factory`, including every member of a bundle.
    /// `factory` verifies and pins its own input on every construction; a prior
    /// verification or plan never authorizes reopening a mutable path unchecked.
    pub fn register_artifact<V, F>(
        &mut self,
        source: impl Into<String>,
        verify: V,
        factory: F,
    ) -> Result<()>
    where
        V: Fn(&ArtifactRef) -> cordis_runtime::Result<()> + Send + Sync + 'static,
        F: Fn(&ArtifactRef, &Value) -> cordis_runtime::Result<Plugin> + Send + Sync + 'static,
    {
        self.insert(
            source.into(),
            Source::Artifact {
                verify: Arc::new(verify),
                build: Arc::new(factory),
            },
        )
    }

    /// File sources whose complete identity is the digest of one bounded file.
    /// Multi-file packages use `register_artifact` with complete member verification.
    pub fn register_file<F>(
        &mut self,
        source: impl Into<String>,
        max_bytes: usize,
        factory: F,
    ) -> Result<()>
    where
        F: Fn(&ArtifactRef, &Value) -> cordis_runtime::Result<Plugin> + Send + Sync + 'static,
    {
        self.register_artifact(
            source,
            move |artifact| {
                artifact
                    .read_file(max_bytes)
                    .map(|_| ())
                    .map_err(|e| cordis_runtime::Error::Message(e.to_string()))
            },
            factory,
        )
    }

    /// Reuse is only requested for a definition already validated by this registry.
    pub(crate) fn verify_retained(
        &self,
        id: &str,
        source: &str,
        artifact: Option<&ArtifactRef>,
    ) -> Result<()> {
        let factory = self
            .factories
            .get(source)
            .ok_or_else(|| Error::InvalidConfig(format!("entry {id}: unknown source {source}")))?;
        catch_unwind(AssertUnwindSafe(|| match (factory, artifact) {
            (Source::Static(_), None) => Ok(()),
            (Source::Artifact { verify, .. }, Some(artifact)) => {
                artifact
                    .validate()
                    .map_err(|e| cordis_runtime::Error::Message(e.to_string()))?;
                verify(artifact)
            }
            _ => Err(cordis_runtime::Error::Message(
                "artifact must be supplied only for an artifact source".into(),
            )),
        }))
        .map_err(|_| Error::Factory {
            entry: id.into(),
            message: "artifact verifier panicked".into(),
        })?
        .map_err(|error| Error::Factory {
            entry: id.into(),
            message: error.to_string(),
        })
    }

    fn insert(&mut self, source: String, factory: Source) -> Result<()> {
        if source.trim().is_empty() || source == GROUP || self.factories.contains_key(&source) {
            return Err(Error::InvalidConfig(format!(
                "invalid or duplicate factory: {source}"
            )));
        }
        self.factories.insert(source, factory);
        Ok(())
    }

    pub(crate) fn build(
        &self,
        id: &str,
        source: &str,
        artifact: Option<&ArtifactRef>,
        config: &Value,
    ) -> Result<Plugin> {
        let factory = self
            .factories
            .get(source)
            .ok_or_else(|| Error::InvalidConfig(format!("entry {id}: unknown source {source}")))?;
        if let Some(artifact) = artifact {
            artifact.validate().map_err(|error| Error::Factory {
                entry: id.into(),
                message: error.to_string(),
            })?;
        }
        catch_unwind(AssertUnwindSafe(|| match (factory, artifact) {
            (Source::Static(factory), None) => factory(config),
            (Source::Artifact { build, .. }, Some(artifact)) => build(artifact, config),
            _ => Err(cordis_runtime::Error::Message(
                "artifact must be supplied only for an artifact source".into(),
            )),
        }))
        .map_err(|_| Error::Factory {
            entry: id.into(),
            message: "factory panicked".into(),
        })?
        .map_err(|error| Error::Factory {
            entry: id.into(),
            message: error.to_string(),
        })
    }
}

#[cfg(test)]
mod message_config_tests {
    use super::*;
    #[test]
    fn configuration_is_checked_before_entering_the_factory() {
        let generated: Value =
            serde_json::from_str(cordis_test_contracts::resources::profile::DESCRIPTOR).unwrap();
        let config = serde_json::from_value(generated["Probe"]["config"].clone()).unwrap();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = calls.clone();
        let mut registry = Registry::new();
        registry
            .register_message("profile", config, move |_| {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(Plugin::new("profile", |_| async { Ok(()) }))
            })
            .unwrap();
        for bad in [
            serde_json::json!({}),
            serde_json::json!({"limit":0}),
            serde_json::json!({"limit":101}),
            serde_json::json!({"limit":10,"unknown":1}),
        ] {
            assert!(registry.build("p", "profile", None, &bad).is_err());
        }
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        registry
            .build("p", "profile", None, &serde_json::json!({"limit":10}))
            .unwrap();
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
