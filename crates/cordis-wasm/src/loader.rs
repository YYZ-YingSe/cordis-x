//! Optional file artifact adapters for the backend-independent Loader registry.
use crate::{Capabilities, Compiler, ContextService, Limits, WasmService};
use cordis_loader::{Registry, Result};
use cordis_runtime::{Broker, BrokerRegistration, Service};

/// Each prepared Plugin owns its compiled component; activation never reopens
/// the file. Loading and metadata probing finish before Loader retires anything.
pub fn register_component(
    registry: &mut Registry,
    source: impl Into<String>,
    service: Service<WasmService>,
    compiler: Compiler,
    limits: Limits,
) -> Result<()> {
    let source = source.into();
    let name = source.clone();
    registry.register_file(source, limits.artifact_bytes, move |artifact, config| {
        let bytes = artifact
            .read_file(limits.artifact_bytes)
            .map_err(|error| cordis_runtime::Error::Message(error.to_string()))?;
        compiler
            .component(&bytes, limits.clone())?
            .plugin(&name, &service, config.clone())
            .map_err(Into::into)
    })
}

/// Dependencies, Context bindings and WASI authority are supplied by the host
/// registry, never widened by artifact configuration. Required imports and the
/// full dependency manifest are checked by ContextComponent preflight.
pub fn register_context_component(
    registry: &mut Registry,
    source: impl Into<String>,
    service: Service<ContextService>,
    compiler: Compiler,
    limits: Limits,
    capabilities: Capabilities,
) -> Result<()> {
    let source = source.into();
    let name = source.clone();
    registry.register_file(source, limits.artifact_bytes, move |artifact, config| {
        let bytes = artifact
            .read_file(limits.artifact_bytes)
            .map_err(|error| cordis_runtime::Error::Message(error.to_string()))?;
        compiler
            .context_component(&bytes, limits.clone(), capabilities.clone())?
            .plugin(&name, &service, config.clone())
            .map_err(Into::into)
    })
}

/// Prepare independently named rolling candidates. Keep the active Entry while
/// adding a candidate; promote its token after initialization, then remove the
/// old Entry. Loader apply by itself never switches a Broker route.
pub fn register_broker_component<F>(
    registry: &mut Registry,
    source: impl Into<String>,
    broker: Service<Broker<WasmService>>,
    compiler: Compiler,
    limits: Limits,
    registered: F,
) -> Result<()>
where
    F: Fn(BrokerRegistration<WasmService>) -> cordis_runtime::Result<()> + Send + Sync + 'static,
{
    let source = source.into();
    let name = source.clone();
    let registered = std::sync::Arc::new(registered);
    registry.register_file(source, limits.artifact_bytes, move |artifact, config| {
        let bytes = artifact
            .read_file(limits.artifact_bytes)
            .map_err(|error| cordis_runtime::Error::Message(error.to_string()))?;
        let registered = registered.clone();
        compiler
            .component(&bytes, limits.clone())?
            .broker_plugin(&name, &broker, config.clone(), move |candidate| {
                registered(candidate)
            })
            .map_err(Into::into)
    })
}

/// Prepare Context-capable rolling candidates from pinned artifact bytes.
/// Dynamic children remain forbidden under Broker call contexts by the Runtime.
pub fn register_context_broker_component<F>(
    registry: &mut Registry,
    source: impl Into<String>,
    broker: Service<Broker<ContextService>>,
    compiler: Compiler,
    limits: Limits,
    capabilities: Capabilities,
    registered: F,
) -> Result<()>
where
    F: Fn(BrokerRegistration<ContextService>) -> cordis_runtime::Result<()> + Send + Sync + 'static,
{
    let source = source.into();
    let name = source.clone();
    let registered = std::sync::Arc::new(registered);
    registry.register_file(source, limits.artifact_bytes, move |artifact, config| {
        let bytes = artifact
            .read_file(limits.artifact_bytes)
            .map_err(|e| cordis_runtime::Error::Message(e.to_string()))?;
        let registered = registered.clone();
        compiler
            .context_component(&bytes, limits.clone(), capabilities.clone())?
            .broker_plugin(&name, &broker, config.clone(), move |candidate| {
                registered(candidate)
            })
            .map_err(Into::into)
    })
}
