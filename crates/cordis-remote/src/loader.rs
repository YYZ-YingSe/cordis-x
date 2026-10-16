//! File preparation is separate from process handshake/activation. Factories
//! never spawn a process. Register only code sources authorized by the host.
use crate::{ArtifactOptions, ProcessArtifact, ProcessSpec, RemoteService};
use cordis_loader::{Registry, Result};
use cordis_runtime::{Broker, BrokerRegistration, Service};

/// The configuration callback chooses arguments, endpoint, codec and bindings;
/// its executable is replaced by the fixed artifact launch. Host policy controls
/// staging, limits and the script interpreter for every subsequent candidate.
pub fn register_process<F>(
    registry: &mut Registry,
    source: impl Into<String>,
    service: Service<RemoteService>,
    options: ArtifactOptions,
    configure: F,
) -> Result<()>
where
    F: Fn(&crate::Value) -> cordis_runtime::Result<ProcessSpec> + Send + Sync + 'static,
{
    let source = source.into();
    let name = source.clone();
    let policy = options.package.clone();
    registry.register_artifact(
        source,
        move |reference| {
            policy
                .verify(reference)
                .map_err(|e| cordis_runtime::Error::Message(e.to_string()))
        },
        move |reference, config| {
            let spec = configure(config)?;
            ProcessArtifact::prepare(reference, &options)?
                .plugin(&name, &service, spec)
                .map_err(Into::into)
        },
    )
}

pub fn register_broker_process<F, R>(
    registry: &mut Registry,
    source: impl Into<String>,
    broker: Service<Broker<RemoteService>>,
    options: ArtifactOptions,
    configure: F,
    registered: R,
) -> Result<()>
where
    F: Fn(&crate::Value) -> cordis_runtime::Result<ProcessSpec> + Send + Sync + 'static,
    R: Fn(BrokerRegistration<RemoteService>) -> cordis_runtime::Result<()> + Send + Sync + 'static,
{
    let source = source.into();
    let name = source.clone();
    let registered = std::sync::Arc::new(registered);
    let policy = options.package.clone();
    registry.register_artifact(
        source,
        move |reference| {
            policy
                .verify(reference)
                .map_err(|e| cordis_runtime::Error::Message(e.to_string()))
        },
        move |reference, config| {
            let spec = configure(config)?;
            let registered = registered.clone();
            ProcessArtifact::prepare(reference, &options)?
                .broker_plugin(&name, &broker, spec, move |token| registered(token))
                .map_err(Into::into)
        },
    )
}
