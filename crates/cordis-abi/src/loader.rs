//! Opt-in native artifact factories. Preparation may execute native loader code.
use crate::{AbiService, ArtifactOptions, Capabilities, Library};
use cordis_loader::{Registry, Result};
use cordis_runtime::{Broker, BrokerRegistration, Service};

/// Register a source whose prepared plugins pin a verified private library copy.
///
/// # Safety
/// EVERY artifact subsequently passed to this source, including previews and
/// disabled entries, must satisfy `Library::load_artifact`'s safety contract.
/// The host must restrict configuration to trusted binaries/dependencies; a
/// content digest alone does not establish trust. Constructors/probing can run
/// during plan/apply and their external effects cannot be rolled back.
#[allow(unsafe_code)]
pub unsafe fn register_library(
    registry: &mut Registry,
    source: impl Into<String>,
    service: Service<AbiService>,
    options: ArtifactOptions,
) -> Result<()> {
    // SAFETY: same lifetime-wide trust contract as this wrapper.
    unsafe { register_context_library(registry, source, service, options, Capabilities::default()) }
}

/// Register a library with host-supplied dependency adapters and Context limits.
/// Components without Context imports use the empty dependency policy.
///
/// # Safety
/// Same obligations as `register_library`, for every future candidate.
#[allow(unsafe_code)]
pub unsafe fn register_context_library(
    registry: &mut Registry,
    source: impl Into<String>,
    service: Service<AbiService>,
    options: ArtifactOptions,
    capabilities: Capabilities,
) -> Result<()> {
    let source = source.into();
    let name = source.clone();
    let policy = options.package.clone();
    registry.register_artifact(
        source,
        move |artifact| {
            policy
                .verify(artifact)
                .map_err(|e| cordis_runtime::Error::Message(e.to_string()))
        },
        move |artifact, config| {
            // SAFETY: registering host accepts this obligation for every candidate.
            configured(
                unsafe { Library::load_artifact(artifact, &options) }?,
                &capabilities,
            )?
            .plugin(&name, &service, config.clone())
            .map_err(Into::into)
        },
    )
}

/// Register independently named rolling candidates. Promotion is explicit and
/// only valid after the candidate activation is Active.
///
/// # Safety
/// Same lifetime-wide artifact/dependency trust obligations as `register_library`.
#[allow(unsafe_code)]
pub unsafe fn register_broker_library<F>(
    registry: &mut Registry,
    source: impl Into<String>,
    broker: Service<Broker<AbiService>>,
    options: ArtifactOptions,
    registered: F,
) -> Result<()>
where
    F: Fn(BrokerRegistration<AbiService>) -> cordis_runtime::Result<()> + Send + Sync + 'static,
{
    // SAFETY: same lifetime-wide trust contract as this wrapper.
    unsafe {
        register_broker_context_library(
            registry,
            source,
            broker,
            options,
            Capabilities::default(),
            registered,
        )
    }
}

/// Register Context-enabled candidates for explicit stable-Broker promotion.
///
/// # Safety
/// Same obligations as `register_library`, for every future candidate.
#[allow(unsafe_code)]
pub unsafe fn register_broker_context_library<F>(
    registry: &mut Registry,
    source: impl Into<String>,
    broker: Service<Broker<AbiService>>,
    options: ArtifactOptions,
    capabilities: Capabilities,
    registered: F,
) -> Result<()>
where
    F: Fn(BrokerRegistration<AbiService>) -> cordis_runtime::Result<()> + Send + Sync + 'static,
{
    let source = source.into();
    let name = source.clone();
    let registered = std::sync::Arc::new(registered);
    let policy = options.package.clone();
    registry.register_artifact(
        source,
        move |artifact| {
            policy
                .verify(artifact)
                .map_err(|e| cordis_runtime::Error::Message(e.to_string()))
        },
        move |artifact, config| {
            let registered = registered.clone();
            // SAFETY: registering host accepts this obligation for every candidate.
            configured(
                unsafe { Library::load_artifact(artifact, &options) }?,
                &capabilities,
            )?
            .broker_plugin(&name, &broker, config.clone(), move |candidate| {
                registered(candidate)
            })
            .map_err(Into::into)
        },
    )
}

fn configured(library: Library, capabilities: &Capabilities) -> crate::Result<Library> {
    if library.supports_context() {
        library.with_context(capabilities.clone())
    } else {
        capabilities.validate(None, None)?;
        Ok(library)
    }
}
