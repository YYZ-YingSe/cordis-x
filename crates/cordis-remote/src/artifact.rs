use crate::{Error, ProcessSpec, RemoteService, Result};
use cordis_loader::{
    ArtifactRef,
    package::{FileKind, PackageOptions, PreparedPackage},
};
use cordis_runtime::{Broker, BrokerRegistration, Plugin, Service};
use std::{path::PathBuf, sync::Arc};

/// Execution mode is chosen by the host, never by a package.
#[derive(Clone, Debug, Default)]
pub enum Launch {
    #[default]
    Executable,
    Interpreter {
        executable: PathBuf,
        args: Vec<String>,
    },
}
#[derive(Clone, Debug, Default)]
pub struct ArtifactOptions {
    pub launch: Launch,
    pub package: PackageOptions,
}

#[derive(Clone)]
pub struct ProcessArtifact {
    code: Arc<PreparedPackage>,
    executable: PathBuf,
    prefix_args: Vec<String>,
}
impl ProcessArtifact {
    pub fn prepare(reference: &ArtifactRef, options: &ArtifactOptions) -> Result<Self> {
        let code = PreparedPackage::prepare(reference, &options.package)
            .map_err(|e| invalid(e.to_string()))?;
        let (executable, prefix_args) = match &options.launch {
            Launch::Executable => {
                if code.entry_kind() != FileKind::Executable {
                    return Err(invalid("process entry must be a native executable"));
                }
                (code.entry(), Vec::new())
            }
            Launch::Interpreter { executable, args } => {
                if !executable.is_absolute()
                    || !executable.is_file()
                    || args.iter().any(|arg| arg.contains('\0'))
                {
                    return Err(invalid(
                        "interpreter must be an absolute host-controlled file with valid arguments",
                    ));
                }
                if !code
                    .manifest()
                    .host_dependencies
                    .contains_key("interpreter")
                {
                    return Err(invalid(
                        "script package must declare its host interpreter identity",
                    ));
                }
                if code.entry_kind() != FileKind::Data {
                    return Err(invalid("interpreter entry must be a data file"));
                }
                let mut args = args.clone();
                args.push(
                    code.entry()
                        .to_str()
                        .ok_or_else(|| invalid("staging path must be UTF-8"))?
                        .into(),
                );
                (std::fs::canonicalize(executable)?, args)
            }
        };
        Ok(Self {
            code: Arc::new(code),
            executable,
            prefix_args,
        })
    }
    fn bind(&self, mut spec: ProcessSpec) -> Result<ProcessSpec> {
        // Validate host-selected startup settings without executing foreign code.
        if spec.timeout.is_zero()
            || std::time::Instant::now()
                .checked_add(spec.timeout)
                .is_none()
            || spec.task_stop_timeout.is_zero()
            || std::time::Instant::now()
                .checked_add(spec.task_stop_timeout)
                .is_none()
            || spec.args.iter().any(|arg| arg.contains('\0'))
        {
            return Err(invalid("invalid process arguments or timeouts"));
        }
        crate::dependency::Router::new(&spec)?;
        spec.executable = self.executable.clone();
        spec.args.splice(0..0, self.prefix_args.iter().cloned());
        Ok(spec)
    }
    /// `spec.executable` is replaced by the prepared launch. Its arguments follow
    /// the script entry, if any; endpoint, codec and declared bindings are kept.
    pub fn plugin(
        &self,
        name: impl Into<String>,
        service: &Service<RemoteService>,
        spec: ProcessSpec,
    ) -> Result<Plugin> {
        crate::dependency::Router::new(&spec)?.validate_primary(service.contract())?;
        Ok(self
            .bind(spec)?
            .pinned_plugin(name, service, Some(self.code.clone())))
    }
    pub fn broker_plugin<F>(
        &self,
        name: impl Into<String>,
        broker: &Service<Broker<RemoteService>>,
        spec: ProcessSpec,
        registered: F,
    ) -> Result<Plugin>
    where
        F: Fn(BrokerRegistration<RemoteService>) -> cordis_runtime::Result<()>
            + Send
            + Sync
            + 'static,
    {
        crate::dependency::Router::new(&spec)?.validate_primary(broker.contract())?;
        Ok(self
            .bind(spec)?
            .pinned_broker_plugin(name, broker, Some(self.code.clone()), registered))
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::InvalidInput(message.into())
}
