//! Process-specific launch description over the common Session plugin binding.
use crate::{RemoteService, SessionSpec, plugin::CodeLease};
use cordis_runtime::{Broker, BrokerRegistration, Plugin, Service};
use std::{path::PathBuf, sync::Arc};

/// Process transport declaration. All protocol and lifecycle configuration lives
/// in `session`, so adding a connected browser never invents an executable path.
#[derive(Clone, Debug)]
pub struct ProcessSpec {
    pub executable: PathBuf,
    pub args: Vec<String>,
    pub session: SessionSpec,
}
impl ProcessSpec {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
            args: Vec::new(),
            session: SessionSpec::default(),
        }
    }
    pub fn requires(mut self, dependency: cordis_host::Dependency) -> Self {
        self.session = self.session.requires(dependency);
        self
    }
    pub fn event(mut self, binding: cordis_host::EventBinding) -> Self {
        self.session = self.session.event(binding);
        self
    }
    pub fn middleware(mut self, binding: cordis_host::MiddlewareBinding) -> Self {
        self.session = self.session.middleware(binding);
        self
    }
    pub fn factory(mut self, binding: cordis_host::FactoryBinding) -> Self {
        self.session = self.session.factory(binding);
        self
    }
    pub fn provision(mut self, binding: cordis_host::ProvisionBinding) -> Self {
        self.session = self.session.provision(binding);
        self
    }
    pub fn plugin(self, name: impl Into<String>, service: &Service<RemoteService>) -> Plugin {
        self.pinned_plugin(name, service, None)
    }
    pub(crate) fn pinned_plugin(
        self,
        name: impl Into<String>,
        service: &Service<RemoteService>,
        code: CodeLease,
    ) -> Plugin {
        let connector = Arc::new(crate::transport::ProcessConnector {
            executable: self.executable,
            args: self.args,
        });
        self.session
            .connected_plugin(name, service, connector, code)
    }
    pub fn broker_plugin<F>(
        self,
        name: impl Into<String>,
        broker: &Service<Broker<RemoteService>>,
        registered: F,
    ) -> Plugin
    where
        F: Fn(BrokerRegistration<RemoteService>) -> cordis_runtime::Result<()>
            + Send
            + Sync
            + 'static,
    {
        self.pinned_broker_plugin(name, broker, None, registered)
    }
    pub(crate) fn pinned_broker_plugin<F>(
        self,
        name: impl Into<String>,
        broker: &Service<Broker<RemoteService>>,
        code: CodeLease,
        registered: F,
    ) -> Plugin
    where
        F: Fn(BrokerRegistration<RemoteService>) -> cordis_runtime::Result<()>
            + Send
            + Sync
            + 'static,
    {
        let connector = Arc::new(crate::transport::ProcessConnector {
            executable: self.executable,
            args: self.args,
        });
        self.session
            .connected_broker_plugin(name, broker, connector, code, registered)
    }
}
