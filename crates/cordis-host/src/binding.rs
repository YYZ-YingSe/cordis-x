use crate::{Body, Dependency};
use cordis_runtime::{Event, Middleware, Plugin, Service};
use std::sync::Arc;

pub type Factory = dyn Fn(Body) -> cordis_runtime::Result<Plugin> + Send + Sync;
#[derive(Clone)]
pub struct EventBinding {
    pub alias: String,
    pub event: Event<Body, Body>,
}
impl EventBinding {
    pub fn new(alias: impl Into<String>, event: &Event<Body, Body>) -> Self {
        Self {
            alias: alias.into(),
            event: event.clone(),
        }
    }
}
#[derive(Clone)]
pub struct MiddlewareBinding {
    pub alias: String,
    pub middleware: Middleware<Body, Body>,
}
impl MiddlewareBinding {
    pub fn new(alias: impl Into<String>, middleware: &Middleware<Body, Body>) -> Self {
        Self {
            alias: alias.into(),
            middleware: middleware.clone(),
        }
    }
}
#[derive(Clone)]
pub struct FactoryBinding {
    pub alias: String,
    pub factory: Arc<Factory>,
}
impl FactoryBinding {
    pub fn new<F>(alias: impl Into<String>, factory: F) -> Self
    where
        F: Fn(Body) -> cordis_runtime::Result<Plugin> + Send + Sync + 'static,
    {
        Self {
            alias: alias.into(),
            factory: Arc::new(factory),
        }
    }
}
impl std::fmt::Debug for FactoryBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("FactoryBinding").field(&self.alias).finish()
    }
}
#[derive(Clone)]
pub struct ProvisionBinding {
    pub alias: String,
    pub service: Service<Body>,
}
impl ProvisionBinding {
    pub fn new(alias: impl Into<String>, service: &Service<Body>) -> Self {
        Self {
            alias: alias.into(),
            service: service.clone(),
        }
    }
}
/// Host authority and semantic channels, independent of execution options.
#[derive(Clone, Debug, Default)]
pub struct HostBindings {
    pub validation: cordis_contract::Validation,
    pub dependencies: Vec<Dependency>,
    pub events: Vec<EventBinding>,
    pub middleware: Vec<MiddlewareBinding>,
    pub factories: Vec<FactoryBinding>,
    pub provisions: Vec<ProvisionBinding>,
}
impl HostBindings {
    pub fn event(&self, alias: &str) -> Option<&Event<Body, Body>> {
        self.events
            .iter()
            .find(|b| b.alias == alias)
            .map(|b| &b.event)
    }
    pub fn middleware(&self, alias: &str) -> Option<&Middleware<Body, Body>> {
        self.middleware
            .iter()
            .find(|b| b.alias == alias)
            .map(|b| &b.middleware)
    }
    pub fn factory(&self, alias: &str) -> Option<&Arc<Factory>> {
        self.factories
            .iter()
            .find(|b| b.alias == alias)
            .map(|b| &b.factory)
    }
    pub fn provision(&self, alias: &str) -> Option<&Service<Body>> {
        self.provisions
            .iter()
            .find(|b| b.alias == alias)
            .map(|b| &b.service)
    }
}

impl std::fmt::Debug for EventBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("EventBinding")
            .field(&self.alias)
            .field(self.event.contract())
            .finish()
    }
}

impl std::fmt::Debug for MiddlewareBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("MiddlewareBinding")
            .field(&self.alias)
            .field(self.middleware.contract())
            .finish()
    }
}

impl std::fmt::Debug for ProvisionBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ProvisionBinding")
            .field(&self.alias)
            .field(self.service.contract())
            .finish()
    }
}

impl HostBindings {
    /// Validate host selections once during preparation, before any instance is
    /// activated. This does not replace runtime authority or message admission.
    pub fn validate(&self) -> crate::Result<()> {
        use std::collections::BTreeSet;
        let aliases = |kind: &str, names: Vec<&str>| {
            let mut seen = BTreeSet::new();
            for name in names {
                if name.is_empty() || name.len() > 1024 || !seen.insert(name) {
                    return Err(crate::refused(
                        "binding_mismatch",
                        format!("{kind}: invalid or duplicate alias {name:?}"),
                    ));
                }
            }
            Ok(())
        };
        aliases(
            "dependencies",
            self.dependencies
                .iter()
                .map(|b| b.contract().name.as_ref())
                .collect(),
        )?;
        aliases(
            "events",
            self.events.iter().map(|b| b.alias.as_str()).collect(),
        )?;
        aliases(
            "middleware",
            self.middleware.iter().map(|b| b.alias.as_str()).collect(),
        )?;
        aliases(
            "factories",
            self.factories.iter().map(|b| b.alias.as_str()).collect(),
        )?;
        aliases(
            "provisions",
            self.provisions.iter().map(|b| b.alias.as_str()).collect(),
        )?;
        let mut slots = BTreeSet::new();
        for binding in &self.provisions {
            if !slots.insert(binding.service.contract()) {
                return Err(crate::refused(
                    "binding_mismatch",
                    format!(
                        "provisions.{}: duplicate service {}",
                        binding.alias,
                        binding.service.contract()
                    ),
                ));
            }
        }
        // Named resources are concrete requirements. A dynamic dispatcher
        // describes possible requests and can refuse individual kinds at runtime.
        for binding in &self.dependencies {
            if !binding.supports_resources()
                && matches!(&binding.descriptor().interface().resources, cordis_contract::Dispatch::Named(resources) if !resources.is_empty())
            {
                return Err(crate::refused(
                    "unsupported_resource",
                    format!(
                        "dependencies.{}: selected endpoint has no resource adapter",
                        binding.contract()
                    ),
                ));
            }
        }
        Ok(())
    }
}
