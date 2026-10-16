use crate::{Context, Message, Result, Value};
use std::collections::BTreeMap;
use std::{future::Future, pin::Pin, rc::Rc};

pub type LocalFuture<T> = Pin<Box<dyn Future<Output = Result<T>>>>;
pub type FutureResult = LocalFuture<Value>;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Contract {
    pub name: String,
    pub revision: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PluginManifest {
    pub provides: Contract,
    pub requires: Vec<Contract>,
}

/// Optional named capabilities requested by a plugin. Adapters compare these
/// declarations with host-selected bindings; aliases never grant authority.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bindings {
    pub events: BTreeMap<String, Contract>,
    pub middleware: BTreeMap<String, Contract>,
    pub factories: Vec<String>,
    pub provisions: BTreeMap<String, Contract>,
}

/// Business entry points. No generated WIT types occur in this contract.
pub trait Plugin: 'static {
    /// Generated dispatch decodes and checks typed inputs using Context policy.
    /// Raw implementations leave this false so adapters validate opaque input.
    const TYPED_DISPATCH: bool = false;
    fn manifest() -> PluginManifest;
    /// Explicit portable call surface. Foreign hosts require and validate it.
    fn interface() -> cordis_contract::Interface;
    /// One explicit interface for every declared dependency.
    fn dependency_interfaces() -> BTreeMap<String, cordis_contract::Interface> {
        BTreeMap::new()
    }
    fn bindings() -> Bindings {
        Bindings::default()
    }
    fn create(config: Value) -> Result<Self>
    where
        Self: Sized;
    fn activate(self: Rc<Self>, _ctx: Context) -> LocalFuture<()> {
        Box::pin(async { Ok(()) })
    }
    fn deactivate(self: Rc<Self>, _ctx: Context) -> LocalFuture<()> {
        Box::pin(async { Ok(()) })
    }
    fn invoke(
        self: Rc<Self>,
        ctx: Context,
        method: String,
        args: Message,
    ) -> crate::message::FutureResult;
    /// Return an owned object only once acquisition succeeds. Rejected means
    /// no cleanup responsibility remains; Unknown must be retained by the host.
    fn acquire(
        self: Rc<Self>,
        _ctx: Context,
        _kind: String,
        _args: Message,
    ) -> LocalFuture<Rc<dyn crate::ResourceObject>> {
        Box::pin(async {
            Err(crate::refused(
                "unsupported_resource",
                "plugin has no resource factory",
            ))
        })
    }
}
