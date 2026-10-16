//! Session-bound capabilities over native child ownership and staged provision.
use crate::{
    Bytes, Error, Result, Value, json, resource::runtime_error, scope::text, session::WeakSession,
};
use cordis_contract::ContractId;
use cordis_runtime::{Context, Plugin, Service, WeakPluginHandle};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, Mutex},
};

type Factory = dyn Fn(Bytes) -> cordis_runtime::Result<Plugin> + Send + Sync;
/// Constructs a definition only. Acquire resources in the plugin initializer.
#[derive(Clone)]
pub struct FactoryBinding {
    alias: String,
    factory: Arc<Factory>,
}
impl FactoryBinding {
    pub fn new<F>(alias: impl Into<String>, factory: F) -> Self
    where
        F: Fn(Bytes) -> cordis_runtime::Result<Plugin> + Send + Sync + 'static,
    {
        Self {
            alias: alias.into(),
            factory: Arc::new(factory),
        }
    }
}
impl fmt::Debug for FactoryBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("FactoryBinding").field(&self.alias).finish()
    }
}
#[derive(Clone)]
pub struct ProvisionBinding {
    alias: String,
    pub(crate) service: Service<Bytes>,
}
impl ProvisionBinding {
    pub fn new(alias: impl Into<String>, service: &Service<Bytes>) -> Self {
        Self {
            alias: alias.into(),
            service: service.clone(),
        }
    }
}
impl fmt::Debug for ProvisionBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ProvisionBinding")
            .field(&self.alias)
            .field(self.service.contract())
            .finish()
    }
}
#[derive(Clone, Debug)]
pub struct CompositionLimits {
    pub children: usize,
}
impl Default for CompositionLimits {
    fn default() -> Self {
        Self { children: 1024 }
    }
}

pub(crate) struct Composition {
    factories: BTreeMap<String, Arc<Factory>>,
    provisions: BTreeMap<String, Service<Bytes>>,
    limits: CompositionLimits,
    entries: Mutex<BTreeMap<String, Option<Entry>>>,
}
struct Entry {
    grant: String,
    child: WeakPluginHandle,
}
impl Composition {
    pub fn new(
        factories: &[FactoryBinding],
        provisions: &[ProvisionBinding],
        limits: CompositionLimits,
    ) -> Result<Arc<Self>> {
        let mut bound_factories = BTreeMap::new();
        let mut bound_provisions = BTreeMap::new();
        let mut slots = BTreeSet::new();
        let valid = |s: &str| !s.is_empty() && s.len() <= 1024;
        for f in factories {
            if !valid(&f.alias)
                || bound_factories
                    .insert(f.alias.clone(), f.factory.clone())
                    .is_some()
            {
                return Err(Error::InvalidInput(
                    "invalid or duplicate factory alias".into(),
                ));
            }
        }
        for p in provisions {
            if !valid(&p.alias)
                || p.service.contract().name.is_empty()
                || p.service.contract().revision.is_empty()
                || !slots.insert(p.service.contract().clone())
                || bound_provisions
                    .insert(p.alias.clone(), p.service.clone())
                    .is_some()
            {
                return Err(Error::InvalidInput("invalid or duplicate provision".into()));
            }
        }
        Ok(Arc::new(Self {
            factories: bound_factories,
            provisions: bound_provisions,
            limits,
            entries: Mutex::default(),
        }))
    }
    pub fn validate_primary(&self, primary: &ContractId) -> Result<()> {
        if self.provisions.values().any(|s| s.contract() == primary) {
            return Err(Error::InvalidInput(
                "provision collides with primary contract".into(),
            ));
        }
        Ok(())
    }
    pub fn declarations(&self) -> Value {
        let provisions: BTreeMap<_,_>=self.provisions.iter().map(|(alias,s)|(alias,json!({"name":s.contract().name.as_ref(),"revision":s.contract().revision.as_ref()}))).collect();
        json!({"factories":self.factories.keys().collect::<Vec<_>>(),"provisions":provisions})
    }
    pub async fn install(
        self: &Arc<Self>,
        ctx: Context,
        payload: Value,
        config: Bytes,
    ) -> Result<Value> {
        let alias = text(&payload, "factory")?;
        let factory = self
            .factories
            .get(&alias)
            .ok_or_else(|| Error::Refused {
                code: "undeclared_factory".into(),
                message: alias,
            })?
            .clone();
        let id = token()?;
        let grant = token()?;
        {
            let mut entries = self.entries.lock().unwrap();
            if entries.len() >= self.limits.children {
                return Err(Error::Overloaded);
            }
            if entries.contains_key(&id) {
                return Err(Error::Overloaded);
            }
            entries.insert(id.clone(), None);
        }
        let _reservation = Reservation {
            registry: self.clone(),
            id: id.clone(),
        };
        // install_with checks Broker/cleanup/owner authority BEFORE factory code.
        let child = tokio::task::spawn_blocking(move || ctx.install_with(|| factory(config)))
            .await
            .map_err(|e| Error::Resource(format!("factory failed: {e}")))?
            .map_err(runtime_error)?;
        self.entries.lock().unwrap().insert(
            id.clone(),
            Some(Entry {
                grant: grant.clone(),
                child: child.downgrade(),
            }),
        );
        Ok(json!({"child":id,"grant":grant}))
    }
    pub fn provide(&self, ctx: &Context, payload: &Value, value: Bytes) -> Result<Value> {
        let alias = text(payload, "service")?;
        let service = self.provisions.get(&alias).ok_or_else(|| Error::Refused {
            code: "undeclared_provision".into(),
            message: alias,
        })?;
        ctx.provide(service, Arc::new(value))
            .map_err(runtime_error)?;
        Ok(Value::Null)
    }
    pub async fn control(
        &self,
        command: &str,
        payload: &Value,
        session: WeakSession,
    ) -> Result<Value> {
        let id = text(payload, "child")?;
        let grant = text(payload, "grant")?;
        let child = {
            let entries = self.entries.lock().unwrap();
            let entry = entries
                .get(&id)
                .and_then(Option::as_ref)
                .ok_or(Error::Closed)?;
            if entry.grant != grant {
                return Err(Error::Closed);
            }
            entry.child.upgrade().ok_or(Error::Closed)?
        };
        let session = session.upgrade().ok_or(Error::Closed)?;
        let operation = async {
            let snapshot = match command {
                "child_status" => child.snapshot(),
                "child_ready" => child.ready().await.map_err(runtime_error)?,
                "child_settled" => child.settled().await.map_err(runtime_error)?,
                "child_dispose" => {
                    child.dispose().await.map_err(runtime_error)?;
                    return Ok(Value::Null);
                }
                _ => return Err(Error::InvalidInput("unknown child operation".into())),
            };
            Ok(
                json!({"state":format!("{:?}",snapshot.state).to_lowercase(),"retired":snapshot.retired,"outcome":snapshot.outcome.map(|o|format!("{o:?}"))}),
            )
        };
        // A dead peer cannot consume a reply. Stop waiting (not ownership or
        // native retirement) so initialization failure can drain its children.
        tokio::select! {
            result=operation=>result,
            _=session.peer_stopped()=>Err(Error::Closed),
        }
    }
}
struct Reservation {
    registry: Arc<Composition>,
    id: String,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        let mut entries = self.registry.entries.lock().unwrap();
        if matches!(entries.get(&self.id), Some(None)) {
            entries.remove(&self.id);
        }
    }
}
fn token() -> Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| Error::Io(e.to_string()))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}
