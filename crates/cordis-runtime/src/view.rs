use crate::{Error, Result, Service};
use cordis_contract::{ContractId, RealmId, Slot};
use cordis_kernel::FiberId;
use std::{
    any::Any,
    collections::BTreeMap,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

static NEXT_REALM: AtomicU64 = AtomicU64::new(1);
type Metadata = Arc<dyn Any + Send + Sync>;

pub(crate) type RealmMap = BTreeMap<ContractId, RealmId>;
pub(crate) type RealmValues = BTreeMap<FiberId, RealmMap>;

/// One publication point for every managed view in a Runtime. Snapshots retain
/// values, never a mutable alias to this table. Only the coordinator writes it.
#[derive(Clone, Default)]
pub(crate) struct RealmDomain(Arc<RwLock<Arc<RealmValues>>>);
impl RealmDomain {
    pub(crate) fn snapshot(&self) -> Arc<RealmValues> {
        self.0.read().unwrap().clone()
    }
    pub(crate) fn publish(&self, values: RealmValues) {
        *self.0.write().unwrap() = Arc::new(values);
    }
    fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Clone)]
struct RealmSource {
    domain: RealmDomain,
    fiber: FiberId,
}

/// A complete, typed use-configuration revision. No dependency slots are mutable.
#[derive(Clone, Default)]
pub struct InterceptConfig {
    revision: u64,
    values: BTreeMap<ContractId, Metadata>,
}
impl InterceptConfig {
    pub fn new(revision: u64) -> Self {
        Self {
            revision,
            ..Self::default()
        }
    }
    pub fn set<T: ?Sized, M: Any + Send + Sync>(&mut self, service: &Service<T>, value: M) {
        self.values
            .insert(service.contract().clone(), Arc::new(value));
    }
    pub fn with_revision(mut self, revision: u64) -> Self {
        self.revision = revision;
        self
    }
}

/// Host-owned publication cell. Each admitted call retains one immutable revision.
#[derive(Clone, Default)]
pub struct Intercepts(Arc<RwLock<Arc<InterceptConfig>>>);
impl Intercepts {
    pub fn new(config: InterceptConfig) -> Self {
        Self(Arc::new(RwLock::new(Arc::new(config))))
    }
    pub fn replace(&self, config: InterceptConfig) {
        let old = std::mem::replace(&mut *self.0.write().unwrap(), Arc::new(config));
        drop(old); // typed user values are dropped outside the publication lock
    }
    fn snapshot(&self) -> Arc<InterceptConfig> {
        self.0.read().unwrap().clone()
    }
}

#[derive(Clone)]
enum InterceptView {
    Live(Intercepts),
    Fixed(Arc<InterceptConfig>),
}
impl InterceptView {
    fn snapshot(&self) -> Arc<InterceptConfig> {
        match self {
            Self::Live(source) => source.snapshot(),
            Self::Fixed(config) => config.clone(),
        }
    }
}

/// Derived views share resource ownership; isolation only redirects the selected service key.
#[derive(Clone, Default)]
pub struct View {
    realms: BTreeMap<ContractId, RealmId>,
    source: Option<RealmSource>,
    metadata: BTreeMap<ContractId, Vec<Metadata>>,
    intercepts: Option<InterceptView>,
}

impl View {
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach host-managed configuration underneath explicitly derived layers.
    pub fn with_intercepts(mut self, source: Intercepts) -> Self {
        self.intercepts = Some(InterceptView::Live(source));
        self
    }

    pub fn snapshot(&self) -> Self {
        Self {
            realms: self.realm_map(),
            source: None,
            intercepts: self
                .intercepts
                .as_ref()
                .map(|source| InterceptView::Fixed(source.snapshot())),
            ..self.clone()
        }
    }

    pub fn metadata_revision(&self) -> Option<u64> {
        self.intercepts
            .as_ref()
            .map(|source| source.snapshot().revision)
    }

    pub fn isolate<T: ?Sized>(self, service: &Service<T>) -> Self {
        self.in_realm(service, RealmId(NEXT_REALM.fetch_add(1, Ordering::Relaxed)))
    }

    pub fn in_realm<T: ?Sized>(mut self, service: &Service<T>, realm: RealmId) -> Self {
        self.realms.insert(service.contract().clone(), realm);
        self
    }

    pub fn realm<T: ?Sized>(&self, service: &Service<T>) -> RealmId {
        self.slot(service.contract()).realm
    }

    /// Layers are returned oldest first. The service owns its merge semantics.
    pub fn intercept<T: ?Sized, M: Any + Send + Sync>(
        mut self,
        service: &Service<T>,
        metadata: M,
    ) -> Self {
        self.metadata
            .entry(service.contract().clone())
            .or_default()
            .push(Arc::new(metadata));
        self
    }

    pub fn metadata<T: ?Sized, M: Any + Send + Sync>(
        &self,
        service: &Service<T>,
    ) -> Result<Vec<Arc<M>>> {
        let config = self.intercepts.as_ref().map(InterceptView::snapshot);
        config
            .as_ref()
            .and_then(|config| config.values.get(service.contract()))
            .into_iter()
            .chain(self.metadata.get(service.contract()).into_iter().flatten())
            .map(|m| {
                m.clone()
                    .downcast::<M>()
                    .map_err(|_| Error::TypeMismatch(service.contract().clone()))
            })
            .collect()
    }

    pub(crate) fn slot(&self, contract: &ContractId) -> Slot {
        let realm = self.realms.get(contract).copied().or_else(|| {
            let source = self.source.as_ref()?;
            source
                .domain
                .snapshot()
                .get(&source.fiber)?
                .get(contract)
                .copied()
        });
        Slot::new(contract.clone(), realm.unwrap_or(RealmId::ROOT))
    }

    pub(crate) fn realm_map(&self) -> RealmMap {
        let mut values = self.source.as_ref().map_or_else(RealmMap::new, |source| {
            source.domain.snapshot()[&source.fiber].clone()
        });
        values.extend(self.realms.clone());
        values
    }

    pub(crate) fn resolve_realms(&self, domain: &RealmDomain, values: &RealmValues) -> RealmMap {
        let mut result = match &self.source {
            Some(source) if source.domain.same(domain) => values[&source.fiber].clone(),
            _ => self.realm_map(),
        };
        result.extend(self.realms.clone());
        result
    }

    pub(crate) fn with_realms(mut self, realms: RealmMap) -> Self {
        self.realms = realms;
        self.source = None;
        self
    }

    pub(crate) fn localize(self, domain: &RealmDomain) -> Self {
        if self
            .source
            .as_ref()
            .is_some_and(|source| !source.domain.same(domain))
        {
            let realms = self.realm_map();
            self.with_realms(realms)
        } else {
            self
        }
    }

    pub(crate) fn managed(mut self, domain: RealmDomain, fiber: FiberId) -> Self {
        self.realms.clear();
        self.source = Some(RealmSource { domain, fiber });
        self
    }
}
