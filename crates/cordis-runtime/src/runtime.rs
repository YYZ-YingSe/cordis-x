use crate::{
    BoxFuture, Context, Error, Result, View,
    effects::{Effect, EffectSnapshot},
    gate::{Gate, Lease},
    protected,
};
use cordis_contract::ContractId;
use cordis_kernel::{
    Action, Activation, BindingView, Event, FiberId, FiberSpec, Kernel, Lifecycle, Outcome,
    Snapshot,
};
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::{runtime::Handle, sync::watch};

static NEXT_RUNTIME: AtomicU64 = AtomicU64::new(1);
type Start = dyn Fn(Context) -> BoxFuture<'static, Result<()>> + Send + Sync;

mod ownership;
mod realms;
pub use ownership::OwnershipChange;
pub use realms::{RealmChange, RealmReport};

#[derive(Clone)]
pub struct Plugin {
    pub(crate) name: String,
    pub(crate) requires: BTreeSet<ContractId>,
    pub(crate) provides: BTreeSet<ContractId>,
    start: Arc<Start>,
}

impl Plugin {
    pub fn new<F, Fut>(name: impl Into<String>, start: F) -> Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        Self {
            name: name.into(),
            requires: BTreeSet::new(),
            provides: BTreeSet::new(),
            start: Arc::new(move |ctx| Box::pin(start(ctx))),
        }
    }

    pub fn requires<T: ?Sized>(mut self, service: &crate::Service<T>) -> Self {
        self.requires.insert(service.contract().clone());
        self
    }

    pub fn provides<T: ?Sized>(mut self, service: &crate::Service<T>) -> Self {
        self.provides.insert(service.contract().clone());
        self
    }
}

struct Installed {
    plugin: Plugin,
    origin: View,
    view: View,
    inheritance: Option<Inheritance>,
}

struct Inheritance {
    view: View,
    contracts: BTreeSet<ContractId>,
}

pub(crate) struct Binding {
    pub descriptor: Option<cordis_contract::Descriptor>,
    pub value: Arc<dyn Any + Send + Sync>,
    pub definition: Weak<ActivationData>,
    pub gate: Arc<Gate>,
}

pub(crate) struct ActivationData {
    pub activation: Activation,
    pub gate: Arc<Gate>,
    pub view: View,
    pub bindings: BTreeMap<ContractId, Arc<Binding>>,
    pub provision_view: View,
    pub effects: Mutex<Vec<Arc<Effect>>>,
    pub next_effect: AtomicU64,
    pins: Mutex<Vec<Lease>>,
}

pub(crate) struct Authority {
    alive: AtomicBool,
    pub strict: bool,
}
impl Authority {
    pub fn new(strict: bool) -> Arc<Self> {
        Arc::new(Self {
            alive: AtomicBool::new(true),
            strict,
        })
    }
    pub fn valid(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }
    pub fn expire(&self) {
        self.alive.store(false, Ordering::Release);
    }
}

pub(crate) struct State {
    pub kernel: Kernel,
    installed: BTreeMap<FiberId, Installed>,
    pub activations: BTreeMap<Activation, Arc<ActivationData>>,
    pub services: BTreeMap<(Activation, ContractId), Arc<Binding>>,
    realms: crate::view::RealmDomain,
    pub stopping: bool,
}

pub(crate) struct Inner {
    pub state: Mutex<State>,
    pub executor: Handle,
    pub changed: watch::Sender<u64>,
    pub events: Mutex<crate::events::Registry>,
    pub retained: Mutex<Vec<Weak<crate::retention::Pin>>>,
    diagnostics: Mutex<VecDeque<Error>>,
}

#[derive(Clone)]
pub struct Runtime {
    pub(crate) inner: Arc<Inner>,
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

impl Runtime {
    /// Construct inside a Tokio runtime. Always await `shutdown()` before stopping the executor.
    pub fn new() -> Self {
        let (changed, _) = watch::channel(0);
        Self {
            inner: Arc::new(Inner {
                executor: Handle::current(),
                changed,
                events: Mutex::default(),
                retained: Mutex::default(),
                diagnostics: Mutex::default(),
                state: Mutex::new(State {
                    kernel: Kernel::new(NEXT_RUNTIME.fetch_add(1, Ordering::Relaxed)),
                    installed: BTreeMap::new(),
                    activations: BTreeMap::new(),
                    services: BTreeMap::new(),
                    realms: crate::view::RealmDomain::default(),
                    stopping: false,
                }),
            }),
        }
    }

    pub fn install(&self, plugin: Plugin) -> Result<PluginHandle> {
        self.install_in(View::new(), plugin)
    }

    pub fn install_in(&self, view: View, plugin: Plugin) -> Result<PluginHandle> {
        self.inner.install(view, plugin, None, None)
    }

    pub fn snapshots(&self) -> Vec<Snapshot> {
        self.inner.state.lock().unwrap().kernel.snapshots()
    }

    /// Subscribe before reading snapshots to avoid missing a concurrent change.
    /// Values are wake-up counters, not a lossless event log or configuration revisions.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.inner.changed.subscribe()
    }

    /// Validate a proposed replacement against all current declarations, including
    /// plugins installed outside the loader. This executes no plugin callbacks and
    /// reserves nothing; ordinary installation checks still run when applying it.
    pub fn validate_replacement(
        &self,
        retiring: &[PluginHandle],
        additions: &[(View, Plugin)],
    ) -> Result<()> {
        self.validate_reconfiguration(retiring, additions, &[], &[])
            .map(|_| ())
    }

    /// Most recent 256 asynchronous dispatch failures, including dispatches whose wait was dropped.
    pub fn diagnostics(&self) -> Vec<Error> {
        self.inner
            .diagnostics
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect()
    }

    pub async fn shutdown(&self) -> Result<()> {
        let mut changed = self.inner.changed.subscribe();
        {
            let mut state = self.inner.state.lock().unwrap();
            state.stopping = true;
            let ids: Vec<_> = state.kernel.snapshots().into_iter().map(|s| s.id).collect();
            for id in ids {
                let actions = state.kernel.step(Event::Retire(id))?;
                self.inner.execute_locked(&mut state, actions);
            }
        }
        self.inner.notify();
        loop {
            let snapshots = self.snapshots();
            let failures: Vec<_> = snapshots
                .iter()
                .filter_map(|s| match &s.outcome {
                    Some(Outcome::CleanupFailed(message)) => {
                        Some(Error::CleanupFailed(s.id, message.clone()))
                    }
                    _ => None,
                })
                .collect();
            if !failures.is_empty() {
                return Err(Error::Aggregate(failures));
            }
            if snapshots.iter().all(|s| s.state == Lifecycle::Inactive) {
                return Ok(());
            }
            changed.changed().await.map_err(|_| Error::RuntimeStopped)?;
        }
    }
}

impl Inner {
    pub fn record(&self, error: Error) {
        let mut diagnostics = self.diagnostics.lock().unwrap();
        if diagnostics.len() == 256 {
            diagnostics.pop_front();
        }
        diagnostics.push_back(error);
    }
    pub fn notify(&self) {
        self.changed.send_modify(|revision| *revision += 1);
    }

    pub fn install(
        self: &Arc<Self>,
        view: View,
        plugin: Plugin,
        parent: Option<Activation>,
        waiter: Option<Activation>,
    ) -> Result<PluginHandle> {
        let mut state = self.state.lock().unwrap();
        if state.stopping {
            return Err(Error::RuntimeStopped);
        }
        let view = view.localize(&state.realms);
        let mut requires: BTreeSet<_> = plugin.requires.iter().map(|c| view.slot(c)).collect();
        let mut inheritance = None;
        if let Some(definition) = waiter {
            let source = state.activations.get(&definition).ok_or(Error::Closed)?;
            if source.gate.phase() == crate::gate::Phase::Closed {
                return Err(Error::Closed);
            }
            // Project the installing definition's committed view into this child.
            // Each hop filters by realm; an explicit declaration shadows inheritance.
            // Making these real graph edges retains providers even when the child
            // is owned by the caller of a service rather than by its definition.
            for contract in source.bindings.keys() {
                let slot = source.view.slot(contract);
                if !plugin.requires.contains(contract) && view.slot(contract) == slot {
                    requires.insert(slot);
                }
            }
            inheritance = Some(Inheritance {
                view: source.view.clone(),
                contracts: source.bindings.keys().cloned().collect(),
            });
        }
        let spec = FiberSpec {
            name: plugin.name.clone(),
            parent,
            requires,
            provides: plugin.provides.iter().map(|c| view.slot(c)).collect(),
        };
        let (id, actions) = state.kernel.install(spec)?;
        let mut values = (*state.realms.snapshot()).clone();
        values.insert(id, view.realm_map());
        state.realms.publish(values);
        let managed = view.clone().managed(state.realms.clone(), id);
        state.installed.insert(
            id,
            Installed {
                plugin,
                origin: view,
                view: managed,
                inheritance,
            },
        );
        self.execute_locked(&mut state, actions);
        drop(state);
        self.notify();
        Ok(PluginHandle {
            id,
            inner: self.clone(),
            waiter,
        })
    }

    pub fn event(self: &Arc<Self>, event: Event) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let actions = state.kernel.step(event)?;
        self.execute_locked(&mut state, actions);
        drop(state);
        self.notify();
        Ok(())
    }

    // Only bookkeeping and spawn happen under the state lock; spawn never polls synchronously.
    fn execute_locked(self: &Arc<Self>, state: &mut State, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Start {
                    activation,
                    bindings,
                } => self.start_locked(state, activation, bindings),
                Action::Publish(a) => state.activations[&a].gate.activate(),
                Action::Withdraw(a) => {
                    let data = state.activations[&a].clone();
                    data.gate.close();
                    let inner = self.clone();
                    self.executor.spawn(async move {
                        data.gate.drained().await;
                        inner
                            .event(Event::Drained(a))
                            .expect("drain event for live activation");
                    });
                }
                Action::Recover(a) => {
                    let data = state.activations[&a].clone();
                    data.gate.recover();
                    let inner = self.clone();
                    self.executor.spawn(async move {
                        let result = loop {
                            let effect = data.effects.lock().unwrap().last().cloned();
                            let Some(effect) = effect else {
                                break Ok(());
                            };
                            if let Err(error) = effect.run().await {
                                break Err(error.to_string());
                            }
                            data.gate.drained().await;
                            let released = data.effects.lock().unwrap().pop();
                            drop(released);
                        };
                        inner
                            .event(Event::Recovered {
                                activation: a,
                                result,
                            })
                            .expect("recovery event for live activation");
                    });
                }
                Action::Release(a) => {
                    let data = state.activations.remove(&a).unwrap();
                    data.gate.finish();
                    // Drop resources and dependency pins only after all inverses have completed.
                    let keys: Vec<_> = state
                        .services
                        .keys()
                        .filter(|(activation, _)| *activation == a)
                        .cloned()
                        .collect();
                    let removed: Vec<_> = keys
                        .into_iter()
                        .filter_map(|key| state.services.remove(&key))
                        .collect();
                    data.pins.lock().unwrap().clear();
                    // Service destructors are user code too; never execute them under the coordinator lock.
                    self.executor.spawn(async move {
                        drop(removed);
                        drop(data);
                    });
                }
            }
        }
        // Retired tombstones retain diagnostics, not plugin captures. Also covers
        // retirement while waiting for a missing dependency (no Release action).
        let retired: Vec<_> = state
            .kernel
            .snapshots()
            .into_iter()
            .filter(|s| s.retired && s.state == Lifecycle::Inactive)
            .filter_map(|s| state.installed.remove(&s.id))
            .collect();
        if !retired.is_empty() {
            self.executor.spawn(async move {
                drop(retired);
            });
        }
    }

    fn start_locked(
        self: &Arc<Self>,
        state: &mut State,
        activation: Activation,
        bindings: BindingView,
    ) {
        let mut resolved = BTreeMap::new();
        let mut pins = Vec::new();
        for (slot, provider) in bindings {
            let binding = state.services[&(provider, slot.contract.clone())].clone();
            pins.push(
                binding
                    .gate
                    .enter(false, false)
                    .expect("committed provider is active"),
            );
            resolved.insert(slot.contract, binding);
        }
        let installed = &state.installed[&activation.fiber];
        let callback = installed.plugin.start.clone();
        let data = Arc::new(ActivationData {
            activation,
            gate: Gate::new(),
            view: installed.view.clone(),
            provision_view: View::new().with_realms(installed.view.realm_map()),
            bindings: resolved,
            effects: Mutex::new(Vec::new()),
            next_effect: AtomicU64::new(1),
            pins: Mutex::new(pins),
        });
        let setup_lease = data.gate.enter(true, false).unwrap();
        let authority = Authority::new(false);
        let context = Context::base(self, data.clone(), Some(authority.clone()));
        state.activations.insert(activation, data);
        let inner = self.clone();
        self.executor.spawn(async move {
            let result = protected(async move { callback(context).await })
                .await
                .map_err(|e| e.to_string());
            authority.expire();
            drop(setup_lease);
            inner
                .event(Event::Initialized { activation, result })
                .expect("initializer completion for live activation");
        });
    }
}

#[derive(Clone)]
pub struct PluginHandle {
    pub(crate) id: FiberId,
    pub(crate) inner: Arc<Inner>,
    pub(crate) waiter: Option<Activation>,
}

/// Adapter registries can refer to a child without retaining the Runtime.
#[derive(Clone)]
pub struct WeakPluginHandle {
    id: FiberId,
    inner: Weak<Inner>,
    waiter: Option<Activation>,
}
impl WeakPluginHandle {
    pub fn upgrade(&self) -> Option<PluginHandle> {
        Some(PluginHandle {
            id: self.id,
            inner: self.inner.upgrade()?,
            waiter: self.waiter,
        })
    }
}

impl PluginHandle {
    pub fn downgrade(&self) -> WeakPluginHandle {
        WeakPluginHandle {
            id: self.id,
            inner: Arc::downgrade(&self.inner),
            waiter: self.waiter,
        }
    }
    pub fn id(&self) -> FiberId {
        self.id
    }
    pub fn snapshot(&self) -> Snapshot {
        self.inner
            .state
            .lock()
            .unwrap()
            .kernel
            .snapshot(self.id)
            .unwrap()
    }

    pub fn effects(&self) -> Vec<EffectSnapshot> {
        let state = self.inner.state.lock().unwrap();
        let Some(a) = state.kernel.snapshot(self.id).unwrap().activation else {
            return Vec::new();
        };
        state
            .activations
            .get(&a)
            .map(|data| {
                data.effects
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|e| e.snapshot())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Wait for Active; missing dependencies remain pending. Dropping this wait does not stop initialization.
    pub async fn ready(&self) -> Result<Snapshot> {
        self.wait(true, false).await
    }

    /// Unlike `ready`, also returns when waiting for missing dependencies.
    pub async fn settled(&self) -> Result<Snapshot> {
        self.wait(false, false).await
    }

    pub async fn dispose(&self) -> Result<()> {
        self.inner.event(Event::Retire(self.id))?;
        self.wait(false, true).await.map(|_| ())
    }

    pub async fn restart(&self) -> Result<Snapshot> {
        self.inner.event(Event::Restart(self.id))?;
        self.settled().await
    }

    async fn wait(&self, active: bool, disposal: bool) -> Result<Snapshot> {
        let mut changed = self.inner.changed.subscribe();
        loop {
            let snapshot = {
                let state = self.inner.state.lock().unwrap();
                if active && let Some(waiter) = self.waiter {
                    let waiting = state.kernel.snapshot(waiter.fiber)?;
                    if waiting.activation == Some(waiter)
                        && waiting.state == Lifecycle::Loading
                        && state.kernel.depends_on(self.id, waiter.fiber)
                    {
                        return Err(Error::WouldDeadlock);
                    }
                }
                let snapshot = state.kernel.snapshot(self.id)?;
                if let Some(Outcome::CleanupFailed(message)) = &snapshot.outcome {
                    return Err(Error::CleanupFailed(self.id, message.clone()));
                }
                // Report a failed dependent instead of hanging forever behind its cleanup lease.
                if snapshot.state == Lifecycle::Unloading {
                    let retained = self.inner.retained.lock().unwrap();
                    let reachable = retention_waits(&state, &retained, self.id);
                    for pin in retained.iter().filter_map(Weak::upgrade) {
                        if pin.live()
                            && reachable.contains(&pin.provider.fiber)
                            && let Some(error) = pin.failure.lock().unwrap().clone()
                        {
                            return Err(Error::CleanupBlocked(pin.owner.fiber, error.to_string()));
                        }
                    }
                    for other in state.kernel.snapshots() {
                        if let Some(Outcome::CleanupFailed(message)) = other.outcome
                            && reachable.contains(&other.id)
                        {
                            return Err(Error::CleanupBlocked(other.id, message));
                        }
                    }
                }
                snapshot
            };
            if snapshot.state == Lifecycle::Inactive {
                if disposal {
                    return Ok(snapshot);
                }
                if let Some(Outcome::InitializationFailed(message)) = &snapshot.outcome {
                    return Err(Error::InitializationFailed(self.id, message.clone()));
                }
                if let Some(Outcome::ExecutionFailed(message)) = &snapshot.outcome {
                    return Err(Error::ExecutionFailed(self.id, message.clone()));
                }
                if snapshot.retired {
                    return Err(Error::Retired(self.id));
                }
                if !active {
                    return Ok(snapshot);
                }
            } else if snapshot.state == Lifecycle::Active && !disposal {
                return Ok(snapshot);
            }
            changed.changed().await.map_err(|_| Error::RuntimeStopped)?;
        }
    }
}

// Dynamic retention contributes drain barriers, never discovery/required edges.
pub(crate) fn retention_waits(
    state: &State,
    pins: &[Weak<crate::retention::Pin>],
    start: FiberId,
) -> BTreeSet<FiberId> {
    let snapshots = state.kernel.snapshots();
    let mut found = BTreeSet::from([start]);
    loop {
        let previous = found.clone();
        for current in &previous {
            for snapshot in &snapshots {
                if state.kernel.recovery_waits_for(*current, snapshot.id) {
                    found.insert(snapshot.id);
                }
            }
            for pin in pins.iter().filter_map(Weak::upgrade) {
                if pin.live() && pin.provider.fiber == *current {
                    found.insert(pin.owner.fiber);
                }
            }
        }
        if found == previous {
            return found;
        }
    }
}
