//! Deterministic lifecycle state machine: no executor, I/O, codec, or service objects.

use cordis_contract::{ContractId, Slot};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FiberId(pub u64);

/// An incarnation is supplied by the owning runtime; epochs never repeat in a fiber.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Activation {
    pub incarnation: u64,
    pub fiber: FiberId,
    pub epoch: u64,
}

pub type BindingView = BTreeMap<Slot, Activation>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lifecycle {
    Inactive,
    Loading,
    Active,
    Unloading,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    InitializationFailed(String),
    ExecutionFailed(String),
    CleanupFailed(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    UnknownFiber(FiberId),
    SlotOccupied(Slot),
    DependencyCycle,
    InactiveParent,
    StaleActivation(Activation),
    UnexpectedCompletion(Activation),
    UndeclaredProvision(Slot),
    DuplicateProvision(Slot),
    Retired(FiberId),
    CleanupFailed(FiberId),
    DuplicateContract(ContractId),
    ChangedProvisions(FiberId),
    ClosingFiber(FiberId),
    OwnershipCycle,
    RecoveryCycle,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Clone, Debug)]
pub struct FiberSpec {
    pub name: String,
    pub requires: BTreeSet<Slot>,
    pub provides: BTreeSet<Slot>,
    pub parent: Option<Activation>,
}

impl FiberSpec {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            requires: BTreeSet::new(),
            provides: BTreeSet::new(),
            parent: None,
        }
    }
}

/// Only namespace resolution changes; code, ownership and provided contracts do not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RealmSlots {
    pub requires: BTreeSet<Slot>,
    pub provides: BTreeSet<Slot>,
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub id: FiberId,
    pub name: String,
    pub parent: Option<Activation>,
    pub activation: Option<Activation>,
    pub state: Lifecycle,
    pub retired: bool,
    pub outcome: Option<Outcome>,
    pub target: Option<BindingView>,
    pub committed: BindingView,
    /// This activation's namespace; retained while an old initializer/recovery
    /// finishes even if the next declaration has already moved elsewhere.
    pub retained_provisions: BTreeSet<Slot>,
    pub waiting_for: Vec<Slot>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Start {
        activation: Activation,
        bindings: BindingView,
    },
    Publish(Activation),
    Withdraw(Activation),
    Recover(Activation),
    Release(Activation),
}

#[derive(Clone, Debug)]
pub enum Event {
    Fault {
        activation: Activation,
        message: String,
    },
    Retire(FiberId),
    Restart(FiberId),
    Provided {
        activation: Activation,
        slot: Slot,
    },
    Initialized {
        activation: Activation,
        result: Result<(), String>,
    },
    Drained(Activation),
    Recovered {
        activation: Activation,
        result: Result<(), String>,
    },
}

#[derive(Clone, Debug)]
struct Fiber {
    spec: FiberSpec,
    state: Lifecycle,
    epoch: u64,
    retired: bool,
    outcome: Option<Outcome>,
    target: Option<BindingView>,
    committed: BindingView,
    committed_requires: BTreeSet<Slot>,
    activation_provides: BTreeSet<Slot>,
    staged: BTreeSet<Slot>,
    setup_pending: bool,
    drained: bool,
    recovering: bool,
}

#[derive(Clone, Debug)]
pub struct Kernel {
    incarnation: u64,
    next_id: u64,
    fibers: BTreeMap<FiberId, Fiber>,
    reservations: BTreeMap<Slot, FiberId>,
    published: BTreeMap<Slot, Activation>,
    subscribers: BTreeMap<Slot, BTreeSet<FiberId>>,
    dirty: BTreeSet<FiberId>,
}

impl Kernel {
    pub fn new(incarnation: u64) -> Self {
        Self {
            incarnation,
            next_id: 0,
            fibers: BTreeMap::new(),
            reservations: BTreeMap::new(),
            published: BTreeMap::new(),
            subscribers: BTreeMap::new(),
            dirty: BTreeSet::new(),
        }
    }

    /// Validation is transactional: a rejected installation changes no kernel state.
    pub fn install(&mut self, spec: FiberSpec) -> Result<(FiberId, Vec<Action>), Error> {
        unique_contracts(&spec.requires)?;
        unique_contracts(&spec.provides)?;
        if let Some(parent) = spec.parent {
            let f = self.current(parent)?;
            if f.retired || !matches!(f.state, Lifecycle::Loading | Lifecycle::Active) {
                return Err(Error::InactiveParent);
            }
        }
        for slot in &spec.provides {
            if self.reservations.contains_key(slot) {
                return Err(Error::SlotOccupied(slot.clone()));
            }
        }
        let id = FiberId(self.next_id + 1);
        let mut reservations = self.reservations.clone();
        for slot in &spec.provides {
            reservations.insert(slot.clone(), id);
        }
        let mut graph: BTreeMap<FiberId, BTreeSet<FiberId>> = self
            .fibers
            .iter()
            .filter(|(_, f)| !f.retired)
            .map(|(&id, f)| {
                (
                    id,
                    f.spec
                        .requires
                        .iter()
                        .filter_map(|s| reservations.get(s).copied())
                        .collect(),
                )
            })
            .collect();
        graph.insert(
            id,
            spec.requires
                .iter()
                .filter_map(|s| reservations.get(s).copied())
                .collect(),
        );
        if has_cycle(&graph) {
            return Err(Error::DependencyCycle);
        }
        self.next_id += 1;
        for slot in &spec.provides {
            self.reservations.insert(slot.clone(), id);
        }
        for slot in &spec.requires {
            self.subscribers.entry(slot.clone()).or_default().insert(id);
        }
        self.fibers.insert(
            id,
            Fiber {
                spec,
                state: Lifecycle::Inactive,
                epoch: 0,
                retired: false,
                outcome: None,
                target: None,
                committed: BTreeMap::new(),
                committed_requires: BTreeSet::new(),
                activation_provides: BTreeSet::new(),
                staged: BTreeSet::new(),
                setup_pending: false,
                drained: false,
                recovering: false,
            },
        );
        self.dirty.insert(id);
        let mut actions = Vec::new();
        self.reconcile(&mut actions);
        self.assert_invariants();
        Ok((id, actions))
    }

    /// Check the final declaration graph without withdrawing any live activation.
    /// Ownership descendants of removed roots disappear with those roots. Retired
    /// but unrecovered fibers outside the removal set still reserve their slots.
    /// This is preflight, not a reservation or proof that user initialization succeeds.
    pub fn validate_replacement(
        &self,
        removing: &BTreeSet<FiberId>,
        additions: &[FiberSpec],
    ) -> Result<(), Error> {
        self.validate_reconfiguration(removing, additions, &BTreeMap::new())
    }

    pub fn validate_reconfiguration(
        &self,
        removing: &BTreeSet<FiberId>,
        additions: &[FiberSpec],
        changes: &BTreeMap<FiberId, RealmSlots>,
    ) -> Result<(), Error> {
        self.validate_realm_changes(changes)?;
        for id in removing {
            if !self.fibers.contains_key(id) {
                return Err(Error::UnknownFiber(*id));
            }
        }
        let mut preview = Self::new(self.incarnation);
        let mut retained = Vec::new();
        for (&id, fiber) in &self.fibers {
            if (fiber.retired && fiber.state == Lifecycle::Inactive)
                || removing
                    .iter()
                    .any(|root| id == *root || self.is_descendant(id, *root))
            {
                continue;
            }
            let mut spec = fiber.spec.clone();
            if let Some(slots) = changes.get(&id) {
                spec.requires = slots.requires.clone();
                spec.provides = slots.provides.clone();
            }
            // Parent lifetimes are validated at installation; only declarations
            // are simulated here, without manufacturing parent activations.
            spec.parent = None;
            let (preview_id, _) = preview.install(spec)?;
            if fiber.state == Lifecycle::Unloading {
                retained.push((preview_id, fiber.activation_provides.clone()));
            }
        }
        for spec in additions {
            let mut spec = spec.clone();
            spec.parent = None;
            preview.install(spec)?;
        }
        for (id, slots) in retained {
            for slot in slots {
                if preview
                    .reservations
                    .insert(slot.clone(), id)
                    .is_some_and(|owner| owner != id)
                {
                    return Err(Error::SlotOccupied(slot));
                }
            }
        }
        let graph = preview
            .fibers
            .iter()
            .map(|(&id, f)| {
                (
                    id,
                    f.spec
                        .requires
                        .iter()
                        .filter_map(|s| preview.reservations.get(s).copied())
                        .collect(),
                )
            })
            .collect();
        if has_cycle(&graph) {
            return Err(Error::DependencyCycle);
        }
        Ok(())
    }

    pub fn slots(&self, id: FiberId) -> Result<RealmSlots, Error> {
        let fiber = self.fibers.get(&id).ok_or(Error::UnknownFiber(id))?;
        Ok(RealmSlots {
            requires: fiber.spec.requires.clone(),
            provides: fiber.spec.provides.clone(),
        })
    }

    /// Transfer only the lifetime parent. Activation, bindings, effects and
    /// namespace provenance stay with the child. Additional edges are durable
    /// recovery waits supplied by the executor (provider -> resource owner).
    /// The entire batch is checked before any edge changes become visible.
    pub fn reparent(
        &mut self,
        changes: &BTreeMap<FiberId, Option<Activation>>,
        additional_waits: &[(FiberId, FiberId)],
    ) -> Result<(), Error> {
        if changes.is_empty() {
            return Ok(());
        }
        let mut next = self.clone();
        for (&id, &parent) in changes {
            let fiber = self.fibers.get(&id).ok_or(Error::UnknownFiber(id))?;
            if fiber.retired {
                return Err(Error::Retired(id));
            }
            // A move cannot shed existing cleanup responsibility, even when a
            // failed descendant was created dynamically and is not in the batch.
            for (&child, f) in &self.fibers {
                if child == id || self.is_descendant(child, id) {
                    if matches!(f.outcome, Some(Outcome::CleanupFailed(_))) {
                        return Err(Error::CleanupFailed(child));
                    }
                    if f.state == Lifecycle::Unloading {
                        return Err(Error::ClosingFiber(child));
                    }
                }
            }
            if let Some(parent) = parent {
                let owner = self.current(parent)?;
                if owner.retired || !matches!(owner.state, Lifecycle::Loading | Lifecycle::Active) {
                    return Err(Error::InactiveParent);
                }
            }
            next.fibers.get_mut(&id).unwrap().spec.parent = parent;
        }
        let ownership: BTreeMap<_, BTreeSet<_>> = next
            .fibers
            .iter()
            .map(|(&id, f)| (id, f.spec.parent.iter().map(|a| a.fiber).collect()))
            .collect();
        if has_cycle(&ownership) {
            return Err(Error::OwnershipCycle);
        }
        next.validate_recovery_order(additional_waits)?;
        next.assert_invariants();
        *self = next;
        Ok(())
    }

    pub fn validate_recovery_order(
        &self,
        additional_waits: &[(FiberId, FiberId)],
    ) -> Result<(), Error> {
        // Separate acyclic dependency and ownership graphs can still form a
        // cycle together. Include target declarations as well as old bindings:
        // an inactive consumer must not acquire a deadlock on its next start.
        let mut waits: BTreeMap<FiberId, BTreeSet<FiberId>> = self
            .fibers
            .keys()
            .map(|&id| (id, BTreeSet::new()))
            .collect();
        for (&id, f) in &self.fibers {
            if f.retired && f.state == Lifecycle::Inactive {
                continue;
            }
            if let Some(parent) = f.spec.parent {
                waits.get_mut(&parent.fiber).unwrap().insert(id);
            }
            for provider in f.committed.values().map(|a| a.fiber).chain(
                f.spec
                    .requires
                    .iter()
                    .filter_map(|slot| self.reservations.get(slot).copied()),
            ) {
                waits.get_mut(&provider).unwrap().insert(id);
            }
        }
        for &(waiter, blocker) in additional_waits {
            waits.entry(waiter).or_default().insert(blocker);
        }
        if has_cycle(&waits) {
            return Err(Error::RecoveryCycle);
        }
        Ok(())
    }

    fn validate_realm_changes(&self, changes: &BTreeMap<FiberId, RealmSlots>) -> Result<(), Error> {
        for (id, slots) in changes {
            let fiber = self.fibers.get(id).ok_or(Error::UnknownFiber(*id))?;
            if fiber.retired {
                return Err(Error::Retired(*id));
            }
            if matches!(fiber.outcome, Some(Outcome::CleanupFailed(_))) {
                return Err(Error::CleanupFailed(*id));
            }
            unique_contracts(&slots.requires)?;
            unique_contracts(&slots.provides)?;
            if !slots.provides.iter().map(|s| &s.contract).eq(fiber
                .spec
                .provides
                .iter()
                .map(|s| &s.contract))
            {
                return Err(Error::ChangedProvisions(*id));
            }
        }
        Ok(())
    }

    /// Atomic bookkeeping transaction. The clone executes no user code; errors
    /// leave the original graph, published slots and all responsibility unchanged.
    pub fn reconfigure_realms(
        &mut self,
        changes: &BTreeMap<FiberId, RealmSlots>,
    ) -> Result<Vec<Action>, Error> {
        if changes.is_empty() {
            return Ok(Vec::new());
        }
        self.validate_realm_changes(changes)?;
        let mut next = self.clone();
        let mut actions = Vec::new();
        for (id, slots) in changes {
            let fiber = next.fibers.get_mut(id).unwrap();
            let changed =
                fiber.spec.requires != slots.requires || fiber.spec.provides != slots.provides;
            fiber.spec.requires = slots.requires.clone();
            fiber.spec.provides = slots.provides.clone();
            // An unfinished initializer keeps its original provision namespace.
            // Its late effects must land and recover before the new activation.
            if changed && fiber.state == Lifecycle::Loading {
                next.withdraw(*id, &mut actions);
            }
        }
        next.published.clear();
        for (&id, fiber) in &next.fibers {
            if fiber.state == Lifecycle::Active {
                for slot in &fiber.spec.provides {
                    if next
                        .published
                        .insert(slot.clone(), next.activation(id))
                        .is_some()
                    {
                        return Err(Error::SlotOccupied(slot.clone()));
                    }
                }
            }
        }
        next.reindex()?;
        next.dirty.extend(next.fibers.keys().copied());
        next.reconcile(&mut actions);
        for fiber in next
            .fibers
            .values_mut()
            .filter(|f| f.state == Lifecycle::Active)
        {
            fiber.activation_provides = fiber.spec.provides.clone();
            fiber.staged = fiber.spec.provides.clone();
        }
        // Departing activations still reserve their old slots until recovery.
        next.reindex()?;
        next.validate_recovery_order(&[])?;
        next.assert_invariants();
        *self = next;
        Ok(actions)
    }

    fn reindex(&mut self) -> Result<(), Error> {
        let mut reservations = BTreeMap::new();
        let mut subscribers: BTreeMap<Slot, BTreeSet<FiberId>> = BTreeMap::new();
        for (&id, fiber) in &self.fibers {
            if fiber.retired && fiber.state == Lifecycle::Inactive {
                continue;
            }
            let mut slots = fiber.spec.provides.clone();
            if fiber.state == Lifecycle::Unloading {
                slots.extend(fiber.activation_provides.clone());
            }
            for slot in slots {
                if reservations
                    .insert(slot.clone(), id)
                    .is_some_and(|old| old != id)
                {
                    return Err(Error::SlotOccupied(slot));
                }
            }
            for slot in &fiber.spec.requires {
                subscribers.entry(slot.clone()).or_default().insert(id);
            }
        }
        let graph = self
            .fibers
            .iter()
            .filter(|(_, f)| !f.retired)
            .map(|(&id, f)| {
                (
                    id,
                    f.spec
                        .requires
                        .iter()
                        .filter_map(|s| reservations.get(s).copied())
                        .collect(),
                )
            })
            .collect();
        if has_cycle(&graph) {
            return Err(Error::DependencyCycle);
        }
        self.reservations = reservations;
        self.subscribers = subscribers;
        Ok(())
    }

    pub fn step(&mut self, event: Event) -> Result<Vec<Action>, Error> {
        let mut actions = Vec::new();
        match event {
            Event::Fault {
                activation,
                message,
            } => {
                let f = self.current_mut(activation)?;
                if matches!(f.state, Lifecycle::Loading | Lifecycle::Active) {
                    f.outcome = Some(Outcome::ExecutionFailed(message));
                    self.withdraw(activation.fiber, &mut actions);
                    self.dirty.insert(activation.fiber);
                }
            }
            Event::Retire(id) => {
                self.retire(id, &mut actions)?;
            }
            Event::Restart(id) => {
                let f = self.fibers.get_mut(&id).ok_or(Error::UnknownFiber(id))?;
                if f.retired {
                    return Err(Error::Retired(id));
                }
                if matches!(f.outcome, Some(Outcome::CleanupFailed(_))) {
                    return Err(Error::CleanupFailed(id));
                }
                f.outcome = None;
                if matches!(f.state, Lifecycle::Loading | Lifecycle::Active) {
                    self.withdraw(id, &mut actions);
                }
                self.dirty.insert(id);
            }
            Event::Provided { activation, slot } => {
                let f = self.current_mut(activation)?;
                if !f.setup_pending {
                    return Err(Error::UnexpectedCompletion(activation));
                }
                if !f.activation_provides.contains(&slot) {
                    return Err(Error::UndeclaredProvision(slot));
                }
                if !f.staged.insert(slot.clone()) {
                    return Err(Error::DuplicateProvision(slot));
                }
            }
            Event::Initialized { activation, result } => {
                let f = self.current_mut(activation)?;
                if !f.setup_pending {
                    return Err(Error::UnexpectedCompletion(activation));
                }
                f.setup_pending = false;
                let result = result.and_then(|()| {
                    // A stopped initializer need not run its remaining provision
                    // steps. Only an activation eligible for publication is complete.
                    if f.state != Lifecycle::Loading || f.staged == f.activation_provides {
                        Ok(())
                    } else {
                        Err("missing declared service provision".into())
                    }
                });
                if let Err(message) = result {
                    f.outcome = Some(Outcome::InitializationFailed(message));
                }
                if f.state == Lifecycle::Loading
                    && f.outcome.is_none()
                    && f.target
                        .as_ref()
                        .is_some_and(|target| same_bindings(target, &f.committed))
                    && !f.retired
                {
                    f.state = Lifecycle::Active;
                    let slots = f.staged.clone();
                    for slot in slots {
                        self.published.insert(slot.clone(), activation);
                        self.invalidate(&slot);
                    }
                    actions.push(Action::Publish(activation));
                } else if f.state == Lifecycle::Loading {
                    self.withdraw(activation.fiber, &mut actions);
                }
            }
            Event::Drained(activation) => {
                let f = self.current_mut(activation)?;
                if f.state != Lifecycle::Unloading {
                    return Err(Error::UnexpectedCompletion(activation));
                }
                f.drained = true;
            }
            Event::Recovered { activation, result } => {
                let f = self.current_mut(activation)?;
                if !f.recovering || matches!(f.outcome, Some(Outcome::CleanupFailed(_))) {
                    return Err(Error::UnexpectedCompletion(activation));
                }
                // No automatic cleanup retry: an inverse may have partially executed.
                if let Err(message) = result {
                    f.outcome = Some(Outcome::CleanupFailed(message));
                } else {
                    f.state = Lifecycle::Inactive;
                    f.committed.clear();
                    f.committed_requires.clear();
                    f.activation_provides.clear();
                    f.staged.clear();
                    f.recovering = false;
                    let reserved = if f.retired {
                        BTreeSet::new()
                    } else {
                        f.spec.provides.clone()
                    };
                    actions.push(Action::Release(activation));
                    self.dirty.insert(activation.fiber);
                    self.reservations.retain(|_, id| *id != activation.fiber);
                    for slot in reserved {
                        self.reservations.insert(slot, activation.fiber);
                    }
                }
            }
        }
        self.reconcile(&mut actions);
        self.assert_invariants();
        Ok(actions)
    }

    pub fn snapshot(&self, id: FiberId) -> Result<Snapshot, Error> {
        let f = self.fibers.get(&id).ok_or(Error::UnknownFiber(id))?;
        Ok(Snapshot {
            id,
            name: f.spec.name.clone(),
            parent: f.spec.parent,
            activation: (f.epoch > 0).then_some(self.activation(id)),
            state: f.state,
            retired: f.retired,
            outcome: f.outcome.clone(),
            target: f.target.clone(),
            committed: f.committed.clone(),
            retained_provisions: f.activation_provides.clone(),
            waiting_for: f
                .spec
                .requires
                .iter()
                .filter(|s| !self.published.contains_key(*s))
                .cloned()
                .collect(),
        })
    }

    pub fn snapshots(&self) -> Vec<Snapshot> {
        self.fibers
            .keys()
            .map(|&id| self.snapshot(id).unwrap())
            .collect()
    }

    pub fn published(&self, slot: &Slot) -> Option<Activation> {
        self.published.get(slot).copied()
    }

    pub fn is_descendant(&self, mut child: FiberId, parent: FiberId) -> bool {
        while let Some(owner) = self.fibers.get(&child).and_then(|f| f.spec.parent) {
            if owner.fiber == parent {
                return true;
            }
            child = owner.fiber;
        }
        false
    }

    /// Follow the actual recovery barrier, including paths that alternate
    /// committed dependency edges and child ownership edges.
    pub fn recovery_waits_for(&self, waiter: FiberId, blocker: FiberId) -> bool {
        let mut todo = vec![waiter];
        let mut seen = BTreeSet::new();
        while let Some(id) = todo.pop() {
            if !seen.insert(id) {
                continue;
            }
            if id == blocker {
                return true;
            }
            if !self.fibers.contains_key(&id) {
                continue;
            }
            let activation = self.activation(id);
            todo.extend(self.fibers.iter().filter_map(|(&other, f)| {
                (other != id
                    && (f.committed.values().any(|&a| a == activation)
                        || (f.spec.parent == Some(activation) && f.state != Lifecycle::Inactive)))
                    .then_some(other)
            }));
        }
        false
    }

    /// Detect waiting for a child whose activation transitively needs the waiting initializer.
    pub fn depends_on(&self, consumer: FiberId, provider: FiberId) -> bool {
        let mut todo = vec![consumer];
        let mut seen = BTreeSet::new();
        while let Some(id) = todo.pop() {
            if !seen.insert(id) {
                continue;
            }
            if id == provider {
                return true;
            }
            if let Some(f) = self.fibers.get(&id) {
                todo.extend(
                    f.spec
                        .requires
                        .iter()
                        .filter_map(|s| self.reservations.get(s))
                        .copied(),
                );
            }
        }
        false
    }

    fn activation(&self, id: FiberId) -> Activation {
        Activation {
            incarnation: self.incarnation,
            fiber: id,
            epoch: self.fibers[&id].epoch,
        }
    }

    fn current(&self, a: Activation) -> Result<&Fiber, Error> {
        let f = self
            .fibers
            .get(&a.fiber)
            .ok_or(Error::UnknownFiber(a.fiber))?;
        if a.incarnation != self.incarnation || a.epoch != f.epoch || f.state == Lifecycle::Inactive
        {
            return Err(Error::StaleActivation(a));
        }
        Ok(f)
    }

    fn current_mut(&mut self, a: Activation) -> Result<&mut Fiber, Error> {
        self.current(a)?;
        Ok(self.fibers.get_mut(&a.fiber).unwrap())
    }

    fn invalidate(&mut self, slot: &Slot) {
        if let Some(ids) = self.subscribers.get(slot) {
            self.dirty.extend(ids);
        }
    }

    fn retire(&mut self, id: FiberId, actions: &mut Vec<Action>) -> Result<(), Error> {
        let f = self.fibers.get_mut(&id).ok_or(Error::UnknownFiber(id))?;
        f.retired = true;
        self.dirty.insert(id);
        if matches!(f.state, Lifecycle::Loading | Lifecycle::Active) {
            self.withdraw(id, actions);
        }
        Ok(())
    }

    fn withdraw(&mut self, id: FiberId, actions: &mut Vec<Action>) {
        let a = self.activation(id);
        let f = self.fibers.get_mut(&id).unwrap();
        if f.state == Lifecycle::Unloading {
            return;
        }
        f.state = Lifecycle::Unloading;
        f.drained = false;
        let slots: Vec<_> = self
            .published
            .iter()
            .filter_map(|(slot, provider)| (*provider == a).then_some(slot.clone()))
            .collect();
        actions.push(Action::Withdraw(a));
        for slot in slots {
            if self.published.get(&slot) == Some(&a) {
                self.published.remove(&slot);
            }
            self.invalidate(&slot);
        }
        let children: Vec<_> = self
            .fibers
            .iter()
            .filter(|(_, f)| f.spec.parent == Some(a))
            .map(|(&id, _)| id)
            .collect();
        for child in children {
            self.retire(child, actions).unwrap();
        }
    }

    fn reconcile(&mut self, actions: &mut Vec<Action>) {
        loop {
            let mut changed = false;
            for id in std::mem::take(&mut self.dirty) {
                let f = &self.fibers[&id];
                let target = if f.retired || f.outcome.is_some() {
                    None
                } else {
                    f.spec
                        .requires
                        .iter()
                        .map(|s| self.published.get(s).map(|&a| (s.clone(), a)))
                        .collect()
                };
                let f = self.fibers.get_mut(&id).unwrap();
                f.target = target;
                if matches!(f.state, Lifecycle::Loading | Lifecycle::Active)
                    && !f
                        .target
                        .as_ref()
                        .is_some_and(|target| same_bindings(target, &f.committed))
                {
                    self.withdraw(id, actions);
                    changed = true;
                }
            }
            // Finish the withdrawal closure before starting any waiting fiber.
            // Otherwise an early ID could commit a provider that a later ID in
            // this same namespace transaction is about to withdraw.
            if !self.dirty.is_empty() {
                continue;
            }
            let ids: Vec<_> = self.fibers.keys().copied().collect();
            for &id in &ids {
                let f = self.fibers.get_mut(&id).unwrap();
                if f.state == Lifecycle::Inactive
                    && let Some(bindings) = f.target.clone()
                {
                    f.epoch += 1;
                    f.state = Lifecycle::Loading;
                    f.committed = bindings.clone();
                    f.committed_requires = f.spec.requires.clone();
                    f.activation_provides = f.spec.provides.clone();
                    f.setup_pending = true;
                    f.recovering = false;
                    f.drained = false;
                    actions.push(Action::Start {
                        activation: self.activation(id),
                        bindings,
                    });
                    changed = true;
                }
            }
            for id in ids {
                let f = &self.fibers[&id];
                let a = self.activation(id);
                if f.state == Lifecycle::Unloading && !f.setup_pending && f.drained && !f.recovering
                {
                    let relied = self.fibers.iter().any(|(&other, g)| {
                        other != id
                            && (g.committed.values().any(|&b| b == a)
                                || (g.spec.parent == Some(a) && g.state != Lifecycle::Inactive))
                    });
                    if !relied {
                        self.fibers.get_mut(&id).unwrap().recovering = true;
                        actions.push(Action::Recover(a));
                        changed = true;
                    }
                }
                let f = &self.fibers[&id];
                if f.retired && f.state == Lifecycle::Inactive {
                    for slot in &f.spec.provides {
                        if self.reservations.get(slot) == Some(&id) {
                            self.reservations.remove(slot);
                        }
                    }
                    for slot in &f.spec.requires {
                        if let Some(ids) = self.subscribers.get_mut(slot) {
                            ids.remove(&id);
                        }
                    }
                }
            }
            if !changed && self.dirty.is_empty() {
                break;
            }
        }
    }

    /// Executable structural invariants, also called after every successful transition.
    pub fn check_invariants(&self) -> Result<(), String> {
        let ownership = self
            .fibers
            .iter()
            .map(|(&id, f)| (id, f.spec.parent.iter().map(|a| a.fiber).collect()))
            .collect();
        if has_cycle(&ownership) {
            return Err("cyclic ownership".into());
        }
        let mut reservations = BTreeMap::new();
        for (&id, f) in &self.fibers {
            unique_contracts(&f.spec.requires).map_err(|_| "duplicate required contract")?;
            unique_contracts(&f.spec.provides).map_err(|_| "duplicate provided contract")?;
            if !f.retired || f.state != Lifecycle::Inactive {
                let mut slots = f.spec.provides.clone();
                if f.state == Lifecycle::Unloading {
                    slots.extend(f.activation_provides.clone());
                }
                for slot in slots {
                    if reservations.insert(slot, id).is_some_and(|old| old != id) {
                        return Err("overlapping live or recovering reservations".into());
                    }
                }
            }
            if let Some(parent) = f.spec.parent
                && !self.fibers.contains_key(&parent.fiber)
            {
                return Err("dangling parent".into());
            }
            if !f.retired
                && let Some(parent) = f.spec.parent
            {
                let owner = self.current(parent).map_err(|_| "stale live parent")?;
                if owner.retired || !matches!(owner.state, Lifecycle::Loading | Lifecycle::Active) {
                    return Err("live child belongs to closing parent".into());
                }
            }
            if f.state != Lifecycle::Inactive {
                if f.committed.keys().cloned().collect::<BTreeSet<_>>() != f.committed_requires {
                    return Err("incomplete committed view".into());
                }
                for a in f.committed.values() {
                    let provider = self.current(*a).map_err(|_| "dangling committed binding")?;
                    if provider.recovering {
                        return Err("provider recovering before consumer".into());
                    }
                }
            } else if !f.committed.is_empty() {
                return Err("inactive fiber retains bindings".into());
            }
            if f.state == Lifecycle::Active {
                if !f
                    .target
                    .as_ref()
                    .is_some_and(|target| same_bindings(target, &f.committed))
                {
                    return Err("active binding differs from target".into());
                }
                for slot in &f.spec.provides {
                    if self.published.get(slot) != Some(&self.activation(id)) {
                        return Err("active provision missing".into());
                    }
                }
            }
            if f.recovering && (f.state != Lifecycle::Unloading || f.setup_pending || !f.drained) {
                return Err("recovery crossed barrier".into());
            }
        }
        if reservations != self.reservations {
            return Err("reservation index differs from live and recovering declarations".into());
        }
        for (slot, a) in &self.published {
            let f = self.current(*a).map_err(|_| "stale published binding")?;
            if f.state != Lifecycle::Active || !f.spec.provides.contains(slot) {
                return Err("non-active published binding".into());
            }
        }
        Ok(())
    }

    fn assert_invariants(&self) {
        debug_assert!(
            self.check_invariants().is_ok(),
            "{:?}",
            self.check_invariants()
        );
    }
}

fn unique_contracts(slots: &BTreeSet<Slot>) -> Result<(), Error> {
    let mut seen = BTreeSet::new();
    for slot in slots {
        if !seen.insert(&slot.contract) {
            return Err(Error::DuplicateContract(slot.contract.clone()));
        }
    }
    Ok(())
}

// Slot is a discovery address. The actual committed service is identified by
// contract and provider activation; relocating that address does not replace it.
fn same_bindings(left: &BindingView, right: &BindingView) -> bool {
    left.iter()
        .map(|(slot, provider)| (&slot.contract, provider))
        .eq(right
            .iter()
            .map(|(slot, provider)| (&slot.contract, provider)))
}

fn has_cycle(graph: &BTreeMap<FiberId, BTreeSet<FiberId>>) -> bool {
    fn visit(
        id: FiberId,
        graph: &BTreeMap<FiberId, BTreeSet<FiberId>>,
        visiting: &mut BTreeSet<FiberId>,
        done: &mut BTreeSet<FiberId>,
    ) -> bool {
        if done.contains(&id) {
            return false;
        }
        if !visiting.insert(id) {
            return true;
        }
        if graph
            .get(&id)
            .is_some_and(|deps| deps.iter().any(|&next| visit(next, graph, visiting, done)))
        {
            return true;
        }
        visiting.remove(&id);
        done.insert(id);
        false
    }
    let mut visiting = BTreeSet::new();
    let mut done = BTreeSet::new();
    graph
        .keys()
        .any(|&id| visit(id, graph, &mut visiting, &mut done))
}
