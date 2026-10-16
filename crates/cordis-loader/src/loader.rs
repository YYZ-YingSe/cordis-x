use crate::{
    ArtifactRef, Configuration, Entry, Error, Failure, Realm, Registry, Result, config::GROUP,
};
use cordis_contract::{ContractId, RealmId};
use cordis_kernel::{Lifecycle, Snapshot};
use cordis_runtime::{
    Context, EffectSnapshot, InterceptConfig, Intercepts, OwnershipChange, Plugin, PluginHandle,
    RealmChange, RealmReport, Runtime, Service, View,
};
use futures_util::future::join_all;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot, watch};

/// Effective configuration after group enablement and realm inheritance.
/// Group child lists are reconciled separately, so editing a child does not
/// rebuild its surviving group or siblings.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedEntry {
    pub id: String,
    pub source: String,
    pub artifact: Option<ArtifactRef>,
    pub config: Value,
    pub parent: Option<String>,
    pub enabled: bool,
    pub realms: BTreeMap<ContractId, RealmId>,
    /// Ancestor-to-child use-config layers; validated by registered service adapters.
    pub intercepts: BTreeMap<ContractId, Vec<Value>>,
}
impl ResolvedEntry {
    fn same_definition(&self, other: &Self) -> bool {
        self.id == other.id
            && self.source == other.source
            && self.artifact == other.artifact
            && self.config == other.config
            && self.enabled == other.enabled
    }
    fn view(&self) -> View {
        self.realms
            .iter()
            .fold(View::new(), |view, (contract, realm)| {
                view.in_realm(
                    &Service::<()>::new(contract.name.clone(), contract.revision.clone()),
                    *realm,
                )
            })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Plan {
    pub base_revision: u64,
    /// New and replacement instances, in ownership parent-before-child order.
    pub install: Vec<String>,
    pub retire: Vec<String>,
    pub unchanged: Vec<String>,
    /// Update use configuration while preserving the Fiber and its activation.
    pub update_intercepts: Vec<String>,
    /// Same code; update resolution without replacing the Fiber.
    pub update_realms: Vec<String>,
    /// Transfer surviving entries to a different lifetime parent.
    pub reparent: Vec<String>,
    /// Includes inherited dynamic children; dependency slot changes are resolved
    /// at commit and only a changed binding causes an active consumer to unload.
    pub realm_changes: Vec<RealmChange>,
    pub disabled: Vec<String>,
    /// Candidate code identities, including disabled entries. A plan is a
    /// preview; apply rechecks and pins its own prepared candidates.
    pub artifacts: BTreeMap<String, ArtifactRef>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Phase {
    #[default]
    Idle,
    Applying,
    Failed,
}

#[derive(Clone, Debug)]
pub struct EntrySnapshot {
    pub id: String,
    pub desired: Option<ResolvedEntry>,
    pub actual: Option<ResolvedEntry>,
    /// Revision that created this instance, not its current activation epoch.
    pub actual_revision: Option<u64>,
    /// Revision of the live use-config source; older calls/resources may retain older snapshots.
    pub intercept_revision: Option<u64>,
    pub realm_revision: Option<u64>,
    pub ownership_revision: Option<u64>,
    pub instance: Option<Snapshot>,
    pub effects: Vec<EffectSnapshot>,
}

#[derive(Clone, Debug)]
pub struct LoaderSnapshot {
    pub desired_revision: u64,
    pub applied_revision: u64,
    pub configuration: Configuration,
    /// State of the most recent accepted configuration operation. Runtime faults
    /// that happen later remain visible in each entry's live instance snapshot.
    pub phase: Phase,
    pub entries: Vec<EntrySnapshot>,
    pub last_error: Option<Error>,
}

#[derive(Clone, Debug)]
pub struct ApplyReport {
    pub revision: u64,
    pub plan: Plan,
    pub snapshot: LoaderSnapshot,
    pub preparation: PreparationStats,
    pub realms: RealmReport,
    pub ownership: Vec<OwnershipChange>,
}

/// Host preparation costs, excluding retirement and new business initialization.
#[derive(Clone, Debug, Default)]
pub struct PreparationStats {
    pub resolve: Duration,
    pub verify_retained: Duration,
    pub build: Duration,
    pub validate_graph: Duration,
    pub definitions_built: usize,
    pub definitions_retained: usize,
}

#[derive(Clone)]
struct Actual {
    entry: ResolvedEntry,
    revision: u64,
    handle: PluginHandle,
    group_context: Option<Context>,
    intercepts: Intercepts,
    intercept_revision: u64,
    realm_revision: u64,
    ownership_revision: u64,
}

#[derive(Default)]
struct State {
    desired_revision: u64,
    applied_revision: u64,
    configuration: Configuration,
    desired: BTreeMap<String, ResolvedEntry>,
    actual: BTreeMap<String, Actual>,
    phase: Phase,
    last_error: Option<Error>,
}
impl State {
    fn snapshot(&self) -> LoaderSnapshot {
        let ids: BTreeSet<_> = self
            .desired
            .keys()
            .chain(self.actual.keys())
            .cloned()
            .collect();
        LoaderSnapshot {
            desired_revision: self.desired_revision,
            applied_revision: self.applied_revision,
            configuration: self.configuration.clone(),
            phase: self.phase,
            last_error: self.last_error.clone(),
            entries: ids
                .into_iter()
                .map(|id| {
                    let actual = self.actual.get(&id);
                    EntrySnapshot {
                        desired: self.desired.get(&id).cloned(),
                        actual: actual.map(|a| a.entry.clone()),
                        actual_revision: actual.map(|a| a.revision),
                        intercept_revision: actual.map(|a| a.intercept_revision),
                        realm_revision: actual.map(|a| a.realm_revision),
                        ownership_revision: actual.map(|a| a.ownership_revision),
                        instance: actual.map(|a| a.handle.snapshot()),
                        effects: actual.map(|a| a.handle.effects()).unwrap_or_default(),
                        id,
                    }
                })
                .collect(),
        }
    }
}

enum Command {
    Plan(Configuration, oneshot::Sender<Result<Plan>>),
    Apply(Configuration, oneshot::Sender<Result<ApplyReport>>),
}

/// One serialized reconciler. Once a request is queued, cancellation of its wait
/// does not cancel preparation, retirement, or installation. Concurrent requests
/// run FIFO; at most 16 wait in the queue. Drop does not dispose plugins: apply an
/// empty Configuration or shut down the owning Runtime explicitly.
#[derive(Clone)]
pub struct Loader {
    commands: mpsc::Sender<Command>,
    state: Arc<Mutex<State>>,
    runtime: Runtime,
    changed: watch::Sender<()>,
}

/// Coalesced wake-ups for configuration operations and live runtime transitions.
/// Subscribe before reading `Loader::snapshot`; this is not a history of events.
pub struct LoaderChanges {
    configuration: watch::Receiver<()>,
    runtime: watch::Receiver<u64>,
}
impl LoaderChanges {
    pub async fn changed(&mut self) -> Result<()> {
        tokio::select! {
            result = self.configuration.changed() => result.map_err(|_| Error::Stopped),
            result = self.runtime.changed() => result.map_err(|_| Error::Stopped),
        }
    }
}

impl Loader {
    pub fn new(runtime: Runtime, registry: Registry) -> Self {
        let state = Arc::new(Mutex::new(State::default()));
        let (commands, mut receiver) = mpsc::channel(16);
        let (changed, _) = watch::channel(());
        let mut worker = Worker {
            runtime: runtime.clone(),
            registry,
            state: state.clone(),
            changed: changed.clone(),
            realms: BTreeMap::new(),
        };
        tokio::spawn(async move {
            while let Some(command) = receiver.recv().await {
                match command {
                    Command::Plan(config, reply) => {
                        let _ =
                            reply.send(worker.prepare(&config).await.map(|prepared| prepared.plan));
                    }
                    Command::Apply(config, reply) => {
                        let result = worker.apply(config).await;
                        let _ = reply.send(result);
                    }
                }
            }
        });
        Self {
            commands,
            state,
            runtime,
            changed,
        }
    }

    pub fn snapshot(&self) -> LoaderSnapshot {
        self.state.lock().unwrap().snapshot()
    }

    pub fn subscribe(&self) -> LoaderChanges {
        LoaderChanges {
            configuration: self.changed.subscribe(),
            runtime: self.runtime.subscribe(),
        }
    }

    /// A preview executes factory validation but no plugin effects. Apply always
    /// recomputes its plan; a preview does not reserve runtime slots.
    pub async fn plan(&self, config: Configuration) -> Result<Plan> {
        let (reply, wait) = oneshot::channel();
        self.send(Command::Plan(config, reply))?;
        wait.await.map_err(|_| Error::Stopped)?
    }

    /// Finish old recovery and install the new definitions. New initializers may
    /// still be running; use settled() separately so a later apply can withdraw
    /// an initializer that needs cancellation to complete.
    pub async fn apply(&self, config: Configuration) -> Result<ApplyReport> {
        self.submit(config)?.await.map_err(|_| Error::Stopped)?
    }

    pub(crate) fn submit(
        &self,
        config: Configuration,
    ) -> Result<oneshot::Receiver<Result<ApplyReport>>> {
        let (reply, wait) = oneshot::channel();
        self.send(Command::Apply(config, reply))?;
        Ok(wait)
    }

    /// Wait for the current accepted configuration to be stable. Missing services
    /// may leave entries Inactive; initialization and cleanup failures are errors.
    /// A concurrent accepted apply causes this wait to follow the newer revision.
    pub async fn settled(&self) -> Result<LoaderSnapshot> {
        let mut changed = self.changed.subscribe();
        loop {
            let (revision, phase, error, actual) = {
                let state = self.state.lock().unwrap();
                (
                    state.desired_revision,
                    state.phase,
                    state.last_error.clone(),
                    state.actual.clone(),
                )
            };
            if phase == Phase::Applying {
                changed.changed().await.map_err(|_| Error::Stopped)?;
                continue;
            }
            if let Some(error) = error {
                return Err(error);
            }
            let results =
                join_all(actual.iter().map(|(id, actual)| async move {
                    (id.clone(), actual.handle.settled().await)
                }))
                .await;
            let state = self.state.lock().unwrap();
            if state.desired_revision != revision || state.phase == Phase::Applying {
                continue;
            }
            let failures: Vec<_> = results
                .into_iter()
                .filter_map(|(entry, result)| result.err().map(|error| Failure { entry, error }))
                .collect();
            if !failures.is_empty() {
                return Err(Error::Apply(failures));
            }
            let ids: BTreeSet<_> = actual.values().map(|a| a.handle.id()).collect();
            // A provider can activate a consumer after its earlier settled()
            // returned Inactive. Inspect all managed states under one runtime lock.
            if self
                .runtime
                .snapshots()
                .iter()
                .filter(|s| ids.contains(&s.id))
                .all(|s| {
                    matches!(s.state, Lifecycle::Inactive | Lifecycle::Active)
                        && s.outcome.is_none()
                        && !s.retired
                })
            {
                return Ok(state.snapshot());
            }
        }
    }

    fn send(&self, command: Command) -> Result<()> {
        self.commands
            .try_send(command)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Error::QueueFull,
                mpsc::error::TrySendError::Closed(_) => Error::Stopped,
            })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum RealmKey {
    Local(String, ContractId),
    Shared(String, ContractId),
}

struct Prepared {
    entries: Vec<ResolvedEntry>,
    plugins: BTreeMap<String, Plugin>,
    intercepts: BTreeMap<String, InterceptConfig>,
    realms: BTreeMap<RealmKey, RealmId>,
    plan: Plan,
    stats: PreparationStats,
}

#[derive(Clone)]
struct Worker {
    runtime: Runtime,
    registry: Registry,
    state: Arc<Mutex<State>>,
    changed: watch::Sender<()>,
    // Retained for this loader's lifetime: disable/re-enable and moves preserve
    // realm identity. These entries contain only IDs, never resources or plugins.
    realms: BTreeMap<RealmKey, RealmId>,
}
impl Worker {
    async fn prepare(&self, config: &Configuration) -> Result<Prepared> {
        let worker = self.clone();
        let config = config.clone();
        tokio::task::spawn_blocking(move || worker.prepare_sync(&config))
            .await
            .map_err(|error| Error::Preparation(error.to_string()))?
    }

    fn prepare_sync(&self, config: &Configuration) -> Result<Prepared> {
        let start = Instant::now();
        let mut prepared = Prepared {
            entries: Vec::new(),
            plugins: BTreeMap::new(),
            intercepts: BTreeMap::new(),
            realms: self.realms.clone(),
            plan: Plan::default(),
            stats: PreparationStats::default(),
        };
        let mut ids = BTreeSet::new();
        self.resolve(
            &config.entries,
            None,
            &BTreeMap::new(),
            &BTreeMap::new(),
            true,
            0,
            &mut ids,
            &mut prepared,
        )?;
        let (revision, actual) = {
            let state = self.state.lock().unwrap();
            (state.desired_revision, state.actual.clone())
        };
        prepared.plan.base_revision = revision;
        let desired: BTreeMap<_, _> = prepared
            .entries
            .iter()
            .map(|entry| (&entry.id, entry))
            .collect();
        let mut retiring = BTreeSet::new();
        for (id, old) in &actual {
            let snapshot = old.handle.snapshot();
            if desired
                .get(id)
                .is_none_or(|new| !new.enabled || !new.same_definition(&old.entry))
                || snapshot.retired
                || snapshot.outcome.is_some()
            {
                retiring.insert(id.clone());
            }
        }
        // Preserve a subtree explicitly moved out of a retiring group. Children
        // staying with a replaced parent still belong to its old activation and
        // are rebuilt. Moving into a replaced parent follows the same rule.
        let children: Vec<_> = actual
            .iter()
            .filter(|(id, _)| {
                desired.get(*id).is_some_and(|new| {
                    let mut ancestor = new.parent.as_ref();
                    while let Some(parent) = ancestor {
                        if retiring.contains(parent) {
                            return true;
                        }
                        ancestor = desired[parent].parent.as_ref();
                    }
                    false
                })
            })
            .map(|(id, _)| id.clone())
            .collect();
        retiring.extend(children);
        for entry in &prepared.entries {
            if !entry.enabled {
                prepared.plan.disabled.push(entry.id.clone());
            } else if !actual.contains_key(&entry.id) || retiring.contains(&entry.id) {
                prepared.plan.install.push(entry.id.clone());
            } else {
                if actual[&entry.id].entry.intercepts != entry.intercepts {
                    prepared.plan.update_intercepts.push(entry.id.clone());
                }
                if actual[&entry.id].entry.realms != entry.realms {
                    prepared.plan.update_realms.push(entry.id.clone());
                }
                if actual[&entry.id].entry.parent != entry.parent {
                    prepared.plan.reparent.push(entry.id.clone());
                }
                if actual[&entry.id].entry.intercepts == entry.intercepts
                    && actual[&entry.id].entry.realms == entry.realms
                    && actual[&entry.id].entry.parent == entry.parent
                {
                    prepared.plan.unchanged.push(entry.id.clone());
                }
            }
        }
        prepared.plan.retire = retiring.into_iter().collect();
        prepared.stats.resolve = start.elapsed();
        // Resolve the desired graph and ownership diff before constructing code.
        // Disabled entries still receive full validation, even though no definition
        // is retained for them. Existing fibers already own their pinned code.
        let installing: BTreeSet<_> = prepared.plan.install.iter().collect();
        for entry in &prepared.entries {
            if !entry.enabled || installing.contains(&entry.id) {
                let start = Instant::now();
                let plugin = if entry.source == GROUP {
                    Plugin::new(format!("group:{}", entry.id), |_| async { Ok(()) })
                } else {
                    self.registry.build(
                        &entry.id,
                        &entry.source,
                        entry.artifact.as_ref(),
                        &entry.config,
                    )?
                };
                prepared.stats.build += start.elapsed();
                prepared.stats.definitions_built += 1;
                if entry.enabled {
                    prepared.plugins.insert(entry.id.clone(), plugin);
                }
            } else {
                let start = Instant::now();
                if entry.source != GROUP {
                    self.registry.verify_retained(
                        &entry.id,
                        &entry.source,
                        entry.artifact.as_ref(),
                    )?;
                }
                prepared.stats.verify_retained += start.elapsed();
                prepared.stats.definitions_retained += 1;
            }
        }
        let start = Instant::now();
        let removing: Vec<_> = prepared
            .plan
            .retire
            .iter()
            .map(|id| actual[id].handle.clone())
            .collect();
        let additions: Vec<_> = prepared
            .entries
            .iter()
            .filter(|entry| prepared.plan.install.contains(&entry.id))
            .map(|entry| (entry.view(), prepared.plugins[&entry.id].clone()))
            .collect();
        let updates: Vec<_> = prepared
            .entries
            .iter()
            .filter(|entry| prepared.plan.update_realms.contains(&entry.id))
            .map(|entry| (actual[&entry.id].handle.clone(), entry.view()))
            .collect();
        let ownership: Vec<_> = prepared
            .entries
            .iter()
            .filter(|entry| prepared.plan.reparent.contains(&entry.id))
            .map(|entry| {
                (
                    actual[&entry.id].handle.clone(),
                    entry
                        .parent
                        .as_ref()
                        .and_then(|id| actual.get(id))
                        .and_then(|a| a.handle.snapshot().activation),
                )
            })
            .collect();
        // A new built-in group has no services, dependencies or user effects.
        // Preview its future child as a root; execute creates the group and
        // rechecks the complete actual ownership graph before transferring it.
        prepared.plan.realm_changes = self
            .runtime
            .validate_reconfiguration(&removing, &additions, &updates, &ownership)?;
        prepared.stats.validate_graph = start.elapsed();
        Ok(prepared)
    }

    #[allow(clippy::too_many_arguments)]
    fn resolve(
        &self,
        entries: &[Entry],
        parent: Option<&str>,
        inherited: &BTreeMap<ContractId, RealmId>,
        inherited_intercepts: &BTreeMap<ContractId, Vec<Value>>,
        enabled: bool,
        depth: usize,
        ids: &mut BTreeSet<String>,
        prepared: &mut Prepared,
    ) -> Result<()> {
        if depth > 64 {
            return Err(Error::InvalidConfig("group nesting exceeds 64".into()));
        }
        for entry in entries {
            if entry.id.trim().is_empty() || !ids.insert(entry.id.clone()) {
                return Err(Error::InvalidConfig(format!(
                    "empty or duplicate entry id: {}",
                    entry.id
                )));
            }
            let group = entry.source == GROUP;
            if (!group && !entry.children.is_empty())
                || (group && (entry.config != Value::Null || entry.artifact.is_some()))
            {
                return Err(Error::InvalidConfig(format!(
                    "entry {}: only cordis:group accepts children; groups require null config and no artifact",
                    entry.id
                )));
            }
            if let Some(artifact) = &entry.artifact {
                prepared
                    .plan
                    .artifacts
                    .insert(entry.id.clone(), artifact.clone());
            }
            let mut realms = inherited.clone();
            let mut seen = BTreeSet::new();
            for isolation in &entry.isolate {
                let contract = ContractId::new(isolation.name.clone(), isolation.revision.clone());
                if isolation.name.trim().is_empty()
                    || isolation.revision.trim().is_empty()
                    || !seen.insert(contract.clone())
                {
                    return Err(Error::InvalidConfig(format!(
                        "entry {}: invalid or duplicate isolation",
                        entry.id
                    )));
                }
                let key = match &isolation.realm {
                    Realm::Root => {
                        realms.remove(&contract);
                        continue;
                    }
                    Realm::Local => RealmKey::Local(entry.id.clone(), contract.clone()),
                    Realm::Shared(label) if !label.trim().is_empty() => {
                        RealmKey::Shared(label.clone(), contract.clone())
                    }
                    Realm::Shared(_) => {
                        return Err(Error::InvalidConfig("shared realm must have a name".into()));
                    }
                };
                let realm = *prepared.realms.entry(key).or_insert_with(|| {
                    let service =
                        Service::<()>::new(contract.name.clone(), contract.revision.clone());
                    View::new().isolate(&service).realm(&service)
                });
                realms.insert(contract, realm);
            }
            let enabled = enabled && entry.enabled;
            let mut intercepts = inherited_intercepts.clone();
            let mut seen = BTreeSet::new();
            for intercept in &entry.intercept {
                let contract = ContractId::new(intercept.name.clone(), intercept.revision.clone());
                if intercept.name.trim().is_empty()
                    || intercept.revision.trim().is_empty()
                    || !seen.insert(contract.clone())
                {
                    return Err(Error::InvalidConfig(format!(
                        "entry {}: invalid or duplicate intercept",
                        entry.id
                    )));
                }
                intercepts
                    .entry(contract)
                    .or_default()
                    .push(intercept.config.clone());
            }
            prepared.intercepts.insert(
                entry.id.clone(),
                self.registry.intercept_config(&entry.id, &intercepts)?,
            );
            prepared.entries.push(ResolvedEntry {
                id: entry.id.clone(),
                source: entry.source.clone(),
                artifact: entry.artifact.clone(),
                config: entry.config.clone(),
                enabled,
                parent: parent.map(str::to_owned),
                realms: realms.clone(),
                intercepts: intercepts.clone(),
            });
            self.resolve(
                &entry.children,
                Some(&entry.id),
                &realms,
                &intercepts,
                enabled,
                depth + 1,
                ids,
                prepared,
            )?;
        }
        Ok(())
    }

    async fn apply(&mut self, config: Configuration) -> Result<ApplyReport> {
        // Rejected preparation leaves both the running graph and desired state intact.
        let prepared = self.prepare(&config).await?;
        let revision = {
            let mut state = self.state.lock().unwrap();
            state.desired_revision += 1;
            state.configuration = config;
            state.desired = prepared
                .entries
                .iter()
                .map(|entry| (entry.id.clone(), entry.clone()))
                .collect();
            state.phase = Phase::Applying;
            state.last_error = None;
            state.desired_revision
        };
        self.realms = prepared.realms;
        self.changed.send_replace(());
        let result = self
            .execute(
                revision,
                &prepared.entries,
                prepared.plugins,
                prepared.intercepts,
                &prepared.plan,
            )
            .await;
        let mut state = self.state.lock().unwrap();
        let result = match result {
            Ok((realms, ownership)) => {
                state.applied_revision = revision;
                state.phase = Phase::Idle;
                Ok(ApplyReport {
                    revision,
                    plan: prepared.plan,
                    snapshot: state.snapshot(),
                    preparation: prepared.stats,
                    realms,
                    ownership,
                })
            }
            Err(error) => {
                state.phase = Phase::Failed;
                state.last_error = Some(error.clone());
                Err(error)
            }
        };
        drop(state);
        self.changed.send_replace(());
        result
    }

    async fn execute(
        &self,
        revision: u64,
        entries: &[ResolvedEntry],
        mut plugins: BTreeMap<String, Plugin>,
        mut intercept_configs: BTreeMap<String, InterceptConfig>,
        plan: &Plan,
    ) -> Result<(RealmReport, Vec<OwnershipChange>)> {
        // Prepare new effect-free groups before any old parent is retired.
        // They are recorded as roots until the single ownership commit below;
        // if apply fails, the actual snapshot remains truthful and retryable.
        let mut staged = BTreeSet::new();
        for entry in entries
            .iter()
            .filter(|entry| entry.source == GROUP && plan.install.contains(&entry.id))
        {
            if self.state.lock().unwrap().actual.contains_key(&entry.id) {
                continue;
            }
            let mut ancestor = entry.parent.as_ref();
            let mut replacing_ancestor = false;
            while let Some(id) = ancestor {
                if plan.retire.contains(id) {
                    replacing_ancestor = true;
                    break;
                }
                ancestor = entries
                    .iter()
                    .find(|e| &e.id == id)
                    .unwrap()
                    .parent
                    .as_ref();
            }
            if replacing_ancestor {
                continue;
            }
            let mut root = entry.clone();
            root.parent = None;
            self.install_entry(revision, &root, &mut plugins, &mut intercept_configs)
                .await?;
            staged.insert(entry.id.clone());
        }
        let ownership_updates = {
            let state = self.state.lock().unwrap();
            entries
                .iter()
                .filter(|entry| plan.reparent.contains(&entry.id) || staged.contains(&entry.id))
                .map(|entry| {
                    let parent = entry
                        .parent
                        .as_ref()
                        .map(|id| {
                            state
                                .actual
                                .get(id)
                                .and_then(|a| a.handle.snapshot().activation)
                                .ok_or(cordis_runtime::Error::Closed)
                        })
                        .transpose()?;
                    Ok((state.actual[&entry.id].handle.clone(), parent))
                })
                .collect::<cordis_runtime::Result<Vec<_>>>()?
        };
        let ownership = self.runtime.reparent(&ownership_updates)?;
        {
            let mut state = self.state.lock().unwrap();
            for entry in entries
                .iter()
                .filter(|entry| plan.reparent.contains(&entry.id) || staged.contains(&entry.id))
            {
                let actual = state.actual.get_mut(&entry.id).unwrap();
                actual.entry.parent = entry.parent.clone();
                actual.ownership_revision = revision;
            }
        }
        let retiring: Vec<_> = {
            let state = self.state.lock().unwrap();
            plan.retire
                .iter()
                .map(|id| (id.clone(), state.actual[id].handle.clone()))
                .collect()
        };
        // Issue every retirement without awaiting one provider before its peers.
        // The kernel retains committed consumers and orders all actual recovery.
        let results = join_all(
            retiring
                .iter()
                .map(|(id, handle)| async move { (id.clone(), handle.dispose().await) }),
        )
        .await;
        let mut failures = Vec::new();
        let mut removed = Vec::new();
        {
            let mut state = self.state.lock().unwrap();
            for (id, result) in results {
                match result {
                    Ok(()) => removed.push(state.actual.remove(&id)),
                    Err(error) => failures.push(Failure { entry: id, error }),
                }
            }
        }
        drop(removed);
        if !failures.is_empty() {
            return Err(Error::Apply(failures));
        }

        let updates = {
            let state = self.state.lock().unwrap();
            entries
                .iter()
                .filter(|entry| plan.update_realms.contains(&entry.id))
                .map(|entry| (state.actual[&entry.id].handle.clone(), entry.view()))
                .collect::<Vec<_>>()
        };
        let realms = self.runtime.reconfigure_realms(&updates)?;
        {
            let mut state = self.state.lock().unwrap();
            for entry in entries
                .iter()
                .filter(|entry| plan.update_realms.contains(&entry.id))
            {
                let actual = state.actual.get_mut(&entry.id).unwrap();
                actual.entry.realms = entry.realms.clone();
                actual.realm_revision = revision;
            }
        }

        // Publish only after all old recovery succeeds. Each entry swaps a complete
        // map once; this is not a transaction across multiple entries.
        for entry in entries
            .iter()
            .filter(|entry| plan.update_intercepts.contains(&entry.id))
        {
            let source = self.state.lock().unwrap().actual[&entry.id]
                .intercepts
                .clone();
            source.replace(
                intercept_configs
                    .remove(&entry.id)
                    .unwrap()
                    .with_revision(revision),
            );
            let mut state = self.state.lock().unwrap();
            let actual = state.actual.get_mut(&entry.id).unwrap();
            actual.entry = entry.clone();
            actual.intercept_revision = revision;
        }

        for entry in entries
            .iter()
            .filter(|entry| plan.install.contains(&entry.id) && !staged.contains(&entry.id))
        {
            self.install_entry(revision, entry, &mut plugins, &mut intercept_configs)
                .await?;
        }

        Ok((realms, ownership))
    }

    async fn install_entry(
        &self,
        revision: u64,
        entry: &ResolvedEntry,
        plugins: &mut BTreeMap<String, Plugin>,
        intercept_configs: &mut BTreeMap<String, InterceptConfig>,
    ) -> Result<()> {
        let group_context = Arc::new(Mutex::new(None));
        let plugin = if entry.source == GROUP {
            let saved = group_context.clone();
            Plugin::new(format!("group:{}", entry.id), move |ctx| {
                let saved = saved.clone();
                async move {
                    *saved.lock().unwrap() = Some(ctx);
                    Ok(())
                }
            })
        } else {
            plugins.remove(&entry.id).unwrap()
        };
        let parent = {
            let state = self.state.lock().unwrap();
            entry
                .parent
                .as_ref()
                .and_then(|id| state.actual.get(id))
                .and_then(|actual| actual.group_context.clone())
        };
        let intercepts = Intercepts::new(
            intercept_configs
                .remove(&entry.id)
                .unwrap()
                .with_revision(revision),
        );
        let view = entry.view().with_intercepts(intercepts.clone());
        let installation = if let Some(parent) = parent {
            parent.derive(view).install(plugin)
        } else if entry.parent.is_some() {
            Err(cordis_runtime::Error::Closed)
        } else {
            self.runtime.install_in(view, plugin)
        };
        let handle = installation.map_err(|error| {
            Error::Apply(vec![Failure {
                entry: entry.id.clone(),
                error,
            }])
        })?;
        self.state.lock().unwrap().actual.insert(
            entry.id.clone(),
            Actual {
                entry: entry.clone(),
                revision,
                handle: handle.clone(),
                group_context: None,
                intercepts,
                intercept_revision: revision,
                realm_revision: revision,
                ownership_revision: revision,
            },
        );
        if entry.source == GROUP {
            // Built-in groups have no required services or user initializer.
            // Only groups are awaited here; consumers must be installed before
            // waiting so a provider later in the configuration can activate them.
            handle.ready().await.map_err(|error| {
                Error::Apply(vec![Failure {
                    entry: entry.id.clone(),
                    error,
                }])
            })?;
            self.state
                .lock()
                .unwrap()
                .actual
                .get_mut(&entry.id)
                .unwrap()
                .group_context = group_context.lock().unwrap().take();
        }
        Ok(())
    }
}
