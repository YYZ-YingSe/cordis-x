use super::*;
use crate::view::{RealmMap, RealmValues};
use cordis_kernel::RealmSlots;

/// A preview of namespace and declaration changes, including dynamic descendants.
/// A changed dependency address requires resolution, not necessarily a restart.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RealmChange {
    pub fiber: FiberId,
    pub previous: RealmMap,
    pub next: RealmMap,
    pub previous_slots: RealmSlots,
    pub next_slots: RealmSlots,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RealmReport {
    pub changes: Vec<RealmChange>,
    /// Activations actually withdrawn, including dependent/ownership cascades.
    pub withdrawn: Vec<Activation>,
}

struct Prepared {
    origins: BTreeMap<FiberId, View>,
    values: RealmValues,
    slots: BTreeMap<FiberId, RealmSlots>,
    changes: Vec<RealmChange>,
}

impl Runtime {
    pub(super) fn check_handles(&self, handles: impl Iterator<Item = PluginHandle>) -> Result<()> {
        if handles
            .into_iter()
            .any(|handle| !Arc::ptr_eq(&self.inner, &handle.inner))
        {
            return Err(Error::Message(
                "configuration handle belongs to another runtime".into(),
            ));
        }
        Ok(())
    }

    /// Validate the final declaration graph against unmanaged instances too.
    /// This is a preview; commit rechecks current state and reserves nothing here.
    pub fn validate_reconfiguration(
        &self,
        retiring: &[PluginHandle],
        additions: &[(View, Plugin)],
        updates: &[(PluginHandle, View)],
        ownership: &[(PluginHandle, Option<Activation>)],
    ) -> Result<Vec<RealmChange>> {
        self.check_handles(
            retiring
                .iter()
                .cloned()
                .chain(updates.iter().map(|(h, _)| h.clone()))
                .chain(ownership.iter().map(|(h, _)| h.clone())),
        )?;
        let state = self.inner.state.lock().unwrap();
        let prepared = state.prepare_realms(updates)?;
        let (mut kernel, _) = self.prepare_ownership(&state, ownership)?;
        let removing = retiring.iter().map(|handle| handle.id).collect();
        let specs: Vec<_> = additions
            .iter()
            .map(|(view, plugin)| FiberSpec {
                name: plugin.name.clone(),
                parent: None,
                requires: plugin.requires.iter().map(|c| view.slot(c)).collect(),
                provides: plugin.provides.iter().map(|c| view.slot(c)).collect(),
            })
            .collect();
        kernel.validate_reconfiguration(&removing, &specs, &prepared.slots)?;
        // With no removals/additions, preview the complete transition too: old
        // unloading reservations can conflict even when the final graph does not.
        if retiring.is_empty() && additions.is_empty() {
            kernel.reconfigure_realms(&prepared.slots)?;
            kernel.validate_recovery_order(&self.durable_waits())?;
        }
        Ok(prepared.changes)
    }

    /// Change realm resolution without changing code or ownership. All managed
    /// views publish together after the kernel transaction has been validated.
    pub fn reconfigure_realms(&self, updates: &[(PluginHandle, View)]) -> Result<RealmReport> {
        self.check_handles(updates.iter().map(|(h, _)| h.clone()))?;
        let mut state = self.inner.state.lock().unwrap();
        let prepared = state.prepare_realms(updates)?;
        let mut kernel = state.kernel.clone();
        let actions = kernel.reconfigure_realms(&prepared.slots)?;
        kernel.validate_recovery_order(&self.durable_waits())?;
        state.kernel = kernel;
        let withdrawn = actions
            .iter()
            .filter_map(|action| match action {
                Action::Withdraw(a) => Some(*a),
                _ => None,
            })
            .collect();
        for (id, origin) in prepared.origins {
            state.installed.get_mut(&id).unwrap().origin = origin;
        }
        // Withdraw gates before exposing the new namespace. Calls already
        // admitted retain their own snapshot and immutable dependency bindings.
        for activation in &withdrawn {
            // A previously waiting consumer can start and be withdrawn in the
            // same reconciliation batch. Its gate is created by Start below.
            if let Some(data) = state.activations.get(activation) {
                data.gate.close();
            }
        }
        state.realms.publish(prepared.values);
        self.inner.execute_locked(&mut state, actions);
        drop(state);
        self.inner.notify();
        Ok(RealmReport {
            changes: prepared.changes,
            withdrawn,
        })
    }
}

impl State {
    fn prepare_realms(&self, updates: &[(PluginHandle, View)]) -> Result<Prepared> {
        if self.stopping {
            return Err(Error::RuntimeStopped);
        }
        if updates.is_empty() {
            return Ok(Prepared {
                origins: BTreeMap::new(),
                values: (*self.realms.snapshot()).clone(),
                slots: BTreeMap::new(),
                changes: Vec::new(),
            });
        }
        let mut requested = BTreeMap::new();
        for (handle, view) in updates {
            let snapshot = self.kernel.snapshot(handle.id)?;
            if snapshot.retired {
                return Err(Error::Retired(handle.id));
            }
            if requested.insert(handle.id, view.realm_map()).is_some() {
                return Err(Error::Message("duplicate realm update".into()));
            }
        }
        let previous = self.realms.snapshot();
        let mut values = (*previous).clone();
        let mut origins = BTreeMap::new();
        // A live source already existed when its child was installed, so source
        // IDs precede derived IDs. Explicit overrides and fixed snapshots break
        // inheritance; no ancestry or equality-of-realm heuristic is needed.
        for (&id, installed) in &self.installed {
            if self.kernel.snapshot(id)?.retired {
                continue;
            }
            let origin = match requested.get(&id) {
                Some(realms) => installed.origin.clone().with_realms(realms.clone()),
                None => installed.origin.clone(),
            };
            values.insert(id, origin.resolve_realms(&self.realms, &values));
            if requested.contains_key(&id) {
                origins.insert(id, origin);
            }
        }
        let mut slots = BTreeMap::new();
        let mut changes = Vec::new();
        for (&id, installed) in &self.installed {
            if self.kernel.snapshot(id)?.retired {
                continue;
            }
            let view = installed.view.clone().with_realms(values[&id].clone());
            let mut requires: BTreeSet<_> = installed
                .plugin
                .requires
                .iter()
                .map(|c| view.slot(c))
                .collect();
            if let Some(inheritance) = &installed.inheritance {
                let source = inheritance
                    .view
                    .clone()
                    .with_realms(inheritance.view.resolve_realms(&self.realms, &values));
                for contract in &inheritance.contracts {
                    if !installed.plugin.requires.contains(contract)
                        && view.slot(contract) == source.slot(contract)
                    {
                        requires.insert(view.slot(contract));
                    }
                }
            }
            let next_slots = RealmSlots {
                requires,
                provides: installed
                    .plugin
                    .provides
                    .iter()
                    .map(|c| view.slot(c))
                    .collect(),
            };
            let previous_slots = self.kernel.slots(id)?;
            if next_slots != previous_slots {
                slots.insert(id, next_slots.clone());
            }
            if previous[&id] != values[&id] || previous_slots != next_slots {
                changes.push(RealmChange {
                    fiber: id,
                    previous: previous[&id].clone(),
                    next: values[&id].clone(),
                    previous_slots,
                    next_slots,
                });
            }
        }
        Ok(Prepared {
            origins,
            values,
            slots,
            changes,
        })
    }
}
