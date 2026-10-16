use super::*;

/// A lifetime edge transfer, independent of realm and dependency provenance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnershipChange {
    pub fiber: FiberId,
    pub previous: Option<Activation>,
    pub next: Option<Activation>,
}

impl Runtime {
    /// Transfer subtree stopping responsibility in one coordinator transaction.
    /// A parent is an exact activation, never a name resolved after a restart.
    /// None makes the instance a Runtime root. No user code runs in this operation.
    pub fn reparent(
        &self,
        updates: &[(PluginHandle, Option<Activation>)],
    ) -> Result<Vec<OwnershipChange>> {
        self.check_handles(updates.iter().map(|(handle, _)| handle.clone()))?;
        let mut state = self.inner.state.lock().unwrap();
        let (kernel, changes) = self.prepare_ownership(&state, updates)?;
        state.kernel = kernel;
        drop(state);
        self.inner.notify();
        Ok(changes)
    }

    pub(super) fn prepare_ownership(
        &self,
        state: &State,
        updates: &[(PluginHandle, Option<Activation>)],
    ) -> Result<(Kernel, Vec<OwnershipChange>)> {
        if state.stopping {
            return Err(Error::RuntimeStopped);
        }
        let mut edges = BTreeMap::new();
        let mut changes = Vec::new();
        for (handle, parent) in updates {
            if edges.insert(handle.id, *parent).is_some() {
                return Err(Error::Message("duplicate ownership update".into()));
            }
            let previous = state.kernel.snapshot(handle.id)?.parent;
            if previous != *parent {
                changes.push(OwnershipChange {
                    fiber: handle.id,
                    previous,
                    next: *parent,
                });
            }
        }
        // Durable Broker resources participate in recovery just like committed
        // dependencies. Transient application call waits are not lifetime edges.
        let waits = self.durable_waits();
        let mut kernel = state.kernel.clone();
        kernel.reparent(&edges, &waits)?;
        Ok((kernel, changes))
    }

    pub(super) fn durable_waits(&self) -> Vec<(FiberId, FiberId)> {
        let pins = self.inner.retained.lock().unwrap();
        pins.iter()
            .filter_map(Weak::upgrade)
            .filter(|pin| pin.durable && pin.live())
            .map(|pin| (pin.provider.fiber, pin.owner.fiber))
            .collect()
    }
}
