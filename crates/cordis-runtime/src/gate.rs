use crate::{Error, Result};
use std::sync::Arc;
mod admission;
use admission::Admission;
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    Loading,
    Active,
    Closing,
    Recovering,
    Closed,
}

/// Admission and closing share one lock. A lease is registered before closing can observe zero.
#[derive(Debug)]
pub(crate) struct Gate {
    state: Admission,
    changed: Notify,
}

impl Gate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Admission::new(),
            changed: Notify::new(),
        })
    }

    pub fn phase(&self) -> Phase {
        self.state.phase()
    }

    pub fn activate(&self) {
        self.state.activate();
    }
    pub fn close(&self) {
        self.state.close();
        self.changed.notify_waiters();
    }
    pub fn recover(&self) {
        self.state.recover();
    }
    pub fn finish(&self) {
        self.state.finish();
        self.changed.notify_waiters();
    }
    pub fn enter(self: &Arc<Self>, allow_loading: bool, continuation: bool) -> Result<Lease> {
        self.state.enter(allow_loading, continuation, false)?;
        Ok(Lease(self.clone()))
    }
    pub fn enter_cleanup(self: &Arc<Self>) -> Result<Lease> {
        self.state.enter(false, false, true)?;
        Ok(Lease(self.clone()))
    }

    pub async fn drained(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.count() == 0 {
                return;
            }
            notified.await;
        }
    }

    pub async fn cancelled(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if matches!(
                self.phase(),
                Phase::Closing | Phase::Recovering | Phase::Closed
            ) {
                return;
            }
            notified.await;
        }
    }
}

pub(crate) struct Lease(Arc<Gate>);
impl Drop for Lease {
    fn drop(&mut self) {
        self.0.state.release();
        self.0.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    #[test]
    fn admission_racing_with_close_is_counted_or_rejected() {
        // Real-thread race coverage; this is not an exhaustive Loom proof.
        for _ in 0..128 {
            let gate = Gate::new();
            gate.activate();
            let barrier = Arc::new(Barrier::new(5));
            std::thread::scope(|scope| {
                let mut tasks = Vec::new();
                for _ in 0..4 {
                    let gate = gate.clone();
                    let barrier = barrier.clone();
                    tasks.push(scope.spawn(move || {
                        barrier.wait();
                        gate.enter(false, false)
                    }));
                }
                barrier.wait();
                gate.close();
                let leases: Vec<_> = tasks
                    .into_iter()
                    .filter_map(|task| task.join().unwrap().ok())
                    .collect();
                assert_eq!(gate.state.count(), leases.len());
                assert!(matches!(gate.enter(false, false), Err(Error::Closed)));
                drop(leases);
            });
            assert_eq!(gate.state.count(), 0);
            gate.recover();
            gate.finish();
            assert!(matches!(gate.enter_cleanup(), Err(Error::Closed)));
        }
    }
}
