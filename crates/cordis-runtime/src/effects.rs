use crate::{BoxFuture, Context, Error, Result, protected};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::watch;

pub(crate) type Cleanup = Box<dyn FnOnce(Context) -> BoxFuture<'static, Result<()>> + Send>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EffectStatus {
    Owned,
    Releasing,
    Released,
    Failed(Error),
}

#[derive(Clone, Debug)]
pub struct EffectSnapshot {
    pub id: u64,
    pub label: String,
    pub status: EffectStatus,
}

struct State {
    cleanup: Option<Cleanup>,
    pins: Option<crate::retention::Pins>,
    capacity: Vec<tokio::sync::OwnedSemaphorePermit>,
    recovery: Option<Arc<dyn Send + Sync>>,
}

pub(crate) struct Effect {
    observation: Arc<Observation>,
    state: Mutex<State>,
    context: Context,
}

// Contains no Context, user closure or code lease. Registries may retain a
// terminal observation without retaining the owner or its implementation.
struct Observation {
    id: u64,
    label: String,
    status: Mutex<EffectStatus>,
    result: watch::Sender<Option<Result<()>>>,
}
impl Observation {
    fn snapshot(&self) -> EffectSnapshot {
        EffectSnapshot {
            id: self.id,
            label: self.label.clone(),
            status: self.status.lock().unwrap().clone(),
        }
    }
}

impl Effect {
    pub fn new(
        id: u64,
        label: String,
        context: Context,
        cleanup: Cleanup,
        capacity: Vec<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<Arc<Self>> {
        let (context, pins) = context.snapshot().retain_dynamic(true)?;
        let (result, _) = watch::channel(None);
        Ok(Arc::new(Self {
            observation: Arc::new(Observation {
                id,
                label,
                status: Mutex::new(EffectStatus::Owned),
                result,
            }),
            context,
            state: Mutex::new(State {
                cleanup: Some(cleanup),
                pins: Some(pins),
                capacity,
                recovery: None,
            }),
        }))
    }

    pub fn snapshot(&self) -> EffectSnapshot {
        self.observation.snapshot()
    }

    pub async fn run(self: &Arc<Self>) -> Result<()> {
        let mut result_rx = self.observation.result.subscribe();
        let cleanup = {
            let mut state = self.state.lock().unwrap();
            let cleanup = state.cleanup.take();
            if cleanup.is_some() {
                *self.observation.status.lock().unwrap() = EffectStatus::Releasing;
            }
            cleanup
        };
        if let Some(cleanup) = cleanup {
            let effect = self.clone();
            let context = self.context.clone();
            // The executor retains the operation even if the waiting caller is dropped.
            context.executor().clone().spawn(async move {
                let (context, authority, barrier) = context.cleanup_context();
                let result = protected(async move { cleanup(context).await }).await;
                authority.expire();
                barrier.close();
                // A cancelled wait inside an inverse must not let the next inverse
                // destroy resources still used by its admitted continuations.
                barrier.drained().await;
                barrier.finish();
                if result.is_ok() {
                    let mut state = effect.state.lock().unwrap();
                    state.pins.take();
                    state.capacity.clear();
                    let recovered = state.recovery.take();
                    drop(state);
                    drop(recovered);
                } else if let Err(error) = &result {
                    for pin in &effect.context.dynamic {
                        *pin.failure.lock().unwrap() = Some(error.clone());
                    }
                }
                *effect.observation.status.lock().unwrap() = match &result {
                    Ok(()) => EffectStatus::Released,
                    Err(error) => EffectStatus::Failed(error.clone()),
                };
                if result.is_ok() {
                    // Remove exactly this recovered registration. Other inverses
                    // retain their order and failed recovery remains a barrier.
                    let retired = effect
                        .context
                        .owner
                        .effects
                        .lock()
                        .unwrap()
                        .remove(&effect.observation.id);
                    drop(retired);
                }
                effect.observation.result.send_replace(Some(result));
                if let Some(inner) = effect.context.inner.upgrade() {
                    inner.notify();
                }
            });
        }
        loop {
            if let Some(result) = result_rx.borrow().clone() {
                return result;
            }
            result_rx
                .changed()
                .await
                .map_err(|_| Error::RuntimeStopped)?;
        }
    }
}

/// An owned observation of one registration. Completed observations do not
/// need to keep an owning activation alive to return the same cleanup result.
#[derive(Clone)]
pub struct EffectHandle {
    effect: Option<Arc<Effect>>,
    observation: Arc<Observation>,
}
impl EffectHandle {
    pub(crate) fn retain_recovery(&self, recovery: Arc<dyn Send + Sync>) {
        self.effect
            .as_ref()
            .expect("live effect")
            .state
            .lock()
            .unwrap()
            .recovery = Some(recovery);
    }
    pub(crate) fn new(effect: Arc<Effect>) -> Self {
        Self {
            observation: effect.observation.clone(),
            effect: Some(effect),
        }
    }
    pub(crate) fn context(&self) -> Option<Context> {
        self.effect.as_ref().map(|e| e.context.owned_context())
    }
    pub fn downgrade(&self) -> WeakEffectHandle {
        WeakEffectHandle {
            effect: self.effect.as_ref().map(Arc::downgrade).unwrap_or_default(),
            observation: self.observation.clone(),
        }
    }
    pub fn snapshot(&self) -> EffectSnapshot {
        self.observation.snapshot()
    }
    pub async fn dispose(&self) -> Result<()> {
        if let Some(result) = self.observation.result.borrow().clone() {
            return result;
        }
        let effect = self.effect.as_ref().ok_or(Error::Closed)?;
        let context = effect.context.owned_context();
        let lease = context.enter_owner();
        if lease.is_err() && self.snapshot().status == EffectStatus::Owned {
            return Err(Error::Closed);
        }
        effect.run().await
    }
}

/// Weak access to live responsibility, with an independent terminal result.
#[derive(Clone)]
pub struct WeakEffectHandle {
    effect: Weak<Effect>,
    observation: Arc<Observation>,
}
impl WeakEffectHandle {
    /// True only after the inverse and all admitted cleanup work have drained.
    /// Failure and a missing live pointer are never evidence of recovery.
    pub fn is_released(&self) -> bool {
        matches!(*self.observation.result.borrow(), Some(Ok(())))
    }
    pub fn upgrade(&self) -> Option<EffectHandle> {
        let effect = self.effect.upgrade();
        if effect.is_none() && self.observation.result.borrow().is_none() {
            return None;
        }
        Some(EffectHandle {
            effect,
            observation: self.observation.clone(),
        })
    }
}
