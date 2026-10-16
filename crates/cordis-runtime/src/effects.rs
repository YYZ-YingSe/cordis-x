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
    status: EffectStatus,
    pins: Option<crate::retention::Pins>,
}

pub(crate) struct Effect {
    id: u64,
    label: String,
    state: Mutex<State>,
    result: watch::Sender<Option<Result<()>>>,
    context: Context,
}

impl Effect {
    pub fn new(id: u64, label: String, context: Context, cleanup: Cleanup) -> Result<Arc<Self>> {
        let (context, pins) = context.snapshot().retain_dynamic(true)?;
        let (result, _) = watch::channel(None);
        Ok(Arc::new(Self {
            id,
            label,
            context,
            state: Mutex::new(State {
                cleanup: Some(cleanup),
                status: EffectStatus::Owned,
                pins: Some(pins),
            }),
            result,
        }))
    }

    pub(crate) fn context(&self) -> &Context {
        &self.context
    }

    pub fn snapshot(&self) -> EffectSnapshot {
        EffectSnapshot {
            id: self.id,
            label: self.label.clone(),
            status: self.state.lock().unwrap().status.clone(),
        }
    }

    pub async fn run(self: &Arc<Self>) -> Result<()> {
        let mut result_rx = self.result.subscribe();
        let cleanup = {
            let mut state = self.state.lock().unwrap();
            let cleanup = state.cleanup.take();
            if cleanup.is_some() {
                state.status = EffectStatus::Releasing;
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
                    effect.state.lock().unwrap().pins.take();
                } else if let Err(error) = &result {
                    for pin in &effect.context.dynamic {
                        *pin.failure.lock().unwrap() = Some(error.clone());
                    }
                }
                effect.state.lock().unwrap().status = match &result {
                    Ok(()) => EffectStatus::Released,
                    Err(error) => EffectStatus::Failed(error.clone()),
                };
                effect.result.send_replace(Some(result));
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

/// Manual disposal affects only this registration. Scope shutdown also observes its result.
#[derive(Clone)]
pub struct EffectHandle {
    pub(crate) effect: Arc<Effect>,
    pub(crate) context: Context,
}

impl EffectHandle {
    /// A registry can address an effect without keeping its owning scope alive.
    pub fn downgrade(&self) -> WeakEffectHandle {
        WeakEffectHandle(Arc::downgrade(&self.effect))
    }
    pub fn snapshot(&self) -> EffectSnapshot {
        self.effect.snapshot()
    }
    pub async fn dispose(&self) -> Result<()> {
        let lease = self.context.enter_owner();
        // During shutdown, only the ordered scope recovery may start a pending inverse.
        if lease.is_err() && self.effect.snapshot().status == EffectStatus::Owned {
            return Err(Error::Closed);
        }
        self.effect.run().await
    }
}

#[derive(Clone)]
pub struct WeakEffectHandle(Weak<Effect>);

impl WeakEffectHandle {
    pub fn upgrade(&self) -> Option<EffectHandle> {
        let effect = self.0.upgrade()?;
        Some(EffectHandle {
            context: effect.context.owned_context(),
            effect,
        })
    }
}
