use crate::{
    Context, EffectHandle, EffectSnapshot, Error, Result, WeakEffectHandle,
    effects::{Cleanup, Effect},
    protected,
};
use std::{
    future::Future,
    sync::{Arc, Mutex, Weak, atomic::Ordering},
};
use tokio::sync::watch;

/// The inverse returned by one completed effect step. On a step error, the step
/// must already have recovered anything for which it has not returned an inverse.
pub struct Inverse {
    label: String,
    cleanup: Cleanup,
}
impl Inverse {
    pub fn new<F, Fut>(label: impl Into<String>, cleanup: F) -> Self
    where
        F: FnOnce(Context) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        Self {
            label: label.into(),
            cleanup: Box::new(move |ctx| Box::pin(cleanup(ctx))),
        }
    }
}

/// A step can finish (or fail) while still yielding an inverse. Collect it first.
pub struct EffectStep {
    pub inverse: Option<Inverse>,
    pub completion: StepCompletion,
}
pub enum StepCompletion {
    Continue,
    Complete,
    Failed(Error),
}
impl EffectStep {
    pub fn yielded(inverse: Option<Inverse>) -> Self {
        Self {
            inverse,
            completion: StepCompletion::Continue,
        }
    }
    pub fn complete(inverse: Option<Inverse>) -> Self {
        Self {
            inverse,
            completion: StepCompletion::Complete,
        }
    }
    /// The inverse may represent an unresolved acquisition whose recovery fails.
    pub fn failed(error: Error, inverse: Option<Inverse>) -> Self {
        Self {
            inverse,
            completion: StepCompletion::Failed(error),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EffectCompletion {
    /// The producer completed. Its inverses remain owned until disposal.
    Completed,
    /// Disposal or lifecycle withdrawal prevented the next step from starting.
    Stopped,
}

struct State {
    armed: bool,
    inverses: Vec<Arc<Effect>>,
}
struct Sequence {
    state: Mutex<State>,
    completed: watch::Sender<Option<Result<EffectCompletion>>>,
}
impl Sequence {
    fn stop(&self) {
        self.state.lock().unwrap().armed = false;
    }
    async fn ready(&self) -> Result<EffectCompletion> {
        let mut completed = self.completed.subscribe();
        loop {
            if let Some(result) = completed.borrow().clone() {
                return result;
            }
            completed
                .changed()
                .await
                .map_err(|_| Error::RuntimeStopped)?;
        }
    }
    async fn recover(&self) -> Result<()> {
        self.stop();
        // Even an unsuccessful producer may have yielded inverses. A failure of
        // forward execution is not itself a failure to clean those contributions.
        let _ = self.ready().await;
        loop {
            let effect = self.state.lock().unwrap().inverses.last().cloned();
            let Some(effect) = effect else {
                return Ok(());
            };
            effect.run().await?;
            let released = self.state.lock().unwrap().inverses.pop();
            drop(released);
        }
    }
}

/// A scope-owned, executor-driven effect iterator. Dropping this handle or the
/// ready/dispose wait never drops an admitted step or its eventual inverse.
#[derive(Clone)]
pub struct EffectSequence {
    sequence: Arc<Sequence>,
    effect: EffectHandle,
}
impl EffectSequence {
    /// A registry reference that does not keep the owner alive.
    pub fn downgrade(&self) -> WeakEffectSequence {
        WeakEffectSequence {
            sequence: Arc::downgrade(&self.sequence),
            effect: self.effect.downgrade(),
        }
    }
    /// Wait for forward execution, not recovery. This wait can be cancelled.
    pub async fn ready(&self) -> Result<EffectCompletion> {
        self.sequence.ready().await
    }
    pub async fn dispose(&self) -> Result<()> {
        self.sequence.stop();
        self.effect.dispose().await
    }
    pub fn snapshot(&self) -> EffectSnapshot {
        self.effect.snapshot()
    }
    /// Pending and failed inverses, in acquisition order. Successful recovery
    /// removes entries; a failed inverse and all its predecessors remain visible.
    pub fn steps(&self) -> Vec<EffectSnapshot> {
        self.sequence
            .state
            .lock()
            .unwrap()
            .inverses
            .iter()
            .map(|effect| effect.snapshot())
            .collect()
    }
}

/// Registry reference that does not retain the owning scope.
#[derive(Clone)]
pub struct WeakEffectSequence {
    sequence: Weak<Sequence>,
    effect: WeakEffectHandle,
}
impl WeakEffectSequence {
    pub fn upgrade(&self) -> Option<EffectSequence> {
        Some(EffectSequence {
            sequence: self.sequence.upgrade()?,
            effect: self.effect.upgrade()?,
        })
    }
}

impl Context {
    /// Poll `next` one step at a time. `Some(inverse)` commits a step; `None`
    /// completes the iterator. Stop admission at step boundaries when disposed,
    /// the owner closes, or the defining service closes. Never cancel a step
    /// already admitted. Yielded inverses form one independently disposable group.
    ///
    /// Await `ready()` in plugin initialization to make step errors initialization
    /// errors. Otherwise handle that error and dispose the sequence explicitly;
    /// all yielded inverses remain registered for eventual scope recovery.
    /// Only returned inverses belong to this group; `defer`, `acquire`, or a nested
    /// `effect` on the step Context register separately in the owner's scope.
    pub fn effect<F, Fut>(&self, label: impl Into<String>, mut next: F) -> Result<EffectSequence>
    where
        F: FnMut(Context) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Option<Inverse>>> + Send + 'static,
    {
        self.effect_steps(label, move |ctx| {
            let step = next(ctx);
            async move {
                Ok(match step.await? {
                    Some(inverse) => EffectStep::yielded(Some(inverse)),
                    None => EffectStep::complete(None),
                })
            }
        })
    }

    /// General step result, including a final inverse or an inverse on failure.
    pub fn effect_steps<F, Fut>(&self, label: impl Into<String>, next: F) -> Result<EffectSequence>
    where
        F: FnMut(Context) -> Fut + Send + 'static,
        Fut: Future<Output = Result<EffectStep>> + Send + 'static,
    {
        self.snapshot().start_sequence(label.into(), next, None)
    }

    /// Register the producer's cleanup before admitting its first step. It runs
    /// after all yielded inverses, even when stopped before the first step.
    pub fn effect_steps_with_cleanup<F, Fut>(
        &self,
        label: impl Into<String>,
        next: F,
        cleanup: Inverse,
    ) -> Result<EffectSequence>
    where
        F: FnMut(Context) -> Fut + Send + 'static,
        Fut: Future<Output = Result<EffectStep>> + Send + 'static,
    {
        self.snapshot()
            .start_sequence(label.into(), next, Some(cleanup))
    }

    fn start_sequence<F, Fut>(
        &self,
        label: String,
        mut next: F,
        cleanup: Option<Inverse>,
    ) -> Result<EffectSequence>
    where
        F: FnMut(Context) -> Fut + Send + 'static,
        Fut: Future<Output = Result<EffectStep>> + Send + 'static,
    {
        if self.cleanup {
            return Err(Error::Closed);
        }
        let mut leases = self.call_leases()?;
        if self.definition.activation != self.owner.activation {
            leases.push(self.definition_lease()?);
        }
        let (completed, _) = watch::channel(None);
        let sequence = Arc::new(Sequence {
            state: Mutex::new(State {
                armed: true,
                inverses: cleanup
                    .into_iter()
                    .map(|inverse| {
                        Effect::new(
                            self.owner.next_effect.fetch_add(1, Ordering::Relaxed),
                            inverse.label,
                            self.clone(),
                            inverse.cleanup,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
            }),
            completed,
        });
        let cleanup = sequence.clone();
        let effect = self.defer(label, move |_| async move { cleanup.recover().await })?;
        // Use the group's reserved lifetime edges throughout production. A late
        // inverse must not need a new recovery-cycle decision after user work
        // has already incurred a cleanup responsibility.
        let mut production = self.clone();
        production.dynamic = effect.context.dynamic.clone();
        let (context, authority, pins) = production.operation_context()?;
        let handle = EffectSequence {
            sequence: sequence.clone(),
            effect,
        };
        self.executor().spawn(async move {
            let result = protected(async {
                loop {
                    // Disposal and admission share this lock. Gate admission
                    // also linearizes a step against lifecycle withdrawal.
                    let step_leases = {
                        let state = sequence.state.lock().unwrap();
                        if !state.armed {
                            return Ok(EffectCompletion::Stopped);
                        }
                        let Ok(owner) = context.owner.gate.enter(true, false) else {
                            return Ok(EffectCompletion::Stopped);
                        };
                        let mut admitted = vec![owner];
                        if context.definition.activation != context.owner.activation {
                            let Ok(definition) = context.definition.gate.enter(false, false) else {
                                return Ok(EffectCompletion::Stopped);
                            };
                            admitted.push(definition);
                        }
                        admitted
                    };
                    // Create an authority for this step so a saved step Context
                    // cannot become a permanent continuation of the iterator.
                    let (step_context, step_authority, step_pins) = context.operation_context()?;
                    let result = protected(async { next(step_context).await }).await;
                    step_authority.expire();
                    drop(step_pins);
                    let step = result?;
                    if let Some(inverse) = step.inverse {
                        let effect = Effect::new(
                            context.owner.next_effect.fetch_add(1, Ordering::Relaxed),
                            inverse.label,
                            context.clone(),
                            inverse.cleanup,
                        )?;
                        sequence.state.lock().unwrap().inverses.push(effect);
                    }
                    match step.completion {
                        StepCompletion::Continue => {}
                        StepCompletion::Complete => return Ok(EffectCompletion::Completed),
                        StepCompletion::Failed(error) => return Err(error),
                    }
                    drop(step_leases);
                    tokio::task::yield_now().await;
                }
            })
            .await;
            authority.expire();
            drop(pins);
            sequence.completed.send_replace(Some(result));
            drop(leases);
        });
        Ok(handle)
    }
}
