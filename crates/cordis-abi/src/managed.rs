//! Native sequence/scope/task ownership, with bounded weak registries.
use crate::{
    Error, Instance, Result, Value,
    context::{Router, next_id},
    ffi::Reply,
    json,
};
use cordis_runtime::{
    Context, EffectCompletion, EffectStatus, EffectStep, Inverse, TaskHandle, WeakEffectSequence,
    WeakScopeHandle,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, OnceLock, atomic::Ordering},
};
use tokio::sync::{Semaphore, watch};

#[derive(Clone, Debug, Default)]
pub struct ManagedLimits {
    pub sequences: Option<usize>,
    pub steps: Option<usize>,
    pub scopes: Option<usize>,
    pub scope_runs: Option<usize>,
    pub tasks: Option<usize>,
    pub active_sequences: Option<usize>,
    pub active_tasks: Option<usize>,
}
impl ManagedLimits {
    pub(crate) fn validate(&self) -> Result<()> {
        if self
            .steps
            .is_some_and(|n| n > (cordis_wire::MAX_INTEGER as usize).min(Semaphore::MAX_PERMITS))
        {
            return Err(Error::InvalidInput(
                "step capacity exceeds portable integers".into(),
            ));
        }
        Ok(())
    }
}
pub(crate) fn active() -> Option<(u32, u64)> {
    crate::context::active_callback()
}
fn self_wait(operation: u32, callback: u64) -> Result<()> {
    if active() == Some((operation, callback)) {
        return Err(Error::Refused {
            code: "self_wait".into(),
            message: "callback cannot wait for itself".into(),
        });
    }
    Ok(())
}
#[derive(Default)]
struct Registry {
    scopes_sweep: cordis_wire::retention::Sweep,
    tasks_sweep: cordis_wire::retention::Sweep,
    sequences_sweep: cordis_wire::retention::Sweep,
    sequences: BTreeMap<u64, Arc<Sequence>>,
    scopes: BTreeMap<u64, WeakScopeHandle>,
    tasks: BTreeMap<u64, Arc<Task>>,
    callbacks: BTreeSet<u64>,
    runs: BTreeMap<u64, u64>,
}
pub(crate) struct Managed {
    entries: Mutex<Registry>,
    steps: Arc<tokio::sync::Semaphore>,
    runs: Arc<Semaphore>,
    waits: Semaphore,
    disposals: Semaphore,
    joins: Semaphore,
}
impl Managed {
    pub fn new(capacity: usize, steps: usize) -> Self {
        Self {
            entries: Mutex::default(),
            steps: Arc::new(Semaphore::new(steps)),
            runs: Arc::new(Semaphore::new(capacity)),
            waits: Semaphore::new(capacity),
            disposals: Semaphore::new(capacity),
            joins: Semaphore::new(capacity),
        }
    }
    pub fn recovered(&self) -> bool {
        let entries = self.entries.lock().unwrap();
        entries
            .sequences
            .values()
            .all(|entry| matches!(*entry.released.lock().unwrap(), Some(Ok(()))))
            && entries
                .tasks
                .values()
                .all(|entry| matches!(*entry.outcome.lock().unwrap(), Some(Ok(_))))
            && entries.scopes.values().all(|weak| {
                weak.upgrade()
                    .is_none_or(|h| h.snapshot().state == cordis_kernel::Lifecycle::Inactive)
            })
    }
}
struct Sequence {
    step_sweep: cordis_wire::retention::Sweep,
    callback: u64,
    handle: OnceLock<WeakEffectSequence>,
    forward: watch::Sender<Option<Result<Value>>>,
    forward_error: Mutex<Option<Error>>,
    cleanup_error: Mutex<Option<Error>>,
    released: Mutex<Option<Result<()>>>,
    steps: Mutex<Vec<Arc<StepTicket>>>,
    next_step: std::sync::atomic::AtomicU64,
}
struct StepTicket {
    number: u64,
    state: Mutex<&'static str>,
}
struct Task {
    callback: u64,
    handle: OnceLock<TaskHandle>,
    outcome: Mutex<Option<Result<Value>>>,
    observed: std::sync::atomic::AtomicBool,
}

impl Router {
    /// Observe managed state without entering the mutation dispatcher. Query
    /// capacity and self-wait rules still apply; dropping this future releases
    /// only its observation permit, never the underlying lifecycle handle.
    pub(crate) async fn observe_managed(&self, kind: u32, target: u64) -> Result<Reply> {
        if !self.instance.upgrade().is_some_and(|i| i.module.managed()) {
            return Err(Error::Closed);
        }
        if crate::context::in_cleanup() && matches!(kind, 2..=4) {
            return Err(Error::Refused {
                code: "cleanup_wait".into(),
                message: "blocking managed observations during cleanup are unsupported".into(),
            });
        }
        let value = match kind {
            2 | 10 => self.task_observe(kind == 10, target).await?,
            3 | 8 => self.sequence_observe(kind == 8, target).await?,
            4 | 9 => self.scope_observe(kind == 9, target).await?,
            _ => return Err(Error::InvalidInput("unknown managed observation".into())),
        };
        Ok(Reply {
            value: value.into(),
            handle: 0,
        })
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn manage(
        self: &Arc<Self>,
        grant: u64,
        operation: u32,
        target: u64,
        callback: u64,
        label: String,
        _args: Value,
    ) -> Result<Reply> {
        if !self.instance.upgrade().is_some_and(|i| i.module.managed()) {
            return Err(Error::Closed);
        }
        if crate::context::in_cleanup() && matches!(operation, 3 | 7 | 8) {
            return Err(Error::Refused {
                code: "cleanup_wait".into(),
                message: "blocking managed operations during cleanup are unsupported".into(),
            });
        }
        if label.len() > 1024 {
            return Err(Error::InvalidInput("managed label too long".into()));
        }
        let result = match operation {
            1 => {
                return Ok(Reply {
                    value: Value::Null.into(),
                    handle: self.sequence_register(grant, callback, label)?,
                });
            }
            5 => {
                return Ok(Reply {
                    value: Value::Null.into(),
                    handle: self.scope_create(grant, label)?,
                });
            }
            10 => {
                return Ok(Reply {
                    value: Value::Null.into(),
                    handle: self.task_spawn(grant, callback, label)?,
                });
            }
            3 => self.sequence_dispose(target).await?,
            7 | 8 => self.scope_action(operation, target, callback).await?,
            _ => return Err(Error::InvalidInput("unknown managed operation".into())),
        };
        Ok(Reply {
            value: result.into(),
            handle: 0,
        })
    }
    fn sequence_register(&self, grant: u64, callback: u64, label: String) -> Result<u64> {
        let ctx = self.context(grant)?;
        let instance = self.instance.upgrade().ok_or(Error::Closed)?;
        let mut entries = self.managed.entries.lock().unwrap();
        check_callback(&entries, callback)?;
        let limits = &self.capabilities.managed;
        if entries.sequences_sweep.due(entries.sequences.len())
            || limits
                .sequences
                .is_some_and(|limit| entries.sequences.len() >= limit)
        {
            let retired: Vec<_> = entries
                .sequences
                .iter()
                .filter(|(_, e)| e.handle.get().is_some_and(WeakEffectSequence::is_released))
                .map(|(id, e)| (*id, e.callback))
                .collect();
            for (id, callback) in retired {
                entries.sequences.remove(&id);
                entries.callbacks.remove(&callback);
            }
            entries.sequences_sweep.retained(entries.sequences.len());
        }
        if limits
            .sequences
            .is_some_and(|limit| entries.sequences.len() >= limit)
            || limits
                .active_sequences
                .into_iter()
                .chain(self.capabilities.max_contexts)
                .min()
                .is_some_and(|limit| {
                    entries
                        .sequences
                        .values()
                        .filter(|e| e.forward.borrow().is_none())
                        .count()
                        >= limit
                })
        {
            return Err(Error::Overloaded);
        }
        let id = next_id()?;
        let (forward, _) = watch::channel(None);
        let entry = Arc::new(Sequence {
            step_sweep: Default::default(),
            callback,
            handle: OnceLock::new(),
            forward,
            forward_error: Mutex::default(),
            cleanup_error: Mutex::default(),
            released: Mutex::default(),
            steps: Mutex::default(),
            next_step: std::sync::atomic::AtomicU64::new(1),
        });
        let runner = entry.clone();
        let producer = entry.clone();
        let release_instance = instance.clone();
        let total = self.managed.steps.clone();
        let limit = limits.steps;
        let handle = ctx.effect_steps_with_cleanup(
            label,
            move |ctx| {
                let entry = runner.clone();
                let instance = instance.clone();
                let total = total.clone();
                async move { Ok(entry.next(ctx, instance, total, limit).await) }
            },
            Inverse::new("ABI sequence producer", move |cleanup| async move {
                let result = release_instance
                    .managed_call(cleanup, 10, callback, Value::Null)
                    .await
                    .map(|_| ());
                *producer.released.lock().unwrap() = Some(result.clone());
                result.map_err(Into::into)
            }),
        )?;
        let _ = entry.handle.set(handle.downgrade());
        let completion = entry.clone();
        tokio::spawn(async move {
            let result=handle.ready().await.map(|state| json!({"completion":match state { EffectCompletion::Completed=>"completed", EffectCompletion::Stopped=>"stopped" }}))
                .map_err(|e| completion.forward_error.lock().unwrap().clone().unwrap_or_else(|| e.into()));
            completion.forward.send_replace(Some(result));
        });
        entries.callbacks.insert(callback);
        entries.sequences.insert(id, entry);
        Ok(id)
    }
    fn sequence(&self, id: u64) -> Result<Arc<Sequence>> {
        self.managed
            .entries
            .lock()
            .unwrap()
            .sequences
            .get(&id)
            .cloned()
            .ok_or(Error::Closed)
    }
    async fn sequence_observe(&self, snapshot: bool, id: u64) -> Result<Value> {
        let entry = self.sequence(id)?;
        if snapshot {
            let native = entry.handle.get().and_then(WeakEffectSequence::upgrade);
            let status = native
                .as_ref()
                .map(|h| effect_status(h.snapshot().status))
                .unwrap_or_else(|| {
                    if entry.cleanup_error.lock().unwrap().is_some()
                        || entry
                            .released
                            .lock()
                            .unwrap()
                            .as_ref()
                            .is_some_and(Result::is_err)
                    {
                        "failed"
                    } else {
                        "released"
                    }
                });
            let steps: Vec<_> = entry
                .steps
                .lock()
                .unwrap()
                .iter()
                .map(|s| json!({"step":s.number,"state":*s.state.lock().unwrap()}))
                .collect();
            return Ok(
                json!({"state":status,"forward":entry.forward.borrow().as_ref().map(outcome),"steps":steps}),
            );
        }
        self_wait(8, entry.callback)?;
        let _wait = self
            .managed
            .waits
            .try_acquire()
            .map_err(|_| Error::Overloaded)?;
        let mut rx = entry.forward.subscribe();
        loop {
            if let Some(result) = rx.borrow().clone() {
                return result;
            }
            rx.changed().await.map_err(|_| Error::Closed)?;
        }
    }
    async fn sequence_dispose(&self, id: u64) -> Result<Value> {
        let entry = self.sequence(id)?;
        self_wait(8, entry.callback)?;
        let _wait = self
            .managed
            .disposals
            .try_acquire()
            .map_err(|_| Error::Overloaded)?;
        if let Some(handle) = entry.handle.get().and_then(WeakEffectSequence::upgrade) {
            if let Err(error) = handle.dispose().await {
                return Err(entry
                    .cleanup_error
                    .lock()
                    .unwrap()
                    .clone()
                    .or_else(|| {
                        entry
                            .released
                            .lock()
                            .unwrap()
                            .as_ref()
                            .and_then(|r| r.clone().err())
                    })
                    .unwrap_or_else(|| error.into()));
            }
        } else {
            if let Some(error) = entry.cleanup_error.lock().unwrap().clone() {
                return Err(error);
            }
            entry
                .released
                .lock()
                .unwrap()
                .clone()
                .unwrap_or(Err(Error::Closed))?;
        }
        Ok(Value::Null)
    }
    fn scope_create(&self, grant: u64, label: String) -> Result<u64> {
        let ctx = self.context(grant)?;
        let mut entries = self.managed.entries.lock().unwrap();
        if entries.scopes_sweep.due(entries.scopes.len())
            || self
                .capabilities
                .managed
                .scopes
                .is_some_and(|limit| entries.scopes.len() >= limit)
        {
            entries.scopes.retain(|_, e| !e.is_retired());
            entries.scopes_sweep.retained(entries.scopes.len());
        }
        if self
            .capabilities
            .managed
            .scopes
            .is_some_and(|limit| entries.scopes.len() >= limit)
        {
            return Err(Error::Overloaded);
        }
        let id = next_id()?;
        let handle = ctx.scope(label)?; // Includes native Broker/install restriction.
        entries.scopes.insert(id, handle.downgrade());
        Ok(id)
    }
    fn scope_handle(&self, id: u64) -> Result<cordis_runtime::ScopeHandle> {
        self.managed
            .entries
            .lock()
            .unwrap()
            .scopes
            .get(&id)
            .and_then(WeakScopeHandle::upgrade)
            .ok_or(Error::Closed)
    }
    fn check_scope_wait(&self, id: u64) -> Result<()> {
        if let Some((11, running)) = active()
            && self.managed.entries.lock().unwrap().runs.get(&running) == Some(&id)
        {
            return Err(Error::Refused {
                code: "self_wait".into(),
                message: "scope cannot close itself from run".into(),
            });
        }
        Ok(())
    }
    async fn scope_observe(&self, snapshot: bool, id: u64) -> Result<Value> {
        let handle = self.scope_handle(id)?;
        if snapshot {
            let snapshot = handle.snapshot();
            return Ok(
                json!({"state":format!("{:?}",snapshot.state).to_lowercase(),"retired":snapshot.retired,"error":snapshot.outcome.map(|e|format!("{e:?}")),"effects":handle.effects().into_iter().map(|e|json!({"label":e.label,"state":effect_status(e.status)})).collect::<Vec<_>>()}),
            );
        }
        self.check_scope_wait(id)?;
        let _wait = self
            .managed
            .waits
            .try_acquire()
            .map_err(|_| Error::Overloaded)?;
        handle.ready().await?;
        Ok(Value::Null)
    }
    async fn scope_action(
        self: &Arc<Self>,
        operation: u32,
        id: u64,
        callback: u64,
    ) -> Result<Value> {
        let handle = self.scope_handle(id)?;
        if operation == 7 {
            let permit = self
                .managed
                .runs
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::Overloaded)?;
            {
                let mut entries = self.managed.entries.lock().unwrap();
                check_callback(&entries, callback)?;
                if self
                    .capabilities
                    .managed
                    .scope_runs
                    .is_some_and(|limit| entries.runs.len() >= limit)
                {
                    return Err(Error::Overloaded);
                }
                entries.callbacks.insert(callback);
                entries.runs.insert(callback, id);
            }
            let instance = self.instance.upgrade().ok_or(Error::Closed)?;
            let router = self.clone();
            let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let started = entered.clone();
            let result = handle
                .run(move |ctx| async move {
                    started.store(true, Ordering::Release);
                    let _permit = permit;
                    let result = instance
                        .managed_call(ctx.clone(), 11, callback, Value::Null)
                        .await;
                    if let Err(error @ Error::Unknown(_)) = &result {
                        unknown(&ctx, error.clone())?;
                    }
                    if !matches!(result, Err(Error::Unknown(_))) {
                        let mut entries = router.managed.entries.lock().unwrap();
                        entries.runs.remove(&callback);
                        entries.callbacks.remove(&callback);
                    }
                    Ok(result)
                })
                .await;
            if !entered.load(Ordering::Acquire) {
                let mut entries = self.managed.entries.lock().unwrap();
                entries.runs.remove(&callback);
                entries.callbacks.remove(&callback);
            }
            return result?;
        }
        self.check_scope_wait(id)?;
        let _wait = self
            .managed
            .disposals
            .try_acquire()
            .map_err(|_| Error::Overloaded)?;
        handle.dispose().await?;
        Ok(Value::Null)
    }
    fn task_spawn(&self, grant: u64, callback: u64, label: String) -> Result<u64> {
        let ctx = self.context(grant)?;
        let instance = self.instance.upgrade().ok_or(Error::Closed)?;
        let mut entries = self.managed.entries.lock().unwrap();
        check_callback(&entries, callback)?;
        let limits = &self.capabilities.managed;
        if entries.tasks_sweep.due(entries.tasks.len())
            || limits
                .tasks
                .is_some_and(|limit| entries.tasks.len() >= limit)
        {
            let retired: Vec<_> = entries
                .tasks
                .iter()
                .filter(|(_, e)| {
                    e.observed.load(Ordering::Acquire)
                        && e.handle
                            .get()
                            .is_some_and(|h| matches!(h.outcome(), Some(Ok(()))))
                })
                .map(|(id, e)| (*id, e.callback))
                .collect();
            for (id, callback) in retired {
                entries.tasks.remove(&id);
                entries.callbacks.remove(&callback);
            }
            entries.tasks_sweep.retained(entries.tasks.len());
        }
        if limits
            .tasks
            .is_some_and(|limit| entries.tasks.len() >= limit)
            || limits
                .active_tasks
                .into_iter()
                .chain(self.capabilities.max_contexts)
                .min()
                .is_some_and(|limit| {
                    entries
                        .tasks
                        .values()
                        .filter(|e| e.outcome.lock().unwrap().is_none())
                        .count()
                        >= limit
                })
        {
            return Err(Error::Overloaded);
        }
        let id = next_id()?;
        let entry = Arc::new(Task {
            callback,
            handle: OnceLock::new(),
            outcome: Mutex::default(),
            observed: std::sync::atomic::AtomicBool::new(false),
        });
        let task = entry.clone();
        let handle = ctx.spawn(label, move |ctx| async move {
            let result = instance
                .managed_call(ctx.clone(), 12, callback, Value::Null)
                .await;
            let not_run = matches!(
                result,
                Err(Error::Closed | Error::Overloaded | Error::InvalidInput(_))
            );
            *task.outcome.lock().unwrap() = Some(result.clone());
            if let Err(error @ Error::Unknown(_)) = &result {
                unknown(&ctx, error.clone())?;
            }
            if not_run {
                Ok(())
            } else {
                result.map(|_| ()).map_err(Into::into)
            }
        })?;
        let _ = entry.handle.set(handle);
        entries.callbacks.insert(callback);
        entries.tasks.insert(id, entry);
        Ok(id)
    }
    async fn task_observe(&self, snapshot: bool, id: u64) -> Result<Value> {
        let entry = self
            .managed
            .entries
            .lock()
            .unwrap()
            .tasks
            .get(&id)
            .cloned()
            .ok_or(Error::Closed)?;
        if snapshot {
            let result = entry.outcome.lock().unwrap();
            return Ok(
                json!({"state":match result.as_ref() { None=>"running", Some(Err(Error::Unknown(_)))=>"unknown", Some(Err(Error::Closed|Error::Overloaded|Error::InvalidInput(_)))=>"not_executed", _=>"completed" },"outcome":result.as_ref().map(outcome)}),
            );
        }
        self_wait(12, entry.callback)?;
        let _wait = self
            .managed
            .joins
            .try_acquire()
            .map_err(|_| Error::Overloaded)?;
        let native = entry.handle.get().ok_or(Error::Closed)?.join().await;
        entry.observed.store(true, Ordering::Release);
        entry
            .outcome
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| native.map(|_| Value::Null).map_err(Into::into))
    }
}
impl Sequence {
    async fn next(
        self: Arc<Self>,
        ctx: Context,
        instance: Arc<Instance>,
        total: Arc<tokio::sync::Semaphore>,
        limit: Option<usize>,
    ) -> EffectStep {
        let permit = match total.try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => return self.fail(Error::Overloaded, None),
        };
        let number = match self
            .next_step
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n <= cordis_wire::MAX_INTEGER as u64).then(|| n + 1)
            }) {
            Ok(number) => number,
            Err(_) => {
                return self.fail(
                    Error::Refused {
                        code: "exhausted".into(),
                        message: "sequence identity space exhausted".into(),
                    },
                    None,
                );
            }
        };
        let ticket = {
            let mut steps = self.steps.lock().unwrap();
            if self.step_sweep.due(steps.len()) || limit.is_some_and(|n| steps.len() >= n) {
                steps.retain(|t| {
                    matches!(
                        *t.state.lock().unwrap(),
                        "pending" | "owned" | "unknown" | "failed"
                    )
                });
                self.step_sweep.retained(steps.len());
            }
            let ticket = Arc::new(StepTicket {
                number,
                state: Mutex::new("pending"),
            });
            steps.push(ticket.clone());
            ticket
        };
        let result = instance
            .managed_call(ctx.clone(), 8, self.callback, json!(ticket.number))
            .await
            .and_then(parse_step);
        match result {
            Ok((done, label, error)) => {
                *ticket.state.lock().unwrap() = if label.is_some() {
                    "owned"
                } else {
                    "no_effect"
                };
                let entry = self.clone();
                let inverse = label.map(|label| {
                    Inverse::new(label, move |ctx| async move {
                        let result = instance
                            .managed_call(ctx, 9, entry.callback, json!(ticket.number))
                            .await
                            .map(|_| ());
                        *ticket.state.lock().unwrap() =
                            if result.is_ok() { "released" } else { "failed" };
                        if let Err(error) = &result {
                            *entry.cleanup_error.lock().unwrap() = Some(error.clone());
                        }
                        result.map_err(Into::into)
                    })
                    .retain_capacity(permit)
                });
                if let Some(error) = error {
                    self.fail(error, inverse)
                } else if done {
                    EffectStep::complete(inverse)
                } else {
                    EffectStep::yielded(inverse)
                }
            }
            Err(error) => {
                if matches!(
                    error,
                    Error::Closed | Error::Overloaded | Error::InvalidInput(_)
                ) {
                    *ticket.state.lock().unwrap() = "not_executed";
                    return self.fail(error, None);
                }
                *ticket.state.lock().unwrap() = "unknown";
                let error = Error::Unknown(format!("ABI sequence step {}: {error}", ticket.number));
                instance.fault(error.clone());
                let _ = ctx.fail(error.to_string());
                let failure = error.clone();
                let entry = self.clone();
                self.fail(
                    error,
                    Some(
                        Inverse::new("unknown ABI step", move |_| async move {
                            *entry.cleanup_error.lock().unwrap() = Some(failure.clone());
                            Err(failure.into())
                        })
                        .retain_capacity(permit),
                    ),
                )
            }
        }
    }
    fn fail(&self, error: Error, inverse: Option<Inverse>) -> EffectStep {
        *self.forward_error.lock().unwrap() = Some(error.clone());
        EffectStep::failed(error.into(), inverse)
    }
}
fn check_callback(entries: &Registry, callback: u64) -> Result<()> {
    if callback == 0 || entries.callbacks.contains(&callback) {
        return Err(Error::InvalidInput(
            "invalid or reused managed callback".into(),
        ));
    }
    Ok(())
}
fn unknown(ctx: &Context, error: Error) -> Result<()> {
    let failure = error.clone();
    ctx.defer("unknown ABI managed callback", move |_| async move {
        Err(failure.into())
    })?;
    ctx.fail(error.to_string())?;
    Ok(())
}
fn outcome(result: &Result<Value>) -> Value {
    match result {
        Ok(value) => json!({"kind":"success","value":value}),
        Err(error) => json!({"kind":"failure","error":error.to_string()}),
    }
}
fn effect_status(status: EffectStatus) -> &'static str {
    match status {
        EffectStatus::Owned => "owned",
        EffectStatus::Releasing => "releasing",
        EffectStatus::Released => "released",
        EffectStatus::Failed(_) => "failed",
    }
}
fn parse_step(value: Value) -> Result<(bool, Option<String>, Option<Error>)> {
    let invalid = || Error::Unknown("invalid ABI sequence step".into());
    if value.as_object().is_none_or(|o| o.len() != 3) {
        return Err(invalid());
    }
    let done = value["done"].as_bool().ok_or_else(invalid)?;
    let label = match value.get("inverse") {
        Some(Value::Null) => None,
        Some(Value::String(s)) if s.len() <= 1024 => Some(s.clone()),
        _ => return Err(invalid()),
    };
    let error = match value.get("error") {
        Some(Value::Null) => None,
        Some(Value::Object(o)) if done && o.len() == 2 => Some(Error::Rejected {
            code: o
                .get("code")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .ok_or_else(invalid)?
                .into(),
            message: o
                .get("message")
                .and_then(Value::as_str)
                .ok_or_else(invalid)?
                .into(),
        }),
        _ => return Err(invalid()),
    };
    Ok((done, label, error))
}
