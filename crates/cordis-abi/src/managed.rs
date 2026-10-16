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
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Semaphore, watch};

#[derive(Clone, Debug)]
pub struct ManagedLimits {
    pub sequences: usize,
    pub steps: usize,
    pub scopes: usize,
    pub scope_runs: usize,
    pub tasks: usize,
    pub active_sequences: usize,
    pub active_tasks: usize,
}
impl Default for ManagedLimits {
    fn default() -> Self {
        Self {
            sequences: 256,
            steps: 4096,
            scopes: 256,
            scope_runs: 4096,
            tasks: 4096,
            active_sequences: 16,
            active_tasks: 16,
        }
    }
}
impl ManagedLimits {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.steps > cordis_wire::MAX_INTEGER as usize {
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
    sequences: BTreeMap<u64, Arc<Sequence>>,
    scopes: BTreeMap<u64, WeakScopeHandle>,
    tasks: BTreeMap<u64, Arc<Task>>,
    callbacks: BTreeSet<u64>,
    runs: BTreeMap<u64, u64>,
}
pub(crate) struct Managed {
    entries: Mutex<Registry>,
    steps: Arc<AtomicUsize>,
    runs: Arc<Semaphore>,
    waits: Semaphore,
    disposals: Semaphore,
    joins: Semaphore,
}
impl Managed {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: Mutex::default(),
            steps: Arc::default(),
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
    callback: u64,
    handle: OnceLock<WeakEffectSequence>,
    forward: watch::Sender<Option<Result<Value>>>,
    forward_error: Mutex<Option<Error>>,
    cleanup_error: Mutex<Option<Error>>,
    released: Mutex<Option<Result<()>>>,
    steps: Mutex<Vec<Arc<StepTicket>>>,
}
struct StepTicket {
    number: usize,
    state: Mutex<&'static str>,
}
struct Task {
    callback: u64,
    handle: OnceLock<TaskHandle>,
    outcome: Mutex<Option<Result<Value>>>,
}

impl Router {
    #[allow(clippy::too_many_arguments)]
    pub async fn manage(
        self: &Arc<Self>,
        grant: u64,
        operation: u32,
        target: u64,
        callback: u64,
        label: String,
        args: Value,
    ) -> Result<Reply> {
        if !self.instance.upgrade().is_some_and(|i| i.module.managed()) {
            return Err(Error::Closed);
        }
        if crate::context::in_cleanup() && matches!(operation, 2 | 3 | 6 | 7 | 8 | 11 | 13) {
            return Err(Error::Refused {
                code: "cleanup_wait".into(),
                message: "blocking managed operations during cleanup are unsupported".into(),
            });
        }
        if label.len() > 1024 {
            return Err(Error::InvalidInput("managed label too long".into()));
        }
        let result = match operation {
            14 => {
                let ctx = self.context(grant)?.snapshot();
                let revision = args
                    .as_str()
                    .filter(|s| !s.is_empty() && s.len() <= 1024)
                    .ok_or_else(|| Error::InvalidInput("invalid metadata revision".into()))?;
                if label.is_empty() || target != 0 || callback != 0 {
                    return Err(Error::InvalidInput("invalid metadata request".into()));
                }
                let service = cordis_runtime::Service::<()>::new(label, revision);
                let contract = service.contract();
                let instance = self.instance.upgrade().ok_or(Error::Closed)?;
                if &instance.module.contract != contract
                    && !self
                        .capabilities
                        .dependencies
                        .iter()
                        .any(|d| d.contract() == contract)
                    && !self
                        .capabilities
                        .provisions
                        .iter()
                        .any(|p| p.service.contract() == contract)
                {
                    return Err(Error::Refused {
                        code: "undeclared_metadata".into(),
                        message: contract.to_string(),
                    });
                }
                let layers = ctx.metadata::<_, Value>(&service)?;
                json!({"revision":ctx.metadata_revision().map(|r|r.to_string()),
                    "layers":layers.iter().map(|v|v.as_ref()).collect::<Vec<_>>()})
            }
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
            2..=4 => self.sequence_action(operation, target).await?,
            6..=9 => self.scope_action(operation, target, callback).await?,
            11 | 12 => self.task_action(operation, target).await?,
            13 => {
                let ctx = self.task_context(grant)?;
                if args.is_null() {
                    ctx.cancelled().await;
                    json!(true)
                } else {
                    let millis = args
                        .as_u64()
                        .filter(|n| *n <= 86_400_000)
                        .ok_or_else(|| Error::InvalidInput("invalid cancellation wait".into()))?;
                    json!(
                        tokio::time::timeout(Duration::from_millis(millis), ctx.cancelled())
                            .await
                            .is_ok()
                    )
                }
            }
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
        if entries.sequences.len() >= limits.sequences
            || entries
                .sequences
                .values()
                .filter(|e| e.forward.borrow().is_none())
                .count()
                >= limits.active_sequences.min(self.capabilities.max_contexts)
        {
            return Err(Error::Overloaded);
        }
        let id = next_id()?;
        let (forward, _) = watch::channel(None);
        let entry = Arc::new(Sequence {
            callback,
            handle: OnceLock::new(),
            forward,
            forward_error: Mutex::default(),
            cleanup_error: Mutex::default(),
            released: Mutex::default(),
            steps: Mutex::default(),
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
    async fn sequence_action(&self, operation: u32, id: u64) -> Result<Value> {
        let entry = self
            .managed
            .entries
            .lock()
            .unwrap()
            .sequences
            .get(&id)
            .cloned()
            .ok_or(Error::Closed)?;
        if operation == 4 {
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
        let capacity = if operation == 2 {
            &self.managed.waits
        } else {
            &self.managed.disposals
        };
        let _wait = capacity.try_acquire().map_err(|_| Error::Overloaded)?;
        if operation == 2 {
            let mut rx = entry.forward.subscribe();
            loop {
                if let Some(result) = rx.borrow().clone() {
                    return result;
                }
                rx.changed().await.map_err(|_| Error::Closed)?;
            }
        }
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
        if entries.scopes.len() >= self.capabilities.managed.scopes {
            return Err(Error::Overloaded);
        }
        let id = next_id()?;
        let handle = ctx.scope(label)?; // Includes native Broker/install restriction.
        entries.scopes.insert(id, handle.downgrade());
        Ok(id)
    }
    async fn scope_action(
        self: &Arc<Self>,
        operation: u32,
        id: u64,
        callback: u64,
    ) -> Result<Value> {
        let handle = self
            .managed
            .entries
            .lock()
            .unwrap()
            .scopes
            .get(&id)
            .and_then(WeakScopeHandle::upgrade)
            .ok_or(Error::Closed)?;
        if operation == 9 {
            let snapshot = handle.snapshot();
            return Ok(
                json!({"state":format!("{:?}",snapshot.state).to_lowercase(),"retired":snapshot.retired,"error":snapshot.outcome.map(|e|format!("{e:?}")),"effects":handle.effects().into_iter().map(|e|json!({"label":e.label,"state":effect_status(e.status)})).collect::<Vec<_>>()}),
            );
        }
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
                if entries.runs.len() >= self.capabilities.managed.scope_runs {
                    return Err(Error::Overloaded);
                }
                entries.callbacks.insert(callback);
                entries.runs.insert(callback, id);
            }
            let instance = self.instance.upgrade().ok_or(Error::Closed)?;
            return handle
                .run(move |ctx| async move {
                    let _permit = permit;
                    let result = instance
                        .managed_call(ctx.clone(), 11, callback, Value::Null)
                        .await;
                    if let Err(error @ Error::Unknown(_)) = &result {
                        unknown(&ctx, error.clone())?;
                    }
                    Ok(result)
                })
                .await?;
        }
        if let Some((11, running)) = active()
            && self.managed.entries.lock().unwrap().runs.get(&running) == Some(&id)
        {
            return Err(Error::Refused {
                code: "self_wait".into(),
                message: "scope cannot close itself from run".into(),
            });
        }
        let capacity = if operation == 6 {
            &self.managed.waits
        } else {
            &self.managed.disposals
        };
        let _wait = capacity.try_acquire().map_err(|_| Error::Overloaded)?;
        if operation == 6 {
            handle.ready().await?;
        } else {
            handle.dispose().await?;
        }
        Ok(Value::Null)
    }
    fn task_spawn(&self, grant: u64, callback: u64, label: String) -> Result<u64> {
        let ctx = self.context(grant)?;
        let instance = self.instance.upgrade().ok_or(Error::Closed)?;
        let mut entries = self.managed.entries.lock().unwrap();
        check_callback(&entries, callback)?;
        let limits = &self.capabilities.managed;
        if entries.tasks.len() >= limits.tasks
            || entries
                .tasks
                .values()
                .filter(|e| e.outcome.lock().unwrap().is_none())
                .count()
                >= limits.active_tasks.min(self.capabilities.max_contexts)
        {
            return Err(Error::Overloaded);
        }
        let id = next_id()?;
        let entry = Arc::new(Task {
            callback,
            handle: OnceLock::new(),
            outcome: Mutex::default(),
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
    async fn task_action(&self, operation: u32, id: u64) -> Result<Value> {
        let entry = self
            .managed
            .entries
            .lock()
            .unwrap()
            .tasks
            .get(&id)
            .cloned()
            .ok_or(Error::Closed)?;
        if operation == 12 {
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
        total: Arc<AtomicUsize>,
        limit: usize,
    ) -> EffectStep {
        if total
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < limit).then(|| n + 1)
            })
            .is_err()
        {
            return self.fail(Error::Overloaded, None);
        }
        let ticket = {
            let mut steps = self.steps.lock().unwrap();
            let ticket = Arc::new(StepTicket {
                number: steps.len() + 1,
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
                    Some(Inverse::new("unknown ABI step", move |_| async move {
                        *entry.cleanup_error.lock().unwrap() = Some(failure.clone());
                        Err(failure.into())
                    })),
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
