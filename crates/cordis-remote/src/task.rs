use crate::{Error, Result, Value, json, session::WeakSession};
use cordis_runtime::{Context, TaskHandle};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Default)]
pub(crate) struct Tasks {
    entries: Mutex<BTreeMap<String, Entry>>,
}
struct Entry {
    grant: String,
    handle: TaskHandle,
    state: Arc<Mutex<State>>,
}
struct State {
    phase: &'static str,
    outcome: Option<Result<Value>>,
}
impl Tasks {
    pub fn register(&self, ctx: &Context, payload: &Value, session: WeakSession) -> Result<Value> {
        let id = crate::scope::identifier(payload, "task", 't')?;
        let label = crate::scope::text(payload, "label")?;
        if label.len() > 1024 {
            return Err(Error::InvalidInput("task label too long".into()));
        }
        let mut entries = self.entries.lock().unwrap();
        if entries.contains_key(&id) {
            return Err(Error::InvalidInput("duplicate task ID".into()));
        }
        if entries.len() >= 4096
            || entries
                .values()
                .filter(|e| e.state.lock().unwrap().phase == "running")
                .count()
                >= 16
        {
            return Err(Error::Overloaded);
        }
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).map_err(|e| Error::Io(e.to_string()))?;
        let grant: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        let state = Arc::new(Mutex::new(State {
            phase: "running",
            outcome: None,
        }));
        let task_state = state.clone();
        let task_id = id.clone();
        let handle = ctx
            .spawn(label, move |ctx| async move {
                let reply = execute(&ctx, &task_id, session).await;
                let (phase, result) = match reply {
                    Ok(value) => match crate::scope::parse_outcome(value) {
                        Ok(result) => ("completed", result),
                        Err(error) => ("unknown", Err(error)),
                    },
                    Err(error) if crate::scope::not_executed(&error) => {
                        ("not_executed", Err(error))
                    }
                    Err(error) => ("unknown", Err(error)),
                };
                let result = if phase == "unknown" {
                    let failure =
                        Error::OutcomeUnknown(format!("task {task_id}: {:?}", result.err()));
                    let cleanup = failure.clone();
                    // Place uncertainty above every contribution made by this task.
                    // Killing a process is not evidence that its effects were reversed.
                    ctx.defer("unknown remote task", move |_| async move {
                        Err(cleanup.into())
                    })?;
                    ctx.fail(failure.to_string())?;
                    Err(failure)
                } else {
                    result
                };
                *task_state.lock().unwrap() = State {
                    phase,
                    outcome: Some(result.clone()),
                };
                if phase == "not_executed" {
                    Ok(())
                } else {
                    result.map(|_| ()).map_err(Into::into)
                }
            })
            .map_err(crate::resource::runtime_error)?;
        entries.insert(
            id.clone(),
            Entry {
                grant: grant.clone(),
                handle,
                state,
            },
        );
        Ok(json!({"task":id,"grant":grant}))
    }
    pub async fn invoke(&self, command: &str, payload: &Value) -> Result<Value> {
        let id = crate::scope::text(payload, "task")?;
        let grant = crate::scope::text(payload, "grant")?;
        let (handle, state) = {
            let entries = self.entries.lock().unwrap();
            let entry = entries
                .get(&id)
                .filter(|e| e.grant == grant)
                .ok_or(Error::Closed)?;
            (entry.handle.clone(), entry.state.clone())
        };
        match command {
            "task_join" => {
                let native = handle.join().await;
                let result = state.lock().unwrap().outcome.clone();
                result.unwrap_or_else(|| {
                    native
                        .map(|()| json!({}))
                        .map_err(crate::resource::runtime_error)
                })
            }
            "task_status" => {
                let state = state.lock().unwrap();
                Ok(
                    json!({"state":state.phase,"outcome":state.outcome.clone().map(crate::dependency::outcome)}),
                )
            }
            _ => Err(Error::InvalidInput("unknown task operation".into())),
        }
    }
}

async fn execute(ctx: &Context, id: &str, session: WeakSession) -> Result<Value> {
    let session = session
        .upgrade()
        .ok_or_else(|| Error::HostLost("task host no longer exists".into()))?;
    let running = session.scoped_request(ctx, "task_call", json!({"task":id}));
    tokio::pin!(running);
    tokio::select! {
        biased;
        reply = &mut running => reply,
        _ = ctx.cancelled() => {
            // Cancellation travels on its own lane while the task retains its leases.
            let stopping = async {
                session.request("task_cancel", json!({"task":id})).await?;
                running.await
            };
            match tokio::time::timeout(session.task_stop_timeout(), stopping).await {
                Ok(Ok(reply)) => Ok(reply),
                other => {
                    let failure = Error::OutcomeUnknown(format!("task {id} did not confirm exit: {other:?}"));
                    // Session publishes failure after killing/reaping, before joining
                    // reverse task_join waiters, avoiding a shutdown wait cycle.
                    let _ = session.abort(failure.clone()).await;
                    Err(failure)
                },
            }
        },
    }
}
