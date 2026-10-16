use crate::{Error, Result, Value, json, session::WeakSession};
use cordis_runtime::{
    Context, EffectCompletion, EffectStatus, EffectStep, Inverse, WeakEffectSequence,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::watch;

const MAX_SEQUENCES: usize = 256;
const MAX_STEPS: usize = 4096;
#[derive(Default)]
pub(crate) struct Sequences {
    entries: Mutex<BTreeMap<String, Arc<Entry>>>,
    steps: Arc<AtomicUsize>,
}
struct Entry {
    id: String,
    grant: String,
    handle: OnceLock<WeakEffectSequence>,
    forward: watch::Sender<Option<Result<Value>>>,
    forward_error: Mutex<Option<Error>>,
    cleanup_error: Mutex<Option<Error>>,
    released: Mutex<Option<Result<Value>>>,
    steps: Mutex<Vec<Arc<Ticket>>>,
}
struct Ticket {
    number: String,
    state: Mutex<Value>,
}
impl Ticket {
    fn set(&self, state: &str, error: Option<&Error>) {
        *self.state.lock().unwrap() =
            json!({"step":self.number,"state":state,"error":error.map(ToString::to_string)});
    }
}
impl Sequences {
    pub fn register(&self, ctx: &Context, payload: &Value, session: WeakSession) -> Result<Value> {
        let id = text(payload, "sequence")?;
        let label = text(payload, "label")?;
        if !id.strip_prefix('s').is_some_and(|n| {
            !n.is_empty() && n.len() <= 20 && n.bytes().all(|b| b.is_ascii_digit())
        }) || label.len() > 1024
        {
            return Err(Error::InvalidInput("invalid sequence ID or label".into()));
        }
        let mut entries = self.entries.lock().unwrap();
        if entries.contains_key(&id) {
            return Err(Error::InvalidInput("duplicate sequence ID".into()));
        }
        if entries.len() >= MAX_SEQUENCES {
            return Err(Error::Overloaded);
        }
        let mut random = [0u8; 16];
        getrandom::fill(&mut random).map_err(|e| Error::Io(e.to_string()))?;
        let grant: String = random.iter().map(|b| format!("{b:02x}")).collect();
        let (forward, _) = watch::channel(None);
        let entry = Arc::new(Entry {
            id: id.clone(),
            grant: grant.clone(),
            handle: OnceLock::new(),
            forward,
            forward_error: Mutex::new(None),
            cleanup_error: Mutex::new(None),
            released: Mutex::new(None),
            steps: Mutex::new(Vec::new()),
        });
        let runner = entry.clone();
        let total = self.steps.clone();
        let release_entry = entry.clone();
        let release_session = session.clone();
        let handle = ctx
            .effect_steps_with_cleanup(
                label,
                move |ctx| {
                    let entry = runner.clone();
                    let session = session.clone();
                    let total = total.clone();
                    async move { Ok(entry.next(ctx, session, total).await) }
                },
                Inverse::new("remote sequence producer", move |ctx| async move {
                    let result = request(
                        &release_session,
                        &ctx,
                        "sequence_release",
                        json!({"sequence":release_entry.id}),
                    )
                    .await;
                    *release_entry.released.lock().unwrap() = Some(result.clone());
                    result.map(|_| ()).map_err(Into::into)
                }),
            )
            .map_err(crate::resource::runtime_error)?;
        let _ = entry.handle.set(handle.downgrade());
        let completion = entry.clone();
        // This temporary strong handle lasts only through forward execution.
        // The registry and callbacks never retain a scope through a strong cycle.
        tokio::spawn(async move {
            let result = match handle.ready().await {
                Ok(value) => Ok(
                    json!({"completion":match value { EffectCompletion::Completed => "completed", EffectCompletion::Stopped => "stopped" }}),
                ),
                Err(error) => Err(completion
                    .forward_error
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or_else(|| crate::resource::runtime_error(error))),
            };
            completion.forward.send_replace(Some(result));
        });
        entries.insert(id.clone(), entry);
        Ok(json!({"sequence":id,"grant":grant}))
    }
    fn entry(&self, payload: &Value) -> Result<Arc<Entry>> {
        let id = text(payload, "sequence")?;
        let grant = text(payload, "grant")?;
        let entry = self
            .entries
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or(Error::Closed)?;
        if entry.grant != grant {
            return Err(Error::Closed);
        }
        Ok(entry)
    }
    pub async fn invoke(&self, command: &str, payload: &Value) -> Result<Value> {
        let entry = self.entry(payload)?;
        match command {
            "sequence_ready" => {
                let mut rx = entry.forward.subscribe();
                loop {
                    if let Some(result) = rx.borrow().clone() {
                        return result;
                    }
                    rx.changed().await.map_err(|_| Error::Closed)?;
                }
            }
            "sequence_dispose" => {
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
                            .unwrap_or_else(|| crate::resource::runtime_error(error)));
                    }
                    return Ok(json!({}));
                }
                // Recover consumes the native producer closure. On failure its
                // sequence state can disappear while the owner's failed group
                // and our SDK tickets still retain the cleanup responsibility.
                if let Some(error) = entry.cleanup_error.lock().unwrap().clone() {
                    return Err(error);
                }
                entry
                    .released
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or(Err(Error::Closed))
            }
            "sequence_status" => {
                let state = entry
                    .handle
                    .get()
                    .and_then(WeakEffectSequence::upgrade)
                    .map(|h| h.snapshot().status);
                let state = match state {
                    Some(EffectStatus::Owned) => "owned",
                    Some(EffectStatus::Releasing) => "releasing",
                    Some(EffectStatus::Failed(_)) => "failed",
                    Some(EffectStatus::Released) => "released",
                    None => {
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
                    }
                };
                let steps: Vec<_> = entry
                    .steps
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|t| t.state.lock().unwrap().clone())
                    .collect();
                let completion = entry
                    .forward
                    .borrow()
                    .clone()
                    .map(crate::dependency::outcome);
                Ok(json!({"state":state,"steps":steps,"forward":completion}))
            }
            _ => Err(Error::InvalidInput("unknown sequence operation".into())),
        }
    }
}
impl Entry {
    async fn next(
        self: Arc<Self>,
        ctx: Context,
        session: WeakSession,
        total: Arc<AtomicUsize>,
    ) -> EffectStep {
        if total
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < MAX_STEPS).then_some(n + 1)
            })
            .is_err()
        {
            return self.fail(Error::Overloaded, None);
        }
        // Record responsibility before sending a step. The native sequence lease
        // prevents recovery until this ticket has landed, including unknown results.
        let ticket = {
            let mut steps = self.steps.lock().unwrap();
            let number = (steps.len() + 1).to_string();
            let ticket = Arc::new(Ticket {
                number,
                state: Mutex::new(Value::Null),
            });
            ticket.set("pending", None);
            steps.push(ticket.clone());
            ticket
        };
        let response = request(
            &session,
            &ctx,
            "sequence_next",
            json!({"sequence":self.id,"step":ticket.number}),
        )
        .await;
        let parsed = response.and_then(parse_step);
        match parsed {
            Ok((done, label, error)) => {
                ticket.set(
                    if label.is_some() {
                        "owned"
                    } else {
                        "no_effect"
                    },
                    error.as_ref(),
                );
                let inverse = label.map(|label| {
                    let id = self.id.clone();
                    let cleanup_entry = self.clone();
                    let ticket = ticket.clone();
                    Inverse::new(label, move |ctx| async move {
                        let result = request(
                            &session,
                            &ctx,
                            "sequence_undo",
                            json!({"sequence":id,"step":ticket.number}),
                        )
                        .await;
                        if let Err(error) = &result {
                            *cleanup_entry.cleanup_error.lock().unwrap() = Some(error.clone());
                        }
                        ticket.set(
                            if result.is_ok() { "released" } else { "failed" },
                            result.as_ref().err(),
                        );
                        result.map(|_| ()).map_err(Into::into)
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
                // Only bounded admission rejections certify no step ran. Any
                // other failure (including malformed replies) is conservatively unknown.
                let not_run = error == Error::Overloaded
                    || matches!(&error, Error::Refused { code, .. } if matches!(code.as_str(), "overloaded" | "closed" | "session_exhausted" | "steps_exhausted"));
                if not_run {
                    ticket.set("not_executed", Some(&error));
                    return self.fail(error, None);
                }
                ticket.set("unknown", Some(&error));
                let unknown = Error::OutcomeUnknown(format!(
                    "sequence {} step {}: {error}",
                    self.id, ticket.number
                ));
                let failure = unknown.clone();
                let cleanup_entry = self.clone();
                let inverse = Inverse::new(
                    format!("unknown remote step {}", ticket.number),
                    move |_| async move {
                        *cleanup_entry.cleanup_error.lock().unwrap() = Some(failure.clone());
                        Err(failure.into())
                    },
                );
                self.fail(unknown, Some(inverse))
            }
        }
    }
    fn fail(&self, error: Error, inverse: Option<Inverse>) -> EffectStep {
        *self.forward_error.lock().unwrap() = Some(error.clone());
        EffectStep::failed(error.into(), inverse)
    }
}
async fn request(
    session: &WeakSession,
    ctx: &Context,
    command: &str,
    payload: Value,
) -> Result<Value> {
    session
        .upgrade()
        .ok_or_else(|| Error::HostLost("sequence host no longer exists".into()))?
        .scoped_request(ctx, command, payload)
        .await
}
fn parse_step(value: Value) -> Result<(bool, Option<String>, Option<Error>)> {
    let invalid = || Error::Protocol("invalid sequence step result".into());
    if value.as_object().is_none_or(|v| v.len() != 3) {
        return Err(invalid());
    }
    let done = value["done"].as_bool().ok_or_else(invalid)?;
    let label = match value.get("inverse") {
        Some(Value::Null) => None,
        Some(Value::String(label)) if label.len() <= 1024 => Some(label.clone()),
        _ => return Err(invalid()),
    };
    let error = match value.get("error") {
        Some(Value::Null) => None,
        Some(Value::Object(error)) if done && error.len() == 2 => Some(Error::Remote {
            code: error
                .get("code")
                .and_then(Value::as_str)
                .ok_or_else(invalid)?
                .into(),
            message: error
                .get("message")
                .and_then(Value::as_str)
                .ok_or_else(invalid)?
                .into(),
        }),
        _ => return Err(invalid()),
    };
    Ok((done, label, error))
}
fn text(payload: &Value, key: &str) -> Result<String> {
    payload[key]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| Error::InvalidInput(format!("missing {key}")))
}
