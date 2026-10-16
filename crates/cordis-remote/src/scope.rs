use crate::{Error, Result, Value, json, session::WeakSession};
use cordis_runtime::{Context, EffectStatus, WeakScopeHandle};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

#[derive(Default)]
pub(crate) struct Scopes {
    entries: Mutex<BTreeMap<String, Arc<Entry>>>,
    runs: Mutex<BTreeSet<String>>,
}
struct Entry {
    grant: String,
    handle: WeakScopeHandle,
    runs: Mutex<Vec<Arc<Ticket>>>,
}
struct Ticket {
    id: String,
    state: Mutex<&'static str>,
}
impl Scopes {
    pub fn register(&self, ctx: &Context, payload: &Value) -> Result<Value> {
        let id = identifier(payload, "scope", 'c')?;
        let label = text(payload, "label")?;
        if label.len() > 1024 {
            return Err(Error::InvalidInput("scope label too long".into()));
        }
        let mut entries = self.entries.lock().unwrap();
        if entries.contains_key(&id) {
            return Err(Error::InvalidInput("duplicate scope ID".into()));
        }
        if entries.len() >= 256 {
            return Err(Error::Overloaded);
        }
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).map_err(|e| Error::Io(e.to_string()))?;
        let grant: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        let handle = ctx.scope(label).map_err(crate::resource::runtime_error)?;
        entries.insert(
            id.clone(),
            Arc::new(Entry {
                grant: grant.clone(),
                handle: handle.downgrade(),
                runs: Mutex::default(),
            }),
        );
        Ok(json!({"scope":id,"grant":grant}))
    }
    pub async fn invoke(
        &self,
        command: &str,
        payload: &Value,
        session: WeakSession,
    ) -> Result<Value> {
        let id = text(payload, "scope")?;
        let grant = text(payload, "grant")?;
        let entry = self
            .entries
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or(Error::Closed)?;
        if grant != entry.grant {
            return Err(Error::Closed);
        }
        let handle = entry.handle.upgrade().ok_or(Error::Closed)?;
        match command {
            "scope_ready" => {
                handle
                    .ready()
                    .await
                    .map_err(crate::resource::runtime_error)?;
                Ok(json!({}))
            }
            "scope_dispose" => {
                handle
                    .dispose()
                    .await
                    .map_err(crate::resource::runtime_error)?;
                Ok(json!({}))
            }
            "scope_status" => {
                let snapshot = handle.snapshot();
                let effects: Vec<_> = handle.effects().into_iter().map(|e| json!({
                    "label": e.label,
                    "state": match e.status { EffectStatus::Owned => "owned", EffectStatus::Releasing => "releasing", EffectStatus::Released => "released", EffectStatus::Failed(_) => "failed" }
                })).collect();
                let runs: Vec<_> = entry
                    .runs
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|t| json!({"run":t.id,"state":*t.state.lock().unwrap()}))
                    .collect();
                Ok(
                    json!({"state":format!("{:?}",snapshot.state).to_lowercase(),"retired":snapshot.retired,"error":snapshot.outcome.map(|o|format!("{o:?}")),"effects":effects,"runs":runs}),
                )
            }
            "scope_run" => {
                let run = identifier(payload, "run", 'u')?;
                {
                    let mut runs = self.runs.lock().unwrap();
                    if runs.contains(&run) {
                        return Err(Error::InvalidInput("duplicate scope run".into()));
                    }
                    if runs.len() >= 4096 {
                        return Err(Error::Overloaded);
                    }
                    runs.insert(run.clone());
                }
                let ticket = Arc::new(Ticket {
                    id: run.clone(),
                    state: Mutex::new("not_executed"),
                });
                entry.runs.lock().unwrap().push(ticket.clone());
                handle
                    .run(move |ctx| async move {
                        // This inverse is registered before any remote execution. A
                        // transport failure never turns pending work into clean work.
                        let guard = ticket.clone();
                        ctx.defer("remote scope operation", move |_| async move {
                            match *guard.state.lock().unwrap() {
                                "completed" | "not_executed" => Ok(()),
                                _ => {
                                    Err(Error::OutcomeUnknown(format!("scope run {}", guard.id))
                                        .into())
                                }
                            }
                        })?;
                        *ticket.state.lock().unwrap() = "pending";
                        let reply = match session.upgrade() {
                            Some(session) => {
                                session
                                    .scoped_request(
                                        &ctx,
                                        "scope_call",
                                        json!({"scope":id,"run":run}),
                                    )
                                    .await
                            }
                            None => Err(Error::HostLost("scope host no longer exists".into())),
                        };
                        let result = match reply {
                            Ok(value) => match parse_outcome(value) {
                                Ok(result) => {
                                    *ticket.state.lock().unwrap() = "completed";
                                    result
                                }
                                Err(error) => Err(error),
                            },
                            Err(error) if not_executed(&error) => {
                                *ticket.state.lock().unwrap() = "not_executed";
                                Err(error)
                            }
                            Err(error) => Err(error),
                        };
                        if *ticket.state.lock().unwrap() == "pending" {
                            *ticket.state.lock().unwrap() = "unknown";
                            let failure = Error::OutcomeUnknown(format!(
                                "scope run {}: {:?}",
                                ticket.id,
                                result.err()
                            ));
                            let cleanup = failure.clone();
                            // Place a blocker above contributions registered during
                            // this run, before relinquishing its admission leases.
                            ctx.defer("unknown remote scope operation", move |_| async move {
                                Err(cleanup.into())
                            })?;
                            ctx.fail(failure.to_string())?;
                            return Ok(Err(failure));
                        }
                        Ok(result)
                    })
                    .await
                    .map_err(crate::resource::runtime_error)?
            }
            _ => Err(Error::InvalidInput("unknown scope operation".into())),
        }
    }
}
pub(crate) fn not_executed(error: &Error) -> bool {
    *error == Error::Overloaded
        || matches!(error, Error::Refused { code, .. } if matches!(code.as_str(), "overloaded" | "closed" | "session_exhausted"))
}
pub(crate) fn parse_outcome(value: Value) -> Result<Result<Value>> {
    let invalid = || Error::Protocol("invalid callback outcome".into());
    if value.as_object().is_none_or(|v| v.len() != 2) {
        return Err(invalid());
    }
    match value["kind"].as_str() {
        Some("success") => Ok(Ok(value.get("value").cloned().ok_or_else(invalid)?)),
        Some("failure" | "refused") => {
            let error = value["error"]
                .as_object()
                .filter(|o| o.len() == 2)
                .ok_or_else(invalid)?;
            let parsed = Error::Remote {
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
            };
            Ok(Err(match parsed {
                Error::Remote { code, message } if value["kind"] == "refused" => {
                    Error::Refused { code, message }
                }
                error => error,
            }))
        }
        _ => Err(invalid()),
    }
}
pub(crate) fn text(payload: &Value, key: &str) -> Result<String> {
    payload[key]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| Error::InvalidInput(format!("missing {key}")))
}
pub(crate) fn identifier(payload: &Value, key: &str, prefix: char) -> Result<String> {
    let id = text(payload, key)?;
    if !id
        .strip_prefix(prefix)
        .is_some_and(|s| !s.is_empty() && s.len() <= 20 && s.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err(Error::InvalidInput(format!("invalid {key} ID")));
    }
    Ok(id)
}
