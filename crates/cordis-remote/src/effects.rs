use crate::{Error, Result, Value, json, session::WeakSession};
use cordis_runtime::{Context, WeakEffectHandle};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, OnceLock},
};

// Only weak native handles: a session registry must not retain the scopes that
// themselves own the process. Terminal results remain as bounded tombstones.
#[derive(Default)]
pub(crate) struct Effects {
    entries: Mutex<BTreeMap<String, Arc<Entry>>>,
}
struct Entry {
    grant: String,
    handle: OnceLock<WeakEffectHandle>,
    result: Mutex<Option<Result<Value>>>,
}
impl Effects {
    pub fn register(&self, ctx: &Context, payload: &Value, session: WeakSession) -> Result<Value> {
        let id = text(payload, "effect")?;
        if !id.strip_prefix('e').is_some_and(|n| {
            !n.is_empty() && n.len() <= 20 && n.bytes().all(|b| b.is_ascii_digit())
        }) {
            return Err(Error::InvalidInput("invalid effect ID".into()));
        }
        let label = text(payload, "label")?;
        if label.len() > 1024 {
            return Err(Error::InvalidInput("effect label too long".into()));
        }
        let mut entries = self.entries.lock().unwrap();
        if entries.contains_key(&id) {
            return Err(Error::InvalidInput("duplicate effect ID".into()));
        }
        if entries.len() >= 4096 {
            return Err(Error::Overloaded);
        }
        let mut random = [0u8; 16];
        getrandom::fill(&mut random).map_err(|e| Error::Io(e.to_string()))?;
        let grant: String = random.iter().map(|b| format!("{b:02x}")).collect();
        let entry = Arc::new(Entry {
            grant: grant.clone(),
            handle: OnceLock::new(),
            result: Mutex::new(None),
        });
        let cleanup_entry = entry.clone();
        let cleanup_id = id.clone();
        let handle = ctx
            .defer(label, move |cleanup| async move {
                let result = match session.upgrade() {
                    Some(session) => {
                        session
                            .scoped_request(
                                &cleanup,
                                "effect_release",
                                json!({"effect":cleanup_id}),
                            )
                            .await
                    }
                    None => Err(Error::HostLost("effect host no longer exists".into())),
                };
                *cleanup_entry.result.lock().unwrap() = Some(result.clone());
                result.map(|_| ()).map_err(Into::into)
            })
            .map_err(crate::resource::runtime_error)?;
        let _ = entry.handle.set(handle.downgrade());
        entries.insert(id.clone(), entry);
        Ok(json!({"effect":id, "grant":grant}))
    }

    pub async fn dispose(&self, payload: &Value) -> Result<Value> {
        let id = text(payload, "effect")?;
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
        if let Some(handle) = entry.handle.get().and_then(WeakEffectHandle::upgrade) {
            if let Err(error) = handle.dispose().await {
                return Err(entry
                    .result
                    .lock()
                    .unwrap()
                    .as_ref()
                    .and_then(|r| r.clone().err())
                    .unwrap_or_else(|| crate::resource::runtime_error(error)));
            }
            return Ok(json!({}));
        }
        entry
            .result
            .lock()
            .unwrap()
            .clone()
            .unwrap_or(Err(Error::Closed))
    }
}

fn text(payload: &Value, key: &str) -> Result<String> {
    payload[key]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| Error::InvalidInput(format!("missing {key}")))
}
