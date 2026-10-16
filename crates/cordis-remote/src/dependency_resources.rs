use crate::{Error, Result, Value, json, resource::WeakRemoteResource};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, OnceLock},
};

/// Only weak Runtime capabilities. This registry must not own its Session's
/// scope through a resource acquired during plugin initialization.
#[derive(Default)]
pub(crate) struct Resources {
    entries: Mutex<BTreeMap<String, Arc<Entry>>>,
}
pub(crate) struct Entry {
    grant: String,
    resource: OnceLock<WeakRemoteResource>,
}
impl Resources {
    pub fn reserve(&self, payload: &Value) -> Result<(String, Arc<Entry>)> {
        let id = text(payload, "resource")?;
        if !crate::resource::valid_resource_id(&id) {
            return Err(Error::InvalidInput("invalid resource ID".into()));
        }
        let mut entries = self.entries.lock().unwrap();
        if entries.contains_key(&id) {
            return Err(Error::InvalidInput("duplicate resource ID".into()));
        }
        if entries.len() >= 4096 {
            return Err(Error::Overloaded);
        }
        let mut bytes = [0; 16];
        getrandom::fill(&mut bytes).map_err(|e| Error::Io(e.to_string()))?;
        let entry = Arc::new(Entry {
            grant: bytes.iter().map(|b| format!("{b:02x}")).collect(),
            resource: OnceLock::new(),
        });
        entries.insert(id.clone(), entry.clone());
        Ok((id, entry))
    }
    pub fn get(&self, payload: &Value) -> Result<WeakRemoteResource> {
        let id = text(payload, "resource")?;
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
        entry.resource.get().cloned().ok_or(Error::Closed)
    }
}
impl Entry {
    pub fn commit(&self, id: String, resource: crate::RemoteResource) -> Value {
        let _ = self.resource.set(resource.downgrade());
        success(json!({"resource":id,"grant":self.grant}))
    }
}
pub fn text(payload: &Value, key: &str) -> Result<String> {
    payload[key]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| Error::InvalidInput(format!("missing {key}")))
}
pub fn success(value: Value) -> Value {
    json!({"status":"ok","value":value})
}
pub fn failure(error: Error, acquisition_started: bool) -> Value {
    let known = matches!(
        error,
        Error::Remote { .. } | Error::Refused { .. } | Error::InvalidInput(_) | Error::Overloaded
    ) || (!acquisition_started && matches!(error, Error::Closed));
    let business = matches!(error, Error::Remote { .. });
    let (code, message) = match error {
        Error::Remote { code, message } | Error::Refused { code, message } => (code, message),
        Error::Closed => ("closed".into(), "resource or Context expired".into()),
        Error::Overloaded => ("overloaded".into(), "resource capacity reached".into()),
        Error::InvalidInput(message) => ("invalid_argument".into(), message),
        other => ("resource_unknown".into(), other.to_string()),
    };
    json!({"status":if !known {"unknown"} else if business {"rejected"} else {"refused"},"error":{"code":code,"message":message}})
}
