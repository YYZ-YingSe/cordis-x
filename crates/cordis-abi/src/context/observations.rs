//! Observation cancellation never cancels an admitted effect or releases its
//! resources. HostCall continues to pin guest code through the completion
//! callback, including when the observer stopped listening to its result.
use crate::{Error, Result, Value, context::Router, ffi::Reply, json};
use std::{sync::Arc, time::Duration};

pub(crate) struct Observation {
    router: Arc<Router>,
    key: (u64, u64),
    cancelled: tokio::sync::watch::Receiver<bool>,
}
impl Drop for Observation {
    fn drop(&mut self) {
        self.router.observations.lock().unwrap().remove(&self.key);
    }
}
impl Router {
    fn observe_metadata(&self, grant: u64, args: &crate::Value) -> Result<Reply> {
        let label = args["name"]
            .as_str()
            .ok_or_else(|| Error::InvalidInput("missing metadata name".into()))?;
        let ctx = self.context(grant)?.snapshot();
        let revision = args["revision"]
            .as_str()
            .filter(|s| !s.is_empty() && s.len() <= 1024)
            .ok_or_else(|| Error::InvalidInput("invalid metadata revision".into()))?;
        if label.is_empty() || label.len() > 1024 {
            return Err(Error::InvalidInput("invalid metadata request".into()));
        }
        let service = cordis_runtime::Service::<()>::new(label, revision);
        let contract = service.contract();
        let instance = self.instance.upgrade().ok_or(Error::Closed)?;
        if &instance.module.contract != contract
            && !self
                .capabilities
                .bindings
                .dependencies
                .iter()
                .any(|d| d.contract() == contract)
            && !self
                .capabilities
                .bindings
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
        let value = json!({"revision":ctx.metadata_revision().map(|r|r.to_string()),
                    "layers":layers.iter().map(|v|v.as_ref()).collect::<Vec<_>>()});
        Ok(Reply {
            value: value.into(),
            handle: 0,
        })
    }

    /// Register synchronously before returning from the FFI submission. A
    /// cancellation immediately following submission therefore cannot overtake
    /// registration or accidentally cancel a later wait with the same ID.
    pub(crate) fn begin_observation(self: &Arc<Self>, caller: u64, id: u64) -> Result<Observation> {
        if id == 0 {
            return Err(Error::InvalidInput("wait ID must be nonzero".into()));
        }
        let scope = self
            .contexts
            .lock()
            .unwrap()
            .get(&caller)
            .cloned()
            .ok_or(Error::Closed)?;
        // Admission and wait registration are separate FFI steps. Hold the
        // scope gate through insertion so finish cannot miss a newly registered
        // observation after its one cancellation sweep.
        let state = scope.state.lock().unwrap();
        if !state.open {
            return Err(Error::Closed);
        }
        let mut waits = self.observations.lock().unwrap();
        let key = (caller, id);
        if waits.contains_key(&key) {
            return Err(Error::InvalidInput("wait ID is already active".into()));
        }
        let (send, cancelled) = tokio::sync::watch::channel(false);
        waits.insert(key, send);
        Ok(Observation {
            router: self.clone(),
            key,
            cancelled,
        })
    }
    pub(crate) fn cancel_observation(&self, caller: u64, id: u64) {
        if let Some(wait) = self.observations.lock().unwrap().get(&(caller, id)) {
            wait.send_replace(true);
        }
    }
    pub(crate) fn cancel_observations(&self, caller: u64) {
        for ((owner, _), wait) in self.observations.lock().unwrap().iter() {
            if *owner == caller {
                wait.send_replace(true);
            }
        }
    }
}
impl Observation {
    pub(crate) async fn run(
        mut self,
        grant: u64,
        kind: u32,
        target: u64,
        args: crate::Value,
    ) -> Result<Reply> {
        let router = self.router.clone();
        let query =
            async move {
                match kind {
                    0 | 1 => {
                        if kind == 1 && super::in_cleanup() {
                            return Err(Error::Refused {
                                code: "cleanup_wait".into(),
                                message: "cleanup cannot wait for cancellation".into(),
                            });
                        }
                        let ctx = if kind == 0 {
                            router.context(grant)?
                        } else {
                            router.task_context(grant)?
                        };
                        let milliseconds =
                            if kind == 1 && args.is_null() {
                                None
                            } else {
                                Some(args.as_u64().filter(|ms| *ms <= 86_400_000).ok_or_else(
                                    || Error::InvalidInput("invalid observation duration".into()),
                                )?)
                            };
                        let deadline = async {
                            if let Some(ms) = milliseconds {
                                tokio::time::sleep(Duration::from_millis(ms)).await;
                            } else {
                                std::future::pending::<()>().await;
                            }
                        };
                        Ok(Reply {
                        value: json!(
                            tokio::select! { _ = ctx.cancelled() => true, _ = deadline => false }
                        )
                        .into(),
                        handle: 0,
                    })
                    }
                    2 | 3 | 4 | 8 | 9 | 10 => router.observe_managed(kind, target).await,
                    5 | 6 | 11 => router.observe_child(kind, target).await,
                    7 | 12 => router.observe_dispatch(kind == 12, target).await,
                    13 => router.observe_metadata(grant, &args),
                    _ => Err(Error::InvalidInput("unknown observation kind".into())),
                }
            };
        let abandoned = async {
            while !*self.cancelled.borrow_and_update() {
                if self.cancelled.changed().await.is_err() {
                    break;
                }
            }
        };
        tokio::select! {
            biased;
            _ = abandoned => Err(Error::Refused { code: "observation_cancelled".into(), message: "observation was withdrawn".into() }),
            result = query => result,
        }
    }
}
