//! Observation cancellation never cancels an admitted effect or releases its
//! resources. HostCall continues to pin guest code through the completion
//! callback, including when the observer stopped listening to its result.
use crate::{Error, Result, context::Router, ffi::Reply, json};
use std::{sync::Arc, time::Duration};

pub(crate) struct Wait {
    router: Arc<Router>,
    key: (u64, u64),
    cancelled: tokio::sync::watch::Receiver<bool>,
}
impl Drop for Wait {
    fn drop(&mut self) {
        self.router.waits.lock().unwrap().remove(&self.key);
    }
}
impl Router {
    /// Register synchronously before returning from the FFI submission. A
    /// cancellation immediately following submission therefore cannot overtake
    /// registration or accidentally cancel a later wait with the same ID.
    pub(crate) fn begin_wait(self: &Arc<Self>, caller: u64, id: u64) -> Result<Wait> {
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
        let mut waits = self.waits.lock().unwrap();
        let key = (caller, id);
        if waits.contains_key(&key) {
            return Err(Error::InvalidInput("wait ID is already active".into()));
        }
        let (send, cancelled) = tokio::sync::watch::channel(false);
        waits.insert(key, send);
        Ok(Wait {
            router: self.clone(),
            key,
            cancelled,
        })
    }
    pub(crate) fn cancel_wait(&self, caller: u64, id: u64) {
        if let Some(wait) = self.waits.lock().unwrap().get(&(caller, id)) {
            wait.send_replace(true);
        }
    }
    pub(crate) fn cancel_waits(&self, caller: u64) {
        for ((owner, _), wait) in self.waits.lock().unwrap().iter() {
            if *owner == caller {
                wait.send_replace(true);
            }
        }
    }
}
impl Wait {
    pub(crate) async fn run(mut self, grant: u64, kind: u32, milliseconds: u64) -> Result<Reply> {
        if kind == 1 && super::in_cleanup() {
            return Err(Error::Refused {
                code: "cleanup_wait".into(),
                message: "cleanup cannot wait for task cancellation".into(),
            });
        }
        let ctx = match kind {
            0 => self.router.context(grant)?,
            1 => self.router.task_context(grant)?,
            _ => return Err(Error::InvalidInput("unknown wait kind".into())),
        };
        if milliseconds > 86_400_000 && !(kind == 1 && milliseconds == u64::MAX) {
            return Err(Error::InvalidInput("invalid wait duration".into()));
        }
        let deadline = async {
            if milliseconds == u64::MAX {
                std::future::pending::<()>().await;
            } else {
                tokio::time::sleep(Duration::from_millis(milliseconds)).await;
            }
        };
        let abandoned = async {
            while !*self.cancelled.borrow_and_update() {
                if self.cancelled.changed().await.is_err() {
                    break;
                }
            }
        };
        let owner_cancelled = tokio::select! {
            biased;
            _ = abandoned => return Err(Error::Refused {
                code: "wait_cancelled".into(), message: "observation was withdrawn".into(),
            }),
            _ = ctx.cancelled() => true,
            _ = deadline => false,
        };
        Ok(Reply {
            value: json!(owner_cancelled).into(),
            handle: 0,
        })
    }
}
