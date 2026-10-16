use crate::{
    Error, Payload, Result, Value, WeakAbiResource,
    context::{Router, next_id},
    ffi::Reply,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::sync::Semaphore;

pub(crate) struct Resources {
    entries: Mutex<BTreeMap<u64, Arc<OnceLock<WeakAbiResource>>>>,
    calls: Semaphore,
    cleanup: Semaphore,
    disposals: Semaphore,
}
impl Resources {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: Mutex::default(),
            calls: Semaphore::new(capacity),
            cleanup: Semaphore::new(capacity),
            disposals: Semaphore::new(capacity),
        }
    }
    fn reserve(&self) -> Result<(u64, Arc<OnceLock<WeakAbiResource>>)> {
        let mut entries = self.entries.lock().unwrap();
        if entries.len() >= 4096 {
            return Err(Error::Overloaded);
        }
        let id = next_id()?;
        let entry = Arc::new(OnceLock::new());
        entries.insert(id, entry.clone());
        Ok((id, entry))
    }
    fn get(&self, id: u64) -> Result<WeakAbiResource> {
        self.entries
            .lock()
            .unwrap()
            .get(&id)
            .and_then(|e| e.get())
            .cloned()
            .ok_or(Error::Closed)
    }
}
fn uncertainty(error: Error) -> Error {
    match error {
        e @ (Error::Rejected { .. }
        | Error::Refused { .. }
        | Error::InvalidInput(_)
        | Error::Overloaded
        | Error::Closed
        | Error::Unknown(_)) => e,
        e => Error::Unknown(e.to_string()),
    }
}
impl Router {
    pub(crate) async fn resource_operation(
        self: &Arc<Self>,
        grant: u64,
        operation: u32,
        target: u64,
        dependency: String,
        method: String,
        args: Payload,
    ) -> Result<Reply> {
        if !self
            .instance
            .upgrade()
            .ok_or(Error::Closed)?
            .module
            .dependency_resources()
        {
            return Err(Error::Refused {
                code: "unsupported_resource".into(),
                message: "resource extension not negotiated".into(),
            });
        }
        let ctx = self.context(grant)?;
        if !(1..=3).contains(&operation)
            || (operation == 1 && target != 0)
            || (operation != 1 && (target == 0 || !dependency.is_empty()))
            || (operation == 3
                && (!method.is_empty() || (!args.control.is_null() || !args.messages.is_empty())))
        {
            return Err(Error::InvalidInput(
                "invalid resource operation arguments".into(),
            ));
        }
        if ctx.is_cleanup() && matches!(operation, 1 | 3) {
            return Err(Error::Refused {
                code: "cleanup_resource".into(),
                message: "cleanup cannot acquire or synchronously dispose resources".into(),
            });
        }
        let capacity = if operation == 3 {
            &self.resources.disposals
        } else if ctx.is_cleanup() {
            &self.resources.cleanup
        } else {
            &self.resources.calls
        };
        let _permit = capacity.try_acquire().map_err(|_| Error::Overloaded)?;
        let router = self.clone();
        ctx.operation(move |ctx| async move {
            let result = async {
                if operation == 1 {
                    let binding = router
                        .capabilities
                        .dependencies
                        .iter()
                        .find(|d| d.contract().name.as_ref() == dependency)
                        .ok_or_else(|| Error::Refused {
                            code: "undeclared_dependency".into(),
                            message: dependency,
                        })?;
                    let acquire = binding.acquire.as_ref().ok_or_else(|| Error::Refused {
                        code: "unsupported_resource".into(),
                        message: "dependency has no resource binding".into(),
                    })?;
                    let schema = cordis_contract::portable::resource(binding.descriptor(), &method)
                        .map_err(Error::InvalidInput)?;
                    let mut args = cordis_wire::local::Envelope::from(Payload::message(
                        args.one().map_err(|e| Error::InvalidInput(e.to_string()))?,
                    ));
                    args.check(&schema.args).map_err(Error::InvalidInput)?;
                    let (id, entry) = router.resources.reserve()?;
                    let resource = acquire(ctx, method, args).await.map_err(uncertainty)?;
                    let _ = entry.set(resource.downgrade());
                    return Ok(Reply {
                        value: Value::Null.into(),
                        handle: id,
                    });
                }
                let resource = router.resources.get(target)?;
                let value = if operation == 2 {
                    Payload::message(
                        resource
                            .invoke(
                                &method,
                                args.one().map_err(|e| Error::InvalidInput(e.to_string()))?,
                            )
                            .await?,
                    )
                } else {
                    resource.dispose().await?;
                    Value::Null.into()
                };
                Ok(Reply { value, handle: 0 })
            }
            .await;
            Ok(result.map_err(uncertainty))
        })
        .await
        .map_err(|e| uncertainty(e.into()))?
    }
}
