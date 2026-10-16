//! Instance-local object storage and draining. Runtime owns resource lifetimes.
use super::{callbacks::Frame, *};
use cordis_sdk_core::{Context, LocalFuture, ResourceObject};
use futures_util::{FutureExt, future::Shared};
use tokio::sync::Notify;

#[derive(Default)]
pub struct Resources {
    entries: RefCell<BTreeMap<u64, Rc<Entry>>>,
    next: Cell<u64>,
}
impl Resources {
    pub fn reserve(&self, contract: cordis_sdk_core::ResourceContract) -> Result<(u64, Rc<Entry>)> {
        if self.entries.borrow().len() >= 4096 {
            return Err(refused("overloaded", "resource history full"));
        }
        let next = self
            .next
            .get()
            .checked_add(1)
            .ok_or_else(|| refused("exhausted", "resource IDs exhausted"))?;
        self.next.set(next);
        let id = next;
        let entry = Rc::new(Entry {
            contract,
            value: RefCell::default(),
            closing: Cell::new(false),
            running: Cell::new(0),
            idle: Notify::new(),
            completion: RefCell::default(),
        });
        self.entries.borrow_mut().insert(id, entry.clone());
        Ok((id, entry))
    }
    pub fn reject(&self, id: u64) {
        let removed = self.entries.borrow_mut().remove(&id);
        drop(removed);
    }
    pub fn get(&self, id: u64) -> Result<Rc<Entry>> {
        self.entries
            .borrow()
            .get(&id)
            .cloned()
            .ok_or_else(|| refused("closed", "resource absent"))
    }
    pub fn recovered(&self) -> bool {
        self.entries.borrow().values().all(|e| {
            e.completion
                .borrow()
                .as_ref()
                .and_then(|f| f.peek())
                .is_some_and(Result::is_ok)
        })
    }
}
pub struct Entry {
    pub contract: cordis_sdk_core::ResourceContract,
    pub value: RefCell<Option<Rc<dyn ResourceObject>>>,
    closing: Cell<bool>,
    running: Cell<usize>,
    idle: Notify,
    completion: RefCell<Option<Shared<LocalFuture<()>>>>,
}
impl Entry {
    pub async fn invoke(
        &self,
        ctx: Context,
        frame: Rc<Frame>,
        method: String,
        args: Bytes,
    ) -> Result<Bytes> {
        if self.closing.get() {
            return Err(refused("closed", "resource closing"));
        }
        let value = self
            .value
            .borrow()
            .clone()
            .ok_or_else(|| refused("closed", "resource pending"))?;
        self.running.set(self.running.get() + 1);
        let result = std::panic::AssertUnwindSafe(async {
            let signature = cordis_sdk_core::contract::method(&self.contract.methods, &method)
                .map_err(|e| refused("invalid_argument", e))?;
            let args = cordis_sdk_core::Message::from_bytes(signature.args.clone(), args)
                .map_err(|e| refused("invalid_argument", e))?;
            if !value.typed_dispatch() {
                args.validate(args.validation())
                    .map_err(|e| refused("invalid_argument", e))?;
            }
            let output = value
                .invoke(ctx, method, args)
                .await
                .map_err(|e| e.check_declared(&signature.errors, "resource result"))?;
            output.check(&signature.result).map_err(unknown)?;
            output.encode(cordis_wire::MAX_MESSAGE).map_err(unknown)
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(unknown("resource callback panicked")));
        frame.finish().await;
        self.running.set(self.running.get() - 1);
        self.idle.notify_one();
        result
    }
    pub async fn close(self: Rc<Self>, ctx: Context, frame: Rc<Frame>) -> Result<()> {
        let cached = self.completion.borrow().clone();
        let future = if let Some(future) = cached {
            future
        } else {
            self.closing.set(true);
            let entry = self.clone();
            let future: LocalFuture<()> = Box::pin(async move {
                while entry.running.get() != 0 {
                    entry.idle.notified().await;
                }
                let value = entry
                    .value
                    .borrow()
                    .clone()
                    .ok_or_else(|| unknown("resource acquisition uncertain"))?;
                let result = std::panic::AssertUnwindSafe(async { value.close(ctx).await })
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| Err(unknown("resource close panicked")));
                frame.finish().await;
                if result.is_ok() {
                    let removed = entry.value.borrow_mut().take();
                    drop(removed);
                }
                result
            });
            let future = future.shared();
            *self.completion.borrow_mut() = Some(future.clone());
            future
        };
        future.await
    }
}
