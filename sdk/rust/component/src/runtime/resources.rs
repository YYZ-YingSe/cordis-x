//! Store-local business objects. Task cancellation never discharges recovery.
use cordis_component::Bytes;
use cordis_sdk_core::{Context, LocalFuture, ResourceObject, Result, Value, refused};
use futures_util::{FutureExt, future::Shared};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    rc::Rc,
    task::{Poll, Waker},
};

#[derive(Default)]
pub(super) struct Resources {
    entries: RefCell<BTreeMap<u64, Rc<Entry>>>,
    next: Cell<u64>,
}
impl Resources {
    pub fn reserve(
        &self,
        owner: &str,
        contract: cordis_sdk_core::ResourceContract,
    ) -> Result<(u64, Rc<Entry>)> {
        if owner.len() != 32
            || !owner
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        {
            return Err(refused("invalid_argument", "invalid resource owner"));
        }
        if self.entries.borrow().len() >= 4096 {
            return Err(refused("overloaded", "resource history full"));
        }
        let id = self
            .next
            .get()
            .checked_add(1)
            .ok_or_else(|| refused("exhausted", "resource IDs exhausted"))?;
        self.next.set(id);
        let entry = Rc::new(Entry {
            contract,
            owner: owner.into(),
            value: RefCell::default(),
            running: Cell::new(0),
            drained: RefCell::default(),
            completion: RefCell::default(),
        });
        self.entries.borrow_mut().insert(id, entry.clone());
        Ok((id, entry))
    }
    pub fn reject(&self, id: u64) {
        let removed = self.entries.borrow_mut().remove(&id);
        drop(removed);
    }
    pub fn get(&self, args: &Value) -> Result<Rc<Entry>> {
        let id = args["resource"]
            .as_u64()
            .ok_or_else(|| refused("invalid_argument", "missing resource"))?;
        let entry = self
            .entries
            .borrow()
            .get(&id)
            .cloned()
            .ok_or_else(|| refused("closed", "resource absent"))?;
        if args["owner"].as_str() != Some(entry.owner.as_str()) {
            return Err(refused("closed", "resource ownership mismatch"));
        }
        Ok(entry)
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
pub(super) struct Entry {
    contract: cordis_sdk_core::ResourceContract,
    owner: String,
    pub value: RefCell<Option<Rc<dyn ResourceObject>>>,
    running: Cell<usize>,
    drained: RefCell<Option<Waker>>,
    completion: RefCell<Option<Shared<LocalFuture<()>>>>,
}
struct Call<'a>(&'a Entry);
impl Drop for Call<'_> {
    fn drop(&mut self) {
        self.0.running.set(self.0.running.get() - 1);
        if self.0.running.get() == 0 {
            let wake = self.0.drained.borrow_mut().take();
            if let Some(wake) = wake {
                wake.wake();
            }
        }
    }
}
impl Entry {
    pub async fn invoke(&self, ctx: Context, method: String, args: Bytes) -> Result<Bytes> {
        if self.completion.borrow().is_some() {
            return Err(refused("closed", "resource closing"));
        }
        let value = self
            .value
            .borrow()
            .clone()
            .ok_or_else(|| refused("closed", "resource pending"))?;
        self.running.set(self.running.get() + 1);
        let _call = Call(self);
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
        output
            .check(&signature.result)
            .map_err(cordis_sdk_core::typed::violation)?;
        output
            .encode(cordis_component::MAX_BYTES)
            .map_err(cordis_sdk_core::typed::violation)
    }
    pub async fn close(self: Rc<Self>, ctx: Context) -> Result<()> {
        let cached = self.completion.borrow().clone();
        let future = if let Some(f) = cached {
            f
        } else {
            let entry = self.clone();
            let future: LocalFuture<()> = Box::pin(async move {
                std::future::poll_fn(|cx| {
                    if entry.running.get() == 0 {
                        Poll::Ready(())
                    } else {
                        *entry.drained.borrow_mut() = Some(cx.waker().clone());
                        Poll::Pending
                    }
                })
                .await;
                let value = entry
                    .value
                    .borrow()
                    .clone()
                    .ok_or_else(|| refused("closed", "resource pending"))?;
                let result = value.close(ctx).await;
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
