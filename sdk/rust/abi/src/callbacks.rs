use super::*;
use cordis_sdk_core::{
    Context, LocalFuture,
    backend::{Callback, Producer},
};
use futures_util::{FutureExt, future::Shared};
use tokio::sync::Notify;

pub struct Frame {
    pub open: Cell<bool>,
    pub pending: Cell<usize>,
    pub changed: Notify,
}
impl Frame {
    pub fn new() -> Rc<Self> {
        Rc::new(Self {
            open: Cell::new(true),
            pending: Cell::new(0),
            changed: Notify::new(),
        })
    }
    pub async fn finish(&self) {
        self.open.set(false);
        while self.pending.get() > 0 {
            self.changed.notified().await;
        }
    }
}
pub type WireCallback = Rc<dyn Fn(Context, Payload) -> LocalFuture<Payload>>;
pub struct Entry {
    pub callback: RefCell<Option<WireCallback>>,
    result: RefCell<Option<Shared<LocalFuture<Payload>>>>,
    pub running: Cell<usize>,
}
impl Entry {
    fn new(callback: Callback) -> Self {
        Self::wire(Rc::new(move |ctx, args| {
            let callback = callback.clone();
            Box::pin(async move {
                callback(ctx, args.control_only().map_err(unknown)?)
                    .await
                    .map(Into::into)
            })
        }))
    }
    fn wire(callback: WireCallback) -> Self {
        Self {
            callback: RefCell::new(Some(callback)),
            result: RefCell::default(),
            running: Cell::new(0),
        }
    }
    pub async fn call(
        self: Rc<Self>,
        ctx: Context,
        args: Value,
        once: bool,
        release: bool,
    ) -> Result<Value> {
        self.call_payload(ctx, args.into(), once, release)
            .await?
            .control_only()
            .map_err(unknown)
    }
    pub async fn call_payload(
        self: Rc<Self>,
        ctx: Context,
        args: Payload,
        once: bool,
        release: bool,
    ) -> Result<Payload> {
        let cached = if once {
            self.result.borrow().clone()
        } else {
            None
        };
        let future = if let Some(future) = cached {
            future
        } else {
            let callback = self
                .callback
                .borrow()
                .clone()
                .ok_or_else(|| refused("closed", "callback retired"))?;
            let entry = self.clone();
            let future: LocalFuture<Payload> = Box::pin(async move {
                entry.running.set(entry.running.get() + 1);
                let result = std::panic::AssertUnwindSafe(async { callback(ctx, args).await })
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| Err(unknown("callback panicked")));
                entry.running.set(entry.running.get() - 1);
                if release && result.is_ok() {
                    let removed = entry.callback.borrow_mut().take();
                    drop(removed);
                }
                result
            });
            let shared = future.shared();
            if once {
                *self.result.borrow_mut() = Some(shared.clone());
            }
            shared
        };
        future.await
    }
}
pub struct Sequence {
    producer: RefCell<Option<Producer>>,
    next: Cell<u64>,
    running: Cell<bool>,
    ended: Cell<bool>,
    steps: RefCell<BTreeMap<String, Rc<Entry>>>,
}
#[derive(Default)]
pub struct Registry {
    next: Cell<u64>,
    pub entries: RefCell<BTreeMap<String, Rc<Entry>>>,
    sequences: RefCell<BTreeMap<String, Rc<Sequence>>>,
}
impl Registry {
    pub fn id(&self, prefix: char) -> Result<String> {
        if self.next.get() >= 65536 {
            return Err(refused("exhausted", "callback history full"));
        }
        self.next.set(self.next.get() + 1);
        Ok(format!("{prefix}{}", self.next.get()))
    }
    pub fn insert(&self, prefix: char, callback: Callback) -> Result<String> {
        if self.entries.borrow().len() >= 4096 {
            return Err(refused("overloaded", "callback capacity reached"));
        }
        let id = self.id(prefix)?;
        self.entries
            .borrow_mut()
            .insert(id.clone(), Rc::new(Entry::new(callback)));
        Ok(id)
    }
    pub fn insert_payload(&self, prefix: char, callback: WireCallback) -> Result<String> {
        if self.entries.borrow().len() >= 4096 {
            return Err(refused("overloaded", "callback capacity reached"));
        }
        let id = self.id(prefix)?;
        self.entries
            .borrow_mut()
            .insert(id.clone(), Rc::new(Entry::wire(callback)));
        Ok(id)
    }
    pub fn remove(&self, id: &str) {
        let removed = self.entries.borrow_mut().remove(id);
        drop(removed);
    }
    pub fn get(&self, id: &str) -> Result<Rc<Entry>> {
        self.entries
            .borrow()
            .get(id)
            .cloned()
            .ok_or_else(|| refused("closed", "callback absent"))
    }
    pub fn release(&self, id: &str) -> Result<()> {
        let entry = self.get(id)?;
        if entry.running.get() > 0 {
            return Err(refused("busy", "callback still running"));
        }
        let removed = entry.callback.borrow_mut().take();
        drop(removed);
        Ok(())
    }
    pub fn sequence(&self, producer: Producer) -> Result<String> {
        if self.sequences.borrow().len() >= 256 {
            return Err(refused("overloaded", "sequence capacity reached"));
        }
        let id = self.id('s')?;
        self.sequences.borrow_mut().insert(
            id.clone(),
            Rc::new(Sequence {
                producer: RefCell::new(Some(producer)),
                next: Cell::new(1),
                running: Cell::new(false),
                ended: Cell::new(false),
                steps: RefCell::default(),
            }),
        );
        Ok(id)
    }
    pub fn remove_sequence(&self, id: &str) {
        let removed = self.sequences.borrow_mut().remove(id);
        drop(removed);
    }
    pub async fn sequence_call(
        &self,
        command: &str,
        id: &str,
        step: &str,
        ctx: Context,
    ) -> Result<Value> {
        let sequence = self
            .sequences
            .borrow()
            .get(id)
            .cloned()
            .ok_or_else(|| refused("closed", "sequence absent"))?;
        if command == "sequence_release" {
            if sequence.running.get()
                || sequence
                    .steps
                    .borrow()
                    .values()
                    .any(|s| s.callback.borrow().is_some())
            {
                return Err(refused("owned", "sequence inverses remain"));
            }
            let removed = sequence.producer.borrow_mut().take();
            drop(removed);
            sequence.ended.set(true);
            return Ok(Value::Null);
        }
        if command == "sequence_undo" {
            let entry = sequence
                .steps
                .borrow()
                .get(step)
                .cloned()
                .ok_or_else(|| refused("closed", "inverse absent"))?;
            return entry.call(ctx, Value::Null, true, true).await;
        }
        if sequence.running.get() || sequence.ended.get() || step != sequence.next.get().to_string()
        {
            return Err(refused("invalid_step", "sequence order or state mismatch"));
        }
        if sequence.steps.borrow().len() >= 4096 {
            return Err(refused("exhausted", "sequence steps exhausted"));
        }
        let producer = sequence
            .producer
            .borrow()
            .clone()
            .ok_or_else(|| refused("closed", "producer absent"))?;
        sequence.running.set(true);
        sequence.next.set(sequence.next.get() + 1);
        let result = std::panic::AssertUnwindSafe(async { producer(ctx).await })
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(unknown("producer panicked")));
        sequence.running.set(false);
        let (inverse, complete, failure) = match result {
            Ok(step) => step.into_parts(),
            Err(Failure::Rejected(e) | Failure::Refused(e)) => {
                sequence.ended.set(true);
                return Ok(
                    json!({"done":true,"inverse":null,"error":{"code":e.code,"message":e.message}}),
                );
            }
            Err(e) => {
                sequence.ended.set(true);
                return Err(e);
            }
        };
        let label = inverse.as_ref().map(|_| "Rust step inverse");
        if let Some(inverse) = inverse {
            sequence
                .steps
                .borrow_mut()
                .insert(step.into(), Rc::new(Entry::new(inverse)));
        }
        let complete = complete || failure.is_some();
        sequence.ended.set(complete);
        Ok(
            json!({"done":complete,"inverse":label,"error":failure.map(|message|json!({"code":"step_failed","message":message}))}),
        )
    }
    pub fn recovered(&self) -> bool {
        self.entries
            .borrow()
            .values()
            .all(|e| e.callback.borrow().is_none() && e.running.get() == 0)
            && self
                .sequences
                .borrow()
                .values()
                .all(|e| e.producer.borrow().is_none() && !e.running.get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cordis_sdk_core::{
        LocalFuture,
        backend::{Backend, Operation, Reply},
    };
    struct Unused;
    impl Backend for Unused {
        fn submit(&self, _: Operation) -> LocalFuture<Reply> {
            Box::pin(async { Err(refused("unused", "no host calls expected")) })
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }
    #[tokio::test]
    async fn failed_cleanup_is_cached_and_keeps_its_capture() {
        let attempts = Rc::new(Cell::new(0));
        let counter = attempts.clone();
        let entry = Rc::new(Entry::new(Rc::new(move |_, _| {
            counter.set(counter.get() + 1);
            Box::pin(async { Err(refused("failed", "cleanup obligation remains")) })
        })));
        let ctx = Context::from_backend(Rc::new(Unused));
        assert!(
            entry
                .clone()
                .call(ctx.clone(), Value::Null, true, true)
                .await
                .is_err()
        );
        assert!(
            entry
                .clone()
                .call(ctx, Value::Null, true, true)
                .await
                .is_err()
        );
        assert_eq!(attempts.get(), 1);
        assert!(entry.callback.borrow().is_some());
    }
}
