use cordis_component::Payload;
use cordis_sdk_core::{Context, Failure, LocalFuture, Result, backend::Callback, refused};
#[cfg(test)]
use cordis_sdk_core::{FutureResult, Value};
use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

pub(crate) type WireCallback = Rc<dyn Fn(Context, Payload) -> LocalFuture<Payload>>;

#[derive(Default)]
struct State {
    validation: cordis_sdk_core::Validation,
    next: u64,
    entries: BTreeMap<u64, WireCallback>,
}

/// Callback storage only. The host still owns registration and recovery.
#[derive(Clone, Default)]
pub(crate) struct Callbacks(Rc<RefCell<State>>);
impl Callbacks {
    pub(crate) fn validation(&self) -> cordis_sdk_core::Validation {
        self.0.borrow().validation
    }
    pub(crate) fn set_validation(&self, validation: cordis_sdk_core::Validation) {
        self.0.borrow_mut().validation = validation;
    }
    pub(crate) fn insert(&self, callback: Callback) -> u64 {
        self.insert_payload(Rc::new(move |ctx, payload| {
            let args = match payload.control_only() {
                Ok(args) => args,
                Err(error) => {
                    return Box::pin(async move { Err(refused("invalid_argument", error)) });
                }
            };
            let future = callback(ctx, args);
            Box::pin(async move { future.await.map(Into::into) })
        }))
    }
    pub(crate) fn insert_payload(&self, callback: WireCallback) -> u64 {
        let mut state = self.0.borrow_mut();
        state.next = state.next.checked_add(1).expect("callback id exhausted");
        let id = state.next;
        state.entries.insert(id, callback);
        id
    }
    #[cfg(test)]
    pub(crate) fn invoke(&self, id: u64, ctx: Context, args: Value) -> Result<FutureResult> {
        let future = self.invoke_payload(id, ctx, args.into())?;
        Ok(Box::pin(async move {
            future
                .await?
                .control_only()
                .map_err(|e| refused("invalid_argument", e))
        }))
    }
    pub(crate) fn invoke_payload(
        &self,
        id: u64,
        ctx: Context,
        args: Payload,
    ) -> Result<LocalFuture<Payload>> {
        let callback = self
            .0
            .borrow()
            .entries
            .get(&id)
            .cloned()
            .ok_or_else(|| refused("closed", "callback absent"))?;
        // No registry borrow spans user code or destruction of user captures.
        Ok(callback(ctx, args))
    }
    pub(crate) fn remove(&self, id: u64) {
        let removed = self.0.borrow_mut().entries.remove(&id);
        drop(removed);
    }
    pub(crate) fn clear(&self) {
        let entries = std::mem::take(&mut self.0.borrow_mut().entries);
        drop(entries);
        // Keep next monotonic so old callback IDs can never name new captures.
    }
    pub(crate) fn registration_result<T>(&self, id: u64, result: &Result<T>) {
        if matches!(result, Err(Failure::Rejected(_) | Failure::Refused(_))) {
            self.remove(id);
        }
        // An unknown outcome retains callback storage for host recovery.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::ComponentBackend;
    use cordis_sdk_core::ErrorInfo;
    use std::cell::Cell;

    fn callback() -> Callback {
        Rc::new(|_, args| Box::pin(async move { Ok(args) }))
    }
    fn context(callbacks: &Callbacks) -> Context {
        ComponentBackend::context("test".into(), callbacks.clone())
    }
    fn contains(callbacks: &Callbacks, id: u64) -> bool {
        callbacks
            .invoke(id, context(callbacks), Value::Null)
            .is_ok()
    }

    #[test]
    fn rejected_registration_releases_capture_but_unknown_keeps_it() {
        let callbacks = Callbacks::default();
        let capture = Rc::new(());
        let weak = Rc::downgrade(&capture);
        let id = callbacks.insert(Rc::new(move |_, _| {
            let _ = &capture;
            Box::pin(async { Ok(Value::Null) })
        }));
        let unknown: Result<()> = Err(Failure::Unknown(ErrorInfo {
            code: "lost_ack".into(),
            message: "registration outcome unknown".into(),
        }));
        callbacks.registration_result(id, &unknown);
        assert!(contains(&callbacks, id));
        assert!(weak.upgrade().is_some());
        callbacks.registration_result(id, &Err::<(), _>(refused("rejected", "not admitted")));
        assert!(!contains(&callbacks, id));
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn clear_preserves_ids_and_registries_are_isolated() {
        let first = Callbacks::default();
        let second = Callbacks::default();
        let old = first.insert(callback());
        let other = second.insert(callback());
        first.clear();
        let new = first.insert(callback());
        assert!(new > old);
        assert!(!contains(&first, old));
        assert!(contains(&second, other));
    }

    #[test]
    fn callback_can_mutate_its_registry_synchronously() {
        let callbacks = Callbacks::default();
        let registry = callbacks.clone();
        let called = Rc::new(Cell::new(false));
        let flag = called.clone();
        let id = callbacks.insert(Rc::new(move |_, _| {
            let nested = registry.insert(callback());
            registry.remove(nested);
            flag.set(true);
            Box::pin(async { Ok(Value::Null) })
        }));
        assert!(contains(&callbacks, id));
        assert!(called.get());
        callbacks.clear();
    }

    struct ReenterOnDrop(Callbacks, u64, Rc<Cell<bool>>);
    impl Drop for ReenterOnDrop {
        fn drop(&mut self) {
            self.0.remove(self.1);
            self.2.set(true);
        }
    }

    #[test]
    fn remove_and_clear_drop_captures_without_borrowing_registry() {
        for clear in [false, true] {
            let callbacks = Callbacks::default();
            let sentinel = callbacks.insert(callback());
            let dropped = Rc::new(Cell::new(false));
            let capture = ReenterOnDrop(callbacks.clone(), sentinel, dropped.clone());
            let id = callbacks.insert(Rc::new(move |_, _| {
                let _ = &capture;
                Box::pin(async { Ok(Value::Null) })
            }));
            if clear {
                callbacks.clear();
            } else {
                callbacks.remove(id);
            }
            assert!(dropped.get());
            assert!(!contains(&callbacks, sentinel));
        }
    }
}
