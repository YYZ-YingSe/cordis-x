//! Exercise the public SDK through an adapter with no component dependencies.
use cordis_sdk_core::{
    backend::{Backend, Callback, Handle, Operation, Reply},
    *,
};
use std::{
    any::Any,
    cell::{Cell, RefCell},
    future::Future,
    rc::Rc,
    task::{Context as PollContext, Poll, Waker},
};

fn ready<T>(future: impl Future<Output = T>) -> T {
    let mut future = Box::pin(future);
    match future
        .as_mut()
        .poll(&mut PollContext::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("test adapter unexpectedly yielded"),
    }
}

struct Adapter {
    active: Rc<Cell<bool>>,
    phase: &'static str,
    cleanup: Rc<RefCell<Option<Callback>>>,
}
impl Backend for Adapter {
    fn submit(&self, operation: Operation) -> LocalFuture<Reply> {
        let active = self.active.clone();
        let phase = self.phase;
        let cleanup = self.cleanup.clone();
        Box::pin(async move {
            if !active.get() {
                return Err(refused("expired_context", "callback ended"));
            }
            match operation {
                Operation::Metadata { name, revision } => {
                    assert_eq!((name.as_str(), revision.as_str()), ("logger", "1"));
                    Ok(Reply::Value(json!({"phase": phase})))
                }
                Operation::Invoke {
                    dependency,
                    method,
                    args,
                    ..
                } => {
                    assert_eq!((dependency.as_str(), method.as_str()), ("logger", "echo"));
                    Ok(Reply::Message(args))
                }
                Operation::Defer {
                    label,
                    cleanup: callback,
                } => {
                    assert_eq!(label, "subscription");
                    *cleanup.borrow_mut() = Some(callback);
                    // The core does not require a component string grant.
                    Ok(Reply::Handle(Handle::new(42_u32)))
                }
                Operation::Dispose { effect } => {
                    assert_eq!(effect.downcast_ref::<u32>(), Some(&42));
                    let callback = cleanup.borrow_mut().take().expect("registered cleanup");
                    let ctx = Context::from_backend(Rc::new(Adapter {
                        active: Rc::new(Cell::new(true)),
                        phase: "cleanup",
                        cleanup: cleanup.clone(),
                    }));
                    callback(ctx, Value::Null).await.map(Reply::Value)
                }
                _ => Err(refused("unsupported_operation", "test adapter")),
            }
        })
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[test]
fn portable_callbacks_receive_fresh_authority_and_handles_remain_opaque() {
    let active = Rc::new(Cell::new(true));
    let cleanup = Rc::new(RefCell::new(None));
    let ctx = Context::from_backend(Rc::new(Adapter {
        active: active.clone(),
        phase: "call",
        cleanup: cleanup.clone(),
    }));
    assert_eq!(
        ready(ctx.metadata("logger", "1")).unwrap(),
        json!({"phase": "call"})
    );
    assert_eq!(
        ready(test_json::invoke(
            &ctx,
            "logger",
            "echo",
            json!(["hello", 42])
        ))
        .unwrap(),
        json!(["hello", 42.0])
    );

    let released = Rc::new(Cell::new(false));
    let flag = released.clone();
    let effect = ready(ctx.defer("subscription", move |cleanup| {
        let flag = flag.clone();
        async move {
            assert_eq!(
                cleanup.metadata("logger", "1").await?,
                json!({"phase": "cleanup"})
            );
            test_json::invoke(&cleanup, "logger", "echo", json!("released")).await?;
            flag.set(true);
            Ok(())
        }
    }))
    .unwrap();

    let copied = ctx.clone();
    let pending = copied.metadata("logger", "1");
    active.set(false);
    assert!(
        matches!(ready(pending), Err(Failure::Refused(info)) if info.code == "expired_context")
    );
    assert!(ready(effect.dispose(&copied)).is_err());
    assert!(!released.get());

    let later = Context::from_backend(Rc::new(Adapter {
        active: Rc::new(Cell::new(true)),
        phase: "later call",
        cleanup,
    }));
    ready(effect.dispose(&later)).unwrap();
    assert!(released.get());
}

#[test]
fn final_failed_step_keeps_its_inverse_until_the_adapter_takes_it() {
    let released = Rc::new(Cell::new(false));
    let flag = released.clone();
    let step = Step::yield_inverse(move |_| {
        let flag = flag.clone();
        async move {
            flag.set(true);
            Ok(())
        }
    })
    .finish()
    .fail("later acquisition failed");
    assert!(!released.get());
    let (inverse, complete, failure) = step.into_parts();
    assert!(complete);
    assert_eq!(failure.as_deref(), Some("later acquisition failed"));
    let ctx = Context::from_backend(Rc::new(Adapter {
        active: Rc::new(Cell::new(true)),
        phase: "cleanup",
        cleanup: Default::default(),
    }));
    ready(inverse.unwrap()(ctx, Value::Null)).unwrap();
    assert!(released.get());
}

#[path = "../../tests/support/json.rs"]
#[allow(dead_code)]
mod test_json;
