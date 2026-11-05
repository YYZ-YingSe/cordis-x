use cordis_host::{Body, Dependency, Failure};
use cordis_runtime as rt;
use cordis_sdk_core::{Descriptor, Interface, Message, MessageContract, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Semaphore;

fn input() -> Body {
    Message::from_json(MessageContract::json_value(), &json!({}))
        .unwrap()
        .into()
}
async fn owner(runtime: &rt::Runtime, endpoint: &Dependency) -> (rt::PluginHandle, rt::Context) {
    let saved = Arc::new(Mutex::new(None));
    let target = saved.clone();
    let owner = runtime
        .install(endpoint.declare(rt::Plugin::new("consumer", move |ctx| {
            *target.lock().unwrap() = Some(ctx.snapshot());
            async { Ok(()) }
        })))
        .unwrap();
    owner.ready().await.unwrap();
    let context = saved.lock().unwrap().take().unwrap();
    (owner, context)
}
#[tokio::test]
async fn outcome_unknown_is_not_reclassified_by_its_error_code() {
    let runtime = rt::Runtime::new();
    let service = rt::Service::<AtomicUsize>::new("counter", "1");
    let calls = Arc::new(AtomicUsize::new(0));
    let state = calls.clone();
    let provided = service.clone();
    runtime
        .install(
            rt::Plugin::new("provider", move |ctx| {
                let provided = provided.clone();
                let state = state.clone();
                async move { ctx.provide(&provided, state) }
            })
            .provides(&service),
        )
        .unwrap()
        .ready()
        .await
        .unwrap();
    let endpoint = Dependency::local(
        &service,
        Descriptor::new(Interface::value_dispatch(false)).unwrap(),
        |calls, _, _, _| {
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(Failure::Unknown(cordis_contract::ErrorInfo {
                    code: "closed".into(),
                    message: "acknowledgement lost after execution".into(),
                }))
            })
        },
    );
    let (owner, context) = owner(&runtime, &endpoint).await;
    assert!(
        matches!(endpoint.invoke(context.clone(), "work".into(), input()).await, Err(Failure::Unknown(e)) if e.code == "closed")
    );
    owner.dispose().await.unwrap();
    assert!(matches!(
        endpoint.invoke(context, "work".into(), input()).await,
        Err(Failure::Refused(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    runtime.shutdown().await.unwrap();
}
struct Control {
    entered: Semaphore,
    release: Semaphore,
    cleaned: AtomicUsize,
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_the_common_call_wait_does_not_drop_admitted_work_or_late_cleanup() {
    let runtime = rt::Runtime::new();
    let state = Arc::new(Control {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
        cleaned: AtomicUsize::new(0),
    });
    let service = rt::Service::<Arc<Control>>::new("control", "1");
    let provided = service.clone();
    let control = state.clone();
    runtime
        .install(
            rt::Plugin::new("provider", move |ctx| {
                let provided = provided.clone();
                let control = control.clone();
                async move { ctx.provide(&provided, Arc::new(control)) }
            })
            .provides(&service),
        )
        .unwrap()
        .ready()
        .await
        .unwrap();
    let endpoint = Dependency::local(
        &service,
        Descriptor::new(Interface::value_dispatch(false)).unwrap(),
        |control, ctx, _, args| {
            Box::pin(async move {
                control.entered.add_permits(1);
                control.release.acquire().await.unwrap().forget();
                let cleanup = control.clone();
                ctx.defer("late effect", move |_| async move {
                    cleanup.cleaned.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .map_err(cordis_host::unknown)?;
                Ok(args)
            })
        },
    );
    let (owner, context) = owner(&runtime, &endpoint).await;
    let waiter =
        tokio::spawn(async move { endpoint.invoke(context, "work".into(), input()).await });
    state.entered.acquire().await.unwrap().forget();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    let mut disposal = tokio::spawn(async move { owner.dispose().await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut disposal)
            .await
            .is_err()
    );
    assert_eq!(state.cleaned.load(Ordering::SeqCst), 0);
    state.release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(5), disposal)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(state.cleaned.load(Ordering::SeqCst), 1);
    runtime.shutdown().await.unwrap();
}
