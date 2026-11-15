//! References preserve the real caller and share the existing resource inverse.
use cordis_runtime::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Semaphore;

async fn owner(runtime: &Runtime, name: &str) -> (PluginHandle, Context) {
    let output = Arc::new(Mutex::new(None));
    let saved = output.clone();
    let plugin = runtime
        .install(Plugin::new(name, move |ctx| {
            let saved = saved.clone();
            async move {
                *saved.lock().unwrap() = Some(ctx);
                Ok(())
            }
        }))
        .unwrap();
    plugin.ready().await.unwrap();
    let ctx = output.lock().unwrap().take().unwrap();
    (plugin, ctx)
}

#[tokio::test]
async fn references_use_current_caller_and_retire_without_rebinding() {
    let runtime = Runtime::new();
    let (provider, exporting) = owner(&runtime, "provider").await;
    let (_, calling) = owner(&runtime, "consumer").await;
    let closed = Arc::new(AtomicUsize::new(0));
    let count = closed.clone();
    let key = exporting
        .export_reference("object", 41u32, move |_, _| async move {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap();
    let object = calling.reference::<u32>(&key).unwrap();
    let expected = calling.owner();
    let definition = exporting.activation();
    let value = object
        .call(&calling, move |value, ctx| {
            Box::pin(async move {
                assert_eq!(ctx.owner(), expected);
                assert_eq!(ctx.activation(), definition);
                Ok(*value + 1)
            })
        })
        .await
        .unwrap();
    assert_eq!(value, 42);
    let other = Runtime::new();
    let (_, foreign) = owner(&other, "foreign").await;
    assert!(foreign.reference::<u32>(&key).is_err());
    assert!(
        object
            .call(&foreign, |_, _| Box::pin(async { Ok(()) }))
            .await
            .is_err()
    );
    provider.dispose().await.unwrap();
    assert_eq!(closed.load(Ordering::SeqCst), 1);
    assert!(calling.find_reference::<u32>(&key).unwrap().is_none());
    assert!(
        object
            .call(&calling, |_, _| Box::pin(async { Ok(()) }))
            .await
            .is_err()
    );
    runtime.shutdown().await.unwrap();
    other.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_disposal_drains_a_call_and_closes_once() {
    let runtime = Runtime::new();
    let (_, ctx) = owner(&runtime, "objects").await;
    let closed = Arc::new(AtomicUsize::new(0));
    let count = closed.clone();
    let key = ctx
        .export_reference("object", (), move |_, _| async move {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap();
    let object = ctx.reference::<()>(&key).unwrap();
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let call = tokio::spawn({
        let object = object.clone();
        let ctx = ctx.clone();
        let entered = entered.clone();
        let release = release.clone();
        async move {
            object
                .call(&ctx, move |_, _| {
                    Box::pin(async move {
                        entered.add_permits(1);
                        release.acquire().await.unwrap().forget();
                        Ok(())
                    })
                })
                .await
        }
    });
    entered.acquire().await.unwrap().forget();
    let disposing = tokio::spawn({
        let object = object.clone();
        async move { object.dispose().await }
    });
    tokio::task::yield_now().await;
    assert_eq!(closed.load(Ordering::SeqCst), 0);
    release.add_permits(1);
    call.await.unwrap().unwrap();
    disposing.await.unwrap().unwrap();
    object.dispose().await.unwrap();
    assert_eq!(closed.load(Ordering::SeqCst), 1);
    runtime.shutdown().await.unwrap();
}
