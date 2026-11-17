//! Middleware shares invocation ownership while retaining handler implementation
//! lifetime. Registration is a subscription, not the owner of future effects.
use cordis_runtime::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Semaphore;

async fn owner(runtime: &Runtime, name: &str) -> (PluginHandle, Context) {
    let result = Arc::new(Mutex::new(None));
    let output = result.clone();
    let plugin = runtime
        .install(Plugin::new(name, move |ctx| {
            *output.lock().unwrap() = Some(ctx);
            async { Ok(()) }
        }))
        .unwrap();
    plugin.ready().await.unwrap();
    let ctx = result.lock().unwrap().take().unwrap();
    (plugin, ctx)
}

#[tokio::test]
async fn middleware_effects_belong_to_each_invoking_scope() {
    let runtime = Runtime::new();
    let (provider, definition) = owner(&runtime, "handler").await;
    let (_, caller) = owner(&runtime, "caller").await;
    let chain = Middleware::<u32, u32>::new("scope.example", "1");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let closed = Arc::new(AtomicUsize::new(0));
    let expected = definition.activation();
    definition
        .middleware(&chain, {
            let seen = seen.clone();
            let closed = closed.clone();
            move |value, ctx, next| {
                let seen = seen.clone();
                let closed = closed.clone();
                async move {
                    assert_eq!(ctx.activation(), expected);
                    assert_ne!(ctx.owner(), expected);
                    seen.lock().unwrap().push(ctx.owner());
                    let child = ctx.scope("nested implementation resources")?;
                    child
                        .run(move |ctx| async move {
                            assert_eq!(ctx.activation(), expected);
                            ctx.defer("invocation cleanup", move |_| async move {
                                closed.fetch_add(1, Ordering::SeqCst);
                                Ok(())
                            })?;
                            Ok(())
                        })
                        .await?;
                    Ok(next.run(value + 1).await? + 1)
                }
            }
        })
        .unwrap();
    let a = caller.scope("a").unwrap();
    let b = caller.scope("b").unwrap();
    let invoke = |scope: ScopeHandle| {
        let chain = chain.clone();
        async move {
            scope
                .run(move |ctx| async move {
                    let expected = ctx.owner();
                    chain
                        .run(&ctx, 3, move |value, terminal| async move {
                            assert_eq!(terminal.owner(), expected);
                            Ok(value)
                        })
                        .await
                })
                .await
                .unwrap()
        }
    };
    let (ra, rb) = tokio::join!(invoke(a.clone()), invoke(b.clone()));
    assert_eq!((ra, rb), (5, 5));
    let owners = seen.lock().unwrap().clone();
    assert_eq!(owners.len(), 2);
    assert_ne!(owners[0], owners[1]);
    assert_eq!(closed.load(Ordering::SeqCst), 0);
    a.dispose().await.unwrap();
    assert_eq!(closed.load(Ordering::SeqCst), 1);
    b.dispose().await.unwrap();
    assert_eq!(closed.load(Ordering::SeqCst), 2);
    assert!(!provider.snapshot().retired);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn abandoned_middleware_wait_still_drains_effects_under_caller() {
    let runtime = Runtime::new();
    let (_, definition) = owner(&runtime, "handler").await;
    let (_, caller) = owner(&runtime, "caller").await;
    let chain = Middleware::<(), ()>::new("scope.wait", "1");
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let closed = Arc::new(AtomicUsize::new(0));
    definition
        .middleware(&chain, {
            let entered = entered.clone();
            let release = release.clone();
            let closed = closed.clone();
            move |_, ctx, next| {
                let entered = entered.clone();
                let release = release.clone();
                let closed = closed.clone();
                async move {
                    ctx.defer("cleanup", move |_| async move {
                        closed.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })?;
                    entered.add_permits(1);
                    release.acquire().await.unwrap().forget();
                    next.run(()).await
                }
            }
        })
        .unwrap();
    let scope = caller.scope("operation").unwrap();
    let calling = scope.clone();
    let wait = tokio::spawn(async move {
        calling
            .run(move |ctx| async move { chain.run(&ctx, (), |_, _| async { Ok(()) }).await })
            .await
    });
    entered.acquire().await.unwrap().forget();
    wait.abort();
    let _ = wait.await;
    let closing = tokio::spawn(async move { scope.dispose().await });
    tokio::task::yield_now().await;
    assert!(!closing.is_finished());
    assert_eq!(closed.load(Ordering::SeqCst), 0);
    release.add_permits(1);
    closing.await.unwrap().unwrap();
    assert_eq!(closed.load(Ordering::SeqCst), 1);
    runtime.shutdown().await.unwrap();
}
