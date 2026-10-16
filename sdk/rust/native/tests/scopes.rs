use cordis_sdk::*;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Semaphore;

async fn bounded<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), f)
        .await
        .expect("scope did not settle")
}
async fn parent(rt: &Runtime) -> (PluginHandle, Context) {
    let saved = Arc::new(Mutex::new(None));
    let handle = rt
        .install(Plugin::new("parent", {
            let saved = saved.clone();
            move |ctx| {
                let saved = saved.clone();
                async move {
                    *saved.lock().unwrap() = Some(ctx);
                    Ok(())
                }
            }
        }))
        .unwrap();
    bounded(handle.ready()).await.unwrap();
    let ctx = saved.lock().unwrap().take().unwrap();
    (handle, ctx)
}

#[tokio::test]
async fn scope_groups_resources_sequences_and_children_and_expires_run_contexts() {
    let rt = Runtime::new();
    let (parent, ctx) = parent(&rt).await;
    let trace = Arc::new(Mutex::new(Vec::new()));
    let scope = ctx.scope("connection").unwrap();
    let weak = scope.downgrade();
    let (resource, stale) = scope
        .run({
            let trace = trace.clone();
            move |ctx| async move {
                let t = trace.clone();
                ctx.defer("first", move |_| async move {
                    t.lock().unwrap().push("first");
                    Ok(())
                })?;
                let t = trace.clone();
                let resource = ctx
                    .acquire("buffer", async { Ok(vec![1]) }, move |_, _| async move {
                        t.lock().unwrap().push("buffer");
                        Ok(())
                    })
                    .await?;
                let mut once = Some(trace.clone());
                let sequence = ctx.effect("sequence", move |_| {
                    let t = once.take();
                    async move {
                        Ok(t.map(|t| {
                            Inverse::new("step", move |_| async move {
                                t.lock().unwrap().push("step");
                                Ok(())
                            })
                        }))
                    }
                })?;
                sequence.ready().await?;
                let nested = ctx.scope("nested")?;
                nested
                    .run(move |ctx| async move {
                        ctx.defer("nested", move |_| async move {
                            trace.lock().unwrap().push("nested");
                            Ok(())
                        })?;
                        Ok(())
                    })
                    .await?;
                Ok((resource, ctx))
            }
        })
        .await
        .unwrap();
    assert_eq!(
        stale.defer("expired", |_| async { Ok(()) }).err(),
        Some(Error::Closed)
    );
    assert_eq!(resource.with(|v| v.len()).unwrap(), 1);
    drop(scope);
    let scope = weak.upgrade().unwrap();
    bounded(scope.dispose()).await.unwrap();
    bounded(scope.dispose()).await.unwrap();
    assert_eq!(
        *trace.lock().unwrap(),
        ["nested", "step", "buffer", "first"]
    );
    assert!(resource.with(|_| ()).is_err());
    assert!(scope.run(|_| async { Ok(()) }).await.is_err());
    assert_eq!(parent.snapshot().state, Lifecycle::Active);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_scope_run_keeps_late_resource_owned_until_parent_recovery() {
    let rt = Runtime::new();
    let (parent, ctx) = parent(&rt).await;
    let trace = Arc::new(Mutex::new(Vec::new()));
    let t = trace.clone();
    ctx.defer("parent", move |_| async move {
        t.lock().unwrap().push("parent");
        Ok(())
    })
    .unwrap();
    let scope = ctx.scope("child").unwrap();
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let run = tokio::spawn({
        let scope = scope.clone();
        let entered = entered.clone();
        let release = release.clone();
        let trace = trace.clone();
        async move {
            scope
                .run(move |ctx| async move {
                    ctx.acquire(
                        "late",
                        async move {
                            entered.add_permits(1);
                            release.acquire().await.unwrap().forget();
                            Ok(())
                        },
                        move |_, _| async move {
                            trace.lock().unwrap().push("late");
                            Ok(())
                        },
                    )
                    .await?;
                    Ok(())
                })
                .await
        }
    });
    bounded(entered.acquire()).await.unwrap().forget();
    run.abort();
    let _ = run.await;
    let closing = tokio::spawn({
        let parent = parent.clone();
        async move { parent.dispose().await }
    });
    bounded(async {
        while scope.snapshot().state != Lifecycle::Unloading {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(trace.lock().unwrap().is_empty());
    release.add_permits(1);
    bounded(closing).await.unwrap().unwrap();
    assert_eq!(*trace.lock().unwrap(), ["late", "parent"]);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn scope_preserves_defining_dependencies_across_use_views_and_blocks_on_failed_cleanup() {
    let rt = Runtime::new();
    let db = Service::<usize>::new("database", "1");
    let api = Service::<()>::new("api", "1");
    let definition_view = View::new().isolate(&db);
    rt.install_in(
        definition_view.clone(),
        Plugin::new("db", {
            let db = db.clone();
            move |ctx| {
                let db = db.clone();
                async move { ctx.provide(&db, Arc::new(42)) }
            }
        })
        .provides(&db),
    )
    .unwrap();
    let provider = rt
        .install_in(
            definition_view,
            Plugin::new("api", {
                let api = api.clone();
                move |ctx| {
                    let api = api.clone();
                    async move { ctx.provide(&api, Arc::new(())) }
                }
            })
            .requires(&db)
            .provides(&api),
        )
        .unwrap();
    let saved = Arc::new(Mutex::new(None));
    let consumer = rt
        .install_in(
            View::new().isolate(&db),
            Plugin::new("consumer", {
                let saved = saved.clone();
                let api = api.clone();
                move |ctx| {
                    let saved = saved.clone();
                    let api = api.clone();
                    async move {
                        *saved.lock().unwrap() = Some(ctx.dependency(&api)?);
                        Ok(())
                    }
                }
            })
            .requires(&api),
        )
        .unwrap();
    bounded(consumer.ready()).await.unwrap();
    let service = saved.lock().unwrap().take().unwrap();
    let scope = service
        .call(|_, ctx| Box::pin(async move { ctx.scope("child") }))
        .await
        .unwrap();
    scope
        .run(move |ctx| async move {
            assert_eq!(ctx.dependency(&db)?.with(|v, _| Ok(*v))?, 42);
            ctx.defer("fails", move |ctx| async move {
                assert_eq!(ctx.dependency(&db)?.with(|v, _| Ok(*v))?, 42);
                Err(Error::Message("failed child cleanup".into()))
            })?;
            Ok(())
        })
        .await
        .unwrap();
    let first = bounded(scope.dispose()).await.unwrap_err();
    assert_eq!(bounded(scope.dispose()).await.unwrap_err(), first);
    assert!(bounded(consumer.dispose()).await.is_err());
    assert!(bounded(provider.dispose()).await.is_err());
    assert!(bounded(rt.shutdown()).await.is_err());
}
