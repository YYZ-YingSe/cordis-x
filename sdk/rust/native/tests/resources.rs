use cordis_sdk::*;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("resource did not settle")
}
async fn owner(rt: &Runtime) -> (PluginHandle, Context) {
    let saved = Arc::new(Mutex::new(None));
    let handle = rt
        .install(Plugin::new("owner", {
            let saved = saved.clone();
            move |ctx| {
                *saved.lock().unwrap() = Some(ctx);
                async { Ok(()) }
            }
        }))
        .unwrap();
    bounded(handle.ready()).await.unwrap();
    let context = saved.lock().unwrap().take().unwrap();
    (handle, context)
}

#[tokio::test]
async fn dropped_acquisition_wait_still_records_and_releases_late_resource() {
    let rt = Runtime::new();
    let (owner, ctx) = owner(&rt).await;
    let started = Arc::new(Semaphore::new(0));
    let finish = Arc::new(Semaphore::new(0));
    let cleaned = Arc::new(AtomicUsize::new(0));
    let waiter = tokio::spawn({
        let finish = finish.clone();
        let started = started.clone();
        let cleaned = cleaned.clone();
        async move {
            ctx.acquire(
                "late resource",
                async move {
                    started.add_permits(1);
                    finish.acquire().await.unwrap().forget();
                    Ok(42)
                },
                move |value, _| async move {
                    assert_eq!(*value, 42);
                    cleaned.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
            )
            .await
        }
    });
    bounded(started.acquire()).await.unwrap().forget();
    waiter.abort();
    let _ = waiter.await;
    let dispose = tokio::spawn({
        let owner = owner.clone();
        async move { owner.dispose().await }
    });
    bounded(async {
        while owner.snapshot().state != Lifecycle::Unloading {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!dispose.is_finished());
    assert_eq!(cleaned.load(Ordering::SeqCst), 0);
    finish.add_permits(1);
    bounded(dispose).await.unwrap().unwrap();
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn concurrent_dispose_and_cancelled_wait_share_one_inverse() {
    let rt = Runtime::new();
    let (owner, ctx) = owner(&rt).await;
    let started = Arc::new(Semaphore::new(0));
    let finish = Arc::new(Semaphore::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let effect = ctx
        .defer("manual", {
            let calls = calls.clone();
            let started = started.clone();
            let finish = finish.clone();
            move |_| async move {
                calls.fetch_add(1, Ordering::SeqCst);
                started.add_permits(1);
                finish.acquire().await.unwrap().forget();
                Ok(())
            }
        })
        .unwrap();
    let first = tokio::spawn({
        let effect = effect.clone();
        async move { effect.dispose().await }
    });
    bounded(started.acquire()).await.unwrap().forget();
    first.abort();
    let _ = first.await;
    let second = tokio::spawn({
        let effect = effect.clone();
        async move { effect.dispose().await }
    });
    let disposing = tokio::spawn({
        let owner = owner.clone();
        async move { owner.dispose().await }
    });
    finish.add_permits(1);
    bounded(second).await.unwrap().unwrap();
    bounded(disposing).await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(effect.snapshot().status, EffectStatus::Released);
    effect.dispose().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn service_created_resource_and_disposal_token_outlive_the_creating_call() {
    let rt = Runtime::new();
    let factory = Service::<()>::new("factory", "1");
    let provider = rt
        .install(
            Plugin::new("factory", {
                let factory = factory.clone();
                move |ctx| {
                    let factory = factory.clone();
                    async move { ctx.provide(&factory, Arc::new(())) }
                }
            })
            .provides(&factory),
        )
        .unwrap();
    let saved = Arc::new(Mutex::new(None));
    let owner = rt
        .install(
            Plugin::new("client", {
                let factory = factory.clone();
                let saved = saved.clone();
                move |ctx| {
                    let factory = factory.clone();
                    let saved = saved.clone();
                    async move {
                        *saved.lock().unwrap() = Some(ctx.dependency(&factory)?);
                        Ok(())
                    }
                }
            })
            .requires(&factory),
        )
        .unwrap();
    bounded(owner.ready()).await.unwrap();
    let service = saved.lock().unwrap().take().unwrap();
    let cleaned = Arc::new(AtomicUsize::new(0));
    let resource = service
        .call({
            let cleaned = cleaned.clone();
            move |_, ctx| {
                Box::pin(async move {
                    ctx.acquire("session", async { Ok(42) }, move |_, _| async move {
                        cleaned.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
                })
            }
        })
        .await
        .unwrap();
    assert_eq!(resource.with(|v| *v).unwrap(), 42);
    let token = service
        .with({
            let cleaned = cleaned.clone();
            move |_, ctx| {
                ctx.defer("subscription", move |_| async move {
                    cleaned.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            }
        })
        .unwrap();
    token.dispose().await.unwrap();
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    assert!(provider.effects().is_empty());
    assert_eq!(owner.effects().len(), 2);
    bounded(owner.dispose()).await.unwrap();
    assert_eq!(cleaned.load(Ordering::SeqCst), 2);
    assert!(matches!(resource.with(|v| *v), Err(Error::Closed)));
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn dropped_cleanup_call_wait_finishes_before_earlier_resources_are_released() {
    let rt = Runtime::new();
    let service = Service::<()>::new("cleanup-dependency", "1");
    rt.install(
        Plugin::new("provider", {
            let service = service.clone();
            move |ctx| {
                let service = service.clone();
                async move { ctx.provide(&service, Arc::new(())) }
            }
        })
        .provides(&service),
    )
    .unwrap();
    let started = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let returned = Arc::new(Semaphore::new(0));
    let open = Arc::new(AtomicBool::new(true));
    let consumer = rt
        .install(
            Plugin::new("consumer", {
                let service = service.clone();
                let started = started.clone();
                let release = release.clone();
                let returned = returned.clone();
                let open = open.clone();
                move |ctx| {
                    let service = service.clone();
                    let started = started.clone();
                    let release = release.clone();
                    let returned = returned.clone();
                    let open = open.clone();
                    async move {
                        let close = open.clone();
                        ctx.defer("earlier resource", move |_| async move {
                            close.store(false, Ordering::SeqCst);
                            Ok(())
                        })?;
                        ctx.defer("cleanup call", move |ctx| async move {
                            let handle = ctx.dependency(&service)?;
                            let observed = started.clone();
                            let wait = tokio::spawn(async move {
                                handle
                                    .call(move |_, _| {
                                        Box::pin(async move {
                                            started.add_permits(1);
                                            release.acquire().await.unwrap().forget();
                                            assert!(open.load(Ordering::SeqCst));
                                            Ok(())
                                        })
                                    })
                                    .await
                            });
                            observed.acquire().await.unwrap().forget();
                            wait.abort();
                            let _ = wait.await;
                            returned.add_permits(1);
                            Ok(())
                        })?;
                        Ok(())
                    }
                }
            })
            .requires(&service),
        )
        .unwrap();
    bounded(consumer.ready()).await.unwrap();
    let disposal = tokio::spawn(async move { consumer.dispose().await });
    bounded(returned.acquire()).await.unwrap().forget();
    assert!(open.load(Ordering::SeqCst));
    assert!(!disposal.is_finished());
    release.add_permits(1);
    bounded(disposal).await.unwrap().unwrap();
    assert!(!open.load(Ordering::SeqCst));
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn trait_object_services_are_checked_and_shutdown_rejects_new_installs() {
    trait Counter: Send + Sync {
        fn count(&self) -> usize;
    }
    struct NativeCounter;
    impl Counter for NativeCounter {
        fn count(&self) -> usize {
            42
        }
    }
    let rt = Runtime::new();
    let counter = Service::<dyn Counter>::new("counter", "1");
    rt.install(
        Plugin::new("provider", {
            let counter = counter.clone();
            move |ctx| {
                let counter = counter.clone();
                async move { ctx.provide(&counter, Arc::new(NativeCounter)) }
            }
        })
        .provides(&counter),
    )
    .unwrap();
    let consumer = rt
        .install(
            Plugin::new("consumer", {
                let counter = counter.clone();
                move |ctx| {
                    let counter = counter.clone();
                    async move {
                        assert_eq!(
                            ctx.dependency(&counter)?
                                .with(|counter, _| Ok(counter.count()))?,
                            42
                        );
                        assert!(matches!(
                            ctx.dependency(&Service::<usize>::new("counter", "1")),
                            Err(Error::TypeMismatch(_))
                        ));
                        Ok(())
                    }
                }
            })
            .requires(&counter),
        )
        .unwrap();
    bounded(consumer.ready()).await.unwrap();
    bounded(rt.shutdown()).await.unwrap();
    assert!(matches!(
        rt.install(Plugin::new("late", |_| async { Ok(()) })),
        Err(Error::RuntimeStopped)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retirement_releases_plugin_captures_outside_the_coordinator_lock() {
    struct Capture {
        runtime: Runtime,
        dropped: Arc<Semaphore>,
    }
    impl Drop for Capture {
        fn drop(&mut self) {
            // Re-entering the coordinator must not deadlock a destructor.
            let _ = self.runtime.snapshots();
            self.dropped.add_permits(1);
        }
    }
    let rt = Runtime::new();
    let dropped = Arc::new(Semaphore::new(0));
    for missing_dependency in [false, true] {
        let capture = Capture {
            runtime: rt.clone(),
            dropped: dropped.clone(),
        };
        let mut plugin = Plugin::new("captured", move |_| {
            std::hint::black_box(&capture);
            async { Ok(()) }
        });
        if missing_dependency {
            plugin = plugin.requires(&Service::<()>::new("missing", "1"));
        }
        let handle = rt.install(plugin).unwrap();
        bounded(handle.settled()).await.unwrap();
        bounded(handle.dispose()).await.unwrap();
        bounded(dropped.acquire()).await.unwrap().forget();
        assert_eq!(handle.snapshot().state, Lifecycle::Inactive);
    }
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn manual_resource_disposal_drains_calls_and_closes_all_clones() {
    let rt = Runtime::new();
    let (_, ctx) = owner(&rt).await;
    let closed = Arc::new(AtomicUsize::new(0));
    let resource = ctx
        .acquire("resource", async { Ok(42) }, {
            let closed = closed.clone();
            move |_, _| async move {
                closed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .unwrap();
    let started = Arc::new(Semaphore::new(0));
    let finish = Arc::new(Semaphore::new(0));
    let call = tokio::spawn({
        let resource = resource.clone();
        let started = started.clone();
        let finish = finish.clone();
        async move {
            resource
                .call(move |value, _| {
                    Box::pin(async move {
                        started.add_permits(1);
                        finish.acquire().await.unwrap().forget();
                        assert_eq!(*value, 42);
                        Ok(())
                    })
                })
                .await
        }
    });
    bounded(started.acquire()).await.unwrap().forget();
    call.abort();
    let _ = call.await;
    let dispose = tokio::spawn({
        let resource = resource.clone();
        async move { resource.dispose().await }
    });
    bounded(async {
        while resource.snapshot().status != EffectStatus::Releasing {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(closed.load(Ordering::SeqCst), 0);
    assert!(!dispose.is_finished());
    finish.add_permits(1);
    bounded(dispose).await.unwrap().unwrap();
    assert!(matches!(resource.with(|v| *v), Err(Error::Closed)));
    resource.dispose().await.unwrap();
    bounded(rt.shutdown()).await.unwrap();
    assert_eq!(closed.load(Ordering::SeqCst), 1);
}
