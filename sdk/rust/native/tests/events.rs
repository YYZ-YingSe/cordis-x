use cordis_sdk::*;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("dispatch did not settle")
}

async fn emitter(rt: &Runtime) -> (PluginHandle, Context) {
    let context = Arc::new(Mutex::new(None));
    let handle = rt
        .install(Plugin::new("emitter", {
            let context = context.clone();
            move |ctx| {
                *context.lock().unwrap() = Some(ctx);
                async { Ok(()) }
            }
        }))
        .unwrap();
    bounded(handle.ready()).await.unwrap();
    let ctx = context.lock().unwrap().take().unwrap();
    (handle, ctx)
}

#[tokio::test]
async fn manual_unsubscribe_drains_detached_operations_and_keeps_their_late_inverse() {
    let rt = Runtime::new();
    let (owner, ctx) = emitter(&rt).await;
    let event = Event::<()>::new("drain", "1");
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let cleaned = Arc::new(AtomicBool::new(false));
    let subscription = ctx
        .on(&event, {
            let (entered, release, cleaned) = (entered.clone(), release.clone(), cleaned.clone());
            move |(), callback| {
                let (entered, release, cleaned) =
                    (entered.clone(), release.clone(), cleaned.clone());
                async move {
                    let started = entered.clone();
                    let wait = tokio::spawn(async move {
                        callback
                            .operation(move |operation| async move {
                                started.add_permits(1);
                                release.acquire().await.unwrap().forget();
                                operation.defer("late", move |cleanup| async move {
                                    cleanup.operation(|_| async { Ok(()) }).await?;
                                    cleaned.store(true, Ordering::SeqCst);
                                    Ok(())
                                })?;
                                Ok(())
                            })
                            .await
                    });
                    entered.acquire().await.unwrap().forget();
                    wait.abort();
                    let _ = wait.await;
                    Ok(Continue(()))
                }
            }
        })
        .unwrap();
    bounded(event.parallel(&ctx, ())).await.unwrap();
    let disposing = tokio::spawn({
        let subscription = subscription.clone();
        async move { subscription.dispose().await }
    });
    bounded(async {
        while subscription.snapshot().status == EffectStatus::Owned {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!disposing.is_finished());
    release.add_permits(1);
    bounded(disposing).await.unwrap().unwrap();
    assert!(event.parallel(&ctx, ()).await.unwrap().is_empty());
    bounded(owner.dispose()).await.unwrap();
    assert!(cleaned.load(Ordering::SeqCst));
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn nested_listener_and_managed_task_outlive_the_creating_registration() {
    let rt = Runtime::new();
    let (_, ctx) = emitter(&rt).await;
    let outer = Event::<()>::new("outer", "1");
    let nested = Event::<(), i32>::new("nested", "1");
    let registration = ctx
        .on(&outer, {
            let nested = nested.clone();
            move |(), callback| {
                let nested = nested.clone();
                async move {
                    callback.on(&nested, |(), ctx| async move {
                        ctx.operation(|_| async { Ok(()) }).await?;
                        Ok(Break(7))
                    })?;
                    callback.spawn("background", |worker| async move {
                        worker.cancelled().await;
                        Ok(())
                    })?;
                    Ok(Continue(()))
                }
            }
        })
        .unwrap();
    outer.parallel(&ctx, ()).await.unwrap();
    bounded(registration.dispose()).await.unwrap();
    assert_eq!(nested.parallel(&ctx, ()).await.unwrap(), vec![Break(7)]);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn registrations_have_independent_ownership_order_and_realms() {
    let rt = Runtime::new();
    let (_, ctx) = emitter(&rt).await;
    let event = Event::<(), usize>::new("changed", "1");
    let mut handles = Vec::new();
    for id in 1..=3 {
        let event = event.clone();
        let handle = rt
            .install(Plugin::new(format!("listener-{id}"), move |ctx| {
                let event = event.clone();
                async move {
                    ctx.on(&event, move |(), _| async move { Ok(Break(id)) })?;
                    Ok(())
                }
            }))
            .unwrap();
        bounded(handle.ready()).await.unwrap();
        handles.push(handle);
    }
    assert_eq!(
        event.parallel(&ctx, ()).await.unwrap(),
        vec![Break(1), Break(2), Break(3)]
    );
    bounded(handles[0].dispose()).await.unwrap();
    assert_eq!(
        event.parallel(&ctx, ()).await.unwrap(),
        vec![Break(2), Break(3)]
    );
    assert_eq!(event.serial(&ctx, ()).await.unwrap(), Break(2));
    assert!(
        event
            .in_realm(RealmId(999))
            .parallel(&ctx, ())
            .await
            .unwrap()
            .is_empty()
    );
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn only_explicit_initialization_listeners_run_before_publication() {
    let rt = Runtime::new();
    let (_, ctx) = emitter(&rt).await;
    let event = Event::<(), usize>::new("handshake", "1");
    let registered = Arc::new(Semaphore::new(0));
    let finish = Arc::new(Semaphore::new(0));
    let handle = rt
        .install(Plugin::new("initializing", {
            let event = event.clone();
            let registered = registered.clone();
            let finish = finish.clone();
            move |ctx| {
                let event = event.clone();
                let registered = registered.clone();
                let finish = finish.clone();
                async move {
                    ctx.on(&event, |(), _| async { Ok(Break(1)) })?;
                    ctx.on_initializing(&event, |(), _| async { Ok(Break(2)) })?;
                    registered.add_permits(1);
                    finish.acquire().await.unwrap().forget();
                    Ok(())
                }
            }
        }))
        .unwrap();
    bounded(registered.acquire()).await.unwrap().forget();
    assert_eq!(event.parallel(&ctx, ()).await.unwrap(), vec![Break(2)]);
    finish.add_permits(1);
    bounded(handle.ready()).await.unwrap();
    assert_eq!(
        event.parallel(&ctx, ()).await.unwrap(),
        vec![Break(1), Break(2)]
    );
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn parallel_waits_for_all_errors_and_panics_while_serial_short_circuits() {
    let rt = Runtime::new();
    let (_, ctx) = emitter(&rt).await;
    let event = Event::<(), usize>::new("errors", "1");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let handle = rt
        .install(Plugin::new("listeners", {
            let event = event.clone();
            let calls = calls.clone();
            move |ctx| {
                let event = event.clone();
                let calls = calls.clone();
                async move {
                    for id in 0..3 {
                        let calls = calls.clone();
                        ctx.on(&event, move |(), _| {
                            let calls = calls.clone();
                            async move {
                                calls.lock().unwrap().push(id);
                                match id {
                                    0 => Ok(Break(7)),
                                    1 => Err(Error::Message("failure".into())),
                                    _ => panic!("listener panic"),
                                }
                            }
                        })?;
                    }
                    Ok(())
                }
            }
        }))
        .unwrap();
    bounded(handle.ready()).await.unwrap();
    assert_eq!(event.serial(&ctx, ()).await.unwrap(), Break(7));
    assert_eq!(*calls.lock().unwrap(), vec![0]);
    calls.lock().unwrap().clear();
    let error = bounded(event.parallel(&ctx, ())).await.unwrap_err();
    assert!(
        matches!(error, Error::Aggregate(ref errors) if errors.len() == 2 && matches!(&errors[1], Error::Panic(_)))
    );
    assert_eq!(calls.lock().unwrap().len(), 3);
    assert_eq!(rt.diagnostics(), vec![error]);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_dispatch_wait_retains_listener_and_emitter_until_callback_finishes() {
    let rt = Runtime::new();
    let (emitter, ctx) = emitter(&rt).await;
    let event = Event::<()>::new("slow", "1");
    let started = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let cleaned = Arc::new(AtomicBool::new(false));
    let handle = rt
        .install(Plugin::new("listener", {
            let event = event.clone();
            let started = started.clone();
            let release = release.clone();
            let cleaned = cleaned.clone();
            move |ctx| {
                let event = event.clone();
                let started = started.clone();
                let release = release.clone();
                let cleaned = cleaned.clone();
                async move {
                    ctx.defer("resource", move |_| async move {
                        cleaned.store(true, Ordering::SeqCst);
                        Ok(())
                    })?;
                    ctx.on(&event, move |(), ctx| {
                        let started = started.clone();
                        let release = release.clone();
                        async move {
                            started.add_permits(1);
                            release.acquire().await.unwrap().forget();
                            ctx.defer("late callback resource", |_| async { Ok(()) })?;
                            Err(Error::Message("late failure".into()))
                        }
                    })?;
                    Ok(())
                }
            }
        }))
        .unwrap();
    bounded(handle.ready()).await.unwrap();
    drop(event.emit(&ctx, ()).unwrap());
    bounded(started.acquire()).await.unwrap().forget();
    let shutdown = tokio::spawn({
        let rt = rt.clone();
        async move { rt.shutdown().await }
    });
    bounded(async {
        while emitter.snapshot().state != Lifecycle::Unloading
            || handle.snapshot().state != Lifecycle::Unloading
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!cleaned.load(Ordering::SeqCst));
    assert!(!shutdown.is_finished());
    release.add_permits(1);
    bounded(shutdown).await.unwrap().unwrap();
    assert!(cleaned.load(Ordering::SeqCst));
    assert_eq!(rt.diagnostics().len(), 1);
    assert!(matches!(event.emit(&ctx, ()), Err(Error::Closed)));
}

#[tokio::test]
async fn middleware_nests_transforms_and_rejects_duplicate_and_expired_next() {
    let rt = Runtime::new();
    let (_, ctx) = emitter(&rt).await;
    let middleware = Middleware::<usize, usize>::new("pipeline", "1");
    let log = Arc::new(Mutex::new(Vec::new()));
    let saved = Arc::new(Mutex::new(None));
    let handle = rt
        .install(Plugin::new("middleware", {
            let middleware = middleware.clone();
            let log = log.clone();
            let saved = saved.clone();
            move |ctx| {
                let middleware = middleware.clone();
                let log = log.clone();
                let saved = saved.clone();
                async move {
                    let outer = log.clone();
                    ctx.middleware(&middleware, move |value, _, next| {
                        let log = outer.clone();
                        let saved = saved.clone();
                        async move {
                            log.lock().unwrap().push("outer before");
                            *saved.lock().unwrap() = Some(next.clone());
                            let result = next.run(value + 1).await?;
                            assert_eq!(next.run(0).await, Err(Error::DuplicateNext));
                            log.lock().unwrap().push("outer after");
                            Ok(result + 100)
                        }
                    })?;
                    ctx.middleware(&middleware, move |value, _, next| {
                        let log = log.clone();
                        async move {
                            log.lock().unwrap().push("inner before");
                            let result = next.run(value * 2).await?;
                            log.lock().unwrap().push("inner after");
                            Ok(result + 10)
                        }
                    })?;
                    Ok(())
                }
            }
        }))
        .unwrap();
    bounded(handle.ready()).await.unwrap();
    let terminal_log = log.clone();
    let result = bounded(middleware.run(&ctx, 2, move |value, _| {
        let log = terminal_log.clone();
        async move {
            log.lock().unwrap().push("terminal");
            Ok(value)
        }
    }))
    .await
    .unwrap();
    assert_eq!(result, 116);
    assert_eq!(
        *log.lock().unwrap(),
        vec![
            "outer before",
            "inner before",
            "terminal",
            "inner after",
            "outer after"
        ]
    );
    let next = saved.lock().unwrap().take().unwrap();
    assert_eq!(next.run(0).await, Err(Error::ExpiredContinuation));
    // Retaining an expired continuation must not pin a live activation.
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn unpolled_next_future_expires_when_its_frame_returns() {
    let rt = Runtime::new();
    let (_, ctx) = emitter(&rt).await;
    let middleware = Middleware::<(), usize>::new("deferred-next", "1");
    let saved = Arc::new(Mutex::new(None));
    let handle = rt
        .install(Plugin::new("middleware", {
            let middleware = middleware.clone();
            let saved = saved.clone();
            move |ctx| {
                let middleware = middleware.clone();
                let saved = saved.clone();
                async move {
                    ctx.middleware(&middleware, move |(), _, next| {
                        let saved = saved.clone();
                        async move {
                            *saved.lock().unwrap() = Some(next.run(()));
                            Ok(7)
                        }
                    })?;
                    Ok(())
                }
            }
        }))
        .unwrap();
    bounded(handle.ready()).await.unwrap();
    assert_eq!(
        middleware
            .run(&ctx, (), |(), _| async { panic!("expired terminal ran") })
            .await
            .unwrap(),
        7
    );
    let future = saved.lock().unwrap().take().unwrap();
    assert_eq!(future.await, Err(Error::ExpiredContinuation));
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detached_next_and_cancelled_root_wait_cannot_bypass_shutdown_barrier() {
    let rt = Runtime::new();
    let (_, ctx) = emitter(&rt).await;
    let middleware = Middleware::<(), usize>::new("detached-next", "1");
    let started = Arc::new(Semaphore::new(0));
    let returned = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let cleaned = Arc::new(AtomicBool::new(false));
    let handle = rt
        .install(Plugin::new("middleware", {
            let middleware = middleware.clone();
            let started = started.clone();
            let returned = returned.clone();
            let cleaned = cleaned.clone();
            move |ctx| {
                let middleware = middleware.clone();
                let started = started.clone();
                let returned = returned.clone();
                let cleaned = cleaned.clone();
                async move {
                    ctx.defer("resource", move |_| async move {
                        cleaned.store(true, Ordering::SeqCst);
                        Ok(())
                    })?;
                    ctx.middleware(&middleware, move |(), _, next| {
                        let started = started.clone();
                        let returned = returned.clone();
                        async move {
                            let wait = tokio::spawn(next.run(()));
                            started.acquire().await.unwrap().forget();
                            wait.abort();
                            let _ = wait.await;
                            returned.add_permits(1);
                            Ok(9)
                        }
                    })?;
                    Ok(())
                }
            }
        }))
        .unwrap();
    bounded(handle.ready()).await.unwrap();
    let dispatch = tokio::spawn({
        let release = release.clone();
        let cleaned = cleaned.clone();
        async move {
            middleware
                .run(&ctx, (), move |(), ctx| {
                    let release = release.clone();
                    let started = started.clone();
                    let cleaned = cleaned.clone();
                    async move {
                        started.add_permits(1);
                        release.acquire().await.unwrap().forget();
                        assert!(!cleaned.load(Ordering::SeqCst));
                        ctx.defer("terminal resource", |_| async { Ok(()) })?;
                        Ok(1)
                    }
                })
                .await
        }
    });
    bounded(returned.acquire()).await.unwrap().forget();
    dispatch.abort();
    let _ = dispatch.await;
    let shutdown = tokio::spawn({
        let rt = rt.clone();
        async move { rt.shutdown().await }
    });
    bounded(async {
        while handle.snapshot().state != Lifecycle::Unloading {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!cleaned.load(Ordering::SeqCst));
    assert!(!shutdown.is_finished());
    release.add_permits(1);
    bounded(shutdown).await.unwrap().unwrap();
    assert!(cleaned.load(Ordering::SeqCst));
}

#[tokio::test]
async fn middleware_unregister_drains_detached_operations_and_keeps_late_inverse() {
    let rt = Runtime::new();
    let (owner, ctx) = emitter(&rt).await;
    let event = Middleware::<(), ()>::new("drain", "1");
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let cleaned = Arc::new(AtomicBool::new(false));
    let subscription = ctx
        .middleware(&event, {
            let (entered, release, cleaned) = (entered.clone(), release.clone(), cleaned.clone());
            move |(), callback, _| {
                let (entered, release, cleaned) =
                    (entered.clone(), release.clone(), cleaned.clone());
                async move {
                    let started = entered.clone();
                    let wait = tokio::spawn(async move {
                        callback
                            .operation(move |operation| async move {
                                started.add_permits(1);
                                release.acquire().await.unwrap().forget();
                                operation.defer("late", move |cleanup| async move {
                                    cleanup.operation(|_| async { Ok(()) }).await?;
                                    cleaned.store(true, Ordering::SeqCst);
                                    Ok(())
                                })?;
                                Ok(())
                            })
                            .await
                    });
                    entered.acquire().await.unwrap().forget();
                    wait.abort();
                    let _ = wait.await;
                    Ok(())
                }
            }
        })
        .unwrap();
    bounded(event.run(&ctx, (), |(), _| async { Ok(()) }))
        .await
        .unwrap();
    let disposing = tokio::spawn({
        let subscription = subscription.clone();
        async move { subscription.dispose().await }
    });
    bounded(async {
        while subscription.snapshot().status == EffectStatus::Owned {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!disposing.is_finished());
    release.add_permits(1);
    bounded(disposing).await.unwrap().unwrap();
    event.run(&ctx, (), |(), _| async { Ok(()) }).await.unwrap();
    bounded(owner.dispose()).await.unwrap();
    assert!(cleaned.load(Ordering::SeqCst));
    bounded(rt.shutdown()).await.unwrap();
}
