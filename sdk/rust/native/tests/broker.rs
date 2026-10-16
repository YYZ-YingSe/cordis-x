use cordis_sdk::*;
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

fn service() -> Service<Broker<Implementation>> {
    Service::new("test.broker", "1")
}

#[tokio::test]
async fn intercept_updates_and_broker_promotion_preserve_old_resource_configuration() {
    let rt = Runtime::new();
    broker(&rt, View::new()).await;
    let (old, first, old_closed) = candidate(&rt, View::new(), 1).await;
    first.promote().unwrap();
    let config = |revision| {
        let mut config = InterceptConfig::new(revision);
        config.set(&service(), revision * 1000);
        config
    };
    let source = Intercepts::new(config(1));
    let (consumer, _, binding) = client(&rt, View::new().with_intercepts(source.clone())).await;
    let activation = consumer.snapshot().activation;
    let cleaned = Arc::new(Mutex::new(None));
    let resource = binding
        .route({
            let cleaned = cleaned.clone();
            move |value, ctx| {
                Box::pin(async move {
                    let version = value.version;
                    ctx.acquire(
                        "configured resource",
                        async move { Ok(version) },
                        move |version, cleanup| async move {
                            *cleaned.lock().unwrap() = Some((
                                *version,
                                cleanup.metadata_revision(),
                                *cleanup.metadata::<_, u64>(&service())?[0],
                            ));
                            Ok(())
                        },
                    )
                    .await
                })
            }
        })
        .await
        .unwrap();
    source.replace(config(2));
    let (_, second, _) = candidate(&rt, View::new(), 2).await;
    second.promote().unwrap();
    let observed = binding
        .route(|value, ctx| {
            Box::pin(async move {
                Ok((
                    value.version,
                    ctx.metadata_revision(),
                    *ctx.metadata::<_, u64>(&service())?[0],
                ))
            })
        })
        .await
        .unwrap();
    assert_eq!(observed, (2, Some(2), 2000));
    let retiring = tokio::spawn({
        let old = old.clone();
        async move { old.dispose().await }
    });
    closing(&old).await;
    assert!(!old_closed.load(Ordering::SeqCst));
    assert_eq!(
        resource
            .call(|version, ctx| Box::pin(async move { Ok((*version, ctx.metadata_revision())) }))
            .await
            .unwrap(),
        (1, Some(1))
    );
    resource.dispose().await.unwrap();
    bounded(retiring).await.unwrap().unwrap();
    assert_eq!(*cleaned.lock().unwrap(), Some((1, Some(1), 1000)));
    assert_eq!(consumer.snapshot().activation, activation);
    rt.shutdown().await.unwrap();
}
struct Implementation {
    version: u32,
    closed: Arc<AtomicBool>,
}
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("Broker did not settle")
}
async fn wait(gate: &Semaphore) {
    bounded(gate.acquire()).await.unwrap().forget();
}
async fn closing(handle: &PluginHandle) {
    bounded(async {
        while handle.snapshot().state != Lifecycle::Unloading {
            tokio::task::yield_now().await;
        }
    })
    .await;
}
async fn broker(rt: &Runtime, view: View) -> PluginHandle {
    let handle = rt
        .install_in(
            view,
            Plugin::new(
                "broker",
                |ctx| async move { ctx.provide_broker(&service()) },
            )
            .provides(&service()),
        )
        .unwrap();
    bounded(handle.ready()).await.unwrap();
    handle
}
async fn client(
    rt: &Runtime,
    view: View,
) -> (PluginHandle, Context, ServiceHandle<Broker<Implementation>>) {
    let saved = Arc::new(Mutex::new(None));
    let handle = rt
        .install_in(
            view,
            Plugin::new("client", {
                let saved = saved.clone();
                move |ctx| {
                    *saved.lock().unwrap() = Some(ctx);
                    async { Ok(()) }
                }
            })
            .requires(&service()),
        )
        .unwrap();
    bounded(handle.ready()).await.unwrap();
    let ctx = saved.lock().unwrap().take().unwrap();
    let binding = ctx.dependency(&service()).unwrap();
    (handle, ctx, binding)
}
async fn candidate(
    rt: &Runtime,
    view: View,
    version: u32,
) -> (
    PluginHandle,
    BrokerRegistration<Implementation>,
    Arc<AtomicBool>,
) {
    let saved = Arc::new(Mutex::new(None));
    let closed = Arc::new(AtomicBool::new(false));
    let handle = rt
        .install_in(
            view,
            Plugin::new(format!("v{version}"), {
                let saved = saved.clone();
                let closed = closed.clone();
                move |ctx| {
                    let saved = saved.clone();
                    let closed = closed.clone();
                    async move {
                        let finish = closed.clone();
                        ctx.defer("implementation", move |_| async move {
                            finish.store(true, Ordering::SeqCst);
                            Ok(())
                        })?;
                        *saved.lock().unwrap() = Some(
                            ctx.dependency(&service())?
                                .register(Arc::new(Implementation { version, closed }))?,
                        );
                        Ok(())
                    }
                }
            })
            .requires(&service()),
        )
        .unwrap();
    bounded(handle.ready()).await.unwrap();
    let registration = saved.lock().unwrap().take().unwrap();
    (handle, registration, closed)
}
async fn version(handle: &ServiceHandle<Broker<Implementation>>) -> Result<u32> {
    handle
        .route(|implementation, _| {
            Box::pin(async move {
                assert!(!implementation.closed.load(Ordering::SeqCst));
                Ok(implementation.version)
            })
        })
        .await
}

#[tokio::test]
async fn promotion_preserves_consumers_and_stale_revocation_cannot_remove_new_route() {
    let rt = Runtime::new();
    let view = View::new();
    broker(&rt, view.clone()).await;
    let (consumer, _, handle) = client(&rt, view.clone()).await;
    assert_eq!(version(&handle).await, Err(Error::BrokerUnavailable));
    let activation = consumer.snapshot().activation;
    let (v1, first, _) = candidate(&rt, view.clone(), 1).await;
    first.promote().unwrap();
    assert_eq!(version(&handle).await.unwrap(), 1);
    let (_, second, _) = candidate(&rt, view, 2).await;
    assert_eq!(version(&handle).await.unwrap(), 1);
    second.promote().unwrap();
    second.promote().unwrap();
    first.dispose().await.unwrap();
    first.dispose().await.unwrap();
    assert_eq!(first.promote(), Err(Error::Closed));
    bounded(v1.dispose()).await.unwrap();
    assert_eq!(version(&handle).await.unwrap(), 2);
    assert_eq!(consumer.snapshot().activation, activation);
    bounded(rt.shutdown()).await.unwrap();
    assert!(version(&handle).await.is_err());
    assert_eq!(second.promote(), Err(Error::Closed));
}

#[tokio::test]
async fn loading_and_failed_candidates_are_never_visible() {
    let rt = Runtime::new();
    broker(&rt, View::new()).await;
    let (_, _, handle) = client(&rt, View::new()).await;
    let (_, first, _) = candidate(&rt, View::new(), 1).await;
    first.promote().unwrap();
    let registered = Arc::new(Semaphore::new(0));
    let finish = Arc::new(Semaphore::new(0));
    let saved = Arc::new(Mutex::new(None));
    let broken = rt
        .install(
            Plugin::new("broken", {
                let registered = registered.clone();
                let finish = finish.clone();
                let saved = saved.clone();
                move |ctx| {
                    let registered = registered.clone();
                    let finish = finish.clone();
                    let saved = saved.clone();
                    async move {
                        *saved.lock().unwrap() = Some(ctx.dependency(&service())?.register(
                            Arc::new(Implementation {
                                version: 2,
                                closed: Arc::new(AtomicBool::new(false)),
                            }),
                        )?);
                        registered.add_permits(1);
                        wait(&finish).await;
                        Err(Error::Message("init failed".into()))
                    }
                }
            })
            .requires(&service()),
        )
        .unwrap();
    wait(&registered).await;
    let second = saved.lock().unwrap().take().unwrap();
    assert_eq!(second.promote(), Err(Error::Closed));
    assert_eq!(version(&handle).await.unwrap(), 1);
    finish.add_permits(1);
    assert!(matches!(
        bounded(broken.ready()).await,
        Err(Error::InitializationFailed(..))
    ));
    assert_eq!(second.promote(), Err(Error::Closed));
    assert_eq!(version(&handle).await.unwrap(), 1);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn cancelled_calls_drain_on_the_old_implementation_after_switch() {
    let rt = Runtime::new();
    broker(&rt, View::new()).await;
    let (_, _, handle) = client(&rt, View::new()).await;
    let (old, first, closed) = candidate(&rt, View::new(), 1).await;
    first.promote().unwrap();
    let started = Arc::new(Semaphore::new(0));
    let finish = Arc::new(Semaphore::new(0));
    let observed = Arc::new(AtomicUsize::new(0));
    let waiter = tokio::spawn({
        let handle = handle.clone();
        let started = started.clone();
        let finish = finish.clone();
        let observed = observed.clone();
        async move {
            handle
                .route(move |s, _| {
                    Box::pin(async move {
                        started.add_permits(1);
                        wait(&finish).await;
                        assert!(!s.closed.load(Ordering::SeqCst));
                        observed.store(s.version as usize, Ordering::SeqCst);
                        Ok(())
                    })
                })
                .await
        }
    });
    wait(&started).await;
    waiter.abort();
    let _ = waiter.await;
    let (_, second, _) = candidate(&rt, View::new(), 2).await;
    second.promote().unwrap();
    let disposing = tokio::spawn({
        let old = old.clone();
        async move { old.dispose().await }
    });
    closing(&old).await;
    assert!(!disposing.is_finished());
    assert!(!closed.load(Ordering::SeqCst));
    assert_eq!(version(&handle).await.unwrap(), 2);
    finish.add_permits(1);
    bounded(disposing).await.unwrap().unwrap();
    assert_eq!(observed.load(Ordering::SeqCst), 1);
    assert!(closed.load(Ordering::SeqCst));
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn resource_and_cancelled_resource_call_retain_old_code_until_inverse_finishes() {
    let rt = Runtime::new();
    broker(&rt, View::new()).await;
    let (consumer, _, handle) = client(&rt, View::new()).await;
    let activation = consumer.snapshot().activation;
    let (old, first, closed) = candidate(&rt, View::new(), 1).await;
    first.promote().unwrap();
    let released = Arc::new(AtomicUsize::new(0));
    let resource = handle
        .route({
            let released = released.clone();
            move |s, ctx| {
                let value = s.version;
                let closed = s.closed.clone();
                Box::pin(async move {
                    ctx.acquire(
                        "transaction",
                        async move { Ok(value) },
                        move |value, cleanup| async move {
                            assert_eq!(*value, 1);
                            assert!(cleanup.is_cleanup());
                            assert!(!closed.load(Ordering::SeqCst));
                            released.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        },
                    )
                    .await
                })
            }
        })
        .await
        .unwrap();
    let (_, second, _) = candidate(&rt, View::new(), 2).await;
    second.promote().unwrap();
    let disposing = tokio::spawn({
        let old = old.clone();
        async move { old.dispose().await }
    });
    closing(&old).await;
    assert_eq!(
        resource
            .call(|v, _| Box::pin(async move { Ok(*v) }))
            .await
            .unwrap(),
        1
    );
    assert_eq!(version(&handle).await.unwrap(), 2);
    let started = Arc::new(Semaphore::new(0));
    let finish = Arc::new(Semaphore::new(0));
    let waiter = tokio::spawn({
        let resource = resource.clone();
        let started = started.clone();
        let finish = finish.clone();
        async move {
            resource
                .call(move |v, _| {
                    Box::pin(async move {
                        started.add_permits(1);
                        wait(&finish).await;
                        Ok(*v)
                    })
                })
                .await
        }
    });
    wait(&started).await;
    waiter.abort();
    let _ = waiter.await;
    let release = tokio::spawn({
        let resource = resource.clone();
        async move { resource.dispose().await }
    });
    bounded(async {
        while resource.snapshot().status == EffectStatus::Owned {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!release.is_finished());
    assert!(!disposing.is_finished());
    assert!(!closed.load(Ordering::SeqCst));
    finish.add_permits(1);
    bounded(release).await.unwrap().unwrap();
    bounded(disposing).await.unwrap().unwrap();
    resource.dispose().await.unwrap();
    assert_eq!(released.load(Ordering::SeqCst), 1);
    assert!(resource.with(|_| ()).is_err());
    assert_eq!(consumer.snapshot().activation, activation);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn failed_inverse_is_cached_and_reported_as_a_dynamic_cleanup_blocker() {
    let rt = Runtime::new();
    broker(&rt, View::new()).await;
    let (consumer, _, handle) = client(&rt, View::new()).await;
    let (old, first, closed) = candidate(&rt, View::new(), 1).await;
    first.promote().unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let resource = handle
        .route({
            let attempts = attempts.clone();
            move |_, ctx| {
                Box::pin(async move {
                    ctx.acquire(
                        "failed transaction",
                        async { Ok(()) },
                        move |_, _| async move {
                            attempts.fetch_add(1, Ordering::SeqCst);
                            Err(Error::Message("rollback failed".into()))
                        },
                    )
                    .await
                })
            }
        })
        .await
        .unwrap();
    let error = resource.dispose().await.unwrap_err();
    assert_eq!(resource.dispose().await.unwrap_err(), error);
    let (_, second, _) = candidate(&rt, View::new(), 2).await;
    second.promote().unwrap();
    assert!(
        matches!(bounded(old.dispose()).await, Err(Error::CleanupBlocked(id, _)) if id == consumer.id())
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert!(!closed.load(Ordering::SeqCst));
    assert_eq!(version(&handle).await.unwrap(), 2);
    assert!(bounded(rt.shutdown()).await.is_err());
}

#[tokio::test]
async fn realms_and_runtime_instances_have_independent_routes() {
    let rt = Runtime::new();
    let other = Runtime::new();
    let isolated = View::new().isolate(&service());
    broker(&rt, View::new()).await;
    broker(&rt, isolated.clone()).await;
    broker(&other, View::new()).await;
    let (_, root_ctx, root) = client(&rt, View::new()).await;
    let (_, _, local) = client(&rt, isolated.clone()).await;
    let (_, _, remote) = client(&other, View::new()).await;
    let (_, first, _) = candidate(&rt, View::new(), 1).await;
    first.promote().unwrap();
    let (_, second, _) = candidate(&rt, isolated.clone(), 2).await;
    second.promote().unwrap();
    let (_, third, _) = candidate(&other, View::new(), 3).await;
    third.promote().unwrap();
    assert_eq!(version(&root).await.unwrap(), 1);
    assert_eq!(version(&local).await.unwrap(), 2);
    assert_eq!(version(&remote).await.unwrap(), 3);
    // Use-side derivation cannot change committed definition-side bindings.
    assert_eq!(
        version(&root_ctx.derive(isolated).dependency(&service()).unwrap())
            .await
            .unwrap(),
        1
    );
    first.withdraw();
    assert!(version(&root).await.is_err());
    assert_eq!(version(&local).await.unwrap(), 2);
    bounded(rt.shutdown()).await.unwrap();
    bounded(other.shutdown()).await.unwrap();
}

#[tokio::test]
async fn dynamic_context_preserves_definition_owner_and_effects_beyond_the_call() {
    let rt = Runtime::new();
    let broker = broker(&rt, View::new()).await;
    let (consumer, ctx, handle) = client(&rt, View::new()).await;
    let (old, first, closed) = candidate(&rt, View::new(), 1).await;
    first.promote().unwrap();
    let event = Event::<(), u32>::new("dynamic", "1");
    let listener = handle
        .route({
            let event = event.clone();
            let owner = ctx.owner();
            let caller = broker.snapshot().activation.unwrap();
            let definition = first.activation().unwrap();
            move |s, ctx| {
                assert_eq!(ctx.owner(), owner);
                assert_eq!(ctx.activation(), definition);
                assert_eq!(ctx.caller(), Some(caller));
                let closed = s.closed.clone();
                Box::pin(async move {
                    assert!(ctx.scope("unsupported").is_err());
                    ctx.on(&event, move |(), event_ctx| {
                        let closed = closed.clone();
                        async move {
                            assert_eq!(event_ctx.activation(), definition);
                            assert_eq!(event_ctx.owner(), owner);
                            assert!(!closed.load(Ordering::SeqCst));
                            Ok(Break(1))
                        }
                    })
                })
            }
        })
        .await
        .unwrap();
    let (_, second, _) = candidate(&rt, View::new(), 2).await;
    second.promote().unwrap();
    let disposing = tokio::spawn({
        let old = old.clone();
        async move { old.dispose().await }
    });
    closing(&old).await;
    assert_eq!(event.serial(&ctx, ()).await.unwrap(), Break(1));
    assert!(!closed.load(Ordering::SeqCst));
    listener.dispose().await.unwrap();
    bounded(disposing).await.unwrap().unwrap();
    assert_eq!(consumer.snapshot().state, Lifecycle::Active);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn self_owned_dynamic_resources_reject_cycles_before_acquiring() {
    let rt = Runtime::new();
    broker(&rt, View::new()).await;
    let (_, ctx, handle) = client(&rt, View::new()).await;
    let registration = handle
        .register(Arc::new(Implementation {
            version: 1,
            closed: Arc::new(AtomicBool::new(false)),
        }))
        .unwrap();
    registration.promote().unwrap();
    let acquired = Arc::new(AtomicBool::new(false));
    let result = handle
        .route({
            let acquired = acquired.clone();
            move |_, ctx| {
                Box::pin(async move {
                    ctx.acquire(
                        "cycle",
                        async move {
                            acquired.store(true, Ordering::SeqCst);
                            Ok(())
                        },
                        |_, _| async { Ok(()) },
                    )
                    .await
                })
            }
        })
        .await;
    assert!(matches!(result, Err(Error::WouldDeadlock)));
    assert!(!acquired.load(Ordering::SeqCst));
    assert_eq!(
        version(&ctx.dependency(&service()).unwrap()).await.unwrap(),
        1
    );
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn cancelled_inner_acquisition_registers_late_resource_and_pins_definition_dependencies() {
    let rt = Runtime::new();
    broker(&rt, View::new()).await;
    let dependency = Service::<u32>::new("implementation.only", "1");
    let dependency_handle = rt
        .install(
            Plugin::new("dependency", {
                let dependency = dependency.clone();
                move |ctx| {
                    let dependency = dependency.clone();
                    async move { ctx.provide(&dependency, Arc::new(42)) }
                }
            })
            .provides(&dependency),
        )
        .unwrap();
    dependency_handle.ready().await.unwrap();
    let saved = Arc::new(Mutex::new(None));
    let old = rt
        .install(
            Plugin::new("implementation", {
                let saved = saved.clone();
                move |ctx| {
                    let saved = saved.clone();
                    async move {
                        *saved.lock().unwrap() = Some(ctx.dependency(&service())?.register(
                            Arc::new(Implementation {
                                version: 1,
                                closed: Arc::new(AtomicBool::new(false)),
                            }),
                        )?);
                        Ok(())
                    }
                }
            })
            .requires(&dependency)
            .requires(&service()),
        )
        .unwrap();
    old.ready().await.unwrap();
    saved.lock().unwrap().as_ref().unwrap().promote().unwrap();
    let (consumer, ctx, handle) = client(&rt, View::new()).await;
    assert!(ctx.dependency(&dependency).is_err());
    let started = Arc::new(Semaphore::new(0));
    let finish = Arc::new(Semaphore::new(0));
    let cleaned = Arc::new(AtomicBool::new(false));
    handle.route({ let started = started.clone(); let finish = finish.clone(); let cleaned = cleaned.clone(); move |_, ctx| Box::pin(async move {
        assert_eq!(ctx.dependency(&dependency)?.with(|v, _| Ok(*v))?, 42);
        let acquisition = ctx.acquire("late", {
            let started = started.clone(); async move { started.add_permits(1); wait(&finish).await; Ok(()) }
        }, move |_, cleanup| async move {
            assert_eq!(cleanup.dependency(&dependency)?.with(|v, _| Ok(*v))?, 42);
            cleaned.store(true, Ordering::SeqCst); Ok(())
        });
        tokio::pin!(acquisition);
        tokio::select! {
            result = &mut acquisition => panic!("unexpected early acquisition: {}", result.is_ok()),
            _ = wait(&started) => {}
        }
        Ok(()) // acquisition wait drops here, before the resource exists
    })}).await.unwrap();
    let retiring = tokio::spawn({
        let old = old.clone();
        async move { old.dispose().await }
    });
    closing(&old).await;
    assert!(!retiring.is_finished());
    let disposing = tokio::spawn({
        let consumer = consumer.clone();
        async move { consumer.dispose().await }
    });
    closing(&consumer).await;
    finish.add_permits(1);
    bounded(disposing).await.unwrap().unwrap();
    bounded(retiring).await.unwrap().unwrap();
    assert!(cleaned.load(Ordering::SeqCst));
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_promotion_never_exposes_a_route_gap_or_destroyed_implementation() {
    let rt = Runtime::new();
    broker(&rt, View::new()).await;
    let (consumer, _, handle) = client(&rt, View::new()).await;
    let (v1, first, _) = candidate(&rt, View::new(), 1).await;
    let (v2, second, _) = candidate(&rt, View::new(), 2).await;
    let (_, third, _) = candidate(&rt, View::new(), 3).await;
    first.promote().unwrap();
    let start = Arc::new(tokio::sync::Barrier::new(17));
    let mut workers = Vec::new();
    for _ in 0..16 {
        let start = start.clone();
        let handle = handle.clone();
        workers.push(tokio::spawn(async move {
            start.wait().await;
            for _ in 0..32 {
                assert!((1..=3).contains(&version(&handle).await.unwrap()));
                tokio::task::yield_now().await;
            }
        }));
    }
    start.wait().await;
    second.promote().unwrap();
    bounded(v1.dispose()).await.unwrap();
    third.promote().unwrap();
    bounded(v2.dispose()).await.unwrap();
    for worker in workers {
        bounded(worker).await.unwrap();
    }
    assert_eq!(version(&handle).await.unwrap(), 3);
    assert_eq!(consumer.snapshot().state, Lifecycle::Active);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn late_sequence_inverse_retains_the_implementation_after_its_creating_route_returns() {
    let rt = Runtime::new();
    broker(&rt, View::new()).await;
    let (_, _, handle) = client(&rt, View::new()).await;
    let (old, first, closed) = candidate(&rt, View::new(), 1).await;
    first.promote().unwrap();
    let started = Arc::new(Semaphore::new(0));
    let finish = Arc::new(Semaphore::new(0));
    let cleaned = Arc::new(AtomicBool::new(false));
    let sequence = handle
        .route({
            let started = started.clone();
            let finish = finish.clone();
            let closed = closed.clone();
            let cleaned = cleaned.clone();
            move |_, ctx| {
                Box::pin(async move {
                    ctx.effect_steps("late sequence", move |_| {
                        let started = started.clone();
                        let finish = finish.clone();
                        let closed = closed.clone();
                        let cleaned = cleaned.clone();
                        async move {
                            started.add_permits(1);
                            wait(&finish).await;
                            Ok(EffectStep::complete(Some(Inverse::new(
                                "late inverse",
                                move |_| async move {
                                    assert!(!closed.load(Ordering::SeqCst));
                                    cleaned.store(true, Ordering::SeqCst);
                                    Ok(())
                                },
                            ))))
                        }
                    })
                })
            }
        })
        .await
        .unwrap();
    wait(&started).await;
    let (_, second, _) = candidate(&rt, View::new(), 2).await;
    second.promote().unwrap();
    let retiring = tokio::spawn({
        let old = old.clone();
        async move { old.dispose().await }
    });
    closing(&old).await;
    finish.add_permits(1);
    bounded(sequence.ready()).await.unwrap();
    assert!(!retiring.is_finished());
    bounded(sequence.dispose()).await.unwrap();
    bounded(retiring).await.unwrap().unwrap();
    assert!(cleaned.load(Ordering::SeqCst));
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn candidate_probe_is_unpublished_and_rejects_cross_broker_tokens() {
    let rt = Runtime::new();
    broker(&rt, View::new()).await;
    let (_, first, _) = candidate(&rt, View::new(), 1).await;
    first.promote_if_current(None).unwrap();
    let (_, _, binding) = client(&rt, View::new()).await;
    let (_, next, _) = candidate(&rt, View::new(), 2).await;
    assert_eq!(
        binding
            .probe(&next, |v, _| Box::pin(async move { Ok(v.version) }))
            .await
            .unwrap(),
        2
    );
    assert_eq!(version(&binding).await.unwrap(), 1);
    assert_eq!(next.promote_if_current(None), Err(Error::BrokerChanged));
    assert_eq!(version(&binding).await.unwrap(), 1);
    let other = Runtime::new();
    broker(&other, View::new()).await;
    let (_, foreign, _) = candidate(&other, View::new(), 3).await;
    assert_eq!(
        binding
            .probe(&foreign, |_, _| Box::pin(async {
                panic!("foreign candidate must not run")
            }))
            .await,
        Err::<(), _>(Error::BrokerMismatch)
    );
    let isolated = View::new().isolate(&service());
    broker(&rt, isolated.clone()).await;
    let (_, foreign, _) = candidate(&rt, isolated, 4).await;
    assert_eq!(
        binding
            .probe(&foreign, |_, _| Box::pin(async { Ok(()) }))
            .await,
        Err(Error::BrokerMismatch)
    );
    next.promote_if_current(Some(&first)).unwrap();
    assert_eq!(version(&binding).await.unwrap(), 2);
    assert_eq!(first.promote_if_current(Some(&next)), Err(Error::Closed));
    bounded(other.shutdown()).await.unwrap();
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn probe_resources_pin_unpublished_candidate_and_weak_registration_does_not_keep_code() {
    let rt = Runtime::new();
    broker(&rt, View::new()).await;
    let (_, first, _) = candidate(&rt, View::new(), 1).await;
    first.promote().unwrap();
    let (_, _, binding) = client(&rt, View::new()).await;
    let (provider, candidate, closed) = candidate(&rt, View::new(), 2).await;
    let weak = candidate.downgrade();
    let resource = binding
        .probe(&candidate, |value, ctx| {
            let version = value.version;
            Box::pin(async move {
                ctx.acquire("probe resource", async move { Ok(version) }, |_, _| async {
                    Ok(())
                })
                .await
            })
        })
        .await
        .unwrap();
    let retiring = tokio::spawn({
        let provider = provider.clone();
        async move { provider.dispose().await }
    });
    closing(&provider).await;
    assert!(!retiring.is_finished());
    assert!(!closed.load(Ordering::SeqCst));
    assert_eq!(version(&binding).await.unwrap(), 1);
    assert_eq!(
        resource
            .call(|v, _| Box::pin(async move { Ok(*v) }))
            .await
            .unwrap(),
        2
    );
    resource.dispose().await.unwrap();
    bounded(retiring).await.unwrap().unwrap();
    drop(resource);
    drop(candidate);
    bounded(async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(closed.load(Ordering::SeqCst));
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn concurrent_compare_and_promote_has_one_winner_and_no_stale_overwrite() {
    let rt = Runtime::new();
    broker(&rt, View::new()).await;
    let (_, old, _) = candidate(&rt, View::new(), 1).await;
    old.promote().unwrap();
    let (_, second, _) = candidate(&rt, View::new(), 2).await;
    let (_, third, _) = candidate(&rt, View::new(), 3).await;
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let tasks: Vec<_> = [second, third]
        .into_iter()
        .map(|candidate| {
            let expected = old.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                candidate.promote_if_current(Some(&expected))
            })
        })
        .collect();
    let results: Vec<_> = tasks.into_iter().map(|task| task.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| **r == Err(Error::BrokerChanged))
            .count(),
        1
    );
    let (_, _, binding) = client(&rt, View::new()).await;
    assert_eq!(
        version(&binding).await.unwrap(),
        if results[0].is_ok() { 2 } else { 3 }
    );
    bounded(rt.shutdown()).await.unwrap();
}
