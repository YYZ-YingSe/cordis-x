use cordis_sdk::*;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

type Log = Arc<Mutex<Vec<String>>>;
struct Database {
    open: Arc<AtomicBool>,
    version: usize,
}
fn database() -> Service<Database> {
    Service::new("test.database", "1")
}
fn search() -> Service<()> {
    Service::new("test.search", "1")
}

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("lifecycle did not settle")
}
async fn phase(handle: &PluginHandle, target: Lifecycle) {
    bounded(async {
        while handle.snapshot().state != target {
            tokio::task::yield_now().await;
        }
    })
    .await;
}
fn provider(open: Arc<AtomicBool>, log: Log) -> Plugin {
    Plugin::new("database", move |ctx| {
        let open = open.clone();
        let log = log.clone();
        async move {
            open.store(true, Ordering::SeqCst);
            let closing = open.clone();
            ctx.defer("database connection", move |_| async move {
                closing.store(false, Ordering::SeqCst);
                log.lock().unwrap().push("database closed".into());
                Ok(())
            })?;
            ctx.provide(&database(), Arc::new(Database { open, version: 1 }))
        }
    })
    .provides(&database())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_cleanup_keeps_provider_alive_and_cascades() {
    let rt = Runtime::new();
    let log: Log = Arc::default();
    let open = Arc::new(AtomicBool::new(false));
    let started = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let consumer = rt
        .install(
            Plugin::new("search", {
                let log = log.clone();
                let started = started.clone();
                let release = release.clone();
                move |ctx| {
                    let log = log.clone();
                    let started = started.clone();
                    let release = release.clone();
                    async move {
                        ctx.defer("search cleanup", move |cleanup| async move {
                            log.lock().unwrap().push("search cleaning".into());
                            started.add_permits(1);
                            release.acquire().await.unwrap().forget();
                            cleanup.dependency(&database())?.with(|db, _| {
                                assert!(db.open.load(Ordering::SeqCst));
                                Ok(())
                            })?;
                            log.lock().unwrap().push("search cleaned".into());
                            Ok(())
                        })?;
                        ctx.provide(&search(), Arc::new(()))
                    }
                }
            })
            .requires(&database())
            .provides(&search()),
        )
        .unwrap();
    assert_eq!(consumer.settled().await.unwrap().state, Lifecycle::Inactive);
    let tools = rt
        .install(
            Plugin::new("tools", {
                let log = log.clone();
                move |ctx| {
                    let log = log.clone();
                    async move {
                        ctx.defer("tools cleanup", move |_| async move {
                            log.lock().unwrap().push("tools cleaned".into());
                            Ok(())
                        })?;
                        Ok(())
                    }
                }
            })
            .requires(&search()),
        )
        .unwrap();
    let db = rt.install(provider(open.clone(), log.clone())).unwrap();
    bounded(tools.ready()).await.unwrap();
    let stopping = tokio::spawn({
        let db = db.clone();
        async move { db.dispose().await }
    });
    bounded(started.acquire()).await.unwrap().forget();
    assert!(
        open.load(Ordering::SeqCst),
        "provider closed while consumer was cleaning"
    );
    assert!(!stopping.is_finished());
    release.add_permits(1);
    bounded(stopping).await.unwrap().unwrap();
    assert_eq!(
        &*log.lock().unwrap(),
        &[
            "tools cleaned",
            "search cleaning",
            "search cleaned",
            "database closed"
        ]
    );
    assert_eq!(consumer.snapshot().state, Lifecycle::Inactive);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn stale_initializer_is_never_published_and_late_resource_is_recovered() {
    let rt = Runtime::new();
    let started = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let cleanups = Arc::new(AtomicUsize::new(0));
    let consumers = Arc::new(AtomicUsize::new(0));
    let db = rt
        .install(
            Plugin::new("slow", {
                let started = started.clone();
                let release = release.clone();
                let cleanups = cleanups.clone();
                move |ctx| {
                    let started = started.clone();
                    let release = release.clone();
                    let cleanups = cleanups.clone();
                    async move {
                        let _resource = ctx
                            .acquire(
                                "late resource",
                                async move {
                                    started.add_permits(1);
                                    release.acquire().await.unwrap().forget();
                                    Ok(())
                                },
                                move |_, _| async move {
                                    cleanups.fetch_add(1, Ordering::SeqCst);
                                    Ok(())
                                },
                            )
                            .await?;
                        ctx.provide(
                            &database(),
                            Arc::new(Database {
                                open: Arc::new(AtomicBool::new(true)),
                                version: 1,
                            }),
                        )
                    }
                }
            })
            .provides(&database()),
        )
        .unwrap();
    rt.install(
        Plugin::new("waiting", {
            let consumers = consumers.clone();
            move |_| {
                consumers.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            }
        })
        .requires(&database()),
    )
    .unwrap();
    bounded(started.acquire()).await.unwrap().forget();
    let stopping = tokio::spawn({
        let db = db.clone();
        async move { db.dispose().await }
    });
    phase(&db, Lifecycle::Unloading).await;
    release.add_permits(1);
    bounded(stopping).await.unwrap().unwrap();
    assert_eq!(cleanups.load(Ordering::SeqCst), 1);
    assert_eq!(consumers.load(Ordering::SeqCst), 0);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn independent_effects_have_unique_ownership_and_reverse_cleanup() {
    let rt = Runtime::new();
    let log: Log = Arc::default();
    let make = |name: &'static str| {
        let log = log.clone();
        Plugin::new(name, move |ctx| {
            let log = log.clone();
            async move {
                for i in 0..3 {
                    let log = log.clone();
                    ctx.defer(format!("{name}:{i}"), move |_| async move {
                        log.lock().unwrap().push(format!("{name}:{i}"));
                        Ok(())
                    })?;
                }
                Ok(())
            }
        })
    };
    let a = rt.install(make("a")).unwrap();
    let b = rt.install(make("b")).unwrap();
    a.ready().await.unwrap();
    b.ready().await.unwrap();
    bounded(a.dispose()).await.unwrap();
    bounded(a.dispose()).await.unwrap();
    assert_eq!(&*log.lock().unwrap(), &["a:2", "a:1", "a:0"]);
    assert_eq!(b.snapshot().state, Lifecycle::Active);
    bounded(rt.shutdown()).await.unwrap();
    assert_eq!(
        &*log.lock().unwrap(),
        &["a:2", "a:1", "a:0", "b:2", "b:1", "b:0"]
    );
}

#[tokio::test]
async fn restart_uses_new_epochs_and_reactivates_consumers() {
    let rt = Runtime::new();
    let versions = Arc::new(AtomicUsize::new(0));
    let observed: Arc<Mutex<Vec<usize>>> = Arc::default();
    let db = rt
        .install(
            Plugin::new("database", {
                let versions = versions.clone();
                move |ctx| {
                    let version = versions.fetch_add(1, Ordering::SeqCst) + 1;
                    async move {
                        ctx.provide(
                            &database(),
                            Arc::new(Database {
                                open: Arc::new(AtomicBool::new(true)),
                                version,
                            }),
                        )
                    }
                }
            })
            .provides(&database()),
        )
        .unwrap();
    let consumer = rt
        .install(
            Plugin::new("consumer", {
                let observed = observed.clone();
                move |ctx| {
                    let observed = observed.clone();
                    async move {
                        let version = ctx.dependency(&database())?.with(|db, _| Ok(db.version))?;
                        observed.lock().unwrap().push(version);
                        Ok(())
                    }
                }
            })
            .requires(&database()),
        )
        .unwrap();
    consumer.ready().await.unwrap();
    let epoch = consumer.snapshot().activation.unwrap().epoch;
    bounded(db.restart()).await.unwrap();
    bounded(consumer.ready()).await.unwrap();
    assert!(consumer.snapshot().activation.unwrap().epoch > epoch);
    assert_eq!(&*observed.lock().unwrap(), &[1, 2]);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn realms_are_independent_and_contract_revisions_are_exact() {
    let rt = Runtime::new();
    let realm_a = View::new().isolate(&database());
    let realm_b = View::new().isolate(&database());
    let db_a = rt
        .install_in(
            realm_a.clone(),
            provider(Arc::new(AtomicBool::new(false)), Arc::default()),
        )
        .unwrap();
    let db_b = rt
        .install_in(
            realm_b.clone(),
            provider(Arc::new(AtomicBool::new(false)), Arc::default()),
        )
        .unwrap();
    let a = rt
        .install_in(
            realm_a,
            Plugin::new("a", |_| async { Ok(()) }).requires(&database()),
        )
        .unwrap();
    let b = rt
        .install_in(
            realm_b,
            Plugin::new("b", |_| async { Ok(()) }).requires(&database()),
        )
        .unwrap();
    bounded(a.ready()).await.unwrap();
    bounded(b.ready()).await.unwrap();
    let incompatible = rt
        .install(
            Plugin::new("v2", |_| async { Ok(()) })
                .requires(&Service::<Database>::new("test.database", "2")),
        )
        .unwrap();
    assert_eq!(
        incompatible.settled().await.unwrap().state,
        Lifecycle::Inactive
    );
    bounded(db_a.dispose()).await.unwrap();
    assert_eq!(a.snapshot().state, Lifecycle::Inactive);
    assert_eq!(b.snapshot().state, Lifecycle::Active);
    assert_eq!(db_b.snapshot().state, Lifecycle::Active);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn definition_dependencies_and_use_site_effects_and_metadata_are_separate() {
    let rt = Runtime::new();
    let route = Service::<()>::new("test.router", "1");
    let log: Log = Arc::default();
    let provider_view = View::new().isolate(&database());
    rt.install_in(
        provider_view.clone(),
        provider(Arc::new(AtomicBool::new(false)), Arc::default()),
    )
    .unwrap();
    let router = rt
        .install_in(
            provider_view,
            Plugin::new("router", {
                let route = route.clone();
                move |ctx| {
                    let route = route.clone();
                    async move { ctx.provide(&route, Arc::new(())) }
                }
            })
            .requires(&database())
            .provides(&route),
        )
        .unwrap();
    let caller_view = View::new()
        .isolate(&database())
        .intercept(&route, "caller prefix".to_string());
    let consumer = rt
        .install_in(
            caller_view,
            Plugin::new("caller", {
                let route = route.clone();
                let log = log.clone();
                move |ctx| {
                    let route = route.clone();
                    let log = log.clone();
                    async move {
                        let owner = ctx.owner();
                        ctx.dependency(&route)?.with(|_, invocation| {
                            assert_eq!(invocation.owner(), owner);
                            assert_ne!(invocation.activation(), owner);
                            assert_eq!(invocation.caller(), Some(owner));
                            assert_eq!(
                                invocation
                                    .dependency(&database())?
                                    .with(|db, _| Ok(db.version))?,
                                1
                            );
                            assert_eq!(
                                &*invocation.metadata::<_, String>(&route)?[0],
                                "caller prefix"
                            );
                            invocation.defer("route", move |cleanup| async move {
                                // The inverse must retain the router's definition-side dependencies too.
                                assert_eq!(
                                    cleanup
                                        .dependency(&database())?
                                        .with(|db, _| Ok(db.version))?,
                                    1
                                );
                                log.lock().unwrap().push("route removed".into());
                                Ok(())
                            })?;
                            Ok(())
                        })
                    }
                }
            })
            .requires(&route),
        )
        .unwrap();
    bounded(consumer.ready()).await.unwrap();
    assert_eq!(consumer.effects().len(), 1);
    assert!(router.effects().is_empty());
    bounded(consumer.dispose()).await.unwrap();
    assert_eq!(&*log.lock().unwrap(), &["route removed"]);
    assert_eq!(router.snapshot().state, Lifecycle::Active);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_call_wait_still_pins_the_provider_and_consumer() {
    let rt = Runtime::new();
    let open = Arc::new(AtomicBool::new(false));
    let db = rt.install(provider(open.clone(), Arc::default())).unwrap();
    let saved: Arc<Mutex<Option<ServiceHandle<Database>>>> = Arc::default();
    let consumer = rt
        .install(
            Plugin::new("client", {
                let saved = saved.clone();
                move |ctx| {
                    let saved = saved.clone();
                    async move {
                        *saved.lock().unwrap() = Some(ctx.dependency(&database())?);
                        Ok(())
                    }
                }
            })
            .requires(&database()),
        )
        .unwrap();
    bounded(consumer.ready()).await.unwrap();
    let handle = saved.lock().unwrap().as_ref().unwrap().clone();
    let started = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let call = tokio::spawn({
        let started = started.clone();
        let release = release.clone();
        async move {
            handle
                .call(move |db, _| {
                    Box::pin(async move {
                        started.add_permits(1);
                        release.acquire().await.unwrap().forget();
                        assert!(db.open.load(Ordering::SeqCst));
                        Ok(())
                    })
                })
                .await
        }
    });
    bounded(started.acquire()).await.unwrap().forget();
    call.abort();
    let _ = call.await;
    let stopping = tokio::spawn({
        let db = db.clone();
        async move { db.dispose().await }
    });
    phase(&consumer, Lifecycle::Unloading).await;
    assert!(open.load(Ordering::SeqCst));
    assert!(!stopping.is_finished());
    let stale = saved.lock().unwrap().as_ref().unwrap().clone();
    assert!(matches!(stale.with(|_, _| Ok(())), Err(Error::Closed)));
    release.add_permits(1);
    bounded(stopping).await.unwrap().unwrap();
    assert!(!open.load(Ordering::SeqCst));
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn failed_cleanup_stays_visible_and_blocks_provider_destruction() {
    let rt = Runtime::new();
    let open = Arc::new(AtomicBool::new(false));
    let db = rt.install(provider(open.clone(), Arc::default())).unwrap();
    let consumer = rt
        .install(
            Plugin::new("bad cleanup", |ctx| async move {
                ctx.defer("cannot release", |_| async {
                    Err(Error::Message("release failed".into()))
                })?;
                Ok(())
            })
            .requires(&database()),
        )
        .unwrap();
    consumer.ready().await.unwrap();
    assert!(matches!(
        bounded(db.dispose()).await,
        Err(Error::CleanupBlocked(_, _))
    ));
    assert!(open.load(Ordering::SeqCst));
    assert_eq!(consumer.snapshot().state, Lifecycle::Unloading);
    assert!(matches!(
        consumer.effects()[0].status,
        EffectStatus::Failed(_)
    ));
    assert!(bounded(rt.shutdown()).await.is_err());
}

#[tokio::test]
async fn initializer_panic_recovers_effects_and_requires_explicit_restart() {
    let rt = Runtime::new();
    let attempts = Arc::new(AtomicUsize::new(0));
    let cleaned = Arc::new(AtomicUsize::new(0));
    let plugin = rt
        .install(Plugin::new("panics once", {
            let attempts = attempts.clone();
            let cleaned = cleaned.clone();
            move |ctx| {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                let cleaned = cleaned.clone();
                async move {
                    ctx.defer("cleanup", move |_| async move {
                        cleaned.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })?;
                    assert_ne!(attempt, 0, "expected test panic");
                    Ok(())
                }
            }
        }))
        .unwrap();
    assert!(matches!(
        bounded(plugin.ready()).await,
        Err(Error::InitializationFailed(_, _))
    ));
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    bounded(plugin.restart()).await.unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    bounded(rt.shutdown()).await.unwrap();
    assert_eq!(cleaned.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn child_dependency_on_parent_is_safe_and_ready_wait_is_diagnosed() {
    let rt = Runtime::new();
    let child_slot: Arc<Mutex<Option<PluginHandle>>> = Arc::default();
    let parent = rt
        .install(
            Plugin::new("parent", {
                let child_slot = child_slot.clone();
                move |ctx| {
                    let child_slot = child_slot.clone();
                    async move {
                        let child = ctx.install(
                            Plugin::new("child", |_| async { Ok(()) }).requires(&search()),
                        )?;
                        assert_eq!(child.ready().await.unwrap_err(), Error::WouldDeadlock);
                        *child_slot.lock().unwrap() = Some(child);
                        ctx.provide(&search(), Arc::new(()))
                    }
                }
            })
            .provides(&search()),
        )
        .unwrap();
    bounded(parent.ready()).await.unwrap();
    let child = child_slot.lock().unwrap().as_ref().unwrap().clone();
    bounded(child.ready()).await.unwrap();
    bounded(parent.dispose()).await.unwrap();
    assert!(child.snapshot().retired);
    assert_eq!(child.snapshot().state, Lifecycle::Inactive);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn managed_task_is_signalled_and_joined_before_resource_cleanup() {
    let rt = Runtime::new();
    let log: Log = Arc::default();
    let plugin = rt
        .install(Plugin::new("worker", {
            let log = log.clone();
            move |ctx| {
                let log = log.clone();
                async move {
                    let cleanup_log = log.clone();
                    ctx.defer("connection", move |_| async move {
                        cleanup_log.lock().unwrap().push("closed".into());
                        Ok(())
                    })?;
                    ctx.spawn("task", move |task| async move {
                        task.cancelled().await;
                        log.lock().unwrap().push("task stopped".into());
                        Ok(())
                    })?;
                    Ok(())
                }
            }
        }))
        .unwrap();
    plugin.ready().await.unwrap();
    bounded(plugin.dispose()).await.unwrap();
    assert_eq!(&*log.lock().unwrap(), &["task stopped", "closed"]);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn missing_provision_and_undeclared_access_fail_initialization() {
    let rt = Runtime::new();
    let missing = rt
        .install(Plugin::new("missing", |_| async { Ok(()) }).provides(&database()))
        .unwrap();
    assert!(matches!(
        bounded(missing.ready()).await,
        Err(Error::InitializationFailed(_, _))
    ));
    let undeclared = rt
        .install(Plugin::new("undeclared", |ctx| async move {
            ctx.dependency(&database())?;
            Ok(())
        }))
        .unwrap();
    assert!(matches!(
        bounded(undeclared.ready()).await,
        Err(Error::InitializationFailed(_, _))
    ));
    bounded(rt.shutdown()).await.unwrap();
}
