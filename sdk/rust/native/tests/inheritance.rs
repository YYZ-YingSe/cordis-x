use cordis_sdk::*;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

type Trace = Arc<Mutex<Vec<String>>>;
async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("inheritance did not settle")
}
fn database() -> Service<usize> {
    Service::new("test.database", "1")
}
fn router() -> Service<()> {
    Service::new("test.router", "1")
}
fn database_plugin(value: usize) -> Plugin {
    Plugin::new("database", move |ctx| async move {
        ctx.provide(&database(), Arc::new(value))
    })
    .provides(&database())
}
fn read(ctx: &Context) -> Result<usize> {
    ctx.dependency(&database())?.with(|db, _| Ok(*db))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grandchildren_inherit_committed_dependencies_and_clean_before_ancestors_and_provider() {
    let rt = Runtime::new();
    let trace: Trace = Arc::default();
    let generation = Arc::new(AtomicUsize::new(0));
    let db = rt
        .install(
            Plugin::new("database", {
                let trace = trace.clone();
                move |ctx| {
                    let current = generation.fetch_add(1, Ordering::SeqCst) + 1;
                    let trace = trace.clone();
                    async move {
                        ctx.defer("database", move |_| async move {
                            trace.lock().unwrap().push(format!("db {current}"));
                            Ok(())
                        })?;
                        ctx.provide(&database(), Arc::new(current))
                    }
                }
            })
            .provides(&database()),
        )
        .unwrap();
    let grandchildren = Arc::new(Mutex::new(Vec::new()));
    let handles: Arc<Mutex<Vec<ServiceHandle<usize>>>> = Arc::default();
    let parent = rt
        .install(
            Plugin::new("parent", {
                let trace = trace.clone();
                let grandchildren = grandchildren.clone();
                let handles = handles.clone();
                move |ctx| {
                    let trace = trace.clone();
                    let grandchildren = grandchildren.clone();
                    let handles = handles.clone();
                    async move {
                        let parent_trace = trace.clone();
                        ctx.defer("parent", move |ctx| async move {
                            parent_trace
                                .lock()
                                .unwrap()
                                .push(format!("parent {}", read(&ctx)?));
                            Ok(())
                        })?;
                        let child = ctx.install(Plugin::new("child", move |ctx| {
                            let trace = trace.clone();
                            let grandchildren = grandchildren.clone();
                            let handles = handles.clone();
                            async move {
                                let child_trace = trace.clone();
                                ctx.defer("child", move |ctx| async move {
                                    child_trace
                                        .lock()
                                        .unwrap()
                                        .push(format!("child {}", read(&ctx)?));
                                    Ok(())
                                })?;
                                let grandchild =
                                    ctx.install(Plugin::new("grandchild", move |ctx| {
                                        let trace = trace.clone();
                                        let handles = handles.clone();
                                        async move {
                                            handles
                                                .lock()
                                                .unwrap()
                                                .push(ctx.dependency(&database())?);
                                            ctx.defer("grandchild", move |ctx| async move {
                                                trace
                                                    .lock()
                                                    .unwrap()
                                                    .push(format!("grandchild {}", read(&ctx)?));
                                                Ok(())
                                            })?;
                                            Ok(())
                                        }
                                    }))?;
                                grandchild.ready().await?;
                                grandchildren.lock().unwrap().push(grandchild);
                                Ok(())
                            }
                        }))?;
                        child.ready().await?;
                        Ok(())
                    }
                }
            })
            .requires(&database()),
        )
        .unwrap();
    bounded(parent.ready()).await.unwrap();
    let old = handles.lock().unwrap()[0].clone();
    assert_eq!(old.with(|db, _| Ok(*db)).unwrap(), 1);
    bounded(db.restart()).await.unwrap();
    bounded(parent.ready()).await.unwrap();
    assert_eq!(
        &trace.lock().unwrap()[..4],
        &["grandchild 1", "child 1", "parent 1", "db 1"]
    );
    assert!(grandchildren.lock().unwrap()[0].snapshot().retired);
    assert_eq!(handles.lock().unwrap()[1].with(|db, _| Ok(*db)).unwrap(), 2);
    assert!(matches!(old.with(|_, _| Ok(())), Err(Error::Closed)));
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn isolation_blocks_inheritance_at_each_hop_and_explicit_declarations_shadow_it() {
    let rt = Runtime::new();
    let parent_view = View::new().isolate(&database());
    let child_view = View::new().isolate(&database());
    rt.install_in(parent_view.clone(), database_plugin(1))
        .unwrap();
    rt.install_in(child_view.clone(), database_plugin(2))
        .unwrap();
    let blocked = Arc::new(AtomicUsize::new(0));
    let parent = rt
        .install_in(
            parent_view.clone(),
            Plugin::new("parent", {
                let blocked = blocked.clone();
                move |ctx| {
                    let child_view = child_view.clone();
                    let parent_view = parent_view.clone();
                    let blocked = blocked.clone();
                    async move {
                        assert_eq!(read(&ctx)?, 1);
                        let isolated = ctx.derive(child_view.clone());
                        let child =
                            isolated.install(Plugin::new("no inherited binding", move |ctx| {
                                let parent_view = parent_view.clone();
                                let blocked = blocked.clone();
                                async move {
                                    assert!(matches!(
                                        read(&ctx),
                                        Err(Error::UndeclaredDependency(_))
                                    ));
                                    blocked.fetch_add(1, Ordering::SeqCst);
                                    // Returning to the grandparent's realm must not bridge the excluded hop.
                                    let grandchild = ctx.derive(parent_view).install(
                                        Plugin::new("no bridge", move |ctx| {
                                            let blocked = blocked.clone();
                                            async move {
                                                assert!(matches!(
                                                    read(&ctx),
                                                    Err(Error::UndeclaredDependency(_))
                                                ));
                                                blocked.fetch_add(1, Ordering::SeqCst);
                                                Ok(())
                                            }
                                        }),
                                    )?;
                                    grandchild.ready().await?;
                                    Ok(())
                                }
                            }))?;
                        child.ready().await?;
                        let own = isolated.install(
                            Plugin::new("own binding", |ctx| async move {
                                assert_eq!(read(&ctx)?, 2);
                                Ok(())
                            })
                            .requires(&database()),
                        )?;
                        own.ready().await?;
                        let waiting = ctx.derive(View::new().isolate(&database())).install(
                            Plugin::new("missing own binding", |_| async {
                                panic!("must not fall back to parent's realm")
                            })
                            .requires(&database()),
                        )?;
                        assert_eq!(waiting.settled().await?.state, Lifecycle::Inactive);
                        Ok(())
                    }
                }
            })
            .requires(&database()),
        )
        .unwrap();
    bounded(parent.ready()).await.unwrap();
    assert_eq!(blocked.load(Ordering::SeqCst), 2);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn service_installed_child_inherits_definition_dependencies_but_belongs_to_caller() {
    let rt = Runtime::new();
    let definition_view = View::new().isolate(&database());
    let caller_view = View::new().isolate(&database());
    rt.install_in(definition_view.clone(), database_plugin(1))
        .unwrap();
    rt.install_in(caller_view.clone(), database_plugin(2))
        .unwrap();
    let service = rt
        .install_in(
            definition_view.clone(),
            Plugin::new("router", |ctx| async move {
                ctx.provide(&router(), Arc::new(()))
            })
            .requires(&database())
            .provides(&router()),
        )
        .unwrap();
    let children: Arc<Mutex<Vec<PluginHandle>>> = Arc::default();
    let cleaned = Arc::new(AtomicBool::new(false));
    let caller = rt
        .install_in(
            caller_view,
            Plugin::new("caller", {
                let children = children.clone();
                let cleaned = cleaned.clone();
                move |ctx| {
                    let children = children.clone();
                    let cleaned = cleaned.clone();
                    let definition_view = definition_view.clone();
                    async move {
                        assert_eq!(read(&ctx)?, 2);
                        let child = ctx.dependency(&router())?.with(move |_, source| {
                            assert_eq!(read(&source)?, 1);
                            // Installation view determines realm visibility; adopt the definition realm.
                            source.derive(definition_view).install(Plugin::new(
                                "created by router",
                                move |ctx| {
                                    let cleaned = cleaned.clone();
                                    async move {
                                        assert_eq!(read(&ctx)?, 1);
                                        ctx.defer("child cleanup", move |ctx| async move {
                                            assert_eq!(read(&ctx)?, 1);
                                            cleaned.store(true, Ordering::SeqCst);
                                            Ok(())
                                        })?;
                                        Ok(())
                                    }
                                },
                            ))
                        })?;
                        child.ready().await?;
                        children.lock().unwrap().push(child);
                        Ok(())
                    }
                }
            })
            .requires(&database())
            .requires(&router()),
        )
        .unwrap();
    bounded(caller.ready()).await.unwrap();
    bounded(caller.dispose()).await.unwrap();
    assert!(cleaned.load(Ordering::SeqCst));
    assert!(children.lock().unwrap()[0].snapshot().retired);
    assert_eq!(service.snapshot().state, Lifecycle::Active);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inherited_calls_survive_cancelled_wait_and_hold_provider_until_child_drains() {
    let rt = Runtime::new();
    let saved = Arc::new(Mutex::new(None));
    let open = Arc::new(AtomicBool::new(true));
    let db = rt
        .install(
            Plugin::new("database", {
                let open = open.clone();
                move |ctx| {
                    let open = open.clone();
                    async move {
                        ctx.defer("database", move |_| async move {
                            open.store(false, Ordering::SeqCst);
                            Ok(())
                        })?;
                        ctx.provide(&database(), Arc::new(1))
                    }
                }
            })
            .provides(&database()),
        )
        .unwrap();
    let parent = rt
        .install(
            Plugin::new("parent", {
                let saved = saved.clone();
                move |ctx| {
                    let saved = saved.clone();
                    async move {
                        let child = ctx.install(Plugin::new("child", move |ctx| {
                            let saved = saved.clone();
                            async move {
                                *saved.lock().unwrap() = Some(ctx.dependency(&database())?);
                                Ok(())
                            }
                        }))?;
                        child.ready().await?;
                        Ok(())
                    }
                }
            })
            .requires(&database()),
        )
        .unwrap();
    bounded(parent.ready()).await.unwrap();
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let handle = saved.lock().unwrap().as_ref().unwrap().clone();
    let call = tokio::spawn({
        let entered = entered.clone();
        let release = release.clone();
        let open = open.clone();
        async move {
            handle
                .call(move |_, _| {
                    Box::pin(async move {
                        entered.add_permits(1);
                        release.acquire().await.unwrap().forget();
                        assert!(open.load(Ordering::SeqCst));
                        Ok(())
                    })
                })
                .await
        }
    });
    bounded(entered.acquire()).await.unwrap().forget();
    call.abort();
    let _ = call.await;
    let stopping = tokio::spawn(async move { db.dispose().await });
    bounded(async {
        while parent.snapshot().state != Lifecycle::Unloading {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(open.load(Ordering::SeqCst));
    assert!(!stopping.is_finished());
    release.add_permits(1);
    bounded(stopping).await.unwrap().unwrap();
    assert!(!open.load(Ordering::SeqCst));
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn inherited_edges_preserve_initialization_wait_cycle_diagnostics() {
    let rt = Runtime::new();
    rt.install(database_plugin(1)).unwrap();
    let parent = rt
        .install(
            Plugin::new("parent", |ctx| async move {
                let child = ctx.install(
                    Plugin::new("child", |ctx| async move {
                        assert_eq!(read(&ctx)?, 1);
                        Ok(())
                    })
                    .requires(&router()),
                )?;
                assert_eq!(child.ready().await.unwrap_err(), Error::WouldDeadlock);
                ctx.provide(&router(), Arc::new(()))
            })
            .requires(&database())
            .provides(&router()),
        )
        .unwrap();
    bounded(parent.ready()).await.unwrap();
    bounded(rt.shutdown()).await.unwrap();
}
