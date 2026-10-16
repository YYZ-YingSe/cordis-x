use cordis_contract::RealmId;
use cordis_kernel::Lifecycle;
use cordis_runtime::*;
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

fn service() -> Service<u32> {
    Service::new("realm.database", "1")
}
fn view(realm: u64) -> View {
    View::new().in_realm(&service(), RealmId(realm))
}
async fn bounded<T>(f: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), f)
        .await
        .expect("realm transition stalled")
}
async fn provider(rt: &Runtime, realm: u64, value: u32) -> PluginHandle {
    let h = rt
        .install_in(
            view(realm),
            Plugin::new("database", move |ctx| async move {
                ctx.provide(&service(), Arc::new(value))
            })
            .provides(&service()),
        )
        .unwrap();
    bounded(h.ready()).await.unwrap();
    h
}
fn consumer(saved: Arc<Mutex<Option<Context>>>, starts: Arc<AtomicUsize>) -> Plugin {
    Plugin::new("reader", move |ctx| {
        let saved = saved.clone();
        let starts = starts.clone();
        async move {
            ctx.dependency(&service())?.with(|_, _| Ok(()))?;
            starts.fetch_add(1, Ordering::SeqCst);
            *saved.lock().unwrap() = Some(ctx);
            Ok(())
        }
    })
    .requires(&service())
}
fn context(saved: &Arc<Mutex<Option<Context>>>) -> Context {
    saved.lock().unwrap().as_ref().unwrap().clone()
}

#[tokio::test]
async fn moving_a_binding_keeps_instances_and_tracks_live_derived_views_only() {
    let rt = Runtime::new();
    let p = provider(&rt, 0, 7).await;
    let saved: Arc<Mutex<Option<Context>>> = Arc::default();
    let starts = Arc::new(AtomicUsize::new(0));
    let c = rt
        .install_in(view(0), consumer(saved.clone(), starts.clone()))
        .unwrap();
    c.ready().await.unwrap();
    let ctx = context(&saved);
    let old_snapshot = ctx.snapshot();
    let inherited_saved: Arc<Mutex<Option<Context>>> = Arc::default();
    let child = ctx
        .install(Plugin::new("inherited reader", {
            let saved = inherited_saved.clone();
            move |ctx| {
                let saved = saved.clone();
                async move {
                    ctx.dependency(&service())?.with(|_, _| Ok(()))?;
                    *saved.lock().unwrap() = Some(ctx);
                    Ok(())
                }
            }
        }))
        .unwrap();
    child.ready().await.unwrap();
    // An explicit override remains a boundary even when its old value equals
    // the parent's realm. A captured snapshot is an independent fixed view too.
    let isolated = ctx
        .derive(ctx.view().in_realm(&service(), RealmId(0)))
        .install(consumer(Arc::default(), Arc::default()))
        .unwrap();
    isolated.ready().await.unwrap();
    let fixed = old_snapshot
        .install(consumer(Arc::default(), Arc::default()))
        .unwrap();
    fixed.ready().await.unwrap();
    let before = [p.snapshot(), c.snapshot(), child.snapshot()];
    let old_handle = ctx.dependency(&service()).unwrap();
    let scope = ctx.scope("request").unwrap();
    scope.ready().await.unwrap();
    let report = rt
        .reconfigure_realms(&[(p.clone(), view(1)), (c.clone(), view(1))])
        .unwrap();
    for (h, before) in [&p, &c, &child].into_iter().zip(before) {
        let after = bounded(h.ready()).await.unwrap();
        assert_eq!(after.activation, before.activation);
        assert_eq!(after.committed, before.committed);
    }
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert!(
        report
            .changes
            .iter()
            .any(|change| change.fiber == child.id())
    );
    assert!(!report.withdrawn.iter().any(|a| a.fiber == c.id()));
    assert_eq!(
        bounded(isolated.settled()).await.unwrap().state,
        Lifecycle::Inactive
    );
    assert_eq!(
        bounded(fixed.settled()).await.unwrap().state,
        Lifecycle::Inactive
    );
    assert_eq!(ctx.view().realm(&service()), RealmId(1));
    assert_eq!(old_snapshot.view().realm(&service()), RealmId(0));
    assert_eq!(
        old_handle
            .with(|value, call| Ok((*value, call.view().realm(&service()))))
            .unwrap(),
        (7, RealmId(1))
    );
    // An existing request scope keeps its use snapshot and the same committed
    // dependency even though its live definition namespace follows the source.
    assert_eq!(
        scope
            .run(|ctx| async move {
                let value = ctx.dependency(&service())?.with(|value, _| Ok(*value))?;
                Ok((value, ctx.view().realm(&service())))
            })
            .await
            .unwrap(),
        (7, RealmId(0))
    );
    let later = ctx
        .install(consumer(Arc::default(), Arc::default()))
        .unwrap();
    bounded(later.ready()).await.unwrap();
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn changed_dependency_drains_old_calls_and_cleans_with_old_binding() {
    let rt = Runtime::new();
    let a = provider(&rt, 1, 11).await;
    let b = provider(&rt, 2, 22).await;
    let saved: Arc<Mutex<Option<Context>>> = Arc::default();
    let cleaned: Arc<Mutex<Vec<u32>>> = Arc::default();
    let c = rt
        .install_in(
            view(1),
            Plugin::new("reader", {
                let saved = saved.clone();
                let cleaned = cleaned.clone();
                move |ctx| {
                    let saved = saved.clone();
                    let cleaned = cleaned.clone();
                    async move {
                        ctx.defer("reader cleanup", move |ctx| async move {
                            let value = ctx.dependency(&service())?.with(|value, _| Ok(*value))?;
                            cleaned.lock().unwrap().push(value);
                            Ok(())
                        })?;
                        *saved.lock().unwrap() = Some(ctx);
                        Ok(())
                    }
                }
            })
            .requires(&service()),
        )
        .unwrap();
    c.ready().await.unwrap();
    let before = c.snapshot().activation.unwrap();
    let old = context(&saved).dependency(&service()).unwrap();
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let running = tokio::spawn({
        let entered = entered.clone();
        let release = release.clone();
        let old = old.clone();
        async move {
            old.call(move |value, ctx| {
                Box::pin(async move {
                    entered.add_permits(1);
                    release.acquire().await.unwrap().forget();
                    Ok((*value, ctx.view().realm(&service())))
                })
            })
            .await
        }
    });
    bounded(entered.acquire()).await.unwrap().forget();
    let report = rt.reconfigure_realms(&[(c.clone(), view(2))]).unwrap();
    assert!(report.withdrawn.contains(&before));
    assert_eq!(c.snapshot().state, Lifecycle::Unloading);
    assert!(old.with(|_, _| Ok(())).is_err());
    assert!(cleaned.lock().unwrap().is_empty());
    release.add_permits(1);
    assert_eq!(bounded(running).await.unwrap().unwrap(), (11, RealmId(1)));
    let after = bounded(c.ready()).await.unwrap().activation.unwrap();
    assert_eq!(after.fiber, before.fiber);
    assert_eq!(after.epoch, before.epoch + 1);
    assert_eq!(*cleaned.lock().unwrap(), vec![11]);
    assert_eq!(
        context(&saved)
            .dependency(&service())
            .unwrap()
            .with(|value, _| Ok(*value))
            .unwrap(),
        22
    );
    assert_eq!(a.snapshot().activation.unwrap().epoch, 1);
    assert_eq!(b.snapshot().activation.unwrap().epoch, 1);
    bounded(rt.shutdown()).await.unwrap();
    assert_eq!(*cleaned.lock().unwrap(), vec![11, 22]);
}

#[tokio::test]
async fn late_acquisition_recovers_once_before_new_namespace_publishes() {
    let rt = Runtime::new();
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let starts = Arc::new(AtomicUsize::new(0));
    let cleaned: Arc<Mutex<Vec<RealmId>>> = Arc::default();
    let p = rt
        .install_in(
            view(1),
            Plugin::new("late provider", {
                let entered = entered.clone();
                let release = release.clone();
                let starts = starts.clone();
                let cleaned = cleaned.clone();
                move |ctx| {
                    let entered = entered.clone();
                    let release = release.clone();
                    let starts = starts.clone();
                    let cleaned = cleaned.clone();
                    async move {
                        let first = starts.fetch_add(1, Ordering::SeqCst) == 0;
                        ctx.acquire(
                            "late object",
                            async move {
                                if first {
                                    entered.add_permits(1);
                                    release.acquire().await.unwrap().forget();
                                }
                                Ok(())
                            },
                            move |_, cleanup| async move {
                                cleaned
                                    .lock()
                                    .unwrap()
                                    .push(cleanup.view().realm(&service()));
                                Ok(())
                            },
                        )
                        .await?;
                        ctx.provide(&service(), Arc::new(9))
                    }
                }
            })
            .provides(&service()),
        )
        .unwrap();
    bounded(entered.acquire()).await.unwrap().forget();
    let first = p.snapshot().activation.unwrap();
    rt.reconfigure_realms(&[(p.clone(), view(2))]).unwrap();
    assert_eq!(p.snapshot().state, Lifecycle::Unloading);
    assert!(
        rt.install_in(
            view(1),
            Plugin::new("collision", |_| async { Ok(()) }).provides(&service())
        )
        .is_err()
    );
    release.add_permits(1);
    let after = bounded(p.ready()).await.unwrap();
    assert_eq!(after.activation.unwrap().epoch, first.epoch + 1);
    assert_eq!(*cleaned.lock().unwrap(), vec![RealmId(1)]);
    bounded(rt.shutdown()).await.unwrap();
    assert_eq!(*cleaned.lock().unwrap(), vec![RealmId(1), RealmId(2)]);
}

#[tokio::test]
async fn conflict_after_preview_and_foreign_handles_cannot_mutate_views() {
    let rt = Runtime::new();
    let p = provider(&rt, 1, 7).await;
    let updates = [(p.clone(), view(2))];
    rt.validate_reconfiguration(&[], &[], &updates, &[])
        .unwrap();
    let _other = provider(&rt, 2, 8).await;
    let before = p.snapshot().activation;
    assert!(rt.reconfigure_realms(&updates).is_err());
    assert_eq!(p.snapshot().activation, before);
    let saved: Arc<Mutex<Option<Context>>> = Arc::default();
    let c = rt
        .install_in(view(1), consumer(saved.clone(), Arc::default()))
        .unwrap();
    bounded(c.ready()).await.unwrap();
    assert_eq!(
        context(&saved)
            .dependency(&service())
            .unwrap()
            .with(|v, _| Ok(*v))
            .unwrap(),
        7
    );
    let foreign = Runtime::new();
    assert!(foreign.reconfigure_realms(&updates).is_err());
    bounded(rt.shutdown()).await.unwrap();
    foreign.shutdown().await.unwrap();
}

#[tokio::test]
async fn swapping_two_live_providers_is_one_namespace_transaction() {
    let rt = Runtime::new();
    let a = provider(&rt, 1, 10).await;
    let b = provider(&rt, 2, 20).await;
    let left: Arc<Mutex<Option<Context>>> = Arc::default();
    let right: Arc<Mutex<Option<Context>>> = Arc::default();
    let c = rt
        .install_in(view(1), consumer(left.clone(), Arc::default()))
        .unwrap();
    let d = rt
        .install_in(view(2), consumer(right.clone(), Arc::default()))
        .unwrap();
    c.ready().await.unwrap();
    d.ready().await.unwrap();
    let before = [a.snapshot().activation, b.snapshot().activation];
    let report = rt
        .reconfigure_realms(&[(a.clone(), view(2)), (b.clone(), view(1))])
        .unwrap();
    assert_eq!(report.withdrawn.len(), 2);
    bounded(c.ready()).await.unwrap();
    bounded(d.ready()).await.unwrap();
    assert_eq!([a.snapshot().activation, b.snapshot().activation], before);
    assert_eq!(
        context(&left)
            .dependency(&service())
            .unwrap()
            .with(|v, _| Ok(*v))
            .unwrap(),
        20
    );
    assert_eq!(
        context(&right)
            .dependency(&service())
            .unwrap()
            .with(|v, _| Ok(*v))
            .unwrap(),
        10
    );
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn a_waiting_consumer_does_not_start_against_a_provider_withdrawn_by_the_same_update() {
    let rt = Runtime::new();
    let dependency = Service::<()>::new("realm.driver", "1");
    let binding = dependency.clone();
    let driver = rt
        .install(
            Plugin::new("driver", move |ctx| {
                let binding = binding.clone();
                async move { ctx.provide(&binding, Arc::new(())) }
            })
            .provides(&dependency),
        )
        .unwrap();
    driver.ready().await.unwrap();
    let starts = Arc::new(AtomicUsize::new(0));
    // Install the waiting consumer before its provider to exercise ID ordering.
    let c = rt
        .install_in(view(1), consumer(Arc::default(), starts.clone()))
        .unwrap();
    let p = rt
        .install(
            Plugin::new("database", |ctx| async move {
                ctx.provide(&service(), Arc::new(1))
            })
            .requires(&dependency)
            .provides(&service()),
        )
        .unwrap();
    p.ready().await.unwrap();
    let missing = View::new().in_realm(&dependency, RealmId(2));
    rt.reconfigure_realms(&[(c.clone(), view(0)), (p.clone(), missing)])
        .unwrap();
    assert_eq!(
        bounded(p.settled()).await.unwrap().state,
        Lifecycle::Inactive
    );
    assert_eq!(
        bounded(c.settled()).await.unwrap().state,
        Lifecycle::Inactive
    );
    assert_eq!(starts.load(Ordering::SeqCst), 0);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn failed_inverse_keeps_original_provider_pinned_across_further_realm_updates() {
    let rt = Runtime::new();
    let a = provider(&rt, 1, 1).await;
    provider(&rt, 2, 2).await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let c = rt
        .install_in(
            view(1),
            Plugin::new("failed reader", {
                let attempts = attempts.clone();
                move |ctx| {
                    let attempts = attempts.clone();
                    async move {
                        ctx.defer("failed inverse", move |ctx| async move {
                            assert_eq!(ctx.dependency(&service())?.with(|v, _| Ok(*v))?, 1);
                            attempts.fetch_add(1, Ordering::SeqCst);
                            Err(Error::Message("unrecovered effect".into()))
                        })?;
                        Ok(())
                    }
                }
            })
            .requires(&service()),
        )
        .unwrap();
    c.ready().await.unwrap();
    rt.reconfigure_realms(&[(c.clone(), view(2))]).unwrap();
    assert!(bounded(c.settled()).await.is_err());
    assert!(rt.reconfigure_realms(&[(c.clone(), view(3))]).is_err());
    assert!(bounded(a.dispose()).await.is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(a.snapshot().state, Lifecycle::Unloading);
    assert_eq!(c.snapshot().state, Lifecycle::Unloading);
    assert!(bounded(rt.shutdown()).await.is_err());
}
