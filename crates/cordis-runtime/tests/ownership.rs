use cordis_kernel::{Error as KernelError, Lifecycle};
use cordis_runtime::*;
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Barrier, Semaphore};

async fn bounded<T>(f: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), f)
        .await
        .expect("ownership transition stalled")
}
async fn group(rt: &Runtime, name: &str) -> (PluginHandle, Context) {
    let saved = Arc::new(Mutex::new(None));
    let output = saved.clone();
    let handle = rt
        .install(Plugin::new(name, move |ctx| {
            let output = output.clone();
            async move {
                *output.lock().unwrap() = Some(ctx);
                Ok(())
            }
        }))
        .unwrap();
    bounded(handle.ready()).await.unwrap();
    let ctx = saved.lock().unwrap().take().unwrap();
    (handle, ctx)
}

#[tokio::test]
async fn moving_loading_subtree_preserves_late_acquisition_and_new_parent_drains_it() {
    let rt = Runtime::new();
    let (old, ctx) = group(&rt, "old").await;
    let (new, _) = group(&rt, "new").await;
    let started = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let closed = Arc::new(AtomicUsize::new(0));
    let child = ctx
        .install(Plugin::new("loading resource", {
            let started = started.clone();
            let release = release.clone();
            let closed = closed.clone();
            move |ctx| {
                let started = started.clone();
                let release = release.clone();
                let closed = closed.clone();
                async move {
                    ctx.acquire(
                        "late",
                        async move {
                            started.add_permits(1);
                            release.acquire().await.unwrap().forget();
                            Ok(7u32)
                        },
                        move |value, _| async move {
                            assert_eq!(*value, 7);
                            closed.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        },
                    )
                    .await?;
                    Ok(())
                }
            }
        }))
        .unwrap();
    bounded(started.acquire()).await.unwrap().forget();
    let activation = child.snapshot().activation;
    rt.reparent(&[(child.clone(), new.snapshot().activation)])
        .unwrap();
    bounded(old.dispose()).await.unwrap();
    assert_eq!(child.snapshot().activation, activation);
    assert!(!child.snapshot().retired);
    // The new owner closes while the operation is still in flight. Its eventual
    // value must be registered and cleaned exactly once before the owner stops.
    let stop = tokio::spawn(async move { new.dispose().await });
    bounded(async {
        while !child.snapshot().retired {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!stop.is_finished());
    release.add_permits(1);
    bounded(stop).await.unwrap().unwrap();
    assert_eq!(closed.load(Ordering::SeqCst), 1);
    assert_eq!(child.snapshot().state, Lifecycle::Inactive);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retirement_and_transfer_have_one_linearization_winner() {
    for _ in 0..24 {
        let rt = Runtime::new();
        let (old, ctx) = group(&rt, "old").await;
        let (new, _) = group(&rt, "new").await;
        let child = ctx
            .install(Plugin::new("child", |_| async { Ok(()) }))
            .unwrap();
        child.ready().await.unwrap();
        let a = child.snapshot().activation;
        let barrier = Arc::new(Barrier::new(2));
        let stop = tokio::spawn({
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;
                old.dispose().await
            }
        });
        barrier.wait().await;
        let moved = rt.reparent(&[(child.clone(), new.snapshot().activation)]);
        bounded(stop).await.unwrap().unwrap();
        match moved {
            Ok(_) => {
                assert_eq!(child.snapshot().state, Lifecycle::Active);
                assert_eq!(child.snapshot().activation, a);
                assert_eq!(child.snapshot().parent, new.snapshot().activation);
            }
            Err(Error::Kernel(KernelError::Retired(id))) => {
                assert_eq!(id, child.id());
                assert_eq!(child.snapshot().state, Lifecycle::Inactive);
            }
            other => panic!("unexpected race result: {other:?}"),
        }
        bounded(rt.shutdown()).await.unwrap();
    }
}

#[tokio::test]
async fn closing_destination_and_stale_parent_and_foreign_handles_reject_without_mutation() {
    let rt = Runtime::new();
    let (old, ctx) = group(&rt, "old").await;
    let (new, _) = group(&rt, "new").await;
    let destination = new.snapshot().activation;
    let child = ctx
        .install(Plugin::new("child", |_| async { Ok(()) }))
        .unwrap();
    child.ready().await.unwrap();
    new.dispose().await.unwrap();
    assert!(rt.reparent(&[(child.clone(), destination)]).is_err());
    assert_eq!(child.snapshot().parent, old.snapshot().activation);
    assert!(
        rt.reparent(&[(
            child.clone(),
            Some(cordis_kernel::Activation {
                epoch: destination.unwrap().epoch + 1,
                ..destination.unwrap()
            })
        )])
        .is_err()
    );
    let foreign = Runtime::new();
    assert!(foreign.reparent(&[(child.clone(), None)]).is_err());
    assert_eq!(child.snapshot().parent, old.snapshot().activation);
    bounded(rt.shutdown()).await.unwrap();
    foreign.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_dynamic_descendant_prevents_moving_its_ancestor() {
    let rt = Runtime::new();
    let (parent, ctx) = group(&rt, "parent").await;
    let (new, _) = group(&rt, "new").await;
    let child = ctx
        .install(Plugin::new("failed inverse", |ctx| async move {
            ctx.defer("failure", |_| async {
                Err(Error::Message("retained".into()))
            })?;
            Ok(())
        }))
        .unwrap();
    child.ready().await.unwrap();
    assert!(bounded(child.dispose()).await.is_err());
    let before = parent.snapshot();
    assert_eq!(
        rt.reparent(&[(parent.clone(), new.snapshot().activation)]),
        Err(Error::Kernel(KernelError::CleanupFailed(child.id())))
    );
    assert_eq!(parent.snapshot().parent, before.parent);
    assert_eq!(parent.snapshot().activation, before.activation);
    assert!(!child.effects().is_empty());
    assert!(bounded(rt.shutdown()).await.is_err());
}

#[tokio::test]
async fn ownership_and_dependency_union_cannot_form_recovery_cycle() {
    let rt = Runtime::new();
    let service = Service::<u32>::new("ownership.value", "1");
    let provider = rt
        .install(
            Plugin::new("provider", {
                let service = service.clone();
                move |ctx| {
                    let service = service.clone();
                    async move { ctx.provide(&service, Arc::new(7)) }
                }
            })
            .provides(&service),
        )
        .unwrap();
    provider.ready().await.unwrap();
    let consumer = rt
        .install(Plugin::new("consumer", |_| async { Ok(()) }).requires(&service))
        .unwrap();
    consumer.ready().await.unwrap();
    assert_eq!(
        rt.reparent(&[(provider.clone(), consumer.snapshot().activation)]),
        Err(Error::Kernel(KernelError::RecoveryCycle))
    );
    assert_eq!(provider.snapshot().parent, None);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn durable_broker_resource_participates_in_transfer_cycle_check() {
    let rt = Runtime::new();
    let service = Service::<Broker<u32>>::new("ownership.broker", "1");
    let broker = rt
        .install(
            Plugin::new("broker", {
                let service = service.clone();
                move |ctx| {
                    let service = service.clone();
                    async move { ctx.provide_broker(&service) }
                }
            })
            .provides(&service),
        )
        .unwrap();
    broker.ready().await.unwrap();
    let registered = Arc::new(Mutex::new(None));
    let provider = rt
        .install(
            Plugin::new("implementation", {
                let service = service.clone();
                let registered = registered.clone();
                move |ctx| {
                    let service = service.clone();
                    let registered = registered.clone();
                    async move {
                        *registered.lock().unwrap() =
                            Some(ctx.dependency(&service)?.register(Arc::new(7))?);
                        Ok(())
                    }
                }
            })
            .requires(&service),
        )
        .unwrap();
    provider.ready().await.unwrap();
    registered
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .promote()
        .unwrap();
    let consumer = rt
        .install(
            Plugin::new("resource owner", {
                let service = service.clone();
                move |ctx| {
                    let service = service.clone();
                    async move {
                        ctx.dependency(&service)?
                            .route(|_, ctx| {
                                Box::pin(async move {
                                    ctx.defer("durable", |_| async { Ok(()) })?;
                                    Ok(())
                                })
                            })
                            .await
                    }
                }
            })
            .requires(&service),
        )
        .unwrap();
    consumer.ready().await.unwrap();
    assert_eq!(
        rt.reparent(&[(provider.clone(), consumer.snapshot().activation)]),
        Err(Error::Kernel(KernelError::RecoveryCycle))
    );
    assert_eq!(provider.snapshot().parent, None);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn waiting_child_moves_and_later_activates_under_its_new_owner() {
    let rt = Runtime::new();
    let (old, ctx) = group(&rt, "old").await;
    let (new, _) = group(&rt, "new").await;
    let service = Service::<u32>::new("late.provider", "1");
    let child = ctx
        .install(Plugin::new("waiting", |_| async { Ok(()) }).requires(&service))
        .unwrap();
    assert_eq!(child.settled().await.unwrap().state, Lifecycle::Inactive);
    rt.reparent(&[(child.clone(), new.snapshot().activation)])
        .unwrap();
    old.dispose().await.unwrap();
    assert!(!child.snapshot().retired);
    let provider = rt
        .install(
            Plugin::new("provider", {
                let service = service.clone();
                move |ctx| {
                    let service = service.clone();
                    async move { ctx.provide(&service, Arc::new(1)) }
                }
            })
            .provides(&service),
        )
        .unwrap();
    provider.ready().await.unwrap();
    bounded(child.ready()).await.unwrap();
    assert_eq!(child.snapshot().parent, new.snapshot().activation);
    bounded(new.dispose()).await.unwrap();
    assert!(child.snapshot().retired);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn subsequent_realm_change_cannot_make_a_parent_depend_on_its_owned_provider() {
    let rt = Runtime::new();
    let service = Service::<u32>::new("ownership.realm-cycle", "1");
    let plugin = || {
        Plugin::new("provider", {
            let service = service.clone();
            move |ctx| {
                let service = service.clone();
                async move { ctx.provide(&service, Arc::new(1)) }
            }
        })
        .provides(&service)
    };
    let first = rt.install(plugin()).unwrap();
    first.ready().await.unwrap();
    let isolated = View::new().isolate(&service);
    let second = rt.install_in(isolated.clone(), plugin()).unwrap();
    second.ready().await.unwrap();
    let consumer = rt
        .install(Plugin::new("parent", |_| async { Ok(()) }).requires(&service))
        .unwrap();
    consumer.ready().await.unwrap();
    rt.reparent(&[(second.clone(), consumer.snapshot().activation)])
        .unwrap();
    let before = format!("{:?}", rt.snapshots());
    assert_eq!(
        rt.reconfigure_realms(&[(consumer, isolated)]),
        Err(Error::Kernel(KernelError::RecoveryCycle))
    );
    assert_eq!(format!("{:?}", rt.snapshots()), before);
    bounded(rt.shutdown()).await.unwrap();
}
