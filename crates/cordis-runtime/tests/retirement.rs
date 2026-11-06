use cordis_runtime::{EffectStatus, Plugin, Runtime};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[tokio::test]
async fn completed_effects_leave_the_owner_ledger_but_keep_their_cached_result() {
    let runtime = Runtime::new();
    let count = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(Mutex::new(None));
    let (calls, terminal) = (count.clone(), observed.clone());
    let plugin = runtime
        .install(Plugin::new("turnover", move |ctx| {
            let (calls, terminal) = (calls.clone(), terminal.clone());
            async move {
                for _ in 0..10_000 {
                    let calls = calls.clone();
                    let effect = ctx.defer("temporary", move |_| async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })?;
                    let observer = effect.downgrade();
                    effect.dispose().await?;
                    drop(effect);
                    let result = observer.upgrade().expect("terminal observation");
                    assert_eq!(result.snapshot().status, EffectStatus::Released);
                    result.dispose().await?;
                    *terminal.lock().unwrap() = Some(observer);
                }
                Ok(())
            }
        }))
        .unwrap();
    plugin.ready().await.unwrap();
    assert!(
        plugin.effects().is_empty(),
        "successful history is not recovery work"
    );
    plugin.dispose().await.unwrap();
    let result = observed.lock().unwrap().take().unwrap().upgrade().unwrap();
    result.dispose().await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 10_000);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn retiring_an_interior_success_does_not_bypass_a_failed_inverse() {
    let runtime = Runtime::new();
    let log = Arc::new(Mutex::new(Vec::new()));
    let output = log.clone();
    let plugin = runtime
        .install(Plugin::new("ordered", move |ctx| {
            let output = output.clone();
            async move {
                let earlier = output.clone();
                ctx.defer("earlier", move |_| async move {
                    earlier.lock().unwrap().push("earlier");
                    Ok(())
                })?;
                let middle = output.clone();
                let middle = ctx.defer("middle", move |_| async move {
                    middle.lock().unwrap().push("middle");
                    Ok(())
                })?;
                ctx.defer("failed", |_| async {
                    Err(cordis_runtime::Error::Message("unrecovered".into()))
                })?;
                middle.dispose().await?;
                Ok(())
            }
        }))
        .unwrap();
    plugin.ready().await.unwrap();
    assert_eq!(plugin.effects().len(), 2);
    assert!(plugin.dispose().await.is_err());
    assert_eq!(*log.lock().unwrap(), vec!["middle"]);
    assert_eq!(plugin.effects().len(), 2);
}

#[tokio::test]
async fn child_scopes_retire_without_invalidating_held_terminal_observers() {
    let runtime = Runtime::new();
    let terminal = Arc::new(Mutex::new(None));
    let saved = terminal.clone();
    let root = runtime
        .install(Plugin::new("scope turnover", move |ctx| {
            let saved = saved.clone();
            async move {
                for _ in 0..2_000 {
                    let scope = ctx.scope("temporary scope")?;
                    scope.ready().await?;
                    scope.run(|_| async { Ok(()) }).await?;
                    scope.dispose().await?;
                    *saved.lock().unwrap() = Some(scope.downgrade());
                }
                Ok(())
            }
        }))
        .unwrap();
    root.ready().await.unwrap();
    assert_eq!(runtime.snapshots().len(), 1);
    let scope = terminal.lock().unwrap().take().unwrap().upgrade().unwrap();
    assert!(scope.snapshot().retired);
    scope.dispose().await.unwrap();
    assert!(scope.run(|_| async { Ok(()) }).await.is_err());
    root.dispose().await.unwrap();
    assert!(runtime.snapshots().is_empty());
    assert!(root.snapshot().retired);
    root.dispose().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn inverse_capacity_waits_for_cleanup_drain_and_is_retained_on_failure() {
    use cordis_runtime::{EffectStep, Inverse};
    use tokio::sync::{Semaphore, oneshot};
    for fail in [false, true] {
        let runtime = Runtime::new();
        let capacity = Arc::new(Semaphore::new(1));
        let permit = capacity.clone().try_acquire_owned().unwrap();
        let permit = Arc::new(Mutex::new(Some(permit)));
        let resumed = Arc::new(Semaphore::new(0));
        let released = resumed.clone();
        let (returned, returned_rx) = oneshot::channel();
        let returned = Arc::new(Mutex::new(Some(returned)));
        let saved = Arc::new(Mutex::new(None));
        let result = saved.clone();
        let plugin = runtime.install(Plugin::new("capacity", move |ctx| {
            let (permit, released, returned, saved) = (permit.clone(), released.clone(), returned.clone(), result.clone());
            async move {
                let sequence = ctx.effect_steps("one", move |_| {
                    let permit = permit.lock().unwrap().take().unwrap();
                    let released = released.clone();
                    let returned = returned.lock().unwrap().take().unwrap();
                    async move {
                        Ok(EffectStep::complete(Some(Inverse::new("draining", move |cleanup| async move {
                            let started = Arc::new(Semaphore::new(0));
                            let notify = started.clone();
                            let operation = cleanup.operation(move |_| async move {
                                notify.add_permits(1);
                                released.acquire().await.unwrap().forget();
                                Ok(())
                            });
                            tokio::pin!(operation);
                            tokio::select! {
                                _ = started.acquire() => (),
                                _ = &mut operation => panic!("operation must still be running"),
                            }
                            let _ = returned.send(());
                            if fail { Err(cordis_runtime::Error::Message("unrecovered".into())) }
                            else { Ok(()) }
                        }).retain_capacity(permit))))
                    }
                })?;
                sequence.ready().await?;
                *saved.lock().unwrap() = Some(sequence);
                Ok(())
            }
        })).unwrap();
        plugin.ready().await.unwrap();
        let sequence = saved.lock().unwrap().take().unwrap();
        let disposal = tokio::spawn(async move { sequence.dispose().await });
        returned_rx.await.unwrap();
        assert_eq!(
            capacity.available_permits(),
            0,
            "cleanup continuations still own capacity"
        );
        resumed.add_permits(1);
        assert_eq!(disposal.await.unwrap().is_err(), fail);
        assert_eq!(capacity.available_permits(), usize::from(!fail));
        assert_eq!(runtime.shutdown().await.is_err(), fail);
    }
}
