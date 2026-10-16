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
        .expect("sequence did not settle")
}
async fn phase(plugin: &PluginHandle, expected: Lifecycle) {
    bounded(async {
        while plugin.snapshot().state != expected {
            tokio::task::yield_now().await;
        }
    })
    .await;
}
fn database() -> Service<usize> {
    Service::new("test.database", "1")
}
fn search() -> Service<usize> {
    Service::new("test.search", "1")
}
fn provider(trace: Trace) -> Plugin {
    let generations = Arc::new(AtomicUsize::new(0));
    Plugin::new("database", move |ctx| {
        let generation = generations.fetch_add(1, Ordering::SeqCst) + 1;
        let trace = trace.clone();
        async move {
            ctx.defer("connection", move |_| async move {
                trace
                    .lock()
                    .unwrap()
                    .push(format!("database {generation} closed"));
                Ok(())
            })?;
            ctx.provide(&database(), Arc::new(generation))
        }
    })
    .provides(&database())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dependency_change_stops_later_steps_recovers_late_inverse_and_reactivates() {
    let rt = Runtime::new();
    let trace: Trace = Arc::default();
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let completions = Arc::new(Mutex::new(Vec::new()));
    let db = rt.install(provider(trace.clone())).unwrap();
    let worker = rt
        .install(
            Plugin::new("search", {
                let trace = trace.clone();
                let entered = entered.clone();
                let release = release.clone();
                let completions = completions.clone();
                move |ctx| {
                    let trace = trace.clone();
                    let entered = entered.clone();
                    let release = release.clone();
                    let completions = completions.clone();
                    async move {
                        let generation = ctx.dependency(&database())?.with(|db, _| Ok(*db))?;
                        let mut index = 0;
                        let effect = ctx.effect("search setup", move |step| {
                            let index_now = index;
                            index += 1;
                            let trace = trace.clone();
                            let entered = entered.clone();
                            let release = release.clone();
                            async move {
                                if index_now == 2 {
                                    trace.lock().unwrap().push(format!("publish {generation}"));
                                    step.provide(&search(), Arc::new(generation))?;
                                    return Ok(None);
                                }
                                if generation == 1 && index_now == 1 {
                                    entered.add_permits(1);
                                    release.acquire().await.unwrap().forget();
                                }
                                trace
                                    .lock()
                                    .unwrap()
                                    .push(format!("step {generation}:{index_now}"));
                                Ok(Some(Inverse::new(
                                    format!("step {index_now}"),
                                    move |cleanup| async move {
                                        assert_eq!(
                                            cleanup
                                                .dependency(&database())?
                                                .with(|db, _| Ok(*db))?,
                                            generation
                                        );
                                        trace
                                            .lock()
                                            .unwrap()
                                            .push(format!("undo {generation}:{index_now}"));
                                        Ok(())
                                    },
                                )))
                            }
                        })?;
                        let completion = effect.ready().await?;
                        completions.lock().unwrap().push((generation, completion));
                        Ok(())
                    }
                }
            })
            .requires(&database())
            .provides(&search()),
        )
        .unwrap();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let consumer = rt
        .install(
            Plugin::new("consumer", {
                let observed = observed.clone();
                move |ctx| {
                    let observed = observed.clone();
                    async move {
                        observed
                            .lock()
                            .unwrap()
                            .push(ctx.dependency(&search())?.with(|value, _| Ok(*value))?);
                        Ok(())
                    }
                }
            })
            .requires(&search()),
        )
        .unwrap();
    bounded(entered.acquire()).await.unwrap().forget();
    let restarting = tokio::spawn({
        let db = db.clone();
        async move { db.restart().await }
    });
    phase(&worker, Lifecycle::Unloading).await;
    assert!(!restarting.is_finished());
    release.add_permits(1);
    bounded(restarting).await.unwrap().unwrap();
    bounded(worker.ready()).await.unwrap();
    bounded(consumer.ready()).await.unwrap();
    assert_eq!(
        &*completions.lock().unwrap(),
        &[
            (1, EffectCompletion::Stopped),
            (2, EffectCompletion::Completed)
        ]
    );
    assert_eq!(&*observed.lock().unwrap(), &[2]);
    assert_eq!(
        &trace.lock().unwrap()[..5],
        &[
            "step 1:0",
            "step 1:1",
            "undo 1:1",
            "undo 1:0",
            "database 1 closed"
        ]
    );
    assert!(!trace.lock().unwrap().iter().any(|line| line == "publish 1"));
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_disposal_survives_cancelled_wait_and_recovers_a_late_step_once() {
    let rt = Runtime::new();
    let trace: Trace = Arc::default();
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let saved = Arc::new(Mutex::new(None));
    let independent = Arc::new(AtomicBool::new(false));
    let plugin = rt
        .install(Plugin::new("worker", {
            let trace = trace.clone();
            let entered = entered.clone();
            let release = release.clone();
            let saved = saved.clone();
            let independent = independent.clone();
            move |ctx| {
                let trace = trace.clone();
                let entered = entered.clone();
                let release = release.clone();
                let saved = saved.clone();
                let independent = independent.clone();
                async move {
                    ctx.defer("independent", move |_| async move {
                        independent.store(true, Ordering::SeqCst);
                        Ok(())
                    })?;
                    let mut index = 0;
                    let sequence = ctx.effect("sequence", move |_| {
                        let current = index;
                        index += 1;
                        let trace = trace.clone();
                        let entered = entered.clone();
                        let release = release.clone();
                        async move {
                            assert!(current < 2, "admitted a step after disposal");
                            if current == 1 {
                                entered.add_permits(1);
                                release.acquire().await.unwrap().forget();
                            }
                            trace.lock().unwrap().push(format!("step {current}"));
                            Ok(Some(Inverse::new(
                                format!("step {current}"),
                                move |_| async move {
                                    trace.lock().unwrap().push(format!("undo {current}"));
                                    Ok(())
                                },
                            )))
                        }
                    })?;
                    *saved.lock().unwrap() = Some(sequence);
                    Ok(())
                }
            }
        }))
        .unwrap();
    bounded(plugin.ready()).await.unwrap();
    bounded(entered.acquire()).await.unwrap().forget();
    let sequence = saved.lock().unwrap().as_ref().unwrap().clone();
    let disposing = tokio::spawn({
        let sequence = sequence.clone();
        async move { sequence.dispose().await }
    });
    bounded(async {
        while sequence.snapshot().status != EffectStatus::Releasing {
            tokio::task::yield_now().await;
        }
    })
    .await;
    disposing.abort();
    let _ = disposing.await;
    release.add_permits(1);
    assert_eq!(
        bounded(sequence.ready()).await.unwrap(),
        EffectCompletion::Stopped
    );
    let (a, b) = tokio::join!(sequence.dispose(), sequence.dispose());
    a.unwrap();
    b.unwrap();
    assert!(sequence.steps().is_empty());
    assert_eq!(
        &*trace.lock().unwrap(),
        &["step 0", "step 1", "undo 1", "undo 0"]
    );
    assert!(!independent.load(Ordering::SeqCst));
    bounded(rt.shutdown()).await.unwrap();
    assert!(independent.load(Ordering::SeqCst));
}

#[tokio::test]
async fn manual_disposal_during_initialization_can_use_committed_dependencies() {
    let rt = Runtime::new();
    rt.install(provider(Arc::default())).unwrap();
    let cleaned = Arc::new(AtomicBool::new(false));
    let plugin = rt
        .install(
            Plugin::new("worker", {
                let cleaned = cleaned.clone();
                move |ctx| {
                    let cleaned = cleaned.clone();
                    async move {
                        let mut yielded = false;
                        let sequence = ctx.effect("temporary setup", move |_| {
                            let done = std::mem::replace(&mut yielded, true);
                            let cleaned = cleaned.clone();
                            async move {
                                if done {
                                    return Ok(None);
                                }
                                Ok(Some(Inverse::new("temporary", move |cleanup| async move {
                                    let generation = cleanup
                                        .dependency(&database())?
                                        .call(|value, _| Box::pin(async move { Ok(*value) }))
                                        .await?;
                                    assert_eq!(generation, 1);
                                    cleaned.store(true, Ordering::SeqCst);
                                    Ok(())
                                })))
                            }
                        })?;
                        assert_eq!(sequence.ready().await?, EffectCompletion::Completed);
                        sequence.dispose().await?;
                        ctx.provide(&search(), Arc::new(1))
                    }
                }
            })
            .requires(&database())
            .provides(&search()),
        )
        .unwrap();
    bounded(plugin.ready()).await.unwrap();
    assert!(cleaned.load(Ordering::SeqCst));
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn forward_error_and_panic_preserve_yielded_inverses_for_scope_recovery() {
    for panic in [false, true] {
        let rt = Runtime::new();
        let cleaned = Arc::new(AtomicUsize::new(0));
        let plugin = rt
            .install(Plugin::new("failure", {
                let cleaned = cleaned.clone();
                move |ctx| {
                    let cleaned = cleaned.clone();
                    async move {
                        let mut index = 0;
                        ctx.effect("failed sequence", move |_| {
                            let current = index;
                            index += 1;
                            let cleaned = cleaned.clone();
                            async move {
                                if current == 0 {
                                    return Ok(Some(Inverse::new("first", move |_| async move {
                                        cleaned.fetch_add(1, Ordering::SeqCst);
                                        Ok(())
                                    })));
                                }
                                assert!(!panic, "injected effect panic");
                                Err(Error::Message("injected effect failure".into()))
                            }
                        })?
                        .ready()
                        .await?;
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
        assert!(plugin.effects().is_empty());
        bounded(rt.shutdown()).await.unwrap();
    }
}

#[tokio::test]
async fn inverse_failure_retains_earlier_steps_and_does_not_retry() {
    let rt = Runtime::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let saved = Arc::new(Mutex::new(None));
    let db = rt.install(provider(Arc::default())).unwrap();
    let plugin = rt
        .install(
            Plugin::new("failure", {
                let calls = calls.clone();
                let saved = saved.clone();
                move |ctx| {
                    let calls = calls.clone();
                    let saved = saved.clone();
                    async move {
                        let mut index = 0;
                        let effect = ctx.effect("failed cleanup", move |_| {
                            let current = index;
                            index += 1;
                            let calls = calls.clone();
                            async move {
                                if current == 2 {
                                    return Ok(None);
                                }
                                Ok(Some(Inverse::new(
                                    format!("step {current}"),
                                    move |_| async move {
                                        assert_eq!(
                                            current, 1,
                                            "earlier inverse ran despite failure"
                                        );
                                        calls.fetch_add(1, Ordering::SeqCst);
                                        Err(Error::Message("cannot clean".into()))
                                    },
                                )))
                            }
                        })?;
                        effect.ready().await?;
                        *saved.lock().unwrap() = Some(effect);
                        Ok(())
                    }
                }
            })
            .requires(&database()),
        )
        .unwrap();
    bounded(plugin.ready()).await.unwrap();
    let effect = saved.lock().unwrap().as_ref().unwrap().clone();
    assert!(bounded(effect.dispose()).await.is_err());
    assert!(bounded(effect.dispose()).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(effect.steps()[0].status, EffectStatus::Owned);
    assert!(matches!(effect.steps()[1].status, EffectStatus::Failed(_)));
    assert!(matches!(
        bounded(db.dispose()).await,
        Err(Error::CleanupBlocked(_, _))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(bounded(rt.shutdown()).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequence_inverse_drains_cancelled_nested_call_before_earlier_inverse() {
    async fn release_task(
        cleanup: Context,
        open: Arc<AtomicBool>,
        entered: Arc<Semaphore>,
        returned: Arc<Semaphore>,
        release: Arc<Semaphore>,
    ) -> Result<()> {
        let handle = cleanup.dependency(&database())?;
        let signal = entered.clone();
        let call = handle.call(move |_, _| {
            Box::pin(async move {
                signal.add_permits(1);
                release.acquire().await.unwrap().forget();
                assert!(open.load(Ordering::SeqCst));
                Ok(())
            })
        });
        tokio::pin!(call);
        tokio::select! {
            result = &mut call => result?,
            permit = entered.acquire() => permit.unwrap().forget(),
        }
        returned.add_permits(1);
        Ok(())
    }

    let rt = Runtime::new();
    let saved = Arc::new(Mutex::new(None));
    let open = Arc::new(AtomicBool::new(true));
    let entered = Arc::new(Semaphore::new(0));
    let returned = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    rt.install(provider(Arc::default())).unwrap();
    let plugin = rt
        .install(
            Plugin::new("client", {
                let saved = saved.clone();
                let open = open.clone();
                let entered = entered.clone();
                let returned = returned.clone();
                let release = release.clone();
                move |ctx| {
                    let saved = saved.clone();
                    let open = open.clone();
                    let entered = entered.clone();
                    let returned = returned.clone();
                    let release = release.clone();
                    async move {
                        let mut index = 0;
                        let sequence = ctx.effect("connection and task", move |_| {
                            let current = index;
                            index += 1;
                            let open = open.clone();
                            let entered = entered.clone();
                            let returned = returned.clone();
                            let release = release.clone();
                            async move {
                                match current {
                                    0 => {
                                        Ok(Some(Inverse::new("connection", move |_| async move {
                                            open.store(false, Ordering::SeqCst);
                                            Ok(())
                                        })))
                                    }
                                    1 => Ok(Some(Inverse::new("task", move |cleanup| {
                                        release_task(cleanup, open, entered, returned, release)
                                    }))),
                                    _ => Ok(None),
                                }
                            }
                        })?;
                        sequence.ready().await?;
                        *saved.lock().unwrap() = Some(sequence);
                        Ok(())
                    }
                }
            })
            .requires(&database()),
        )
        .unwrap();
    bounded(plugin.ready()).await.unwrap();
    let sequence = saved.lock().unwrap().as_ref().unwrap().clone();
    let disposing = tokio::spawn(async move { sequence.dispose().await });
    bounded(returned.acquire()).await.unwrap().forget();
    assert!(open.load(Ordering::SeqCst));
    assert!(!disposing.is_finished());
    release.add_permits(1);
    bounded(disposing).await.unwrap().unwrap();
    assert!(!open.load(Ordering::SeqCst));
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn dropped_ready_wait_keeps_iterator_running_but_old_step_context_expires() {
    let rt = Runtime::new();
    let gate = Arc::new(Semaphore::new(0));
    let saved = Arc::new(Mutex::new(None));
    let cleanup_count = Arc::new(AtomicUsize::new(0));
    let plugin = rt
        .install(Plugin::new("worker", {
            let gate = gate.clone();
            let saved = saved.clone();
            let cleanup_count = cleanup_count.clone();
            move |ctx| {
                let gate = gate.clone();
                let saved = saved.clone();
                let cleanup_count = cleanup_count.clone();
                async move {
                    let mut index = 0;
                    let mut previous: Option<Context> = None;
                    let effect = ctx.effect("sequence", move |step| {
                        let current = index;
                        index += 1;
                        let old = previous.replace(step);
                        let gate = gate.clone();
                        let cleanup_count = cleanup_count.clone();
                        async move {
                            if let Some(old) = old {
                                assert!(matches!(
                                    old.defer("invalid", |_| async { Ok(()) }),
                                    Err(Error::Closed)
                                ));
                            }
                            if current == 1 {
                                return Ok(None);
                            }
                            gate.acquire().await.unwrap().forget();
                            Ok(Some(Inverse::new("resource", move |_| async move {
                                cleanup_count.fetch_add(1, Ordering::SeqCst);
                                Ok(())
                            })))
                        }
                    })?;
                    *saved.lock().unwrap() = Some(effect);
                    Ok(())
                }
            }
        }))
        .unwrap();
    bounded(plugin.ready()).await.unwrap();
    let effect = saved.lock().unwrap().as_ref().unwrap().clone();
    let waiter = tokio::spawn({
        let effect = effect.clone();
        async move { effect.ready().await }
    });
    tokio::task::yield_now().await;
    waiter.abort();
    let _ = waiter.await;
    gate.add_permits(1);
    assert_eq!(
        bounded(effect.ready()).await.unwrap(),
        EffectCompletion::Completed
    );
    bounded(rt.shutdown()).await.unwrap();
    assert_eq!(cleanup_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn terminal_steps_collect_their_inverse_before_completion_or_failure() {
    for failed in [false, true] {
        let rt = Runtime::new();
        let trace: Trace = Arc::default();
        let saved = Arc::new(Mutex::new(None));
        let plugin = rt
            .install(Plugin::new("terminal", {
                let saved = saved.clone();
                let trace = trace.clone();
                move |ctx| {
                    let saved = saved.clone();
                    let trace = trace.clone();
                    async move {
                        let cleanup = trace.clone();
                        let sequence = ctx.effect_steps_with_cleanup(
                            "terminal inverse",
                            move |_| {
                                let trace = trace.clone();
                                async move {
                                    trace.lock().unwrap().push("forward".into());
                                    let inverse = Inverse::new("last", move |_| async move {
                                        trace.lock().unwrap().push("last".into());
                                        Ok(())
                                    });
                                    Ok(if failed {
                                        EffectStep::failed(
                                            Error::Message("forward failed".into()),
                                            Some(inverse),
                                        )
                                    } else {
                                        EffectStep::complete(Some(inverse))
                                    })
                                }
                            },
                            Inverse::new("producer", move |_| async move {
                                cleanup.lock().unwrap().push("producer".into());
                                Ok(())
                            }),
                        )?;
                        assert_eq!(sequence.ready().await.is_err(), failed);
                        assert_eq!(sequence.steps().len(), 2);
                        *saved.lock().unwrap() = Some(sequence);
                        Ok(())
                    }
                }
            }))
            .unwrap();
        bounded(plugin.ready()).await.unwrap();
        assert_eq!(*trace.lock().unwrap(), ["forward"]);
        let sequence = saved.lock().unwrap().take().unwrap();
        bounded(sequence.dispose()).await.unwrap();
        bounded(sequence.dispose()).await.unwrap();
        assert_eq!(*trace.lock().unwrap(), ["forward", "last", "producer"]);
        bounded(rt.shutdown()).await.unwrap();
    }
}

#[tokio::test]
async fn producer_cleanup_is_registered_even_when_disposed_before_first_step() {
    let rt = Runtime::new();
    let count = Arc::new(AtomicUsize::new(0));
    let plugin = rt
        .install(Plugin::new("never polled", {
            let count = count.clone();
            move |ctx| {
                let count = count.clone();
                async move {
                    let sequence = ctx.effect_steps_with_cleanup(
                        "stopped",
                        |_| async { panic!("step admitted after stop") },
                        Inverse::new("producer", move |_| async move {
                            count.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }),
                    )?;
                    sequence.dispose().await?;
                    assert_eq!(sequence.ready().await?, EffectCompletion::Stopped);
                    Ok(())
                }
            }
        }))
        .unwrap();
    bounded(plugin.ready()).await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    bounded(rt.shutdown()).await.unwrap();
}
