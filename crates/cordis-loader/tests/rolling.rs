use cordis_kernel::{Activation, Lifecycle, Outcome};
use cordis_loader::{
    Configuration, ConfigurationChanges, Entry, Registry, json,
    rolling::{
        Candidates, DeploymentError, DeploymentPhase, DeploymentSnapshot, Readiness,
        ReadinessPolicy, RollingDeployment,
    },
};
use cordis_runtime::{
    BoxFuture, Broker, Context, Error, Plugin, PluginHandle, Runtime, Service, ServiceHandle,
};
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

struct Version {
    value: u64,
    context: Context,
}
fn service() -> Service<Broker<Version>> {
    Service::new("deployment.version", "1")
}
fn config(value: u64) -> Configuration {
    Configuration {
        entries: vec![Entry::new("app", "app", json!({"version": value}))],
    }
}
fn policy() -> ReadinessPolicy {
    ReadinessPolicy {
        timeout: Duration::from_secs(2),
        interval: Duration::from_millis(2),
        consecutive_successes: 1,
    }
}
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(8), future)
        .await
        .expect("deployment stalled")
}
async fn state(
    deployment: &RollingDeployment<Version>,
    condition: impl Fn(&DeploymentSnapshot) -> bool,
) -> DeploymentSnapshot {
    let mut changes = deployment.subscribe();
    bounded(async {
        loop {
            let snapshot = deployment.snapshot();
            if condition(&snapshot) {
                return snapshot;
            }
            changes.changed().await.unwrap();
        }
    })
    .await
}
struct Fixture {
    runtime: Runtime,
    registry: Registry,
    candidates: Candidates<Version>,
    deployment: RollingDeployment<Version>,
    client: ServiceHandle<Broker<Version>>,
    consumer: PluginHandle,
    trace: Arc<Mutex<Vec<String>>>,
}
impl Fixture {
    async fn new<F>(policy: ReadinessPolicy, probe: F) -> Self
    where
        F: for<'a> Fn(&'a Version, Context) -> BoxFuture<'a, cordis_runtime::Result<Readiness>>
            + Send
            + Sync
            + 'static,
    {
        let runtime = Runtime::new();
        let broker = runtime
            .install(
                Plugin::new(
                    "broker",
                    |ctx| async move { ctx.provide_broker(&service()) },
                )
                .provides(&service()),
            )
            .unwrap();
        bounded(broker.ready()).await.unwrap();
        let saved = Arc::new(Mutex::new(None));
        let consumer = runtime
            .install(
                Plugin::new("consumer", {
                    let saved = saved.clone();
                    move |ctx| {
                        *saved.lock().unwrap() = Some(ctx.dependency(&service()));
                        async { Ok(()) }
                    }
                })
                .requires(&service()),
            )
            .unwrap();
        bounded(consumer.ready()).await.unwrap();
        let client = saved.lock().unwrap().take().unwrap().unwrap();
        let candidates = Candidates::default();
        let trace = Arc::new(Mutex::new(Vec::new()));
        let mut registry = Registry::new();
        registry
            .register("app", {
                let candidates = candidates.clone();
                let trace = trace.clone();
                move |value| {
                    let version = value["version"]
                        .as_u64()
                        .ok_or_else(|| Error::Message("version required".into()))?;
                    let init_failure = value["fail_init"] == true;
                    let loading = value["loading"] == true;
                    let cleanup_failure = value["fail_cleanup"] == true;
                    let candidates = candidates.clone();
                    let trace = trace.clone();
                    Ok(Plugin::new("app", move |ctx| {
                        let candidates = candidates.clone();
                        let trace = trace.clone();
                        async move {
                            trace.lock().unwrap().push(format!("start {version}"));
                            let cleanup_trace = trace.clone();
                            ctx.defer("implementation", move |_| async move {
                                cleanup_trace
                                    .lock()
                                    .unwrap()
                                    .push(format!("stop {version}"));
                                if cleanup_failure {
                                    Err(Error::Message("retained resource".into()))
                                } else {
                                    Ok(())
                                }
                            })?;
                            if loading {
                                ctx.cancelled().await;
                                ctx.defer("late inverse", move |_| async move {
                                    trace.lock().unwrap().push("late inverse".into());
                                    Ok(())
                                })?;
                            }
                            if init_failure {
                                return Err(Error::Message("startup failed".into()));
                            }
                            if !loading {
                                candidates.register(ctx.dependency(&service())?.register(
                                    Arc::new(Version {
                                        value: version,
                                        context: ctx.clone(),
                                    }),
                                )?)?;
                            }
                            Ok(())
                        }
                    })
                    .requires(&service()))
                }
            })
            .unwrap();
        let deployment = RollingDeployment::new(
            runtime.clone(),
            registry.clone(),
            candidates.clone(),
            client.clone(),
            policy,
            probe,
        )
        .unwrap();
        Self {
            runtime,
            registry,
            candidates,
            deployment,
            client,
            consumer,
            trace,
        }
    }
    async fn version(&self) -> u64 {
        bounded(
            self.client
                .route(|v, _| Box::pin(async move { Ok(v.value) })),
        )
        .await
        .unwrap()
    }
    async fn apply(&self, value: u64) -> DeploymentSnapshot {
        bounded(self.deployment.apply(config(value))).await.unwrap()
    }
    async fn close(&self) {
        bounded(self.deployment.apply(Configuration::default()))
            .await
            .unwrap();
        bounded(self.runtime.shutdown()).await.unwrap();
    }
}
fn ready(_: &Version, _: Context) -> BoxFuture<'_, cordis_runtime::Result<Readiness>> {
    Box::pin(async { Ok(Readiness::Ready) })
}
fn current(snapshot: &DeploymentSnapshot) -> Activation {
    snapshot
        .deployment
        .current
        .as_ref()
        .unwrap()
        .activation
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consecutive_readiness_promotes_then_drains_old_resources_without_restarting_consumer() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let f = Fixture::new(
        ReadinessPolicy {
            consecutive_successes: 2,
            ..policy()
        },
        {
            let attempts = attempts.clone();
            move |v, _| {
                let next = if v.value == 2 {
                    attempts.fetch_add(1, Ordering::SeqCst)
                } else {
                    10
                };
                Box::pin(async move {
                    Ok(if next == 1 {
                        Readiness::NotReady
                    } else {
                        Readiness::Ready
                    })
                })
            }
        },
    )
    .await;
    let first = f.apply(1).await;
    let activation = f.consumer.snapshot().activation;
    let resource = f
        .client
        .route(|v, ctx| {
            let version = v.value;
            Box::pin(async move {
                ctx.acquire(
                    "old transaction",
                    async move { Ok(version) },
                    |_, _| async { Ok(()) },
                )
                .await
            })
        })
        .await
        .unwrap();
    let update = tokio::spawn({
        let deployment = f.deployment.clone();
        async move { deployment.apply(config(2)).await }
    });
    state(&f.deployment, |s| {
        s.deployment.phase == DeploymentPhase::Draining
            && s.deployment.current.as_ref().unwrap().entry.config["version"] == 2
    })
    .await;
    assert_eq!(f.version().await, 2);
    assert_eq!(attempts.load(Ordering::SeqCst), 4);
    assert!(!update.is_finished());
    assert!(!f.trace.lock().unwrap().contains(&"stop 1".into()));
    assert_eq!(
        resource
            .call(|v, _| Box::pin(async move { Ok(*v) }))
            .await
            .unwrap(),
        1
    );
    resource.dispose().await.unwrap();
    let second = bounded(update).await.unwrap().unwrap();
    assert_ne!(current(&first), current(&second));
    assert_eq!(f.consumer.snapshot().activation, activation);
    assert_eq!(*f.trace.lock().unwrap(), ["start 1", "start 2", "stop 1"]);
    let duplicate = f.apply(2).await;
    assert_eq!(current(&duplicate), current(&second));
    assert_eq!(attempts.load(Ordering::SeqCst), 4);
    f.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readiness_rejection_panic_and_timeout_leave_old_route_and_recover_candidate() {
    for failure in [0, 1, 2] {
        let f = Fixture::new(
            ReadinessPolicy {
                timeout: Duration::from_millis(80),
                ..policy()
            },
            move |v, _| {
                let version = v.value;
                Box::pin(async move {
                    if version == 1 {
                        return Ok(Readiness::Ready);
                    }
                    match failure {
                        0 => Err(Error::Message("health rejected".into())),
                        1 => panic!("probe panic"),
                        _ => Ok(Readiness::NotReady),
                    }
                })
            },
        )
        .await;
        let old = f.apply(1).await;
        let error = bounded(f.deployment.apply(config(2))).await.unwrap_err();
        match failure {
            0 => assert!(matches!(error, DeploymentError::Probe(Error::Message(_)))),
            1 => assert!(matches!(error, DeploymentError::Probe(Error::Panic(_)))),
            _ => assert_eq!(error, DeploymentError::Timeout),
        }
        let failed = f.deployment.snapshot();
        assert_eq!(current(&failed), current(&old));
        assert_eq!(failed.loader.entries.len(), 1);
        assert!(failed.deployment.candidate.is_none());
        assert!(failed.deployment.cleanup_error.is_none());
        assert_eq!(f.version().await, 1);
        assert_eq!(*f.trace.lock().unwrap(), ["start 1", "start 2", "stop 2"]);
        f.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initialization_failure_and_loading_timeout_never_publish_candidate() {
    for mode in ["fail_init", "loading"] {
        let probes = Arc::new(AtomicUsize::new(0));
        let f = Fixture::new(
            ReadinessPolicy {
                timeout: Duration::from_millis(80),
                ..policy()
            },
            {
                let probes = probes.clone();
                move |v, _| {
                    if v.value == 2 {
                        probes.fetch_add(1, Ordering::SeqCst);
                    }
                    Box::pin(async { Ok(Readiness::Ready) })
                }
            },
        )
        .await;
        f.apply(1).await;
        let mut next = config(2);
        next.entries[0].config[mode] = json!(true);
        let error = bounded(f.deployment.apply(next)).await.unwrap_err();
        if mode == "loading" {
            assert_eq!(error, DeploymentError::Timeout);
            assert!(f.trace.lock().unwrap().contains(&"late inverse".into()));
        } else {
            assert!(matches!(error, DeploymentError::Candidate(_)));
        }
        assert_eq!(probes.load(Ordering::SeqCst), 0);
        assert_eq!(f.version().await, 1);
        f.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_probe_keeps_lease_until_cooperative_completion_even_if_apply_wait_is_cancelled()
{
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let f = Fixture::new(
        ReadinessPolicy {
            timeout: Duration::from_millis(80),
            ..policy()
        },
        {
            let entered = entered.clone();
            let release = release.clone();
            move |v, ctx| {
                let version = v.value;
                let entered = entered.clone();
                let release = release.clone();
                Box::pin(async move {
                    if version == 2 {
                        entered.add_permits(1);
                        ctx.cancelled().await;
                        release.acquire().await.unwrap().forget();
                    }
                    Ok(Readiness::Ready)
                })
            }
        },
    )
    .await;
    f.apply(1).await;
    let update = tokio::spawn({
        let deployment = f.deployment.clone();
        async move { deployment.apply(config(2)).await }
    });
    bounded(entered.acquire()).await.unwrap().forget();
    state(&f.deployment, |s| {
        s.deployment.phase == DeploymentPhase::Rejecting
            && s.loader.entries.iter().any(|e| {
                e.instance
                    .as_ref()
                    .is_some_and(|i| i.state == Lifecycle::Unloading)
            })
    })
    .await;
    assert!(!update.is_finished());
    assert!(!f.trace.lock().unwrap().contains(&"stop 2".into()));
    assert_eq!(f.version().await, 1);
    update.abort();
    assert!(update.await.unwrap_err().is_cancelled());
    release.add_permits(1);
    let failed = state(&f.deployment, |s| {
        s.deployment.phase == DeploymentPhase::Failed
    })
    .await;
    assert_eq!(failed.deployment.error, Some(DeploymentError::Timeout));
    assert!(failed.deployment.candidate.is_none());
    f.apply(3).await;
    assert_eq!(f.version().await, 3);
    f.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn old_cleanup_failure_keeps_new_route_and_blocks_further_generations() {
    let f = Fixture::new(policy(), ready).await;
    let mut first = config(1);
    first.entries[0].config["fail_cleanup"] = json!(true);
    bounded(f.deployment.apply(first)).await.unwrap();
    assert!(matches!(
        bounded(f.deployment.apply(config(2))).await,
        Err(DeploymentError::Drain(_))
    ));
    assert_eq!(f.version().await, 2);
    let failed = f.deployment.snapshot();
    assert!(failed.deployment.cleanup_error.is_some());
    assert!(failed.loader.entries.iter().any(|e| {
        e.instance
            .as_ref()
            .is_some_and(|i| matches!(i.outcome, Some(Outcome::CleanupFailed(_))))
    }));
    assert!(matches!(
        bounded(f.deployment.apply(config(3))).await,
        Err(DeploymentError::Blocked(_))
    ));
    assert!(!f.trace.lock().unwrap().contains(&"start 3".into()));
    assert!(bounded(f.runtime.shutdown()).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejection_and_failed_candidate_cleanup_are_reported_separately() {
    let f = Fixture::new(policy(), |v, _| {
        let version = v.value;
        Box::pin(async move {
            if version == 2 {
                Err(Error::Message("bad health".into()))
            } else {
                Ok(Readiness::Ready)
            }
        })
    })
    .await;
    f.apply(1).await;
    let mut next = config(2);
    next.entries[0].config["fail_cleanup"] = json!(true);
    assert!(matches!(
        bounded(f.deployment.apply(next)).await,
        Err(DeploymentError::Probe(_))
    ));
    let failed = f.deployment.snapshot();
    assert!(matches!(
        failed.deployment.error,
        Some(DeploymentError::Probe(_))
    ));
    assert!(failed.deployment.cleanup_error.is_some());
    assert!(failed.deployment.candidate.is_some());
    assert_eq!(f.version().await, 1);
    assert!(matches!(
        bounded(f.deployment.apply(config(3))).await,
        Err(DeploymentError::Blocked(_))
    ));
    assert!(bounded(f.runtime.shutdown()).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn competing_deployment_cannot_overwrite_route_and_preflight_failures_preserve_old_generation()
 {
    let f = Fixture::new(policy(), ready).await;
    let old = f.apply(1).await;
    let competing = RollingDeployment::new(
        f.runtime.clone(),
        f.registry.clone(),
        f.candidates.clone(),
        f.client.clone(),
        policy(),
        ready,
    )
    .unwrap();
    assert_eq!(
        bounded(competing.apply(config(2))).await.unwrap_err(),
        DeploymentError::Promote(Error::BrokerChanged)
    );
    assert_eq!(f.version().await, 1);
    assert!(competing.snapshot().loader.entries.is_empty());
    let mut bad = config(3);
    bad.entries[0].source = "unknown".into();
    assert!(matches!(
        bounded(f.deployment.apply(bad)).await,
        Err(DeploymentError::Prepare(_))
    ));
    assert_eq!(current(&f.deployment.snapshot()), current(&old));
    let multiple = Configuration {
        entries: vec![config(3).entries.remove(0), Entry::group("group", vec![])],
    };
    assert!(matches!(
        bounded(f.deployment.apply(multiple)).await,
        Err(DeploymentError::Invalid(_))
    ));
    f.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_entry_validates_before_clearing_and_can_be_enabled_as_new_generation() {
    let f = Fixture::new(policy(), ready).await;
    let old = f.apply(1).await;
    let mut disabled = config(1);
    disabled.entries[0].enabled = false;
    let mut bad = disabled.clone();
    bad.entries[0].source = "unknown".into();
    assert!(bounded(f.deployment.apply(bad)).await.is_err());
    assert_eq!(f.version().await, 1);
    bounded(f.deployment.apply(disabled)).await.unwrap();
    assert!(f.deployment.snapshot().loader.entries.is_empty());
    assert_eq!(
        f.client
            .route(|v, _| Box::pin(async move { Ok(v.value) }))
            .await
            .unwrap_err(),
        Error::BrokerUnavailable
    );
    let next = f.apply(1).await;
    assert_ne!(current(&old), current(&next));
    f.close().await;
}

#[cfg(feature = "watch")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_input_drives_readiness_promotion_and_pending_changes_use_the_same_bounded_watcher() {
    use cordis_loader::watch::{ConfigWatcher, WatchOptions, WatchPhase};
    let release = Arc::new(Semaphore::new(0));
    let f = Fixture::new(policy(), {
        let release = release.clone();
        move |v, _| {
            let version = v.value;
            let release = release.clone();
            Box::pin(async move {
                if version == 2 {
                    release.acquire().await.unwrap().forget();
                }
                Ok(Readiness::Ready)
            })
        }
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("deployment.json");
    let save = |value| {
        let mut file = tempfile::NamedTempFile::new_in(dir.path()).unwrap();
        std::io::Write::write_all(&mut file, &serde_json::to_vec(&config(value)).unwrap()).unwrap();
        file.persist(&path).unwrap();
    };
    save(1);
    let watcher = ConfigWatcher::start(
        f.deployment.clone(),
        &path,
        WatchOptions {
            debounce: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .unwrap();
    let mut updates = watcher.subscribe();
    state(&f.deployment, |s| {
        s.deployment
            .current
            .as_ref()
            .is_some_and(|g| g.entry.config["version"] == 1)
    })
    .await;
    let activation = f.consumer.snapshot().activation;
    save(2);
    state(&f.deployment, |s| {
        s.deployment.phase == DeploymentPhase::CheckingReadiness
            && s.deployment
                .candidate
                .as_ref()
                .is_some_and(|g| g.entry.config["version"] == 2)
    })
    .await;
    assert_eq!(f.version().await, 1);
    for version in 3..=8 {
        save(version);
    }
    watcher.refresh().unwrap();
    release.add_permits(1);
    bounded(async {
        loop {
            let snapshot = updates.borrow_and_update().clone();
            if snapshot.phase == WatchPhase::Idle
                && snapshot
                    .target
                    .deployment
                    .current
                    .as_ref()
                    .is_some_and(|g| g.entry.config["version"] == 8)
            {
                break;
            }
            updates.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(f.version().await, 8);
    assert_eq!(f.consumer.snapshot().activation, activation);
    for version in 3..8 {
        assert!(
            !f.trace
                .lock()
                .unwrap()
                .contains(&format!("start {version}"))
        );
    }
    let stopped = bounded(watcher.stop()).await.unwrap();
    assert_eq!(stopped.submissions, 3);
    f.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_success_cannot_promote_an_activation_that_failed_during_the_probe() {
    let f = Fixture::new(policy(), |version, ctx| {
        let value = version.value;
        Box::pin(async move {
            if value == 2 {
                ctx.fail("candidate backend lost")?;
            }
            Ok(Readiness::Ready)
        })
    })
    .await;
    let old = f.apply(1).await;
    assert!(matches!(
        bounded(f.deployment.apply(config(2))).await,
        Err(DeploymentError::Promote(Error::Closed))
    ));
    assert_eq!(f.version().await, 1);
    assert_eq!(current(&f.deployment.snapshot()), current(&old));
    assert!(f.deployment.snapshot().deployment.candidate.is_none());
    f.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_request_repairs_a_lost_serving_activation_without_reusing_stale_registration() {
    let f = Fixture::new(policy(), ready).await;
    let first = f.apply(1).await;
    let context = f
        .client
        .route(|version, _| Box::pin(async move { Ok(version.context.clone()) }))
        .await
        .unwrap();
    context.fail("serving backend lost").unwrap();
    state(&f.deployment, |s| {
        s.loader.entries.iter().any(|e| {
            e.instance
                .as_ref()
                .is_some_and(|i| i.state != Lifecycle::Active)
        })
    })
    .await;
    let second = f.apply(1).await;
    assert_ne!(current(&first), current(&second));
    assert_eq!(f.version().await, 1);
    assert_eq!(second.loader.entries.len(), 1);
    assert_eq!(*f.trace.lock().unwrap(), ["start 1", "stop 1", "start 1"]);
    f.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_deployments_run_fifo_and_each_apply_returns_its_own_committed_snapshot() {
    let release = Arc::new(Semaphore::new(0));
    let f = Fixture::new(policy(), {
        let release = release.clone();
        move |v, _| {
            let version = v.value;
            let release = release.clone();
            Box::pin(async move {
                if version == 2 {
                    release.acquire().await.unwrap().forget();
                }
                Ok(Readiness::Ready)
            })
        }
    })
    .await;
    f.apply(1).await;
    let mut second = Box::pin(f.deployment.apply(config(2)));
    assert!(futures_util::poll!(second.as_mut()).is_pending());
    state(&f.deployment, |s| {
        s.deployment.phase == DeploymentPhase::CheckingReadiness
    })
    .await;
    let mut third = Box::pin(f.deployment.apply(config(3)));
    assert!(futures_util::poll!(third.as_mut()).is_pending());
    release.add_permits(1);
    // Let both operations complete before collecting either reply.
    state(&f.deployment, |s| {
        s.deployment.phase == DeploymentPhase::Idle
            && s.deployment
                .current
                .as_ref()
                .is_some_and(|g| g.entry.config["version"] == 3)
    })
    .await;
    let second = bounded(second).await.unwrap();
    let third = bounded(third).await.unwrap();
    assert_eq!(second.deployment.revision, 2);
    assert_eq!(
        second.deployment.current.unwrap().entry.config["version"],
        2
    );
    assert_eq!(third.deployment.revision, 3);
    assert_eq!(third.deployment.current.unwrap().entry.config["version"], 3);
    assert_eq!(
        *f.trace.lock().unwrap(),
        ["start 1", "start 2", "stop 1", "start 3", "stop 2"]
    );
    f.close().await;
}
