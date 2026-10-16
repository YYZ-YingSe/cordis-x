#![cfg(feature = "watch")]

use cordis_kernel::{Activation, Lifecycle, Outcome};
use cordis_loader::{
    Configuration, Entry, Loader, Registry, json,
    watch::{ConfigWatcher, WatchError, WatchOptions, WatchPhase, WatchStatus},
};
use cordis_runtime::{Error, Plugin, Runtime};
use std::{
    future::Future,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Semaphore, watch};

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("watch timed out")
}
async fn state(
    updates: &mut watch::Receiver<WatchStatus>,
    predicate: impl Fn(&WatchStatus) -> bool,
) -> WatchStatus {
    bounded(async {
        loop {
            let status = updates.borrow_and_update().clone();
            if predicate(&status) {
                return status;
            }
            updates.changed().await.expect("watch stopped unexpectedly");
        }
    })
    .await
}
fn config(value: u64) -> Configuration {
    Configuration {
        entries: vec![
            Entry::new("worker", "worker", json!(value)),
            Entry::new("metrics", "metrics", json!(null)),
        ],
    }
}
fn save(path: &Path, config: &Configuration) {
    std::fs::write(path, serde_json::to_vec(config).unwrap()).unwrap();
}
fn replace(path: &Path, config: &Configuration) {
    let mut file = tempfile::NamedTempFile::new_in(path.parent().unwrap()).unwrap();
    std::io::Write::write_all(&mut file, &serde_json::to_vec(config).unwrap()).unwrap();
    file.persist(path).unwrap();
}
fn active(status: &WatchStatus, value: u64) -> bool {
    status.error.is_none()
        && status.phase == WatchPhase::Idle
        && status.target.entries.iter().any(|entry| {
            entry.id == "worker"
                && entry
                    .actual
                    .as_ref()
                    .is_some_and(|entry| entry.config == json!(value))
                && entry
                    .instance
                    .as_ref()
                    .is_some_and(|s| s.state == Lifecycle::Active)
        })
}
fn activation(status: &WatchStatus, id: &str) -> Activation {
    status
        .target
        .entries
        .iter()
        .find(|e| e.id == id)
        .unwrap()
        .instance
        .as_ref()
        .unwrap()
        .activation
        .unwrap()
}
fn options() -> WatchOptions {
    WatchOptions {
        debounce: Duration::from_millis(20),
        max_bytes: 2048,
    }
}
struct Fixture {
    _dir: tempfile::TempDir,
    path: PathBuf,
    runtime: Runtime,
    loader: Loader,
    trace: Arc<Mutex<Vec<String>>>,
    cleanup_entered: Arc<Semaphore>,
    cleanup_release: Arc<Semaphore>,
}
impl Fixture {
    fn new(block_cleanup: bool, fail_cleanup: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        let runtime = Runtime::new();
        let trace = Arc::new(Mutex::new(Vec::new()));
        let cleanup_entered = Arc::new(Semaphore::new(0));
        let cleanup_release = Arc::new(Semaphore::new(0));
        let mut registry = Registry::new();
        registry
            .register("worker", {
                let trace = trace.clone();
                let entered = cleanup_entered.clone();
                let release = cleanup_release.clone();
                move |value| {
                    let value = value
                        .as_u64()
                        .ok_or_else(|| Error::Message("expected number".into()))?;
                    let trace = trace.clone();
                    let entered = entered.clone();
                    let release = release.clone();
                    Ok(Plugin::new("worker", move |ctx| {
                        let trace = trace.clone();
                        let entered = entered.clone();
                        let release = release.clone();
                        async move {
                            trace.lock().unwrap().push(format!("start {value}"));
                            ctx.defer("resource", move |_| async move {
                                trace.lock().unwrap().push(format!("stop {value}"));
                                if value == 1 {
                                    entered.add_permits(1);
                                    if block_cleanup {
                                        release.acquire().await.unwrap().forget();
                                    }
                                    if fail_cleanup {
                                        return Err(Error::Message("resource still owned".into()));
                                    }
                                }
                                Ok(())
                            })?;
                            Ok(())
                        }
                    }))
                }
            })
            .unwrap();
        registry
            .register("metrics", |_| {
                Ok(Plugin::new("metrics", |_| async { Ok(()) }))
            })
            .unwrap();
        let loader = Loader::new(runtime.clone(), registry);
        Self {
            _dir: dir,
            path,
            runtime,
            loader,
            trace,
            cleanup_entered,
            cleanup_release,
        }
    }
    fn watch(&self) -> ConfigWatcher {
        ConfigWatcher::start(self.loader.clone(), &self.path, options()).unwrap()
    }
    async fn clear(&self) {
        bounded(self.loader.apply(Configuration::default()))
            .await
            .unwrap();
        bounded(self.runtime.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn automatic_save_and_atomic_replace_preserve_unaffected_activation() {
    let f = Fixture::new(false, false);
    save(&f.path, &config(1));
    let watcher = f.watch();
    let mut updates = watcher.subscribe();
    let first = state(&mut updates, |s| active(s, 1)).await;
    save(&f.path, &config(2));
    let second = state(&mut updates, |s| active(s, 2)).await;
    replace(&f.path, &config(3));
    let third = state(&mut updates, |s| active(s, 3)).await;
    assert_eq!(
        activation(&first, "metrics"),
        activation(&second, "metrics")
    );
    assert_eq!(activation(&first, "metrics"), activation(&third, "metrics"));
    assert_ne!(activation(&first, "worker"), activation(&third, "worker"));
    assert_eq!(third.submissions, 3);
    bounded(watcher.stop()).await.unwrap();
    assert_eq!(f.loader.snapshot().entries.len(), 2); // Stop does not dispose.
    f.clear().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_missing_and_oversize_input_leave_live_state_then_recover() {
    let f = Fixture::new(false, false);
    let watcher = f.watch();
    let mut updates = watcher.subscribe();
    state(&mut updates, |s| {
        matches!(s.error, Some(WatchError::Read(_)))
    })
    .await;
    save(&f.path, &config(1));
    let first = state(&mut updates, |s| active(s, 1)).await;
    for bad in [b"{".to_vec(), vec![255], vec![b' '; 2049]] {
        let reads = updates.borrow().reads;
        std::fs::write(&f.path, bad).unwrap();
        let rejected = state(&mut updates, |s| {
            s.reads > reads && s.phase == WatchPhase::Idle && s.error.is_some()
        })
        .await;
        assert_eq!(
            rejected.target.applied_revision,
            first.target.applied_revision
        );
        assert_eq!(
            activation(&rejected, "worker"),
            activation(&first, "worker")
        );
    }
    // Good input clears the error while retaining the unaffected activation.
    save(&f.path, &config(1));
    state(&mut updates, |s| active(s, 1)).await;
    std::fs::remove_file(&f.path).unwrap();
    state(&mut updates, |s| {
        matches!(s.error, Some(WatchError::Read(_)))
    })
    .await;
    save(&f.path, &config(1));
    let recovered = state(&mut updates, |s| active(s, 1)).await;
    assert_eq!(
        activation(&recovered, "worker"),
        activation(&first, "worker")
    );
    let mut invalid = config(2);
    invalid.entries[0].source = "unregistered".into();
    replace(&f.path, &invalid);
    let rejected = state(&mut updates, |s| {
        matches!(s.error, Some(WatchError::Apply(_)))
    })
    .await;
    assert_eq!(
        activation(&rejected, "worker"),
        activation(&first, "worker")
    );
    replace(&f.path, &config(2));
    state(&mut updates, |s| active(s, 2)).await;
    bounded(watcher.stop()).await.unwrap();
    f.clear().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saves_during_recovery_coalesce_to_latest_without_cancelling_accepted_apply() {
    let f = Fixture::new(true, false);
    save(&f.path, &config(1));
    let watcher = f.watch();
    let mut updates = watcher.subscribe();
    state(&mut updates, |s| active(s, 1)).await;
    save(&f.path, &config(2));
    bounded(f.cleanup_entered.acquire()).await.unwrap().forget();
    for value in 3..=20 {
        replace(&f.path, &config(value));
    }
    let before = updates.borrow().input_revision;
    watcher.refresh().unwrap();
    let pending = state(&mut updates, |s| s.input_revision > before).await;
    assert_eq!(pending.submissions, 2);
    assert_eq!(pending.phase, WatchPhase::Applying);
    f.cleanup_release.add_permits(1);
    let last = state(&mut updates, |s| active(s, 20)).await;
    assert_eq!(last.submissions, 3);
    assert_eq!(
        *f.trace.lock().unwrap(),
        ["start 1", "stop 1", "start 2", "stop 2", "start 20"]
    );
    bounded(watcher.stop()).await.unwrap();
    f.clear().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_stop_wait_and_drop_preserve_accepted_apply_but_discard_pending_input() {
    for cancel_stop in [false, true] {
        let f = Fixture::new(true, false);
        save(&f.path, &config(1));
        let watcher = f.watch();
        let mut updates = watcher.subscribe();
        state(&mut updates, |s| active(s, 1)).await;
        save(&f.path, &config(2));
        bounded(f.cleanup_entered.acquire()).await.unwrap().forget();
        replace(&f.path, &config(3));
        watcher.refresh().unwrap();
        if cancel_stop {
            let stop = tokio::spawn(watcher.stop());
            state(&mut updates, |s| s.phase == WatchPhase::Stopping).await;
            assert!(!stop.is_finished());
            stop.abort();
            assert!(stop.await.unwrap_err().is_cancelled());
        } else {
            drop(watcher);
        }
        f.cleanup_release.add_permits(1);
        let stopped = state(&mut updates, |s| s.phase == WatchPhase::Stopped).await;
        assert_eq!(stopped.submissions, 2);
        bounded(f.loader.settled()).await.unwrap();
        assert!(f.trace.lock().unwrap().contains(&"start 2".into()));
        assert!(!f.trace.lock().unwrap().contains(&"start 3".into()));
        f.clear().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_update_can_withdraw_loading_initializer_and_collect_late_inverse() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.json");
    let runtime = Runtime::new();
    let cleaned = Arc::new(AtomicBool::new(false));
    let mut registry = Registry::new();
    registry
        .register("pending", {
            let cleaned = cleaned.clone();
            move |_| {
                let cleaned = cleaned.clone();
                Ok(Plugin::new("pending", move |ctx| {
                    let cleaned = cleaned.clone();
                    async move {
                        ctx.cancelled().await;
                        ctx.defer("late", move |_| async move {
                            cleaned.store(true, Ordering::SeqCst);
                            Ok(())
                        })?;
                        Ok(())
                    }
                }))
            }
        })
        .unwrap();
    let loader = Loader::new(runtime.clone(), registry);
    save(
        &path,
        &Configuration {
            entries: vec![Entry::new("pending", "pending", json!(null))],
        },
    );
    let watcher = ConfigWatcher::start(loader, &path, options()).unwrap();
    let mut updates = watcher.subscribe();
    state(&mut updates, |s| {
        s.phase == WatchPhase::Idle
            && s.target.entries.iter().any(|e| {
                e.instance
                    .as_ref()
                    .is_some_and(|i| i.state == Lifecycle::Loading)
            })
    })
    .await;
    replace(&path, &Configuration::default());
    state(&mut updates, |s| {
        s.submissions == 2 && s.phase == WatchPhase::Idle && s.target.entries.is_empty()
    })
    .await;
    assert!(cleaned.load(Ordering::SeqCst));
    bounded(watcher.stop()).await.unwrap();
    bounded(runtime.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleanup_failure_remains_visible_and_duplicate_events_do_not_retry() {
    let f = Fixture::new(false, true);
    save(&f.path, &config(1));
    let watcher = f.watch();
    let mut updates = watcher.subscribe();
    state(&mut updates, |s| active(s, 1)).await;
    save(&f.path, &config(2));
    let failed = state(&mut updates, |s| {
        matches!(s.error, Some(WatchError::Apply(_)))
    })
    .await;
    assert!(failed.target.entries.iter().any(|e| {
        e.instance
            .as_ref()
            .is_some_and(|i| matches!(i.outcome, Some(Outcome::CleanupFailed(_))))
    }));
    replace(&f.path, &config(2)); // Same content, new inode.
    let unchanged = state(&mut updates, |s| {
        s.reads > failed.reads && s.phase == WatchPhase::Idle
    })
    .await;
    assert_eq!(unchanged.submissions, failed.submissions);
    assert_eq!(unchanged.error, failed.error);
    assert_eq!(*f.trace.lock().unwrap(), ["start 1", "stop 1"]);
    bounded(watcher.stop()).await.unwrap();
    assert!(bounded(f.runtime.shutdown()).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_initialization_failure_is_observed_without_a_new_file_event() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.json");
    let runtime = Runtime::new();
    let release = Arc::new(Semaphore::new(0));
    let mut registry = Registry::new();
    registry
        .register("broken", {
            let release = release.clone();
            move |_| {
                let release = release.clone();
                Ok(Plugin::new("broken", move |_| {
                    let release = release.clone();
                    async move {
                        release.acquire().await.unwrap().forget();
                        Err(Error::Message("initialization failed".into()))
                    }
                }))
            }
        })
        .unwrap();
    let loader = Loader::new(runtime.clone(), registry);
    save(
        &path,
        &Configuration {
            entries: vec![Entry::new("broken", "broken", json!(null))],
        },
    );
    let watcher = ConfigWatcher::start(loader.clone(), &path, options()).unwrap();
    let mut updates = watcher.subscribe();
    state(&mut updates, |s| {
        s.phase == WatchPhase::Idle && s.submissions == 1
    })
    .await;
    release.add_permits(1);
    let failed = state(&mut updates, |s| {
        s.target.entries.iter().any(|e| {
            e.instance
                .as_ref()
                .is_some_and(|i| matches!(i.outcome, Some(Outcome::InitializationFailed(_))))
        })
    })
    .await;
    assert!(failed.error.is_none()); // Apply succeeded; actual initialization failed later.
    assert_eq!(failed.submissions, 1);
    bounded(watcher.stop()).await.unwrap();
    bounded(loader.apply(Configuration::default()))
        .await
        .unwrap();
    bounded(runtime.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn package_commit_verifies_members_and_explicit_refresh_retries_unchanged_input() {
    use cordis_loader::package::{
        FileKind, HOST_TARGET, PackageFile, PackageManifest, PackageOptions, PreparedPackage,
        digest,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.json");
    let package = |name: &str, content: &[u8]| {
        let root = dir.path().join(name);
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("entry.txt"), content).unwrap();
        PackageManifest {
            target: HOST_TARGET.into(),
            entry: "entry.txt".into(),
            files: vec![PackageFile {
                path: "entry.txt".into(),
                sha256: digest(content),
                kind: FileKind::Data,
            }],
            host_dependencies: Default::default(),
        }
        .write(&root)
        .unwrap()
    };
    let first = package("v1", b"version one");
    let second = package("v2", b"version two");
    let options = PackageOptions {
        staging_dir: dir.path().join("snapshots"),
        ..Default::default()
    };
    let trace = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::new();
    registry
        .register_artifact(
            "package",
            {
                let options = options.clone();
                move |artifact| {
                    options
                        .verify(artifact)
                        .map_err(|e| Error::Message(e.to_string()))
                }
            },
            {
                let trace = trace.clone();
                move |artifact, _| {
                    let prepared = PreparedPackage::prepare(artifact, &options)
                        .map_err(|e| Error::Message(e.to_string()))?;
                    let trace = trace.clone();
                    Ok(Plugin::new("package", move |_| {
                        let prepared = prepared.clone();
                        let trace = trace.clone();
                        async move {
                            trace
                                .lock()
                                .unwrap()
                                .push(std::fs::read_to_string(prepared.entry()).unwrap());
                            Ok(())
                        }
                    }))
                }
            },
        )
        .unwrap();
    let runtime = Runtime::new();
    let loader = Loader::new(runtime.clone(), registry);
    let desired = |artifact| Configuration {
        entries: vec![Entry::new("code", "package", json!(null)).artifact(artifact)],
    };
    save(&path, &desired(first.clone()));
    let watcher = ConfigWatcher::start(loader.clone(), &path, WatchOptions::default()).unwrap();
    let mut updates = watcher.subscribe();
    let ready = |s: &WatchStatus, artifact: &cordis_loader::ArtifactRef| {
        s.phase == WatchPhase::Idle
            && s.error.is_none()
            && s.target.entries.iter().any(|e| {
                e.actual
                    .as_ref()
                    .is_some_and(|e| e.artifact.as_ref() == Some(artifact))
                    && e.instance
                        .as_ref()
                        .is_some_and(|i| i.state == Lifecycle::Active)
            })
    };
    let initial = state(&mut updates, |s| ready(s, &first)).await;
    std::fs::write(dir.path().join("v2/entry.txt"), b"corrupt").unwrap();
    replace(&path, &desired(second.clone()));
    let rejected = state(&mut updates, |s| {
        matches!(s.error, Some(WatchError::Apply(_)))
    })
    .await;
    assert_eq!(activation(&initial, "code"), activation(&rejected, "code"));
    assert_eq!(*trace.lock().unwrap(), ["version one"]);
    std::fs::write(dir.path().join("v2/entry.txt"), b"version two").unwrap();
    replace(&path, &desired(second.clone())); // Same digest: no implicit retry.
    let duplicate = state(&mut updates, |s| {
        s.reads > rejected.reads && s.phase == WatchPhase::Idle
    })
    .await;
    assert_eq!(duplicate.submissions, rejected.submissions);
    assert!(duplicate.error.is_some());
    watcher.refresh().unwrap();
    let applied = state(&mut updates, |s| ready(s, &second)).await;
    assert_eq!(applied.submissions, rejected.submissions + 1);
    assert_eq!(*trace.lock().unwrap(), ["version one", "version two"]);
    bounded(watcher.stop()).await.unwrap();
    bounded(loader.apply(Configuration::default()))
        .await
        .unwrap();
    bounded(runtime.shutdown()).await.unwrap();
}
