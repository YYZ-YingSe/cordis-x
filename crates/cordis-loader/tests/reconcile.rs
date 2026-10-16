use cordis_contract::RealmId;
use cordis_kernel::{Activation, Lifecycle, Outcome};
use cordis_loader::{
    Configuration, Entry, Error as LoaderError, Loader, LoaderSnapshot, Phase, Realm, Registry,
    json,
};
use cordis_runtime::*;
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

type Trace = Arc<Mutex<Vec<String>>>;
struct Database {
    value: u64,
    open: AtomicBool,
}
fn database() -> Service<Database> {
    Service::new("test.database", "1")
}
fn other() -> Service<()> {
    Service::new("test.other", "1")
}
fn config(entries: Vec<Entry>) -> Configuration {
    Configuration { entries }
}
fn provider(value: u64) -> Entry {
    Entry::new("db", "database", json!(value))
}
fn reader() -> Entry {
    Entry::new("reader", "reader", json!(null))
}
fn idle() -> Entry {
    Entry::new("idle", "idle", json!(null))
}
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("loader did not settle")
}
fn activation(state: &LoaderSnapshot, id: &str) -> Activation {
    state
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
fn registry(trace: Trace) -> Registry {
    let mut registry = Registry::new();
    registry
        .register("database", {
            let trace = trace.clone();
            move |value| {
                let value = value
                    .as_u64()
                    .ok_or_else(|| Error::Message("database config must be u64".into()))?;
                let trace = trace.clone();
                Ok(Plugin::new("database", move |ctx| {
                    let trace = trace.clone();
                    async move {
                        trace.lock().unwrap().push(format!("open {value}"));
                        let db = Arc::new(Database {
                            value,
                            open: AtomicBool::new(true),
                        });
                        let cleanup = db.clone();
                        ctx.defer("database", move |_| async move {
                            cleanup.open.store(false, Ordering::SeqCst);
                            trace.lock().unwrap().push(format!("close {value}"));
                            Ok(())
                        })?;
                        ctx.provide(&database(), db)
                    }
                })
                .provides(&database()))
            }
        })
        .unwrap();
    registry
        .register("reader", move |_| {
            let trace = trace.clone();
            Ok(Plugin::new("reader", move |ctx| {
                let trace = trace.clone();
                async move {
                    let value = ctx.dependency(&database())?.with(|db, _| Ok(db.value))?;
                    trace.lock().unwrap().push(format!("read {value}"));
                    ctx.defer("reader", move |ctx| async move {
                        ctx.dependency(&database())?.with(|db, _| {
                            assert!(db.open.load(Ordering::SeqCst));
                            trace
                                .lock()
                                .unwrap()
                                .push(format!("undo reader {}", db.value));
                            Ok(())
                        })
                    })?;
                    Ok(())
                }
            })
            .requires(&database()))
        })
        .unwrap();
    registry
        .register("idle", |_| Ok(Plugin::new("idle", |_| async { Ok(()) })))
        .unwrap();
    registry
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_update_preserves_unaffected_instances_and_restarts_only_dependency_consumers() {
    let rt = Runtime::new();
    let trace: Trace = Arc::default();
    let loader = Loader::new(rt.clone(), registry(trace.clone()));
    let initial = config(vec![reader(), idle(), provider(1)]); // No provider-first load order.
    let plan = loader.plan(initial.clone()).await.unwrap();
    assert_eq!(plan.install, ["reader", "idle", "db"]);
    assert!(trace.lock().unwrap().is_empty());
    loader.apply(initial).await.unwrap();
    let first = bounded(loader.settled()).await.unwrap();
    let reversed = config(vec![provider(1), idle(), reader()]);
    let no_op = loader.apply(reversed).await.unwrap();
    assert!(no_op.plan.install.is_empty());
    assert!(no_op.plan.retire.is_empty());
    let unchanged = loader.settled().await.unwrap();
    for id in ["db", "reader", "idle"] {
        assert_eq!(activation(&first, id), activation(&unchanged, id));
    }

    let report = bounded(loader.apply(config(vec![provider(2), idle(), reader()])))
        .await
        .unwrap();
    assert_eq!(report.plan.install, ["db"]);
    assert_eq!(report.plan.retire, ["db"]);
    let next = bounded(loader.settled()).await.unwrap();
    assert_eq!(activation(&first, "idle"), activation(&next, "idle"));
    assert_eq!(
        activation(&first, "reader").fiber,
        activation(&next, "reader").fiber
    );
    assert!(activation(&next, "reader").epoch > activation(&first, "reader").epoch);
    assert_ne!(
        activation(&first, "db").fiber,
        activation(&next, "db").fiber
    );
    assert_eq!(
        &*trace.lock().unwrap(),
        &[
            "open 1",
            "read 1",
            "undo reader 1",
            "close 1",
            "open 2",
            "read 2"
        ]
    );
    bounded(loader.apply(Configuration::default()))
        .await
        .unwrap();
    assert!(loader.snapshot().entries.is_empty());
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn missing_services_disable_and_reenable_keep_desired_entries_and_report_waiting_slots() {
    let rt = Runtime::new();
    let loader = Loader::new(rt.clone(), registry(Arc::default()));
    loader
        .apply(config(vec![reader(), provider(1).enabled(false)]))
        .await
        .unwrap();
    let waiting = loader.settled().await.unwrap();
    let read = waiting
        .entries
        .iter()
        .find(|e| e.id == "reader")
        .unwrap()
        .instance
        .as_ref()
        .unwrap();
    assert_eq!(read.state, Lifecycle::Inactive);
    assert_eq!(read.waiting_for.len(), 1);
    assert!(
        waiting
            .entries
            .iter()
            .find(|e| e.id == "db")
            .unwrap()
            .instance
            .is_none()
    );
    loader
        .apply(config(vec![reader(), provider(1)]))
        .await
        .unwrap();
    let first = loader.settled().await.unwrap();
    loader
        .apply(config(vec![reader(), provider(1).enabled(false)]))
        .await
        .unwrap();
    assert_eq!(
        loader
            .settled()
            .await
            .unwrap()
            .entries
            .iter()
            .find(|e| e.id == "reader")
            .unwrap()
            .instance
            .as_ref()
            .unwrap()
            .state,
        Lifecycle::Inactive
    );
    loader
        .apply(config(vec![reader(), provider(1)]))
        .await
        .unwrap();
    let second = loader.settled().await.unwrap();
    assert_ne!(
        activation(&first, "db").fiber,
        activation(&second, "db").fiber
    );
    assert_eq!(
        activation(&first, "reader").fiber,
        activation(&second, "reader").fiber
    );
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_configuration_and_final_graph_are_rejected_before_live_state_changes() {
    let rt = Runtime::new();
    let trace: Trace = Arc::default();
    let mut registry = registry(trace.clone());
    registry
        .register("cycle", |_| {
            Ok(Plugin::new("cycle", |_| async { Ok(()) })
                .provides(&database())
                .requires(&other()))
        })
        .unwrap();
    registry
        .register("other", |_| {
            Ok(Plugin::new("other", |_| async { Ok(()) })
                .provides(&other())
                .requires(&database()))
        })
        .unwrap();
    registry
        .register("panic", |_| panic!("factory panic"))
        .unwrap();
    let loader = Loader::new(rt.clone(), registry);
    loader
        .apply(config(vec![provider(1), reader()]))
        .await
        .unwrap();
    let first = loader.settled().await.unwrap();
    for invalid in [
        config(vec![provider(2), provider(3)]),
        config(vec![Entry::new("db", "missing", json!(null))]),
        config(vec![Entry::new("db", "database", json!("invalid"))]),
        config(vec![
            provider(2),
            Entry::new("another", "database", json!(3)),
        ]),
        config(vec![
            Entry::new("db", "cycle", json!(null)),
            Entry::new("other", "other", json!(null)),
        ]),
        config(vec![Entry::new("db", "panic", json!(null))]),
        // A later factory fails after an earlier replacement was prepared;
        // removing the reader must also remain unapplied.
        config(vec![provider(2), Entry::new("late", "panic", json!(null))]),
        config(vec![Entry::group("db", vec![provider(2)])]),
    ] {
        assert!(loader.apply(invalid).await.is_err());
        let after = loader.snapshot();
        assert_eq!(after.desired_revision, first.desired_revision);
        assert_eq!(after.configuration, first.configuration);
        assert_eq!(after.applied_revision, first.applied_revision);
        assert_eq!(activation(&after, "db"), activation(&first, "db"));
        assert_eq!(activation(&after, "reader"), activation(&first, "reader"));
        assert_eq!(&*trace.lock().unwrap(), &["open 1", "read 1"]);
    }
    assert!(Configuration::from_json(r#"{"entries":[],"typo":true}"#).is_err());
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn preflight_includes_unmanaged_providers_and_consumers() {
    let rt = Runtime::new();
    let external = rt
        .install(
            Plugin::new("external", |ctx| async move {
                ctx.provide(&other(), Arc::new(()))
            })
            .provides(&other())
            .requires(&database()),
        )
        .unwrap();
    let mut registry = registry(Arc::default());
    registry
        .register("cycle", |_| {
            Ok(Plugin::new("cycle", |_| async { Ok(()) })
                .provides(&database())
                .requires(&other()))
        })
        .unwrap();
    registry
        .register("conflict", |_| {
            Ok(Plugin::new("conflict", |_| async { Ok(()) }).provides(&other()))
        })
        .unwrap();
    let loader = Loader::new(rt.clone(), registry);
    for source in ["cycle", "conflict"] {
        assert!(
            loader
                .apply(config(vec![Entry::new("test", source, json!(null))]))
                .await
                .is_err()
        );
        assert_eq!(loader.snapshot().desired_revision, 0);
    }
    assert_eq!(external.snapshot().state, Lifecycle::Inactive);
    loader.apply(config(vec![provider(1)])).await.unwrap();
    bounded(external.ready()).await.unwrap();
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn groups_inherit_realms_and_reconcile_children_without_restarting_siblings() {
    let rt = Runtime::new();
    let trace: Trace = Arc::default();
    let loader = Loader::new(rt.clone(), registry(trace.clone()));
    let tenant = |value| {
        Entry::group(
            "tenant",
            vec![
                provider(value),
                Entry::group("nested", vec![reader(), idle()]),
            ],
        )
        .isolate(&database(), Realm::Local)
    };
    loader.apply(config(vec![tenant(1)])).await.unwrap();
    let first = loader.settled().await.unwrap();
    let realm = first
        .entries
        .iter()
        .find(|e| e.id == "reader")
        .unwrap()
        .actual
        .as_ref()
        .unwrap()
        .realms[database().contract()];
    assert_ne!(realm, RealmId::ROOT);
    loader.apply(config(vec![tenant(2)])).await.unwrap();
    let updated = loader.settled().await.unwrap();
    for id in ["tenant", "nested", "idle"] {
        assert_eq!(activation(&first, id), activation(&updated, id));
    }
    loader
        .apply(config(vec![tenant(2).enabled(false)]))
        .await
        .unwrap();
    assert!(
        loader
            .settled()
            .await
            .unwrap()
            .entries
            .iter()
            .all(|e| e.instance.is_none() && !e.desired.as_ref().unwrap().enabled)
    );
    loader.apply(config(vec![tenant(2)])).await.unwrap();
    let enabled = loader.settled().await.unwrap();
    assert_eq!(
        enabled
            .entries
            .iter()
            .find(|e| e.id == "reader")
            .unwrap()
            .actual
            .as_ref()
            .unwrap()
            .realms[database().contract()],
        realm
    );
    assert_ne!(
        activation(&updated, "tenant").fiber,
        activation(&enabled, "tenant").fiber
    );
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn group_move_preserves_subtree_and_local_realm() {
    let rt = Runtime::new();
    let loader = Loader::new(rt.clone(), registry(Arc::default()));
    let local =
        || Entry::group("local", vec![provider(1), reader()]).isolate(&database(), Realm::Local);
    loader
        .apply(config(vec![
            Entry::group("left", vec![local()]),
            Entry::group("right", vec![]),
        ]))
        .await
        .unwrap();
    let first = loader.settled().await.unwrap();
    let report = loader
        .apply(config(vec![
            Entry::group("right", vec![local()]),
            Entry::group("left", vec![]),
        ]))
        .await
        .unwrap();
    assert_eq!(report.plan.reparent, ["local"]);
    assert!(report.plan.install.is_empty());
    assert!(report.plan.retire.is_empty());
    assert_eq!(report.ownership.len(), 1);
    let moved = loader.settled().await.unwrap();
    for id in ["left", "right"] {
        assert_eq!(activation(&first, id), activation(&moved, id));
    }
    for id in ["local", "db", "reader"] {
        assert_eq!(activation(&first, id), activation(&moved, id));
        assert_eq!(
            first
                .entries
                .iter()
                .find(|e| e.id == id)
                .unwrap()
                .actual
                .as_ref()
                .unwrap()
                .realms,
            moved
                .entries
                .iter()
                .find(|e| e.id == id)
                .unwrap()
                .actual
                .as_ref()
                .unwrap()
                .realms
        );
    }
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn move_into_new_nested_groups_then_to_root_survives_removing_old_owners() {
    let rt = Runtime::new();
    let trace: Trace = Arc::default();
    let loader = Loader::new(rt.clone(), registry(trace.clone()));
    let local =
        || Entry::group("local", vec![provider(9), reader()]).isolate(&database(), Realm::Local);
    loader
        .apply(config(vec![Entry::group("old", vec![local()])]))
        .await
        .unwrap();
    let first = loader.settled().await.unwrap();
    let report = bounded(loader.apply(config(vec![Entry::group(
        "new",
        vec![Entry::group("nested", vec![local()])],
    )])))
    .await
    .unwrap();
    assert_eq!(report.plan.retire, ["old"]);
    assert_eq!(report.plan.install, ["new", "nested"]);
    assert_eq!(report.plan.reparent, ["local"]);
    let moved = loader.settled().await.unwrap();
    for id in ["local", "db", "reader"] {
        assert_eq!(activation(&first, id), activation(&moved, id));
    }
    let local_state = moved.entries.iter().find(|e| e.id == "local").unwrap();
    assert_eq!(local_state.actual_revision, Some(1));
    assert_eq!(local_state.ownership_revision, Some(2));
    assert_eq!(
        local_state.instance.as_ref().unwrap().parent,
        Some(activation(&moved, "nested"))
    );
    assert_eq!(&*trace.lock().unwrap(), &["open 9", "read 9"]);
    bounded(loader.apply(config(vec![local()]))).await.unwrap();
    let rooted = loader.settled().await.unwrap();
    for id in ["local", "db", "reader"] {
        assert_eq!(activation(&first, id), activation(&rooted, id));
    }
    assert_eq!(
        rooted
            .entries
            .iter()
            .find(|e| e.id == "local")
            .unwrap()
            .instance
            .as_ref()
            .unwrap()
            .parent,
        None
    );
    bounded(loader.apply(config(vec![]))).await.unwrap();
    assert_eq!(
        &*trace.lock().unwrap(),
        &["open 9", "read 9", "undo reader 9", "close 9"]
    );
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn cross_group_move_resolves_new_dependency_after_old_binding_cleanup() {
    let rt = Runtime::new();
    let trace: Trace = Arc::default();
    let loader = Loader::new(rt.clone(), registry(trace.clone()));
    let left = |children| Entry::group("left", children).isolate(&database(), Realm::Local);
    let right = |children| Entry::group("right", children).isolate(&database(), Realm::Local);
    let db2 = || Entry::new("db2", "database", json!(2));
    loader
        .apply(config(vec![
            left(vec![provider(1), reader()]),
            right(vec![db2(), idle()]),
        ]))
        .await
        .unwrap();
    let first = loader.settled().await.unwrap();
    let report = loader
        .apply(config(vec![
            left(vec![provider(1)]),
            right(vec![db2(), reader(), idle()]),
        ]))
        .await
        .unwrap();
    assert_eq!(report.plan.reparent, ["reader"]);
    assert!(report.plan.install.is_empty());
    assert!(report.plan.retire.is_empty());
    let moved = bounded(loader.settled()).await.unwrap();
    let before = activation(&first, "reader");
    let after = activation(&moved, "reader");
    assert_eq!(before.fiber, after.fiber);
    assert_eq!(before.epoch + 1, after.epoch);
    for id in ["left", "right", "db", "db2", "idle"] {
        assert_eq!(activation(&first, id), activation(&moved, id));
    }
    let log = trace.lock().unwrap().clone();
    let cleanup = log.iter().position(|s| s == "undo reader 1").unwrap();
    let restart = log.iter().position(|s| s == "read 2").unwrap();
    assert!(cleanup < restart);
    assert_eq!(
        moved
            .entries
            .iter()
            .find(|e| e.id == "reader")
            .unwrap()
            .instance
            .as_ref()
            .unwrap()
            .parent,
        Some(activation(&moved, "right"))
    );
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn failed_apply_keeps_transferred_ownership_visible_and_retains_failed_cleanup() {
    let rt = Runtime::new();
    let mut registry = registry(Arc::default());
    registry
        .register("failure", |_| {
            Ok(Plugin::new("failed inverse", |ctx| async move {
                ctx.defer("failure", |_| async {
                    Err(Error::Message("retained".into()))
                })?;
                Ok(())
            }))
        })
        .unwrap();
    let loader = Loader::new(rt.clone(), registry);
    loader
        .apply(config(vec![
            Entry::group("old", vec![idle()]),
            Entry::new("bad", "failure", json!(null)),
        ]))
        .await
        .unwrap();
    let first = loader.settled().await.unwrap();
    let next = config(vec![Entry::group("new", vec![idle()])]);
    assert!(bounded(loader.apply(next)).await.is_err());
    let failed = loader.snapshot();
    assert_eq!(failed.phase, Phase::Failed);
    assert_eq!(activation(&first, "idle"), activation(&failed, "idle"));
    let child = failed.entries.iter().find(|e| e.id == "idle").unwrap();
    assert_eq!(
        child.actual.as_ref().unwrap().parent.as_deref(),
        Some("new")
    );
    assert_eq!(
        child.instance.as_ref().unwrap().parent,
        Some(activation(&failed, "new"))
    );
    assert_eq!(child.ownership_revision, Some(2));
    let bad = failed.entries.iter().find(|e| e.id == "bad").unwrap();
    assert!(matches!(
        bad.instance.as_ref().unwrap().outcome,
        Some(Outcome::CleanupFailed(_))
    ));
    assert!(!bad.effects.is_empty());
    assert!(bounded(rt.shutdown()).await.is_err());
}

#[tokio::test]
async fn move_through_new_group_into_replaced_owner_rebuilds_with_that_owner() {
    let rt = Runtime::new();
    let loader = Loader::new(rt.clone(), registry(Arc::default()));
    loader
        .apply(config(vec![
            Entry::group("old", vec![idle()]),
            Entry::new("destination", "idle", json!(null)),
        ]))
        .await
        .unwrap();
    let first = loader.settled().await.unwrap();
    let next = config(vec![
        Entry::group("old", vec![]),
        Entry::group("destination", vec![Entry::group("bridge", vec![idle()])]),
    ]);
    let report = bounded(loader.apply(next)).await.unwrap();
    assert!(report.plan.reparent.is_empty());
    assert_eq!(report.plan.retire, ["destination", "idle"]);
    let moved = loader.settled().await.unwrap();
    assert_ne!(
        activation(&first, "idle").fiber,
        activation(&moved, "idle").fiber
    );
    assert_eq!(activation(&first, "old"), activation(&moved, "old"));
    assert_eq!(
        moved
            .entries
            .iter()
            .find(|e| e.id == "idle")
            .unwrap()
            .instance
            .as_ref()
            .unwrap()
            .parent,
        Some(activation(&moved, "bridge"))
    );
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn shared_realms_cross_groups_and_realm_change_preserves_unrelated_activations() {
    let rt = Runtime::new();
    let trace: Trace = Arc::default();
    let loader = Loader::new(rt.clone(), registry(trace.clone()));
    let left = Entry::group("left", vec![provider(7)])
        .isolate(&database(), Realm::Shared("common".into()));
    let right = |realm| Entry::group("right", vec![reader(), idle()]).isolate(&database(), realm);
    loader
        .apply(config(vec![left.clone(), right(Realm::Local)]))
        .await
        .unwrap();
    let first = loader.settled().await.unwrap();
    assert_eq!(&*trace.lock().unwrap(), &["open 7"]);
    loader
        .apply(config(vec![left, right(Realm::Shared("common".into()))]))
        .await
        .unwrap();
    let shared = loader.settled().await.unwrap();
    assert_eq!(activation(&first, "db"), activation(&shared, "db"));
    assert_eq!(activation(&first, "left"), activation(&shared, "left"));
    assert_eq!(activation(&first, "right"), activation(&shared, "right"));
    assert_eq!(activation(&first, "idle"), activation(&shared, "idle"));
    assert_eq!(
        first
            .entries
            .iter()
            .find(|e| e.id == "reader")
            .unwrap()
            .instance
            .as_ref()
            .unwrap()
            .id,
        shared
            .entries
            .iter()
            .find(|e| e.id == "reader")
            .unwrap()
            .instance
            .as_ref()
            .unwrap()
            .id
    );
    assert!(trace.lock().unwrap().iter().any(|line| line == "read 7"));
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn moving_provider_and_consumer_together_retains_state_and_explains_the_plan() {
    let rt = Runtime::new();
    let trace: Trace = Arc::default();
    let loader = Loader::new(rt.clone(), registry(trace.clone()));
    let target = |realm| {
        config(vec![
            Entry::group("tenant", vec![provider(5), reader(), idle()]).isolate(&database(), realm),
        ])
    };
    loader.apply(target(Realm::Local)).await.unwrap();
    let first = loader.settled().await.unwrap();
    let plan = loader
        .plan(target(Realm::Shared("shared".into())))
        .await
        .unwrap();
    assert!(plan.install.is_empty());
    assert!(plan.retire.is_empty());
    assert_eq!(plan.update_realms, ["tenant", "db", "reader", "idle"]);
    assert_eq!(plan.realm_changes.len(), 4);
    let report = loader
        .apply(target(Realm::Shared("shared".into())))
        .await
        .unwrap();
    assert_eq!(report.preparation.definitions_built, 0);
    assert!(report.realms.withdrawn.is_empty());
    let moved = loader.settled().await.unwrap();
    for id in ["tenant", "db", "reader", "idle"] {
        assert_eq!(activation(&first, id), activation(&moved, id));
        let entry = moved.entries.iter().find(|e| e.id == id).unwrap();
        assert_eq!(entry.actual_revision, Some(first.applied_revision));
        assert_eq!(entry.realm_revision, Some(moved.applied_revision));
    }
    assert_eq!(*trace.lock().unwrap(), vec!["open 5", "read 5"]);
    bounded(rt.shutdown()).await.unwrap();
    assert_eq!(
        *trace.lock().unwrap(),
        vec!["open 5", "read 5", "undo reader 5", "close 5"]
    );
}

#[tokio::test]
async fn changing_provider_selection_reactivates_reader_but_keeps_group_and_metrics() {
    let rt = Runtime::new();
    let trace: Trace = Arc::default();
    let loader = Loader::new(rt.clone(), registry(trace.clone()));
    let target = |name: &str| {
        config(vec![
            Entry::new("blue-db", "database", json!(1))
                .isolate(&database(), Realm::Shared("blue".into())),
            Entry::new("red-db", "database", json!(2))
                .isolate(&database(), Realm::Shared("red".into())),
            Entry::group("tenant", vec![reader(), idle()])
                .isolate(&database(), Realm::Shared(name.into())),
        ])
    };
    loader.apply(target("blue")).await.unwrap();
    let first = loader.settled().await.unwrap();
    let report = loader.apply(target("red")).await.unwrap();
    let next = bounded(loader.settled()).await.unwrap();
    assert!(report.plan.retire.is_empty());
    assert!(report.plan.install.is_empty());
    assert_eq!(report.realms.withdrawn, vec![activation(&first, "reader")]);
    for id in ["blue-db", "red-db", "tenant", "idle"] {
        assert_eq!(activation(&first, id), activation(&next, id));
    }
    assert_eq!(
        activation(&first, "reader").fiber,
        activation(&next, "reader").fiber
    );
    assert_eq!(
        activation(&next, "reader").epoch,
        activation(&first, "reader").epoch + 1
    );
    assert_eq!(
        *trace.lock().unwrap(),
        vec!["open 1", "open 2", "read 1", "undo reader 1", "read 2"]
    );
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn conflicting_realm_plan_preserves_desired_actual_and_live_graph() {
    let rt = Runtime::new();
    let loader = Loader::new(rt.clone(), registry(Arc::default()));
    let target = |realm| {
        config(vec![
            Entry::new("a", "database", json!(1)).isolate(&database(), realm),
            Entry::new("b", "database", json!(2))
                .isolate(&database(), Realm::Shared("occupied".into())),
        ])
    };
    loader.apply(target(Realm::Local)).await.unwrap();
    let before = loader.settled().await.unwrap();
    assert!(
        loader
            .apply(target(Realm::Shared("occupied".into())))
            .await
            .is_err()
    );
    let after = loader.snapshot();
    assert_eq!(before.desired_revision, after.desired_revision);
    assert_eq!(before.applied_revision, after.applied_revision);
    assert_eq!(before.configuration, after.configuration);
    for id in ["a", "b"] {
        assert_eq!(activation(&before, id), activation(&after, id));
    }
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn cleanup_failure_keeps_residual_actual_state_and_prevents_replacement_or_retry() {
    let rt = Runtime::new();
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut registry = registry(Arc::default());
    registry
        .register("broken", {
            let attempts = attempts.clone();
            move |_| {
                let attempts = attempts.clone();
                Ok(Plugin::new("broken", move |ctx| {
                    let attempts = attempts.clone();
                    async move {
                        ctx.defer("broken inverse", move |_| async move {
                            attempts.fetch_add(1, Ordering::SeqCst);
                            Err(Error::Message("cleanup failed".into()))
                        })?;
                        Ok(())
                    }
                })
                .requires(&database()))
            }
        })
        .unwrap();
    let loader = Loader::new(rt.clone(), registry);
    let target = |value| {
        config(vec![
            provider(value),
            Entry::new("broken", "broken", json!(null)),
        ])
    };
    loader.apply(target(1)).await.unwrap();
    let first = loader.settled().await.unwrap();
    assert!(bounded(loader.apply(target(2))).await.is_err());
    let failed = loader.snapshot();
    assert_eq!(failed.phase, Phase::Failed);
    assert_eq!(failed.applied_revision, first.applied_revision);
    assert!(failed.desired_revision > failed.applied_revision);
    let db = failed.entries.iter().find(|e| e.id == "db").unwrap();
    assert_eq!(db.desired.as_ref().unwrap().config, json!(2));
    assert_eq!(db.actual.as_ref().unwrap().config, json!(1));
    assert_eq!(db.instance.as_ref().unwrap().state, Lifecycle::Unloading);
    let broken = failed.entries.iter().find(|e| e.id == "broken").unwrap();
    assert!(matches!(
        broken.instance.as_ref().unwrap().outcome,
        Some(Outcome::CleanupFailed(_))
    ));
    assert!(!broken.effects.is_empty());
    assert!(bounded(loader.apply(target(2))).await.is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert!(bounded(rt.shutdown()).await.is_err());
}

#[tokio::test]
async fn initialization_failure_is_visible_and_explicit_resubmission_retries_only_failed_entry() {
    let rt = Runtime::new();
    let fail = Arc::new(AtomicBool::new(true));
    let mut registry = registry(Arc::default());
    registry
        .register("flaky", {
            let fail = fail.clone();
            move |_| {
                let fail = fail.clone();
                Ok(Plugin::new("flaky", move |_| {
                    let fail = fail.clone();
                    async move {
                        if fail.load(Ordering::SeqCst) {
                            Err(Error::Message("initialization failed".into()))
                        } else {
                            Ok(())
                        }
                    }
                }))
            }
        })
        .unwrap();
    let loader = Loader::new(rt.clone(), registry);
    let target = config(vec![idle(), Entry::new("flaky", "flaky", json!(null))]);
    loader.apply(target.clone()).await.unwrap();
    assert!(matches!(
        bounded(loader.settled()).await,
        Err(LoaderError::Apply(_))
    ));
    let first = loader.snapshot();
    fail.store(false, Ordering::SeqCst);
    let retry = loader.apply(target).await.unwrap();
    assert_eq!(retry.plan.install, ["flaky"]);
    let fixed = loader.settled().await.unwrap();
    assert_eq!(activation(&first, "idle"), activation(&fixed, "idle"));
    assert_eq!(
        fixed
            .entries
            .iter()
            .find(|e| e.id == "flaky")
            .unwrap()
            .instance
            .as_ref()
            .unwrap()
            .state,
        Lifecycle::Active
    );
    rt.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conflicting_external_install_after_preflight_is_reported_without_overwriting_provider() {
    let rt = Runtime::new();
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let mut registry = Registry::new();
    registry
        .register("held", {
            let entered = entered.clone();
            let release = release.clone();
            move |_| {
                let entered = entered.clone();
                let release = release.clone();
                Ok(Plugin::new("held", move |ctx| {
                    let entered = entered.clone();
                    let release = release.clone();
                    async move {
                        ctx.defer("held", move |_| async move {
                            entered.add_permits(1);
                            release.acquire().await.unwrap().forget();
                            Ok(())
                        })?;
                        Ok(())
                    }
                }))
            }
        })
        .unwrap();
    registry
        .register("other", |_| {
            Ok(Plugin::new(
                "other",
                |ctx| async move { ctx.provide(&other(), Arc::new(())) },
            )
            .provides(&other()))
        })
        .unwrap();
    let loader = Loader::new(rt.clone(), registry);
    loader
        .apply(config(vec![Entry::new("old", "held", json!(null))]))
        .await
        .unwrap();
    loader.settled().await.unwrap();
    let updating = tokio::spawn({
        let loader = loader.clone();
        async move {
            loader
                .apply(config(vec![Entry::new("new", "other", json!(null))]))
                .await
        }
    });
    bounded(entered.acquire()).await.unwrap().forget();
    let foreign = rt
        .install(
            Plugin::new("foreign", |ctx| async move {
                ctx.provide(&other(), Arc::new(()))
            })
            .provides(&other()),
        )
        .unwrap();
    bounded(foreign.ready()).await.unwrap();
    release.add_permits(1);
    assert!(matches!(
        bounded(updating).await.unwrap(),
        Err(LoaderError::Apply(_))
    ));
    let failed = loader.snapshot();
    assert_eq!(failed.phase, Phase::Failed);
    assert_eq!(failed.applied_revision, 1);
    assert_eq!(failed.desired_revision, 2);
    assert_eq!(failed.entries.len(), 1);
    assert_eq!(failed.entries[0].id, "new");
    assert!(failed.entries[0].actual.is_none());
    assert_eq!(foreign.snapshot().state, Lifecycle::Active);
    loader.apply(Configuration::default()).await.unwrap();
    assert_eq!(foreign.snapshot().state, Lifecycle::Active);
    rt.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_can_withdraw_initialization_and_collect_its_late_inverse() {
    let rt = Runtime::new();
    let entered = Arc::new(Semaphore::new(0));
    let cleaned = Arc::new(AtomicBool::new(false));
    let mut registry = Registry::new();
    registry
        .register("pending", {
            let entered = entered.clone();
            let cleaned = cleaned.clone();
            move |_| {
                let entered = entered.clone();
                let cleaned = cleaned.clone();
                Ok(Plugin::new("pending", move |ctx| {
                    let entered = entered.clone();
                    let cleaned = cleaned.clone();
                    async move {
                        entered.add_permits(1);
                        ctx.cancelled().await;
                        ctx.defer("late inverse", move |_| async move {
                            cleaned.store(true, Ordering::SeqCst);
                            Ok(())
                        })?;
                        Ok(())
                    }
                }))
            }
        })
        .unwrap();
    let loader = Loader::new(rt.clone(), registry);
    bounded(loader.apply(config(vec![Entry::new("pending", "pending", json!(null))])))
        .await
        .unwrap();
    bounded(entered.acquire()).await.unwrap().forget();
    let waiting = tokio::spawn({
        let loader = loader.clone();
        async move { loader.settled().await }
    });
    bounded(loader.apply(Configuration::default()))
        .await
        .unwrap();
    assert!(bounded(waiting).await.unwrap().unwrap().entries.is_empty());
    assert!(cleaned.load(Ordering::SeqCst));
    rt.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_update_wait_does_not_cancel_recovery_and_queued_updates_run_in_order() {
    let rt = Runtime::new();
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let trace: Trace = Arc::default();
    let mut registry = Registry::new();
    registry
        .register("worker", {
            let entered = entered.clone();
            let release = release.clone();
            let trace = trace.clone();
            move |value| {
                let version = value.as_u64().unwrap();
                let entered = entered.clone();
                let release = release.clone();
                let trace = trace.clone();
                Ok(Plugin::new("worker", move |ctx| {
                    let entered = entered.clone();
                    let release = release.clone();
                    let trace = trace.clone();
                    async move {
                        trace.lock().unwrap().push(format!("start {version}"));
                        ctx.defer("worker", move |_| async move {
                            if version == 1 {
                                entered.add_permits(1);
                                release.acquire().await.unwrap().forget();
                            }
                            trace.lock().unwrap().push(format!("stop {version}"));
                            Ok(())
                        })?;
                        Ok(())
                    }
                }))
            }
        })
        .unwrap();
    let loader = Loader::new(rt.clone(), registry);
    let target = |v| config(vec![Entry::new("worker", "worker", json!(v))]);
    loader.apply(target(1)).await.unwrap();
    loader.settled().await.unwrap();
    let updating = tokio::spawn({
        let loader = loader.clone();
        let next = target(2);
        async move { loader.apply(next).await }
    });
    bounded(entered.acquire()).await.unwrap().forget();
    updating.abort();
    let _ = updating.await;
    let next = tokio::spawn({
        let loader = loader.clone();
        let next = target(3);
        async move { loader.apply(next).await }
    });
    assert_eq!(loader.snapshot().phase, Phase::Applying);
    assert_eq!(loader.snapshot().desired_revision, 2);
    release.add_permits(1);
    assert_eq!(bounded(next).await.unwrap().unwrap().revision, 3);
    bounded(loader.settled()).await.unwrap();
    // Version 2 may be withdrawn before its initializer runs, but it still must
    // finish and recover before version 3 starts.
    assert_eq!(
        &*trace.lock().unwrap(),
        &["start 1", "stop 1", "start 2", "stop 2", "start 3"]
    );
    bounded(rt.shutdown()).await.unwrap();
}
