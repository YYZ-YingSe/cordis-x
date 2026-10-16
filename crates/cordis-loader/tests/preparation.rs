use cordis_loader::{ArtifactRef, Configuration, Entry, Loader, Registry, json};
use cordis_runtime::{Plugin, Runtime, Service};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn config(entries: Vec<Entry>) -> Configuration {
    Configuration { entries }
}
fn count(n: &AtomicUsize) -> usize {
    n.load(Ordering::SeqCst)
}

#[tokio::test]
async fn retained_definitions_skip_construction_but_changed_and_disabled_configs_are_validated() {
    let rt = Runtime::new();
    let builds = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register("idle", {
            let builds = builds.clone();
            move |value| {
                builds.fetch_add(1, Ordering::SeqCst);
                value
                    .as_u64()
                    .ok_or_else(|| cordis_runtime::Error::Message("expected u64".into()))?;
                Ok(Plugin::new("idle", |_| async { Ok(()) }))
            }
        })
        .unwrap();
    let service = Service::<()>::new("settings", "1");
    registry
        .register_intercept(&service, |layers| Ok(layers.to_vec()))
        .unwrap();
    let loader = Loader::new(rt.clone(), registry);
    let original = Entry::new("a", "idle", json!(1));
    loader.apply(config(vec![original.clone()])).await.unwrap();
    let before = loader.settled().await.unwrap();
    loader.plan(config(vec![original.clone()])).await.unwrap();
    let report = loader
        .apply(config(vec![original.clone().intercept(&service, json!(2))]))
        .await
        .unwrap();
    assert_eq!(count(&builds), 1);
    assert_eq!(report.preparation.definitions_built, 0);
    assert_eq!(report.preparation.definitions_retained, 1);
    assert_eq!(report.plan.update_intercepts, ["a"]);
    assert_eq!(
        before.entries[0].instance.as_ref().unwrap().activation,
        report.snapshot.entries[0]
            .instance
            .as_ref()
            .unwrap()
            .activation
    );
    let revision = report.revision;
    assert!(
        loader
            .apply(config(vec![
                Entry::new("bad", "idle", json!("invalid")).enabled(false)
            ]))
            .await
            .is_err()
    );
    assert_eq!(loader.snapshot().desired_revision, revision);
    assert_eq!(count(&builds), 2);
    loader
        .apply(config(vec![Entry::new("a", "idle", json!(2))]))
        .await
        .unwrap();
    assert_eq!(count(&builds), 3);
    let after = loader.settled().await.unwrap();
    assert_ne!(
        before.entries[0].instance.as_ref().unwrap().activation,
        after.entries[0].instance.as_ref().unwrap().activation
    );
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn retaining_code_rechecks_bytes_and_rejects_edits_before_retiring_other_entries() {
    let rt = Runtime::new();
    let path = std::env::temp_dir().join(format!("cordis-retained-{}.bin", std::process::id()));
    std::fs::write(&path, b"first").unwrap();
    let artifact = ArtifactRef::from_file(&path, 32).unwrap();
    let builds = Arc::new(AtomicUsize::new(0));
    let retired = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_file("code", 32, {
            let builds = builds.clone();
            move |artifact, _| {
                artifact
                    .read_file(32)
                    .map_err(|e| cordis_runtime::Error::Message(e.to_string()))?;
                builds.fetch_add(1, Ordering::SeqCst);
                Ok(Plugin::new("code", |_| async { Ok(()) }))
            }
        })
        .unwrap();
    registry
        .register("old", {
            let retired = retired.clone();
            move |_| {
                let retired = retired.clone();
                Ok(Plugin::new("old", move |ctx| {
                    let retired = retired.clone();
                    async move {
                        ctx.defer("old", move |_| async move {
                            retired.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        })?;
                        Ok(())
                    }
                }))
            }
        })
        .unwrap();
    let loader = Loader::new(rt.clone(), registry);
    let current = Entry::new("new", "code", json!(null)).artifact(artifact);
    loader
        .apply(config(vec![
            Entry::new("old", "old", json!(null)),
            current.clone(),
        ]))
        .await
        .unwrap();
    let before = loader.settled().await.unwrap();
    loader.plan(config(vec![current.clone()])).await.unwrap();
    std::fs::write(&path, b"other").unwrap(); // same path and length, different bytes
    assert!(
        loader
            .apply(config(vec![current.clone()]))
            .await
            .unwrap_err()
            .to_string()
            .contains("SHA-256 mismatch")
    );
    assert_eq!(loader.snapshot().desired_revision, before.desired_revision);
    assert_eq!(count(&retired), 0);
    assert_eq!(count(&builds), 1);
    std::fs::write(&path, b"first").unwrap();
    let report = loader.apply(config(vec![current])).await.unwrap();
    assert_eq!(report.plan.retire, ["old"]);
    assert_eq!(report.plan.unchanged, ["new"]);
    assert_eq!(report.preparation.definitions_built, 0);
    assert_eq!(count(&builds), 1);
    assert_eq!(count(&retired), 1);
    rt.shutdown().await.unwrap();
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn verifier_panics_are_reported_without_mutation() {
    let rt = Runtime::new();
    let builds = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_artifact("code", |_| panic!("verification failure"), {
            let builds = builds.clone();
            move |_, _| {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok(Plugin::new("code", |_| async { Ok(()) }))
            }
        })
        .unwrap();
    let loader = Loader::new(rt.clone(), registry);
    let desired = config(vec![
        Entry::new("a", "code", json!(null)).artifact(ArtifactRef::from_bytes("memory:code", b"a")),
    ]);
    loader.apply(desired.clone()).await.unwrap();
    loader.settled().await.unwrap();
    assert!(
        loader
            .apply(desired)
            .await
            .unwrap_err()
            .to_string()
            .contains("verifier panicked")
    );
    assert_eq!(loader.snapshot().desired_revision, 1);
    assert_eq!(count(&builds), 1);
    rt.shutdown().await.unwrap();
}
