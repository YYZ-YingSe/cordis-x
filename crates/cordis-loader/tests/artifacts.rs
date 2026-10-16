use cordis_loader::{ArtifactRef, Configuration, Entry, Loader, Registry, json};
use cordis_runtime::{Plugin, Runtime};
use std::{
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

#[test]
fn identities_use_sha256_and_reject_ambiguous_references() {
    let artifact = ArtifactRef::from_bytes("memory:example", b"abc");
    assert_eq!(
        artifact.sha256,
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    artifact.verify(b"abc").unwrap();
    assert!(artifact.verify(b"abcd").is_err());
    for invalid in [
        ArtifactRef {
            location: "".into(),
            ..artifact.clone()
        },
        ArtifactRef {
            sha256: "f".repeat(63),
            ..artifact.clone()
        },
        ArtifactRef {
            sha256: "A".repeat(64),
            ..artifact.clone()
        },
    ] {
        assert!(invalid.validate().is_err());
    }
}

#[test]
fn file_reads_are_bounded_and_verify_the_bytes_actually_read() {
    let path = std::env::temp_dir().join(format!("cordis-artifact-{}.bin", std::process::id()));
    std::fs::write(&path, b"abc").unwrap();
    let artifact = ArtifactRef::from_file(&path, 3).unwrap();
    assert_eq!(artifact.read_file(3).unwrap(), b"abc");
    assert!(artifact.read_file(2).is_err());
    assert!(ArtifactRef::from_file(&path, 0).is_err());
    std::fs::write(&path, b"xyz").unwrap();
    assert!(
        artifact
            .read_file(3)
            .unwrap_err()
            .to_string()
            .contains("SHA-256 mismatch")
    );
    std::fs::remove_file(&path).unwrap();
    assert!(artifact.read_file(3).is_err());
}

#[tokio::test]
async fn artifact_sources_are_explicit_and_checked_even_when_disabled() {
    let runtime = Runtime::new();
    let mut registry = Registry::new();
    registry
        .register("static", |_| {
            Ok(Plugin::new("static", |_| async { Ok(()) }))
        })
        .unwrap();
    registry
        .register_artifact(
            "code",
            |artifact| {
                artifact
                    .verify(b"abc")
                    .map_err(|e| cordis_runtime::Error::Message(e.to_string()))
            },
            |artifact, _| {
                artifact
                    .verify(b"abc")
                    .map_err(|e| cordis_runtime::Error::Message(e.to_string()))?;
                Ok(Plugin::new("artifact", |_| async { Ok(()) }))
            },
        )
        .unwrap();
    let loader = Loader::new(runtime.clone(), registry);
    let reference = ArtifactRef::from_bytes("memory:example", b"abc");
    for entry in [
        Entry::new("a", "code", json!(null)),
        Entry::new("a", "static", json!(null)).artifact(reference.clone()),
        Entry::group("a", vec![]).artifact(reference.clone()),
        Entry::new("a", "code", json!(null))
            .artifact(ArtifactRef {
                sha256: "bad".into(),
                ..reference.clone()
            })
            .enabled(false),
    ] {
        assert!(
            loader
                .apply(Configuration {
                    entries: vec![entry]
                })
                .await
                .is_err()
        );
        assert_eq!(loader.snapshot().desired_revision, 0);
        assert!(runtime.snapshots().is_empty());
    }
    let config = Configuration {
        entries: vec![Entry::new("a", "code", json!(null)).artifact(reference.clone())],
    };
    let plan = loader.plan(config.clone()).await.unwrap();
    assert_eq!(plan.artifacts["a"], reference);
    loader.apply(config.clone()).await.unwrap();
    let first = loader.settled().await.unwrap();
    let next = loader.apply(config).await.unwrap();
    assert_eq!(next.plan.unchanged, ["a"]);
    assert_eq!(
        first.entries[0].actual.as_ref().unwrap().artifact,
        Some(reference)
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn preparation_is_off_executor_and_survives_cancelled_wait() {
    let runtime = Runtime::new();
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let mut registry = Registry::new();
    registry
        .register_artifact(
            "code",
            |artifact| {
                artifact
                    .verify(b"abc")
                    .map_err(|e| cordis_runtime::Error::Message(e.to_string()))
            },
            {
                let entered = entered.clone();
                let release = release.clone();
                move |_, _| {
                    entered.add_permits(1);
                    let (lock, signal) = &*release;
                    let (guard, timeout) = signal
                        .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |ready| {
                            !*ready
                        })
                        .unwrap();
                    assert!(
                        *guard && !timeout.timed_out(),
                        "preparation blocked the Tokio executor"
                    );
                    Ok(Plugin::new("prepared", |_| async { Ok(()) }))
                }
            },
        )
        .unwrap();
    let loader = Loader::new(runtime.clone(), registry);
    let worker = loader.clone();
    let pending = tokio::spawn(async move {
        worker
            .apply(Configuration {
                entries: vec![
                    Entry::new("a", "code", json!(null))
                        .artifact(ArtifactRef::from_bytes("memory:code", b"abc")),
                ],
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert_eq!(loader.snapshot().desired_revision, 0);
    pending.abort();
    let _ = pending.await;
    *release.0.lock().unwrap() = true;
    release.1.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        while loader.snapshot().applied_revision != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    loader.settled().await.unwrap();
    runtime.shutdown().await.unwrap();
}
