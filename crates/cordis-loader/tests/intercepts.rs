use cordis_loader::{Configuration, Entry, Loader, Registry, Value, json};
use cordis_runtime::*;
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

fn service() -> Service<()> {
    Service::new("test.configured", "1")
}
fn config(value: i64) -> Configuration {
    Configuration {
        entries: vec![
            Entry::new("provider", "provider", Value::Null),
            Entry::group(
                "group",
                vec![
                    Entry::new("client", "client", Value::Null)
                        .intercept(&service(), json!({"child":true})),
                ],
            )
            .intercept(&service(), json!({"timeout":value})),
            Entry::new("unrelated", "idle", Value::Null),
        ],
    }
}
fn metadata(ctx: &Context) -> Result<(Option<u64>, Value)> {
    let snapshot = ctx.snapshot();
    let layers = snapshot.metadata::<_, Value>(&service())?;
    Ok((
        snapshot.metadata_revision(),
        layers
            .first()
            .map(|v| v.as_ref().clone())
            .unwrap_or(Value::Null),
    ))
}
fn registry(saved: Arc<Mutex<Option<ServiceHandle<()>>>>) -> Registry {
    let mut r = Registry::new();
    r.register("provider", |_| {
        Ok(Plugin::new("provider", |ctx| async move {
            ctx.provide(&service(), Arc::new(()))
        })
        .provides(&service()))
    })
    .unwrap();
    r.register("idle", |_| Ok(Plugin::new("idle", |_| async { Ok(()) })))
        .unwrap();
    r.register("client", move |_| {
        let saved = saved.clone();
        Ok(Plugin::new("client", move |ctx| {
            let saved = saved.clone();
            async move {
                *saved.lock().unwrap() = Some(ctx.dependency(&service())?);
                Ok(())
            }
        })
        .requires(&service()))
    })
    .unwrap();
    r.register_portable_intercept(&service(), |layers| {
        let mut value = json!({});
        for layer in layers {
            let map = layer
                .as_object()
                .ok_or_else(|| Error::Message("expected object".into()))?;
            for (key, item) in map {
                value[key] = item.clone();
            }
        }
        if value["timeout"].as_i64().is_none_or(|n| n <= 0) {
            return Err(Error::Message("timeout must be positive".into()));
        }
        Ok(value)
    })
    .unwrap();
    r
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inherited_updates_preserve_fibers_and_pin_inflight_nested_calls_and_resources() {
    let rt = Runtime::new();
    let saved = Arc::new(Mutex::new(None));
    let loader = Loader::new(rt.clone(), registry(saved.clone()));
    loader.apply(config(1000)).await.unwrap();
    let before = loader.settled().await.unwrap();
    let handle = saved.lock().unwrap().clone().unwrap();
    let started = Arc::new(Semaphore::new(0));
    let proceed = Arc::new(Semaphore::new(0));
    let cleanups = Arc::new(Mutex::new(Vec::new()));
    let pending = tokio::spawn({
        let handle = handle.clone();
        let started = started.clone();
        let proceed = proceed.clone();
        let cleanups = cleanups.clone();
        async move {
            handle
                .call(move |_, ctx| {
                    Box::pin(async move {
                        let first = metadata(&ctx)?;
                        started.add_permits(1);
                        proceed.acquire().await.unwrap().forget();
                        assert_eq!(metadata(&ctx)?, first);
                        // Nested operations must retain the admitted snapshot too.
                        let nested = ctx.operation(|ctx| async move { metadata(&ctx) }).await?;
                        assert_eq!(nested, first);
                        ctx.acquire("pinned", async { Ok(()) }, move |_, cleanup| async move {
                            cleanups.lock().unwrap().push(metadata(&cleanup)?);
                            Ok(())
                        })
                        .await
                    })
                })
                .await
                .unwrap()
        }
    });
    started.acquire().await.unwrap().forget();
    let update = loader.apply(config(3000)).await.unwrap();
    assert!(update.plan.install.is_empty());
    assert!(update.plan.retire.is_empty());
    assert_eq!(update.plan.update_intercepts, ["group", "client"]);
    let after = loader.settled().await.unwrap();
    for old in &before.entries {
        let new = after.entries.iter().find(|e| e.id == old.id).unwrap();
        assert_eq!(
            old.instance.as_ref().unwrap().activation,
            new.instance.as_ref().unwrap().activation
        );
        assert_eq!(old.actual_revision, new.actual_revision);
    }
    assert_eq!(
        handle.with(|_, ctx| metadata(&ctx)).unwrap(),
        (Some(2), json!({"timeout":3000,"child":true}))
    );
    proceed.add_permits(1);
    let resource = pending.await.unwrap();
    assert_eq!(
        resource
            .call(|_, ctx| Box::pin(async move { metadata(&ctx) }))
            .await
            .unwrap()
            .0,
        Some(1)
    );
    resource.dispose().await.unwrap();
    assert_eq!(
        *cleanups.lock().unwrap(),
        [(Some(1), json!({"timeout":1000,"child":true}))]
    );
    // Remove every inherited layer: no replacement, no old value leakage.
    let mut empty = config(3000);
    empty.entries[1].intercept.clear();
    empty.entries[1].children[0].intercept.clear();
    loader.apply(empty).await.unwrap();
    assert_eq!(
        handle.with(|_, ctx| metadata(&ctx)).unwrap(),
        (Some(3), Value::Null)
    );
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_unknown_duplicate_and_panicking_config_leave_running_revision_intact() {
    let rt = Runtime::new();
    let saved = Arc::new(Mutex::new(None));
    let mut r = registry(saved.clone());
    let panic_key = Service::<()>::new("panic", "1");
    r.register_intercept(&panic_key, |_| -> Result<()> {
        panic!("invalid validator")
    })
    .unwrap();
    let loader = Loader::new(rt.clone(), r);
    loader.apply(config(1000)).await.unwrap();
    loader.settled().await.unwrap();
    let mut invalid = vec![config(-1)];
    let mut unknown = config(1);
    unknown.entries[1].intercept[0].revision = "2".into();
    invalid.push(unknown);
    let mut duplicate = config(1);
    let repeated = duplicate.entries[1].intercept[0].clone();
    duplicate.entries[1].intercept.push(repeated);
    invalid.push(duplicate);
    let mut panic = config(1);
    panic.entries[1] = panic.entries[1].clone().intercept(&panic_key, Value::Null);
    invalid.push(panic);
    let mut oversized = config(1);
    oversized.entries[1].children[0].intercept[0].config["text"] = json!("x".repeat(1024 * 1024));
    invalid.push(oversized);
    for bad in invalid {
        assert!(loader.plan(bad.clone()).await.is_err());
        assert!(loader.apply(bad).await.is_err());
        assert_eq!(loader.snapshot().desired_revision, 1);
    }
    let handle = saved.lock().unwrap().clone().unwrap();
    assert_eq!(handle.with(|_, ctx| metadata(&ctx)).unwrap().0, Some(1));
    // A business-config edit still follows ordinary recovery and replacement.
    let mut replacement = config(2000);
    replacement.entries[1].children[0].config = json!(true);
    let report = loader.apply(replacement).await.unwrap();
    assert_eq!(report.plan.retire, ["client"]);
    assert_eq!(report.plan.install, ["client"]);
    loader.settled().await.unwrap();
    assert!(handle.with(|_, _| Ok(())).is_err());
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_recovery_does_not_publish_pending_intercepts() {
    let rt = Runtime::new();
    let saved = Arc::new(Mutex::new(None));
    let mut r = registry(saved.clone());
    r.register("fails", |_| {
        Ok(Plugin::new("fails", |ctx| async move {
            ctx.defer("failure", |_| async {
                Err(Error::Message("unrecovered".into()))
            })?;
            Ok(())
        }))
    })
    .unwrap();
    let loader = Loader::new(rt.clone(), r);
    let mut first = config(1000);
    first
        .entries
        .push(Entry::new("fails", "fails", Value::Null));
    loader.apply(first).await.unwrap();
    loader.settled().await.unwrap();
    assert!(loader.apply(config(3000)).await.is_err());
    let handle = saved.lock().unwrap().clone().unwrap();
    assert_eq!(handle.with(|_, ctx| metadata(&ctx)).unwrap().0, Some(1));
    let snapshot = loader.snapshot();
    let client = snapshot.entries.iter().find(|e| e.id == "client").unwrap();
    assert_eq!(client.intercept_revision, Some(1));
    assert_ne!(client.actual, client.desired);
    assert!(rt.shutdown().await.is_err());
}

#[test]
fn publication_is_atomic_and_derived_layers_remain_ordered() {
    let a = Service::<()>::new("a", "1");
    let b = Service::<()>::new("b", "1");
    let make = |revision| {
        let mut c = InterceptConfig::new(revision);
        c.set(&a, revision);
        c.set(&b, revision);
        c
    };
    let source = Intercepts::new(make(0));
    let view = View::new()
        .with_intercepts(source.clone())
        .intercept(&a, 999_u64);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            for revision in 1..1000 {
                source.replace(make(revision));
            }
        });
        for _ in 0..1000 {
            let snapshot = view.snapshot();
            let revision = snapshot.metadata_revision().unwrap();
            let layers = snapshot.metadata::<_, u64>(&a).unwrap();
            assert_eq!(*layers[0], revision);
            assert_eq!(*layers[1], 999);
            assert_eq!(*snapshot.metadata::<_, u64>(&b).unwrap()[0], revision);
        }
    });
}
