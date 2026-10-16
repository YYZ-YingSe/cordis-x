use cordis_sdk::*;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Semaphore;

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("task did not settle")
}

#[tokio::test]
async fn cancelling_join_wait_keeps_task_authority_until_exit_and_late_cleanup() {
    let rt = Runtime::new();
    let saved = Arc::new(Mutex::new(None));
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let trace = Arc::new(Mutex::new(Vec::new()));
    let owner = rt
        .install(Plugin::new("owner", {
            let (saved, entered, release, trace) = (
                saved.clone(),
                entered.clone(),
                release.clone(),
                trace.clone(),
            );
            move |ctx| {
                let (saved, entered, release, trace) = (
                    saved.clone(),
                    entered.clone(),
                    release.clone(),
                    trace.clone(),
                );
                async move {
                    let log = trace.clone();
                    ctx.defer("connection", move |_| async move {
                        log.lock().unwrap().push("closed");
                        Ok(())
                    })?;
                    let handle = ctx.spawn("worker", move |worker| async move {
                        worker.cancelled().await;
                        entered.add_permits(1);
                        release.acquire().await.unwrap().forget();
                        let log = trace.clone();
                        worker.defer("late", move |_| async move {
                            log.lock().unwrap().push("late cleanup");
                            Ok(())
                        })?;
                        trace.lock().unwrap().push("exit");
                        Ok(())
                    })?;
                    *saved.lock().unwrap() = Some(handle);
                    Ok(())
                }
            }
        }))
        .unwrap();
    bounded(owner.ready()).await.unwrap();
    let task = saved.lock().unwrap().take().unwrap();
    let join = tokio::spawn({
        let task = task.clone();
        async move { task.join().await }
    });
    join.abort();
    let _ = join.await;
    let dispose = tokio::spawn(async move { owner.dispose().await });
    bounded(entered.acquire()).await.unwrap().forget();
    assert!(!dispose.is_finished());
    assert!(trace.lock().unwrap().is_empty());
    release.add_permits(1);
    bounded(dispose).await.unwrap().unwrap();
    for _ in 0..2 {
        bounded(task.join()).await.unwrap();
    }
    assert_eq!(*trace.lock().unwrap(), ["exit", "late cleanup", "closed"]);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test]
async fn failed_task_join_is_cached_and_retains_earlier_effects() {
    let rt = Runtime::new();
    let saved = Arc::new(Mutex::new(None));
    let cleaned = Arc::new(Mutex::new(false));
    let owner = rt
        .install(Plugin::new("owner", {
            let (saved, cleaned) = (saved.clone(), cleaned.clone());
            move |ctx| {
                let (saved, cleaned) = (saved.clone(), cleaned.clone());
                async move {
                    ctx.defer("earlier", move |_| async move {
                        *cleaned.lock().unwrap() = true;
                        Ok(())
                    })?;
                    *saved.lock().unwrap() = Some(ctx.spawn("worker", |_| async {
                        Err(Error::Message("task failed".into()))
                    })?);
                    Ok(())
                }
            }
        }))
        .unwrap();
    bounded(owner.ready()).await.unwrap();
    let task = saved.lock().unwrap().take().unwrap();
    for _ in 0..2 {
        assert_eq!(
            bounded(task.join()).await.unwrap_err(),
            Error::Message("task failed".into())
        );
    }
    assert!(bounded(owner.dispose()).await.is_err());
    assert!(!*cleaned.lock().unwrap());
    assert!(bounded(rt.shutdown()).await.is_err());
}
