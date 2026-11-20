use cordis_runtime::{Plugin, Runtime, Service};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};

struct Probe {
    count: usize,
    entered: AtomicUsize,
    finished: AtomicUsize,
    all_entered: Notify,
    all_finished: Notify,
    release: Semaphore,
}

impl Probe {
    async fn reply(&self, id: usize) -> cordis_runtime::Result<usize> {
        if self.entered.fetch_add(1, Ordering::AcqRel) + 1 == self.count {
            self.all_entered.notify_one();
        }
        self.release.acquire().await.unwrap().forget();
        Ok(id)
    }
}

// Count inside one shared service, after the caller and provider have both
// admitted the request. Every task must receive its own reply before shutdown.
async fn concurrent_tasks(count: usize) {
    let rt = Runtime::new();
    let probe = Arc::new(Probe {
        count,
        entered: AtomicUsize::new(0),
        finished: AtomicUsize::new(0),
        all_entered: Notify::new(),
        all_finished: Notify::new(),
        release: Semaphore::new(0),
    });
    let service = Service::<Probe>::new("test.concurrent-service", "1");
    let provide = service.clone();
    let fixture = probe.clone();
    rt.install(
        Plugin::new("shared-provider", move |ctx| {
            let (provide, fixture) = (provide.clone(), fixture.clone());
            async move { ctx.provide(&provide, fixture) }
        })
        .provides(&service),
    )
    .unwrap()
    .ready()
    .await
    .unwrap();

    let binding = service.clone();
    let fixture = probe.clone();
    let owner = rt
        .install(
            Plugin::new("task-owner", move |ctx| {
                let (binding, fixture) = (binding.clone(), fixture.clone());
                async move {
                    for id in 0..count {
                        let (binding, fixture) = (binding.clone(), fixture.clone());
                        ctx.spawn("service request", move |ctx| async move {
                            let reply = ctx
                                .dependency(&binding)?
                                .call(move |provider, _| Box::pin(provider.reply(id)))
                                .await?;
                            assert_eq!(reply, id);
                            if fixture.finished.fetch_add(1, Ordering::AcqRel) + 1 == count {
                                fixture.all_finished.notify_one();
                            }
                            Ok(())
                        })?;
                    }
                    Ok(())
                }
            })
            .requires(&service),
        )
        .unwrap();
    owner.ready().await.unwrap();
    tokio::time::timeout(Duration::from_secs(180), probe.all_entered.notified())
        .await
        .unwrap();
    assert_eq!(probe.entered.load(Ordering::Acquire), count);
    assert_eq!(probe.finished.load(Ordering::Acquire), 0);
    eprintln!("shared service barrier: entered={count} finished=0");
    probe.release.add_permits(count);
    tokio::time::timeout(Duration::from_secs(180), probe.all_finished.notified())
        .await
        .unwrap();
    rt.shutdown().await.unwrap();
    assert_eq!(probe.finished.load(Ordering::Acquire), count);
    eprintln!("shutdown: matched_replies={count}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_task_lifetimes() {
    concurrent_tasks(5000).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "opt-in million-task load test"]
async fn one_million_live_managed_tasks() {
    concurrent_tasks(1_000_000).await;
}
