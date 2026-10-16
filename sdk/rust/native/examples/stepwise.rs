use cordis_sdk::*;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

struct Database {
    generation: usize,
    open: AtomicBool,
}
fn database() -> Service<Database> {
    Service::new("example.database", "1")
}
fn search() -> Service<usize> {
    Service::new("example.search", "1")
}

#[derive(Clone)]
struct Pause {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), demonstrate())
        .await
        .map_err(|_| Error::Message("stepwise example timed out".into()))?
}

async fn demonstrate() -> Result<()> {
    let runtime = Runtime::new();
    let generations = AtomicUsize::new(0);
    let db = runtime.install(
        Plugin::new("database", move |ctx| {
            let generation = generations.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                let db = Arc::new(Database {
                    generation,
                    open: AtomicBool::new(true),
                });
                let closing = db.clone();
                ctx.defer("database connection", move |_| async move {
                    closing.open.store(false, Ordering::SeqCst);
                    println!("database {generation}: closed");
                    Ok(())
                })?;
                println!("database {generation}: opened");
                ctx.provide(&database(), db)
            }
        })
        .provides(&database()),
    )?;

    let pause = Pause {
        entered: Arc::new(Semaphore::new(0)),
        release: Arc::new(Semaphore::new(0)),
    };
    let search_plugin = runtime.install(
        Plugin::new("search", {
            let pause = pause.clone();
            move |ctx| {
                let pause = pause.clone();
                async move {
                    let generation = ctx
                        .dependency(&database())?
                        .with(|db, _| Ok(db.generation))?;
                    let mut index = 0;
                    let sequence = ctx.effect("search initialization", move |step| {
                        let current = index;
                        index += 1;
                        let pause = pause.clone();
                        async move {
                            if current == 2 {
                                println!("search {generation}: stage service for publication");
                                step.provide(&search(), Arc::new(generation))?;
                                return Ok(None);
                            }
                            let label = ["allocate index", "load index"][current];
                            println!("search {generation}: begin {label}");
                            // Hold generation 1's second step until withdrawal.
                            if generation == 1 && current == 1 {
                                pause.entered.add_permits(1);
                                pause.release.acquire().await.unwrap().forget();
                            }
                            println!("search {generation}: finish {label}");
                            Ok(Some(Inverse::new(label, move |cleanup| async move {
                                cleanup.dependency(&database())?.with(|db, _| {
                                    assert!(db.open.load(Ordering::SeqCst));
                                    assert_eq!(db.generation, generation);
                                    println!(
                                        "search {generation}: undo {label} with database still open"
                                    );
                                    Ok(())
                                })
                            })))
                        }
                    })?;
                    let completion = sequence.ready().await?;
                    println!("search {generation}: {completion:?}");
                    if completion == EffectCompletion::Stopped {
                        return Ok(());
                    }

                    // No .requires(&database()): this child inherits from search.
                    ctx.install(Plugin::new("index observer", |child| async move {
                        let generation = child
                            .dependency(&database())?
                            .with(|db, _| Ok(db.generation))?;
                        println!("child: inherited database {generation}");
                        child.defer("observer", move |cleanup| async move {
                            cleanup.dependency(&database())?.with(|db, _| {
                                assert!(db.open.load(Ordering::SeqCst));
                                println!("child: cleaned before search and database {generation}");
                                Ok(())
                            })
                        })?;
                        Ok(())
                    }))?
                    .ready()
                    .await?;
                    Ok(())
                }
            }
        })
        .requires(&database())
        .provides(&search()),
    )?;
    let consumer = runtime.install(
        Plugin::new("consumer", |ctx| async move {
            let generation = ctx.dependency(&search())?.with(|value, _| Ok(*value))?;
            assert_eq!(generation, 2, "generation 1 must never be published");
            println!("consumer: sees only search {generation}");
            Ok(())
        })
        .requires(&search()),
    )?;

    pause.entered.acquire().await.unwrap().forget();
    println!("-- restart database during the load-index step --");
    let (restarted, ()) = tokio::join!(db.restart(), async {
        while search_plugin.snapshot().state != Lifecycle::Unloading {
            tokio::task::yield_now().await;
        }
        pause.release.add_permits(1);
    });
    restarted?;
    consumer.ready().await?;
    println!("-- shutdown --");
    runtime.shutdown().await
}
