use cordis_sdk::{prelude::*, runtime::Runtime};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct Database {
    generation: usize,
    open: Arc<AtomicBool>,
}
fn database() -> Service<Database> {
    Service::new("example.database", "1")
}
fn search() -> Service<()> {
    Service::new("example.search", "1")
}

#[tokio::main]
async fn main() -> Result<()> {
    let runtime = Runtime::new();

    // Consumers may be installed before their providers. Missing dependencies wait.
    let search_plugin = runtime.install(
        Plugin::new("search", |ctx| async move {
            let generation = ctx
                .dependency(&database())?
                .with(|db, _| Ok(db.generation))?;
            println!("search: using database generation {generation}");
            ctx.defer("search index", |cleanup| async move {
                cleanup
                    .dependency(&database())?
                    .call(|db, _| {
                        Box::pin(async move {
                            tokio::task::yield_now().await;
                            assert!(db.open.load(Ordering::SeqCst));
                            println!(
                                "search: cleaned using still-open database {}",
                                db.generation
                            );
                            Ok(())
                        })
                    })
                    .await
            })?;
            ctx.provide(&search(), Arc::new(()))
        })
        .requires(&database())
        .provides(&search()),
    )?;
    println!(
        "search: {:?} (waiting for database)",
        search_plugin.settled().await?.state
    );

    let tools = runtime.install(
        Plugin::new("tools", |ctx| async move {
            println!("tools: ready");
            ctx.defer("tool registrations", |_| async {
                println!("tools: cleaned");
                Ok(())
            })?;
            Ok(())
        })
        .requires(&search()),
    )?;

    let generation = Arc::new(AtomicUsize::new(0));
    let db = runtime.install(
        Plugin::new("database", move |ctx| {
            let generation = generation.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                let open = Arc::new(AtomicBool::new(true));
                let close = open.clone();
                ctx.defer("database connection", move |_| async move {
                    close.store(false, Ordering::SeqCst);
                    println!("database {generation}: closed");
                    Ok(())
                })?;
                println!("database {generation}: opened");
                ctx.provide(&database(), Arc::new(Database { generation, open }))
            }
        })
        .provides(&database()),
    )?;

    tools.ready().await?;
    println!("-- restart provider; consumers clean up before the old connection closes --");
    db.restart().await?;
    tools.ready().await?;
    println!("-- shutdown --");
    runtime.shutdown().await
}
