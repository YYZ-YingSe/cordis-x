use cordis_loader::watch::{ConfigWatcher, WatchOptions};
use cordis_loader::{self as loader, Configuration, Loader, LoaderSnapshot, Registry, json};
use cordis_sdk::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct Database {
    connection: String,
    open: AtomicBool,
}
fn database() -> Service<Database> {
    Service::new("example.database", "1")
}
fn search() -> Service<()> {
    Service::new("example.search", "1")
}

fn registry() -> loader::Result<Registry> {
    let mut registry = Registry::new();
    registry.register("database", |config| {
        let connection = config["connection"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::Message("database config requires a nonempty connection".into()))?
            .to_owned();
        Ok(Plugin::new("database", move |ctx| {
            let connection = connection.clone();
            async move {
                let db = Arc::new(Database {
                    connection,
                    open: AtomicBool::new(true),
                });
                println!("database: open {}", db.connection);
                let closing = db.clone();
                ctx.defer("connection", move |_| async move {
                    closing.open.store(false, Ordering::SeqCst);
                    println!("database: close {}", closing.connection);
                    Ok(())
                })?;
                ctx.provide(&database(), db)
            }
        })
        .provides(&database()))
    })?;
    registry.register("search", |_| {
        Ok(Plugin::new("search", |ctx| async move {
            ctx.dependency(&database())?.with(|db, _| {
                println!("search: build index using {}", db.connection);
                Ok(())
            })?;
            ctx.defer("index", |ctx| async move {
                ctx.dependency(&database())?.with(|db, _| {
                    assert!(db.open.load(Ordering::SeqCst));
                    println!("search: clean index using still-open {}", db.connection);
                    Ok(())
                })
            })?;
            ctx.provide(&search(), Arc::new(()))
        })
        .requires(&database())
        .provides(&search()))
    })?;
    registry.register("tools", |_| {
        Ok(Plugin::new("tools", |ctx| async move {
            println!("tools: ready");
            ctx.defer("tools", |_| async {
                println!("tools: cleaned");
                Ok(())
            })?;
            Ok(())
        })
        .requires(&search()))
    })?;
    registry.register("metrics", |_| {
        Ok(Plugin::new("metrics", |ctx| async move {
            println!("metrics: started");
            ctx.defer("metrics", |_| async {
                println!("metrics: stopped");
                Ok(())
            })?;
            Ok(())
        }))
    })?;
    Ok(registry)
}

fn display(snapshot: &LoaderSnapshot) {
    println!(
        "configuration revision {} (applied {})",
        snapshot.desired_revision, snapshot.applied_revision
    );
    for entry in &snapshot.entries {
        if let Some(instance) = &entry.instance {
            println!(
                "  {}: {:?}, activation={:?}, waiting={:?}, outcome={:?}",
                entry.id,
                instance.state,
                instance.activation,
                instance.waiting_for,
                instance.outcome
            );
        } else {
            println!("  {}: disabled", entry.id);
        }
    }
}

async fn watch_file(
    loader: &Loader,
    file: &str,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let watcher = ConfigWatcher::start(loader.clone(), file, WatchOptions::default())?;
    let mut status = watcher.subscribe();
    println!(
        "Watching {}; save to apply, Ctrl-C to stop.",
        watcher.path().display()
    );
    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);
    let result: std::result::Result<(), Box<dyn std::error::Error>> = loop {
        tokio::select! {
            result = &mut interrupt => {
                break result.map_err(Into::into);
            }
            result = status.changed() => {
                if let Err(error) = result { break Err(error.into()); }
                let state = status.borrow_and_update().clone();
                println!("file: {:?}, reads={}, submissions={}", state.phase, state.reads, state.submissions);
                if let Some(error) = state.error { eprintln!("{error}"); }
                if let Some(error) = state.backend_error { eprintln!("filesystem notifications degraded: {error}"); }
                display(&state.target);
            }
        }
    };
    let stopped = watcher.stop().await;
    if let Ok(state) = &stopped
        && let Some(error) = &state.error
    {
        eprintln!("last file update: {error}");
    }
    result?;
    stopped?;
    Ok(())
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let runtime = Runtime::new();
    let loader = Loader::new(runtime.clone(), registry()?);
    let input = if let Some(file) = std::env::args().nth(1) {
        watch_file(&loader, &file).await
    } else {
        let mut config = Configuration::from_json(include_str!("loader.json"))?;
        loader.apply(config.clone()).await?;
        let first = loader.settled().await?;
        let metrics = first
            .entries
            .iter()
            .find(|e| e.id == "metrics")
            .unwrap()
            .instance
            .as_ref()
            .unwrap()
            .activation;
        display(&first);
        config
            .entries
            .iter_mut()
            .find(|e| e.id == "database")
            .unwrap()
            .config = json!({"connection":"database-b"});
        println!("-- change only database configuration --");
        let plan = loader.plan(config.clone()).await?;
        println!("retire {:?}, install {:?}", plan.retire, plan.install);
        loader.apply(config.clone()).await?;
        let updated = loader.settled().await?;
        assert_eq!(
            metrics,
            updated
                .entries
                .iter()
                .find(|e| e.id == "metrics")
                .unwrap()
                .instance
                .as_ref()
                .unwrap()
                .activation
        );
        display(&updated);
        println!("-- disable database: consumers wait, metrics keeps running --");
        config
            .entries
            .iter_mut()
            .find(|e| e.id == "database")
            .unwrap()
            .enabled = false;
        loader.apply(config.clone()).await?;
        display(&loader.settled().await?);
        println!("-- enable database --");
        config
            .entries
            .iter_mut()
            .find(|e| e.id == "database")
            .unwrap()
            .enabled = true;
        loader.apply(config).await?;
        display(&loader.settled().await?);
        Ok(())
    };
    println!("-- clear configuration --");
    let cleared = loader.apply(Configuration::default()).await;
    let stopped = runtime.shutdown().await;
    input?;
    cleared?;
    stopped?;
    Ok(())
}
