//! Host middleware injects real duplicate/expired continuations through typed clients.
use cordis_sdk as rt;
use std::sync::{Arc, Mutex};
pub async fn install<T: Clone + Send + Sync + 'static>(
    runtime: &rt::Runtime,
    expired: bool,
) -> rt::PluginHandle {
    let saved = Arc::new(Mutex::new(None::<rt::Next<T, T>>));
    let plugin = runtime
        .install(rt::Plugin::new("next admission probe", move |ctx| {
            let saved = saved.clone();
            async move {
                ctx.middleware(
                    &rt::Middleware::new("example.request", "1"),
                    move |args, _, next| {
                        let saved = saved.clone();
                        async move {
                            if expired {
                                let previous = saved.lock().unwrap().take();
                                if let Some(previous) = previous {
                                    return previous.run(args).await;
                                }
                                *saved.lock().unwrap() = Some(next.clone());
                                next.run(args).await
                            } else {
                                next.run(args.clone()).await?;
                                next.run(args).await
                            }
                        }
                    },
                )?;
                Ok(())
            }
        }))
        .unwrap();
    plugin.ready().await.unwrap();
    plugin
}
