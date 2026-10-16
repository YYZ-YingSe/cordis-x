use super::*;
use cordis_sdk::{EffectStatus, PluginHandle, Runtime, ServiceHandle};
use std::{sync::mpsc, time::Duration};
#[path = "../tests/support/mod.rs"]
mod support;

pub(crate) struct Hook {
    op: Op,
    entered: tokio::sync::oneshot::Sender<()>,
    resume: mpsc::Receiver<()>,
}
pub(crate) fn after_call(instance: &Instance, op: Op) {
    let hook = {
        let mut hook = instance.hook.lock().unwrap();
        if hook
            .as_ref()
            .is_some_and(|h| std::mem::discriminant(&h.op) == std::mem::discriminant(&op))
        {
            hook.take()
        } else {
            None
        }
    };
    if let Some(hook) = hook {
        let _ = hook.entered.send(());
        // Disconnect also resumes: assertion failures cannot strand a worker.
        let _ = hook.resume.recv();
    }
}
fn gate(instance: &Instance, op: Op) -> (tokio::sync::oneshot::Receiver<()>, mpsc::Sender<()>) {
    let (entered, receiver) = tokio::sync::oneshot::channel();
    let (resume, sender) = mpsc::channel();
    *instance.hook.lock().unwrap() = Some(Hook {
        op,
        entered,
        resume: sender,
    });
    (receiver, resume)
}
struct App {
    rt: Runtime,
    provider: PluginHandle,
    client: PluginHandle,
    handle: ServiceHandle<WasmService>,
    instance: Arc<Instance>,
}
async fn app(config: Value) -> App {
    let rt = Runtime::new();
    let service = Service::new("example.calculator", "1").with_descriptor(
        cordis_contract::portable::decode(
            &cordis_test_contracts::resources::rust_fixture::SERVICE
                .parse()
                .unwrap(),
        )
        .unwrap(),
    );
    let provider = rt
        .install(
            support::component()
                .plugin("wasm", &service, config)
                .unwrap(),
        )
        .unwrap();
    provider.ready().await.unwrap();
    let saved = Arc::new(Mutex::new(None));
    let client = rt
        .install(
            Plugin::new("client", {
                let saved = saved.clone();
                let service = service.clone();
                move |ctx| {
                    let saved = saved.clone();
                    let service = service.clone();
                    async move {
                        *saved.lock().unwrap() = Some(ctx.dependency(&service)?);
                        Ok(())
                    }
                }
            })
            .requires(&service),
        )
        .unwrap();
    client.ready().await.unwrap();
    let handle = saved.lock().unwrap().take().unwrap();
    let instance = handle
        .call(|s, _| Box::pin(async move { Ok(s.instance.clone()) }))
        .await
        .unwrap();
    App {
        rt,
        provider,
        client,
        handle,
        instance,
    }
}
async fn blocked(task: &mut tokio::task::JoinHandle<cordis_runtime::Result<()>>) {
    assert!(
        tokio::time::timeout(Duration::from_millis(30), task)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn cancelled_call_still_blocks_provider_recovery() {
    let app = app(crate::json!({})).await;
    let (entered, resume) = gate(&app.instance, Op::Invoke);
    let handle = app.handle.clone();
    let waiter = tokio::spawn(async move {
        handle
            .call(|s, c| {
                Box::pin(async move {
                    s.invoke_json(&c, "echo", json!(42))
                        .await
                        .map_err(Into::into)
                })
            })
            .await
    });
    entered.await.unwrap();
    waiter.abort();
    let _ = waiter.await;
    let provider = app.provider.clone();
    let mut cleanup = tokio::spawn(async move { provider.dispose().await });
    blocked(&mut cleanup).await;
    resume.send(()).unwrap();
    cleanup.await.unwrap().unwrap();
    assert!(app.instance.state.lock().unwrap().closed);
    app.rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_acquire_registers_and_releases_late_guest_resource() {
    let app = app(crate::json!({})).await;
    let (entered, resume) = gate(&app.instance, Op::Acquire);
    let handle = app.handle.clone();
    let waiter = tokio::spawn(async move {
        handle
            .call(|s, c| {
                Box::pin(async move {
                    s.acquire_json(&c, "counter", crate::json!({}))
                        .await
                        .map_err(Into::into)
                })
            })
            .await
    });
    entered.await.unwrap();
    waiter.abort();
    let _ = waiter.await;
    let provider = app.provider.clone();
    let mut cleanup = tokio::spawn(async move { provider.dispose().await });
    blocked(&mut cleanup).await;
    resume.send(()).unwrap();
    cleanup.await.unwrap().unwrap();
    // Successful recovery retires the activation and its effect list. The guest
    // refuses deactivate while a counter remains, so closed proves late release.
    assert_eq!(app.instance.state.lock().unwrap().resources, 0);
    assert!(app.instance.state.lock().unwrap().closed);
    app.rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn inner_cancelled_acquire_keeps_fault_authority_and_unknown_ticket() {
    let app = app(json!({"fail":"acquire", "unknown":true})).await;
    let (entered, resume) = gate(&app.instance, Op::Acquire);
    let (cancel, cancelled) = tokio::sync::oneshot::channel();
    let handle = app.handle.clone();
    let waiter = tokio::spawn(async move {
        handle.call(move |s,c| Box::pin(async move {
        tokio::select! {
            _ = s.acquire_json(&c, "counter", crate::json!({})) => panic!("gate must hold acquisition"),
            _ = cancelled => {},
        }
        Ok(())
    })).await
    });
    entered.await.unwrap();
    cancel.send(()).unwrap();
    waiter.await.unwrap().unwrap();
    resume.send(()).unwrap();
    // With expired authority this would never withdraw the provider.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if app
                .client
                .effects()
                .iter()
                .any(|e| matches!(e.status, EffectStatus::Failed(_)))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(app.provider.dispose().await.is_err());
    assert!(app.rt.shutdown().await.is_err());
}

#[tokio::test]
async fn resource_release_waits_for_cancelled_resource_call() {
    let app = app(crate::json!({})).await;
    let resource = app
        .handle
        .call(|s, c| {
            Box::pin(async move {
                s.acquire_json(&c, "counter", crate::json!({}))
                    .await
                    .map_err(Into::into)
            })
        })
        .await
        .unwrap();
    let id = resource
        .resource
        .with(|t| *t.acquired.as_ref().unwrap())
        .unwrap();
    let (entered, resume) = gate(&app.instance, Op::ResourceCall(id));
    let worker = resource.clone();
    let waiter = tokio::spawn(async move { worker.invoke_json("add", json!(2)).await });
    entered.await.unwrap();
    waiter.abort();
    let _ = waiter.await;
    let worker = resource.clone();
    let mut cleanup = tokio::spawn(async move { worker.resource.dispose().await });
    blocked(&mut cleanup).await;
    resume.send(()).unwrap();
    cleanup.await.unwrap().unwrap();
    assert_eq!(resource.snapshot().status, EffectStatus::Released);
    app.rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn admission_rejects_excess_work_without_blocking_tokio() {
    let app = app(crate::json!({})).await;
    let permits = app
        .instance
        .capacity
        .clone()
        .acquire_many_owned(16)
        .await
        .unwrap();
    let error = app
        .handle
        .call(|s, c| Box::pin(async move { Ok(s.invoke_json(&c, "echo", Value::Null).await) }))
        .await
        .unwrap();
    assert!(matches!(error, Err(Error::Overloaded)));
    drop(permits);
    app.rt.shutdown().await.unwrap();
}
