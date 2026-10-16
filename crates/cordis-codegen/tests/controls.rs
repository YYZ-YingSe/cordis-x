#[path = "support/next_probe.rs"]
mod next_probe;
use cordis_portable_example::workflows::{Plugin as Workflow, generated::*};
use cordis_sdk as rt;
use cordis_sdk_core::{self as sdk, Plugin as _, json};
use cordis_sdk_runtime as adapter;
use data::{cordis::data::Int64, workflows::Tick};
use sdk::backend::{Backend, Handle, Listener, Operation, Reply};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Default)]
struct Capture {
    listener: RefCell<Option<Listener>>,
    middleware: RefCell<Option<sdk::backend::Middleware>>,
    next_calls: Rc<Cell<usize>>,
}
impl Backend for Capture {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn submit(&self, operation: Operation) -> sdk::LocalFuture<Reply> {
        let answer = match operation {
            Operation::On { handler, .. } => {
                *self.listener.borrow_mut() = Some(handler);
                Ok(Reply::Handle(Handle::new(())))
            }
            Operation::Middleware { handler, .. } => {
                *self.middleware.borrow_mut() = Some(handler);
                Ok(Reply::Handle(Handle::new(())))
            }
            Operation::Next { args, .. } => {
                self.next_calls.set(self.next_calls.get() + 1);
                let value: Tick = args.into_typed().unwrap();
                Ok(Reply::Message(
                    sdk::Message::new(Int64 { value: value.value }).unwrap(),
                ))
            }
            _ => Err(sdk::refused("unbound", "no capability")),
        };
        Box::pin(async { answer })
    }
}
#[tokio::test]
async fn callback_validation_isolates_business_and_next_arguments() {
    let backend = Rc::new(Capture::default());
    let ctx = sdk::Context::from_backend(backend.clone());
    let entered = Rc::new(Cell::new(0));
    WorkflowControls::tick()
        .on(&ctx, {
            let entered = entered.clone();
            move |_, args| {
                entered.set(entered.get() + 1);
                async move {
                    match args.value.unwrap() {
                        0 => Ok(sdk::typed::Flow::Continue),
                        1 => Ok(sdk::typed::Flow::Break(Int64 { value: None })),
                        2 => Err(sdk::error("undeclared", "fault")),
                        _ => Err(sdk::refused("overflow", "authority refusal")),
                    }
                }
            }
        })
        .await
        .unwrap();
    let listener = backend.listener.borrow().clone().unwrap();
    assert!(matches!(
        listener(
            ctx.clone(),
            sdk::Message::new(data::cordis::data::Unit {}).unwrap()
        )
        .await,
        Err(sdk::Failure::Refused(_))
    ));
    assert_eq!(entered.get(), 0);
    assert!(matches!(
        listener(
            ctx.clone(),
            sdk::Message::new(Tick { value: Some(0) }).unwrap()
        )
        .await
        .unwrap(),
        sdk::typed::Flow::Continue
    ));
    for n in [1, 2] {
        assert!(matches!(
            listener(
                ctx.clone(),
                sdk::Message::new(Tick { value: Some(n) }).unwrap()
            )
            .await,
            Err(sdk::Failure::Unknown(_))
        ));
    }
    assert!(matches!(
        listener(
            ctx.clone(),
            sdk::Message::new(Tick { value: Some(3) }).unwrap()
        )
        .await,
        Err(sdk::Failure::Refused(_))
    ));
    WorkflowControls::request()
        .register(&ctx, |ctx, args, next| async move {
            assert!(matches!(
                next.run(&ctx, Tick { value: None }).await,
                Err(sdk::Failure::Refused(_))
            ));
            next.run(&ctx, args).await
        })
        .await
        .unwrap();
    let handler = backend.middleware.borrow().clone().unwrap();
    assert_eq!(
        handler(
            ctx,
            sdk::Message::new(Tick { value: Some(7) }).unwrap(),
            sdk::Next::from_handle(Handle::new(()))
        )
        .await
        .unwrap()
        .into_typed::<Int64>()
        .unwrap(),
        Int64 { value: Some(7) }
    );
    assert_eq!(backend.next_calls.get(), 1);
}
#[test]
fn factory_validation_precedes_business_and_control_identity_is_structural() {
    let entered = Cell::new(0);
    let factory = WorkflowControls::worker();
    for value in [
        json!({}),
        json!({"label":2}),
        json!({"label":"x","extra":true}),
    ] {
        assert!(matches!(
            factory.build_json(value, |_| {
                entered.set(entered.get() + 1);
                Ok(())
            }),
            Err(sdk::Failure::Refused(_))
        ));
    }
    assert_eq!(entered.get(), 0);
    assert!(matches!(
        factory.build_json(json!({"label":"x"}), |_| Err::<(), _>(sdk::error(
            "undeclared",
            "fault"
        ))),
        Err(sdk::Failure::Unknown(_))
    ));
    assert_eq!(
        factory
            .build_json(json!({"label":"x"}), |c| Ok(c.label.unwrap()))
            .unwrap(),
        "x"
    );
    let expected = sdk::Descriptor::new(WorkflowClient::interface()).unwrap();
    assert!(expected.interface().methods.get("tick").is_none());
    let mut changed = expected.interface().clone();
    let sdk::ControlContract::Factory { config, .. } = changed.controls.get_mut("worker").unwrap()
    else {
        panic!()
    };
    *config = sdk::MessageContract::standard("google.protobuf.Empty");
    assert_eq!(
        expected
            .compare(&sdk::Descriptor::new(changed).unwrap())
            .unwrap_err()
            .path,
        "controls"
    );
    let bindings = Workflow::bindings();
    assert_eq!(bindings.events["tick"].name, "example.tick");
    assert_eq!(bindings.middleware["request"].name, "example.request");
    assert_eq!(bindings.factories, vec!["worker"]);
    assert_eq!(bindings.provisions["settings"].name, "example.settings");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controls_run_on_native_runtime_and_children_follow_the_calling_owner() {
    let runtime = rt::Runtime::new();
    let event = rt::Event::new("example.tick", "1");
    let chain = rt::Middleware::new("example.request", "1");
    let settings = rt::Service::<sdk::Message>::new("example.settings", "1");
    let service = rt::Service::<adapter::Service>::new("example.workflow", "1")
        .with_descriptor(sdk::Descriptor::new(WorkflowClient::interface()).unwrap());
    let created = Arc::new(AtomicUsize::new(0));
    let closed = Arc::new(AtomicUsize::new(0));
    let factory: adapter::Factory = {
        let created = created.clone();
        let closed = closed.clone();
        Arc::new(move |config| {
            WorkflowControls::worker()
                .build(config, |config| {
                    created.fetch_add(1, Ordering::SeqCst);
                    let closed = closed.clone();
                    Ok(rt::Plugin::new(config.label.unwrap(), move |ctx| {
                        let closed = closed.clone();
                        async move {
                            ctx.defer("worker", move |_| async move {
                                closed.fetch_add(1, Ordering::SeqCst);
                                Ok(())
                            })?;
                            Ok(())
                        }
                    }))
                })
                .map_err(Into::into)
        })
    };
    let provider = runtime
        .install(
            adapter::plugin::<Workflow>(
                "workflow",
                &service,
                json!({}),
                adapter::Capabilities {
                    events: [("tick".into(), event.clone())].into(),
                    middleware: [("request".into(), chain)].into(),
                    factories: [("worker".into(), factory)].into(),
                    provisions: [("settings".into(), settings.clone())].into(),
                    ..Default::default()
                },
            )
            .unwrap(),
        )
        .unwrap();
    provider.ready().await.unwrap();
    let saved = Arc::new(Mutex::new(None));
    let owner = runtime
        .install(
            rt::Plugin::new("consumer", {
                let saved = saved.clone();
                let service = service.clone();
                let settings = settings.clone();
                move |ctx| {
                    let saved = saved.clone();
                    let service = service.clone();
                    let settings = settings.clone();
                    async move {
                        let config = ctx.dependency(&settings)?.with(|v, _| Ok(v.clone()))?;
                        assert_eq!(
                            WorkflowControls::settings().decode(config).unwrap().answer,
                            Some(42)
                        );
                        *saved.lock().unwrap() = Some(ctx.dependency(&service)?);
                        Ok(())
                    }
                }
            })
            .requires(&service)
            .requires(&settings),
        )
        .unwrap();
    owner.ready().await.unwrap();
    let handle: rt::ServiceHandle<adapter::Service> = saved.lock().unwrap().take().unwrap();
    for (method, args, expected) in [
        ("run", json!({"value":"5"}), json!({"value":"15"})),
        ("run", json!({"value":"0"}), json!({"value":"100"})),
        (
            "serial",
            json!({"value":"5"}),
            json!({"stopped":{"value":"6"}}),
        ),
        ("serial", json!({"value":"-1"}), json!({"continued":{}})),
        (
            "parallel",
            json!({"value":"5"}),
            json!({"values":[{"stopped":{"value":"6"}}]}),
        ),
        ("spawn_worker", json!({"label":"owned-child"}), json!({})),
    ] {
        let value = handle
            .call(move |s, ctx| {
                Box::pin(async move {
                    s.invoke_json(&ctx, method, args)
                        .await
                        .map_err(|e| rt::Error::Message(e.to_string()))
                })
            })
            .await
            .unwrap();
        assert_eq!(value, expected, "{method}");
    }
    for expired in [false, true] {
        let probe = next_probe::install::<cordis_sdk_core::Message>(&runtime, expired).await;
        if expired {
            handle
                .call(|s, ctx| {
                    Box::pin(async move {
                        s.invoke_json(&ctx, "run", json!({"value":"5"}))
                            .await
                            .map_err(Into::into)
                    })
                })
                .await
                .unwrap();
        }
        let error = handle
            .call(|s, ctx| {
                Box::pin(async move {
                    Ok(s.invoke_json(&ctx, "run", json!({"value":"5"}))
                        .await
                        .unwrap_err())
                })
            })
            .await
            .unwrap();
        let code = if expired {
            "expired_next"
        } else {
            "duplicate_next"
        };
        assert!(
            matches!(&error, sdk::Failure::Refused(info) if info.code == code),
            "{error:?}"
        );
        probe.dispose().await.unwrap();
    }
    assert_eq!(created.load(Ordering::SeqCst), 1);
    assert_eq!(closed.load(Ordering::SeqCst), 0);
    owner.dispose().await.unwrap();
    assert_eq!(closed.load(Ordering::SeqCst), 1);
    provider.dispose().await.unwrap();
    runtime.shutdown().await.unwrap();
}
