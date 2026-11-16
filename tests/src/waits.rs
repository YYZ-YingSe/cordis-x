//! The same probe crosses every physical adapter. It distinguishes withdrawn
//! observations from admitted work without depending on tool or Agent code.
use cordis_sdk_core as sdk;
use futures_util::future::{Either, select};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    rc::Rc,
};

pub struct Probe {
    task: RefCell<Option<sdk::Task>>,
    sequence: RefCell<Option<sdk::Sequence>>,
    child: RefCell<Option<sdk::Child>>,
    completed: Rc<Cell<usize>>,
}
pub fn interface(names: &[&str]) -> sdk::Interface {
    sdk::Interface {
        methods: sdk::Dispatch::Named(
            names
                .iter()
                .map(|name| {
                    (
                        (*name).into(),
                        sdk::Method::new(
                            sdk::MessageContract::json_value(),
                            sdk::MessageContract::json_value(),
                        ),
                    )
                })
                .collect(),
        ),
        resources: sdk::Dispatch::Named(BTreeMap::new()),
        controls: BTreeMap::new(),
        capabilities: Default::default(),
    }
}
impl sdk::Plugin for Probe {
    fn manifest() -> sdk::PluginManifest {
        sdk::PluginManifest {
            provides: sdk::Contract {
                name: "test.waits".into(),
                revision: "1".into(),
            },
            requires: vec![sdk::Contract {
                name: "test.wait-log".into(),
                revision: "1".into(),
            }],
        }
    }
    fn interface() -> sdk::Interface {
        interface(&[
            "race",
            "cancel-race",
            "invalid-wait",
            "work",
            "start-task",
            "join-race",
            "join-again",
            "ready-race",
            "ready-again",
            "child-race",
            "child-again",
            "independent",
        ])
    }
    fn dependency_interfaces() -> BTreeMap<String, sdk::Interface> {
        BTreeMap::from([("test.wait-log".into(), interface(&["write"]))])
    }
    fn bindings() -> sdk::Bindings {
        sdk::Bindings {
            factories: vec!["delayed-child".into()],
            ..Default::default()
        }
    }
    fn create(_: sdk::Value) -> sdk::Result<Self> {
        Ok(Self {
            task: RefCell::default(),
            sequence: RefCell::default(),
            child: RefCell::default(),
            completed: Rc::new(Cell::new(0)),
        })
    }
    fn invoke(
        self: Rc<Self>,
        ctx: sdk::Context,
        method: String,
        _: sdk::Message,
    ) -> sdk::message::FutureResult {
        Box::pin(async move {
            match method.as_str() {
                "race" => {
                    // Both branches use the real adapter. A losing wait must
                    // release its timer and completion slot without delaying
                    // this method, including repeated races in one callback.
                    for _ in 0..8 {
                        match select(Box::pin(ctx.sleep(2_000)), Box::pin(ctx.sleep(1))).await {
                            Either::Right((result, pending)) => {
                                result?;
                                drop(pending);
                            }
                            Either::Left(_) => return Err(sdk::refused("probe", "long timer won")),
                        }
                    }
                }
                "cancel-race" => {
                    let task = ctx
                        .spawn("cancel observation", |ctx| async move {
                            match select(Box::pin(ctx.cancelled()), Box::pin(ctx.sleep(1))).await {
                                Either::Right((result, pending)) => {
                                    result?;
                                    drop(pending);
                                }
                                Either::Left(_) => {
                                    return Err(sdk::refused("probe", "owner cancelled early"));
                                }
                            }
                            Ok(())
                        })
                        .await?;
                    task.join(&ctx).await?;
                }
                "invalid-wait" => {
                    if ctx.sleep(60_001).await.is_ok() {
                        return Err(sdk::refused("probe", "invalid timer duration accepted"));
                    }
                }
                "join-race" => {
                    let completed = self.completed.clone();
                    let task = ctx
                        .spawn("observable task", move |ctx| {
                            let completed = completed.clone();
                            async move {
                                ctx.sleep(2_000).await?;
                                completed.set(completed.get() + 1);
                                Ok(())
                            }
                        })
                        .await?;
                    match select(Box::pin(task.join(&ctx)), Box::pin(ctx.sleep(1))).await {
                        Either::Right((r, observer)) => {
                            r?;
                            drop(observer);
                        }
                        Either::Left(_) => {
                            return Err(sdk::refused("probe", "task completed early"));
                        }
                    }
                    *self.task.borrow_mut() = Some(task);
                }
                "join-again" => {
                    let task = self.task.borrow().clone().unwrap();
                    task.join(&ctx).await?;
                    task.join(&ctx).await?;
                    if self.completed.get() != 1 {
                        return Err(sdk::refused("probe", "withdrawal changed task execution"));
                    }
                }
                "ready-race" => {
                    let sequence = ctx
                        .effect("observable sequence", |ctx| async move {
                            ctx.sleep(2_000).await?;
                            Ok(sdk::Step::yield_inverse(|ctx| async move {
                                let contract = sdk::MessageContract::json_value();
                                ctx.invoke(
                                    "test.wait-log",
                                    "write",
                                    sdk::Message::from_json(
                                        contract.clone(),
                                        &sdk::json!("inverse"),
                                    )
                                    .map_err(sdk::typed::violation)?,
                                    &contract,
                                )
                                .await?;
                                Ok(())
                            })
                            .finish())
                        })
                        .await?;
                    match select(Box::pin(sequence.ready(&ctx)), Box::pin(ctx.sleep(1))).await {
                        Either::Right((r, observer)) => {
                            r?;
                            drop(observer);
                        }
                        Either::Left(_) => {
                            return Err(sdk::refused("probe", "sequence completed early"));
                        }
                    }
                    *self.sequence.borrow_mut() = Some(sequence);
                }
                "ready-again" => {
                    let sequence = self.sequence.borrow().clone().unwrap();
                    sequence.ready(&ctx).await?;
                    sequence.ready(&ctx).await?;
                }
                "child-race" => {
                    let config = sdk::Message::from_json(
                        sdk::MessageContract::json_value(),
                        &sdk::json!(null),
                    )
                    .map_err(sdk::typed::violation)?;
                    let child = ctx.install("delayed-child", config).await?;
                    match select(Box::pin(child.ready(&ctx)), Box::pin(ctx.sleep(1))).await {
                        Either::Right((r, observer)) => {
                            r?;
                            drop(observer);
                        }
                        Either::Left(_) => return Err(sdk::refused("probe", "child ready early")),
                    }
                    *self.child.borrow_mut() = Some(child);
                }
                "child-again" => {
                    let child = self.child.borrow().clone().unwrap();
                    child.ready(&ctx).await?;
                    child.ready(&ctx).await?;
                }
                "independent" => {
                    let task = ctx
                        .spawn(
                            "independent observers",
                            |ctx| async move { ctx.sleep(20).await },
                        )
                        .await?;
                    let mut surviving = Box::pin(task.join(&ctx));
                    // Poll both observers before dropping one. They must not
                    // share a cancellation token or cache a withdrawn result.
                    let _ = futures_util::poll!(&mut surviving);
                    match select(Box::pin(task.join(&ctx)), Box::pin(ctx.sleep(1))).await {
                        Either::Right((r, observer)) => {
                            r?;
                            drop(observer);
                        }
                        Either::Left((r, _)) => {
                            r?;
                        }
                    }
                    surviving.await?;
                }
                "work" => {
                    let result_contract = sdk::MessageContract::json_value();
                    let work = Box::pin(
                        ctx.invoke(
                            "test.wait-log",
                            "write",
                            sdk::Message::from_json(
                                sdk::MessageContract::json_value(),
                                &sdk::json!(true),
                            )
                            .map_err(sdk::typed::violation)?,
                            &result_contract,
                        ),
                    );
                    match select(work, Box::pin(ctx.sleep(1))).await {
                        Either::Right((result, accepted)) => {
                            result?;
                            drop(accepted);
                        }
                        Either::Left((result, _)) => {
                            result?;
                        }
                    }
                }
                "start-task" => {
                    ctx.spawn(
                        "owner cancellation",
                        |ctx| async move { ctx.cancelled().await },
                    )
                    .await?;
                }
                _ => return Err(sdk::refused("unknown_method", method)),
            }
            sdk::Message::from_json(sdk::MessageContract::json_value(), &sdk::json!(true))
                .map_err(sdk::typed::violation)
        })
    }
}
