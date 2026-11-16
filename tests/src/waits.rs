//! The same probe crosses every physical adapter. It distinguishes withdrawn
//! observations from admitted work without depending on tool or Agent code.
use cordis_sdk_core as sdk;
use futures_util::future::{Either, select};
use std::{collections::BTreeMap, rc::Rc};

pub struct Probe;
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
        interface(&["race", "cancel-race", "invalid-wait", "work", "start-task"])
    }
    fn dependency_interfaces() -> BTreeMap<String, sdk::Interface> {
        BTreeMap::from([("test.wait-log".into(), interface(&["write"]))])
    }
    fn create(_: sdk::Value) -> sdk::Result<Self> {
        Ok(Self)
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
