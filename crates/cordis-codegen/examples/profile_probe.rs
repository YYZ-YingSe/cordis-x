#![allow(dead_code)]
use cordis_sdk_core::{self as sdk, Plugin};
use cordis_test_contracts::profile as generated;
use generated::data::{
    cordis::data::{Int64, Text, Unit},
    profile::{Config, Data},
};
use std::{cell::Cell, io::BufRead, rc::Rc};
struct Empty;
impl sdk::backend::Backend for Empty {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn submit(&self, _: sdk::backend::Operation) -> sdk::LocalFuture<sdk::backend::Reply> {
        Box::pin(async { Err(sdk::refused("unbound", "no grants")) })
    }
}
struct Probe(Cell<u64>);
impl generated::ProbeServer for Probe {
    fn create(_: Config) -> sdk::Result<Self> {
        Ok(Self(Cell::new(0)))
    }
    fn echo(self: Rc<Self>, _: sdk::Context, args: Data) -> sdk::LocalFuture<Data> {
        Box::pin(async move {
            self.0.set(self.0.get() + 1);
            Ok(args)
        })
    }
    fn number(self: Rc<Self>, _: sdk::Context, args: Int64) -> sdk::LocalFuture<Int64> {
        Box::pin(async move {
            self.0.set(self.0.get() + 1);
            Ok(if args.value == Some(-1) {
                Int64 { value: None }
            } else {
                args
            })
        })
    }
    fn fail(self: Rc<Self>, _: sdk::Context, args: Text) -> sdk::LocalFuture<Unit> {
        Box::pin(async move {
            self.0.set(self.0.get() + 1);
            Err(match args.value.as_deref().unwrap() {
                "refused" => sdk::refused("expected", "runtime refusal"),
                "unknown" => sdk::Failure::Unknown(sdk::ErrorInfo {
                    code: "expected".into(),
                    message: "unknown".into(),
                }),
                _ => sdk::error(args.value.as_deref().unwrap(), "business error"),
            })
        })
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    for line in std::io::stdin().lock().lines() {
        let envelope: sdk::Value = serde_json::from_str(&line.unwrap()).unwrap();
        let plugin =
            Rc::new(generated::ProbeExport::<Probe>::create(sdk::json!({"limit":10})).unwrap());
        let business = plugin.0.clone();
        let result = sdk::json::invoke(
            plugin,
            sdk::Context::from_backend(Rc::new(Empty)),
            envelope["method"].as_str().unwrap_or(""),
            envelope["args"].clone(),
        )
        .await;
        let mut answer = match result {
            Ok(value) => sdk::json!({"kind":"success","value":value}),
            Err(sdk::Failure::Rejected(_)) => sdk::json!({"kind":"rejected"}),
            Err(sdk::Failure::Refused(_)) => sdk::json!({"kind":"refused"}),
            Err(sdk::Failure::Unknown(_)) => sdk::json!({"kind":"unknown"}),
        };
        answer["entered"] = business.0.get().into();
        println!("{answer}");
    }
}
