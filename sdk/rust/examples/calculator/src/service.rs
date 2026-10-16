use crate::generated::{
    ServiceCounterServer, ServiceExport, ServiceServer,
    data::{
        cordis::data::Unit,
        fixtures::calculator_wasm::{AddArgs, CounterArgs, StatsResult},
        google::protobuf::{Empty, Int64Value, StringValue, Value},
    },
};
use cordis_sdk_core::{Context, LocalFuture, Result, error};
use std::{cell::Cell, rc::Rc};

pub type Example = ServiceExport<Calculator>;
#[derive(Default)]
pub struct Calculator {
    calls: Cell<i64>,
    leases: Cell<i64>,
    releases: Cell<i64>,
}
struct Counter {
    owner: Rc<Calculator>,
    value: Cell<i64>,
}
fn add(left: i64, right: i64) -> Result<i64> {
    left.checked_add(right)
        .ok_or_else(|| error("overflow", "integer overflow"))
}
impl Calculator {
    fn called(&self) -> Result<()> {
        self.calls.set(
            self.calls
                .get()
                .checked_add(1)
                .ok_or_else(|| cordis_sdk_core::typed::violation("call count exhausted"))?,
        );
        Ok(())
    }
}
impl ServiceServer for Calculator {
    fn create(_: Unit) -> Result<Self> {
        Ok(Self::default())
    }
    fn code_version(self: Rc<Self>, _: Context, _: Empty) -> LocalFuture<StringValue> {
        Box::pin(async move {
            self.called()?;
            Ok(StringValue {
                value: if cfg!(feature = "version-v2") {
                    "v2"
                } else {
                    "v1"
                }
                .into(),
            })
        })
    }
    fn echo(self: Rc<Self>, _: Context, args: Value) -> LocalFuture<Value> {
        Box::pin(async move {
            self.called()?;
            Ok(args)
        })
    }
    fn add(self: Rc<Self>, _: Context, args: AddArgs) -> LocalFuture<Int64Value> {
        Box::pin(async move {
            self.called()?;
            Ok(Int64Value {
                value: add(args.left.unwrap(), args.right.unwrap())?,
            })
        })
    }
    fn stats(self: Rc<Self>, _: Context, _: Empty) -> LocalFuture<StatsResult> {
        Box::pin(async move {
            self.called()?;
            Ok(StatsResult {
                calls: Some(self.calls.get()),
                leases: Some(self.leases.get()),
                releases: Some(self.releases.get()),
            })
        })
    }
    fn counter(
        self: Rc<Self>,
        _: Context,
        args: CounterArgs,
    ) -> LocalFuture<Rc<dyn ServiceCounterServer>> {
        Box::pin(async move {
            self.leases.set(
                self.leases
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| cordis_sdk_core::refused("capacity", "lease count exhausted"))?,
            );
            Ok(Rc::new(Counter {
                owner: self,
                value: Cell::new(args.initial.unwrap_or(0)),
            }) as Rc<dyn ServiceCounterServer>)
        })
    }
}
impl ServiceCounterServer for Counter {
    fn get(self: Rc<Self>, _: Context, _: Empty) -> LocalFuture<Int64Value> {
        Box::pin(async move {
            Ok(Int64Value {
                value: self.value.get(),
            })
        })
    }
    fn add(self: Rc<Self>, _: Context, args: Int64Value) -> LocalFuture<Int64Value> {
        Box::pin(async move {
            let value = add(self.value.get(), args.value)?;
            self.value.set(value);
            Ok(Int64Value { value })
        })
    }
    fn close(self: Rc<Self>, _: Context) -> LocalFuture<()> {
        Box::pin(async move {
            let releases = self
                .owner
                .releases
                .get()
                .checked_add(1)
                .ok_or_else(|| cordis_sdk_core::typed::violation("release count exhausted"))?;
            self.owner.leases.set(self.owner.leases.get() - 1);
            self.owner.releases.set(releases);
            Ok(())
        })
    }
}
