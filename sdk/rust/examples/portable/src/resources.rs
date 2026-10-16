//! Typed resources shared unchanged by the native, process, ABI and Wasm entries.
pub use cordis_example_contracts::counters as generated;
use cordis_sdk_core::typed::{Codec, violation};
use cordis_sdk_core::{Context, LocalFuture, Result, error};
use generated::{
    CounterHolderDependencies, CounterHolderExport, CounterHolderServer, CountersCounterClient,
    CountersCounterServer, CountersDependencies, CountersExport, CountersServer,
    data::{
        cordis::data::Unit,
        fixtures::counters::CounterClosed,
        google::protobuf::{Empty, Int64Value},
    },
};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

pub struct Counters;
pub type CounterPlugin = CountersExport<Counters>;
struct Counter(Cell<i64>);
impl CountersServer for Counters {
    fn create(_: Unit) -> Result<Self> {
        Ok(Self)
    }
    fn counter(
        self: Rc<Self>,
        _: Context,
        args: Int64Value,
    ) -> LocalFuture<Rc<dyn CountersCounterServer>> {
        Box::pin(async move {
            Ok(Rc::new(Counter(Cell::new(args.value))) as Rc<dyn CountersCounterServer>)
        })
    }
}
impl CountersCounterServer for Counter {
    fn get(self: Rc<Self>, _: Context, _: Empty) -> LocalFuture<Int64Value> {
        Box::pin(async move {
            Ok(Int64Value {
                value: self.0.get(),
            })
        })
    }
    fn add(self: Rc<Self>, _: Context, args: Int64Value) -> LocalFuture<Int64Value> {
        Box::pin(async move {
            let value = self
                .0
                .get()
                .checked_add(args.value)
                .ok_or_else(|| error("overflow", "counter overflow"))?;
            self.0.set(value);
            Ok(Int64Value { value })
        })
    }
    fn close(self: Rc<Self>, ctx: Context) -> LocalFuture<()> {
        Box::pin(async move {
            CountersDependencies::logger()
                .write(
                    &ctx,
                    CounterClosed {
                        counter_closed: Some(self.0.get()),
                    },
                )
                .await?;
            Ok(())
        })
    }
}
/// A saved typed handle still belongs to the callback that acquired it.
pub struct Holder(RefCell<Option<CountersCounterClient>>);
pub type CounterClient = CounterHolderExport<Holder>;
impl Holder {
    fn saved(&self) -> Result<CountersCounterClient> {
        self.0
            .borrow()
            .clone()
            .ok_or_else(|| error("empty", "acquire a counter first"))
    }
}
impl CounterHolderServer for Holder {
    fn create(_: Unit) -> Result<Self> {
        Ok(Self(RefCell::default()))
    }
    fn acquire(self: Rc<Self>, ctx: Context, args: Int64Value) -> LocalFuture<Empty> {
        Box::pin(async move {
            let counter = CounterHolderDependencies::counters()
                .counter(&ctx, args)
                .await?;
            *self.0.borrow_mut() = Some(counter);
            Ok(Empty {})
        })
    }
    fn get(self: Rc<Self>, ctx: Context, args: Empty) -> LocalFuture<Int64Value> {
        Box::pin(async move { self.saved()?.get(&ctx, args).await })
    }
    fn add(self: Rc<Self>, ctx: Context, args: Int64Value) -> LocalFuture<Int64Value> {
        Box::pin(async move { self.saved()?.add(&ctx, args).await })
    }
    fn release(self: Rc<Self>, ctx: Context, _: Empty) -> LocalFuture<Empty> {
        Box::pin(async move {
            self.saved()?.dispose(&ctx).await?;
            Ok(Empty {})
        })
    }
    fn scoped(self: Rc<Self>, ctx: Context, args: Int64Value) -> LocalFuture<Int64Value> {
        Box::pin(async move {
            let counter = CounterHolderDependencies::counters()
                .counter(&ctx, args)
                .await?;
            let scope = ctx.scope("counter scope").await?;
            let saved = counter.clone();
            let result = scope
                .run(&ctx, move |inner| {
                    let saved = saved.clone();
                    async move {
                        let cleanup = saved.clone();
                        inner
                            .defer("read before close", move |ctx| {
                                let cleanup = cleanup.clone();
                                async move {
                                    cleanup.get(&ctx, Empty {}).await?;
                                    Ok(())
                                }
                            })
                            .await?;
                        saved
                            .get(&inner, Empty {})
                            .await?
                            .encode()
                            .map_err(violation)
                    }
                })
                .await?;
            scope.dispose(&ctx).await?;
            counter.dispose(&ctx).await?;
            Int64Value::decode(result).map_err(violation)
        })
    }
}
