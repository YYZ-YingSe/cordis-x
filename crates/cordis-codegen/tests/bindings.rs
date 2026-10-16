#![allow(dead_code)]
use cordis_example_contracts::examples as generated;
use cordis_example_contracts::native_shared as shared;
use cordis_sdk_core::{self as core, Plugin as _, typed::Codec};
use cordis_test_contracts::profile;
use generated::data::{
    cordis::data::{Int64, Unit},
    example::{AddArgs, CounterArgs},
};
use generated::*;
use std::{
    cell::Cell,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
fn number(n: i64) -> Int64 {
    Int64 { value: Some(n) }
}
struct Empty;
impl core::backend::Backend for Empty {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn submit(&self, _: core::backend::Operation) -> core::LocalFuture<core::backend::Reply> {
        Box::pin(async { Err(core::refused("unbound", "no grants")) })
    }
}
fn context() -> core::Context {
    core::Context::from_backend(Rc::new(Empty))
}
struct Adder;
impl AdderServer for Adder {
    fn create(_: Unit) -> core::Result<Self> {
        Ok(Self)
    }
    fn add(self: Rc<Self>, _: core::Context, args: AddArgs) -> core::LocalFuture<Int64> {
        Box::pin(async move {
            Ok(number(
                args.left
                    .unwrap()
                    .checked_add(args.right.unwrap())
                    .ok_or_else(|| AdderAddError::Overflow.failure("overflow"))?,
            ))
        })
    }
}
// This independently emitted Server imports the shared generated DTO package.
// Its caller below uses the original generated client and the same Rust types.
impl shared::AdderServer for Adder {
    fn create(config: Unit) -> core::Result<Self> {
        <Self as AdderServer>::create(config)
    }
    fn add(self: Rc<Self>, ctx: core::Context, args: AddArgs) -> core::LocalFuture<Int64> {
        <Self as AdderServer>::add(self, ctx, args)
    }
}
#[tokio::test(flavor = "current_thread")]
async fn validates_input_before_business_and_preserves_error_categories() {
    assert!(AdderExport::<Adder>::create(core::json!(true)).is_err());
    let service = Rc::new(AdderExport::<Adder>::create(core::json!({})).unwrap());
    let call = |args| core::json::invoke(service.clone(), context(), "add", args);
    assert_eq!(
        call(core::json!({"left":"20","right":"22"})).await.unwrap(),
        core::json!({"value":"42"})
    );
    assert!(matches!(
        call(core::json!({"left":"20"})).await,
        Err(core::Failure::Refused(_))
    ));
    assert!(
        matches!(call(core::json!({"left":"9223372036854775807","right":"1"})).await,Err(core::Failure::Rejected(e))if e.code=="overflow")
    );
    assert!(
        matches!(AdderClient::default().add(&context(),AddArgs{left:Some(20),right:Some(22)}).await,Err(core::Failure::Refused(e))if e.code=="unbound")
    );
}
static CLOSED: AtomicUsize = AtomicUsize::new(0);
struct Counter;
struct CounterObject(Cell<i64>);
impl CounterCounterServer for CounterObject {
    fn add(self: Rc<Self>, _: core::Context, args: Int64) -> core::LocalFuture<Int64> {
        Box::pin(async move {
            let n = self
                .0
                .get()
                .checked_add(args.value.unwrap())
                .ok_or_else(|| CounterCounterAddError::Overflow.failure("overflow"))?;
            self.0.set(n);
            Ok(number(n))
        })
    }
    fn get(self: Rc<Self>, _: core::Context, _: Unit) -> core::LocalFuture<Int64> {
        Box::pin(async move { Ok(number(self.0.get())) })
    }
    fn close(self: Rc<Self>, _: core::Context) -> core::LocalFuture<()> {
        Box::pin(async {
            CLOSED.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}
impl CounterServer for Counter {
    fn create(_: Unit) -> core::Result<Self> {
        Ok(Self)
    }
    fn counter(
        self: Rc<Self>,
        _: core::Context,
        args: CounterArgs,
    ) -> core::LocalFuture<Rc<dyn CounterCounterServer>> {
        Box::pin(async move {
            Ok(Rc::new(CounterObject(Cell::new(args.initial.unwrap())))
                as Rc<dyn CounterCounterServer>)
        })
    }
}
struct Consumer;
impl ConsumerServer for Consumer {
    fn create(_: Unit) -> core::Result<Self> {
        Ok(Self)
    }
    fn run(self: Rc<Self>, ctx: core::Context, args: AddArgs) -> core::LocalFuture<Int64> {
        Box::pin(async move {
            let sum = ConsumerDependencies::adder().add(&ctx, args).await?;
            let counter = ConsumerDependencies::counters()
                .counter(&ctx, CounterArgs { initial: sum.value })
                .await?;
            let copy = counter.clone();
            assert_eq!(counter.get(&ctx, Unit {}).await?, sum);
            let result = counter.add(&ctx, number(1)).await?;
            counter.dispose(&ctx).await?;
            copy.dispose(&ctx).await?;
            assert!(matches!(
                copy.get(&ctx, Unit {}).await,
                Err(core::Failure::Refused(_))
            ));
            Ok(result)
        })
    }
}
#[test]
fn native_binding_rejects_dependency_shape_drift_before_starting_business() {
    use cordis_sdk as rt;
    use cordis_sdk_runtime as adapter;
    let consumer = rt::Service::<adapter::Service>::new("example.consumer", "1")
        .with_descriptor(core::Descriptor::new(ConsumerClient::interface()).unwrap());
    let adder = rt::Service::<()>::new("example.adder", "1");
    let counter = rt::Service::<adapter::Service>::new("example.counter", "1")
        .with_descriptor(core::Descriptor::new(CounterClient::interface()).unwrap());
    let drift = adapter::Dependency::new(
        &adder,
        core::Descriptor::new(core::Interface::value_dispatch(false)).unwrap(),
        |_, _, _, _| Box::pin(async { panic!("drifted dependency must not run") }),
    );
    let result = adapter::plugin::<ConsumerExport<Consumer>>(
        "consumer",
        &consumer,
        core::json!({}),
        adapter::Capabilities {
            dependencies: vec![drift, adapter::Dependency::portable(&counter).unwrap()],
            ..Default::default()
        },
    );
    assert!(
        matches!(result,Err(core::Failure::Refused(e)) if e.code=="contract_mismatch" && e.message.contains("example.adder"))
    );
    assert!(adapter::Dependency::portable(&rt::Service::new("untyped", "1")).is_err());
}
#[tokio::test]
async fn generated_clients_and_resources_run_on_the_actual_native_runtime() {
    use cordis_sdk as rt;
    use cordis_sdk_runtime as adapter;
    fn declaration<P: core::Plugin>() -> rt::Service<adapter::Service> {
        let c = P::manifest().provides;
        rt::Service::new(c.name, c.revision)
            .with_descriptor(core::Descriptor::new(P::interface()).unwrap())
    }
    let runtime = rt::Runtime::new();
    let adder = declaration::<AdderExport<Adder>>();
    let counter = declaration::<CounterExport<Counter>>();
    let consumer = declaration::<ConsumerExport<Consumer>>();
    let a = runtime
        .install(
            adapter::plugin::<shared::AdderExport<Adder>>(
                "adder",
                &adder,
                core::json!({}),
                Default::default(),
            )
            .unwrap(),
        )
        .unwrap();
    a.ready().await.unwrap();
    let c = runtime
        .install(
            adapter::plugin::<CounterExport<Counter>>(
                "counter",
                &counter,
                core::json!({}),
                Default::default(),
            )
            .unwrap(),
        )
        .unwrap();
    c.ready().await.unwrap();
    let p = runtime
        .install(
            adapter::plugin::<ConsumerExport<Consumer>>(
                "consumer",
                &consumer,
                core::json!({}),
                adapter::Capabilities {
                    dependencies: vec![
                        adapter::Dependency::portable(&adder).unwrap(),
                        adapter::Dependency::portable(&counter).unwrap(),
                    ],
                    ..Default::default()
                },
            )
            .unwrap(),
        )
        .unwrap();
    p.ready().await.unwrap();
    let observed = Arc::new(AtomicUsize::new(0));
    let output = observed.clone();
    let dep = consumer.clone();
    let driver = runtime
        .install(
            rt::Plugin::new("driver", move |ctx| {
                let dep = dep.clone();
                let output = output.clone();
                async move {
                    let result = ctx
                        .dependency(&dep)?
                        .call(|value, ctx| {
                            Box::pin(async move {
                                value
                                    .invoke_json(&ctx, "run", core::json!({"left":20,"right":22}))
                                    .await
                                    .map_err(|e| rt::Error::Message(e.to_string()))
                            })
                        })
                        .await?;
                    output.store(
                        result["value"].as_str().unwrap().parse::<usize>().unwrap(),
                        Ordering::SeqCst,
                    );
                    Ok(())
                }
            })
            .requires(&consumer),
        )
        .unwrap();
    driver.ready().await.unwrap();
    assert_eq!(observed.load(Ordering::SeqCst), 43);
    assert_eq!(CLOSED.load(Ordering::SeqCst), 1);
    runtime.shutdown().await.unwrap();
    assert_eq!(CLOSED.load(Ordering::SeqCst), 1);
}
#[test]
fn nested_option_retains_every_branch_and_constraints_apply() {
    use profile::data::profile::{Data, OptionOptionBool};
    for state in [
        core::json!({"none":"NULL_VALUE"}),
        core::json!({"some":{"none":"NULL_VALUE"}}),
        core::json!({"some":{"some":false}}),
        core::json!({"some":{"some":true}}),
    ] {
        let decoded = <OptionOptionBool as Codec>::decode(state.clone()).unwrap();
        assert_eq!(decoded.encode().unwrap(), state);
        assert_eq!(
            <OptionOptionBool as Codec>::decode(decoded.encode().unwrap()).unwrap(),
            decoded
        );
    }
    for invalid in [
        core::json!({}),
        core::json!({"some":{}}),
        core::json!({"some":false}),
        core::json!({"none":"NULL_VALUE","some":{"none":"NULL_VALUE"}}),
    ] {
        assert!(<OptionOptionBool as Codec>::decode(invalid).is_err());
    }
    let value = core::json!({"int32Value":0,"option":{"some":{"some":false}},"int64Value":"-9223372036854775808","uint64Value":"18446744073709551615","float32Value":1.25,"float64Value":-1.5,"binary":"AAH/","position":{"values":[1,2,3]},"tree":{"children":[{}]}});
    let native = <Data as Codec>::decode(value).unwrap();
    assert_eq!(
        <Data as Codec>::decode(native.clone().encode().unwrap()).unwrap(),
        native
    );
    let mut invalid = native;
    invalid.position.as_mut().unwrap().values.pop();
    assert!(invalid.encode().is_err());
}
