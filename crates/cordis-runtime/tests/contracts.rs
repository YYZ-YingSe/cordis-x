use cordis_contract::{Descriptor, Interface};
use cordis_runtime::{Plugin, Runtime, Service};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[tokio::test]
async fn binding_compares_structure_before_exposing_the_typed_service() {
    let runtime = Runtime::new();
    let declared = Descriptor::new(Interface::value_dispatch(false)).unwrap();
    let provided = Service::<u32>::new("test.service", "1").with_descriptor(declared.clone());
    let binding = provided.clone();
    let provider = runtime
        .install(
            Plugin::new("provider", move |ctx| {
                let binding = binding.clone();
                async move { ctx.provide(&binding, Arc::new(42)) }
            })
            .provides(&provided),
        )
        .unwrap();
    provider.ready().await.unwrap();
    for descriptor in [
        Some(declared),
        Some(Descriptor::new(Interface::value_dispatch(true)).unwrap()),
        None,
    ] {
        let matching = descriptor
            .as_ref()
            .is_some_and(|d| d == provided.descriptor().unwrap());
        let mut requested = Service::<u32>::new("test.service", "1");
        if let Some(descriptor) = descriptor {
            requested = requested.with_descriptor(descriptor);
        }
        let binding = requested.clone();
        let called = Arc::new(AtomicBool::new(false));
        let reached = called.clone();
        let consumer = runtime
            .install(
                Plugin::new("consumer", move |ctx| {
                    let binding = binding.clone();
                    let reached = reached.clone();
                    async move {
                        let dependency = ctx.dependency(&binding)?;
                        dependency
                            .call(move |value, _| {
                                Box::pin(async move {
                                    assert_eq!(*value, 42);
                                    reached.store(true, Ordering::SeqCst);
                                    Ok(())
                                })
                            })
                            .await
                    }
                })
                .requires(&requested),
            )
            .unwrap();
        assert_eq!(consumer.ready().await.is_ok(), matching);
        assert_eq!(called.load(Ordering::SeqCst), matching);
        consumer.dispose().await.unwrap();
    }
    runtime.shutdown().await.unwrap();
}
