//! Opaque-body benchmark fixture: two real plugins joined by a host dependency.
//! Deliberately raw dispatch, so both SDKs and hosts apply their selected policy.
use cordis_sdk_core::*;
use std::{collections::BTreeMap, rc::Rc};

pub struct Forwarding<const FORWARD: bool>;
pub fn interface() -> Interface {
    let methods = [
        ("text", "google.protobuf.StringValue"),
        ("bytes", "google.protobuf.BytesValue"),
        ("tree", "google.protobuf.Value"),
    ]
    .into_iter()
    .map(|(name, message)| {
        let shape = MessageContract::standard(message);
        (name.into(), Method::new(shape.clone(), shape))
    })
    .collect();
    Interface {
        methods: Dispatch::Named(methods),
        resources: Dispatch::Named(BTreeMap::new()),
        controls: BTreeMap::new(),
        capabilities: Default::default(),
    }
}
impl<const FORWARD: bool> Plugin for Forwarding<FORWARD> {
    fn manifest() -> PluginManifest {
        let endpoint = |name: &str| Contract {
            name: name.into(),
            revision: "1".into(),
        };
        PluginManifest {
            provides: endpoint(if FORWARD {
                "benchmark.forward"
            } else {
                "benchmark.echo"
            }),
            requires: if FORWARD {
                vec![endpoint("benchmark.echo")]
            } else {
                vec![]
            },
        }
    }
    fn interface() -> Interface {
        interface()
    }
    fn dependency_interfaces() -> BTreeMap<String, Interface> {
        if FORWARD {
            [("benchmark.echo".into(), interface())].into()
        } else {
            BTreeMap::new()
        }
    }
    fn create(_: Value) -> Result<Self> {
        Ok(Self)
    }
    fn invoke(
        self: Rc<Self>,
        ctx: Context,
        method: String,
        args: Message,
    ) -> message::FutureResult {
        Box::pin(async move {
            if FORWARD {
                let result = args.contract().clone();
                ctx.invoke("benchmark.echo", &method, args, &result).await
            } else {
                Ok(args)
            }
        })
    }
}
