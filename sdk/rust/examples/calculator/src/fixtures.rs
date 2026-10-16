use super::{ErrorInfo, Failure};

pub fn interface() -> cordis_contract::Interface {
    use cordis_contract::{Dispatch, Interface, MessageContract, Method, Resource};
    use std::collections::BTreeMap;
    let method = |args, result| Method {
        args,
        result,
        errors: Default::default(),
    };
    let fallible = || Method {
        args: MessageContract::standard("google.protobuf.Int64Value"),
        result: MessageContract::standard("google.protobuf.Int64Value"),
        errors: ["expected".into()].into(),
    };
    // Deliberately misdeclare stats to exercise successful-result violations.
    Interface {
        controls: Default::default(),
        methods: Dispatch::Named(BTreeMap::from([
            (
                "echo".into(),
                method(
                    MessageContract::standard("google.protobuf.Int64Value"),
                    MessageContract::standard("google.protobuf.Int64Value"),
                ),
            ),
            ("error".into(), fallible()),
            (
                "stats".into(),
                method(
                    MessageContract::standard("google.protobuf.Empty"),
                    MessageContract::standard("google.protobuf.Int64Value"),
                ),
            ),
        ])),
        resources: Dispatch::Named(BTreeMap::from([(
            "counter".into(),
            Resource {
                args: {
                    let descriptor = cordis_contract::portable::decode(
                        &cordis_test_contracts::resources::rust_fixture::SERVICE
                            .parse::<super::Value>()
                            .unwrap(),
                    )
                    .unwrap();
                    descriptor
                        .interface()
                        .resources
                        .get("counter")
                        .unwrap()
                        .args
                        .clone()
                },
                methods: Dispatch::Named(BTreeMap::from([
                    (
                        "get".into(),
                        method(
                            MessageContract::standard("google.protobuf.Empty"),
                            MessageContract::standard("google.protobuf.Int64Value"),
                        ),
                    ),
                    ("error".into(), fallible()),
                    (
                        "add".into(),
                        method(
                            MessageContract::standard("google.protobuf.Int64Value"),
                            MessageContract::standard("google.protobuf.Int64Value"),
                        ),
                    ),
                ])),
                errors: ["expected".into()].into(),
                ownership: "caller".into(),
                release: "drain_once".into(),
            },
        )])),
        capabilities: Default::default(),
    }
}

pub fn failure(mode: i64) -> Failure {
    let info = ErrorInfo {
        code: if mode == -2 || mode == -3 {
            "closed"
        } else {
            "expected"
        }
        .into(),
        message: "error contract probe".into(),
    };
    match mode {
        -3 => Failure::Refused(info),
        -4 => Failure::Unknown(info),
        _ => Failure::Rejected(info),
    }
}
