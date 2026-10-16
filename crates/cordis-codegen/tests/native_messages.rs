#![allow(dead_code)]
use cordis_example_contracts::portable;
use cordis_sdk_core::{Message, Validation, typed::Codec};
use cordis_test_contracts::profile;
use portable::data::google::protobuf::StringValue;
use profile::data::profile::{
    Config, Fixed3, OptionBool, OptionOptionBool, User, option_bool, option_option_bool,
};
use prost::Message as _;

fn agrees<T: Codec>(value: T, expected: bool) {
    // Independent descriptor interpreter versus generated native field checks.
    let bytes = value.encode_to_vec();
    let reflected = T::contract()
        .with_validation(Validation::Audit)
        .parse_binary(&bytes)
        .is_ok();
    let native = Message::new(value)
        .unwrap()
        .validate(Validation::Audit)
        .is_ok();
    assert_eq!(native, expected);
    assert_eq!(reflected, expected);
}

#[test]
fn native_checks_cover_presence_bounds_enums_oneofs_and_nested_constraints() {
    agrees(Config { limit: None }, false);
    agrees(Config { limit: Some(0) }, false);
    agrees(Config { limit: Some(1) }, true);
    agrees(Config { limit: Some(100) }, true);
    agrees(Config { limit: Some(101) }, false);
    agrees(
        User {
            id: None,
            name: String::new(),
        },
        false,
    );
    agrees(
        User {
            id: Some(u64::MAX),
            name: "中文".into(),
        },
        true,
    );
    agrees(
        Fixed3 {
            values: vec![1., 2.],
        },
        false,
    );
    agrees(
        Fixed3 {
            values: vec![1., 2., 3.],
        },
        true,
    );
    agrees(
        Fixed3 {
            values: vec![1., 2., f64::INFINITY],
        },
        false,
    );
    agrees(OptionBool { state: None }, false);
    agrees(
        OptionBool {
            state: Some(option_bool::State::None(1)),
        },
        false,
    );
    agrees(
        OptionBool {
            state: Some(option_bool::State::None(0)),
        },
        true,
    );
    agrees(
        OptionOptionBool {
            state: Some(option_option_bool::State::Some(OptionBool { state: None })),
        },
        false,
    );
    agrees(
        OptionOptionBool {
            state: Some(option_option_bool::State::Some(OptionBool {
                state: Some(option_bool::State::Some(false)),
            })),
        },
        true,
    );
    agrees(portable::data::google::protobuf::Value::default(), false);
}

#[test]
fn native_message_moves_string_storage_and_fanout_is_an_immutable_snapshot() {
    let text = "\0\\\"中文".repeat(32768);
    let pointer = text.as_ptr();
    let message = Message::new(StringValue { value: text }).unwrap();
    let value: StringValue = message.into_typed().unwrap();
    assert_eq!(value.value.as_ptr(), pointer);
    let message = Message::new(value).unwrap();
    let copy = message.clone();
    let mut changed: StringValue = message.into_typed().unwrap();
    changed.value.clear();
    let original: StringValue = copy.into_typed().unwrap();
    assert!(!original.value.is_empty());
    assert_eq!(original.value.as_ptr(), pointer);
}

#[test]
fn receiving_policy_checks_at_consumption_and_audits_unknown_fields() {
    let mut bytes = StringValue {
        value: "value".into(),
    }
    .encode_to_vec();
    // Unknown field 2, varint 1. A standard generated decoder would drop it.
    bytes.extend([0x10, 1]);
    assert!(
        Message::from_bytes(
            StringValue::contract().with_validation(Validation::Audit),
            bytes.into()
        )
        .unwrap()
        .into_typed::<StringValue>()
        .is_err()
    );
    let invalid = Config { limit: Some(101) }.encode_to_vec();
    assert!(
        Message::from_bytes(
            Config::contract().with_validation(Validation::Checked),
            invalid.into()
        )
        .unwrap()
        .into_typed::<Config>()
        .is_err()
    );
}

#[test]
fn native_logical_limit_and_foreign_encoded_limit_are_separate() {
    let value = StringValue {
        value: "\0".repeat(cordis_contract::budget::MAX_BYTES),
    };
    let message = Message::new(value).unwrap();
    assert!(message.encode(cordis_contract::budget::MAX_BYTES).is_err());
    let bytes = message
        .encode(cordis_contract::budget::MAX_BYTES + 8)
        .unwrap();
    assert!(Message::from_bytes(StringValue::contract().clone(), bytes).is_err());
    let native: StringValue = message.into_typed().unwrap();
    assert_eq!(native.value.len(), cordis_contract::budget::MAX_BYTES);
}

struct Echo;
impl portable::PortableLogServer for Echo {
    fn create(_: portable::data::cordis::data::Unit) -> cordis_sdk_core::Result<Self> {
        Ok(Self)
    }
    fn write(
        self: std::rc::Rc<Self>,
        _: cordis_sdk_core::Context,
        args: portable::data::google::protobuf::Value,
    ) -> cordis_sdk_core::LocalFuture<portable::data::google::protobuf::Value> {
        Box::pin(async move { Ok(args) })
    }
}

#[tokio::test]
async fn generated_native_server_moves_the_original_string_through_the_worker() {
    use cordis_sdk as rt;
    use cordis_sdk_core::Plugin as _;
    use cordis_sdk_runtime as native;
    use portable::data::google::protobuf::{Value, value::Kind};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    type Export = portable::PortableLogExport<Echo>;
    let runtime = rt::Runtime::new();
    let service = rt::Service::<native::Service>::new("example.portable-log", "1")
        .with_descriptor(cordis_sdk_core::Descriptor::new(Export::interface()).unwrap());
    runtime
        .install(
            native::plugin::<Export>(
                "echo",
                &service,
                cordis_sdk_core::json!({}),
                Default::default(),
            )
            .unwrap(),
        )
        .unwrap()
        .ready()
        .await
        .unwrap();
    let observed = Arc::new(AtomicUsize::new(0));
    let output = observed.clone();
    let dependency = service.clone();
    runtime
        .install(
            rt::Plugin::new("driver", move |ctx| {
                let dependency = dependency.clone();
                let output = output.clone();
                async move {
                    let text = "\0\\\"中文".repeat(32768);
                    let pointer = text.as_ptr() as usize;
                    let args = Message::new(Value {
                        kind: Some(Kind::StringValue(text)),
                    })
                    .map_err(rt::Error::Message)?;
                    let message = ctx
                        .dependency(&dependency)?
                        .call(move |s, ctx| {
                            Box::pin(async move {
                                s.invoke(&ctx, "write", args)
                                    .await
                                    .map_err(|e| rt::Error::Message(e.to_string()))
                            })
                        })
                        .await?;
                    let value: Value = message.into_typed().map_err(rt::Error::Message)?;
                    let Some(Kind::StringValue(text)) = value.kind else {
                        panic!("changed type")
                    };
                    assert_eq!(text.as_ptr() as usize, pointer);
                    output.store(text.len(), Ordering::SeqCst);
                    Ok(())
                }
            })
            .requires(&service),
        )
        .unwrap()
        .ready()
        .await
        .unwrap();
    assert!(observed.load(Ordering::SeqCst) > 100_000);
    runtime.shutdown().await.unwrap();
}

#[test]
fn typed_audit_checks_original_discarded_fields_and_declared_constraints() {
    use portable::data::google::protobuf::Value;
    for raw in [
        vec![42, 11, 10, 9, 10, 1, b'k', 18, 2, 8, 0, 120, 120],
        vec![42, 2, 120, 120, 8, 0],
    ] {
        let message = Message::from_bytes(Value::contract().clone(), raw.into()).unwrap();
        assert!(
            message
                .clone()
                .with_validation(Validation::Checked)
                .into_typed::<Value>()
                .is_ok()
        );
        assert!(
            message
                .with_validation(Validation::Audit)
                .into_typed::<Value>()
                .unwrap_err()
                .contains("undeclared")
        );
    }
    let raw = Config { limit: Some(101) }.encode_to_vec();
    assert!(
        Message::from_bytes(
            Config::contract().with_validation(Validation::Audit),
            raw.into()
        )
        .unwrap()
        .into_typed::<Config>()
        .is_err()
    );
}
