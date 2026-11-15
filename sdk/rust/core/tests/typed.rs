use cordis_sdk_core::{
    Failure, Message, MessageContract, Value, json, message,
    typed::{Budget, Codec, Walk},
};
use std::sync::LazyLock;

#[derive(Clone, PartialEq, prost::Message)]
struct Text {
    #[prost(string, tag = "1")]
    value: String,
}
impl Walk for Text {
    fn walk(&self, depth: usize, budget: &mut Budget) -> Result<(), String> {
        budget.take(depth)?;
        self.value.walk(depth + 1, budget)
    }
}
impl Codec for Text {
    fn contract() -> &'static MessageContract {
        static SHAPE: LazyLock<MessageContract> =
            LazyLock::new(|| MessageContract::standard("google.protobuf.StringValue"));
        &SHAPE
    }
    fn decode(_: Value) -> Result<Self, String> {
        panic!("a custom codec cannot replace boundary validation and conversion")
    }
}

#[test]
fn typed_boundaries_validate_and_convert_even_with_a_custom_codec() {
    let text = "\0\"\\\n中文🦀".repeat(100);
    assert_eq!(
        cordis_sdk_core::json::input::<Text>(json!(text), Text::contract())
            .unwrap()
            .value,
        text
    );
    assert_eq!(
        message::result::<Text>(
            Message::from_json(Text::contract().clone(), &json!(text)).unwrap(),
            Text::contract()
        )
        .unwrap()
        .value,
        text
    );
    for value in [json!(null), json!({"wrong": true}), json!(42)] {
        assert!(
            matches!(cordis_sdk_core::json::input::<Text>(value.clone(), Text::contract()),
            Err(Failure::Refused(e)) if e.code == "invalid_argument" && e.message.starts_with("args: "))
        );
        assert!(
            matches!(cordis_sdk_core::json::message(Text::contract(), &value),
            Err(Failure::Unknown(e)) if e.code == "contract_violation")
        );
    }
}

#[test]
fn contract_identity_and_output_budgets_still_apply() {
    let wrong = MessageContract::standard("google.protobuf.BoolValue");
    assert!(
        matches!(cordis_sdk_core::json::input::<Text>(json!("ok"), &wrong),
        Err(Failure::Refused(e)) if e.code == "invalid_argument" && e.message.contains("mismatch"))
    );
    assert!(
        matches!(message::result::<Text>(Message::new(Text { value: "ok".into() }).unwrap(), &wrong),
        Err(Failure::Unknown(e)) if e.code == "contract_violation" && e.message.contains("mismatch"))
    );
    let large = Message::new(Text {
        value: "\0".repeat(2 * 1024 * 1024),
    })
    .unwrap();
    assert!(
        large
            .validate(cordis_sdk_core::Validation::Contract)
            .is_ok()
    );
    assert!(large.validate(cordis_sdk_core::Validation::Checked).is_ok());
    assert!(large.encode(Some(1024 * 1024)).is_err());
    assert!(large.encode(None).unwrap().len() > 1024 * 1024);
    assert!(large.to_json().is_ok());
}

#[test]
fn foreign_event_results_reject_unknown_tags_and_missing_break_payloads() {
    use cordis_sdk_core::{json::tagged_flow, typed::Flow};
    let shape = MessageContract::standard("google.protobuf.Value");
    for value in [
        json!(null),
        json!({}),
        json!({"kind":"typo"}),
        json!({"kind":"break"}),
        json!({"kind":"continue","value":null}),
        json!({"kind":"break","value":null,"extra":true}),
    ] {
        assert!(matches!(
            tagged_flow(&shape, value),
            Err(Failure::Unknown(_))
        ));
    }
    assert!(matches!(
        tagged_flow(&shape, json!({"kind":"continue"})).unwrap(),
        Flow::Continue
    ));
    let Flow::Break(value) = tagged_flow(&shape, json!({"kind":"break","value":null})).unwrap()
    else {
        panic!("lost Break");
    };
    assert_eq!(value.to_json().unwrap(), json!(null));
}
