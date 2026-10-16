use cordis_sdk_core::{
    Message, MessageContract, Validation,
    typed::{Budget, Codec, Walk},
};
use prost::Message as _;
use std::sync::{
    LazyLock,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone, PartialEq, prost::Message)]
struct Text {
    #[prost(string, tag = "1")]
    value: String,
}
static WALKS: AtomicUsize = AtomicUsize::new(0);
impl Walk for Text {
    fn walk(&self, depth: usize, budget: &mut Budget) -> Result<(), String> {
        WALKS.fetch_add(1, Ordering::Relaxed);
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
}
#[test]
fn policy_consumption_and_immutable_snapshot_proofs() {
    let before = WALKS.load(Ordering::Relaxed);
    let value = Message::new(Text {
        value: "hello".into(),
    })
    .unwrap();
    value.clone().into_typed::<Text>().unwrap();
    assert_eq!(
        WALKS.load(Ordering::Relaxed),
        before,
        "contract is a promise, not a hidden check"
    );
    value
        .check(&Text::contract().with_validation(Validation::Checked))
        .unwrap();
    value.validate(Validation::Checked).unwrap();
    value
        .clone()
        .with_validation(Validation::Audit)
        .into_typed::<Text>()
        .unwrap();
    assert_eq!(
        WALKS.load(Ordering::Relaxed),
        before + 1,
        "same immutable native snapshot reuses field proof"
    );
    let mut changed = value.into_typed::<Text>().unwrap();
    changed.value.push('!');
    Message::new(changed)
        .unwrap()
        .validate(Validation::Checked)
        .unwrap();
    assert_eq!(
        WALKS.load(Ordering::Relaxed),
        before + 2,
        "new owned value has no stale proof"
    );

    let bytes = Text {
        value: "你好".into(),
    }
    .encode_to_vec();
    let body = Message::from_bytes(
        Text::contract().with_validation(Validation::Checked),
        bytes.clone().into(),
    )
    .unwrap();
    assert_eq!(
        WALKS.load(Ordering::Relaxed),
        before + 2,
        "owning bytes is not decoding"
    );
    assert_eq!(body.into_typed::<Text>().unwrap().value, "你好");
    assert_eq!(WALKS.load(Ordering::Relaxed), before + 3);

    let mut unknown = bytes;
    unknown.extend([0x78, 1]);
    for policy in [Validation::Contract, Validation::Checked] {
        let body = Message::from_bytes(
            Text::contract().with_validation(policy),
            unknown.clone().into(),
        )
        .unwrap();
        assert_eq!(body.into_typed::<Text>().unwrap().value, "你好");
    }
    let audited = Message::from_bytes(
        Text::contract().with_validation(Validation::Audit),
        unknown.into(),
    )
    .unwrap();
    assert!(
        audited
            .into_typed::<Text>()
            .unwrap_err()
            .contains("undeclared")
    );
    for policy in [Validation::Contract, Validation::Checked, Validation::Audit] {
        let malformed = Message::from_bytes(
            Text::contract().with_validation(policy),
            vec![0x0a, 1, 0xff].into(),
        )
        .unwrap();
        assert!(
            malformed.into_typed::<Text>().is_err(),
            "standard decoder always constructs safe strings"
        );
        assert!(
            Message::from_bytes(
                Text::contract().with_validation(policy),
                vec![0; cordis_contract::budget::MAX_BYTES + 1].into()
            )
            .is_err()
        );
    }
}
#[test]
fn policy_is_local_binding_state_and_never_descriptor_authority() {
    let shape = Text::contract();
    let audit = shape.with_validation(Validation::Audit);
    assert_eq!(shape, &audit);
    let serialized = serde_json::to_value(&audit).unwrap();
    assert!(serialized.get("validation").is_none());
    let unbound: MessageContract = serde_json::from_value(serialized).unwrap();
    assert_eq!(unbound.validation(), Validation::Contract);
    assert_eq!(Validation::default(), Validation::Checked);
}
