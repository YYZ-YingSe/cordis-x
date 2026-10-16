use cordis_contract::portable as contract;
use cordis_contract::{Descriptor, Interface, MessageContract};
use cordis_wire::{Payload, json};

#[test]
fn schema_identity_survives_protobuf_without_hashing_wire_bytes() {
    let original = Descriptor::new(Interface::value_dispatch(true)).unwrap();
    let bytes = Payload::from(contract::encode(&original)).encode().unwrap();
    let value = Payload::decode(bytes).unwrap().control_only().unwrap();
    let actual = contract::decode(&value).unwrap();
    original.compare(&actual).unwrap();
    assert_eq!(original.digest(), actual.digest());
}

#[test]
fn protojson_64_bit_integers_are_exact_strings() {
    let message = MessageContract::standard("google.protobuf.Int64Value");
    for value in [json!("-9223372036854775808"), json!("9223372036854775807")] {
        contract::value(&message, &value, "args").unwrap();
        let bytes = Payload::from(value.clone()).encode().unwrap();
        assert_eq!(
            Payload::decode(bytes).unwrap().control_only().unwrap(),
            value
        );
    }
    for value in [json!("9223372036854775808"), json!(true), json!(1.5)] {
        assert!(contract::value(&message, &value, "args").is_err());
    }
}

#[test]
fn missing_unknown_and_duplicate_declarations_are_rejected() {
    assert!(contract::decode(&json!(null)).is_err());
    let descriptor = Descriptor::new(Interface::value_dispatch(false)).unwrap();
    let mut value = contract::encode(&descriptor);
    value["capabilities"] = json!(["clock", "clock"]);
    assert!(contract::decode(&value).is_err());
    value["capabilities"] = json!([]);
    value["legacy"] = json!(true);
    assert!(contract::decode(&value).is_err());
    assert!(contract::resource(&descriptor, "counter").is_err());
}
