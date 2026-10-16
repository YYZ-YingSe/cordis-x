use cordis_component::{Bytes, MAX_BYTES, Payload, decode, encode};
use cordis_wire::json;
#[test]
fn canonical_byte_list_preserves_control_and_opaque_business_bodies() {
    let payload = Payload {
        control: json!({"owner":"中文","sequence":42}),
        messages: vec![Bytes::from((0..=255).collect::<Vec<u8>>()), Bytes::new()],
    };
    assert_eq!(decode(encode(&payload).unwrap()).unwrap(), payload);
}
#[test]
fn rejects_malformed_missing_and_oversized_carriers() {
    for bytes in [vec![], vec![255], vec![0], vec![0; MAX_BYTES + 1]] {
        assert!(decode(bytes).is_err());
    }
    assert!(encode(&Payload::message(Bytes::from(vec![0; MAX_BYTES]))).is_err());
    assert!(Payload::from(json!({})).one().is_err());
    assert!(Payload::message(Bytes::new()).control_only().is_err());
}
#[test]
fn metadata_limits_do_not_interpret_business_bytes() {
    let mut value = json!(null);
    for _ in 0..cordis_wire::binary::MAX_CONTROL_DEPTH {
        value = json!([value]);
    }
    assert!(encode(&value.clone().into()).is_ok());
    assert!(encode(&json!([value]).into()).is_err());
    let body = Bytes::from(vec![255; 256 * 1024]);
    assert_eq!(
        decode(encode(&Payload::message(body.clone())).unwrap())
            .unwrap()
            .one()
            .unwrap(),
        body
    );
}

#[test]
fn carrier_audit_rejects_envelope_fields_without_interpreting_business_bytes() {
    use cordis_component::{Validation, decode_with};
    let payload = Payload::message(Bytes::from_static(&[0xff, 0, 0x78, 1]));
    let normal = encode(&payload).unwrap();
    let mut unknown = normal.clone();
    unknown.extend([0x78, 1]);
    for policy in [Validation::Contract, Validation::Checked, Validation::Audit] {
        assert_eq!(decode_with(normal.clone(), policy).unwrap(), payload);
        let result = decode_with(unknown.clone(), policy);
        if policy.audits() {
            assert!(result.is_err());
        } else {
            assert_eq!(result.unwrap(), payload);
        }
        assert!(decode_with(vec![0xff], policy).is_err());
    }
}
