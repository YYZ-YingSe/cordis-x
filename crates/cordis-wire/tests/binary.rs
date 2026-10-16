use cordis_wire::{
    Bytes, Identity, Incoming, MAX_MESSAGE, Outcome, Payload, RemoteError, Request, decode, json,
};
fn request(payload: Payload) -> Request {
    Request {
        session: "a".repeat(32),
        activation: Identity {
            incarnation: "1".into(),
            fiber: "2".into(),
            epoch: "3".into(),
        },
        operation: "4".into(),
        command: "call".into(),
        payload,
    }
}
#[test]
fn opaque_bodies_preserve_all_bytes_including_empty_and_are_owned() {
    let bytes = Bytes::from((0..=255).collect::<Vec<u8>>());
    let mut payload = Payload::with_message(json!({"method":"echo"}), bytes.clone());
    payload.messages.push(Bytes::new());
    let expected = request(payload);
    let encoded = expected.encode().unwrap();
    let Incoming::Request(actual) = decode(encoded.clone()).unwrap() else {
        panic!("request expected")
    };
    assert_eq!(actual, expected);
    let start = encoded.as_ptr() as usize;
    let body = &actual.payload.messages[0];
    assert!(
        (start..start + encoded.len()).contains(&(body.as_ptr() as usize)),
        "decoded bytes should share the owned frame"
    );
    drop(encoded);
    assert_eq!(body, &bytes);
    assert!(actual.payload.one().is_err());
    assert!(actual.payload.control_only().is_err());
}
#[test]
fn audit_unknown_fields_and_all_grade_physical_errors() {
    let frame = request(Payload::message(Bytes::from_static(b"body")))
        .encode()
        .unwrap();
    let mut unknown = frame.to_vec();
    unknown.extend_from_slice(&[0x78, 1]);
    for policy in [
        cordis_contract::Validation::Contract,
        cordis_contract::Validation::Checked,
    ] {
        assert!(cordis_wire::decode_with(unknown.clone().into(), policy).is_ok());
    }
    assert!(cordis_wire::decode_with(unknown.into(), cordis_contract::Validation::Audit).is_err());
    assert!(decode(Bytes::new()).is_err());
    assert!(decode(frame.slice(..frame.len() - 1)).is_err());
    assert!(decode(Bytes::from(vec![0; MAX_MESSAGE + 1])).is_err());
    assert!(
        request(Payload::message(Bytes::from(vec![0; MAX_MESSAGE])))
            .encode()
            .is_err()
    );
    assert!(Payload::decode(Bytes::new()).is_err());
}
#[test]
fn response_identity_and_unknown_outcome_do_not_become_rejection() {
    let request = request(json!({}).into());
    let response = request.response(Outcome::Unknown {
        error: RemoteError {
            code: "lost".into(),
            message: "execution may have committed".into(),
        },
    });
    let Incoming::Response(mut actual) = decode(response.encode().unwrap()).unwrap() else {
        panic!("response expected")
    };
    request.check(&actual).unwrap();
    assert!(matches!(actual.outcome, Outcome::Unknown { .. }));
    actual.operation = "5".into();
    assert!(request.check(&actual).is_err());
}
#[test]
fn maximum_control_shapes_fit_standard_decoder_limits() {
    let mut control = json!(null);
    for _ in 0..cordis_wire::binary::MAX_CONTROL_DEPTH {
        control = json!({"nested":control});
    }
    for control in [
        control,
        serde_json::Value::Object((0..8191).map(|n| (n.to_string(), json!(null))).collect()),
    ] {
        let frame = request(Payload {
            control,
            messages: vec![Bytes::new(); 4096],
        });
        for policy in [
            cordis_wire::Validation::Contract,
            cordis_wire::Validation::Checked,
            cordis_wire::Validation::Audit,
        ] {
            let Incoming::Request(output) =
                cordis_wire::decode_with(frame.encode().unwrap(), policy).unwrap()
            else {
                panic!("request")
            };
            assert_eq!(output, frame);
        }
    }
    assert!(
        request(Payload {
            control: json!(null),
            messages: vec![Bytes::new(); 4097]
        })
        .encode()
        .is_err()
    );
    assert!(Payload::from(json!(vec![0; 8192])).encode().is_err());
}

#[test]
fn audit_sees_fields_discarded_by_maps_and_overwritten_oneofs() {
    use cordis_wire::Validation;
    use prost::encoding::{WireType, encode_key, encode_varint};
    fn message(tag: u32, data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        encode_key(tag, WireType::LengthDelimited, &mut out);
        encode_varint(data.len() as u64, &mut out);
        out.extend_from_slice(data);
        out
    }
    // google.protobuf.Struct.FieldsEntry has key=1 and Value=2. The unknown
    // entry field is discarded by standard map construction in several codecs.
    let mut entry = message(1, b"key");
    entry.extend(message(2, &[8, 0])); // Value.null_value
    entry.extend([0x78, 1]);
    let structure = message(1, &entry);
    let map_control = message(5, &structure); // Value.struct_value
    let mut overwritten_control = message(5, &[0x78, 1]);
    overwritten_control.extend([8, 0]); // Replace the previous oneof branch.
    for control in [map_control, overwritten_control] {
        let raw = Bytes::from(message(1, &control)); // Payload.control
        for p in [Validation::Contract, Validation::Checked] {
            assert!(Payload::decode_with(raw.clone(), p).is_ok());
        }
        assert!(
            Payload::decode_with(raw, Validation::Audit)
                .unwrap_err()
                .to_string()
                .contains("undeclared protobuf")
        );
    }
}
