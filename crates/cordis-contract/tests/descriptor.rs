use cordis_contract::{Descriptor, Dispatch, Interface, MessageContract, Method};
fn interface() -> Interface {
    let mut value = Interface::value_dispatch(false);
    value.methods = Dispatch::Named(
        [(
            "lookup".into(),
            Method::new(
                MessageContract::standard("google.protobuf.StringValue"),
                MessageContract::standard("google.protobuf.Int64Value"),
            ),
        )]
        .into(),
    );
    value
}
#[test]
fn identity_tracks_message_and_current_semantics() {
    let mut a = interface();
    a.capabilities.extend(["log".into(), "clock".into()]);
    let mut b = interface();
    b.capabilities.extend(["clock".into(), "log".into()]);
    let a = Descriptor::new(a).unwrap();
    a.compare(&Descriptor::new(b.clone()).unwrap()).unwrap();
    if let Dispatch::Named(methods) = &mut b.methods {
        methods.get_mut("lookup").unwrap().result =
            MessageContract::standard("google.protobuf.BoolValue");
    }
    let b = Descriptor::new(b).unwrap();
    assert_ne!(a.digest(), b.digest());
    assert_eq!(a.compare(&b).unwrap_err().path, "methods.lookup.result");
}
#[test]
fn capabilities_errors_lifetimes_and_schema_are_checked() {
    let original = Descriptor::new(Interface::value_dispatch(true)).unwrap();
    for change in 0..3 {
        let mut b = original.interface().clone();
        match change {
            0 => {
                b.capabilities.insert("clock".into());
            }
            1 => {
                if let Dispatch::Dynamic(m) = &mut b.methods {
                    m.errors.clear();
                }
            }
            _ => {
                if let Dispatch::Dynamic(r) = &mut b.resources {
                    r.args = MessageContract::standard("google.protobuf.Int64Value");
                }
            }
        }
        assert!(original.compare(&Descriptor::new(b).unwrap()).is_err());
    }
    let mut invalid = original.interface().clone();
    if let Dispatch::Dynamic(r) = &mut invalid.resources {
        r.release = "never".into();
    }
    assert!(Descriptor::new(invalid).is_err());
    let invalid: MessageContract =
        serde_json::from_value(serde_json::json!({"message":"missing.Type","descriptor":"AA=="}))
            .unwrap();
    assert!(invalid.resolve().is_err());
    assert!(
        MessageContract::standard("cordis.data.Lease")
            .resolve()
            .is_err()
    );
}
