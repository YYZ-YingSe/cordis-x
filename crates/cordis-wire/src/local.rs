//! Host-local receipt attached to an immutable body. Receipts never enter a frame.
use crate::{Payload, Value};
use cordis_contract::MessageContract;
use std::ops::Deref;

#[derive(Clone)]
/// Immutable business bodies with host-local validation evidence.
///
/// No mutable payload access or serialization is provided. A wire payload or
/// arbitrary bytes callback must create a fresh envelope, with no receipt.
pub struct Envelope {
    payload: Payload,
    checked: Option<MessageContract>,
    native: Option<cordis_sdk_core::Message>,
    verified: Option<cordis_sdk_core::Message>,
}
impl From<Payload> for Envelope {
    fn from(payload: Payload) -> Self {
        Self {
            payload,
            checked: None,
            native: None,
            verified: None,
        }
    }
}
impl From<Value> for Envelope {
    fn from(value: Value) -> Self {
        Payload::from(value).into()
    }
}
impl Deref for Envelope {
    type Target = Payload;
    fn deref(&self) -> &Payload {
        &self.payload
    }
}
impl Envelope {
    /// Encode a native snapshot while retaining its unforgeable local checks.
    /// The snapshot is private and cannot be replaced independently of the body.
    pub fn from_message(message: cordis_sdk_core::Message) -> Result<Self, String> {
        let payload = Payload::message(message.encode(crate::MAX_MESSAGE)?);
        Ok(Self {
            payload,
            checked: None,
            native: Some(message),
            verified: None,
        })
    }

    pub fn into_message(self, shape: &MessageContract) -> Result<cordis_sdk_core::Message, String> {
        if let Some(native) = self.native {
            native.bind(shape)
        } else if let Some(verified) = self.verified
            && verified.contract().same_message(shape)?
        {
            verified.bind(shape)
        } else {
            cordis_sdk_core::Message::from_bytes(
                shape.clone(),
                self.payload.one().map_err(|e| e.to_string())?,
            )
        }
    }
    pub fn with_control(mut self, control: Value) -> Self {
        self.payload.control = control;
        self
    }
    pub fn into_wire(self) -> Payload {
        self.payload
    }
    pub fn check(&mut self, shape: &MessageContract) -> Result<(), String> {
        if let Some(native) = &self.native {
            return native.check(shape);
        }
        // Contract still enforces cardinality and the physical byte limit, but
        // neither requests a schema plan nor manufactures field evidence.
        if !shape.validation().checks_fields() {
            return shape.check_binary(&self.payload.one().map_err(|e| e.to_string())?);
        }
        if let Some(previous) = &self.checked
            && previous.validation() >= shape.validation()
            && previous.same_message(shape)?
        {
            return Ok(());
        }
        let message = match &self.verified {
            Some(value) if value.contract().same_message(shape)? => value.clone(),
            _ => cordis_sdk_core::Message::from_bytes(
                shape.clone(),
                self.payload.one().map_err(|e| e.to_string())?,
            )?,
        };
        message.check(shape)?;
        self.verified = Some(message);
        // All current checks use the same fixed logical budget. A configurable
        // budget must become part of this identity before receipts can reuse it.
        self.checked = Some(shape.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cordis_contract::Validation;
    use prost::Message;

    #[derive(Clone, PartialEq, prost::Message)]
    struct Snapshot {
        #[prost(string, tag = "1")]
        value: String,
    }
    static WALKS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    impl cordis_sdk_core::typed::Walk for Snapshot {
        fn walk(
            &self,
            depth: usize,
            budget: &mut cordis_sdk_core::typed::Budget,
        ) -> Result<(), String> {
            WALKS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.value.walk(depth, budget)
        }
    }
    impl cordis_sdk_core::typed::Codec for Snapshot {
        fn contract() -> &'static MessageContract {
            static SHAPE: std::sync::OnceLock<MessageContract> = std::sync::OnceLock::new();
            SHAPE.get_or_init(|| MessageContract::standard("google.protobuf.StringValue"))
        }
    }
    #[test]
    fn native_receipt_survives_host_forwarding_but_not_a_physical_payload() {
        use cordis_sdk_core::typed::Codec;
        WALKS.store(0, std::sync::atomic::Ordering::SeqCst);
        let shape = Snapshot::contract().with_validation(Validation::Checked);
        let message = cordis_sdk_core::Message::new(Snapshot {
            value: "snapshot".into(),
        })
        .unwrap()
        .bind(&shape)
        .unwrap();
        let mut envelope = Envelope::from_message(message).unwrap();
        envelope.check(&shape).unwrap();
        let typed = envelope
            .clone()
            .into_message(&shape)
            .unwrap()
            .into_typed::<Snapshot>()
            .unwrap();
        assert_eq!(typed.value, "snapshot");
        assert_eq!(
            WALKS.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "unchanged native snapshot is checked once"
        );
        let mut payload = envelope.into_wire();
        payload.messages[0] = vec![10, 1, 255].into();
        assert!(
            Envelope::from(payload).check(&shape).is_err(),
            "wire data never inherits the host receipt"
        );
    }
    #[test]
    fn opaque_receipt_strength_is_preserved_when_entering_the_sdk() {
        let shape = MessageContract::standard("google.protobuf.StringValue");
        let mut value = Envelope::from(Payload::message(vec![10, 1, b'x', 120, 1].into()));
        value
            .check(&shape.with_validation(Validation::Checked))
            .unwrap();
        let message = value
            .into_message(&shape.with_validation(Validation::Audit))
            .unwrap();
        assert!(
            message.validate(message.validation()).is_err(),
            "checked evidence cannot stand in for audit"
        );
    }

    #[test]
    fn same_name_different_schema_does_not_share_evidence() {
        use prost_types::{
            DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        };
        let shape = |field_type| {
            let descriptor = FileDescriptorSet {
                file: vec![FileDescriptorProto {
                    name: Some("receipt.proto".into()),
                    package: Some("receipt".into()),
                    syntax: Some("proto3".into()),
                    message_type: vec![DescriptorProto {
                        name: Some("Body".into()),
                        field: vec![FieldDescriptorProto {
                            name: Some("value".into()),
                            number: Some(1),
                            label: Some(1),
                            r#type: Some(field_type),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            };
            MessageContract::new("receipt.Body", &descriptor.encode_to_vec())
                .unwrap()
                .with_validation(Validation::Checked)
        };
        let mut value = Envelope::from(Payload::message(vec![10, 1, 255].into()));
        let bytes = shape(12);
        value.check(&bytes).unwrap();
        let previous = value.checked.clone().unwrap();
        assert!(value.check(&shape(9)).is_err()); // Identical name, now UTF-8 string.
        assert_eq!(value.checked.as_ref(), Some(&previous));
        value.check(&bytes).unwrap(); // Failed strengthening did not corrupt old evidence.
    }

    #[test]
    fn receipt_strength_and_fresh_snapshot_boundaries() {
        let shape = MessageContract::standard("google.protobuf.StringValue");
        let mut value = Envelope::from(Payload::message(vec![10, 1, b'x'].into()));
        value
            .check(&shape.with_validation(Validation::Audit))
            .unwrap();
        let mut clone = value
            .clone()
            .with_control(serde_json::json!({"method":"echo"}));
        clone
            .check(&shape.with_validation(Validation::Checked))
            .unwrap();
        assert_eq!(
            clone.checked.as_ref().unwrap().validation(),
            Validation::Audit
        );
        // Turning a local result into application bytes or a physical payload
        // discards evidence, even if the bytes allocation is still shared.
        let wire = clone.into_wire();
        assert!(
            Envelope::from(Payload::message(wire.one().unwrap()))
                .checked
                .is_none()
        );
        let mut wire = value.into_wire();
        wire.messages[0] = vec![10, 1, 255].into();
        let mut fresh = Envelope::from(wire);
        assert!(
            fresh
                .check(&shape.with_validation(Validation::Checked))
                .is_err()
        );
        assert!(fresh.checked.is_none());
    }

    #[test]
    fn contract_keeps_hard_limits_without_resolving_business_schema() {
        let shape = MessageContract::standard("not.a.real.Message");
        let mut value = Envelope::from(Payload::message(vec![255].into()));
        value.check(&shape).unwrap(); // Opaque forwarding has no decoder obligation.
        assert!(value.checked.is_none());
        assert!(Envelope::from(Value::Null).check(&shape).is_err());
        assert!(
            Envelope::from(Payload {
                control: Value::Null,
                messages: vec![Default::default(); 2]
            })
            .check(&shape)
            .is_err()
        );
        assert!(
            Envelope::from(Payload::message(vec![0; crate::MAX_MESSAGE + 1].into()))
                .check(&shape)
                .is_err()
        );
    }
    #[test]
    fn receipts_follow_body_schema_and_strength_but_never_cross_wire() {
        let shape = MessageContract::standard("google.protobuf.StringValue");
        let mut body = Envelope::from(Payload::message(vec![10, 1, b'x', 120, 1].into()));
        body.check(&shape.with_validation(Validation::Contract))
            .unwrap();
        assert!(body.checked.is_none());
        body.check(&shape.with_validation(Validation::Checked))
            .unwrap();
        let mut copy = body
            .clone()
            .with_control(serde_json::json!({"method":"forward"}));
        copy.check(&shape.with_validation(Validation::Checked))
            .unwrap();
        assert!(
            copy.check(&shape.with_validation(Validation::Audit))
                .is_err()
        );
        // The same bytes are not an int64 message (wrong wire type).
        assert!(
            copy.check(
                &MessageContract::standard("google.protobuf.Int64Value")
                    .with_validation(Validation::Checked)
            )
            .is_err()
        );
        let received = Envelope::from(copy.into_wire());
        assert!(received.checked.is_none());
        let mut fresh = Envelope::from(Payload::message(vec![10, 128].into()));
        assert!(
            fresh
                .check(&shape.with_validation(Validation::Checked))
                .is_err()
        );
        assert!(fresh.checked.is_none());
    }
}
