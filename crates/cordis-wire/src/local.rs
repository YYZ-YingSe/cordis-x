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
}
impl From<Payload> for Envelope {
    fn from(payload: Payload) -> Self {
        Self {
            payload,
            checked: None,
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
    pub fn with_control(mut self, control: Value) -> Self {
        self.payload.control = control;
        self
    }
    pub fn into_wire(self) -> Payload {
        self.payload
    }
    pub fn check(&mut self, shape: &MessageContract) -> Result<(), String> {
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
        shape.check_binary(&self.payload.one().map_err(|e| e.to_string())?)?;
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
