//! Owned business data. A host binding selects validation, independently of
//! physical representation. Contract-mode construction never walks the DTO.
use crate::{
    MessageContract, Validation, Value,
    typed::{Budget, Codec},
};
use prost::Message as _;
use std::{
    any::Any,
    sync::{Arc, OnceLock},
};

#[derive(Default)]
struct Checks {
    checked: OnceLock<Result<(), String>>,
    audit: OnceLock<Result<(), String>>,
}
impl Checks {
    fn run(
        &self,
        policy: Validation,
        check: impl FnOnce() -> Result<(), String>,
    ) -> Result<(), String> {
        match policy {
            Validation::Contract => Ok(()),
            Validation::Checked => self.checked.get_or_init(check).clone(),
            Validation::Audit => self.audit.get_or_init(check).clone(),
        }
    }
}
trait Native: Send + Sync {
    fn contract(&self) -> &'static MessageContract;
    fn encode(&self, limit: usize) -> Result<Vec<u8>, String>;
    fn validate(&self, policy: Validation) -> Result<(), String>;
    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
}
struct Owned<T> {
    value: T,
    checks: Checks,
}
impl<T: Codec> Native for Owned<T> {
    fn contract(&self) -> &'static MessageContract {
        T::contract()
    }
    fn encode(&self, limit: usize) -> Result<Vec<u8>, String> {
        let length = self.value.encoded_len();
        if length > limit {
            return Err("encoded message byte limit exceeded".into());
        }
        let mut bytes = Vec::with_capacity(length);
        // Use the standard generated encoder with the already calculated size.
        self.value.encode_raw(&mut bytes);
        if bytes.len() > limit {
            return Err("encoded message byte limit exceeded".into());
        }
        Ok(bytes)
    }
    fn validate(&self, policy: Validation) -> Result<(), String> {
        // Native generated DTOs cannot carry unknown wire fields. Both checked
        // and audit use the same immutable-object proof for declared fields.
        let policy = if policy.audits() {
            Validation::Checked
        } else {
            policy
        };
        self.checks
            .run(policy, || self.value.walk(0, &mut Budget::default()))
    }
    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}
#[derive(Clone)]
enum Body {
    Native(Arc<dyn Native>),
    Encoded {
        contract: MessageContract,
        bytes: prost::bytes::Bytes,
        checks: Arc<Checks>,
    },
}
/// The data snapshot is immutable. Required policy is local and distinct from
/// a completed check; no serialized flag can manufacture a validation proof.
#[derive(Clone)]
pub struct Message {
    body: Body,
    required: Validation,
}
pub type FutureResult = crate::LocalFuture<Message>;
fn identity<T: Codec>(shape: &MessageContract) -> Result<(), String> {
    if shape.same_message(T::contract())? {
        Ok(())
    } else {
        Err("codec/contract message mismatch".into())
    }
}
pub fn input<T: Codec>(value: Message, shape: &MessageContract) -> crate::Result<T> {
    identity::<T>(shape)
        .and_then(|_| value.check_identity(shape))
        .and_then(|_| value.with_validation(shape.validation()).into_typed())
        .map_err(|e| crate::refused("invalid_argument", format!("args: {e}")))
}
pub fn result<T: Codec>(value: Message, shape: &MessageContract) -> crate::Result<T> {
    identity::<T>(shape)
        .and_then(|_| value.check_identity(shape))
        .and_then(|_| value.with_validation(shape.validation()).into_typed())
        .map_err(|e| crate::typed::violation(format!("result: {e}")))
}
pub fn argument<T: Codec>(value: T, shape: &MessageContract) -> crate::Result<Message> {
    identity::<T>(shape)
        .and_then(|_| Message::new(value))
        .and_then(|m| {
            m.validate(shape.validation())?;
            Ok(m.with_validation(shape.validation()))
        })
        .map_err(|e| crate::refused("invalid_argument", format!("args: {e}")))
}
pub fn output<T: Codec>(value: T, shape: &MessageContract) -> crate::Result<Message> {
    identity::<T>(shape)
        .and_then(|_| Message::new(value))
        .and_then(|m| {
            m.validate(shape.validation())?;
            Ok(m.with_validation(shape.validation()))
        })
        .map_err(|e| crate::typed::violation(format!("result: {e}")))
}
impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("type", &self.contract().message())
            .field("required", &self.required)
            .finish_non_exhaustive()
    }
}
impl Message {
    pub fn new<T: Codec>(value: T) -> Result<Self, String> {
        Ok(Self {
            body: Body::Native(Arc::new(Owned {
                value,
                checks: Checks::default(),
            })),
            required: Validation::Contract,
        })
    }
    pub fn with_validation(mut self, policy: Validation) -> Self {
        self.required = self.required.max(policy);
        self
    }
    pub fn validation(&self) -> Validation {
        self.required
    }
    pub fn contract(&self) -> &MessageContract {
        match &self.body {
            Body::Native(value) => value.contract(),
            Body::Encoded { contract, .. } => contract,
        }
    }
    /// Own a physical body. Decode only at its real consumer; the binding's
    /// policy is enforced by typed consumption or explicit opaque validation.
    pub fn from_bytes(
        contract: MessageContract,
        bytes: prost::bytes::Bytes,
    ) -> Result<Self, String> {
        if bytes.len() > cordis_contract::budget::MAX_BYTES {
            return Err("encoded message byte limit exceeded".into());
        }
        let required = contract.validation();
        Ok(Self {
            body: Body::Encoded {
                contract,
                bytes,
                checks: Arc::default(),
            },
            required,
        })
    }
    pub fn from_json(contract: MessageContract, value: &Value) -> Result<Self, String> {
        let parsed = contract.parse_json(value)?;
        let checks = Checks::default();
        let _ = checks.checked.set(Ok(()));
        Ok(Self {
            required: contract.validation(),
            body: Body::Encoded {
                contract,
                bytes: parsed.encode_to_vec().into(),
                checks: Arc::new(checks),
            },
        })
    }
    fn check_identity(&self, expected: &MessageContract) -> Result<(), String> {
        if std::ptr::eq(self.contract(), expected) || self.contract().same_message(expected)? {
            Ok(())
        } else {
            Err("message/contract mismatch".into())
        }
    }
    /// A receiving binding checks opaque messages here. Typed receivers defer
    /// field checks to into_typed, so checked mode decodes just once.
    pub fn check(&self, expected: &MessageContract) -> Result<(), String> {
        self.check_identity(expected)?;
        self.validate(expected.validation())
    }
    pub fn validate(&self, policy: Validation) -> Result<(), String> {
        match &self.body {
            Body::Native(value) => value.validate(policy),
            Body::Encoded {
                contract,
                bytes,
                checks,
            } => checks.run(policy, || {
                contract.with_validation(policy).check_binary(bytes)
            }),
        }
    }
    pub fn into_typed<T: Codec>(self) -> Result<T, String> {
        self.check_identity(T::contract())?;
        match self.body {
            Body::Native(value) => {
                value.validate(self.required)?;
                let value = value
                    .into_any()
                    .downcast::<Owned<T>>()
                    .map_err(|_| "native message type differs from the bound Rust type")?;
                Ok(match Arc::try_unwrap(value) {
                    Ok(value) => value.value,
                    Err(value) => value.value.clone(),
                })
            }
            Body::Encoded {
                contract,
                bytes,
                checks,
            } => {
                // Keep the original immutable bytes for unknown-field auditing.
                // The generated decoder and Walk establish the business value;
                // audit must not materialize a second DynamicMessage tree.
                let value =
                    <T as prost::Message>::decode(bytes.clone()).map_err(|e| e.to_string())?;
                if self.required.audits() {
                    checks.run(Validation::Audit, || {
                        cordis_contract::protobuf::audit_binary(&bytes, contract.resolve()?)?;
                        checks.run(Validation::Checked, || {
                            value.walk(0, &mut Budget::default())
                        })
                    })?;
                } else if self.required.checks_fields() {
                    checks.run(Validation::Checked, || {
                        value.walk(0, &mut Budget::default())
                    })?;
                }
                Ok(value)
            }
        }
    }
    pub fn encode(&self, limit: usize) -> Result<prost::bytes::Bytes, String> {
        self.validate(self.required)?;
        let bytes = match &self.body {
            Body::Native(value) => value.encode(limit)?.into(),
            Body::Encoded { bytes, .. } => bytes.clone(),
        };
        if bytes.len() > limit {
            return Err("encoded message byte limit exceeded".into());
        }
        Ok(bytes)
    }
    pub fn to_json(&self) -> Result<Value, String> {
        self.validate(self.required)?;
        let bytes = self.encode(cordis_contract::budget::MAX_BYTES)?;
        let dynamic =
            prost_reflect::DynamicMessage::decode(self.contract().resolve()?.clone(), bytes)
                .map_err(|e| e.to_string())?;
        serde_json::to_value(&dynamic).map_err(|e| e.to_string())
    }
}

/// Adapter entry point: typed generated servers own the real decode. A raw
/// Plugin consumes opaque bytes, so its adapter owns the descriptor check.
pub fn incoming<P: crate::Plugin>(
    shape: MessageContract,
    bytes: prost::bytes::Bytes,
) -> Result<Message, String> {
    let message = Message::from_bytes(shape, bytes)?;
    if !P::TYPED_DISPATCH {
        message.validate(message.validation())?;
    }
    Ok(message)
}
