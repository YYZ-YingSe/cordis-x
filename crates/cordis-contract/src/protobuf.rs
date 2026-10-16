//! Standard protobuf parsing with Cordis value/authority constraints.
//! Binary encodings are not canonical: correctness is stated on decoded values.
mod audit;
mod plan;
pub use audit::audit_binary;

use crate::budget::Budget;
use base64::{Engine, engine::general_purpose::STANDARD};
use prost::Message;
use prost_reflect::{
    DescriptorPool, DynamicMessage, Kind, MessageDescriptor, ReflectMessage, Value,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, OnceLock, Weak},
};

pub use crate::budget::{MAX_BYTES, MAX_DEPTH, MAX_NODES};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageContract {
    message: Arc<str>,
    descriptor: Arc<str>,
    #[serde(skip)]
    resolved: Arc<OnceLock<Result<ResolvedMessage, String>>>,
    // Local binding policy, never deserialized from a peer's descriptor.
    #[serde(skip, default = "crate::Validation::contract")]
    validation: crate::Validation,
}
#[derive(Debug)]
struct ResolvedMessage {
    message: MessageDescriptor,
    plan: OnceLock<plan::Plan>,
    // Keep the shared schema alive as long as any contract uses it. Weak cache
    // entries must not retain schemas after a component generation is gone.
    _pool: Arc<DescriptorPool>,
}
fn schema(encoded: &str) -> Result<Arc<DescriptorPool>, String> {
    static POOLS: OnceLock<Mutex<BTreeMap<String, Weak<DescriptorPool>>>> = OnceLock::new();
    let mut pools = POOLS
        .get_or_init(Default::default)
        .lock()
        .map_err(|e| e.to_string())?;
    if let Some(pool) = pools.get(encoded).and_then(Weak::upgrade) {
        return Ok(pool);
    }
    let bytes = decode_bytes(encoded)?;
    let mut pool = DescriptorPool::global();
    pool.decode_file_descriptor_set(bytes.as_slice())
        .map_err(|e| e.to_string())?;
    check_schema(&pool)?;
    let pool = Arc::new(pool);
    pools.retain(|_, value| value.strong_count() != 0);
    pools.insert(encoded.to_owned(), Arc::downgrade(&pool));
    Ok(pool)
}
impl PartialEq for MessageContract {
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message && self.descriptor == other.descriptor
    }
}
impl Eq for MessageContract {}
impl MessageContract {
    pub fn new(message: impl Into<String>, descriptor: &[u8]) -> Result<Self, String> {
        Ok(Self {
            message: message.into().into(),
            descriptor: STANDARD.encode(compact_descriptor(descriptor)?).into(),
            resolved: Default::default(),
            validation: crate::Validation::Contract,
        })
    }
    pub fn json_value() -> Self {
        Self::standard("google.protobuf.Value")
    }
    pub fn standard(name: &str) -> Self {
        Self {
            message: name.into(),
            descriptor: include_str!(env!("CORDIS_RUNTIME_SCHEMA")).trim().into(),
            resolved: Default::default(),
            validation: crate::Validation::Contract,
        }
    }
    pub fn parse_json(&self, value: &serde_json::Value) -> Result<DynamicMessage, String> {
        use serde::de::IntoDeserializer;
        crate::portable::validate(value, 0)?;
        json_precision(self.resolve()?, value, 0)?;
        let message =
            DynamicMessage::deserialize(self.resolve()?.clone(), value.into_deserializer())
                .map_err(|e| e.to_string())?;
        self.validate(&message, crate::Validation::Audit)?;
        Ok(message)
    }
    pub fn to_json<T: Message>(&self, message: &T) -> Result<serde_json::Value, String> {
        let message =
            DynamicMessage::decode(self.resolve()?.clone(), message.encode_to_vec().as_slice())
                .map_err(|e| e.to_string())?;
        self.validate(&message, crate::Validation::Audit)?;
        serde_json::to_value(&message).map_err(|e| e.to_string())
    }
    /// Receiving-domain validation. Binary size is checked before decoding;
    /// logical limits are independent of physical protobuf overhead.
    pub fn parse_binary(&self, bytes: &[u8]) -> Result<DynamicMessage, String> {
        if bytes.len() > MAX_BYTES {
            return Err("encoded message byte limit exceeded".into());
        }
        if self.validation.audits() {
            audit_binary(bytes, self.resolve()?)?;
        }
        let message =
            DynamicMessage::decode(self.resolve()?.clone(), bytes).map_err(|e| e.to_string())?;
        self.validate(&message, self.validation)?;
        Ok(message)
    }
    /// Full schemas are interned at resolution. Live bindings compare pool
    /// identity plus the message index, without rescanning descriptor text.
    pub fn same_message(&self, other: &Self) -> Result<bool, String> {
        Ok(self.resolve()? == other.resolve()?)
    }
    pub fn validation(&self) -> crate::Validation {
        self.validation
    }
    pub fn with_validation(&self, validation: crate::Validation) -> Self {
        Self {
            validation,
            ..self.clone()
        }
    }
    /// Check opaque business bytes only when the host requires field checking.
    /// Forwarders in contract mode never materialize the business object.
    pub fn check_binary(&self, bytes: &[u8]) -> Result<(), String> {
        if bytes.len() > MAX_BYTES {
            return Err("encoded message byte limit exceeded".into());
        }
        if self.validation.checks_fields() {
            self.parse_binary(bytes)?;
        }
        Ok(())
    }
    pub fn message(&self) -> &str {
        &self.message
    }
    pub fn descriptor(&self) -> &str {
        &self.descriptor
    }
    pub fn resolve(&self) -> Result<&MessageDescriptor, String> {
        Ok(&self.resolved()?.message)
    }
    fn validate(
        &self,
        message: &DynamicMessage,
        validation: crate::Validation,
    ) -> Result<(), String> {
        if !validation.checks_fields() {
            return Ok(());
        }
        let resolved = self.resolved()?;
        resolved
            .plan
            .get_or_init(|| plan::Plan::new(&resolved.message))
            .validate(message, validation)
    }
    fn resolved(&self) -> Result<&ResolvedMessage, String> {
        self.resolved
            .get_or_init(|| {
                let pool = schema(&self.descriptor)?;
                let message = pool
                    .get_message_by_name(&self.message)
                    .ok_or("unknown protobuf message")?;
                data_message(&message)?;
                Ok(ResolvedMessage {
                    message,
                    plan: OnceLock::new(),
                    _pool: pool,
                })
            })
            .as_ref()
            .map_err(Clone::clone)
    }
}
pub fn decode_bytes(text: &str) -> Result<Vec<u8>, String> {
    if text.len() > MAX_BYTES.div_ceil(3) * 4 {
        return Err("protobuf descriptor exceeds byte limit".into());
    }
    let bytes = STANDARD.decode(text).map_err(|e| e.to_string())?;
    if bytes.len() > MAX_BYTES || STANDARD.encode(&bytes) != text {
        return Err("invalid base64 payload".into());
    }
    Ok(bytes)
}
pub fn option(options: &DynamicMessage, name: &str) -> Option<Value> {
    let extension = options
        .descriptor()
        .parent_pool()
        .get_extension_by_name(name)?;
    options
        .has_extension(&extension)
        .then(|| options.get_extension(&extension).into_owned())
}
pub fn flag(options: &DynamicMessage, name: &str) -> bool {
    option(options, name) == Some(Value::Bool(true))
}
pub fn semantics(message: &MessageDescriptor) -> i32 {
    match option(&message.options(), "cordis.schema.semantics") {
        Some(Value::EnumNumber(v)) => v,
        _ => 0,
    }
}
fn data_message(message: &MessageDescriptor) -> Result<(), String> {
    let name = message.full_name();
    if matches!(name, "cordis.data.Lease" | "cordis.data.Child")
        || name.starts_with("cordis.schema.")
    {
        return Err("resource capabilities and schema metadata are not DTOs".into());
    }
    if name.starts_with("google.protobuf.")
        && ![
            "google.protobuf.Empty",
            "google.protobuf.Value",
            "google.protobuf.Struct",
            "google.protobuf.ListValue",
            "google.protobuf.DoubleValue",
            "google.protobuf.FloatValue",
            "google.protobuf.Int64Value",
            "google.protobuf.UInt64Value",
            "google.protobuf.Int32Value",
            "google.protobuf.UInt32Value",
            "google.protobuf.BoolValue",
            "google.protobuf.StringValue",
            "google.protobuf.BytesValue",
        ]
        .contains(&name)
    {
        return Err(format!("{name}: unsupported well-known data message"));
    }
    Ok(())
}
fn explicit_oneofs(m: &MessageDescriptor) -> impl Iterator<Item = prost_reflect::OneofDescriptor> {
    m.oneofs().filter(|o| {
        !o.fields()
            .all(|f| f.field_descriptor_proto().proto3_optional.unwrap_or(false))
    })
}
/// Schema-level checks, including a least fixed point for inhabited recursive types.
pub fn check_schema(pool: &DescriptorPool) -> Result<(), String> {
    let messages: Vec<_> = pool
        .all_messages()
        .filter(|m| {
            !m.full_name().starts_with("google.protobuf.")
                && !m.full_name().starts_with("cordis.schema.")
        })
        .collect();
    if messages.len() > 4096 {
        return Err("too many message declarations".into());
    }
    for m in &messages {
        if m.parent_file().file_descriptor_proto().syntax() != "proto3" {
            return Err(format!(
                "{}: only proto3 data declarations are supported",
                m.full_name()
            ));
        }
        let sem = semantics(m);
        if !(0..=3).contains(&sem) {
            return Err("unknown message semantics".into());
        }
        if sem != 0 {
            let oneofs: Vec<_> = explicit_oneofs(m).collect();
            if oneofs.len() != 1 || oneofs[0].fields().count() != m.fields().count() {
                return Err(format!(
                    "{}: option/nullable/variant must contain exactly one oneof and no other fields",
                    m.full_name()
                ));
            }
            if sem == 1 || sem == 2 {
                let names: BTreeSet<_> = m.fields().map(|f| f.name().to_owned()).collect();
                let expected = if sem == 1 {
                    ["none", "some"]
                } else {
                    ["null", "value"]
                };
                if names != expected.into_iter().map(str::to_owned).collect() {
                    return Err(format!(
                        "{}: invalid tagged optional branches",
                        m.full_name()
                    ));
                }
                let empty = m.get_field_by_name(expected[0]).unwrap();
                if !matches!(empty.kind(), Kind::Enum(e) if e.full_name() == "cordis.data.Null" && e.values().count()==1 && e.get_value(0).is_some_and(|v|v.name()=="NULL_VALUE"))
                {
                    return Err("empty branch must use cordis.data.Null".into());
                }
                if sem == 2
                    && matches!(m.get_field_by_name("value").unwrap().kind(), Kind::Message(child) if semantics(&child) == 2 || child.full_name() == "cordis.data.DynamicValue")
                {
                    return Err("redundant nullable must be normalized in the schema".into());
                }
            }
        }
        for field in m.fields() {
            let opts = field.options();
            if flag(&opts, "cordis.schema.required")
                && (!field.supports_presence()
                    || field.containing_oneof().is_some_and(|o| {
                        o.fields()
                            .any(|f| !f.field_descriptor_proto().proto3_optional.unwrap_or(false))
                    }))
            {
                return Err(format!(
                    "{}: required needs an optional scalar or singular message; use selected for oneof",
                    field.full_name()
                ));
            }
            if option(&opts, "cordis.schema.exact_size").is_some()
                && !(field.is_list()
                    || field.is_map()
                    || matches!(field.kind(), Kind::Bytes | Kind::String))
            {
                return Err("exact_size requires a container, bytes or string".into());
            }
            if option(&opts, "cordis.schema.minimum").is_some()
                || option(&opts, "cordis.schema.maximum").is_some()
            {
                if !matches!(
                    field.kind(),
                    Kind::Int32
                        | Kind::Sint32
                        | Kind::Sfixed32
                        | Kind::Int64
                        | Kind::Sint64
                        | Kind::Sfixed64
                        | Kind::Uint32
                        | Kind::Fixed32
                        | Kind::Uint64
                        | Kind::Fixed64
                ) || field.is_list()
                    || field.is_map()
                {
                    return Err("integer bounds require a singular integer".into());
                }
                let (min, max) = match field.kind() {
                    Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => {
                        (i32::MIN as i128, i32::MAX as i128)
                    }
                    Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => {
                        (i64::MIN as i128, i64::MAX as i128)
                    }
                    Kind::Uint32 | Kind::Fixed32 => (0, u32::MAX as i128),
                    _ => (0, u64::MAX as i128),
                };
                let min = match option(&opts, "cordis.schema.minimum") {
                    Some(Value::I64(n)) => min.max(n as i128),
                    _ => min,
                };
                let max = match option(&opts, "cordis.schema.maximum") {
                    Some(Value::I64(n)) => max.min(n as i128),
                    _ => max,
                };
                if min > max {
                    return Err("empty integer interval".into());
                }
            }
            if field.is_map()
                && matches!(option(&opts,"cordis.schema.exact_size"),Some(Value::U32(n)) if n>2)
            {
                let Kind::Message(entry) = field.kind() else {
                    unreachable!()
                };
                if matches!(entry.get_field(1).unwrap().kind(), Kind::Bool) {
                    return Err("bool map has at most two distinct keys".into());
                }
            }
            if let Kind::Message(child) = field.kind()
                && !child.is_map_entry()
            {
                data_message(&child)?;
            }
        }
    }
    let mut inhabited = BTreeSet::new();
    loop {
        let before = inhabited.len();
        for m in &messages {
            let field_ok = |f: &prost_reflect::FieldDescriptor| match if f.is_map() {
                let Kind::Message(entry) = f.kind() else {
                    unreachable!()
                };
                entry.get_field(2).unwrap().kind()
            } else {
                f.kind()
            } {
                Kind::Message(child) => {
                    inhabited.contains(child.full_name())
                        || child.full_name().starts_with("google.protobuf.")
                }
                _ => true,
            };
            let required_ok=m.fields().filter(|f| flag(&f.options(),"cordis.schema.required") || matches!(option(&f.options(),"cordis.schema.exact_size"),Some(Value::U32(n)) if n>0)).all(|f| field_ok(&f));
            let choice_ok = explicit_oneofs(m)
                .filter(|o| semantics(m) != 0 || flag(&o.options(), "cordis.schema.selected"))
                .all(|o| o.fields().any(|f| field_ok(&f)));
            if required_ok && choice_ok {
                inhabited.insert(m.full_name().to_owned());
            }
        }
        if inhabited.len() == before {
            break;
        }
    }
    for m in messages {
        if !inhabited.contains(m.full_name()) {
            return Err(format!(
                "{}: recursive type has no finite value",
                m.full_name()
            ));
        }
    }
    Ok(())
}
pub fn validate(message: &DynamicMessage) -> Result<(), String> {
    validate_with(message, crate::Validation::Audit)
}
pub fn validate_with(
    message: &DynamicMessage,
    validation: crate::Validation,
) -> Result<(), String> {
    if !validation.checks_fields() {
        return Ok(());
    }
    plan::Plan::new(&message.descriptor()).validate(message, validation)
}

/// Well-known descriptors are supplied by protobuf runtimes, not repeated in every contract.
pub fn compact_descriptor(bytes: &[u8]) -> Result<Vec<u8>, String> {
    if bytes.len() > MAX_BYTES {
        return Err("descriptor byte limit exceeded".into());
    }
    let desc = DescriptorPool::global()
        .get_message_by_name("google.protobuf.FileDescriptorSet")
        .expect("standard descriptor");
    let mut set = DynamicMessage::decode(desc, bytes).map_err(|e| e.to_string())?;
    let field = set.descriptor().get_field_by_name("file").unwrap();
    let Value::List(files) = set.get_field_mut(&field) else {
        unreachable!()
    };
    let before = files.len();
    files.retain(|file| match file {Value::Message(file)=>!matches!(file.get_field_by_name("name").as_deref(),Some(Value::String(name)) if name.starts_with("google/protobuf/")),_=>false});
    Ok(if files.len() == before {
        bytes.to_vec()
    } else {
        set.encode_to_vec()
    })
}

/// ProtoJSON can accept numeric spellings of int64. Bound those spellings to
/// exactly representable JSON integers; the full integer domain uses strings.
fn json_precision(
    desc: &MessageDescriptor,
    value: &serde_json::Value,
    depth: usize,
) -> Result<(), String> {
    if depth > MAX_DEPTH {
        return Err("message nesting limit exceeded".into());
    }
    let scalar = |kind: &Kind, value: &serde_json::Value| -> Result<(), String> {
        if matches!(
            kind,
            Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 | Kind::Uint64 | Kind::Fixed64
        ) && value.is_number()
            && value
                .as_f64()
                .is_none_or(|n| n.abs() > crate::portable::MAX_INTEGER as f64)
        {
            return Err(
                "int64/uint64 outside the JSON safe range must use a decimal string".into(),
            );
        }
        Ok(())
    };
    if desc.full_name().starts_with("google.protobuf.") {
        if desc.parent_file().name() == "google/protobuf/wrappers.proto" {
            if value.is_null() {
                return Err("scalar message requires a value".into());
            }
            return scalar(&desc.get_field(1).unwrap().kind(), value);
        }
        return Ok(());
    }
    let Some(object) = value.as_object() else {
        return Ok(());
    };
    for field in desc.fields() {
        let value = object
            .get(field.json_name())
            .or_else(|| object.get(field.name()));
        let Some(value) = value.filter(|v| !v.is_null()) else {
            continue;
        };
        let (kind, values) = if field.is_map() {
            let Kind::Message(entry) = field.kind() else {
                unreachable!()
            };
            (
                entry.get_field(2).unwrap().kind(),
                value
                    .as_object()
                    .map(|m| m.values().collect::<Vec<_>>())
                    .unwrap_or_default(),
            )
        } else if field.is_list() {
            (
                field.kind(),
                value
                    .as_array()
                    .map(|v| v.iter().collect::<Vec<_>>())
                    .unwrap_or_default(),
            )
        } else {
            (field.kind(), vec![value])
        };
        for value in values {
            scalar(&kind, value)?;
            if let Kind::Message(child) = &kind {
                json_precision(child, value, depth + 1)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn logical_bytes_do_not_depend_on_escaping_or_protobuf_overhead() {
        let shape = MessageContract::standard("google.protobuf.StringValue");
        for character in ["x", "\0"] {
            let text = character.repeat(MAX_BYTES);
            let message = shape.parse_json(&json!(text)).unwrap();
            assert!(message.encoded_len() > MAX_BYTES);
            assert_eq!(
                shape.parse_json(&json!(text + "x")).unwrap_err(),
                "message byte limit exceeded"
            );
        }
    }

    #[test]
    fn logical_budget_counts_map_keys_and_all_string_values() {
        let shape = MessageContract::json_value();
        let key = "k".repeat(MAX_BYTES / 2);
        let text = "中".repeat(MAX_BYTES / 6);
        assert!(
            shape
                .parse_json(&json!({key.clone(): text.clone()}))
                .is_ok()
        );
        assert!(shape.parse_json(&json!({key: text + "中文"})).is_err());
    }

    #[test]
    fn borrowed_values_keep_validation_and_leave_input_intact() {
        let shape = MessageContract::json_value();
        let value = json!({"nested": [null, true, {"text": "\0\n\\\"中🦀"}, -1.25]});
        let saved = value.clone();
        let message = shape.parse_json(&value).unwrap();
        assert_eq!(shape.to_json(&message).unwrap(), value);
        assert_eq!(value, saved);
        assert!(shape.parse_json(&json!(vec![false; MAX_NODES])).is_err());
        let mut deep = json!(null);
        for _ in 0..MAX_DEPTH + 1 {
            deep = json!([deep]);
        }
        assert!(shape.parse_json(&deep).is_err());
        let integer = MessageContract::standard("google.protobuf.Int64Value");
        assert!(integer.parse_json(&json!(9007199254740992_u64)).is_err());
        assert!(integer.parse_json(&json!(i64::MIN.to_string())).is_ok());
        assert!(
            MessageContract::standard("google.protobuf.Empty")
                .parse_json(&json!({"undeclared": 1}))
                .is_err()
        );
        assert!(
            MessageContract::standard("google.protobuf.DoubleValue")
                .parse_json(&json!("NaN"))
                .is_err()
        );
    }
}
