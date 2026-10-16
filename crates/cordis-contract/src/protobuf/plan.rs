use super::*;
use prost_reflect::{FieldDescriptor, OneofDescriptor};
use std::collections::HashMap;

#[derive(Debug, Default)]
struct Node {
    fields: Vec<Field>,
    selected: Vec<OneofDescriptor>,
}
#[derive(Debug)]
struct Field {
    descriptor: FieldDescriptor,
    kind: Kind,
    nested: Option<usize>,
    required: bool,
    exact: Option<usize>,
    minimum: Option<i128>,
    maximum: Option<i128>,
}
#[derive(Debug)]
pub(super) struct Plan {
    nodes: Vec<Node>,
}
impl Plan {
    pub(super) fn new(root: &MessageDescriptor) -> Self {
        let mut plan = Self { nodes: Vec::new() };
        let mut pending = vec![root.clone()];
        let mut seen = HashMap::from([(root.full_name().to_owned(), 0usize)]);
        // One descriptor pool has unique full names. Iteration handles recursive
        // schemas without recursive compilation or reference-count cycles.
        while plan.nodes.len() < pending.len() {
            let descriptor = pending[plan.nodes.len()].clone();
            let selected = explicit_oneofs(&descriptor)
                .filter(|oneof| {
                    semantics(&descriptor) != 0
                        || descriptor.full_name() == "google.protobuf.Value"
                        || flag(&oneof.options(), "cordis.schema.selected")
                })
                .collect();
            let fields = descriptor
                .fields()
                .map(|descriptor| {
                    let opts = descriptor.options();
                    let kind = if descriptor.is_map() {
                        let Kind::Message(entry) = descriptor.kind() else {
                            unreachable!()
                        };
                        entry.get_field(2).unwrap().kind()
                    } else {
                        descriptor.kind()
                    };
                    let nested = if let Kind::Message(child) = &kind {
                        Some(
                            *seen.entry(child.full_name().to_owned()).or_insert_with(|| {
                                let index = pending.len();
                                pending.push(child.clone());
                                index
                            }),
                        )
                    } else {
                        None
                    };
                    Field {
                        required: flag(&opts, "cordis.schema.required"),
                        exact: match option(&opts, "cordis.schema.exact_size") {
                            Some(Value::U32(n)) => Some(n as usize),
                            _ => None,
                        },
                        minimum: match option(&opts, "cordis.schema.minimum") {
                            Some(Value::I64(n)) => Some(n as i128),
                            _ => None,
                        },
                        maximum: match option(&opts, "cordis.schema.maximum") {
                            Some(Value::I64(n)) => Some(n as i128),
                            _ => None,
                        },
                        descriptor,
                        kind,
                        nested,
                    }
                })
                .collect();
            plan.nodes.push(Node { fields, selected });
        }
        plan
    }
    pub(super) fn validate(
        &self,
        message: &DynamicMessage,
        validation: crate::Validation,
    ) -> Result<(), String> {
        self.visit(0, message, 0, &mut Budget::default(), validation)
    }
    fn visit(
        &self,
        node: usize,
        message: &DynamicMessage,
        depth: usize,
        budget: &mut Budget,
        validation: crate::Validation,
    ) -> Result<(), String> {
        budget.take(depth)?;
        if validation.audits() && message.unknown_fields().next().is_some() {
            return Err("undeclared protobuf field".into());
        }
        for oneof in &self.nodes[node].selected {
            if !oneof.fields().any(|field| message.has_field(&field)) {
                return Err(format!("{}: no branch selected", oneof.full_name()));
            }
        }
        for field in &self.nodes[node].fields {
            let f = &field.descriptor;
            if field.required && !message.has_field(f) {
                return Err(format!("{}: required field missing", f.full_name()));
            }
            if f.supports_presence() && !message.has_field(f) {
                continue;
            }
            let value = message.get_field(f);
            if let Some(expected) = field.exact {
                let size = match value.as_ref() {
                    Value::List(v) => v.len(),
                    Value::Map(v) => v.len(),
                    Value::Bytes(v) => v.len(),
                    Value::String(v) => v.chars().count(),
                    _ => return Err("invalid size constraint".into()),
                };
                if size != expected {
                    return Err(format!(
                        "{}: expected exactly {expected} elements",
                        f.full_name()
                    ));
                }
            }
            let number = match value.as_ref() {
                Value::I32(v) => Some(*v as i128),
                Value::I64(v) => Some(*v as i128),
                Value::U32(v) => Some(*v as i128),
                Value::U64(v) => Some(*v as i128),
                _ => None,
            };
            if let Some(n) = number
                && (field.minimum.is_some_and(|lo| n < lo)
                    || field.maximum.is_some_and(|hi| n > hi))
            {
                return Err(format!("{}: integer outside declared range", f.full_name()));
            }
            match value.as_ref() {
                Value::List(values) => {
                    for value in values {
                        self.leaf(field, value, depth + 1, budget, validation)?;
                    }
                }
                Value::Map(values) => {
                    for (key, value) in values {
                        budget.take(depth + 1)?;
                        budget.take(depth + 2)?;
                        if let prost_reflect::MapKey::String(key) = key {
                            budget.bytes(key.len())?;
                        }
                        self.leaf(field, value, depth + 2, budget, validation)?;
                    }
                }
                value => self.leaf(field, value, depth + 1, budget, validation)?,
            }
        }
        Ok(())
    }
    fn leaf(
        &self,
        field: &Field,
        value: &Value,
        depth: usize,
        budget: &mut Budget,
        validation: crate::Validation,
    ) -> Result<(), String> {
        if let Value::Message(message) = value {
            return self.visit(
                field.nested.expect("message plan"),
                message,
                depth,
                budget,
                validation,
            );
        }
        budget.take(depth)?;
        match (value, &field.kind) {
            (Value::String(v), _) => budget.bytes(v.len()),
            (Value::Bytes(v), _) => budget.bytes(v.len()),
            (Value::F32(v), _) if !v.is_finite() => Err("non-finite float32".into()),
            (Value::F64(v), _) if !v.is_finite() => Err("non-finite float64".into()),
            (Value::EnumNumber(v), Kind::Enum(e)) if e.get_value(*v).is_none() => {
                Err("undeclared enum value".into())
            }
            _ => Ok(()),
        }
    }
}
