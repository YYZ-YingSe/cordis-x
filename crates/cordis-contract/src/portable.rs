//! JSON command-envelope mapping and standard protobuf message validation.
use crate::{Descriptor, Dispatch, Interface, MessageContract, Method, Resource};
pub use serde_json::Value;

pub fn decode(value: &Value) -> Result<Descriptor, String> {
    validate(value, 0)?;
    reject_duplicate_sets(value)?;
    let interface: Interface = serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
    Descriptor::new(interface).map_err(|e| e.to_string())
}
fn reject_duplicate_sets(value: &Value) -> Result<(), String> {
    match value {
        Value::Array(values) => {
            let mut seen = std::collections::BTreeSet::new();
            for value in values {
                if !seen.insert(value.as_str().ok_or("expected identifier set")?) {
                    return Err("duplicate contract identifier".into());
                }
            }
        }
        Value::Object(fields) => {
            for value in fields.values() {
                reject_duplicate_sets(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}
pub fn encode(descriptor: &Descriptor) -> Value {
    serde_json::to_value(descriptor.interface()).expect("descriptor is a portable value")
}
pub fn compare(expected: &Descriptor, actual: &Value) -> Result<(), String> {
    expected
        .compare(&decode(actual)?)
        .map_err(|e| e.to_string())
}
pub fn value(shape: &MessageContract, data: &Value, path: &str) -> Result<(), String> {
    shape
        .parse_json(data)
        .map(|_| ())
        .map_err(|e| format!("{path}: {e}"))
}
pub fn method<'a>(methods: &'a Dispatch<Method>, name: &str) -> Result<&'a Method, String> {
    methods
        .get(name)
        .ok_or_else(|| format!("methods.{name}: undeclared method"))
}
pub fn resource<'a>(descriptor: &'a Descriptor, kind: &str) -> Result<&'a Resource, String> {
    descriptor
        .interface()
        .resources
        .get(kind)
        .ok_or_else(|| format!("resources.{kind}: undeclared factory"))
}

/// Logical portable profile, independent of serialization and calling convention.
pub const MAX_INTEGER: i64 = 9_007_199_254_740_991;
pub const MAX_DEPTH: usize = 96;
pub fn validate(value: &Value, depth: usize) -> Result<(), String> {
    if depth > MAX_DEPTH {
        return Err("value nesting too deep".into());
    }
    match value {
        Value::Number(n) if n.as_f64().is_none_or(|n| !n.is_finite()) => {
            Err("non-finite JSON number".into())
        }
        Value::Array(values) => values.iter().try_for_each(|v| validate(v, depth + 1)),
        Value::Object(values) => values.values().try_for_each(|v| validate(v, depth + 1)),
        _ => Ok(()),
    }
}

/// Check only business rejection codes. Runtime refusals and uncertain outcomes
/// are separate result categories and never enter this set.
pub fn error(
    errors: &std::collections::BTreeSet<String>,
    code: &str,
    path: &str,
) -> Result<(), String> {
    if !code.is_empty() && (errors.contains(code) || errors.contains("*")) {
        Ok(())
    } else {
        Err(format!("{path}: undeclared business error {code:?}"))
    }
}

#[cfg(test)]
mod error_tests {
    use super::*;
    #[test]
    fn business_error_sets_are_explicit_and_do_not_reserve_runtime_code_names() {
        assert!(error(&Default::default(), "closed", "errors").is_err());
        assert!(error(&["expected".into()].into(), "expected", "errors").is_ok());
        assert!(error(&["expected".into()].into(), "other", "errors").is_err());
        assert!(error(&["*".into()].into(), "other", "errors").is_ok());
        assert!(error(&["*".into()].into(), "", "errors").is_err());
    }
}
