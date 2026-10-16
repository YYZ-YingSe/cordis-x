//! Small shared support for generated portable bindings. No scheduling or ownership state.
use crate::{Failure, Value};
use cordis_contract::MessageContract;
mod controls;
pub use controls::{Event, Factory, Flow, Middleware, Next, Provision};
pub fn bindings(text: &str) -> crate::Bindings {
    serde_json::from_str(text).expect("generated control bindings")
}
pub fn control(text: &str) -> crate::ControlContract {
    serde_json::from_str(text).expect("generated control contract")
}

pub trait Codec: prost::Message + Default + Clone + Send + Sync + Walk + 'static {
    fn contract() -> &'static MessageContract;
    fn encode(self) -> std::result::Result<Value, String> {
        self.walk(0, &mut Budget::default())?;
        Self::contract().to_json(&self)
    }
    fn decode(value: Value) -> std::result::Result<Self, String> {
        Self::contract()
            .parse_json(&value)?
            .transcode_to()
            .map_err(|e| e.to_string())
    }
}
/// Compiler-emitted literals only. Untrusted descriptions use contract::decode.
pub fn shape(text: &str) -> MessageContract {
    serde_json::from_str(text).expect("validated generated shape")
}
pub fn method(text: &str) -> crate::Method {
    serde_json::from_str(text).expect("validated generated method")
}
pub fn resource(text: &str) -> crate::ResourceContract {
    serde_json::from_str(text).expect("validated generated resource")
}
pub fn interface(text: &str) -> crate::Interface {
    let value = serde_json::from_str(text).expect("generated JSON");
    crate::contract::decode(&value)
        .expect("validated generated interface")
        .interface()
        .clone()
}
pub fn violation(message: impl ToString) -> Failure {
    Failure::Unknown(crate::ErrorInfo {
        code: "contract_violation".into(),
        message: message.to_string(),
    })
}

pub use cordis_contract::budget::Budget;
pub trait Walk {
    fn walk(&self, depth: usize, budget: &mut Budget) -> std::result::Result<(), String>;
}
macro_rules! scalar { ($($t:ty),*)=>{$(impl Walk for $t {fn walk(&self,d:usize,b:&mut Budget)->std::result::Result<(),String>{b.take(d)}})*}; }
scalar!(bool, i32, i64, u32, u64, u8);
impl Walk for f32 {
    fn walk(&self, d: usize, b: &mut Budget) -> std::result::Result<(), String> {
        b.take(d)?;
        if self.is_finite() {
            Ok(())
        } else {
            Err("non-finite float".into())
        }
    }
}
impl Walk for f64 {
    fn walk(&self, d: usize, b: &mut Budget) -> std::result::Result<(), String> {
        b.take(d)?;
        if self.is_finite() {
            Ok(())
        } else {
            Err("non-finite float".into())
        }
    }
}
impl Walk for String {
    fn walk(&self, d: usize, b: &mut Budget) -> std::result::Result<(), String> {
        b.take(d)?;
        b.bytes(self.len())
    }
}
impl<T: Walk> Walk for Option<T> {
    fn walk(&self, d: usize, b: &mut Budget) -> std::result::Result<(), String> {
        if let Some(v) = self {
            v.walk(d, b)
        } else {
            Ok(())
        }
    }
}
impl<T: Walk> Walk for Box<T> {
    fn walk(&self, d: usize, b: &mut Budget) -> std::result::Result<(), String> {
        self.as_ref().walk(d, b)
    }
}
impl<T: Walk> Walk for Vec<T> {
    fn walk(&self, d: usize, b: &mut Budget) -> std::result::Result<(), String> {
        for v in self {
            v.walk(d, b)?;
        }
        Ok(())
    }
}
impl<K: Walk, V: Walk> Walk for std::collections::BTreeMap<K, V> {
    fn walk(&self, d: usize, b: &mut Budget) -> std::result::Result<(), String> {
        for (k, v) in self {
            b.take(d)?;
            k.walk(d + 1, b)?;
            v.walk(d + 1, b)?;
        }
        Ok(())
    }
}

impl Walk for prost::bytes::Bytes {
    fn walk(&self, d: usize, b: &mut Budget) -> std::result::Result<(), String> {
        b.take(d)?;
        b.bytes(self.len())
    }
}
