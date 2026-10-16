//! Contract compiler. Frontend semantics are independent of language backends.
pub mod backend;
pub mod model;
pub mod output;
mod protobuf;
mod source;
pub mod symbols;
mod tools;
mod validation;

pub use model::Module;
pub use source::compile;
pub use tools::GeneratorTools;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    pub path: String,
    pub message: String,
}
impl Diagnostic {
    pub fn new(path: impl Into<String>, message: impl ToString) -> Self {
        Self {
            path: path.into(),
            message: message.to_string(),
        }
    }
}
impl std::fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.path, self.message)
    }
}
impl std::error::Error for Diagnostic {}
pub type Result<T> = std::result::Result<T, Diagnostic>;
