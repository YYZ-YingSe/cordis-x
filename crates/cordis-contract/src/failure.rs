use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorInfo {
    pub code: String,
    pub message: String,
}

/// Rejected or Refused acquisition guarantees no surviving acquisition responsibility.
/// Unknown acquisition must retain recovery state. Every cleanup failure
/// preserves its obligation; no category permits blindly replaying the inverse.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Failure {
    Rejected(ErrorInfo),
    /// Runtime admission or authority refusal, distinct from business errors.
    Refused(ErrorInfo),
    Unknown(ErrorInfo),
}

pub fn error(code: &str, message: impl ToString) -> Failure {
    Failure::Rejected(ErrorInfo {
        code: code.into(),
        message: message.to_string(),
    })
}
pub fn refused(code: &str, message: impl ToString) -> Failure {
    Failure::Refused(ErrorInfo {
        code: code.into(),
        message: message.to_string(),
    })
}
impl Failure {
    pub fn check_declared(self, errors: &std::collections::BTreeSet<String>, path: &str) -> Self {
        if let Self::Rejected(info) = &self
            && let Err(message) = crate::portable::error(errors, &info.code, path)
        {
            return Self::Unknown(ErrorInfo {
                code: "contract_violation".into(),
                message,
            });
        }
        self
    }
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, info) = match self {
            Self::Rejected(info) => ("rejected", info),
            Self::Refused(info) => ("refused", info),
            Self::Unknown(info) => ("unknown", info),
        };
        write!(f, "{kind}: {}: {}", info.code, info.message)
    }
}
impl std::error::Error for Failure {}
