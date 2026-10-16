use std::fmt;
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Protocol(String),
    InvalidInput(String),
    Io(String),
    Remote { code: String, message: String },
    Refused { code: String, message: String },
    OutcomeUnknown(String),
    HostLost(String),
    Resource(String),
    Closed,
    Overloaded,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}
impl From<cordis_wire::Error> for Error {
    fn from(e: cordis_wire::Error) -> Self {
        Self::Protocol(e.to_string())
    }
}
impl From<Error> for cordis_runtime::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::Remote { code, message } => Self::Portable(cordis_contract::Failure::Rejected(
                cordis_contract::ErrorInfo { code, message },
            )),
            Error::Refused { code, message } => Self::Portable(cordis_contract::Failure::Refused(
                cordis_contract::ErrorInfo { code, message },
            )),
            // Local capacity is checked before submission. Preserve that
            // admission refusal when a callback crosses the native Runtime;
            // turning it into Message would reclassify it as a business error.
            Error::Overloaded => Self::Portable(cordis_contract::Failure::Refused(
                cordis_contract::ErrorInfo {
                    code: "overloaded".into(),
                    message: "remote operation capacity reached".into(),
                },
            )),
            Error::OutcomeUnknown(message) => Self::Portable(cordis_contract::Failure::Unknown(
                cordis_contract::ErrorInfo {
                    code: "outcome_unknown".into(),
                    message,
                },
            )),
            other => Self::Message(other.to_string()),
        }
    }
}
pub(crate) fn outcome(outcome: cordis_wire::Outcome) -> Result<cordis_wire::Payload> {
    match outcome {
        cordis_wire::Outcome::Success { value } => Ok(value),
        cordis_wire::Outcome::Refused { error } => Err(Error::Refused {
            code: error.code,
            message: error.message,
        }),
        cordis_wire::Outcome::Unknown { error } => Err(Error::OutcomeUnknown(format!(
            "{}: {}",
            error.code, error.message
        ))),
        cordis_wire::Outcome::Failure { error } => Err(Error::Remote {
            code: error.code,
            message: error.message,
        }),
    }
}

impl Error {
    pub(crate) fn check_declared(
        self,
        errors: &std::collections::BTreeSet<String>,
        path: &str,
    ) -> Self {
        // At a business boundary, an uncertain transport or opaque Runtime
        // failure must fault the Session even if guest code catches it.
        if matches!(
            self,
            Self::OutcomeUnknown(_) | Self::HostLost(_) | Self::Io(_) | Self::Resource(_)
        ) {
            return Self::Protocol(format!("{path}: outcome unknown: {self}"));
        }
        if let Self::Remote { code, .. } = &self
            && let Err(message) = cordis_contract::portable::error(errors, code, path)
        {
            return Self::Protocol(message);
        }
        self
    }
}
