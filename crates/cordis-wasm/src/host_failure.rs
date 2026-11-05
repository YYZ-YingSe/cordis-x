use crate::Error;
use cordis_contract::{ErrorInfo, Failure, refused};
impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        match error {
            Error::Rejected { code, message } => Self::Rejected(ErrorInfo { code, message }),
            Error::Refused { code, message } => Self::Refused(ErrorInfo { code, message }),
            Error::InvalidInput(message) => refused("invalid_argument", message),
            Error::Overloaded => refused("overloaded", "endpoint capacity exhausted"),
            // Closed is ambiguous after submission. Admission checks are performed
            // before invoking this boundary and retain their explicit refusal.
            other => cordis_host::unknown(other),
        }
    }
}
impl From<Failure> for Error {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Rejected(e) => Self::Rejected {
                code: e.code,
                message: e.message,
            },
            Failure::Refused(e) => Self::Refused {
                code: e.code,
                message: e.message,
            },
            Failure::Unknown(e) => Self::Unknown(format!("{}: {}", e.code, e.message)),
        }
    }
}

/// A resource gate or weak upgrade rejects a closed handle before dispatch.
/// Transport loss and uncertain completion remain Unknown in the shared result.
pub(crate) fn resource_call(error: Error) -> Failure {
    match error {
        Error::Closed => refused("closed", "resource admission is closed"),
        other => other.into(),
    }
}
