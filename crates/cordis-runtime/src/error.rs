use cordis_contract::ContractId;
use cordis_kernel::FiberId;
use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Kernel(cordis_kernel::Error),
    /// Keep business rejection, admission refusal and unknown outcome distinct
    /// while a portable callback crosses native event/middleware/factory code.
    Portable(cordis_contract::Failure),
    Closed,
    BrokerUnavailable,
    BrokerMismatch,
    BrokerChanged,
    RuntimeStopped,
    UndeclaredDependency(ContractId),
    TypeMismatch(ContractId),
    MissingService(ContractId),
    Retired(FiberId),
    InitializationFailed(FiberId, String),
    ExecutionFailed(FiberId, String),
    CleanupFailed(FiberId, String),
    CleanupBlocked(FiberId, String),
    WouldDeadlock,
    DuplicateNext,
    ExpiredContinuation,
    Panic(String),
    Message(String),
    Aggregate(Vec<Error>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(message) => f.write_str(message),
            Self::Kernel(error) => error.fmt(f),
            _ => write!(f, "{self:?}"),
        }
    }
}
impl std::error::Error for Error {}
impl From<cordis_kernel::Error> for Error {
    fn from(error: cordis_kernel::Error) -> Self {
        Self::Kernel(error)
    }
}
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Message(error.to_string())
    }
}

impl From<cordis_contract::Failure> for Error {
    fn from(failure: cordis_contract::Failure) -> Self {
        Self::Portable(failure)
    }
}
