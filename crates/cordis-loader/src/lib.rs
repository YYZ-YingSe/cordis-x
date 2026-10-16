//! Declarative configuration over the existing Cordis runtime lifecycle.
//! No language, invocation protocol, or execution host is selected by this crate.
mod artifact;
mod config;
mod loader;
pub mod package;
mod registry;
pub mod rolling;
mod target;
#[cfg(feature = "watch")]
pub mod watch;

pub use artifact::ArtifactRef;
pub use config::{Configuration, Entry, Intercept, Isolation, Realm};
pub use loader::{
    ApplyReport, EntrySnapshot, Loader, LoaderChanges, LoaderSnapshot, Phase, Plan,
    PreparationStats, ResolvedEntry,
};
pub use registry::Registry;
pub use serde_json::{Value, json};
pub use target::{Commit, ConfigurationChanges, ConfigurationTarget};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failure {
    pub entry: String,
    pub error: cordis_runtime::Error,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidConfig(String),
    Factory { entry: String, message: String },
    Artifact(String),
    Preparation(String),
    Runtime(cordis_runtime::Error),
    Apply(Vec<Failure>),
    QueueFull,
    Stopped,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfig(message) => f.write_str(message),
            Self::Artifact(message) | Self::Preparation(message) => f.write_str(message),
            Self::Factory { entry, message } => write!(f, "entry {entry}: {message}"),
            Self::Runtime(error) => error.fmt(f),
            _ => write!(f, "{self:?}"),
        }
    }
}
impl std::error::Error for Error {}
impl From<cordis_runtime::Error> for Error {
    fn from(error: cordis_runtime::Error) -> Self {
        Self::Runtime(error)
    }
}
