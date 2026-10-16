pub use cordis_contract::{ErrorInfo, Failure, error, refused};
pub type Result<T> = std::result::Result<T, Failure>;
