//! Encoding-independent identities and structural interface descriptors.

use std::{fmt, sync::Arc};

pub mod budget;
mod descriptor;
mod failure;
pub use failure::{ErrorInfo, Failure, error, refused};
pub mod portable;
pub mod protobuf;
mod validation;
pub use descriptor::{
    Control, Descriptor, Difference, Dispatch, Endpoint, Interface, Method, Resource,
};
pub use protobuf::MessageContract;
pub use validation::Validation;

/// Both labels participate in routing identity; structural validation uses `Descriptor`.
/// A revision is not a semver range or a substitute for an interface description.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContractId {
    pub name: Arc<str>,
    pub revision: Arc<str>,
}

impl ContractId {
    pub fn new(name: impl Into<Arc<str>>, revision: impl Into<Arc<str>>) -> Self {
        Self {
            name: name.into(),
            revision: revision.into(),
        }
    }
}

impl fmt::Display for ContractId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.name, self.revision)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RealmId(pub u64);

impl RealmId {
    pub const ROOT: Self = Self(0);
}

/// Exclusive service identity within one realm.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Slot {
    pub contract: ContractId,
    pub realm: RealmId,
}

impl Slot {
    pub fn new(contract: ContractId, realm: RealmId) -> Self {
        Self { contract, realm }
    }
}
