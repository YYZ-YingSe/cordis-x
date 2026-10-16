//! Host-selected business validation. This never grants runtime authority.
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Validation {
    /// The producer promises the business contract; ordinary codec safety remains.
    Contract,
    /// Check declared fields and logical budgets at the receiving/returning binding.
    #[default]
    Checked,
    /// Also reject unknown business and carrier fields at receiving boundaries.
    Audit,
}
impl Validation {
    pub const fn checks_fields(self) -> bool {
        !matches!(self, Self::Contract)
    }
    pub const fn audits(self) -> bool {
        matches!(self, Self::Audit)
    }
    /// Generated DTO contracts describe data; the host binding chooses checks.
    pub const fn contract() -> Self {
        Self::Contract
    }
}
