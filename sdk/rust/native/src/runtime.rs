//! Host-side installation, runtime management and lifecycle observations.
pub use cordis_kernel::{Activation, FiberId, Lifecycle, Outcome, Snapshot};
pub use cordis_runtime::{
    OwnershipChange, Plugin, PluginHandle, RealmChange, RealmReport, Runtime, WeakPluginHandle,
};
