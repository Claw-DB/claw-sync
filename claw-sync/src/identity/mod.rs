//! identity/mod.rs — re-exports for the identity subsystem (device and workspace).

pub mod device;
pub mod workspace;

pub use device::DeviceIdentity;
pub use workspace::WorkspaceIdentity;
