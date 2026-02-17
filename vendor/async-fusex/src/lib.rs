//! Implementation of FUSE library

#[allow(clippy::tests_outside_test_module)]
mod abi_marker;
mod context;
mod de;

pub mod file_system;

// ioctl_read!() macro involves inter arithmetic
#[allow(clippy::arithmetic_side_effects)]
pub mod channel;
pub mod fuse_reply;
pub mod fuse_request;
pub mod mount;
// ioctl_read!() macro involves inter arithmetic
pub mod error;
pub mod fs_util;
#[allow(clippy::arithmetic_side_effects)]
pub mod protocol;
pub mod session;
mod util;
mod virtual_fs;
mod fuse_fs;

pub use virtual_fs::*;
pub use fuse_fs::*;
