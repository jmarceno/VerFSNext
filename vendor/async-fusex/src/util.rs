extern crate alloc;
use alloc::string::ToString;
use std::error::Error;
use std::fmt::Write;
use std::os::raw::c_int;
use nix::errno::Errno;
use nix::libc;
use nix::sys::stat::SFlag;

/// Format `anyhow::Error`
#[must_use]
#[inline]
pub fn format_anyhow_error(error: &anyhow::Error) -> String {
    let err_msg_vec = anyhow::Error::chain(error)
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let mut err_msg = String::new();
    let _ignore = write!(
        err_msg,
        "{}, root cause: {}",
        err_msg_vec.as_slice().join(", caused by: "),
        error.root_cause()
    );

    err_msg
}
/// Build file mode from `SFlag` and file permission
/// # Panics
/// Panics if `SFlag` is unknown
#[must_use]
pub fn mode_from_kind_and_perm(kind: SFlag, perm: u16) -> u32 {
    let file_type = match kind {
        SFlag::S_IFIFO => libc::S_IFIFO,
        SFlag::S_IFCHR => libc::S_IFCHR,
        SFlag::S_IFBLK => libc::S_IFBLK,
        SFlag::S_IFDIR => libc::S_IFDIR,
        SFlag::S_IFREG => libc::S_IFREG,
        SFlag::S_IFLNK => libc::S_IFLNK,
        SFlag::S_IFSOCK => libc::S_IFSOCK,
        _ => panic!("unknown SFlag type={kind:?}"),
    };
    let file_perm: u32 = perm.into();

    #[cfg(target_os = "linux")]
    {
        file_type | file_perm
    }
}

/// Convert `nix::errno::Errno` to `c_int`
#[allow(clippy::as_conversions)]
#[must_use]
pub const fn convert_nix_errno_to_cint(error_no: Errno) -> c_int {
    error_no as c_int
}


/// Round up `len` to a multiple of `align`.
///
/// <https://doc.rust-lang.org/std/alloc/struct.Layout.html#method.padding_needed_for>
///
/// <https://doc.rust-lang.org/src/core/alloc/layout.rs.html#226-250>
#[must_use]
pub const fn round_up(len: usize, align: usize) -> usize {
    len.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1)
}

/// Format `nix::Error`
// TODO: refactor this
#[must_use]
#[inline]
pub fn format_nix_error(error: nix::Error) -> String {
    format!("{}, root cause: {:?}", error, error.source())
}