//! Heap tuning for glibc malloc.
//!
//! glibc gives every thread that allocates its own arena (64 MiB heaps, up to
//! eight per CPU) and returns memory to the kernel only from the top of each
//! heap. The daemon allocates chunk buffers, write batches and metadata from
//! dozens of tokio, FUSE and rayon threads, so freed memory stayed resident in
//! half-empty arenas: the RSS grew to about 2.5 times the chunk cache budget
//! and never came back. Capping the arenas bounds that, and `trim` hands the
//! free pages inside every heap back to the kernel.

use anyhow::{bail, Result};
use nix::libc;

/// Arenas shared by all threads.
const ARENA_MAX: libc::c_int = 2;

/// Must run before the process starts other threads.
pub(crate) fn configure() -> Result<()> {
    // SAFETY: mallopt only changes allocator parameters.
    if unsafe { libc::mallopt(libc::M_ARENA_MAX, ARENA_MAX) } != 1 {
        bail!("mallopt(M_ARENA_MAX, {ARENA_MAX}) failed");
    }
    Ok(())
}

/// Returns free heap pages to the kernel.
pub(crate) fn trim() {
    // SAFETY: malloc_trim takes the arena locks itself.
    unsafe { libc::malloc_trim(0) };
}

/// Heap readings from glibc.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HeapStats {
    /// Bytes in live allocations, mmapped blocks included.
    pub allocated: u64,
    /// Free bytes inside the heaps. `trim` returns their whole pages to the
    /// kernel, so after a trim only part of them is still resident.
    pub free: u64,
}

pub(crate) fn heap_stats() -> HeapStats {
    // SAFETY: mallinfo2 only reads allocator counters.
    let info = unsafe { libc::mallinfo2() };
    HeapStats {
        allocated: (info.uordblks + info.hblkhd) as u64,
        free: info.fordblks as u64,
    }
}
