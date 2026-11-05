//! Heap usage as the allocator sees it.
//!
//! A process's resident memory also counts memory the allocator has freed but kept, so it
//! cannot tell memory the program still holds from memory merely retained. These figures can.
//! glibc reports them (2.33 or later, which a glibc build then needs to start). Other
//! allocators report them once the application registers them with [`use_allocator`]; without
//! one there are none.
//!
//! The figures mean the same thing whichever allocator reports them, so dashboards need not
//! care which one it is. [`allocator_name`] says which, for the details that differ.

use std::sync::OnceLock;

/// Allocator heap usage, in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeapStats {
    /// Allocated and not yet freed.
    pub in_use: u64,
    /// Held by the allocator rather than in use: memory it has freed but not returned to the OS.
    /// An allocator that cannot tell that from fragmentation inside pages it still uses reports
    /// both, so releasing returns at most this much.
    pub free: u64,
    /// Held from the OS for the heap: in use plus free, plus any overhead the allocator counts.
    /// An allocator that cannot measure it reports an estimate.
    pub mapped: u64,
    /// Address space the allocator has returned to the OS but still keeps mapped. It is not
    /// resident, so it does not count towards `mapped`. `None` where the allocator does not
    /// report it.
    pub retained: Option<u64>,
}

/// An allocator's heap statistics and release function, for [`use_allocator`].
#[derive(Debug, Clone, Copy)]
pub struct HeapAllocator {
    /// Allocator name, exported as the `allocator` label of `sv2_heap_allocator_info`.
    pub name: &'static str,
    /// Current heap usage. Called for each periodic refresh of the monitoring snapshot.
    pub stats: fn() -> Option<HeapStats>,
    /// Returns free memory to the OS where it can, and whether it did. An allocator whose
    /// release cannot tell how much it returned reports whether the release ran, so `true`
    /// does not promise that memory was freed: compare [`resident_bytes`] before and after.
    pub release: fn() -> bool,
}

static REGISTERED: OnceLock<HeapAllocator> = OnceLock::new();

/// Reports and releases heap memory through `allocator` instead of the default for this target.
///
/// Call it before the monitoring server starts, since the heap gauges exist only if the
/// allocator reports statistics when the server starts. Only the first call takes effect; a
/// later one gives its allocator back.
pub fn use_allocator(allocator: HeapAllocator) -> Result<(), HeapAllocator> {
    REGISTERED.set(allocator)
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
static BUILT_IN: Option<HeapAllocator> = Some(HeapAllocator {
    name: "glibc",
    stats: glibc_heap_stats,
    release: glibc_release_free_heap,
});

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
static BUILT_IN: Option<HeapAllocator> = None;

fn active() -> Option<&'static HeapAllocator> {
    REGISTERED.get().or(BUILT_IN.as_ref())
}

/// Name of the allocator whose heap figures are exported, if it reports any.
pub fn allocator_name() -> Option<&'static str> {
    active().map(|allocator| allocator.name)
}

/// Current heap usage, if the allocator reports it.
///
/// May be costly, since an allocator can walk its free lists under a lock to produce it, so it
/// is meant for a periodic refresh, not for every request.
pub fn heap_stats() -> Option<HeapStats> {
    (active()?.stats)()
}

/// Walks the free lists while holding each arena's lock.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn glibc_heap_stats() -> Option<HeapStats> {
    // SAFETY: mallinfo2 takes no arguments and only reads allocator state.
    let info = unsafe { libc::mallinfo2() };
    // Memory-mapped allocations (`hblkhd`) are always in use; `uordblks` and `fordblks` cover
    // the arenas only.
    Some(HeapStats {
        in_use: (info.uordblks + info.hblkhd) as u64,
        free: info.fordblks as u64,
        mapped: (info.arena + info.hblkhd) as u64,
        retained: None,
    })
}

/// The process's resident memory in bytes, where the OS reports it (Linux).
///
/// Releasing free heap memory can lower it without changing [`heap_stats`], since glibc returns
/// pages from the middle of the heap without shrinking the heap itself.
#[cfg(target_os = "linux")]
pub fn resident_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    // SAFETY: sysconf only reads a system constant.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    u64::try_from(page_size).ok().map(|size| pages * size)
}

/// The process's resident memory in bytes, where the OS reports it (Linux).
#[cfg(not(target_os = "linux"))]
pub fn resident_bytes() -> Option<u64> {
    None
}

/// Returns the allocator's free memory to the OS where it can. Returns whether any was released,
/// or for an allocator that cannot tell, whether the release ran (see [`HeapAllocator::release`]).
///
/// Holds allocator locks while it runs, which can take a while on a large heap, so call it off
/// the async runtime.
pub fn release_free_heap() -> bool {
    active().is_some_and(|allocator| (allocator.release)())
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn glibc_release_free_heap() -> bool {
    // SAFETY: malloc_trim only reorganises the allocator's own free memory.
    unsafe { libc::malloc_trim(0) == 1 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    #[test]
    fn heap_stats_count_a_live_allocation_as_in_use() {
        // Large enough that other tests freeing memory meanwhile cannot hide it.
        const SIZE: u64 = 256 << 20;
        let before = heap_stats().unwrap();
        let block = vec![1u8; SIZE as usize];
        let during = heap_stats().unwrap();
        assert!(during.in_use >= before.in_use + SIZE / 2);
        assert!(during.mapped >= during.in_use);
        assert_eq!(during.retained, None);
        assert_eq!(allocator_name(), Some("glibc"));
        drop(block);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn resident_memory_is_reported() {
        assert!(resident_bytes().unwrap() > 0);
    }

    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    #[test]
    fn other_allocators_report_nothing() {
        assert_eq!(heap_stats(), None);
        assert_eq!(allocator_name(), None);
        assert!(!release_free_heap());
    }
}
