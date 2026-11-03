//! Heap usage as the allocator sees it.
//!
//! A process's resident memory also counts memory the allocator has freed but kept, so it
//! cannot tell memory the program still holds from memory merely retained. These figures can.
//! Only glibc reports them (2.33 or later, which a glibc build then needs to start); on other
//! targets there are none.

/// Allocator heap usage, in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeapStats {
    /// Allocated and not yet freed.
    pub in_use: u64,
    /// Freed, but still held by the allocator rather than returned to the OS.
    pub free: u64,
    /// Obtained from the OS for the heap: arenas plus memory-mapped allocations.
    pub mapped: u64,
}

/// Current heap usage, if the allocator reports it.
///
/// Walks the allocator's free lists while holding each arena's lock, so it is meant for a
/// periodic refresh, not for every request.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub fn heap_stats() -> Option<HeapStats> {
    // SAFETY: mallinfo2 takes no arguments and only reads allocator state.
    let info = unsafe { libc::mallinfo2() };
    // Memory-mapped allocations (`hblkhd`) are always in use; `uordblks` and `fordblks` cover
    // the arenas only.
    Some(HeapStats {
        in_use: (info.uordblks + info.hblkhd) as u64,
        free: info.fordblks as u64,
        mapped: (info.arena + info.hblkhd) as u64,
    })
}

/// Current heap usage, if the allocator reports it.
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub fn heap_stats() -> Option<HeapStats> {
    None
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

/// Returns the allocator's free memory to the OS where it can. Returns whether any was released.
///
/// Holds each arena's lock while it runs, which can take a while on a large heap, so call it
/// off the async runtime.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub fn release_free_heap() -> bool {
    // SAFETY: malloc_trim only reorganises the allocator's own free memory.
    unsafe { libc::malloc_trim(0) == 1 }
}

/// Returns the allocator's free memory to the OS where it can. Returns whether any was released.
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub fn release_free_heap() -> bool {
    false
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
        assert!(!release_free_heap());
    }
}
