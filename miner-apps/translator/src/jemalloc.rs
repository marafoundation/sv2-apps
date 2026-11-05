//! jemalloc as the translator's allocator: its configuration, and the heap statistics and
//! release function the monitoring gauges and SIGUSR1 use.

use std::ffi::c_char;

#[cfg(feature = "monitoring")]
use stratum_apps::monitoring::heap::{self, HeapAllocator, HeapStats};
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

/// jemalloc reads its default options from a C string named `malloc_conf`, which tikv-jemalloc-sys
/// builds with the symbol prefix `_rjem_`. The `_RJEM_MALLOC_CONF` environment variable is
/// applied after it, so it still overrides these.
///
/// - `background_thread` (not enabled off Linux, where it is untested): a background thread purges
///   freed pages, so they are returned even when the process allocates nothing.
/// - `dirty_decay_ms`: the background thread returns freed pages to the OS after about five
///   seconds.
/// - `muzzy_decay_ms`: no lazily-freed stage in between.
/// - `thp:never` (Linux only): jemalloc opts its own memory out of transparent huge pages, which
///   keep it from returning most of what it frees, even where the host sets them to `always`.
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static malloc_conf: Option<&'static c_char> = {
    #[cfg(target_os = "linux")]
    const CONF: &[u8] = b"background_thread:true,dirty_decay_ms:5000,muzzy_decay_ms:0,thp:never\0";
    #[cfg(not(target_os = "linux"))]
    const CONF: &[u8] = b"dirty_decay_ms:5000,muzzy_decay_ms:0\0";
    // SAFETY: the string is static and nul-terminated.
    Some(unsafe { &*CONF.as_ptr().cast::<c_char>() })
};

/// Makes the heap gauges and SIGUSR1 report and release jemalloc's memory. Call it before the
/// monitoring server starts.
pub fn register_heap_allocator() {
    // Only the first registration takes effect, and every one is this same allocator.
    #[cfg(feature = "monitoring")]
    let _ = heap::use_allocator(HeapAllocator {
        name: "jemalloc",
        stats: heap_stats,
        release: release_free_heap,
    });
}

/// jemalloc's counters are mapped onto the generic heap figures as follows:
///
/// - in use is `stats.allocated`;
/// - free is `stats.active` minus allocated, plus the dirty and muzzy pages: fragmentation inside
///   active pages, and freed pages not yet returned to the OS;
/// - mapped is `stats.active` plus the dirty and muzzy pages and `stats.metadata`, an estimate of
///   what jemalloc keeps resident. It is `stats.resident` with metadata counted as allocated rather
///   than as touched;
/// - retained is `stats.retained`.
///
/// Free and mapped count whole jemalloc pages. Those are 64 KiB on aarch64 builds, so on a
/// kernel with 4 KiB pages they also count the untouched parts of partly used pages and
/// overstate; in use is exact. Where the page sizes match, as on x86_64, they should not.
#[cfg(feature = "monitoring")]
fn heap_stats() -> Option<HeapStats> {
    use tikv_jemalloc_ctl::{epoch, raw};

    // Statistics are cached until the epoch advances.
    epoch::advance().ok()?;
    let read = |name: &[u8]| {
        // SAFETY: every name is nul-terminated and holds a `size_t`.
        unsafe { raw::read::<usize>(name) }
            .ok()
            .map(|bytes| bytes as u64)
    };
    let page = read(b"arenas.page\0")?;
    // The statistics of every arena together.
    let dirty = read(b"stats.arenas.4096.pdirty\0")? * page;
    let muzzy = read(b"stats.arenas.4096.pmuzzy\0")? * page;
    let allocated = read(b"stats.allocated\0")?;
    let active = read(b"stats.active\0")?;
    let metadata = read(b"stats.metadata\0")?;
    Some(HeapStats {
        in_use: allocated,
        free: active.saturating_sub(allocated) + dirty + muzzy,
        mapped: active + dirty + muzzy + metadata,
        retained: Some(read(b"stats.retained\0")?),
    })
}

/// Purges every arena's dirty and muzzy pages, instead of waiting for them to decay. The purge
/// reports success whether or not it returned anything, so `true` means only that it ran.
#[cfg(feature = "monitoring")]
fn release_free_heap() -> bool {
    // MALLCTL_ARENAS_ALL, which jemalloc accepts in place of an arena index.
    const PURGE_ALL_ARENAS: &[u8] = b"arena.4096.purge\0";
    // SAFETY: the name is nul-terminated, and `arena.<i>.purge` neither reads nor writes a value,
    // so jemalloc requires the old and new value pointers to be null.
    let status = unsafe {
        tikv_jemalloc_sys::mallctl(
            PURGE_ALL_ARENAS.as_ptr().cast(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
        )
    };
    status == 0
}

#[cfg(target_os = "linux")]
const TRANSPARENT_HUGEPAGE_FILE: &str = "/sys/kernel/mm/transparent_hugepage/enabled";

/// Logs a warning if the kernel backs all memory with transparent huge pages. jemalloc's own
/// memory is opted out by the built-in `thp:never`, but other allocations and the kernel's
/// khugepaged can still use huge pages. Says nothing if the setting cannot be read.
#[cfg(target_os = "linux")]
pub fn check_transparent_hugepages() {
    let Ok(setting) = std::fs::read_to_string(TRANSPARENT_HUGEPAGE_FILE) else {
        return;
    };
    if transparent_hugepages_always(&setting) {
        tracing::warn!(
            "{TRANSPARENT_HUGEPAGE_FILE} is set to always. jemalloc's own memory is opted out of \
             transparent huge pages (thp:never), but other allocations and the kernel's \
             khugepaged can still use them, which can keep memory from being returned to the OS. \
             madvise is recommended, as described in the \"Memory allocator\" section of the \
             translator README"
        );
    }
}

/// Logs a warning if jemalloc's pages are larger than the kernel's, as with an aarch64 build on
/// a kernel with 4 KiB pages: the heap `free` and `mapped` gauges then count the untouched parts
/// of partly used pages too, and overstate.
#[cfg(all(target_os = "linux", feature = "monitoring"))]
pub fn check_page_size() {
    // SAFETY: `arenas.page` is a `size_t`.
    let Ok(jemalloc) = (unsafe { tikv_jemalloc_ctl::raw::read::<usize>(b"arenas.page\0") }) else {
        return;
    };
    // SAFETY: sysconf only reads a system constant.
    let Ok(kernel) = usize::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }) else {
        return;
    };
    if jemalloc_pages_larger(jemalloc, kernel) {
        tracing::warn!(
            "jemalloc uses {jemalloc}-byte pages and the kernel {kernel}-byte pages, so the \
             sv2_heap_free_bytes and sv2_heap_mapped_bytes gauges overstate; sv2_heap_in_use_bytes \
             is exact"
        );
    }
}

/// Whether jemalloc's pages are larger than the kernel's.
#[cfg(all(target_os = "linux", feature = "monitoring"))]
fn jemalloc_pages_larger(jemalloc: usize, kernel: usize) -> bool {
    jemalloc > kernel
}

/// Whether the contents of the transparent huge pages file select `always`, which the kernel
/// marks by bracketing it among the options, as in `always [madvise] never`.
#[cfg(target_os = "linux")]
fn transparent_hugepages_always(setting: &str) -> bool {
    setting.split_whitespace().any(|mode| mode == "[always]")
}

#[cfg(test)]
mod tests {

    #[cfg(all(target_os = "linux", feature = "monitoring"))]
    #[test]
    fn a_larger_jemalloc_page_is_flagged() {
        assert!(super::jemalloc_pages_larger(65536, 4096));
        assert!(!super::jemalloc_pages_larger(4096, 4096));
        assert!(!super::jemalloc_pages_larger(4096, 16384));
    }
    #[cfg(target_os = "linux")]
    #[test]
    fn only_the_selected_transparent_hugepage_mode_counts() {
        use super::transparent_hugepages_always;

        assert!(transparent_hugepages_always("[always] madvise never\n"));
        assert!(!transparent_hugepages_always("always [madvise] never\n"));
        assert!(!transparent_hugepages_always("always madvise [never]\n"));
        assert!(!transparent_hugepages_always(""));
    }

    #[test]
    fn malloc_conf_is_applied() {
        #[cfg(target_os = "linux")]
        use tikv_jemalloc_ctl::opt;
        use tikv_jemalloc_ctl::raw;

        // SAFETY: both options are signed sizes.
        let (dirty, muzzy) = unsafe {
            (
                raw::read::<isize>(b"opt.dirty_decay_ms\0").unwrap(),
                raw::read::<isize>(b"opt.muzzy_decay_ms\0").unwrap(),
            )
        };
        assert_eq!((dirty, muzzy), (5000, 0));
        #[cfg(target_os = "linux")]
        {
            assert!(opt::background_thread::read().unwrap());
            // jemalloc reports `opt.thp` as unsupported where the kernel has no THP setting.
            if std::fs::read_to_string("/sys/kernel/mm/transparent_hugepage/enabled").is_ok() {
                // SAFETY: `opt.thp` is a nul-terminated string.
                let thp = unsafe { raw::read_str(b"opt.thp\0") }.unwrap();
                assert_eq!(thp.strip_suffix(b"\0").unwrap_or(thp), b"never");
            }
        }
    }

    #[cfg(feature = "monitoring")]
    #[test]
    fn heap_stats_follow_a_large_allocation() {
        use super::{heap, register_heap_allocator};

        // Large enough that other tests allocating meanwhile cannot hide it.
        const SIZE: u64 = 256 << 20;
        register_heap_allocator();
        assert_eq!(heap::allocator_name(), Some("jemalloc"));

        let before = heap::heap_stats().unwrap();
        let block = vec![1u8; SIZE as usize];
        let during = heap::heap_stats().unwrap();
        assert!(during.in_use >= before.in_use + SIZE / 2);
        // What is left over is jemalloc's metadata.
        assert!(during.in_use + during.free <= during.mapped);
        assert!(during.mapped - during.in_use - during.free < SIZE);
        assert!(during.retained.is_some());
        drop(block);
    }

    #[cfg(feature = "monitoring")]
    #[test]
    fn purge_releases_without_error() {
        assert!(super::release_free_heap());
    }

    #[cfg(feature = "monitoring")]
    #[test]
    fn allocator_info_names_jemalloc() {
        use stratum_apps::monitoring::prometheus_metrics::PrometheusMetrics;

        super::register_heap_allocator();
        let metrics = PrometheusMetrics::new(false, false, false, false).unwrap();
        let families = metrics.registry.gather();
        let info = families
            .iter()
            .find(|family| family.get_name() == "sv2_heap_allocator_info")
            .unwrap();
        let label = &info.get_metric()[0].get_label()[0];
        assert_eq!(
            (label.get_name(), label.get_value()),
            ("allocator", "jemalloc")
        );
        assert!(
            families
                .iter()
                .any(|family| family.get_name() == "sv2_heap_retained_bytes")
        );
    }
}
