//! jemalloc as the translator's allocator, with its configuration baked in.

use std::ffi::c_char;

#[cfg(feature = "monitoring")]
use stratum_apps::monitoring::heap::{self, HeapAllocator};
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

/// jemalloc reads its default options from a C string named `malloc_conf`, which tikv-jemalloc-sys
/// builds with the symbol prefix `_rjem_`. The `_RJEM_MALLOC_CONF` environment variable is
/// applied after it, so it still overrides these.
///
/// - `background_thread`: a background thread returns freed pages to the OS even when the process
///   allocates nothing. It is off by default, and not enabled off Linux, where it is untested.
/// - `thp:never` (Linux only): jemalloc opts its own memory out of transparent huge pages, which
///   keep it from returning most of what it frees, even where the host sets them to `always`.
///
/// The decay times are left at jemalloc's defaults: freed pages return to the OS after 10 s
/// (`dirty_decay_ms`), with no lazily-freed stage in between (`muzzy_decay_ms:0`).
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static malloc_conf: Option<&'static c_char> = {
    #[cfg(target_os = "linux")]
    const CONF: &[u8] = b"background_thread:true,thp:never\0";
    #[cfg(not(target_os = "linux"))]
    const CONF: &[u8] = b"\0";
    // SAFETY: the string is static and nul-terminated.
    Some(unsafe { &*CONF.as_ptr().cast::<c_char>() })
};

/// Stops the heap gauges and SIGUSR1 from reading glibc's allocator, which this build no longer
/// uses. Call it before the monitoring server starts.
#[cfg(feature = "monitoring")]
pub fn report_no_heap_stats() {
    // Only the first registration takes effect, and every one is this same allocator.
    let _ = heap::use_allocator(HeapAllocator {
        stats: || None,
        release: || false,
    });
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    use std::ffi::{CStr, c_char};

    #[test]
    fn malloc_conf_is_applied() {
        #[cfg(target_os = "linux")]
        {
            // SAFETY: `opt.background_thread` is a bool and `opt.thp` a nul-terminated string.
            let background_thread = unsafe { mallctl_read(b"opt.background_thread\0", false) };
            assert!(background_thread);
            // jemalloc reports `opt.thp` as unsupported where the kernel has no THP setting.
            if std::fs::read_to_string("/sys/kernel/mm/transparent_hugepage/enabled").is_ok() {
                let thp = unsafe { mallctl_read(b"opt.thp\0", std::ptr::null::<c_char>()) };
                // SAFETY: jemalloc returns a pointer to a static nul-terminated string.
                let thp = unsafe { CStr::from_ptr(thp) };
                assert_eq!(thp.to_bytes(), b"never");
            }
        }
    }

    /// # Safety
    /// `name` must be nul-terminated and name an option of type `T`.
    #[cfg(target_os = "linux")]
    unsafe fn mallctl_read<T>(name: &[u8], mut value: T) -> T {
        let mut len = std::mem::size_of::<T>();
        // SAFETY: guaranteed by the caller; `value` and `len` describe a `T`.
        let status = unsafe {
            tikv_jemalloc_sys::mallctl(
                name.as_ptr().cast(),
                std::ptr::from_mut(&mut value).cast(),
                &mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        assert_eq!(status, 0);
        value
    }
}
