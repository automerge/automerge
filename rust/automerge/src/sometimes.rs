//! Lightweight semantic coverage instrumentation.
//!
//! The [`crate::sometimes!`] macro records that an interesting program state
//! was reached. This is primarily intended for fuzzing and test feedback: unlike
//! source coverage, these labels are deliberately placed around Automerge
//! semantic situations such as conflicts.
//!
//! Recording is enabled by the `fuzzing` feature. Without that feature the macro
//! compiles to a no-op.

/// A semantic coverage label reached during one execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SometimesHit {
    pub name: &'static str,
    pub count: u64,
}

/// A semantic coverage label registered at a `sometimes!(...)` callsite.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SometimesLabel {
    pub name: &'static str,
}

#[cfg(feature = "fuzzing")]
thread_local! {
    static HITS: std::cell::RefCell<std::collections::BTreeMap<&'static str, u64>> =
        const { std::cell::RefCell::new(std::collections::BTreeMap::new()) };
}

/// Return all known semantic coverage labels registered by `sometimes!(...)`
/// callsites in this binary.
///
/// On ELF platforms this is populated by statics emitted into a custom linker
/// section by the macro. Each `sometimes!(...)` callsite expands inside its own
/// block, so the macro can emit a local `static` item with the same Rust
/// identifier at every callsite without name collisions. `#[used]` keeps those
/// statics alive, and `#[link_section = "automerge_sometimes"]` places all of
/// them contiguously in one linker section.
///
/// ELF linkers synthesize `__start_<section>` and `__stop_<section>` symbols for
/// sections whose names are valid C identifiers. For the `automerge_sometimes`
/// section, those symbols point at the first `SometimesLabel` and one-past the
/// last `SometimesLabel`, respectively. `registered_labels()` treats that range
/// as a slice.
///
/// The result is sorted and deduplicated because the same label may appear at
/// multiple callsites.
pub fn known_labels() -> Vec<&'static str> {
    #[cfg(all(feature = "fuzzing", target_family = "unix", not(target_os = "macos")))]
    {
        let mut labels = registered_labels();
        labels.sort_unstable();
        labels.dedup();
        labels
    }

    #[cfg(not(all(feature = "fuzzing", target_family = "unix", not(target_os = "macos"))))]
    Vec::new()
}

#[cfg(all(feature = "fuzzing", target_family = "unix", not(target_os = "macos")))]
fn registered_labels() -> Vec<&'static str> {
    #[allow(improper_ctypes)]
    unsafe extern "C" {
        static __start_automerge_sometimes: SometimesLabel;
        static __stop_automerge_sometimes: SometimesLabel;
    }

    unsafe {
        let start = std::ptr::addr_of!(__start_automerge_sometimes);
        let stop = std::ptr::addr_of!(__stop_automerge_sometimes);
        let len = stop.offset_from(start) as usize;
        std::slice::from_raw_parts(start, len)
            .iter()
            .map(|label| label.name)
            .collect()
    }
}

/// Record that a semantic coverage label was reached.
#[inline]
pub fn hit(name: &'static str) {
    #[cfg(feature = "fuzzing")]
    HITS.with(|hits| {
        *hits.borrow_mut().entry(name).or_default() += 1;
    });

    #[cfg(not(feature = "fuzzing"))]
    let _ = name;
}

/// Clear all hits recorded on the current thread.
#[inline]
pub fn reset() {
    #[cfg(feature = "fuzzing")]
    HITS.with(|hits| hits.borrow_mut().clear());
}

/// Take all hits recorded on the current thread.
#[inline]
pub fn take_hits() -> Vec<SometimesHit> {
    #[cfg(feature = "fuzzing")]
    {
        HITS.with(|hits| {
            std::mem::take(&mut *hits.borrow_mut())
                .into_iter()
                .map(|(name, count)| SometimesHit { name, count })
                .collect()
        })
    }

    #[cfg(not(feature = "fuzzing"))]
    Vec::new()
}
