//! Counters of the changes rebuilt from a document chunk's op set.
//!
//! A document chunk stores its ops, not its changes. To learn the changes'
//! hashes (and so to verify the stored heads), loading regroups every op into
//! the change it came from, re-encodes each change and hashes it. These
//! counters record how much of that work was done, so that tests can check
//! which operations do it without timing anything.
//!
//! The counters are per thread, so tests running concurrently on other
//! threads do not disturb them. They exist only in this crate's own tests and
//! with the `change-rebuild-counters` feature; otherwise they are compiled
//! away.

use std::cell::Cell;

thread_local! {
    static CHANGES: Cell<u64> = const { Cell::new(0) };
    static OPS: Cell<u64> = const { Cell::new(0) };
}

/// The number of changes this thread has rebuilt from a document chunk's
/// ops since the last [`reset`].
pub fn changes_rebuilt() -> u64 {
    CHANGES.with(Cell::get)
}

/// The number of ops, summed over the changes counted by
/// [`changes_rebuilt`], that this thread has rebuilt into changes since the
/// last [`reset`].
pub fn ops_rebuilt() -> u64 {
    OPS.with(Cell::get)
}

/// Sets this thread's counters to zero.
pub fn reset() {
    CHANGES.with(|c| c.set(0));
    OPS.with(|c| c.set(0));
}

/// Records that a change of `ops` ops has been rebuilt.
pub(crate) fn count_rebuilt(ops: u64) {
    CHANGES.with(|c| c.set(c.get() + 1));
    OPS.with(|c| c.set(c.get() + ops));
}
