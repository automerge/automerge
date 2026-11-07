//! A default `PrefixIter` is empty: `next`, `nth` and `next_run` return
//! `None`, and so must `advance_prefix`, which runs to the end of its
//! window and returns `None` when no item's running sum passes the target.

use hexane::PrefixIter;

#[test]
fn advancing_a_default_prefix_iterator_finds_nothing() {
    let mut it = PrefixIter::<u64>::default();
    assert!(it.advance_prefix(0).is_none());
    assert!(it.advance_prefix(5).is_none());
    assert!(it.next().is_none());
}
