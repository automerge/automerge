//! A default iterator has no column. Widened with `set_max`, its window
//! reaches past the (empty) column's end, and `nth` must cleanly return
//! `None`, as `next` does, rather than panic looking for a column.

use hexane::{Iter, PrefixIter};

#[test]
fn default_iter_nth_past_end() {
    let mut it: Iter<'_, u64> = Iter::default();
    it.set_max(10);
    assert_eq!(it.nth(3), None);
    assert_eq!(it.next(), None);
    assert_eq!(it.pos(), 0);
}

#[test]
fn default_prefix_iter_nth_past_end() {
    let mut it: PrefixIter<'_, u64> = PrefixIter::default();
    it.set_max(10);
    assert!(it.nth(3).is_none());
    assert!(it.next().is_none());
    assert_eq!(it.pos(), 0);
    assert_eq!(it.total(), 0);
}

#[test]
fn default_iter_advance_and_shift() {
    let mut it: Iter<'_, u64> = Iter::default();
    it.shift(2..5);
    assert_eq!(it.next(), None);
    let mut it: PrefixIter<'_, u64> = PrefixIter::default();
    it.shift(2..5);
    assert!(it.next().is_none());
}
