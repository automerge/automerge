//! `PrefixIter::advance_prefix(n)` seeks the item holding unit `n + 1`
//! past the iterator's position, adding `n + 1` to its running total
//! unchecked. For an `n` near the prefix type's limit that sum wrapped:
//! release builds landed on an early item (item 0 from the start), and
//! debug builds panicked. There is no such unit, so the iterator now runs
//! to its end and returns `None`, as for any `n` past the column's total.

use hexane::PrefixColumn;

fn ones() -> PrefixColumn<u32> {
    PrefixColumn::<u32>::from_values(vec![1, 1, 1])
}

#[test]
fn advancing_past_the_limit_from_the_start() {
    let col = ones();
    let mut it = col.iter();
    assert!(it.advance_prefix(u64::MAX).is_none());
    assert!(it.next().is_none());
}

#[test]
fn advancing_past_the_limit_from_further_on() {
    let col = ones();
    let mut it = col.iter();
    it.next();
    assert!(it.advance_prefix(u64::MAX - 1).is_none());
    assert!(it.next().is_none());
    // with the running total reset, too
    let mut it = col.iter();
    it.next();
    it.reset_prefix();
    assert!(it.advance_prefix(u64::MAX).is_none());
    assert!(it.next().is_none());
}

#[test]
fn advancing_within_the_column_is_unchanged() {
    let col = ones();
    let mut it = col.iter();
    assert_eq!(it.advance_prefix(1).map(|s| s.pos), Some(1));
    assert_eq!(it.advance_prefix(0).map(|s| s.pos), Some(2));
    assert!(it.advance_prefix(5).is_none());
}
