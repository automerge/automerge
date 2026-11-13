//! A column iterator's window can be extended past the column's end
//! (`set_max`, `shift`), and the iterator then stops at the end. But
//! `ExactSizeIterator::len` (and `size_hint`) counted the whole window:
//! on a five-item column, after `set_max(100)`, `len()` was 100 while the
//! iterator yielded five items. Now they count the items it will yield.

use hexane::{Column, DeltaColumn, PrefixColumn};

#[test]
fn len_after_set_max_past_the_end() {
    let col = Column::<u64>::from_values(vec![1, 2, 3, 4, 5]);
    let mut it = col.iter();
    it.set_max(100);
    assert_eq!(it.len(), 5);
    assert_eq!(it.size_hint(), (5, Some(5)));
    it.next();
    assert_eq!(it.len(), 4);
    assert_eq!(it.clone().count(), 4);
    let mut it = col.iter_range(1..3);
    it.set_max(100);
    assert_eq!(it.len(), 4);
    assert_eq!(it.count(), 4);
    let mut it = col.iter();
    it.set_max(3);
    assert_eq!(it.len(), 3);
}

#[test]
fn len_after_shift_past_the_end() {
    let col = Column::<bool>::from_values(vec![true, false, true, true]);
    let mut it = col.iter();
    it.shift(1..100);
    assert_eq!(it.len(), 3);
    assert_eq!(it.collect::<Vec<_>>(), vec![false, true, true]);
}

#[test]
fn wrapping_iterators_count_the_same() {
    let col = DeltaColumn::<u64>::from_values(vec![10, 11, 12]);
    let mut it = col.iter();
    it.set_max(50);
    assert_eq!(it.len(), 3);
    assert_eq!(it.count(), 3);
    let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4]);
    let mut it = col.iter();
    it.set_max(50);
    assert_eq!(it.len(), 4);
    assert_eq!(it.count(), 4);
}

#[test]
fn len_of_an_empty_column() {
    let col = Column::<u64>::new();
    let mut it = col.iter();
    it.set_max(10);
    assert_eq!(it.len(), 0);
    let mut it = hexane::Iter::<u64>::default();
    it.set_max(10);
    assert_eq!(it.len(), 0);
}
