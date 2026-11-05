//! `Unshift` holds the next item as a lookahead, so its inner iterator
//! sits one item further on. `Unshift::shift(range)` repositioned the
//! inner iterator to `range.start`, which asserts that the range doesn't
//! start behind it: a range starting at the lookahead's own item, the
//! item `peek` shows, panicked. Now that lookahead is kept.

use hexane::{Column, DeltaColumn, PrefixColumn, Shiftable};

#[test]
fn shift_to_a_range_starting_at_the_lookahead() {
    let col = Column::<u64>::from_values(vec![1, 2, 3, 4, 5]);
    let mut u = col.iter().unshift();
    assert_eq!(u.next(), Some(1));
    assert_eq!(u.peek(), Some(&2));
    u.shift(1..3);
    assert_eq!(u.peek(), Some(&2));
    assert_eq!(u.collect::<Vec<_>>(), vec![2, 3]);

    let mut u = col.iter().unshift();
    u.shift(0..100);
    assert_eq!(u.collect::<Vec<_>>(), vec![1, 2, 3, 4, 5]);
}

#[test]
fn an_empty_range_there_drops_it() {
    let col = Column::<u64>::from_values(vec![1, 2, 3, 4, 5]);
    let mut u = col.iter().unshift();
    u.next();
    u.shift(1..1);
    assert_eq!(u.peek(), None);
    assert_eq!(u.next(), None);
    u.shift(3..5);
    assert_eq!(u.collect::<Vec<_>>(), vec![4, 5]);
}

#[test]
fn ranges_after_it_work_as_before() {
    let col = Column::<u64>::from_values(vec![1, 2, 3, 4, 5]);
    let mut u = col.iter().unshift();
    u.next();
    u.shift(2..4);
    assert_eq!(u.collect::<Vec<_>>(), vec![3, 4]);
}

#[test]
fn delta_and_prefix_iterators_too() {
    let col = DeltaColumn::<u64>::from_values(vec![10, 20, 30, 40]);
    let mut u = col.iter().unshift();
    u.next();
    u.shift(1..3);
    assert_eq!(u.collect::<Vec<_>>(), vec![20, 30]);
    let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4]);
    let mut u = col.iter().unshift();
    u.next();
    u.shift(1..3);
    assert_eq!(u.map(|v| v.value).collect::<Vec<_>>(), vec![2, 3]);
}
