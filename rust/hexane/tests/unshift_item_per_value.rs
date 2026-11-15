//! `Unshift::shift` takes its lookahead's position to be the one just
//! before its inner iterator's, which holds only where each item stands
//! for one value. Over a run adapter it gave wrong runs: over
//! `[7, 7, 7, 9, 9]`, `shift(2..5)` kept the lookahead `7 x 3`, a run
//! that starts before the range (`[7 x 3, 9 x 2]`, where `[7 x 1, 9 x 2]`
//! is the range's; without the kept lookahead, on `main`, it gave
//! `[9 x 2]`). It now asks for an inner iterator that is `ItemPerValue`,
//! which the run adapters are not (the doctest on `ItemPerValue` checks
//! that this no longer compiles), and still shifts over the column
//! iterators.

use hexane::{Column, DeltaColumn, ItemPerValue, PrefixColumn, Unshift};

fn shifted<T: ItemPerValue>(inner: T, range: std::ops::Range<usize>) -> Vec<T::Item> {
    let mut u = Unshift::new(inner);
    u.shift(range);
    u.collect()
}

#[test]
fn the_column_iterators_shift() {
    let values = vec![7u64, 7, 7, 9, 9];
    let plain = Column::<u64>::from_values(values.clone());
    let prefix = PrefixColumn::<u64>::from_values(values.clone());
    let delta = DeltaColumn::<u64>::from_values(values.clone());
    assert_eq!(shifted(plain.iter(), 2..5), vec![7, 9, 9]);
    assert_eq!(
        shifted(prefix.iter(), 2..5)
            .into_iter()
            .map(|v| v.value)
            .collect::<Vec<_>>(),
        vec![7, 9, 9]
    );
    assert_eq!(shifted(delta.iter(), 2..5), vec![7, 9, 9]);
}
