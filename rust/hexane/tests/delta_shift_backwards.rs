//! `DeltaIter::shift` moves only forwards, as its docs say: a range that
//! starts behind the iterator yields from where it stands, as for the
//! other column iterators. It panicked in `advance_to` instead.

use hexane::{Column, DeltaColumn, PrefixColumn, Shiftable};

#[test]
fn delta_shift_from_behind_yields_from_the_iterator() {
    let col = DeltaColumn::<u64>::from_values(vec![10, 20, 30, 40, 50]);
    let mut it = col.iter();
    it.nth(2);
    it.shift(1..4);
    assert_eq!(it.collect::<Vec<_>>(), vec![40]);
}

#[test]
fn delta_shift_agrees_with_the_other_iterators() {
    let values: Vec<u64> = (0..200).map(|i| i * 3 % 17).collect();
    let plain = Column::<u64>::from_values(values.clone());
    let prefix = PrefixColumn::<u64>::from_values(values.clone());
    let delta = DeltaColumn::<u64>::from_values(values.clone());
    for (at, range) in [
        (0, 0..5),
        (50, 10..60),
        (50, 50..50),
        (120, 7..300),
        (199, 0..1),
    ] {
        let mut a = plain.iter();
        let mut b = prefix.iter();
        let mut c = delta.iter();
        if at > 0 {
            a.nth(at - 1);
            b.nth(at - 1);
            c.nth(at - 1);
        }
        a.shift(range.clone());
        b.shift(range.clone());
        c.shift(range.clone());
        let want: Vec<u64> = a.collect();
        assert_eq!(b.map(|v| v.value).collect::<Vec<_>>(), want);
        assert_eq!(c.collect::<Vec<_>>(), want);
        // and through the trait, which never panicked
        let mut d = delta.iter();
        if at > 0 {
            d.nth(at - 1);
        }
        Shiftable::shift(&mut d, range);
        assert_eq!(d.collect::<Vec<_>>(), want);
    }
}

#[test]
fn delta_advance_to_behind_stays() {
    let col = DeltaColumn::<u64>::from_values(vec![10, 20, 30, 40, 50]);
    let mut it = col.iter();
    it.nth(2);
    it.advance_to(1);
    assert_eq!(it.pos(), 3);
    assert_eq!(it.collect::<Vec<_>>(), vec![40, 50]);
}
