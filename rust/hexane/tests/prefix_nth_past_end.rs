//! `PrefixIter::nth` past the column's end, in a window that reaches
//! past it, consumes the rest of the column: its running total must
//! count those values, as stepping with `next` does.

use hexane::PrefixColumn;

fn sum(values: &[u64]) -> u128 {
    values.iter().map(|v| *v as u128).sum()
}

#[test]
fn nth_past_end_counts_the_rest() {
    let values = vec![1u64, 2, 3];
    let col = PrefixColumn::<u64>::from_values(values.clone());
    let mut it = col.iter();
    it.set_max(10);
    assert!(it.nth(5).is_none());
    assert_eq!(it.pos(), 3);
    assert_eq!(it.total(), sum(&values));
}

#[test]
fn nth_past_end_across_slabs() {
    let values: Vec<u64> = (0..2000).map(|i| (i % 13) + 1).collect();
    let col = PrefixColumn::<u64>::from_values(values.clone());
    for start in [0, 1, 700, 1999] {
        let mut by_next = col.iter();
        by_next.set_max(5000);
        by_next.advance_by(start);
        while by_next.next().is_some() {}

        let mut it = col.iter();
        it.set_max(5000);
        it.advance_by(start);
        assert!(it.nth(2500).is_none());
        assert_eq!(it.pos(), by_next.pos());
        assert_eq!(it.total(), by_next.total(), "from {start}");
        assert_eq!(it.total(), sum(&values));
    }
}

#[test]
fn nth_past_end_keeps_the_reset_offset() {
    let values: Vec<u64> = (0..2000).map(|i| (i % 7) + 1).collect();
    let col = PrefixColumn::<u64>::from_values(values.clone());
    let mut it = col.iter();
    it.set_max(5000);
    it.advance_by(300);
    it.reset_prefix();
    assert!(it.nth(4000).is_none());
    assert_eq!(it.total(), sum(&values[300..]));
}
