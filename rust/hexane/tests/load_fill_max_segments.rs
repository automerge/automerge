//! A load's segment budget (`LoadOpts::with_max_segments`) holds for the
//! column it returns whichever way the column is built, a fill included:
//! later edits cut slabs to that budget.

use hexane::{Column, LoadOpts, PrefixColumn};

fn widest(segments: Vec<usize>) -> usize {
    segments.into_iter().max().unwrap_or(0)
}

#[test]
fn a_filled_load_keeps_the_segment_budget() {
    let opts = || LoadOpts::new().with_length(10).with_max_segments(4);

    let mut filled = Column::<Option<u64>>::load_with(&[], opts().with_fill(None)).unwrap();
    filled.splice(0, 0, (0..200u64).map(Some));
    assert_eq!(widest(filled.slab_segments()), 4);

    // the same values loaded from bytes, for comparison
    let bytes = Column::<Option<u64>>::from_values(vec![None; 10]).save();
    let mut loaded = Column::<Option<u64>>::load_with(&bytes, opts()).unwrap();
    loaded.splice(0, 0, (0..200u64).map(Some));
    assert_eq!(filled.slab_segments(), loaded.slab_segments());

    let mut bools = Column::<bool>::load_with(&[], opts().with_fill(true)).unwrap();
    bools.splice(0, 0, (0..200).map(|i| i % 2 == 0));
    assert!(widest(bools.slab_segments()) <= 4);

    let mut prefix = PrefixColumn::<bool>::load_with(&[], opts().with_fill(false)).unwrap();
    prefix.splice(0, 0, (0..200).map(|i| i % 2 == 0));
    assert!(widest(prefix.values().slab_segments()) <= 4);
}
