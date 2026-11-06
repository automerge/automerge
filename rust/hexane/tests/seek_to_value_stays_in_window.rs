//! `Iter::seek_to_value(target, range)` searches an iterator's sorted
//! window for `target` and moves the iterator to the range of matches (or
//! to where `target` would go). An explicit `range` reaching past the end
//! of the window searched past it: in a window `0..3` of
//! `[1, 2, 4, 4, 5]`, seeking 5 in `0..5` returned `4..5`, outside the
//! window, while the iterator, which cannot pass its window's end, stopped
//! at 3; and seeking 4 returned `2..4`. The search now stays in the window.

use hexane::Column;

#[test]
fn a_seek_past_the_window_stays_in_it() {
    let col = Column::<u64>::from_values(vec![1, 2, 4, 4, 5]);
    // 5 lies past the window: where it would go is the window's end
    let mut it = col.iter_range(0..3);
    assert_eq!(it.seek_to_value(5, 0..5), 3..3);
    assert_eq!(it.pos(), 3);
    assert_eq!(it.next(), None);
    // the matches are those in the window
    let mut it = col.iter_range(0..3);
    assert_eq!(it.seek_to_value(4, 0..5), 2..3);
    assert_eq!(it.pos(), 2);
    assert_eq!(it.collect::<Vec<_>>(), vec![4]);
}

#[test]
fn a_seek_inside_the_window_is_unchanged() {
    let col = Column::<u64>::from_values(vec![1, 2, 4, 4, 5]);
    let mut it = col.iter();
    assert_eq!(it.seek_to_value(4, 0..5), 2..4);
    assert_eq!(it.pos(), 2);
    let mut it = col.iter_range(0..3);
    assert_eq!(it.seek_to_value(4, ..), 2..3);
}
