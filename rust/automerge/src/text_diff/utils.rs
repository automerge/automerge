// This file was copied from the similar crate at
// https://github.com/mitsuhiko/similar/blob/2b31f65445df9093ba007ca5a5ae6a71b899d491/src/algorithms/utils.rs
// The original license is in the LICENSE file in the same directory as this file
//
// The original file was modified to remove the `UniqueItem` and `IdentifyDistinct` types as well
// as the `unique` function as none of these were used by the myers diff implementation in this
// crate.
use std::ops::{Index, Range};

/// Utility function to check if a range is empty that works on older rust versions
#[inline(always)]
#[allow(clippy::neg_cmp_op_on_partial_ord)]
pub(super) fn is_empty_range<T: PartialOrd<T>>(range: &Range<T>) -> bool {
    !(range.start < range.end)
}

/// Given two lookups and ranges calculates the length of the common prefix.
pub(super) fn common_prefix_len<Old, New>(
    old: &Old,
    old_range: Range<usize>,
    new: &New,
    new_range: Range<usize>,
) -> usize
where
    Old: Index<usize> + ?Sized,
    New: Index<usize> + ?Sized,
    New::Output: PartialEq<Old::Output>,
{
    if is_empty_range(&old_range) || is_empty_range(&new_range) {
        return 0;
    }
    new_range
        .zip(old_range)
        .take_while(
            #[inline(always)]
            |x| new[x.0] == old[x.1],
        )
        .count()
}

/// Given two lookups and ranges calculates the length of common suffix.
pub(super) fn common_suffix_len<Old, New>(
    old: &Old,
    old_range: Range<usize>,
    new: &New,
    new_range: Range<usize>,
) -> usize
where
    Old: Index<usize> + ?Sized,
    New: Index<usize> + ?Sized,
    New::Output: PartialEq<Old::Output>,
{
    if is_empty_range(&old_range) || is_empty_range(&new_range) {
        return 0;
    }
    new_range
        .rev()
        .zip(old_range.rev())
        .take_while(
            #[inline(always)]
            |x| new[x.0] == old[x.1],
        )
        .count()
}

#[allow(dead_code)]
struct OffsetLookup<Int> {
    offset: usize,
    vec: Vec<Int>,
}

impl<Int> Index<usize> for OffsetLookup<Int> {
    type Output = Int;

    #[inline(always)]
    fn index(&self, index: usize) -> &Self::Output {
        &self.vec[index - self.offset]
    }
}

#[test]
fn test_common_prefix_len() {
    assert_eq!(
        common_prefix_len("".as_bytes(), 0..0, "".as_bytes(), 0..0),
        0
    );
    assert_eq!(
        common_prefix_len("foobarbaz".as_bytes(), 0..9, "foobarblah".as_bytes(), 0..10),
        7
    );
    assert_eq!(
        common_prefix_len("foobarbaz".as_bytes(), 0..9, "blablabla".as_bytes(), 0..9),
        0
    );
    assert_eq!(
        common_prefix_len("foobarbaz".as_bytes(), 3..9, "foobarblah".as_bytes(), 3..10),
        4
    );
}

#[test]
fn test_common_suffix_len() {
    assert_eq!(
        common_suffix_len("".as_bytes(), 0..0, "".as_bytes(), 0..0),
        0
    );
    assert_eq!(
        common_suffix_len("1234".as_bytes(), 0..4, "X0001234".as_bytes(), 0..8),
        4
    );
    assert_eq!(
        common_suffix_len("1234".as_bytes(), 0..4, "Xxxx".as_bytes(), 0..4),
        0
    );
    assert_eq!(
        common_suffix_len("1234".as_bytes(), 2..4, "01234".as_bytes(), 2..5),
        2
    );
}
