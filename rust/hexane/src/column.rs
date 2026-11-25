//! The slab, the unit a column keeps its values in, and the splices of
//! `copy_ranges`: what the core of every column shares.

use crate::sealed::Sealed;
use crate::splices::{normalize, Normalized, SpliceList};
use std::ops::{AddAssign, Range, SubAssign};
#[allow(unused_imports)]
use vstd::prelude::*;

/// Default maximum number of RLE/bool segments per slab.
///
/// Slabs are loaded at half capacity (`max / 2`) to leave room for inserts
/// without overflowing. A splice that exceeds `max` triggers an overflow split.
pub const DEFAULT_MAX_SEG: usize = 64;

// ── Slab ─────────────────────────────────────────────────────────────────────

verus! {

#[doc(hidden)]
#[derive(Debug, Default, PartialEq)]
pub struct Slab<Tail: Copy + Clone + std::fmt::Debug + Default = ()> {
    pub(crate) data: Vec<u8>,
    pub(crate) len: usize,
    pub(crate) segments: usize,
    pub(crate) tail: Tail,
}

impl<Tail: Copy + Clone + std::fmt::Debug + Default> Slab<Tail> {
    /// Slab `o` has this slab's bytes, counts and tail: what a clone of
    /// it is.
    pub open(crate) spec fn same(self, o: Self) -> bool {
        &&& o.data@ == self.data@
        &&& o.len == self.len
        &&& o.segments == self.segments
        &&& o.tail == self.tail
    }
}

// Written out, where it was derived, to say what a clone is: Verus gives
// a derived `clone` no specification unless the type is `Copy`.
impl<Tail: Copy + Clone + std::fmt::Debug + Default> Clone for Slab<Tail> {
    /// A slab of its own with the same bytes, counts and tail.
    #[inline]
    fn clone(&self) -> (r: Self)
        ensures
            self.same(r),
    {
        Slab { data: self.data.clone(), len: self.len, segments: self.segments, tail: self.tail }
    }
}

} // verus!

impl<Tail: Copy + Clone + std::fmt::Debug + Default> Slab<Tail> {}

// ── SlabWeight ───────────────────────────────────────────────────────────────

verus! {

/// A value stored in a Fenwick tree (BIT) node.
///
/// For plain `Column`, this is `usize` (just the slab item count).
/// For columns that track prefix sums, this is a compound value carrying
/// both the item count and the slab's contribution to the prefix sum.
///
/// The Fenwick tree ([`crate::fenwick`]) is verified against the item
/// counts alone, so these methods are specified only by what they do to
/// [`weight_len`](Self::weight_len).
#[doc(hidden)]
pub trait SlabWeight: Sealed + Clone + Default + std::fmt::Debug + AddAssign + SubAssign {
    /// [`len`](Self::len), for specifications.
    spec fn weight_len(&self) -> nat;

    /// A weight that is also an aggregate has the same length as both.
    proof fn lemma_weight_len(&self)
        where Self: crate::btree::SlabAggregate
        ensures
            self.weight_len() == self.spec_len(),
    ;

    /// The length (item count) component of this weight.
    fn len(&self) -> (n: usize)
        ensures n == self.weight_len();


    /// The weight of no items ([`Default`]).
    fn zero() -> (z: Self)
        ensures z.weight_len() == 0;

    /// A copy of this weight ([`Clone`]).
    fn copy(&self) -> (c: Self)
        ensures c.weight_len() == self.weight_len();

    /// `*self += other`.
    fn add_weight(&mut self, other: &Self)
        requires old(self).weight_len() + other.weight_len() <= usize::MAX,
        ensures final(self).weight_len() == old(self).weight_len() + other.weight_len();

    /// `*self -= other`.
    fn sub_weight(&mut self, other: &Self)
        requires other.weight_len() <= old(self).weight_len(),
        ensures final(self).weight_len() == old(self).weight_len() - other.weight_len();
}

impl Sealed for usize {}

impl SlabWeight for usize {
    open spec fn weight_len(&self) -> nat {
        *self as nat
    }

    proof fn lemma_weight_len(&self)
        where Self: crate::btree::SlabAggregate
    {
        crate::btree::lemma_count_len(*self);
    }

    #[inline]
    fn len(&self) -> usize {
        *self
    }

    #[inline]
    fn zero() -> usize {
        0
    }

    #[inline]
    fn copy(&self) -> usize {
        *self
    }

    #[inline]
    fn add_weight(&mut self, other: &usize) {
        *self += *other;
    }

    #[inline]
    fn sub_weight(&mut self, other: &usize) {
        *self -= *other;
    }
}

} // verus!

// ── Splice helpers ───────────────────────────────────────────────────────────

/// Byte-level `Vec::splice` without the per-element iterator dance.
///
/// `Vec::splice(range, iter)` in stdlib walks the iterator and does a
/// `ptr::write` per element in its drop handler — no memcpy fast path for
/// `Copy` types with a known size.  This replacement uses three safe
/// stdlib calls that all lower to SIMD memcpy / memmove:
///
///   * [`Vec::resize`] (memset-for-`u8`) to grow,
///   * [`<[u8]>::copy_within`] to shift the suffix,
///   * [`<[u8]>::copy_from_slice`] to drop the new bytes in.
///
/// Compared to `Vec::splice` the inserted bytes go in as one memcpy instead
/// of a byte-at-a-time `ptr::write` loop.  Compared to a hand-rolled
/// `unsafe` version this leaves one wasted pass over the grown tail
/// (the memset-to-zero before we overwrite it), which is negligible:
/// `bytes.len()` is typically tiny next to the suffix shift.
///
/// Used by the RLE, bool, and raw splice paths.
#[inline]
pub(crate) fn splice_bytes(vec: &mut Vec<u8>, index: usize, del: usize, bytes: &[u8]) {
    let old_len = vec.len();
    debug_assert!(index + del <= old_len);
    let insert = bytes.len();
    let new_len = old_len - del + insert;

    use std::cmp::Ordering;
    match insert.cmp(&del) {
        Ordering::Greater => {
            vec.resize(new_len, 0);
            vec.copy_within(index + del..old_len, index + insert);
        }
        Ordering::Less => {
            vec.copy_within(index + del..old_len, index + insert);
            vec.truncate(new_len);
        }
        Ordering::Equal => {}
    }
    vec[index..index + insert].copy_from_slice(bytes);
}

// ── Splices ─────────────────────────────────────────────────────────────────

/// One point of a multi-point [`Column::copy_ranges`](crate::Column::copy_ranges): at destination
/// position `pos` (pre-splice coordinates), delete `delete` rows and
/// insert the source rows `range`.
///
/// The default is *all of the source, inserted at the front, deleting
/// nothing* — source ranges are clamped to the source's length, so
/// `0..usize::MAX` is the whole column. Copying a whole column onto the
/// end of another is then `Splice { pos: dst.len(), ..Default::default() }`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Splice {
    pub pos: usize,
    pub delete: usize,
    pub range: Range<usize>,
}

impl Default for Splice {
    fn default() -> Self {
        Self {
            pos: 0,
            delete: 0,
            range: 0..usize::MAX,
        }
    }
}

/// Normalize a multi-point splice list: clamp src ranges to `src_len`,
/// drop no-ops, and merge contiguous neighbours (next edit starting
/// where the previous one ended, with touching src ranges). Asserts
/// destination regions are ascending/non-overlapping and in bounds,
/// and src ranges globally ascending/non-overlapping.
pub(crate) fn normalize_splices(
    splices: impl IntoIterator<Item = Splice>,
    src_len: usize,
    dst_len: usize,
) -> Normalized {
    // verified (`splices::normalize`): the list is normalized, and applied
    // gives what the splices give one by one, which its type keeps
    normalize(&mut splices.into_iter(), src_len, dst_len)
}

/// The splices a caller gives `copy_ranges`, from its iterator (glue).
impl<I: Iterator<Item = Splice>> SpliceList for I {
    #[inline(always)]
    fn next_splice(&mut self) -> Option<Splice> {
        self.next()
    }
}
