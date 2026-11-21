//! Raw-byte arena column.
//!
//! A [`RawColumn`] is conceptually a giant `Vec<u8>` that supports O(log S)
//! random splices at any byte offset — where S is the number of slabs, kept
//! bounded by [`RawColumn::with_max_segments`].  Unlike [`Column`](crate::Column)
//! it stores no per-item encoding: the bytes you splice in are the bytes that
//! come out.
//!
//! The column does **not** know where logical values begin and end.  That
//! responsibility belongs to the caller (typically a paired `ValueMeta`
//! column whose prefix sum gives byte offsets into the raw arena).  What the
//! column *does* guarantee is that **every splice point becomes a potential
//! slab boundary, and splits only happen at splice points**.  As long as
//! callers splice at value boundaries, no value ever crosses a slab — which
//! is what makes the zero-copy [`RawColumn::get`] API safe.
//!
//! Verification (Verus): the column stands for its slabs' bytes one after
//! another (its `View`), and `wf` ties the rest to them: `bit` is their
//! Fenwick tree (`crate::fenwick`), `total_len` their count, and no slab
//! is empty.  [`try_splice_slice`](RawColumn::try_splice_slice) is proved to
//! be exactly a splice of that byte sequence, and the reads (`try_get`,
//! `iter_at`, [`RawColumnIter`]'s `take` and `skip`, `save_to`) and `load`
//! to return exactly its bytes.  [`get`](RawColumn::get) asks of a verified
//! caller the range that `try_get` takes, where it panics for another;
//! a clone is a column of the same bytes.  The promise above is proved
//! as well: `cuts` are the places where slabs meet, a splice leaves a
//! cut only where one was or at an end of the bytes it puts in
//! (`spliced_cut`), and bytes lie within one slab exactly when no cut is
//! strictly between them (`lemma_in_one_slab`).  Unverified are only the
//! `IntoIterator` plumbing of `try_splice` and `copy_ranges` (which
//! splices through the verified path).
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([`verified`](crate::verified#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`raw/imp.rs`), which has
//! its contract word for word and is proved there.

// Verus has no specs for `Vec::is_empty`, `usize::abs_diff` or `vec![x]`.
#![allow(clippy::len_zero, clippy::manual_abs_diff, clippy::vec_init_then_push)]

#[cfg(verus_keep_ghost)]
use crate::fenwick;
#[cfg(verus_keep_ghost)]
use crate::fenwick::{bit_ok, lens, sum, total};
#[cfg(verus_keep_ghost)]
use crate::merge_walk::Merger;
use crate::PackError;
use std::ops::Range;
#[allow(unused_imports)]
use vstd::prelude::*;

use crate::trusted::drain_slabs;
use crate::trusted::fail;
use crate::trusted::fail_seek;
use crate::trusted::fail_take;
use crate::trusted::get_cross;
use crate::trusted::get_oob;
use crate::trusted::splice_failed;
use crate::trusted::splice_oob;
use crate::trusted::splice_too_long;
mod imp;
pub(crate) use imp::RawSlab;
#[allow(unused_imports)]
use imp::*;

verus! {

/// A byte arena with O(log S) random splices.
///
/// See the module docs for the invariant around splice points and slab
/// boundaries.
#[derive(Debug)]
pub struct RawColumn {
    slabs: Vec<RawSlab>,
    /// Fenwick tree of slab byte lengths, 1-indexed.  `bit[0]` is unused,
    /// `bit[1..=slabs.len()]` holds the tree.
    bit: Vec<usize>,
    total_len: usize,
    /// Target per-slab byte budget (not a hard cap — single blobs larger
    /// than this live in their own oversize slab).
    max_segments: usize,
}

impl Clone for RawColumn {
    /// A column of its own, with the same bytes.
    #[inline(always)]
    fn clone(&self) -> (r: Self)
        ensures
            r@ == self@,
            self.wf() ==> r.wf(),
            r.cuts() == self.cuts(),
    {
        self.clone_impl()
    }
}

/// Whether `x` may be a cut of a column in which `del` bytes at `index`
/// were replaced by `n`, and whose cuts were `cuts`: it is a cut that
/// was there before the splice, or one that was after it, moved as the
/// bytes after it are, or an end of the bytes put in. See
/// [`RawColumn::try_splice_slice`].
pub open spec fn spliced_cut(cuts: ISet<int>, index: int, del: int, n: int, x: int) -> bool {
    ||| x <= index && cuts.contains(x)
    ||| x >= index + n && cuts.contains(x - n + del)
    ||| x == index
    ||| x == index + n
}










// ── RawColumn ───────────────────────────────────────────────────────────────

impl View for RawColumn {
    type V = Seq<u8>;

    closed spec fn view(&self) -> Seq<u8> {
        flat(self.slabs@)
    }
}

impl RawColumn {
    /// `bit` is the Fenwick tree of the slab sizes, `total_len` their sum,
    /// and no slab is empty.
    pub closed spec fn wf(&self) -> bool {
        &&& bit_ok(lens(self.bit@), sizes(self.slabs@))
        &&& self.total_len == total(sizes(self.slabs@))
        &&& full(self.slabs@)
    }

    /// Bytes `[start, end)` lie within one slab.
    pub closed spec fn in_one_slab(&self, start: int, end: int) -> bool {
        exists|i: int|
            0 <= i < self.slabs@.len() && #[trigger] sum(sizes(self.slabs@), 0, i) <= start && end <= sum(
                sizes(self.slabs@),
                0,
                i + 1,
            )
    }

    /// Where the column's slabs meet, and its two ends. Bytes lie within
    /// one slab, which is where [`get`](Self::get) can give them, exactly
    /// when no cut is strictly between them (`lemma_in_one_slab`); and a
    /// splice leaves a cut only where one was, or at an end of the bytes
    /// it puts in (`spliced_cut`). So bytes spliced in as one, and never
    /// spliced into, stay within one slab.
    pub closed spec fn cuts(&self) -> ISet<int> {
        edges(self.slabs@)
    }

    /// Bytes `[start, end)` of the column lie within one slab exactly when
    /// no cut is strictly between `start` and `end`.
    pub proof fn lemma_in_one_slab(&self, start: int, end: int)
        requires
            0 <= start < end <= self@.len(),
        ensures
            self.in_one_slab(start, end) <==> forall|x: int|
                start < x < end ==> !#[trigger] self.cuts().contains(x),
    {
        self.lemma_in_one_slab_impl(start, end)
    }

    #[inline(always)]
    pub fn new() -> (r: Self)
        ensures
            r.wf(),
            r@ == Seq::<u8>::empty(),
            forall|x: int| #[trigger] r.cuts().contains(x) ==> x == 0,
    {
        Self::new_impl()
    }

    #[inline(always)]
    pub fn with_max_segments(max_segments: usize) -> (r: Self)
        ensures
            r.wf(),
            r@ == Seq::<u8>::empty(),
            forall|x: int| #[trigger] r.cuts().contains(x) ==> x == 0,
    {
        Self::with_max_segments_impl(max_segments)
    }

    #[inline(always)]
    pub fn len(&self) -> (n: usize)
        requires
            self.wf(),
        ensures
            n == self@.len(),
    {
        self.len_impl()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> (b: bool)
        requires
            self.wf(),
        ensures
            b == (self@.len() == 0),
    {
        self.is_empty_impl()
    }

    // ── Splice ──────────────────────────────────────────────────────────────

    /// Non-panicking variant of [`splice_slice`](Self::splice_slice).
    ///
    /// Flow mirrors `Column`'s `Encoding::splice`: locate the target slab,
    /// delegate to `splice_slab` which handles one slab plus any overflow,
    /// then clean up cross-slab deletes, merge undersized neighbours, and
    /// update the BIT.
    #[inline(always)]
    pub fn try_splice_slice(&mut self, index: usize, del: usize, bytes: &[u8]) -> (r: Result<
        (),
        PackError,
    >)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            r is Ok <==> index + del <= old(self)@.len() && old(self)@.len() - del + bytes@.len()
                <= usize::MAX,
            r is Ok ==> final(self)@ == old(self)@.take(index as int) + bytes@ + old(self)@.skip(
                index + del,
            ),
            r is Err ==> final(self)@ == old(self)@,
            r is Ok ==> forall|x: int| #[trigger]
                final(self).cuts().contains(x) ==> spliced_cut(
                    old(self).cuts(),
                    index as int,
                    del as int,
                    bytes@.len() as int,
                    x,
                ),
            r is Err ==> final(self).cuts() == old(self).cuts(),
    {
        self.try_splice_slice_impl(index, del, bytes)
    }

    /// Insert / delete / replace bytes at `index` with a single byte slice.
    /// Panics if `index + del` exceeds the column length.
    #[inline(always)]
    pub fn splice_slice(&mut self, index: usize, del: usize, bytes: &[u8])
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self)@ == old(self)@.take(index as int) + bytes@ + old(self)@.skip(index + del),
            forall|x: int| #[trigger]
                final(self).cuts().contains(x) ==> spliced_cut(
                    old(self).cuts(),
                    index as int,
                    del as int,
                    bytes@.len() as int,
                    x,
                ),
    {
        self.splice_slice_impl(index, del, bytes)
    }

    // ── Iter ────────────────────────────────────────────────────────────────

    /// Sequential reader starting at byte 0.
    #[inline(always)]
    pub fn iter(&self) -> (it: RawColumnIter<'_>)
        requires
            self.wf(),
        ensures
            it.wf(),
            it.bytes() == self@,
            it.spec_pos() == 0,
    {
        self.iter_impl()
    }

    /// Sequential reader positioned at byte `pos`.  O(log S) seek.
    /// Panics if `pos > len()`.
    #[inline(always)]
    pub fn iter_at(&self, pos: usize) -> (it: RawColumnIter<'_>)
        requires
            self.wf(),
        ensures
            pos <= self@.len(),
            it.wf(),
            it.bytes() == self@,
            it.spec_pos() == pos,
    {
        self.iter_at_impl(pos)
    }

    // ── Wire format ─────────────────────────────────────────────────────────

    #[inline(always)]
    pub fn save(&self) -> (out: Vec<u8>)
        requires
            self.wf(),
        ensures
            out@ == self@,
    {
        self.save_impl()
    }

    /// Deserialize with a custom `max_segments` budget.
    #[inline(always)]
    pub fn load_with_max_segments(data: &[u8], max_segments: usize) -> (r: Result<Self, PackError>)
        ensures
            r is Ok,
            r->Ok_0.wf(),
            r->Ok_0@ == data@,
            forall|x: int| #[trigger] r->Ok_0.cuts().contains(x) ==> x == 0 || x == data@.len(),
    {
        Self::load_with_max_segments_impl(data, max_segments)
    }
}

/// Sequential byte reader over a [`RawColumn`].
///
/// Not a `std::iter::Iterator` — the natural unit of reading here is a
/// variable-sized blob, so instead of `next()` this exposes [`take`] and
/// [`skip`].  State is maintained across calls so consecutive reads don't
/// re-lookup the slab.
///
/// [`take`]: RawColumnIter::take
/// [`skip`]: RawColumnIter::skip
#[derive(Debug)]
pub struct RawColumnIter<'a> {
    slabs: &'a [RawSlab],
    slab_idx: usize,
    offset_in_slab: usize,
    /// Absolute byte position — `slab_idx` and `offset_in_slab` are the
    /// physical derivation; this mirrors them for `pos()` and `seek_to`.
    pos: usize,
}

impl<'a> Clone for RawColumnIter<'a> {
    /// An iterator at the same place of the same column.
    #[inline(always)]
    fn clone(&self) -> (r: Self)
        ensures
            r == *self,
    {
        self.clone_impl()
    }
}

impl<'a> Default for RawColumnIter<'a> {
    /// The iterator of a column with no bytes.
    #[inline(always)]
    fn default() -> (r: Self)
        ensures
            r.wf(),
            r.bytes() == Seq::<u8>::empty(),
            r.spec_pos() == 0,
    {
        Self::default_impl()
    }
}

impl<'a> RawColumnIter<'a> {
    /// `(slab_idx, offset_in_slab)` is byte `pos` of the slabs, none of
    /// which is empty; only in the last slab can it be the slab's end.
    pub closed spec fn wf(&self) -> bool {
        let w = sizes(self.slabs@);
        &&& total(w) <= usize::MAX
        &&& full(self.slabs@)
        &&& self.slab_idx <= w.len()
        &&& self.slab_idx < w.len() ==> self.offset_in_slab <= w[self.slab_idx as int]
        &&& self.slab_idx + 1 < w.len() ==> self.offset_in_slab < w[self.slab_idx as int]
        &&& self.slab_idx == w.len() ==> self.offset_in_slab == 0
        &&& sum(w, 0, self.slab_idx as int) + self.offset_in_slab == self.pos
    }

    /// The next `n` bytes lie within one slab.
    pub closed spec fn fits(&self, n: nat) -> bool {
        let w = sizes(self.slabs@);
        n == 0 || exists|i: int|
            0 <= i < w.len() && #[trigger] sum(w, 0, i) <= self.pos && self.pos + n <= sum(
                w,
                0,
                i + 1,
            )
    }

    /// The column's bytes.
    pub closed spec fn bytes(&self) -> Seq<u8> {
        flat(self.slabs@)
    }

    pub closed spec fn spec_pos(&self) -> nat {
        self.pos as nat
    }

    /// Current absolute byte position.  Useful for suspending / later
    /// resuming via [`RawColumn::iter_at`].
    #[inline(always)]
    pub fn pos(&self) -> (p: usize)
        ensures
            p == self.spec_pos(),
    {
        self.pos_impl()
    }

    /// Read the next `n` bytes and advance.  Panics if the read would cross
    /// a slab boundary — by the column's invariant this only happens if the
    /// caller passed a length that doesn't correspond to a single value
    /// boundary.  (Proved: it panics only if the bytes don't all lie within
    /// one slab, `fits`.)
    #[inline(always)]
    pub fn take(&mut self, n: usize) -> (out: &'a [u8])
        requires
            old(self).wf(),
        ensures
            old(self).fits(n as nat),
            final(self).wf(),
            final(self).bytes() == old(self).bytes(),
            old(self).spec_pos() + n <= old(self).bytes().len(),
            final(self).spec_pos() == old(self).spec_pos() + n,
            out@ == old(self).bytes().subrange(
                old(self).spec_pos() as int,
                old(self).spec_pos() + n,
            ),
    {
        self.take_impl(n)
    }

    /// Advance the cursor by `n` bytes, crossing slab boundaries as needed.
    /// Panics if `n` walks past the end of the column.
    #[inline(always)]
    pub fn skip(&mut self, n: usize)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self).bytes() == old(self).bytes(),
            old(self).spec_pos() + n <= old(self).bytes().len(),
            final(self).spec_pos() == old(self).spec_pos() + n,
    {
        self.skip_impl(n)
    }

    /// Forward-only absolute seek.  `target` must be `>= pos()`; use
    /// [`RawColumn::iter_at`] for arbitrary seeks.  Equivalent to
    /// `self.skip(target - self.pos())` with a bounds check.
    #[inline(always)]
    pub fn seek_to(&mut self, target: usize)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self).bytes() == old(self).bytes(),
            old(self).spec_pos() <= target <= old(self).bytes().len(),
            final(self).spec_pos() == target,
    {
        self.seek_to_impl(target)
    }
}

impl Default for RawColumn {
    /// A column with no bytes.
    fn default() -> (r: Self)
        ensures
            r.wf(),
            r@ == Seq::<u8>::empty(),
            forall|x: int| #[trigger] r.cuts().contains(x) ==> x == 0,
    {
        Self::new()
    }
}

impl RawColumn {
    // ── Range read ──────────────────────────────────────────────────────────

    /// Return a contiguous slice for `range`.
    /// Panics if `range` is out of bounds or crosses a slab boundary.
    ///
    /// A verified caller shows that it is neither: the `requires` is what
    /// [`try_get`](Self::try_get) gives `Ok` for.
    #[inline(always)]
    pub fn get(&self, range: Range<usize>) -> (out: &[u8])
        requires
            self.wf(),
            range.start <= range.end <= self@.len(),
            range.start == range.end || self.in_one_slab(range.start as int, range.end as int),
        ensures
            out@ == self@.subrange(range.start as int, range.end as int),
    {
        self.get_impl(range)
    }

    /// Non-panicking variant of [`get`](Self::get).
    #[inline(always)]
    pub fn try_get(&self, range: Range<usize>) -> (r: Result<&[u8], PackError>)
        requires
            self.wf(),
        ensures
            r is Ok <==> range.start <= range.end <= self@.len() && (range.start == range.end
                || self.in_one_slab(range.start as int, range.end as int)),
            r is Ok ==> r->Ok_0@ == self@.subrange(range.start as int, range.end as int),
    {
        self.try_get_impl(range)
    }

    // ── Wire format ─────────────────────────────────────────────────────────

    /// Append the column's bytes to `out`; returns where they are.
    #[inline(always)]
    pub fn save_to(&self, out: &mut Vec<u8>) -> (r: Range<usize>)
        ensures
            final(out)@ == old(out)@ + self@,
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        self.save_to_impl(out)
    }

    /// Deserialize from `data`.  Everything lands in a single slab — the
    /// caller knows where value boundaries are and can splice further if
    /// they want splits.
    #[inline(always)]
    pub fn load(data: &[u8]) -> (r: Result<Self, PackError>)
        ensures
            r is Ok,
            r->Ok_0.wf(),
            r->Ok_0@ == data@,
            forall|x: int| #[trigger] r->Ok_0.cuts().contains(x) ==> x == 0 || x == data@.len(),
    {
        Self::load_impl(data)
    }
}

} // verus!
