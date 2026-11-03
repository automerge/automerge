//! `ColumnIndex` abstracts the slab-index backing of a `Column`.
//!
//! Two concrete implementations:
//!
//! * [`BitIndex<W>`] — Fenwick BIT over a parallel `Vec<W>` of per-slab
//!   weights.  Fast, cache-tight; requires `W: SlabWeight` (AddAssign +
//!   SubAssign for incremental updates).
//! * `crate::btree::SlabBTree<W>` — B-tree over per-slab weights.
//!   Slightly slower for plain positional lookups but supports
//!   non-invertible aggregates (min/max) that Fenwick can't handle, and
//!   typically wins on compound prefix-sum queries.
//!
//! `Column<T, WF, Idx>` is parameterised over `Idx: ColumnIndex<WF::Weight>`
//! so you can swap indices with a type parameter change.

use crate::sealed::Sealed;
use std::ops::Range;

#[cfg(verus_keep_ghost)]
use crate::btree::{
    fold, lemma_fold_one, lemma_sum_len_concat, lemma_valid_subrange, spliced, sum_len, valid,
};
use crate::btree::{SlabAggregate, SlabBTree};
use crate::column::SlabWeight;
use crate::fenwick;
#[cfg(verus_keep_ghost)]
use crate::fenwick::{bit_ok, lemma_sum_split, lens, sum, total};
#[allow(unused_imports)]
use vstd::prelude::*;

/// Abstraction over the per-slab weight index a `Column` maintains.
///
/// Implementors store one weight per slab plus whatever aggregation
/// structure they need (Fenwick array, B-tree, etc.) and answer
/// positional queries in O(log n).
pub trait ColumnIndex<W>: Sealed + Default + IndexOps<W> {
    /// Construct from an iterator of per-slab weights in slab order.
    fn from_weights<I: IntoIterator<Item = W>>(iter: I) -> Self;

    /// Number of slabs indexed.
    fn len(&self) -> usize;

    /// `true` if no slabs are indexed.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Locate the slab containing the item at absolute position `index`
    /// within the column.  Returns `(slab_idx, offset_within_slab)`.
    /// If `index >= total_items`, returns `(len(), 0)` — past-end sentinel,
    /// matching the behaviour of `fenwick::find`.
    fn find_slab(&self, index: usize) -> (usize, usize);

    /// Number of items in slabs `0..slab_idx` — the inverse of
    /// [`find_slab`](Self::find_slab).  O(log n) for both implementations.
    /// `slab_idx` may equal `len()`, in which case the total item count
    /// is returned.
    fn items_before(&self, slab_idx: usize) -> usize;

    /// Replace slab `slab_idx`'s weight with `new_weight`.  O(log n) for
    /// both implementations.
    fn update_slab(&mut self, slab_idx: usize, new_weight: W);

    /// Splice a range of slab weights — remove entries in `range`,
    /// insert `new_weights` in their place.  Used for structural
    /// changes (overflow, cross-slab drain, merges).
    fn splice<I: IntoIterator<Item = W>>(&mut self, range: Range<usize>, new_weights: I);
}

// ── IndexOps: the verified interface ─────────────────────────────────────

verus! {

/// The item counts of weights `ws`, as index `I` measures them.
pub open spec fn wlens<W, I: IndexOps<W>>(ws: Seq<W>) -> Seq<nat> {
    ws.map_values(|w: W| I::wlen(w))
}

/// What the column cursor needs of a slab index, specified against its
/// sequence of slab weights (verified for both implementations).
#[doc(hidden)]
pub trait IndexOps<W>: Sized {
    /// A weight's item count.
    spec fn wlen(w: W) -> nat;

    /// The slabs' weights.
    spec fn ws(&self) -> Seq<W>;

    /// The index is well-formed.
    spec fn ok(&self) -> bool;

    /// Weights the index can hold.
    spec fn fits(ws: Seq<W>) -> bool;

    /// Whether weights fit whenever their item counts and number do.
    spec fn plain() -> bool;

    proof fn lemma_fits(ws: Seq<W>)
        requires
            Self::plain(),
            total(ws.map_values(|w: W| Self::wlen(w))) <= usize::MAX,
            ws.len() <= usize::MAX,
        ensures
            Self::fits(ws),
    ;

    /// The number of slabs.
    fn ix_len(&self) -> (n: usize)
        requires
            self.ok(),
        ensures
            n == self.ws().len(),
    ;

    /// The slab holding item `at` and the item's offset in it; past the
    /// end, the number of slabs.
    fn ix_find(&self, at: usize) -> (r: (usize, usize))
        requires
            self.ok(),
        ensures
            r.0 <= self.ws().len(),
            r.0 < self.ws().len() ==> sum(self.ws().map_values(|w: W| Self::wlen(w)), 0, r.0 as int) + r.1 == at
                && r.1 < self.ws().map_values(|w: W| Self::wlen(w))[r.0 as int],
            r.0 == self.ws().len() ==> at >= total(self.ws().map_values(|w: W| Self::wlen(w))),
    ;

    /// The items in the slabs before slab `i`.
    fn ix_before(&self, i: usize) -> (n: usize)
        requires
            self.ok(),
            i <= self.ws().len(),
        ensures
            n == sum(self.ws().map_values(|w: W| Self::wlen(w)), 0, i as int),
    ;

    /// Replace slab `i`'s weight.
    fn ix_set(&mut self, i: usize, w: W)
        requires
            old(self).ok(),
            i < old(self).ws().len(),
            Self::fits(old(self).ws().update(i as int, w)),
        ensures
            final(self).ok(),
            final(self).ws() == old(self).ws().update(i as int, w),
    ;

    /// Replace slabs `start..end`'s weights with `new`.
    fn ix_splice(&mut self, start: usize, end: usize, new: Vec<W>)
        requires
            old(self).ok(),
            start <= end <= old(self).ws().len(),
            Self::fits(old(self).ws().take(start as int) + new@ + old(self).ws().skip(end as int)),
        ensures
            final(self).ok(),
            final(self).ws() == old(self).ws().take(start as int) + new@ + old(self).ws().skip(
                end as int,
            ),
    ;
}

/// `v[start..end] = new`.
#[verifier::external_body]
#[inline(always)]
pub(crate) fn splice_vec<T>(v: &mut Vec<T>, start: usize, end: usize, new: Vec<T>)
    requires
        start <= end <= old(v)@.len(),
    ensures
        final(v)@ == old(v)@.take(start as int) + new@ + old(v)@.skip(end as int),
{
    v.splice(start..end, new);
}

} // verus!

// ── BitIndex: Fenwick-backed ───────────────────────────────────────────────

verus! {

/// Fenwick-BIT-backed `ColumnIndex`.  Holds a parallel cache of
/// per-slab weights alongside the BIT so point updates can read the
/// old weight.
///
/// The queries and updates are verified (`crate::fenwick`) against the
/// weights' item counts, given `wf`; only the `IntoIterator`
/// plumbing of `from_weights` and `splice` is not.
#[derive(Debug, Clone)]
#[verifier::allow(autoderive_clone_without_spec)]
pub struct BitIndex<W: SlabWeight> {
    weights: Vec<W>,
    bit: Vec<W>,
}

impl<W: SlabWeight> BitIndex<W> {
    /// `bit` is the Fenwick tree of `weights`, whose items fit a `usize`.
    pub(crate) closed spec fn wf(&self) -> bool {
        &&& bit_ok(lens(self.bit@), lens(self.weights@))
        &&& total(lens(self.weights@)) <= usize::MAX
    }

    /// The slabs' item counts.
    pub(crate) closed spec fn counts(&self) -> Seq<nat> {
        lens(self.weights@)
    }

    // Verus has no spec for `vec![x]`.
    #[allow(clippy::vec_init_then_push)]
    fn empty() -> (r: Self)
        ensures
            r.wf(),
            r.counts() == Seq::<nat>::empty(),
    {
        let mut bit = Vec::new();
        bit.push(W::zero());
        let r = BitIndex { weights: Vec::new(), bit };
        assert(lens(r.weights@) =~= Seq::<nat>::empty());
        r
    }

    /// The index of `weights`.
    fn from_vec(weights: Vec<W>) -> (r: Self)
        requires
            total(lens(weights@)) <= usize::MAX,
        ensures
            r.wf(),
            r.counts() == lens(weights@),
    {
        let ghost wl = |x: W| x.weight_len();
        assert(weights@.map_values(wl) =~= lens(weights@));
        let bit = fenwick::build(
            weights.as_slice(),
            |x: &W| -> (c: W)
                ensures
                    c.weight_len() == x.weight_len(),
                { x.copy() },
            Ghost(wl),
        );
        BitIndex { weights, bit }
    }

    /// Rebuild the tree after `weights` changed.
    fn rebuild(&mut self)
        requires
            total(lens(old(self).weights@)) <= usize::MAX,
        ensures
            final(self).wf(),
            final(self).counts() == old(self).counts(),
            final(self).weights == old(self).weights,
    {
        let ghost wl = |x: W| x.weight_len();
        assert(self.weights@.map_values(wl) =~= lens(self.weights@));
        self.bit = fenwick::build(
            self.weights.as_slice(),
            |x: &W| -> (c: W)
                ensures
                    c.weight_len() == x.weight_len(),
                { x.copy() },
            Ghost(wl),
        );
    }

    fn find(&self, index: usize) -> (r: (usize, usize))
        requires
            self.wf(),
        ensures
            self.counts().len() == 0 ==> r == (0usize, 0usize),
            self.counts().len() > 0 ==> {
                &&& r.0 <= self.counts().len()
                &&& fenwick::sum(self.counts(), 0, r.0 as int) + r.1 == index
                &&& r.0 < self.counts().len() ==> r.1 < self.counts()[r.0 as int]
            },
    {
        fenwick::find(self.bit.as_slice(), index, self.weights.len(), Ghost(lens(self.weights@)))
    }

    fn before(&self, slab_idx: usize) -> (n: usize)
        requires
            self.wf(),
            slab_idx <= self.counts().len(),
        ensures
            n == fenwick::sum(self.counts(), 0, slab_idx as int),
    {
        fenwick::prefix(self.bit.as_slice(), slab_idx, Ghost(lens(self.weights@)))
    }

    fn set(&mut self, slab_idx: usize, new_weight: W)
        requires
            old(self).wf(),
            slab_idx < old(self).counts().len(),
            total(old(self).counts().update(slab_idx as int, new_weight.weight_len())) <= usize::MAX,
        ensures
            final(self).wf(),
            final(self).counts() == old(self).counts().update(slab_idx as int, new_weight.weight_len()),
            final(self).weights@ == old(self).weights@.update(slab_idx as int, new_weight),
    {
        fenwick::update(
            &mut self.bit,
            slab_idx,
            &self.weights[slab_idx],
            &new_weight,
            Ghost(lens(self.weights@)),
        );
        self.weights.set(slab_idx, new_weight);
        assert(lens(self.weights@) =~= old(self).counts().update(slab_idx as int, new_weight.weight_len()));
    }
}

impl<W: SlabWeight> IndexOps<W> for BitIndex<W> {
    open spec fn wlen(w: W) -> nat {
        w.weight_len()
    }

    closed spec fn ws(&self) -> Seq<W> {
        self.weights@
    }

    closed spec fn ok(&self) -> bool {
        self.wf()
    }

    open spec fn fits(ws: Seq<W>) -> bool {
        total(lens(ws)) <= usize::MAX
    }

    open spec fn plain() -> bool {
        true
    }

    proof fn lemma_fits(ws: Seq<W>) {
        assert(ws.map_values(|w: W| Self::wlen(w)) =~= lens(ws));
    }

    fn ix_len(&self) -> usize {
        self.weights.len()
    }

    fn ix_find(&self, at: usize) -> (usize, usize) {
        proof {
            assert(self.weights@.map_values(|w: W| Self::wlen(w)) =~= self.counts());
            if self.weights@.len() > 0 {
                lemma_sum_split(self.counts(), 0, 0, 0);
            }
        }
        self.find(at)
    }

    fn ix_before(&self, i: usize) -> usize {
        proof {
            assert(self.weights@.map_values(|w: W| Self::wlen(w)) =~= self.counts());
        }
        self.before(i)
    }

    fn ix_set(&mut self, i: usize, w: W) {
        proof {
            assert(lens(self.weights@.update(i as int, w)) =~= self.counts().update(i as int, w.weight_len()));
        }
        self.set(i, w);
    }

    fn ix_splice(&mut self, start: usize, end: usize, new: Vec<W>) {
        splice_vec(&mut self.weights, start, end, new);
        self.rebuild();
    }
}

impl<A: SlabAggregate> IndexOps<A> for SlabBTree<A> {
    open spec fn wlen(w: A) -> nat {
        w.spec_len()
    }

    closed spec fn ws(&self) -> Seq<A> {
        self@
    }

    closed spec fn ok(&self) -> bool {
        self.wf()
    }

    open spec fn fits(ws: Seq<A>) -> bool {
        valid(ws) && ws.len() <= usize::MAX
    }

    open spec fn plain() -> bool {
        A::plain()
    }

    proof fn lemma_fits(ws: Seq<A>) {
        assert(ws.map_values(|w: A| Self::wlen(w)) =~= alens(ws));
        lemma_sum_len_wlens(ws, ws.len() as int);
        assert(ws.take(ws.len() as int) =~= ws);
        assert forall|i: int, j: int, k: int| 0 <= i <= j <= k <= ws.len() implies #[trigger] A::merge_ok(
            fold(ws.subrange(i, j)),
            fold(ws.subrange(j, k)),
        ) by {
            let a = ws.subrange(i, j);
            let b = ws.subrange(j, k);
            assert(ws =~= ws.subrange(0, i) + (a + b) + ws.subrange(k, ws.len() as int));
            lemma_sum_len_concat(ws.subrange(0, i) + (a + b), ws.subrange(k, ws.len() as int));
            lemma_sum_len_concat(ws.subrange(0, i), a + b);
            lemma_sum_len_concat(a, b);
            lemma_fold_plain(a);
            lemma_fold_plain(b);
            A::lemma_plain(fold(a), fold(b));
        }
    }

    fn ix_len(&self) -> usize {
        self.len()
    }

    fn ix_find(&self, at: usize) -> (usize, usize) {
        match self.find_by_prefix(at) {
            Some((si, items_before)) => {
                proof {
                    lemma_sum_len_wlens(self@, si as int);
                    lemma_sum_split(alens(self@), 0, si as int, si + 1);
                    assert(self@.map_values(|w: A| Self::wlen(w)) =~= alens(self@));
                }
                (si, at - items_before)
            },
            None => {
                proof {
                    lemma_sum_len_wlens(self@, self@.len() as int);
                    assert(self@.take(self@.len() as int) =~= self@);
                    assert(self@.map_values(|w: A| Self::wlen(w)) =~= alens(self@));
                }
                (self.len(), 0)
            },
        }
    }

    fn ix_before(&self, i: usize) -> usize {
        proof {
            lemma_sum_len_wlens(self@, i as int);
            assert(self@.map_values(|w: A| Self::wlen(w)) =~= alens(self@));
        }
        self.items_before_slab(i)
    }

    fn ix_set(&mut self, i: usize, w: A) {
        self.update_slab(i, w);
    }

    fn ix_splice(&mut self, start: usize, end: usize, new: Vec<A>) {
        proof {
            assert(spliced(self@, start as int, end as int, new@) =~= self@.take(start as int) + new@
                + self@.skip(end as int));
        }
        self.splice_vec(start, end, new);
    }
}

/// Aggregates' item counts.
spec fn alens<A: SlabAggregate>(s: Seq<A>) -> Seq<nat> {
    s.map_values(|a: A| a.spec_len())
}

/// The B-tree's item count over the first `i` slabs is their counts' sum.
proof fn lemma_sum_len_wlens<A: SlabAggregate>(s: Seq<A>, i: int)
    requires
        0 <= i <= s.len(),
    ensures
        sum_len(s.take(i)) == sum(alens(s), 0, i),
    decreases i,
{
    if i > 0 {
        lemma_sum_len_wlens(s, i - 1);
        assert(s.take(i).drop_last() =~= s.take(i - 1));
    } else {
        assert(s.take(0) =~= Seq::<A>::empty());
    }
}

/// Folding plain aggregates whose counts fit adds the counts up.
proof fn lemma_fold_plain<A: SlabAggregate>(s: Seq<A>)
    requires
        A::plain(),
        sum_len(s) <= usize::MAX,
    ensures
        fold(s).spec_len() == sum_len(s),
    decreases s.len(),
{
    if s.len() == 0 {
        A::lemma_empty(A::spec_empty());
    } else {
        lemma_fold_plain(s.drop_last());
        A::lemma_plain(fold(s.drop_last()), s.last());
        A::lemma_merge_len(fold(s.drop_last()), s.last());
    }
}

} // verus!

impl<W: SlabWeight> Default for BitIndex<W> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<W: SlabWeight> Sealed for BitIndex<W> {}

impl<W: SlabWeight> ColumnIndex<W> for BitIndex<W> {
    fn from_weights<I: IntoIterator<Item = W>>(iter: I) -> Self {
        Self::from_vec(iter.into_iter().collect())
    }

    fn len(&self) -> usize {
        self.weights.len()
    }

    fn find_slab(&self, index: usize) -> (usize, usize) {
        self.find(index)
    }

    fn items_before(&self, slab_idx: usize) -> usize {
        // Standard BIT prefix query: sum of weights[0..slab_idx].
        debug_assert!(slab_idx <= self.weights.len());
        self.before(slab_idx)
    }

    fn update_slab(&mut self, slab_idx: usize, new_weight: W) {
        self.set(slab_idx, new_weight);
    }

    fn splice<I: IntoIterator<Item = W>>(&mut self, range: Range<usize>, new_weights: I) {
        self.weights.splice(range, new_weights);
        self.rebuild();
    }
}

// ── SlabBTree: B-tree-backed ───────────────────────────────────────────────

impl<A: SlabAggregate> Sealed for SlabBTree<A> {}

impl<A: SlabAggregate> ColumnIndex<A> for SlabBTree<A> {
    fn from_weights<I: IntoIterator<Item = A>>(iter: I) -> Self {
        SlabBTree::from_iter(iter)
    }

    fn len(&self) -> usize {
        SlabBTree::len(self)
    }

    fn find_slab(&self, index: usize) -> (usize, usize) {
        match self.find_by_prefix(index) {
            Some((si, items_before)) => (si, index - items_before),
            None => (SlabBTree::len(self), 0),
        }
    }

    fn items_before(&self, slab_idx: usize) -> usize {
        self.items_before_slab(slab_idx)
    }

    fn update_slab(&mut self, slab_idx: usize, new_weight: A) {
        SlabBTree::update_slab(self, slab_idx, new_weight);
    }

    fn splice<I: IntoIterator<Item = A>>(&mut self, range: Range<usize>, new_weights: I) {
        SlabBTree::splice(self, range, new_weights);
    }
}
