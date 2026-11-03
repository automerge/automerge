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

use crate::btree::{SlabAggregate, SlabBTree};
use crate::column::SlabWeight;
use crate::fenwick;
#[cfg(verus_keep_ghost)]
use crate::fenwick::{bit_ok, lens, total};
#[allow(unused_imports)]
use vstd::prelude::*;

/// Abstraction over the per-slab weight index a `Column` maintains.
///
/// Implementors store one weight per slab plus whatever aggregation
/// structure they need (Fenwick array, B-tree, etc.) and answer
/// positional queries in O(log n).
pub trait ColumnIndex<W>: Sealed + Default {
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
