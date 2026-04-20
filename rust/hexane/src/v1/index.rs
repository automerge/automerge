//! `ColumnIndex` abstracts the slab-index backing of [`Column2`].
//!
//! Two concrete implementations:
//!
//! * [`BitIndex<W>`] — Fenwick BIT over a parallel `Vec<W>` of per-slab
//!   weights.  Fast, cache-tight; requires `W: SlabWeight` (AddAssign +
//!   SubAssign for incremental updates).
//! * [`super::btree::SlabBTree<W>`] — B-tree over per-slab weights.
//!   Slightly slower for plain positional lookups but supports
//!   non-invertible aggregates (min/max) that Fenwick can't handle, and
//!   typically wins on compound prefix-sum queries.
//!
//! `Column2<T, WF, Idx>` is parameterised over `Idx: ColumnIndex<WF::Weight>`
//! so you can swap indices with a type parameter change.

use std::ops::Range;

use super::btree::{SlabAggregate, SlabBTree};
use super::column::{bit_point_update, find_slab_bit, rebuild_bit, SlabWeight};

/// Abstraction over the per-slab weight index a `Column` maintains.
///
/// Implementors store one weight per slab plus whatever aggregation
/// structure they need (Fenwick array, B-tree, etc.) and answer
/// positional queries in O(log n).
pub trait ColumnIndex<W>: Default {
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
    /// matching the behaviour of [`find_slab_bit`].
    fn find_slab(&self, index: usize) -> (usize, usize);

    /// Replace slab `slab_idx`'s weight with `new_weight`.  O(log n) for
    /// both implementations.
    fn update_slab(&mut self, slab_idx: usize, new_weight: W);

    /// Splice a range of slab weights — remove entries in `range`,
    /// insert `new_weights` in their place.  Used for structural
    /// changes (overflow, cross-slab drain, merges).
    fn splice<I: IntoIterator<Item = W>>(&mut self, range: Range<usize>, new_weights: I);
}

// ── BitIndex: Fenwick-backed ───────────────────────────────────────────────

/// Fenwick-BIT-backed `ColumnIndex`.  Holds a parallel cache of
/// per-slab weights alongside the BIT so point updates can read the
/// old weight.
#[derive(Debug, Clone)]
pub struct BitIndex<W: SlabWeight> {
    weights: Vec<W>,
    bit: Vec<W>,
}

impl<W: SlabWeight> Default for BitIndex<W> {
    fn default() -> Self {
        Self {
            weights: Vec::new(),
            bit: vec![W::default()],
        }
    }
}

impl<W: SlabWeight> ColumnIndex<W> for BitIndex<W> {
    fn from_weights<I: IntoIterator<Item = W>>(iter: I) -> Self {
        let weights: Vec<W> = iter.into_iter().collect();
        let bit = rebuild_bit(&weights, |w| *w);
        Self { weights, bit }
    }

    fn len(&self) -> usize {
        self.weights.len()
    }

    fn find_slab(&self, index: usize) -> (usize, usize) {
        find_slab_bit(&self.bit, index, self.weights.len())
    }

    fn update_slab(&mut self, slab_idx: usize, new_weight: W) {
        let old = self.weights[slab_idx];
        self.weights[slab_idx] = new_weight;
        bit_point_update(&mut self.bit, slab_idx, old, new_weight);
    }

    fn splice<I: IntoIterator<Item = W>>(&mut self, range: Range<usize>, new_weights: I) {
        self.weights.splice(range, new_weights);
        self.bit = rebuild_bit(&self.weights, |w| *w);
    }
}

// ── SlabBTree: B-tree-backed ───────────────────────────────────────────────

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

    fn update_slab(&mut self, slab_idx: usize, new_weight: A) {
        SlabBTree::update_slab(self, slab_idx, new_weight);
    }

    fn splice<I: IntoIterator<Item = A>>(&mut self, range: Range<usize>, new_weights: I) {
        SlabBTree::splice(self, range, new_weights);
    }
}
