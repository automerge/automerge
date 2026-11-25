//! The indexes of a column's slabs: what each slab weighs, and where in
//! the column it starts.
//!
//! * [`BitIndex<W>`]: a Fenwick tree over a `Vec<W>` of the slabs'
//!   weights.
//! * `crate::btree::SlabBTree<W>`: a B-tree over the slabs' weights, which
//!   also keeps sums that a Fenwick tree cannot (and is the default).
//!
//! [`IndexOps`] is what the verified core asks of an index.

use crate::sealed::Sealed;

#[cfg(verus_keep_ghost)]
use crate::btree::{
    exact_clone, fold, lemma_fold_one, lemma_sum_len_concat, lemma_valid_subrange, spliced,
    sum_len, valid,
};
use crate::btree::{SlabAggregate, SlabBTree};
use crate::column::SlabWeight;
use crate::fenwick;
#[cfg(verus_keep_ghost)]
use crate::fenwick::{bit_ok, lemma_sum_split, lens, sum, total};
#[allow(unused_imports)]
use vstd::prelude::*;

pub(crate) use crate::trusted::splice_vec;
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

    /// Every index type the columns use is plain.
    proof fn lemma_plain()
        ensures
            Self::plain(),
    ;

    /// An index measures a weight that is an aggregate by its length.
    proof fn lemma_wlen(w: W)
        where W: crate::btree::SlabAggregate
        ensures
            Self::wlen(w) == w.spec_len(),
    ;

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

    /// A weight's item count.
    fn w_len(w: &W) -> (n: usize)
        ensures
            n == Self::wlen(*w),
    ;

    /// The index of no slabs.
    fn ix_empty() -> (r: Self)
        ensures
            r.ok(),
            r.ws() == Seq::<W>::empty(),
    ;

    /// A stand-in for an index moved out, made without allocating: it
    /// need not be `ok`, and is only ever dropped.
    fn spent() -> Self;

    /// The index of the weights `ws`.
    fn ix_from(ws: Vec<W>) -> (r: Self)
        requires
            Self::fits(ws@),
        ensures
            r.ok(),
            r.ws() == ws@,
    ;

    /// An index of its own over the same weights: the index's `clone`,
    /// for weights whose clone is the weight.
    fn ix_copy(&self) -> (r: Self)
        where W: Clone
        requires
            self.ok(),
            exact_clone::<W>(),
        ensures
            r.ok(),
            r.ws() == self.ws(),
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


} // verus!

// ── BitIndex: Fenwick-backed ───────────────────────────────────────────────

verus! {

/// A Fenwick-tree index of slab weights.  Holds a parallel cache of
/// per-slab weights alongside the BIT so point updates can read the
/// old weight.
///
/// The queries and updates are verified (`crate::fenwick`) against the
/// weights' item counts, given `wf`; only the `IntoIterator`
/// plumbing of `from_weights` and `splice` is not.
#[derive(Debug)]
pub struct BitIndex<W: SlabWeight> {
    weights: Vec<W>,
    bit: Vec<W>,
}

// Written out, where it was derived, to say what a clone is (see the
// tree's, in `btree.rs`).
impl<W: SlabWeight> Clone for BitIndex<W> {
    /// An index of its own with the same weights and sums.
    #[inline]
    fn clone(&self) -> (r: Self)
        ensures
            exact_clone::<W>() ==> self.same(r),
    {
        let r = BitIndex { weights: self.weights.clone(), bit: self.bit.clone() };
        assert(exact_clone::<W>() ==> r.weights@ =~= self.weights@ && r.bit@ =~= self.bit@);
        r
    }
}

impl<W: SlabWeight> BitIndex<W> {
    /// Index `o` holds what this index holds: what a clone of it is.
    pub closed spec fn same(&self, o: Self) -> bool {
        o.weights@ == self.weights@ && o.bit@ == self.bit@
    }

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
            r.weights@ == Seq::<W>::empty(),
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
            r.weights@ == weights@,
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

    proof fn lemma_plain() {
    }

    proof fn lemma_wlen(w: W)
        where W: crate::btree::SlabAggregate
    {
        w.lemma_weight_len();
    }

    proof fn lemma_fits(ws: Seq<W>) {
        assert(ws.map_values(|w: W| Self::wlen(w)) =~= lens(ws));
    }

    fn w_len(w: &W) -> usize {
        w.len()
    }

    fn ix_empty() -> Self {
        Self::empty()
    }

    fn spent() -> Self {
        BitIndex { weights: Vec::new(), bit: Vec::new() }
    }

    fn ix_from(ws: Vec<W>) -> Self {
        Self::from_vec(ws)
    }

    fn ix_copy(&self) -> Self {
        self.clone()
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

/// Where every merge whose item counts fit is representable
/// (`A::plain()`: item counts, prefix sums), weights whose counts fit are
/// valid.
pub proof fn lemma_plain_valid<A: SlabAggregate>(ws: Seq<A>)
    requires
        A::plain(),
        sum_len(ws) <= usize::MAX,
    ensures
        valid(ws),
{
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

/// The B-tree as an index: its weights are its view, and it is well-formed
/// when it is `ok`.
pub(crate) proof fn lemma_btree_index<A: SlabAggregate>(ix: SlabBTree<A>)
    ensures
        ix.ws() == ix@,
        ix.ok() == ix.wf(),
{
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

    /// The tree keeps its shape on item counts alone; its aggregates are
    /// exact wherever they are representable (`valid`), which only its
    /// queries need. So any weights whose counts fit will do, even a
    /// delta column's in the middle of an edit, when the sums at a seam
    /// can overflow for a moment.
    open spec fn fits(ws: Seq<A>) -> bool {
        ws.len() <= usize::MAX && sum_len(ws) <= usize::MAX
    }

    open spec fn plain() -> bool {
        true
    }

    proof fn lemma_plain() {
    }

    proof fn lemma_wlen(w: A)
        where A: crate::btree::SlabAggregate
    {
    }

    proof fn lemma_fits(ws: Seq<A>) {
        assert(ws.map_values(|w: A| Self::wlen(w)) =~= alens(ws));
        lemma_sum_len_wlens(ws, ws.len() as int);
        assert(ws.take(ws.len() as int) =~= ws);
    }

    fn w_len(w: &A) -> usize {
        w.len()
    }

    fn ix_empty() -> Self {
        Self::new()
    }

    fn spent() -> Self {
        Self::spent()
    }

    fn ix_from(ws: Vec<A>) -> Self {
        Self::from_vec(ws)
    }

    fn ix_copy(&self) -> Self {
        let r = self.clone();
        proof {
            self.lemma_same_tree(&r);
        }
        r
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

// ── SlabBTree: B-tree-backed ───────────────────────────────────────────────

impl<A: SlabAggregate> Sealed for SlabBTree<A> {}
