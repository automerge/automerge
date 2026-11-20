//! The iterator of a prefix column: its values, each with what the
//! values up to it add up to.
//!
//! [`VerifiedColumn::prefix_iter`] gives a [`VerifiedPrefixIter`]. It
//! moves as a [`VerifiedIter`] does, and keeps as it goes the sum of
//! the values it has passed ([`total`](VerifiedPrefixIter::total)):
//! of those from the column's start, or from where
//! [`reset_prefix`](VerifiedPrefixIter::reset_prefix) was last called
//! (`base_pos`). Each value it gives comes as a
//! [`VerifiedPrefixedValue`]: the value, the sum through it and the sum
//! before it. A move that finds a value ([`delta_nth`], [`advance_prefix`])
//! gives a [`VerifiedPrefixSeek`]: the value with its sums, where it is,
//! and what the values passed over on the way add up to.
//!
//! [`delta_nth`]: VerifiedPrefixIter::delta_nth
//! [`advance_prefix`]: VerifiedPrefixIter::advance_prefix
//!
//! As with the sums a prefix column answers with, what is proved is
//! said of integers: the running sum *is* what the column's values from
//! `base_pos()` up to the iterator add up to (`sum_to`), for the sums
//! of a prefix column fit the type they are kept in.
//!
//! The moves are [`PrefixIter`](crate::PrefixIter)'s, the shared ones of
//! `prefix_core.rs` (`p_next`, `p_nth`, `p_next_run_from`, `p_delta_nth`,
//! `p_reset`) and `prefix_search.rs` (`p_advance`), instantiated with
//! the verified sums: a long move asks the column's index for the slab
//! to go to, and for what the slabs passed over add up to.
//!
//! An iterator's place is kept without the borrow of its column in a
//! [`VerifiedPrefixIterState`] ([`VerifiedPrefixIter::suspend`]), from
//! which [`VerifiedPrefixIterState::try_resume`] gives an iterator
//! again: one whose running sum counts from where the suspended one's
//! did.

use super::prefix::SumIndex;
#[cfg(verus_keep_ghost)]
use super::shift::{lemma_singles, lemma_svs_inj};
#[cfg(verus_keep_ghost)]
use super::sums::{
    lemma_nsum, lemma_sum_diff, lemma_sum_range, lemma_sum_run, lemma_sums_fit,
    lemma_sums_fit_take, lemma_vsum, summed, sums_fit,
};
use super::sums::{SumOf, SumWeight, VCount, VSum, VSummed};
use super::Get;
#[cfg(verus_keep_ghost)]
use super::{head_run, lemma_svs_sub, VGet, VSpec};
use super::{VCodec, VValue, VerifiedColumn, VerifiedIter, VerifiedIterState, VerifiedRuns};
use super::{VShiftable, VerifiedRanges, VerifiedUnshift};
#[cfg(verus_keep_ghost)]
use crate::btree::PrefixAggregate;
use crate::btree::SlabBTree;
#[cfg(verus_keep_ghost)]
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::{slab_lens, Slabs};
#[cfg(verus_keep_ghost)]
use crate::fenwick::total;
#[cfg(verus_keep_ghost)]
use crate::iter_core::{past, vals_of, window};
use crate::iter_core::{Decoders, ICore};
#[cfg(verus_keep_ghost)]
use crate::prefix_core::{at_sum, before, index_ok, vsum, PrefixWeight};
use crate::prefix_core::{p_delta_nth, p_next, p_next_run_from, p_nth, p_reset};
use crate::prefix_search::p_advance;
#[cfg(verus_keep_ghost)]
use crate::prefix_search::{nsum, PrefixOrd};
#[cfg(verus_keep_ghost)]
use crate::window::windows;
use crate::{Leb128, PackError, Run};
use std::marker::PhantomData;
use std::ops::Range;
use vstd::prelude::*;

verus! {

// ── A value with its sums ───────────────────────────────────────────────────

/// A value of a prefix column with the running sum through it, as a
/// [`VerifiedPrefixIter`] gives it.
///
/// * [`prefix()`](Self::prefix) is what the values before this one add
///   up to;
/// * [`total()`](Self::total) is what the values through this one do.
///
/// Both count from where the iterator's running sum does: the column's
/// start, unless the iterator's sum was reset
/// ([`VerifiedPrefixIter::reset_prefix`]). With values `[1, 1, 1]`, the
/// one at index 1 has `prefix() == 1` and `total() == 2`.
pub struct VerifiedPrefixedValue<'a, V: VSummed> {
    /// The value.
    pub value: Get<'a, V>,
    /// The running sum through the value: what [`total()`](Self::total)
    /// returns. (Verus lets a verified client read the fields of a type
    /// only if it may read them all, so this one is public too; nothing
    /// is promised of a value a client makes itself.)
    pub total: V::Sum,
}

impl<'a, V: VSummed> VerifiedPrefixedValue<'a, V> {
    /// [`total`](Self::total), as a specification sees it: there it is
    /// written `total()`.
    pub open spec fn spec_total(&self) -> V::Sum {
        self.total
    }

    /// [`prefix`](Self::prefix), as a specification sees it: there it is
    /// written `prefix()`. It is the sum through the value less what the
    /// value adds.
    pub open spec fn spec_prefix(&self) -> V::Sum {
        V::Sum::of_int(self.spec_total().as_int() - V::amount(self.value.sv()))
    }

    /// The running sum through this value.
    #[verifier::when_used_as_spec(spec_total)]
    #[inline]
    pub fn total(&self) -> (r: V::Sum)
        ensures
            r == self.total(),
    {
        self.total
    }

    /// The running sum of the values before this one: `total()` less
    /// what this value adds.
    #[verifier::when_used_as_spec(spec_prefix)]
    #[inline]
    pub fn prefix(&self) -> (r: V::Sum)
        ensures
            r == self.prefix(),
    {
        self.total.w_sub(V::to_part(self.value))
    }
}

impl<'a, V: VSummed> Clone for VerifiedPrefixedValue<'a, V> {
    fn clone(&self) -> (r: Self)
        ensures
            r == *self,
    {
        *self
    }
}

impl<'a, V: VSummed> Copy for VerifiedPrefixedValue<'a, V> {

}

// ── A value found, and how far it was ───────────────────────────────────────
/// What [`VerifiedPrefixIter::delta_nth`],
/// [`VerifiedPrefixIter::advance_prefix`] and [`VerifiedColumn::delta`]
/// find: a value with its sums, where it is, and what the values passed
/// over on the way to it add up to.
///
/// With values `[1, 2, 3, 4]`, `delta_nth(2)` of an iterator at the
/// start finds the `3`: `pos == 2`, `delta == 3` (the `1` and the `2`
/// passed over), `pv.prefix() == 3` and `pv.total() == 6`.
pub struct VerifiedPrefixSeek<'a, V: VSummed> {
    /// The index in the column of the value found. The iterator stands
    /// just after it.
    pub pos: usize,
    /// What the values the move passed over before the value found add
    /// up to: the running sum before the value, less the running sum as
    /// it was before the move.
    pub delta: V::Sum,
    /// The value found, with the running sums through it and before it.
    pub pv: VerifiedPrefixedValue<'a, V>,
}

impl<'a, V: VSummed> Clone for VerifiedPrefixSeek<'a, V> {
    fn clone(&self) -> (r: Self)
        ensures
            r == *self,
    {
        *self
    }
}

impl<'a, V: VSummed> Copy for VerifiedPrefixSeek<'a, V> {

}

// ── What the running sum is ─────────────────────────────────────────────────
/// One more value adds its amount to the sum of a range.
proof fn lemma_step<V: VSummed>(c: Seq<<V as VSpec>::S>, b: int, k: int)
    requires
        0 <= b <= k < c.len(),
    ensures
        summed::<V>(c.subrange(b, k + 1)) == summed::<V>(c.subrange(b, k)) + V::amount(c[k]),
{
    assert(c.subrange(b, k + 1).drop_last() =~= c.subrange(b, k));
}

/// The running sum of an iterator at `pos` whose sum counts from
/// `base_at`, as the shared cores keep it, is what the values from
/// `base_at` up to `pos` add up to.
proof fn lemma_running<'a, V: VSummed + VValue<C>, C: VCodec>(
    s: Seq<Slab<<V::E as Slabs>::Tail>>,
    base: V::Sum,
    sum: V::Sum,
    base_at: int,
    pos: int,
)
    requires
        0 <= base_at <= pos <= vals_of::<V::X>(s).len() <= usize::MAX,
        base == vsum::<Get<'a, V>, SumOf<V>>(before::<V::X>(s, base_at)),
        <SumWeight<V::Sum> as PrefixAggregate>::prefix_plus(base, sum) == vsum::<Get<'a, V>, SumOf<V>>(
            before::<V::X>(s, pos),
        ),
        sums_fit::<V>(<Get<'a, V> as VGet>::svs(vals_of::<V::X>(s))),
    ensures
        sum.as_int() == summed::<V>(<Get<'a, V> as VGet>::svs(vals_of::<V::X>(s)).subrange(base_at, pos)),
{
    let o = vals_of::<V::X>(s);
    let c = <Get<'a, V> as VGet>::svs(o);
    <Get<'a, V> as VGet>::lemma_svs(o);
    let b0 = before::<V::X>(s, base_at);
    let b1 = before::<V::X>(s, pos);
    assert(b0 =~= o.subrange(0, base_at));
    assert(b1 =~= o.subrange(0, pos));
    lemma_svs_sub(o, 0, base_at);
    lemma_svs_sub(o, 0, pos);
    assert(c.subrange(0, base_at) =~= c.take(base_at));
    assert(c.subrange(0, pos) =~= c.take(pos));
    lemma_vsum::<V>(b0);
    lemma_vsum::<V>(b1);
    lemma_sums_fit_take::<V>(c, base_at);
    lemma_sums_fit_take::<V>(c, pos);
    lemma_sum_range::<V>(c, base_at, pos);
    lemma_sum_diff::<V::Sum>(base, sum, summed::<V>(c.take(base_at)), summed::<V>(c.take(pos)));
}

/// What the values before index `i` add up to, as the shared cores
/// keep it, is the sum of the column's first `i` values.
proof fn lemma_sum_before<'a, V: VSummed + VValue<C>, C: VCodec>(s: Seq<Slab<<V::E as Slabs>::Tail>>, i: int)
    requires
        0 <= i <= vals_of::<V::X>(s).len(),
    ensures
        vsum::<Get<'a, V>, SumOf<V>>(before::<V::X>(s, i)) == V::Sum::of_int(
            summed::<V>(<Get<'a, V> as VGet>::svs(vals_of::<V::X>(s)).take(i)),
        ),
{
    let o = vals_of::<V::X>(s);
    let c = <Get<'a, V> as VGet>::svs(o);
    let b = before::<V::X>(s, i);
    assert(b =~= o.subrange(0, i));
    lemma_svs_sub(o, 0, i);
    assert(c.subrange(0, i) =~= c.take(i));
    lemma_vsum::<V>(b);
}

// ── The iterator ────────────────────────────────────────────────────────────
/// An iterator over a prefix column's values that keeps their running
/// sum, which [`VerifiedColumn::prefix_iter`] and
/// [`VerifiedColumn::prefix_iter_range`] give.
///
/// It is a `std::iter::Iterator` of [`VerifiedPrefixedValue`]s. The
/// values it has left to give are in a specification `rest()`, as a
/// [`VerifiedIter`]'s are, and what it gives of each is in `items()`.
/// Verus's specifications of the standard library's iterator functions
/// speak of `remaining()`, which is `items()`.
///
/// The running sum, [`total`](Self::total), is what the column's values
/// from `base_pos()` up to the iterator add up to: `sum_to(pos())`.
/// `base_pos()` is the column's start until
/// [`reset_prefix`](Self::reset_prefix) makes it where the iterator
/// stands.
pub struct VerifiedPrefixIter<'a, V: VSummed + VValue<C> + 'a, C: VCodec + 'a = Leb128> {
    /// The column's index, for long moves; a default iterator has none.
    ix: Option<&'a SlabBTree<SumWeight<V::Sum>>>,
    inner: VerifiedIter<'a, V, C, SumIndex>,
    /// What the values from `base_at` up to the iterator add up to.
    total: V::Sum,
    /// What the values before `base_at` add up to.
    base: V::Sum,
    /// Where the running sum counts from: 0 until a `reset_prefix`.
    base_at: usize,
}

impl<'a, V: VSummed + VValue<C>, C: VCodec> VerifiedPrefixIter<'a, V, C> {
    /// The running sum is kept as the shared cores ask, over the index
    /// of a column whose sums fit.
    #[verifier::type_invariant]
    spec fn inv(&self) -> bool {
        self.ok()
    }

    /// The invariant, for the lemmas to ask for.
    spec fn ok(&self) -> bool {
        let s = self.inner.slabs@;
        &&& at_sum::<V::X, SumOf<V>>(self.inner.it, self.total, self.base, s)
        &&& index_ok::<V::X, SumOf<V>>(self.ix, s)
        &&& self.base_at <= self.inner.it.pos <= total(slab_lens(s))
        &&& self.base == vsum::<Get<'a, V>, SumOf<V>>(before::<V::X>(s, self.base_at as int))
        &&& sums_fit::<V>(<Get<'a, V> as VGet>::svs(vals_of::<V::X>(s)))
    }

    /// The iterator without its sums: a [`VerifiedIter`] that stands
    /// where this one does, over the same column, with the same values
    /// left.
    pub closed spec fn plain(&self) -> VerifiedIter<'a, V, C, SumIndex> {
        self.inner
    }

    /// The values the iterator has left to give.
    pub open spec fn rest(&self) -> Seq<V::S> {
        self.plain().rest()
    }

    /// The values of the column the iterator reads: all of them, whatever
    /// the iterator's window.
    pub open spec fn column(&self) -> Seq<V::S> {
        self.plain().column()
    }

    /// The column the iterator reads, as it was when the iterator was
    /// made of it. No method of the iterator changes it.
    pub open spec fn source(&self) -> Option<VerifiedColumn<V, C, SumIndex>> {
        self.plain().source()
    }

    /// [`pos`](Self::pos), as a specification sees it: there it is
    /// written `pos()`.
    pub open spec fn spec_pos(&self) -> usize {
        self.plain().spec_pos()
    }

    /// [`end_pos`](Self::end_pos), as a specification sees it: there it
    /// is written `end_pos()`.
    pub open spec fn spec_end_pos(&self) -> usize {
        self.plain().spec_end_pos()
    }

    /// [`total`](Self::total), as a specification sees it: there it is
    /// written `total()`.
    pub closed spec fn spec_total(&self) -> V::Sum {
        self.total
    }

    /// The index in the column that the running sum counts from: 0,
    /// until a [`reset_prefix`](Self::reset_prefix) makes it where the
    /// iterator stands then.
    pub closed spec fn base_pos(&self) -> usize {
        self.base_at
    }

    /// What the column's values from `base_pos()` up to index `to` add
    /// up to. The running sum is `sum_to(pos())`.
    pub open spec fn sum_to(&self, to: int) -> int {
        summed::<V>(self.column().subrange(self.base_pos() as int, to))
    }

    /// What the iterator has left to give: each of the values left, with
    /// the running sum through it.
    pub closed spec fn items(&self) -> Seq<VerifiedPrefixedValue<'a, V>> {
        let o = self.inner.items();
        Seq::new(
            o.len(),
            |i: int|
                VerifiedPrefixedValue { value: o[i], total: V::Sum::of_int(self.sum_to(self.inner.it.pos + i + 1)) },
        )
    }

    /// What the invariant says, in the words of the specifications.
    proof fn lemma_sums(&self)
        requires
            self.ok(),
        ensures
            self.spec_pos() == self.inner.it.pos,
            self.spec_end_pos() == self.inner.it.pos + self.inner.it.items_left,
            self.base_pos() <= self.spec_pos() <= self.spec_end_pos(),
            self.spec_pos() <= self.column().len() <= usize::MAX,
            self.column().len() == total(slab_lens(self.inner.slabs@)),
            sums_fit::<V>(self.column()),
            self.spec_total().as_int() == self.sum_to(self.spec_pos() as int),
            self.rest() == window(self.column(), self.spec_pos() as int, self.spec_end_pos() as int),
            self.rest().len() == self.inner.items().len(),
            self.items().len() == self.inner.items().len(),
    {
        let s = self.inner.slabs@;
        self.inner.lemma_window();
        self.inner.it.lemma_out_len::<V::X>(s);
        <Get<'a, V> as VGet>::lemma_svs(vals_of::<V::X>(s));
        lemma_running::<V, C>(s, self.base, self.total, self.base_at as int, self.inner.it.pos as int);
    }

    /// What the `i`th of the values left is given as.
    proof fn lemma_gives(&self, i: int, pv: VerifiedPrefixedValue<'a, V>)
        requires
            self.ok(),
            0 <= i < self.inner.items().len(),
            pv.value == self.inner.items()[i],
            pv.spec_total().as_int() == self.sum_to(self.spec_pos() + i + 1),
        ensures
            pv == self.items()[i],
            pv.value.sv() == self.rest()[i],
            pv.spec_prefix().as_int() == self.sum_to(self.spec_pos() + i),
            pv.spec_total().as_int() == pv.spec_prefix().as_int() + V::amount(pv.value.sv()),
            i == 0 ==> pv.spec_prefix() == self.spec_total(),
    {
        self.lemma_sums();
        let c = self.column();
        let b = self.base_at as int;
        let k = self.inner.it.pos + i;
        <Get<'a, V> as VGet>::lemma_svs(self.inner.items());
        // the value is the column's at `k`
        assert(self.rest()[i] == c[k]);
        lemma_sum_range::<V>(c, b, k);
        lemma_sum_range::<V>(c, b, k + 1);
        lemma_step::<V>(c, b, k);
        V::Sum::lemma_as_int(pv.spec_total());
        V::Sum::lemma_of_int(self.sum_to(k), 0);
        V::Sum::lemma_as_int(self.total);
    }

    /// What a move past `k` of the values left leaves.
    proof fn lemma_moved(&self, after: Self, k: int)
        requires
            self.ok(),
            after.ok(),
            after.inner.slabs == self.inner.slabs,
            after.inner.col == self.inner.col,
            after.base_at == self.base_at,
            0 <= k <= self.inner.items().len(),
            after.inner.items() == self.inner.items().skip(k),
            after.inner.it.pos == self.inner.it.pos + k,
        ensures
            after.column() == self.column(),
            after.source() == self.source(),
            after.base_pos() == self.base_pos(),
            after.rest() == self.rest().skip(k),
            after.items() == self.items().skip(k),
            after.spec_pos() == self.spec_pos() + k,
            after.spec_total().as_int() == self.sum_to(self.spec_pos() + k),
    {
        self.lemma_sums();
        after.lemma_sums();
        let o = self.inner.items();
        <Get<'a, V> as VGet>::lemma_svs(o);
        <Get<'a, V> as VGet>::lemma_svs(o.skip(k));
        lemma_svs_sub(o, k, o.len() as int);
        assert(after.rest() =~= self.rest().skip(k));
        assert(after.items() =~= self.items().skip(k));
    }

    /// What the first `k` of the values left add up to is what takes
    /// the running sum from where the iterator stands to `k` values on.
    proof fn lemma_passed(&self, k: int)
        requires
            self.ok(),
            0 <= k <= self.inner.items().len(),
        ensures
            self.rest().take(k) == self.column().subrange(self.spec_pos() as int, self.spec_pos() + k),
            self.sum_to(self.spec_pos() + k) == self.sum_to(self.spec_pos() as int) + summed::<V>(self.rest().take(k)),
            V::Sum::lo() <= summed::<V>(self.rest().take(k)) <= V::Sum::hi(),
    {
        self.lemma_sums();
        let c = self.column();
        let b = self.base_at as int;
        let p = self.inner.it.pos as int;
        assert(self.rest().take(k) =~= c.subrange(p, p + k));
        lemma_sum_range::<V>(c, b, p);
        lemma_sum_range::<V>(c, b, p + k);
        lemma_sum_range::<V>(c, p, p + k);
    }

    /// The iterator stands in the column, at `base_pos()` or after it,
    /// and its running sum is what the column's values from there up to
    /// it add up to. The sums of the column's values fit.
    pub proof fn lemma_total(tracked &self)
        ensures
            self.base_pos() <= self.pos() <= self.column().len(),
            self.total().as_int() == self.sum_to(self.pos() as int),
            sums_fit::<V>(self.column()),
    {
        use_type_invariant(self);
        self.lemma_sums();
    }

    /// What the iterator gives of each value left: the value, and the
    /// sums through it and before it.
    pub proof fn lemma_items(tracked &self)
        ensures
            self.items().len() == self.rest().len(),
            forall|i: int|
                0 <= i < self.items().len() ==> {
                    &&& (#[trigger] self.items()[i]).value.sv() == self.rest()[i]
                    &&& self.items()[i].total().as_int() == self.sum_to(self.pos() + i + 1)
                    &&& self.items()[i].prefix().as_int() == self.sum_to(self.pos() + i)
                    &&& self.items()[i].total().as_int() == self.items()[i].prefix().as_int() + V::amount(
                        self.rest()[i],
                    )
                },
    {
        use_type_invariant(self);
        self.lemma_sums();
        assert forall|i: int| 0 <= i < self.items().len() implies {
            &&& (#[trigger] self.items()[i]).value.sv() == self.rest()[i]
            &&& self.items()[i].total().as_int() == self.sum_to(self.pos() + i + 1)
            &&& self.items()[i].prefix().as_int() == self.sum_to(self.pos() + i)
            &&& self.items()[i].total().as_int() == self.items()[i].prefix().as_int() + V::amount(self.rest()[i])
        } by {
            let k = self.inner.it.pos + i;
            <Get<'a, V> as VGet>::lemma_svs(self.inner.items());
            assert(self.rest()[i] == self.column()[k]);
            lemma_sum_range::<V>(self.column(), self.base_at as int, k + 1);
            V::Sum::lemma_of_int(self.sum_to(k + 1), 0);
            self.lemma_gives(i, self.items()[i]);
        }
    }

    /// What the iterator reads is what its column holds: of
    /// [`source`](Self::source) and [`column`](Self::column), the first
    /// tells the second.
    pub proof fn lemma_source(tracked &self)
        ensures
            match self.source() {
                Some(c) => self.column() == c@,
                None => self.column().len() == 0,
            },
    {
        self.inner.lemma_source();
    }

    /// Write the state a move leaves: the fields are tied to one another
    /// and change together.
    #[inline(always)]
    fn put(&mut self, it: ICore<<V::X as Decoders<'a>>::D>, sum: V::Sum, base: V::Sum, base_at: usize)
        requires
            at_sum::<V::X, SumOf<V>>(it, sum, base, old(self).inner.slabs@),
            base_at <= it.pos <= total(slab_lens(old(self).inner.slabs@)),
            base == vsum::<Get<'a, V>, SumOf<V>>(before::<V::X>(old(self).inner.slabs@, base_at as int)),
        ensures
            final(self).inner.it == it,
            final(self).inner.slabs == old(self).inner.slabs,
            final(self).inner.col == old(self).inner.col,
            final(self).ix == old(self).ix,
            final(self).total == sum,
            final(self).base == base,
            final(self).base_at == base_at,
    {
        proof {
            use_type_invariant(&*self);
            use_type_invariant(&self.inner);
        }
        *self =
        VerifiedPrefixIter {
            ix: self.ix,
            inner: VerifiedIter { it, slabs: self.inner.slabs, col: self.inner.col },
            total: sum,
            base,
            base_at,
        };
    }

    /// The index in the column of the value `next` gives, if it gives
    /// one.
    ///
    /// What the iterator has left is the column's values from here up to
    /// [`end_pos`](Self::end_pos), as far as the column goes. The
    /// iterator stands in the column or at its end, and at `base_pos()`
    /// or after it.
    #[verifier::when_used_as_spec(spec_pos)]
    #[inline]
    pub fn pos(&self) -> (r: usize)
        ensures
            r == self.pos(),
            self.base_pos() <= self.pos() <= self.end_pos(),
            self.pos() <= self.column().len(),
            self.rest() == window(self.column(), self.pos() as int, self.end_pos() as int),
    {
        proof {
            use_type_invariant(self);
            self.lemma_sums();
        }
        self.inner.pos()
    }

    /// Where the iterator's window ends: the index in the column that
    /// its last value comes before ([`VerifiedIter::end_pos`]).
    #[verifier::when_used_as_spec(spec_end_pos)]
    #[inline]
    pub fn end_pos(&self) -> (r: usize)
        ensures
            r == self.end_pos(),
            self.base_pos() <= self.pos() <= self.end_pos(),
            self.pos() <= self.column().len(),
            self.rest() == window(self.column(), self.pos() as int, self.end_pos() as int),
    {
        proof {
            use_type_invariant(self);
            self.lemma_sums();
        }
        self.inner.end_pos()
    }

    /// How far the window reaches from where the iterator stands:
    /// `end_pos() - pos()`. That is how many values are left unless the
    /// window runs past the column's end; `len()` is how many are left.
    #[inline]
    pub fn items_left(&self) -> (r: usize)
        ensures
            r == self.end_pos() - self.pos(),
    {
        proof {
            use_type_invariant(self);
            self.lemma_sums();
        }
        self.inner.it.items_left
    }

    /// The running sum: what the column's values from `base_pos()` up to
    /// the iterator add up to. It is the sum before the value `next`
    /// gives, if it gives one.
    #[verifier::when_used_as_spec(spec_total)]
    #[inline]
    pub fn total(&self) -> (r: V::Sum)
        ensures
            r == self.total(),
            self.base_pos() <= self.pos() <= self.column().len(),
            self.total().as_int() == self.sum_to(self.pos() as int),
    {
        proof {
            use_type_invariant(self);
            self.lemma_sums();
        }
        self.total
    }

    /// The running sum: [`total`](Self::total), by another name.
    #[inline]
    pub fn prefix(&self) -> (r: V::Sum)
        ensures
            r == self.total(),
            self.base_pos() <= self.pos() <= self.column().len(),
            self.total().as_int() == self.sum_to(self.pos() as int),
    {
        self.total()
    }

    /// Start the running sum again from where the iterator stands:
    /// [`total`](Self::total) and the sums of the values given from
    /// here on count from this position. The iterator does not move,
    /// and its window stays.
    pub fn reset_prefix(&mut self)
        ensures
            final(self).rest() == old(self).rest(),
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).pos() == old(self).pos(),
            final(self).end_pos() == old(self).end_pos(),
            final(self).base_pos() == old(self).pos(),
            final(self).total().as_int() == 0,
    {
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
        }
        let it = self.inner.it.dup();
        let mut sum = self.total;
        let mut base = self.base;
        p_reset::<V::X, SumOf<V>>(&it, self.inner.slabs, &mut sum, &mut base);
        let at = it.pos;
        self.put(it, sum, base, at);
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
            // nothing is summed yet
            assert(self.column().subrange(at as int, at as int) =~= Seq::empty());
        }
    }

    /// Make the window end at `end`: the iterator then gives the
    /// column's values from where it stands up to `end`, and none if it
    /// stands at `end` or past it ([`VerifiedIter::set_max`]). The
    /// running sum stays.
    pub fn set_max(&mut self, end: usize)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
            final(self).total() == old(self).total(),
            final(self).pos() == old(self).pos(),
            final(self).end_pos() == if end >= old(self).pos() {
                end
            } else {
                old(self).pos()
            },
            final(self).rest() == window(old(self).column(), old(self).pos() as int, end as int),
    {
        proof {
            use_type_invariant(&*self);
            use_type_invariant(&self.inner);
            self.lemma_sums();
        }
        self.inner.it.set_max::<V::X>(self.inner.slabs, end);
        proof {
            self.inner.lemma_window();
        }
    }

    /// Move on so that the value at index `target` of the column comes
    /// next, keeping the running sum. The iterator moves only forwards:
    /// at `target` or past it already, it stays. With a `target` past
    /// what it has left, it gives all it has, and stands at the end of
    /// that with nothing left.
    ///
    /// Few of the values passed over are looked at ([`nth`](Self::nth)).
    pub fn advance_to(&mut self, target: usize)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
            final(self).total().as_int() == old(self).sum_to(final(self).pos() as int),
            final(self).rest() == past(old(self).rest(), target - old(self).pos()),
            target <= old(self).pos() ==> {
                &&& final(self).pos() == old(self).pos()
                &&& final(self).end_pos() == old(self).end_pos()
            },
            old(self).pos() < target <= old(self).pos() + old(self).rest().len() ==> {
                &&& final(self).pos() == target
                &&& final(self).end_pos() == old(self).end_pos()
            },
            target > old(self).pos() + old(self).rest().len() ==> {
                &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                &&& final(self).end_pos() == final(self).pos()
            },
    {
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
        }
        if target > self.inner.it.pos {
            self.nth(target - self.inner.it.pos - 1);
        }
    }

    /// Move `amount` values on, keeping the running sum: past as many as
    /// the iterator has left, if it has fewer.
    pub fn advance_by(&mut self, amount: usize)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
            final(self).total().as_int() == old(self).sum_to(final(self).pos() as int),
            final(self).rest() == past(old(self).rest(), amount as int),
            amount <= old(self).rest().len() ==> {
                &&& final(self).pos() == old(self).pos() + amount
                &&& final(self).end_pos() == old(self).end_pos()
            },
            amount > old(self).rest().len() ==> {
                &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                &&& final(self).end_pos() == final(self).pos()
            },
    {
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
        }
        if amount > 0 {
            self.nth(amount - 1);
        }
    }

    /// Move the window to `range`, keeping the running sum: the iterator
    /// then gives the column's values there. It moves only forwards: if
    /// `range` starts before the iterator, the window starts where the
    /// iterator stands.
    ///
    /// If `range` starts past its own end or the column's, the iterator
    /// has nothing left, and stands where `range` ends or the column
    /// does, whichever comes first, if that is ahead of it.
    pub fn shift(&mut self, range: Range<usize>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
            final(self).total().as_int() == old(self).sum_to(final(self).pos() as int),
            final(self).rest() == window(
                old(self).column(),
                if range.start <= old(self).pos() {
                    old(self).pos() as int
                } else {
                    range.start as int
                },
                range.end as int,
            ),
            range.start <= old(self).pos() ==> {
                &&& final(self).pos() == old(self).pos()
                &&& final(self).end_pos() == if range.end >= old(self).pos() {
                    range.end
                } else {
                    old(self).pos()
                }
            },
            old(self).pos() < range.start <= range.end && range.start <= old(self).column().len() ==> {
                &&& final(self).pos() == range.start
                &&& final(self).end_pos() == range.end
            },
            old(self).pos() < range.start && (range.start > range.end || range.start > old(self).column().len()) ==> {
                &&& final(self).pos() == if range.end <= old(self).pos() {
                    old(self).pos() as int
                } else if range.end <= old(self).column().len() {
                    range.end as int
                } else {
                    old(self).column().len() as int
                }
                &&& final(self).end_pos() == final(self).pos()
            },
    {
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
        }
        self.set_max(range.end);
        self.advance_to(range.start);
    }

    /// Move the window to `range` and give its first value, if it has
    /// one, with the running sum through it. `range` is to start at the
    /// iterator or after it.
    ///
    /// If the window has no value, the iterator has nothing left, and
    /// stands where [`shift`](Self::shift) leaves it then.
    pub fn shift_next(&mut self, range: Range<usize>) -> (r: Option<VerifiedPrefixedValue<'a, V>>)
        requires
            range.start >= old(self).pos(),
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
            final(self).total().as_int() == old(self).sum_to(final(self).pos() as int),
            ({
                let w = window(old(self).column(), range.start as int, range.end as int);
                &&& w.len() > 0 ==> {
                    &&& r matches Some(pv)
                    &&& pv.value.sv() == w[0]
                    &&& pv.total() == final(self).total()
                    &&& pv.total().as_int() == old(self).sum_to(range.start + 1)
                    &&& pv.prefix().as_int() == old(self).sum_to(range.start as int)
                    &&& pv.total().as_int() == pv.prefix().as_int() + V::amount(pv.value.sv())
                    &&& final(self).rest() == w.skip(1)
                    &&& final(self).pos() == range.start + 1
                    &&& final(self).end_pos() == range.end
                }
                &&& w.len() == 0 ==> {
                    &&& r is None
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == if range.end <= old(self).pos() {
                        old(self).pos() as int
                    } else if range.end <= old(self).column().len() {
                        range.end as int
                    } else {
                        old(self).column().len() as int
                    }
                    &&& final(self).end_pos() == final(self).pos()
                }
            }),
    {
        let ghost (c, p) = (self.column(), self.inner.it.pos as int);
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
        }
        self.set_max(range.end);
        let ghost o1 = self.rest();
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
        }
        let r = self.nth(range.start - self.inner.it.pos);
        proof {
            let (w, n) = (window(c, range.start as int, range.end as int), range.start - p);
            if n < o1.len() {
                assert(w =~= o1.skip(n));
                assert(w.skip(1) =~= o1.skip(n + 1));
            }
        }
        r
    }

    /// The next run: the value the iterator gives next, and how many
    /// times it comes before another value does or the iterator has no
    /// more ([`VerifiedIter::next_run`]). The iterator moves past the
    /// whole run. The sums given are those of the run's last value:
    /// `total()` is the running sum through the run, and `prefix()` the
    /// sum before its last value.
    pub fn next_run(&mut self) -> (r: Option<Run<VerifiedPrefixedValue<'a, V>>>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
            final(self).total().as_int() == old(self).sum_to(final(self).pos() as int),
            match r {
                Some(run) => {
                    &&& head_run(old(self).rest(), run.value.value.sv(), run.count as int)
                    &&& run.value == old(self).items()[run.count - 1]
                    &&& run.value.total() == final(self).total()
                    &&& run.value.total().as_int() == old(self).sum_to(old(self).pos() + run.count)
                    &&& run.value.prefix().as_int() == old(self).sum_to(old(self).pos() + run.count - 1)
                    &&& run.value.total().as_int() == old(self).total().as_int() + V::amount(run.value.value.sv())
                        * run.count
                    &&& final(self).items() == old(self).items().skip(run.count as int)
                    &&& final(self).rest() == old(self).rest().skip(run.count as int)
                    &&& final(self).pos() == old(self).pos() + run.count
                    &&& final(self).end_pos() == old(self).end_pos()
                },
                None => {
                    &&& old(self).rest().len() == 0
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos()
                    &&& final(self).end_pos() == old(self).pos()
                    &&& final(self).total() == old(self).total()
                },
            },
    {
        let ghost me = *self;
        proof {
            use_type_invariant(&*self);
            me.lemma_sums();
            me.inner.it.lemma_out_len::<V::X>(me.inner.slabs@);
        }
        let mut it = self.inner.it.dup();
        let mut sum = self.total;
        let base = self.base;
        let max = it.items_left;
        let r = p_next_run_from::<V::X, SumOf<V>>(&mut it, self.inner.slabs, max, &mut sum, &base);
        self.put(it, sum, base, self.base_at);
        proof {
            use_type_invariant(&*self);
            self.inner.it.lemma_out_len::<V::X>(self.inner.slabs@);
        }
        match r {
            Some(run) => {
                let pv = VerifiedPrefixedValue { value: run.value, total: sum };
                proof {
                    let o = me.inner.items();
                    let n = run.count as int;
                    let v = run.value.sv();
                    me.lemma_moved(*self, n);
                    assert(o.take(n)[n - 1] == run.value);
                    me.lemma_gives(n - 1, pv);
                    me.lemma_passed(n);
                    <Get<'a, V> as VGet>::lemma_svs(o);
                    assert forall|i: int| 0 <= i < n implies #[trigger] me.rest()[i] == v by {
                        assert(o.take(n)[i] == run.value);
                    }
                    if n < o.len() && o[n].sv() == v {
                        <Get<'a, V> as VGet>::lemma_sv_inj(o[n], run.value);
                    }
                    // the run adds its value as many times as it is long
                    assert(me.rest().take(n) =~= Seq::new(n as nat, |_i: int| v));
                    lemma_sum_run::<V>(n as nat, v);
                }
                Some(Run { count: run.count, value: pv })
            },
            None => {
                proof {
                    assert(self.inner.items() =~= me.inner.items().skip(0));
                    me.lemma_moved(*self, 0);
                }
                None
            },
        }
    }

    /// An iterator of the runs the iterator has left to give, without
    /// their sums ([`VerifiedIter::runs`]): what
    /// [`splice_runs`](VerifiedColumn::splice_runs) takes.
    pub fn runs(self) -> (r: VerifiedRuns<'a, V, C, SumIndex>)
        ensures
            r.iter() == self.plain(),
    {
        VerifiedRuns(self.inner)
    }

    /// The `n`th of the values left, counted from 0, as
    /// [`nth`](Self::nth) gives it, with where it is and what the `n`
    /// values passed over before it add up to (`delta`). It and the
    /// values before it are then given; if there are not that many,
    /// none is left.
    pub fn delta_nth(&mut self, n: usize) -> (r: Option<VerifiedPrefixSeek<'a, V>>)
        ensures
            n < old(self).rest().len() ==> {
                &&& r matches Some(seek)
                &&& seek.pos == old(self).pos() + n
                &&& seek.pv == old(self).items()[n as int]
                &&& seek.pv.value.sv() == old(self).rest()[n as int]
                &&& seek.delta.as_int() == summed::<V>(old(self).rest().take(n as int))
                &&& seek.pv.total() == final(self).total()
                &&& seek.pv.total().as_int() == old(self).sum_to(old(self).pos() + n + 1)
                &&& seek.pv.prefix().as_int() == old(self).sum_to(old(self).pos() + n)
                &&& seek.pv.prefix().as_int() == old(self).total().as_int() + seek.delta.as_int()
                &&& seek.pv.total().as_int() == seek.pv.prefix().as_int() + V::amount(seek.pv.value.sv())
                &&& final(self).items() == old(self).items().skip(n + 1)
                &&& final(self).rest() == old(self).rest().skip(n + 1)
                &&& final(self).pos() == old(self).pos() + n + 1
                &&& final(self).end_pos() == old(self).end_pos()
            },
            n >= old(self).rest().len() ==> {
                &&& r is None
                &&& final(self).rest().len() == 0
                &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                &&& final(self).end_pos() == final(self).pos()
            },
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
            final(self).total().as_int() == old(self).sum_to(final(self).pos() as int),
    {
        let ghost me = *self;
        proof {
            use_type_invariant(&*self);
            me.lemma_sums();
            me.inner.it.lemma_out_len::<V::X>(me.inner.slabs@);
        }
        let mut it = self.inner.it.dup();
        let mut sum = self.total;
        let base = self.base;
        let r = p_delta_nth::<V::X, SumOf<V>>(&mut it, self.inner.slabs, self.ix, n, &mut sum, &base);
        self.put(it, sum, base, self.base_at);
        proof {
            use_type_invariant(&*self);
        }
        match r {
            Some((value, delta)) => {
                let pv = VerifiedPrefixedValue { value, total: sum };
                proof {
                    let s = me.inner.slabs@;
                    let p = me.inner.it.pos as int;
                    me.lemma_moved(*self, n + 1);
                    me.lemma_gives(n as int, pv);
                    me.lemma_passed(n as int);
                    // `delta` is what the values passed over add up to
                    lemma_running::<V, C>(s, vsum::<Get<'a, V>, SumOf<V>>(before::<V::X>(s, p)), delta, p, p + n);
                }
                Some(VerifiedPrefixSeek { pos: self.inner.it.pos - 1, delta, pv })
            },
            None => {
                proof {
                    let k = me.inner.items().len() as int;
                    assert(self.inner.items() =~= me.inner.items().skip(k));
                    me.lemma_moved(*self, k);
                }
                None
            },
        }
    }

    /// Where the iterator stands, where its window ends and where its
    /// running sum counts from, to keep without the borrow of the
    /// column: [`VerifiedPrefixIterState::try_resume`] gives an iterator
    /// from it.
    ///
    /// The state fits the column the iterator reads: resumed on that
    /// column, as it is, it gives an iterator that stands where this one
    /// does, with the same values left and the same running sum.
    pub fn suspend(&self) -> (s: VerifiedPrefixIterState<V>)
        ensures
            s.pos() == self.pos(),
            s.end_pos() == self.end_pos(),
            s.base_pos() == self.base_pos(),
            self.rest() == window(self.column(), s.pos() as int, s.end_pos() as int),
            match self.source() {
                Some(c) => s.fits(&c),
                None => true,
            },
    {
        proof {
            use_type_invariant(self);
            self.lemma_sums();
        }
        VerifiedPrefixIterState { inner: self.inner.suspend(), base_at: self.base_at, _phantom: PhantomData }
    }
}

/// Reading on until the running sum is past a limit: for sums that are
/// counts, which no value takes down.
impl<'a, V: VSummed + VValue<C>, C: VCodec> VerifiedPrefixIter<'a, V, C> where V::Sum: VCount {
    /// The sums the search counts by are those of the values left.
    proof fn lemma_counted(&self, j: int)
        requires
            0 <= j <= self.inner.items().len(),
        ensures
            nsum::<Get<'a, V>, SumOf<V>>(self.inner.items().take(j)) == summed::<V>(self.rest().take(j)),
    {
        let o = self.inner.items();
        lemma_nsum::<V>(o.take(j));
        lemma_svs_sub(o, 0, j);
    }

    /// Move on to the first of the values left with which the values
    /// given from here on add up to more than `n`, and past it: it is
    /// given with its sums, where it is, and what the values passed
    /// over before it add up to (`delta`), which is `n` at most. If all
    /// the values left add up to `n` or less, the iterator gives them
    /// all, and none is found.
    ///
    /// Few of the values passed over are looked at: the column's index
    /// is asked for the slab the sum is passed in.
    ///
    /// An iterator of no column (`default()`) has no index to ask: it
    /// finds nothing, and its window is closed where it stands.
    /// ([`PrefixIter`](crate::PrefixIter)'s leaves the window of such
    /// an iterator as it is.)
    #[verifier::spinoff_prover]
    pub fn advance_prefix(&mut self, n: V::Sum) -> (r: Option<VerifiedPrefixSeek<'a, V>>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
            final(self).total().as_int() == old(self).sum_to(final(self).pos() as int),
            match r {
                Some(seek) => {
                    let k = seek.pos - old(self).pos();
                    &&& 0 <= k < old(self).rest().len()
                    &&& seek.pv == old(self).items()[k]
                    &&& seek.pv.value.sv() == old(self).rest()[k]
                    &&& n.as_int() < summed::<V>(old(self).rest().take(k + 1))
                    &&& forall|j: int| 0 <= j <= k ==> summed::<V>(#[trigger] old(self).rest().take(j)) <= n.as_int()
                    &&& seek.delta.as_int() == summed::<V>(old(self).rest().take(k))
                    &&& seek.pv.total() == final(self).total()
                    &&& seek.pv.total().as_int() == old(self).sum_to(seek.pos + 1)
                    &&& seek.pv.prefix().as_int() == old(self).sum_to(seek.pos as int)
                    &&& seek.pv.prefix().as_int() == old(self).total().as_int() + seek.delta.as_int()
                    &&& seek.pv.total().as_int() == seek.pv.prefix().as_int() + V::amount(seek.pv.value.sv())
                    &&& final(self).items() == old(self).items().skip(k + 1)
                    &&& final(self).rest() == old(self).rest().skip(k + 1)
                    &&& final(self).pos() == seek.pos + 1
                    &&& final(self).end_pos() == old(self).end_pos()
                },
                None => {
                    &&& summed::<V>(old(self).rest()) <= n.as_int()
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                    &&& final(self).end_pos() == final(self).pos()
                },
            },
    {
        let ghost me = *self;
        let ghost s = self.inner.slabs@;
        proof {
            use_type_invariant(&*self);
            me.lemma_sums();
            me.inner.it.lemma_out_len::<V::X>(s);
            V::Sum::lemma_count();
            V::Sum::lemma_as_int(n);
        }
        let ix = match self.ix {
            Some(ix) => ix,
            None => {
                // An iterator of no column: there is nothing to find.
                proof {
                    assert(slab_lens(s) =~= Seq::<nat>::empty());
                    assert(me.rest() =~= Seq::empty());
                }
                let at = self.inner.it.pos;
                self.set_max(at);
                return None;
            },
        };
        proof {
            // the sums of the whole column fit, so the search's do
            let c = me.column();
            lemma_nsum::<V>(vals_of::<V::X>(s));
            lemma_sums_fit_take::<V>(c, c.len() as int);
            assert(c.take(c.len() as int) =~= c);
        }
        let was = self.total;
        let mut it = self.inner.it.dup();
        let mut sum = self.total;
        let base = self.base;
        let r = p_advance::<V::X, SumOf<V>>(&mut it, self.inner.slabs, ix, &n, &mut sum, &base);
        self.put(it, sum, base, self.base_at);
        proof {
            use_type_invariant(&*self);
        }
        let ghost o = me.inner.items();
        match r {
            Some(value) => {
                let pv = VerifiedPrefixedValue::<V> { value, total: sum };
                let delta = pv.prefix().w_sub(was);
                proof {
                    let k = self.inner.it.pos - me.inner.it.pos - 1;
                    me.lemma_moved(*self, k + 1);
                    me.lemma_gives(k, pv);
                    me.lemma_passed(k);
                    V::Sum::lemma_of_int(summed::<V>(me.rest().take(k)), 0);
                    me.lemma_counted(k + 1);
                    assert forall|j: int| 0 <= j <= k implies summed::<V>(#[trigger] me.rest().take(j)) <= n.as_int() by {
                        me.lemma_counted(j);
                        if j > 0 {
                            let i = j - 1;
                            assert(nsum::<Get<'a, V>, SumOf<V>>(o.take(i + 1)) <= <SumOf<V> as PrefixOrd<Get<'a, V>>>::pnum(n));
                        } else {
                            assert(me.rest().take(0) =~= Seq::empty());
                        }
                    }
                }
                Some(VerifiedPrefixSeek { pos: self.inner.it.pos - 1, delta, pv })
            },
            None => {
                proof {
                    let k = o.len() as int;
                    assert(self.inner.items() =~= o.skip(k));
                    me.lemma_moved(*self, k);
                    me.lemma_counted(k);
                    assert(o.take(k) =~= o);
                    assert(me.rest().take(k) =~= me.rest());
                }
                None
            },
        }
    }
}

#[cfg(verus_keep_ghost)]
impl<'a, V: VSummed + VValue<C>, C: VCodec> vstd::std_specs::iter::IteratorSpecImpl for VerifiedPrefixIter<'a, V, C> {
    /// The iterator keeps to what Verus asks of an iterator: `next` gives
    /// `remaining()`, one value after another, and then `None`.
    open spec fn obeys_prophetic_iter_laws(&self) -> bool {
        true
    }

    open spec fn remaining(&self) -> Seq<VerifiedPrefixedValue<'a, V>> {
        self.items()
    }

    open spec fn will_return_none(&self) -> bool {
        true
    }

    open spec fn decrease(&self) -> Option<nat> {
        Some(self.items().len())
    }

    open spec fn peek(&self, index: int) -> Option<VerifiedPrefixedValue<'a, V>> {
        if 0 <= index < self.items().len() {
            Some(self.items()[index])
        } else {
            None
        }
    }
}

impl<'a, V: VSummed + VValue<C>, C: VCodec> Iterator for VerifiedPrefixIter<'a, V, C> {
    type Item = VerifiedPrefixedValue<'a, V>;

    /// The next value, if one is left, with the running sum through it.
    /// The sum before it is the iterator's running sum as it was.
    #[inline(always)]
    fn next(&mut self) -> (r: Option<VerifiedPrefixedValue<'a, V>>)
        ensures
            match r {
                Some(pv) => {
                    &&& old(self).rest().len() > 0
                    &&& pv.value.sv() == old(self).rest()[0]
                    &&& pv.prefix() == old(self).total()
                    &&& pv.total() == final(self).total()
                    &&& pv.prefix().as_int() == old(self).sum_to(old(self).pos() as int)
                    &&& pv.total().as_int() == old(self).sum_to(old(self).pos() + 1)
                    &&& pv.total().as_int() == pv.prefix().as_int() + V::amount(pv.value.sv())
                    &&& final(self).rest() == old(self).rest().skip(1)
                    &&& final(self).pos() == old(self).pos() + 1
                    &&& final(self).end_pos() == old(self).end_pos()
                },
                None => {
                    &&& old(self).rest().len() == 0
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos()
                    &&& final(self).end_pos() == old(self).pos()
                    &&& final(self).total() == old(self).total()
                },
            },
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
    {
        let ghost me = *self;
        proof {
            use_type_invariant(&*self);
            me.lemma_sums();
            me.inner.it.lemma_out_len::<V::X>(me.inner.slabs@);
        }
        let mut it = self.inner.it.dup();
        let mut sum = self.total;
        let base = self.base;
        let r = p_next::<V::X, SumOf<V>>(&mut it, self.inner.slabs, &mut sum, &base);
        self.put(it, sum, base, self.base_at);
        proof {
            use_type_invariant(&*self);
        }
        match r {
            Some(value) => {
                let pv = VerifiedPrefixedValue { value, total: sum };
                proof {
                    me.lemma_moved(*self, 1);
                    me.lemma_gives(0, pv);
                    assert(me.items().skip(1) =~= me.items().drop_first());
                }
                Some(pv)
            },
            None => {
                proof {
                    assert(self.inner.items() =~= me.inner.items().skip(0));
                    me.lemma_moved(*self, 0);
                    assert(me.items().skip(0) =~= me.items());
                }
                None
            },
        }
    }

    /// The `n`th of the values left, counted from 0, if there are that
    /// many, with the running sum through it: it and the values before
    /// it are then given. If there are not, none is left.
    ///
    /// Few of the values passed over are looked at: a long move asks the
    /// column's index for the slab to go to and for what the slabs
    /// passed over add up to, and within a slab a whole run is passed
    /// over at once.
    #[inline]
    fn nth(&mut self, n: usize) -> (r: Option<VerifiedPrefixedValue<'a, V>>)
        ensures
            n < old(self).rest().len() ==> {
                &&& r == Some(old(self).items()[n as int])
                &&& r matches Some(pv)
                &&& pv.value.sv() == old(self).rest()[n as int]
                &&& pv.total() == final(self).total()
                &&& pv.total().as_int() == old(self).sum_to(old(self).pos() + n + 1)
                &&& pv.prefix().as_int() == old(self).sum_to(old(self).pos() + n)
                &&& pv.total().as_int() == pv.prefix().as_int() + V::amount(pv.value.sv())
                &&& final(self).items() == old(self).items().skip(n + 1)
                &&& final(self).rest() == old(self).rest().skip(n + 1)
                &&& final(self).pos() == old(self).pos() + n + 1
                &&& final(self).end_pos() == old(self).end_pos()
            },
            n >= old(self).rest().len() ==> {
                &&& r is None
                &&& final(self).rest().len() == 0
                &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                &&& final(self).end_pos() == final(self).pos()
            },
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
            final(self).total().as_int() == old(self).sum_to(final(self).pos() as int),
    {
        let ghost me = *self;
        proof {
            use_type_invariant(&*self);
            me.lemma_sums();
            me.inner.it.lemma_out_len::<V::X>(me.inner.slabs@);
        }
        let mut it = self.inner.it.dup();
        let mut sum = self.total;
        let base = self.base;
        let r = p_nth::<V::X, SumOf<V>>(&mut it, self.inner.slabs, self.ix, n, &mut sum, &base);
        self.put(it, sum, base, self.base_at);
        proof {
            use_type_invariant(&*self);
        }
        match r {
            Some(value) => {
                let pv = VerifiedPrefixedValue { value, total: sum };
                proof {
                    me.lemma_moved(*self, n + 1);
                    me.lemma_gives(n as int, pv);
                }
                Some(pv)
            },
            None => {
                proof {
                    let k = me.inner.items().len() as int;
                    assert(self.inner.items() =~= me.inner.items().skip(k));
                    me.lemma_moved(*self, k);
                }
                None
            },
        }
    }

    /// How many values are left: exactly.
    #[inline]
    fn size_hint(&self) -> (r: (usize, Option<usize>))
        ensures
            r.0 == self.items().len(),
            r.0 == self.rest().len(),
            r.1 == Some(r.0),
    {
        self.inner.size_hint()
    }
}

#[cfg(verus_keep_ghost)]
impl<'a, V: VSummed + VValue<C>, C: VCodec> vstd::std_specs::iter::ExactSizeIteratorSpecImpl for VerifiedPrefixIter<'a, V, C> {
    open spec fn exact_len(&self) -> usize {
        self.items().len() as usize
    }
}

impl<'a, V: VSummed + VValue<C>, C: VCodec> ExactSizeIterator for VerifiedPrefixIter<'a, V, C> {
    /// How many values are left.
    #[inline]
    fn len(&self) -> (len: usize)
        ensures
            len == self.items().len(),
            len == self.rest().len(),
    {
        self.size_hint().0
    }
}

impl<'a, V: VSummed + VValue<C>, C: VCodec> Clone for VerifiedPrefixIter<'a, V, C> {
    /// An iterator of its own that stands where this one does, with the
    /// same values left and the same running sum.
    fn clone(&self) -> (r: Self)
        ensures
            r.items() == self.items(),
            r.rest() == self.rest(),
            r.column() == self.column(),
            r.source() == self.source(),
            r.pos() == self.pos(),
            r.end_pos() == self.end_pos(),
            r.base_pos() == self.base_pos(),
            r.total() == self.total(),
    {
        proof {
            use_type_invariant(self);
            use_type_invariant(&self.inner);
        }
        let r = VerifiedPrefixIter {
            ix: self.ix,
            inner: VerifiedIter { it: self.inner.it.dup(), slabs: self.inner.slabs, col: self.inner.col },
            total: self.total,
            base: self.base,
            base_at: self.base_at,
        };
        proof {
            assert(r.items() =~= self.items());
        }
        r
    }
}

impl<'a, V: VSummed + VValue<C>, C: VCodec> Default for VerifiedPrefixIter<'a, V, C> {
    /// An iterator with nothing to give.
    fn default() -> (r: Self)
        ensures
            r.items().len() == 0,
            r.rest().len() == 0,
            r.column().len() == 0,
            r.source() is None,
            r.pos() == 0,
            r.end_pos() == 0,
            r.base_pos() == 0,
            r.total().as_int() == 0,
    {
        let inner: VerifiedIter<'a, V, C, SumIndex> = VerifiedIter { it: ICore::empty::<V::X>(), slabs: &[], col: None };
        let zero = <V::Sum as VSum>::zero();
        proof {
            let s = inner.slabs@;
            <Get<'a, V> as VGet>::lemma_svs(inner.items());
            inner.it.lemma_out_len::<V::X>(s);
            assert(slab_lens(s) =~= Seq::<nat>::empty());
            <Get<'a, V> as VGet>::lemma_svs(vals_of::<V::X>(s));
            assert(before::<V::X>(s, 0) =~= Seq::<Get<'a, V>>::empty());
            <SumWeight<V::Sum> as PrefixWeight>::lemma_prefix_add(zero, zero, zero);
            V::Sum::lemma_as_int(zero);
            V::Sum::lemma_of_int(0, 0);
            V::lemma_safe();
            lemma_sums_fit::<V>(<Get<'a, V> as VGet>::svs(vals_of::<V::X>(s)));
        }
        VerifiedPrefixIter { ix: None, inner, total: zero, base: zero, base_at: 0 }
    }
}

// ── A place to come back to ─────────────────────────────────────────────────
/// Where a [`VerifiedPrefixIter`] stood, where its window ended and
/// where its running sum counted from, kept without the borrow of the
/// column: [`VerifiedPrefixIter::suspend`] gives it, and
/// [`try_resume`](Self::try_resume) an iterator from it.
///
/// It holds a [`VerifiedIterState`] (`plain()`) and `base_pos()`. The
/// sums themselves are not kept: `try_resume` asks the column for them.
pub struct VerifiedPrefixIterState<V: VSummed> {
    inner: VerifiedIterState,
    /// Where the running sum counts from.
    base_at: usize,
    _phantom: PhantomData<V>,
}

impl<V: VSummed> VerifiedPrefixIterState<V> {
    /// The sum counts from the iterator's place or before it.
    #[verifier::type_invariant]
    spec fn inv(&self) -> bool {
        self.base_at <= self.inner.pos
    }

    /// The state without where the sum counts from: that of a
    /// [`VerifiedIter`] standing where the iterator stood, with its
    /// window.
    pub closed spec fn plain(&self) -> VerifiedIterState {
        self.inner
    }

    /// Where the iterator stood: its `pos()`.
    pub open spec fn pos(&self) -> usize {
        self.plain().pos()
    }

    /// Where the iterator's window ended: its `end_pos()`.
    pub open spec fn end_pos(&self) -> usize {
        self.plain().end_pos()
    }

    /// The count of edits of the column the state was taken of
    /// (`VerifiedColumn::edits`).
    pub open spec fn edits(&self) -> usize {
        self.plain().edits()
    }

    /// Where the iterator's running sum counted from: its `base_pos()`.
    pub closed spec fn base_pos(&self) -> usize {
        self.base_at
    }

    /// The state is a place in `column` as it is
    /// ([`VerifiedIterState`]'s `fits`):
    /// [`try_resume`](Self::try_resume) gives an iterator exactly then.
    pub open spec fn fits<C: VCodec>(&self, column: &VerifiedColumn<V, C, SumIndex>) -> bool where V: VValue<C> {
        self.plain().fits(column)
    }

    /// A state fits only a column that counts the edits it does: one
    /// edited since the state was taken is refused
    /// ([`VerifiedIterState`]'s `lemma_fits`).
    pub proof fn lemma_fits<C: VCodec>(&self, column: &VerifiedColumn<V, C, SumIndex>) where V: VValue<C>
        requires
            self.fits(column),
        ensures
            self.edits() == column.edits(),
    {
        self.plain().lemma_fits(column);
    }

    /// The running sum counts from the iterator's place or before it.
    pub proof fn lemma_base(tracked &self)
        ensures
            self.base_pos() <= self.pos(),
    {
        use_type_invariant(self);
    }

    /// An iterator of `column` that stands where the suspended one
    /// stood, with its window, and whose running sum counts from where
    /// that one's did, if the state fits the column (`fits`); an error
    /// ([`PackError::InvalidResume`]) if it does not
    /// ([`VerifiedIterState::try_resume`]).
    ///
    /// The iterator given is one of `column` whatever column the state
    /// was taken of, and its running sum is `column`'s: what the values
    /// of `column` from `base_pos()` up to the iterator add up to, which
    /// the column's index gives.
    ///
    /// Panics, as [`PrefixColumn`](crate::PrefixColumn)'s does, where an
    /// edit's panic left `column` without its parts and the state would
    /// fit it.
    pub fn try_resume<'a, C: VCodec>(&self, column: &'a VerifiedColumn<V, C, SumIndex>) -> (r: Result<
        VerifiedPrefixIter<'a, V, C>,
        PackError,
    >) where V: VValue<C>
        ensures
            r is Ok <==> self.fits(column),
            r is Ok ==> self.edits() == column.edits(),
            r matches Ok(it) ==> {
                &&& it.source() == Some(*column)
                &&& it.column() == column@
                &&& it.pos() == self.pos()
                &&& it.rest() == window(column@, self.pos() as int, self.end_pos() as int)
                &&& it.end_pos() == self.end_pos() || (self.pos() == column@.len() && it.end_pos() == self.pos())
                &&& it.base_pos() == self.base_pos()
                &&& it.total().as_int() == summed::<V>(column@.subrange(self.base_pos() as int, self.pos() as int))
            },
    {
        proof {
            use_type_invariant(self);
        }
        let inner = match self.inner.try_resume(column) {
            Ok(it) => it,
            Err(e) => {
                return Err(e);
            },
        };
        let ix = column.sum_index();
        let base = column.get_prefix(self.base_at);
        let sum = column.get_prefix(self.inner.pos).w_sub(base);
        proof {
            use_type_invariant(&inner);
            let s = inner.slabs@;
            assert(s == column.core.slabs_spec());
            inner.it.lemma_out_len::<V::X>(s);
            lemma_sum_before::<V, C>(s, self.base_at as int);
            lemma_sum_before::<V, C>(s, self.inner.pos as int);
            let a = summed::<V>(column@.take(self.base_at as int));
            let b = summed::<V>(column@.take(self.inner.pos as int));
            V::Sum::lemma_as_int(base);
            V::Sum::lemma_of_int(b - a, a);
        }
        let it = VerifiedPrefixIter { ix, inner, total: sum, base, base_at: self.base_at };
        proof {
            it.lemma_sums();
        }
        Ok(it)
    }
}

// ── The column's ────────────────────────────────────────────────────────────
/// A prefix column's values with their running sums.
impl<V: VSummed + VValue<C>, C: VCodec> VerifiedColumn<V, C, SumIndex> {
    /// An iterator over the values that keeps their running sum: each
    /// value comes with what the values through it add up to, and what
    /// those before it do.
    #[inline(always)]
    pub fn prefix_iter(&self) -> (it: VerifiedPrefixIter<'_, V, C>)
        ensures
            it.rest() == self@,
            it.column() == self@,
            it.source() == Some(*self),
            it.pos() == 0,
            it.end_pos() == self@.len(),
            it.base_pos() == 0,
            it.total().as_int() == 0,
    {
        let ix = self.sum_index();
        let inner = self.iter();
        let zero = <V::Sum as VSum>::zero();
        proof {
            use_type_invariant(&inner);
            let s = inner.slabs@;
            assert(s == self.core.slabs_spec());
            assert(before::<V::X>(s, 0) =~= Seq::<Get<'static, V>>::empty());
            <SumWeight<V::Sum> as PrefixWeight>::lemma_prefix_add(zero, zero, zero);
            V::Sum::lemma_as_int(zero);
            V::Sum::lemma_of_int(0, 0);
        }
        VerifiedPrefixIter { ix, inner, total: zero, base: zero, base_at: 0 }
    }

    /// An iterator over the values `start..end` that keeps the running
    /// sum, which counts from the column's start: the sum before the
    /// first value it gives is what the values before `start` add up
    /// to. The index gives that; the values before `start` are not all
    /// read.
    pub fn prefix_iter_range(&self, start: usize, end: usize) -> (it: VerifiedPrefixIter<'_, V, C>)
        requires
            start <= end <= self@.len(),
        ensures
            it.rest() == self@.subrange(start as int, end as int),
            it.column() == self@,
            it.source() == Some(*self),
            it.pos() == start,
            it.end_pos() == end,
            it.base_pos() == 0,
            it.total().as_int() == summed::<V>(self@.take(start as int)),
    {
        let mut it = self.prefix_iter();
        it.set_max(end);
        it.advance_to(start);
        it
    }

    /// The value at `index` with what the values through it add up to,
    /// and what those before it do; `None` if `index` is past the end.
    pub fn get_prefixed(&self, index: usize) -> (r: Option<VerifiedPrefixedValue<'_, V>>)
        ensures
            index < self@.len() ==> {
                &&& r matches Some(pv)
                &&& pv.value.sv() == self@[index as int]
                &&& pv.total().as_int() == summed::<V>(self@.take(index + 1))
                &&& pv.prefix().as_int() == summed::<V>(self@.take(index as int))
                &&& pv.total().as_int() == pv.prefix().as_int() + V::amount(pv.value.sv())
            },
            index >= self@.len() ==> r is None,
    {
        let mut it = self.prefix_iter();
        it.nth(index)
    }

    /// The value at `to` with its sums, as
    /// [`get_prefixed`](Self::get_prefixed) gives them, and what the
    /// values `from..to` before it add up to (`delta`); `None` if `to`
    /// is past the end. `from` is to be `to` at most.
    pub fn delta(&self, from: usize, to: usize) -> (r: Option<VerifiedPrefixSeek<'_, V>>)
        requires
            from <= to,
        ensures
            to < self@.len() ==> {
                &&& r matches Some(seek)
                &&& seek.pos == to
                &&& seek.pv.value.sv() == self@[to as int]
                &&& seek.delta.as_int() == summed::<V>(self@.subrange(from as int, to as int))
                &&& seek.pv.total().as_int() == summed::<V>(self@.take(to + 1))
                &&& seek.pv.prefix().as_int() == summed::<V>(self@.take(to as int))
                &&& seek.pv.total().as_int() == seek.pv.prefix().as_int() + V::amount(seek.pv.value.sv())
            },
            to >= self@.len() ==> r is None,
    {
        let mut it = self.prefix_iter();
        it.advance_to(from);
        let r = it.delta_nth(to - from);
        proof {
            if to < self@.len() {
                assert(self@.skip(from as int).take(to - from) =~= self@.subrange(from as int, to as int));
            }
        }
        r
    }
}


// ── Repositioning (`VShiftable`) ───────────────────────────────────────────

impl<'a, V: VSummed + VValue<C>, C: VCodec> VerifiedPrefixIter<'a, V, C> {
    /// The items the iterator ranges over: at each place of the column,
    /// the value there with the running sum through it (from
    /// `base_pos()`).
    pub closed spec fn prefixed(&self) -> Seq<VerifiedPrefixedValue<'a, V>> {
        let o = vals_of::<V::X>(self.inner.slabs@);
        Seq::new(o.len(), |p: int| VerifiedPrefixedValue { value: o[p], total: V::Sum::of_int(self.sum_to(p + 1)) })
    }

    /// What the iterator has left is a window of those.
    proof fn lemma_prefixed(&self)
        ensures
            self.items() == window(self.prefixed(), self.inner.it.pos as int, self.inner.it.pos + self.inner.it.items_left),
    {
        assert(self.items() =~= window(self.prefixed(), self.inner.it.pos as int, self.inner.it.pos + self.inner.it.items_left));
    }

    /// A move that keeps the column and where the sums start keeps what
    /// the iterator ranges over.
    proof fn lemma_same_prefixed(&self, o: Self)
        requires
            self.column() == o.column(),
            self.base_pos() == o.base_pos(),
        ensures
            self.prefixed() == o.prefixed(),
    {
        lemma_svs_inj(vals_of::<V::X>(self.inner.slabs@), vals_of::<V::X>(o.inner.slabs@));
        assert(self.prefixed() =~= o.prefixed());
    }
}


/// The iterator repositioned as any of the verified ones is: its model
/// is each value of the column with the running sum through it, each an
/// item.
impl<'a, V: VSummed + VValue<C>, C: VCodec> VShiftable for VerifiedPrefixIter<'a, V, C> {
    closed spec fn shift_vals(&self) -> Seq<VerifiedPrefixedValue<'a, V>> {
        self.prefixed()
    }

    closed spec fn shift_at(&self) -> int {
        self.inner.it.pos as int
    }

    closed spec fn shift_end(&self) -> int {
        self.inner.it.pos + self.inner.it.items_left
    }

    closed spec fn shift_left(&self) -> Seq<VerifiedPrefixedValue<'a, V>> {
        self.items()
    }

    closed spec fn shift_items(&self) -> Seq<VerifiedPrefixedValue<'a, V>> {
        self.items()
    }

    open spec fn shift_spans(x: VerifiedPrefixedValue<'a, V>) -> Seq<VerifiedPrefixedValue<'a, V>> {
        seq![x]
    }

    open spec fn shift_unit() -> bool {
        true
    }

    proof fn lemma_shift(&self) {
        let xs = self.items();
        lemma_singles(xs);
        assert(xs.map_values(|x: VerifiedPrefixedValue<'a, V>| Self::shift_spans(x)) =~= xs.map_values(
            |x: VerifiedPrefixedValue<'a, V>| seq![x],
        ));
    }

    proof fn lemma_shift_unit(x: VerifiedPrefixedValue<'a, V>) {
    }

    #[inline(always)]
    fn shift_step(&mut self) -> (r: Option<VerifiedPrefixedValue<'a, V>>) {
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
        }
        let r = self.nth(0);
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
            self.lemma_same_prefixed(*old(self));
        }
        r
    }

    #[inline(always)]
    fn shift_nth(&mut self, n: usize) -> (r: Option<VerifiedPrefixedValue<'a, V>>) {
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
        }
        let r = self.nth(n);
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
            self.lemma_same_prefixed(*old(self));
            if n < old(self).items().len() {
                let xs = old(self).items().take(n + 1);
                lemma_singles(xs);
                assert(xs.map_values(|x: VerifiedPrefixedValue<'a, V>| Self::shift_spans(x)) =~= xs.map_values(
                    |x: VerifiedPrefixedValue<'a, V>| seq![x],
                ));
            }
        }
        r
    }

    #[inline(always)]
    fn dup(&self) -> (r: Self) {
        proof {
            use_type_invariant(self);
            use_type_invariant(&self.inner);
        }
        let inner = VerifiedIter { it: self.inner.it.dup(), slabs: self.inner.slabs, col: self.inner.col };
        VerifiedPrefixIter { ix: self.ix, inner, total: self.total, base: self.base, base_at: self.base_at }
    }

    #[inline(always)]
    fn get_pos(&self) -> (r: usize) {
        proof {
            use_type_invariant(self);
            self.lemma_sums();
        }
        self.pos()
    }

    #[inline(always)]
    fn get_max(&self) -> (r: usize) {
        proof {
            use_type_invariant(self);
            self.lemma_sums();
        }
        self.end_pos()
    }

    #[inline(always)]
    fn set_max(&mut self, end: usize) {
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
        }
        VerifiedPrefixIter::set_max(self, end);
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
            self.lemma_same_prefixed(*old(self));
        }
    }

    #[inline(always)]
    fn advance_to(&mut self, target: usize) {
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
            self.lemma_prefixed();
        }
        VerifiedPrefixIter::advance_to(self, target);
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
            self.lemma_prefixed();
            self.lemma_same_prefixed(*old(self));
        }
    }

    #[inline(always)]
    fn advance_by(&mut self, amount: usize) {
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
            self.lemma_prefixed();
        }
        VerifiedPrefixIter::advance_by(self, amount);
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
            self.lemma_prefixed();
            self.lemma_same_prefixed(*old(self));
        }
    }

    #[inline(always)]
    fn seek_to(&mut self, at: usize) -> (r: Option<VerifiedPrefixedValue<'a, V>>) {
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
        }
        let p = self.pos();
        if at >= p {
            let r = self.nth(at - p);
            proof {
                use_type_invariant(&*self);
                self.lemma_sums();
                self.lemma_same_prefixed(*old(self));
            }
            r
        } else {
            None
        }
    }

    #[inline(always)]
    fn shift(&mut self, range: Range<usize>) {
        let ghost p = self.shift_at();
        proof {
            self.lemma_shift();
        }
        // the window's end, and then its start, as the iterator's own
        // `shift` moves
        VShiftable::set_max(self, range.end);
        let ghost p1 = self.shift_at();
        proof {
            self.lemma_shift();
        }
        VShiftable::advance_to(self, range.start);
        proof {
            self.lemma_shift();
            assert(p1 == p);
        }
    }

    #[inline(always)]
    fn shift_next(&mut self, range: Range<usize>) -> (r: Option<VerifiedPrefixedValue<'a, V>>) {
        let ghost (s, e) = (range.start as int, range.end as int);
        let ghost (v, p) = (self.prefixed(), self.shift_at());
        VShiftable::set_max(self, range.end);
        proof {
            self.lemma_shift();
        }
        let q = VShiftable::get_pos(self);
        let ghost o = self.shift_items();
        let r = VShiftable::shift_nth(self, range.start - q);
        proof {
            let w = window(v, s, e);
            let k = s - p;
            if w.len() > 0 {
                assert(o[k] == w[0]);
                let xs = o.take(k + 1);
                lemma_singles(xs);
                assert(xs.map_values(|x: VerifiedPrefixedValue<'a, V>| Self::shift_spans(x)) =~= xs.map_values(
                    |x: VerifiedPrefixedValue<'a, V>| seq![x],
                ));
            }
        }
        r
    }
}

impl<'a, V: VSummed + VValue<C>, C: VCodec> VerifiedPrefixIter<'a, V, C> {
    /// The iterator's repositioning model ([`VShiftable`]) in the terms
    /// of its own specification: at each place of the column, its value
    /// and what the values come to through it; it stands at `pos()`;
    /// what it has left is `items()`, each item a value.
    pub proof fn lemma_shift_model(&self)
        ensures
            self.shift_vals() == self.prefixed(),
            self.prefixed().len() == self.column().len(),
            forall|p: int| 0 <= p < self.column().len() ==> {
                &&& (#[trigger] self.prefixed()[p]).value.sv() == self.column()[p]
                &&& self.prefixed()[p].total == V::Sum::of_int(self.sum_to(p + 1))
            },
            self.shift_at() == self.pos(),
            self.shift_items() == self.items(),
            self.shift_left() == self.items(),
            Self::shift_unit(),
    {
        <Get<'a, V> as VGet>::lemma_svs(vals_of::<V::X>(self.inner.slabs@));
    }

    /// A lookahead: the iterator wrapped so that the item it gives next
    /// can be looked at first ([`VerifiedUnshift::peek`]), as the old
    /// API's `Shiftable::unshift` gives.
    #[inline(always)]
    pub fn unshift(self) -> (r: VerifiedUnshift<Self>)
        ensures
            r.unshift_vals() == self.prefixed(),
            r.unshift_left() == self.items(),
    {
        VerifiedUnshift::new(self)
    }

    /// The items of each of `ranges` of the column, one range after
    /// another, as the old API's `Shiftable::ranges` gives them (what is
    /// left of the iterator's own window is dropped). The ranges ascend
    /// and do not overlap (they may touch), and start where the iterator
    /// stands or later.
    ///
    /// # Panics
    ///
    /// When it comes to a range that starts before where it stands.
    #[inline(always)]
    pub fn ranges<'r>(self, ranges: &'r [Range<usize>]) -> (r: VerifiedRanges<'r, Self>)
        ensures
            r.ranges_vals() == self.prefixed(),
            r.ranges_left() == windows(self.prefixed(), ranges@),
    {
        VerifiedRanges::new(self, ranges)
    }
}

} // verus!
impl<'a, V: VSummed> std::fmt::Debug for VerifiedPrefixedValue<'a, V>
where
    Get<'a, V>: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedPrefixedValue")
            .field("value", &self.value)
            .field("total", &self.total)
            .finish()
    }
}

impl<'a, V: VSummed> PartialEq for VerifiedPrefixedValue<'a, V>
where
    Get<'a, V>: PartialEq,
    V::Sum: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.total == other.total
    }
}

impl<'a, V: VSummed> std::fmt::Debug for VerifiedPrefixSeek<'a, V>
where
    Get<'a, V>: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedPrefixSeek")
            .field("pos", &self.pos)
            .field("delta", &self.delta)
            .field("pv", &self.pv)
            .finish()
    }
}

impl<'a, V: VSummed> PartialEq for VerifiedPrefixSeek<'a, V>
where
    Get<'a, V>: PartialEq,
    V::Sum: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.pos == other.pos && self.delta == other.delta && self.pv == other.pv
    }
}

impl<'a, V: VSummed + VValue<C>, C: VCodec> std::fmt::Debug for VerifiedPrefixIter<'a, V, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedPrefixIter")
            .field("total", &self.total)
            .field("pos", &self.inner.pos())
            .finish()
    }
}
