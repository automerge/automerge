//! The iterator of an accumulator column: its values, each with what
//! the values up to it add up to.
//!
//! `prefix_iter` of a [`VerifiedAccColumn`](super::VerifiedAccColumn)
//! gives a [`VerifiedAccIter`]. It moves as a [`VerifiedIter`] does, and
//! keeps as it goes what the values it has passed add up to
//! ([`total`](VerifiedAccIter::total)): those from the column's start,
//! or from where [`reset_prefix`](VerifiedAccIter::reset_prefix) was
//! last called (`base_pos`). Each value it gives comes as a
//! [`VerifiedAccValue`]: the value, what the values through it add up to
//! and what those before it do. A move that finds a value
//! ([`delta_nth`](VerifiedAccIter::delta_nth)) gives a
//! [`VerifiedAccSeek`]: the value so, where it is, and what the values
//! passed over on the way add up to.
//!
//! It is what [`VerifiedPrefixIter`](super::VerifiedPrefixIter) is of a
//! prefix column, for an accumulator of the client's. What is proved is
//! said of `accumulated`, which adds values in the order the column
//! holds them: the running sum *is* `accumulated` of the column's values
//! from `base_pos()` up to the iterator (`acc_to`). It rests on the laws
//! the client proves of its accumulator ([`VGroup`]), and on nothing
//! else of it: the accumulators of two values need not add up to the
//! same in either order.
//!
//! The moves are [`PrefixIter`](crate::PrefixIter)'s, the shared ones of
//! `prefix_core.rs` (`p_next`, `p_nth`, `p_next_run_from`, `p_delta_nth`,
//! `p_reset`), instantiated with the client's accumulator: a long move
//! asks the column's index for the slab to go to, and for what the
//! slabs passed over add up to. There is no search by sum: accumulators
//! have no order.
//!
//! An iterator's place is kept without the borrow of its column in a
//! [`VerifiedAccIterState`] ([`VerifiedAccIter::suspend`]), from which
//! [`VerifiedAccIterState::try_resume`] gives an iterator again: one
//! whose running sum counts from where the suspended one's did.

#[cfg(verus_keep_ghost)]
use super::acc::{
    accumulated, lemma_acc_cat, lemma_acc_range, lemma_acc_run, lemma_cancel, lemma_vacc, times,
    VGroup,
};
use super::acc::{AccIndex, AccOf, AccWeight, VAccum, VAccumulated};
use super::Get;
#[cfg(verus_keep_ghost)]
use super::{head_run, lemma_svs_sub, VGet, VSpec};
use super::{VCodec, VValue, VerifiedColumn, VerifiedIter, VerifiedIterState, VerifiedRuns};
use crate::btree::SlabBTree;
#[cfg(verus_keep_ghost)]
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::{slab_lens, Slabs};
#[cfg(verus_keep_ghost)]
use crate::fenwick::total;
use crate::iter_core::ICore;
#[cfg(verus_keep_ghost)]
use crate::iter_core::{past, vals_of, window};
#[cfg(verus_keep_ghost)]
use crate::prefix_core::{at_sum, before, index_ok, vsum};
use crate::prefix_core::{p_delta_nth, p_next, p_next_run_from, p_nth, p_reset};
use crate::{Leb128, PackError, Run};
use std::marker::PhantomData;
use std::ops::Range;
use vstd::prelude::*;

verus! {

// ── A value with what the values up to it add up to ─────────────────────────

/// A value of an accumulator column with what the values through it add
/// up to, as a [`VerifiedAccIter`] gives it.
///
/// * [`prefix()`](Self::prefix) is what the values before this one add
///   up to;
/// * [`total()`](Self::total) is what the values through this one do:
///   `prefix()`, and then what the value adds.
///
/// Both count from where the iterator's running sum does: the column's
/// start, unless the iterator's sum was reset
/// ([`VerifiedAccIter::reset_prefix`]).
pub struct VerifiedAccValue<'a, V: VAccumulated> {
    /// The value.
    pub value: Get<'a, V>,
    /// What the values through this one add up to: what
    /// [`total()`](Self::total) returns a copy of. (Verus lets a
    /// verified client read the fields of a type only if it may read
    /// them all, so this one is public too; nothing is promised of a
    /// value a client makes itself.)
    pub total: V::Acc,
}

/// What was added last is taken away again by adding what undoes it:
/// from the laws of a [`VGroup`] alone.
proof fn lemma_undo<A: VGroup>(p: A, a: A)
    ensures
        A::plus(A::plus(p, a), A::minus(A::zero(), a)) == p,
{
    A::lemma_plus(p, a, A::minus(A::zero(), a));
    A::lemma_minus(A::zero(), a);
    A::lemma_plus(p, p, p);
}

impl<'a, V: VAccumulated> VerifiedAccValue<'a, V> {
    /// [`total`](Self::total), as a specification sees it: there it is
    /// written `total()`.
    pub open spec fn spec_total(&self) -> V::Acc {
        self.total
    }

    /// [`prefix`](Self::prefix), as a specification sees it: there it is
    /// written `prefix()`. It is `total()` and then what undoes the
    /// value's own accumulator: the one that, added after it, gives
    /// zero.
    pub open spec fn spec_prefix(&self) -> V::Acc {
        <V::Acc as VGroup>::plus(
            self.spec_total(),
            <V::Acc as VGroup>::minus(<V::Acc as VGroup>::zero(), V::adds(self.value.sv())),
        )
    }

    /// Where `total()` is some `p` and then what the value adds,
    /// `prefix()` is `p`: by the laws of the accumulator.
    pub proof fn lemma_prefix(&self, p: V::Acc)
        requires
            self.total() == <V::Acc as VGroup>::plus(p, V::adds(self.value.sv())),
        ensures
            self.prefix() == p,
    {
        lemma_undo::<V::Acc>(p, V::adds(self.value.sv()));
    }

    /// What the values through this one add up to.
    #[verifier::when_used_as_spec(spec_total)]
    #[inline]
    pub fn total(&self) -> (r: V::Acc)
        ensures
            r == self.total(),
    {
        self.total.dup()
    }

    /// What the values before this one add up to: `total()` with what
    /// this value adds taken away again.
    #[verifier::when_used_as_spec(spec_prefix)]
    #[inline]
    pub fn prefix(&self) -> (r: V::Acc)
        ensures
            r == self.prefix(),
    {
        let adds = V::to_acc(self.value);
        let mut back = <V::Acc as VAccum>::new_zero();
        back.sub_acc(&adds);
        let mut r = self.total.dup();
        r.add_acc(&back);
        r
    }
}

impl<'a, V: VAccumulated> Clone for VerifiedAccValue<'a, V> {
    fn clone(&self) -> (r: Self)
        ensures
            r == *self,
    {
        VerifiedAccValue { value: self.value, total: self.total.dup() }
    }
}

// ── A value found, and how far it was ───────────────────────────────────────
/// What [`VerifiedAccIter::delta_nth`] and an accumulator column's
/// `delta` find: a value with what the values up to it add up to, where
/// it is, and what the values passed over on the way to it add up to.
pub struct VerifiedAccSeek<'a, V: VAccumulated> {
    /// The index in the column of the value found. The iterator stands
    /// just after it.
    pub pos: usize,
    /// What the values the move passed over before the value found add
    /// up to: added after the running sum as it was before the move, it
    /// gives the running sum before the value.
    pub delta: V::Acc,
    /// The value found, with what the values through it add up to and
    /// what those before it do.
    pub pv: VerifiedAccValue<'a, V>,
}

impl<'a, V: VAccumulated> Clone for VerifiedAccSeek<'a, V> {
    fn clone(&self) -> (r: Self)
        ensures
            r == *self,
    {
        VerifiedAccSeek {
            pos: self.pos,
            delta: self.delta.dup(),
            pv: VerifiedAccValue { value: self.pv.value, total: self.pv.total.dup() },
        }
    }
}

// ── What the running sum is ─────────────────────────────────────────────────
/// One more value adds its accumulator after what a range adds up to.
proof fn lemma_step<V: VAccumulated>(c: Seq<<V as VSpec>::S>, b: int, k: int)
    requires
        0 <= b <= k < c.len(),
    ensures
        accumulated::<V>(c.subrange(b, k + 1)) == <V::Acc as VGroup>::plus(
            accumulated::<V>(c.subrange(b, k)),
            V::adds(c[k]),
        ),
{
    assert(c.subrange(b, k + 1).drop_last() =~= c.subrange(b, k));
}

/// The running sum of an iterator at `pos` whose sum counts from
/// `base_at`, as the shared cores keep it, is what the values from
/// `base_at` up to `pos` add up to.
proof fn lemma_running<'a, V: VAccumulated + VValue<C>, C: VCodec>(
    s: Seq<Slab<<V::E as Slabs>::Tail>>,
    base: V::Acc,
    sum: V::Acc,
    base_at: int,
    pos: int,
)
    requires
        0 <= base_at <= pos <= vals_of::<V::X>(s).len(),
        base == vsum::<Get<'a, V>, AccOf<V>>(before::<V::X>(s, base_at)),
        <V::Acc as VGroup>::plus(base, sum) == vsum::<Get<'a, V>, AccOf<V>>(before::<V::X>(s, pos)),
    ensures
        sum == accumulated::<V>(<Get<'a, V> as VGet>::svs(vals_of::<V::X>(s)).subrange(base_at, pos)),
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
    lemma_vacc::<V>(b0);
    lemma_vacc::<V>(b1);
    // what is added after `base` and gives the same is the same
    lemma_acc_range::<V>(c, base_at, pos);
    lemma_cancel::<V::Acc>(base, sum, accumulated::<V>(c.subrange(base_at, pos)));
}

/// What the values before index `i` add up to, as the shared cores
/// keep it, is `accumulated` of the column's first `i` values.
proof fn lemma_acc_before<'a, V: VAccumulated + VValue<C>, C: VCodec>(s: Seq<Slab<<V::E as Slabs>::Tail>>, i: int)
    requires
        0 <= i <= vals_of::<V::X>(s).len(),
    ensures
        vsum::<Get<'a, V>, AccOf<V>>(before::<V::X>(s, i)) == accumulated::<V>(
            <Get<'a, V> as VGet>::svs(vals_of::<V::X>(s)).take(i),
        ),
{
    let o = vals_of::<V::X>(s);
    let c = <Get<'a, V> as VGet>::svs(o);
    let b = before::<V::X>(s, i);
    assert(b =~= o.subrange(0, i));
    lemma_svs_sub(o, 0, i);
    assert(c.subrange(0, i) =~= c.take(i));
    lemma_vacc::<V>(b);
}

// ── The iterator ────────────────────────────────────────────────────────────
/// An iterator over an accumulator column's values that keeps what they
/// add up to, which an accumulator column's `prefix_iter` and
/// `prefix_iter_range` give.
///
/// It is a `std::iter::Iterator` of [`VerifiedAccValue`]s. The values it
/// has left to give are in a specification `rest()`, as a
/// [`VerifiedIter`]'s are, and what it gives of each is in `items()`.
/// Verus's specifications of the standard library's iterator functions
/// speak of `remaining()`, which is `items()`.
///
/// The running sum, [`total`](Self::total), is what the column's values
/// from `base_pos()` up to the iterator add up to: `acc_to(pos())`.
/// `base_pos()` is the column's start until
/// [`reset_prefix`](Self::reset_prefix) makes it where the iterator
/// stands.
///
/// An accumulator is the client's, and its functions may panic. If one
/// does while the iterator moves, the iterator is left with nothing to
/// give, as a `default()` one.
pub struct VerifiedAccIter<'a, V: VAccumulated + VValue<C> + 'a, C: VCodec + 'a = Leb128> {
    /// The column's index, for long moves; a default iterator has none.
    ix: Option<&'a SlabBTree<AccWeight<V::Acc>>>,
    inner: VerifiedIter<'a, V, C, AccIndex>,
    /// What the values from `base_at` up to the iterator add up to.
    total: V::Acc,
    /// What the values before `base_at` add up to.
    base: V::Acc,
    /// Where the running sum counts from: 0 until a `reset_prefix`.
    base_at: usize,
}

impl<'a, V: VAccumulated + VValue<C>, C: VCodec> VerifiedAccIter<'a, V, C> {
    /// The running sum is kept as the shared cores ask, over the index
    /// of the column.
    #[verifier::type_invariant]
    spec fn inv(&self) -> bool {
        self.ok()
    }

    /// The invariant, for the lemmas to ask for.
    spec fn ok(&self) -> bool {
        let s = self.inner.slabs@;
        &&& at_sum::<V::X, AccOf<V>>(self.inner.it, self.total, self.base, s)
        &&& index_ok::<V::X, AccOf<V>>(self.ix, s)
        &&& self.base_at <= self.inner.it.pos <= total(slab_lens(s))
        &&& self.base == vsum::<Get<'a, V>, AccOf<V>>(before::<V::X>(s, self.base_at as int))
    }

    /// The iterator without its sums: a [`VerifiedIter`] that stands
    /// where this one does, over the same column, with the same values
    /// left.
    pub closed spec fn plain(&self) -> VerifiedIter<'a, V, C, AccIndex> {
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
    pub open spec fn source(&self) -> Option<VerifiedColumn<V, C, AccIndex>> {
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
    pub closed spec fn spec_total(&self) -> V::Acc {
        self.total
    }

    /// The index in the column that the running sum counts from: 0,
    /// until a [`reset_prefix`](Self::reset_prefix) makes it where the
    /// iterator stands then.
    pub closed spec fn base_pos(&self) -> usize {
        self.base_at
    }

    /// What the column's values from `base_pos()` up to index `to` add
    /// up to. The running sum is `acc_to(pos())`.
    pub open spec fn acc_to(&self, to: int) -> V::Acc {
        accumulated::<V>(self.column().subrange(self.base_pos() as int, to))
    }

    /// What the iterator has left to give: each of the values left, with
    /// what the values through it add up to.
    pub closed spec fn items(&self) -> Seq<VerifiedAccValue<'a, V>> {
        let o = self.inner.items();
        Seq::new(
            o.len(),
            |i: int| VerifiedAccValue { value: o[i], total: self.acc_to(self.inner.it.pos + i + 1) },
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
            self.spec_total() == self.acc_to(self.spec_pos() as int),
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
    proof fn lemma_gives(&self, i: int, pv: VerifiedAccValue<'a, V>)
        requires
            self.ok(),
            0 <= i < self.inner.items().len(),
            pv.value == self.inner.items()[i],
            pv.spec_total() == self.acc_to(self.spec_pos() + i + 1),
        ensures
            pv == self.items()[i],
            pv.value.sv() == self.rest()[i],
            pv.spec_prefix() == self.acc_to(self.spec_pos() + i),
            pv.spec_total() == <V::Acc as VGroup>::plus(pv.spec_prefix(), V::adds(pv.value.sv())),
            i == 0 ==> pv.spec_prefix() == self.spec_total(),
    {
        self.lemma_sums();
        let c = self.column();
        let b = self.base_at as int;
        let k = self.inner.it.pos + i;
        <Get<'a, V> as VGet>::lemma_svs(self.inner.items());
        // the value is the column's at `k`
        assert(self.rest()[i] == c[k]);
        lemma_step::<V>(c, b, k);
        lemma_undo::<V::Acc>(self.acc_to(k), V::adds(c[k]));
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
            after.spec_total() == self.acc_to(self.spec_pos() + k),
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

    /// What the first `k` of the values left add up to is what, added
    /// after the running sum where the iterator stands, gives it `k`
    /// values on.
    proof fn lemma_passed(&self, k: int)
        requires
            self.ok(),
            0 <= k <= self.inner.items().len(),
        ensures
            self.rest().take(k) == self.column().subrange(self.spec_pos() as int, self.spec_pos() + k),
            self.acc_to(self.spec_pos() + k) == <V::Acc as VGroup>::plus(
                self.acc_to(self.spec_pos() as int),
                accumulated::<V>(self.rest().take(k)),
            ),
    {
        self.lemma_sums();
        let c = self.column();
        let b = self.base_at as int;
        let p = self.inner.it.pos as int;
        assert(self.rest().take(k) =~= c.subrange(p, p + k));
        assert(c.subrange(b, p + k) =~= c.subrange(b, p) + c.subrange(p, p + k));
        lemma_acc_cat::<V>(c.subrange(b, p), c.subrange(p, p + k));
    }

    /// The iterator stands in the column, at `base_pos()` or after it,
    /// and its running sum is what the column's values from there up to
    /// it add up to.
    pub proof fn lemma_total(tracked &self)
        ensures
            self.base_pos() <= self.pos() <= self.column().len(),
            self.total() == self.acc_to(self.pos() as int),
    {
        use_type_invariant(self);
        self.lemma_sums();
    }

    /// What the iterator gives of each value left: the value, what the
    /// values through it add up to and what those before it do.
    pub proof fn lemma_items(tracked &self)
        ensures
            self.items().len() == self.rest().len(),
            forall|i: int|
                0 <= i < self.items().len() ==> {
                    &&& (#[trigger] self.items()[i]).value.sv() == self.rest()[i]
                    &&& self.items()[i].total() == self.acc_to(self.pos() + i + 1)
                    &&& self.items()[i].prefix() == self.acc_to(self.pos() + i)
                    &&& self.items()[i].total() == <V::Acc as VGroup>::plus(
                        self.items()[i].prefix(),
                        V::adds(self.rest()[i]),
                    )
                },
    {
        use_type_invariant(self);
        self.lemma_sums();
        assert forall|i: int| 0 <= i < self.items().len() implies {
            &&& (#[trigger] self.items()[i]).value.sv() == self.rest()[i]
            &&& self.items()[i].total() == self.acc_to(self.pos() + i + 1)
            &&& self.items()[i].prefix() == self.acc_to(self.pos() + i)
            &&& self.items()[i].total() == <V::Acc as VGroup>::plus(self.items()[i].prefix(), V::adds(self.rest()[i]))
        } by {
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

    /// An iterator of no column.
    fn none() -> (r: Self)
        ensures
            r.items().len() == 0,
            r.rest().len() == 0,
            r.column().len() == 0,
            r.source() is None,
            r.pos() == 0,
            r.end_pos() == 0,
            r.base_pos() == 0,
            r.total() == <V::Acc as VGroup>::zero(),
    {
        let inner: VerifiedIter<'a, V, C, AccIndex> = VerifiedIter { it: ICore::empty::<V::X>(), slabs: &[], col: None };
        let zero = <V::Acc as VAccum>::new_zero();
        let base = <V::Acc as VAccum>::new_zero();
        proof {
            let s = inner.slabs@;
            <Get<'a, V> as VGet>::lemma_svs(inner.items());
            inner.it.lemma_out_len::<V::X>(s);
            assert(slab_lens(s) =~= Seq::<nat>::empty());
            <Get<'a, V> as VGet>::lemma_svs(vals_of::<V::X>(s));
            assert(before::<V::X>(s, 0) =~= Seq::<Get<'a, V>>::empty());
            <V::Acc as VGroup>::lemma_plus(zero, zero, zero);
        }
        VerifiedAccIter { ix: None, inner, total: zero, base, base_at: 0 }
    }

    /// The iterator, taken out of its place to be moved: an accumulator
    /// need not be `Copy`, so a move takes the running sum with the
    /// rest of the iterator and writes all of it back, the fields being
    /// tied to one another. Until then an iterator of no column stands
    /// in its place.
    #[inline(always)]
    fn take(&mut self) -> (me: Self)
        ensures
            me == *old(self),
    {
        let mut me = Self::none();
        core::mem::swap(self, &mut me);
        me
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
    /// the iterator add up to. It is what the values before the one
    /// `next` gives add up to, if it gives one.
    #[verifier::when_used_as_spec(spec_total)]
    #[inline]
    pub fn total(&self) -> (r: V::Acc)
        ensures
            r == self.total(),
            self.base_pos() <= self.pos() <= self.column().len(),
            self.total() == self.acc_to(self.pos() as int),
    {
        proof {
            use_type_invariant(self);
            self.lemma_sums();
        }
        self.total.dup()
    }

    /// The running sum: [`total`](Self::total), by another name.
    #[inline]
    pub fn prefix(&self) -> (r: V::Acc)
        ensures
            r == self.total(),
            self.base_pos() <= self.pos() <= self.column().len(),
            self.total() == self.acc_to(self.pos() as int),
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
            final(self).total() == <V::Acc as VGroup>::zero(),
    {
        proof {
            use_type_invariant(&*self);
            use_type_invariant(&self.inner);
            self.lemma_sums();
        }
        let own = self.take();
        let (ix, slabs, col, it) = (own.ix, own.inner.slabs, own.inner.col, own.inner.it);
        let (mut sum, mut base) = (own.total, own.base);
        p_reset::<V::X, AccOf<V>>(&it, slabs, &mut sum, &mut base);
        let at = it.pos;
        *self = VerifiedAccIter { ix, inner: VerifiedIter { it, slabs, col }, total: sum, base, base_at: at };
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
            // nothing is added up yet
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
            final(self).total() == old(self).acc_to(final(self).pos() as int),
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
            final(self).total() == old(self).acc_to(final(self).pos() as int),
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
            final(self).total() == old(self).acc_to(final(self).pos() as int),
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
    /// one, with what the values through it add up to. `range` is to
    /// start at the iterator or after it.
    ///
    /// If the window has no value, the iterator has nothing left, and
    /// stands where [`shift`](Self::shift) leaves it then.
    pub fn shift_next(&mut self, range: Range<usize>) -> (r: Option<VerifiedAccValue<'a, V>>)
        requires
            range.start >= old(self).pos(),
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
            final(self).total() == old(self).acc_to(final(self).pos() as int),
            ({
                let w = window(old(self).column(), range.start as int, range.end as int);
                &&& w.len() > 0 ==> {
                    &&& r matches Some(pv)
                    &&& pv.value.sv() == w[0]
                    &&& pv.total() == final(self).total()
                    &&& pv.total() == old(self).acc_to(range.start + 1)
                    &&& pv.prefix() == old(self).acc_to(range.start as int)
                    &&& pv.total() == <V::Acc as VGroup>::plus(pv.prefix(), V::adds(pv.value.sv()))
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
    /// `total()` is the running sum through the run, which is the
    /// running sum as it was and then what the value adds, as many times
    /// over as the run is long; and `prefix()` is the sum before the
    /// run's last value.
    pub fn next_run(&mut self) -> (r: Option<Run<VerifiedAccValue<'a, V>>>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
            final(self).total() == old(self).acc_to(final(self).pos() as int),
            match r {
                Some(run) => {
                    &&& head_run(old(self).rest(), run.value.value.sv(), run.count as int)
                    &&& run.value == old(self).items()[run.count - 1]
                    &&& run.value.total() == final(self).total()
                    &&& run.value.total() == old(self).acc_to(old(self).pos() + run.count)
                    &&& run.value.prefix() == old(self).acc_to(old(self).pos() + run.count - 1)
                    &&& run.value.total() == <V::Acc as VGroup>::plus(
                        old(self).total(),
                        times::<V::Acc>(run.count as nat, V::adds(run.value.value.sv())),
                    )
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
            use_type_invariant(&self.inner);
            me.lemma_sums();
            me.inner.it.lemma_out_len::<V::X>(me.inner.slabs@);
        }
        let own = self.take();
        let (ix, slabs, col, base_at) = (own.ix, own.inner.slabs, own.inner.col, own.base_at);
        let (mut it, mut sum, base) = (own.inner.it, own.total, own.base);
        let max = it.items_left;
        let r = p_next_run_from::<V::X, AccOf<V>>(&mut it, slabs, max, &mut sum, &base);
        *self = VerifiedAccIter { ix, inner: VerifiedIter { it, slabs, col }, total: sum, base, base_at };
        proof {
            use_type_invariant(&*self);
            self.inner.it.lemma_out_len::<V::X>(self.inner.slabs@);
        }
        match r {
            Some(run) => {
                let pv = VerifiedAccValue { value: run.value, total: self.total.dup() };
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
                    lemma_acc_run::<V>(n as nat, v);
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
    pub fn runs(self) -> (r: VerifiedRuns<'a, V, C, AccIndex>)
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
    pub fn delta_nth(&mut self, n: usize) -> (r: Option<VerifiedAccSeek<'a, V>>)
        ensures
            n < old(self).rest().len() ==> {
                &&& r matches Some(seek)
                &&& seek.pos == old(self).pos() + n
                &&& seek.pv == old(self).items()[n as int]
                &&& seek.pv.value.sv() == old(self).rest()[n as int]
                &&& seek.delta == accumulated::<V>(old(self).rest().take(n as int))
                &&& seek.pv.total() == final(self).total()
                &&& seek.pv.total() == old(self).acc_to(old(self).pos() + n + 1)
                &&& seek.pv.prefix() == old(self).acc_to(old(self).pos() + n)
                &&& seek.pv.prefix() == <V::Acc as VGroup>::plus(old(self).total(), seek.delta)
                &&& seek.pv.total() == <V::Acc as VGroup>::plus(seek.pv.prefix(), V::adds(seek.pv.value.sv()))
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
            final(self).total() == old(self).acc_to(final(self).pos() as int),
    {
        let ghost me = *self;
        proof {
            use_type_invariant(&*self);
            use_type_invariant(&self.inner);
            me.lemma_sums();
            me.inner.it.lemma_out_len::<V::X>(me.inner.slabs@);
        }
        let own = self.take();
        let (ix, slabs, col, base_at) = (own.ix, own.inner.slabs, own.inner.col, own.base_at);
        let (mut it, mut sum, base) = (own.inner.it, own.total, own.base);
        let r = p_delta_nth::<V::X, AccOf<V>>(&mut it, slabs, ix, n, &mut sum, &base);
        *self = VerifiedAccIter { ix, inner: VerifiedIter { it, slabs, col }, total: sum, base, base_at };
        proof {
            use_type_invariant(&*self);
        }
        match r {
            Some((value, delta)) => {
                let pv = VerifiedAccValue { value, total: self.total.dup() };
                proof {
                    let s = me.inner.slabs@;
                    let p = me.inner.it.pos as int;
                    me.lemma_moved(*self, n + 1);
                    me.lemma_gives(n as int, pv);
                    me.lemma_passed(n as int);
                    // `delta` is what the values passed over add up to
                    lemma_running::<V, C>(s, vsum::<Get<'a, V>, AccOf<V>>(before::<V::X>(s, p)), delta, p, p + n);
                }
                Some(VerifiedAccSeek { pos: self.inner.it.pos - 1, delta, pv })
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
    /// column: [`VerifiedAccIterState::try_resume`] gives an iterator
    /// from it.
    ///
    /// The state fits the column the iterator reads: resumed on that
    /// column, as it is, it gives an iterator that stands where this one
    /// does, with the same values left and the same running sum.
    pub fn suspend(&self) -> (s: VerifiedAccIterState<V>)
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
        VerifiedAccIterState { inner: self.inner.suspend(), base_at: self.base_at, _phantom: PhantomData }
    }
}

#[cfg(verus_keep_ghost)]
impl<'a, V: VAccumulated + VValue<C>, C: VCodec> vstd::std_specs::iter::IteratorSpecImpl for VerifiedAccIter<'a, V, C> {
    /// The iterator keeps to what Verus asks of an iterator: `next` gives
    /// `remaining()`, one value after another, and then `None`.
    open spec fn obeys_prophetic_iter_laws(&self) -> bool {
        true
    }

    open spec fn remaining(&self) -> Seq<VerifiedAccValue<'a, V>> {
        self.items()
    }

    open spec fn will_return_none(&self) -> bool {
        true
    }

    open spec fn decrease(&self) -> Option<nat> {
        Some(self.items().len())
    }

    open spec fn peek(&self, index: int) -> Option<VerifiedAccValue<'a, V>> {
        if 0 <= index < self.items().len() {
            Some(self.items()[index])
        } else {
            None
        }
    }
}

impl<'a, V: VAccumulated + VValue<C>, C: VCodec> Iterator for VerifiedAccIter<'a, V, C> {
    type Item = VerifiedAccValue<'a, V>;

    /// The next value, if one is left, with what the values through it
    /// add up to. What those before it add up to is the iterator's
    /// running sum as it was.
    #[inline(always)]
    fn next(&mut self) -> (r: Option<VerifiedAccValue<'a, V>>)
        ensures
            match r {
                Some(pv) => {
                    &&& old(self).rest().len() > 0
                    &&& pv.value.sv() == old(self).rest()[0]
                    &&& pv.prefix() == old(self).total()
                    &&& pv.total() == final(self).total()
                    &&& pv.prefix() == old(self).acc_to(old(self).pos() as int)
                    &&& pv.total() == old(self).acc_to(old(self).pos() + 1)
                    &&& pv.total() == <V::Acc as VGroup>::plus(pv.prefix(), V::adds(pv.value.sv()))
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
            use_type_invariant(&self.inner);
            me.lemma_sums();
            me.inner.it.lemma_out_len::<V::X>(me.inner.slabs@);
        }
        let own = self.take();
        let (ix, slabs, col, base_at) = (own.ix, own.inner.slabs, own.inner.col, own.base_at);
        let (mut it, mut sum, base) = (own.inner.it, own.total, own.base);
        let r = p_next::<V::X, AccOf<V>>(&mut it, slabs, &mut sum, &base);
        *self = VerifiedAccIter { ix, inner: VerifiedIter { it, slabs, col }, total: sum, base, base_at };
        proof {
            use_type_invariant(&*self);
        }
        match r {
            Some(value) => {
                let pv = VerifiedAccValue { value, total: self.total.dup() };
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
    /// many, with what the values through it add up to: it and the
    /// values before it are then given. If there are not, none is left.
    ///
    /// Few of the values passed over are looked at: a long move asks the
    /// column's index for the slab to go to and for what the slabs
    /// passed over add up to, and within a slab a whole run is passed
    /// over at once.
    #[inline]
    fn nth(&mut self, n: usize) -> (r: Option<VerifiedAccValue<'a, V>>)
        ensures
            n < old(self).rest().len() ==> {
                &&& r == Some(old(self).items()[n as int])
                &&& r matches Some(pv)
                &&& pv.value.sv() == old(self).rest()[n as int]
                &&& pv.total() == final(self).total()
                &&& pv.total() == old(self).acc_to(old(self).pos() + n + 1)
                &&& pv.prefix() == old(self).acc_to(old(self).pos() + n)
                &&& pv.total() == <V::Acc as VGroup>::plus(pv.prefix(), V::adds(pv.value.sv()))
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
            final(self).total() == old(self).acc_to(final(self).pos() as int),
    {
        let ghost me = *self;
        proof {
            use_type_invariant(&*self);
            use_type_invariant(&self.inner);
            me.lemma_sums();
            me.inner.it.lemma_out_len::<V::X>(me.inner.slabs@);
        }
        let own = self.take();
        let (ix, slabs, col, base_at) = (own.ix, own.inner.slabs, own.inner.col, own.base_at);
        let (mut it, mut sum, base) = (own.inner.it, own.total, own.base);
        let r = p_nth::<V::X, AccOf<V>>(&mut it, slabs, ix, n, &mut sum, &base);
        *self = VerifiedAccIter { ix, inner: VerifiedIter { it, slabs, col }, total: sum, base, base_at };
        proof {
            use_type_invariant(&*self);
        }
        match r {
            Some(value) => {
                let pv = VerifiedAccValue { value, total: self.total.dup() };
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
impl<'a, V: VAccumulated + VValue<C>, C: VCodec> vstd::std_specs::iter::ExactSizeIteratorSpecImpl for VerifiedAccIter<'a, V, C> {
    open spec fn exact_len(&self) -> usize {
        self.items().len() as usize
    }
}

impl<'a, V: VAccumulated + VValue<C>, C: VCodec> ExactSizeIterator for VerifiedAccIter<'a, V, C> {
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

impl<'a, V: VAccumulated + VValue<C>, C: VCodec> Clone for VerifiedAccIter<'a, V, C> {
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
        let r = VerifiedAccIter {
            ix: self.ix,
            inner: VerifiedIter { it: self.inner.it.dup(), slabs: self.inner.slabs, col: self.inner.col },
            total: self.total.dup(),
            base: self.base.dup(),
            base_at: self.base_at,
        };
        proof {
            assert(r.items() =~= self.items());
        }
        r
    }
}

impl<'a, V: VAccumulated + VValue<C>, C: VCodec> Default for VerifiedAccIter<'a, V, C> {
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
            r.total() == <V::Acc as VGroup>::zero(),
    {
        Self::none()
    }
}

// ── A place to come back to ─────────────────────────────────────────────────
/// Where a [`VerifiedAccIter`] stood, where its window ended and where
/// its running sum counted from, kept without the borrow of the column:
/// [`VerifiedAccIter::suspend`] gives it, and
/// [`try_resume`](Self::try_resume) an iterator from it.
///
/// It holds a [`VerifiedIterState`] (`plain()`) and `base_pos()`. The
/// sums themselves are not kept: `try_resume` asks the column for them.
pub struct VerifiedAccIterState<V: VAccumulated> {
    inner: VerifiedIterState,
    /// Where the running sum counts from.
    base_at: usize,
    _phantom: PhantomData<V>,
}

impl<V: VAccumulated> VerifiedAccIterState<V> {
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
    pub open spec fn fits<C: VCodec>(&self, column: &VerifiedColumn<V, C, AccIndex>) -> bool where V: VValue<C> {
        self.plain().fits(column)
    }

    /// A state fits only a column that counts the edits it does: one
    /// edited since the state was taken is refused
    /// ([`VerifiedIterState`]'s `lemma_fits`).
    pub proof fn lemma_fits<C: VCodec>(&self, column: &VerifiedColumn<V, C, AccIndex>) where V: VValue<C>
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
    pub fn try_resume<'a, C: VCodec>(&self, column: &'a VerifiedColumn<V, C, AccIndex>) -> (r: Result<
        VerifiedAccIter<'a, V, C>,
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
                &&& it.total() == accumulated::<V>(column@.subrange(self.base_pos() as int, self.pos() as int))
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
        let ix = column.acc_index();
        let base = column.get_prefix(self.base_at);
        // what, added after the sum before `base_at`, gives the sum
        // before the iterator
        let mut sum = column.get_prefix(self.inner.pos);
        sum.sub_acc(&base);
        proof {
            use_type_invariant(&inner);
            let s = inner.slabs@;
            assert(s == column.core.slabs_spec());
            inner.it.lemma_out_len::<V::X>(s);
            lemma_acc_before::<V, C>(s, self.base_at as int);
            lemma_acc_before::<V, C>(s, self.inner.pos as int);
            <V::Acc as VGroup>::lemma_minus(accumulated::<V>(column@.take(self.inner.pos as int)), base);
        }
        let it = VerifiedAccIter { ix, inner, total: sum, base, base_at: self.base_at };
        proof {
            it.lemma_sums();
        }
        Ok(it)
    }
}

// ── The column's ────────────────────────────────────────────────────────────
/// An accumulator column's values with what the values up to each add
/// up to.
impl<V: VAccumulated + VValue<C>, C: VCodec> VerifiedColumn<V, C, AccIndex> {
    /// An iterator over the values that keeps what they add up to: each
    /// value comes with what the values through it add up to, and what
    /// those before it do.
    #[inline(always)]
    pub fn prefix_iter(&self) -> (it: VerifiedAccIter<'_, V, C>)
        ensures
            it.rest() == self@,
            it.column() == self@,
            it.source() == Some(*self),
            it.pos() == 0,
            it.end_pos() == self@.len(),
            it.base_pos() == 0,
            it.total() == <V::Acc as VGroup>::zero(),
    {
        let ix = self.acc_index();
        let inner = self.iter();
        let zero = <V::Acc as VAccum>::new_zero();
        let base = <V::Acc as VAccum>::new_zero();
        proof {
            use_type_invariant(&inner);
            let s = inner.slabs@;
            assert(s == self.core.slabs_spec());
            assert(before::<V::X>(s, 0) =~= Seq::<Get<'static, V>>::empty());
            <V::Acc as VGroup>::lemma_plus(zero, zero, zero);
        }
        VerifiedAccIter { ix, inner, total: zero, base, base_at: 0 }
    }

    /// An iterator over the values `start..end` that keeps the running
    /// sum, which counts from the column's start: the sum before the
    /// first value it gives is what the values before `start` add up
    /// to. The index gives that; the values before `start` are not all
    /// read.
    pub fn prefix_iter_range(&self, start: usize, end: usize) -> (it: VerifiedAccIter<'_, V, C>)
        requires
            start <= end <= self@.len(),
        ensures
            it.rest() == self@.subrange(start as int, end as int),
            it.column() == self@,
            it.source() == Some(*self),
            it.pos() == start,
            it.end_pos() == end,
            it.base_pos() == 0,
            it.total() == accumulated::<V>(self@.take(start as int)),
    {
        let mut it = self.prefix_iter();
        it.set_max(end);
        it.advance_to(start);
        it
    }

    /// The value at `index` with what the values through it add up to,
    /// and what those before it do; `None` if `index` is past the end.
    pub fn get_prefixed(&self, index: usize) -> (r: Option<VerifiedAccValue<'_, V>>)
        ensures
            index < self@.len() ==> {
                &&& r matches Some(pv)
                &&& pv.value.sv() == self@[index as int]
                &&& pv.total() == accumulated::<V>(self@.take(index + 1))
                &&& pv.prefix() == accumulated::<V>(self@.take(index as int))
                &&& pv.total() == <V::Acc as VGroup>::plus(pv.prefix(), V::adds(pv.value.sv()))
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
    pub fn delta(&self, from: usize, to: usize) -> (r: Option<VerifiedAccSeek<'_, V>>)
        requires
            from <= to,
        ensures
            to < self@.len() ==> {
                &&& r matches Some(seek)
                &&& seek.pos == to
                &&& seek.pv.value.sv() == self@[to as int]
                &&& seek.delta == accumulated::<V>(self@.subrange(from as int, to as int))
                &&& seek.pv.total() == accumulated::<V>(self@.take(to + 1))
                &&& seek.pv.prefix() == accumulated::<V>(self@.take(to as int))
                &&& seek.pv.total() == <V::Acc as VGroup>::plus(seek.pv.prefix(), V::adds(seek.pv.value.sv()))
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

} // verus!
impl<'a, V: VAccumulated> std::fmt::Debug for VerifiedAccValue<'a, V>
where
    Get<'a, V>: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedAccValue")
            .field("value", &self.value)
            .field("total", &self.total)
            .finish()
    }
}

impl<'a, V: VAccumulated> PartialEq for VerifiedAccValue<'a, V>
where
    Get<'a, V>: PartialEq,
    V::Acc: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.total == other.total
    }
}

impl<'a, V: VAccumulated> std::fmt::Debug for VerifiedAccSeek<'a, V>
where
    Get<'a, V>: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedAccSeek")
            .field("pos", &self.pos)
            .field("delta", &self.delta)
            .field("pv", &self.pv)
            .finish()
    }
}

impl<'a, V: VAccumulated> PartialEq for VerifiedAccSeek<'a, V>
where
    Get<'a, V>: PartialEq,
    V::Acc: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.pos == other.pos && self.delta == other.delta && self.pv == other.pv
    }
}

impl<'a, V: VAccumulated + VValue<C>, C: VCodec> std::fmt::Debug for VerifiedAccIter<'a, V, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedAccIter")
            .field("total", &self.total)
            .field("pos", &self.inner.pos())
            .finish()
    }
}
