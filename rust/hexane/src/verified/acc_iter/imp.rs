//! The implementation of `verified::acc_iter` and its proofs: what its
//! specification (`acc_iter.rs`) says. Each function named `..._impl` here has
//! the contract of the function there that calls it, word for word; a
//! function named after a trait, a method and a type (`vpack_pack_u64`)
//! is the body of that method in that type's impl of the trait, with the
//! trait's contract said of the type.

#![allow(unused_imports)]

#[cfg(verus_keep_ghost)]
use super::super::acc::{lemma_acc_cat, lemma_acc_range, lemma_acc_run, lemma_cancel, lemma_vacc};
#[cfg(verus_keep_ghost)]
use super::super::lemma_svs_sub;
#[cfg(verus_keep_ghost)]
use super::super::shift::{lemma_singles, lemma_svs_inj};
use super::*;
use crate::verified::imp::range_in_exec;

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
#[cfg(verus_keep_ghost)]
use crate::verified::concat;
#[cfg(verus_keep_ghost)]
use crate::window::windows;
use crate::{Leb128, PackError, Run};
use std::marker::PhantomData;
use std::ops::Range;
use vstd::prelude::*;

verus! {

/// What was added last is taken away again by adding what undoes it:
/// from the laws of a [`VGroup`] alone.
pub(super) proof fn lemma_undo<A: VGroup>(p: A, a: A)
    ensures
        A::plus(A::plus(p, a), A::minus(A::zero(), a)) == p,
{
    A::lemma_plus(p, a, A::minus(A::zero(), a));
    A::lemma_minus(A::zero(), a);
    A::lemma_plus(p, p, p);
}

impl<'a, V: Accumulated> AccValue<'a, V> {
    pub(super) proof fn lemma_prefix_impl(&self, p: V::Acc)
        requires
            self.total() == <V::Acc as VGroup>::plus(p, V::adds(self.value.sv())),
        ensures
            self.prefix() == p,
    {
        lemma_undo::<V::Acc>(p, V::adds(self.value.sv()));
    }

    #[inline]
    pub(super) fn total_impl(&self) -> (r: V::Acc)
        ensures
            r == self.total(),
    {
        self.total.dup()
    }

    #[inline]
    pub(super) fn prefix_impl(&self) -> (r: V::Acc)
        ensures
            r == self.prefix(),
    {
        let adds = V::to_acc(self.value);
        let mut back = <V::Acc as Accum>::new_zero();
        back.sub_acc(&adds);
        let mut r = self.total.dup();
        r.add_acc(&back);
        r
    }
}

impl<'a, V: Accumulated> AccSeek<'a, V> {
    pub(super) fn clone_impl(&self) -> (r: Self)
        ensures
            r == *self,
    {
        AccSeek {
            pos: self.pos,
            delta: self.delta.dup(),
            pv: AccValue { value: self.pv.value, total: self.pv.total.dup() },
        }
    }
}

// ── What the running sum is ─────────────────────────────────────────────────
/// One more value adds its accumulator after what a range adds up to.
pub(super) proof fn lemma_step<V: Accumulated>(c: Seq<<V as VSpec>::S>, b: int, k: int)
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
pub(super) proof fn lemma_running<'a, V: Accumulated + ColumnValueRef<C>, C: Codec>(
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
pub(super) proof fn lemma_acc_before<'a, V: Accumulated + ColumnValueRef<C>, C: Codec>(s: Seq<Slab<<V::E as Slabs>::Tail>>, i: int)
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

impl<'a, V: Accumulated + ColumnValueRef<C>, C: Codec> AccIter<'a, V, C> {
    /// The running sum is kept as the shared cores ask, over the index
    /// of the column.
    #[verifier::type_invariant]
    pub(super) open spec fn inv(&self) -> bool {
        self.ok()
    }

    /// The invariant, for the lemmas to ask for.
    pub(super) open spec fn ok(&self) -> bool {
        let s = self.inner.slabs@;
        &&& at_sum::<V::X, AccOf<V>>(self.inner.it, self.total, self.base, s)
        &&& index_ok::<V::X, AccOf<V>>(self.ix, s)
        &&& self.base_at <= self.inner.it.pos <= total(slab_lens(s))
        &&& self.base == vsum::<Get<'a, V>, AccOf<V>>(before::<V::X>(s, self.base_at as int))
    }

    /// What the invariant says, in the words of the specifications.
    pub(super) proof fn lemma_sums(&self)
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
    pub(super) proof fn lemma_gives(&self, i: int, pv: AccValue<'a, V>)
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
    pub(super) proof fn lemma_moved(&self, after: Self, k: int)
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
    pub(super) proof fn lemma_passed(&self, k: int)
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

    pub(super) proof fn lemma_total_impl(tracked &self)
        ensures
            self.base_pos() <= self.pos() <= self.column().len(),
            self.total() == self.acc_to(self.pos() as int),
    {
        use_type_invariant(self);
        self.lemma_sums();
    }

    pub(super) proof fn lemma_items_impl(tracked &self)
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

    pub(super) proof fn lemma_source_impl(tracked &self)
        ensures
            match self.source() {
                Some(c) => self.column() == c@,
                None => self.column().len() == 0,
            },
    {
        self.inner.lemma_source();
    }

    /// An iterator of no column.
    pub(super) fn none() -> (r: Self)
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
        let inner: Iter<'a, V, C, AccIndex> = Iter { it: ICore::empty::<V::X>(), slabs: &[], col: None };
        let zero = <V::Acc as Accum>::new_zero();
        let base = <V::Acc as Accum>::new_zero();
        proof {
            let s = inner.slabs@;
            <Get<'a, V> as VGet>::lemma_svs(inner.items());
            inner.it.lemma_out_len::<V::X>(s);
            assert(slab_lens(s) =~= Seq::<nat>::empty());
            <Get<'a, V> as VGet>::lemma_svs(vals_of::<V::X>(s));
            assert(before::<V::X>(s, 0) =~= Seq::<Get<'a, V>>::empty());
            <V::Acc as VGroup>::lemma_plus(zero, zero, zero);
        }
        AccIter { ix: None, inner, total: zero, base, base_at: 0 }
    }

    /// The iterator, taken out of its place to be moved: an accumulator
    /// need not be `Copy`, so a move takes the running sum with the
    /// rest of the iterator and writes all of it back, the fields being
    /// tied to one another. Until then an iterator of no column stands
    /// in its place.
    #[inline(always)]
    pub(super) fn take(&mut self) -> (me: Self)
        ensures
            me == *old(self),
    {
        let mut me = Self::none();
        core::mem::swap(self, &mut me);
        me
    }

    pub(super) proof fn lemma_rest_impl(tracked &self)
        ensures
            self.base_pos() <= self.pos() <= self.end_pos(),
            self.pos() <= self.column().len(),
            self.rest() == window(self.column(), self.pos() as int, self.end_pos() as int),
    {
        use_type_invariant(self);
        self.lemma_sums();
    }

    #[inline]
    pub(super) fn pos_impl(&self) -> (r: usize)
        ensures
            r == self.pos(),
    {
        proof {
            use_type_invariant(self);
            self.lemma_sums();
        }
        self.inner.pos()
    }

    #[inline]
    pub(super) fn end_pos_impl(&self) -> (r: usize)
        ensures
            r == self.end_pos(),
    {
        proof {
            use_type_invariant(self);
            self.lemma_sums();
        }
        self.inner.end_pos()
    }

    #[inline]
    pub(super) fn items_left_impl(&self) -> (r: usize)
        ensures
            r == self.end_pos() - self.pos(),
    {
        proof {
            use_type_invariant(self);
            self.lemma_sums();
        }
        self.inner.it.items_left
    }

    #[inline]
    pub(super) fn total_impl(&self) -> (r: V::Acc)
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

    #[inline]
    pub(super) fn prefix_impl(&self) -> (r: V::Acc)
        ensures
            r == self.total(),
            self.base_pos() <= self.pos() <= self.column().len(),
            self.total() == self.acc_to(self.pos() as int),
    {
        self.total()
    }

    pub(super) fn reset_prefix_impl(&mut self)
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
        *self = AccIter { ix, inner: Iter { it, slabs, col }, total: sum, base, base_at: at };
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
            // nothing is added up yet
            assert(self.column().subrange(at as int, at as int) =~= Seq::empty());
        }
    }

    pub(super) fn set_max_impl(&mut self, end: usize)
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

    pub(super) fn advance_to_impl(&mut self, target: usize)
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

    pub(super) fn advance_by_impl(&mut self, amount: usize)
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

    pub(super) fn shift_impl(&mut self, range: Range<usize>)
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

    pub(super) fn shift_next_impl(&mut self, range: Range<usize>) -> (r: Option<AccValue<'a, V>>)
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

    pub(super) fn next_run_impl(&mut self) -> (r: Option<Run<AccValue<'a, V>>>)
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
        *self = AccIter { ix, inner: Iter { it, slabs, col }, total: sum, base, base_at };
        proof {
            use_type_invariant(&*self);
            self.inner.it.lemma_out_len::<V::X>(self.inner.slabs@);
        }
        match r {
            Some(run) => {
                let pv = AccValue { value: run.value, total: self.total.dup() };
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

    pub(super) fn runs_impl(self) -> (r: Runs<'a, V, C, AccIndex>)
        ensures
            r.iter() == self.plain(),
    {
        Runs(self.inner)
    }

    pub(super) fn delta_nth_impl(&mut self, n: usize) -> (r: Option<AccSeek<'a, V>>)
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
        *self = AccIter { ix, inner: Iter { it, slabs, col }, total: sum, base, base_at };
        proof {
            use_type_invariant(&*self);
        }
        match r {
            Some((value, delta)) => {
                let pv = AccValue { value, total: self.total.dup() };
                proof {
                    let s = me.inner.slabs@;
                    let p = me.inner.it.pos as int;
                    me.lemma_moved(*self, n + 1);
                    me.lemma_gives(n as int, pv);
                    me.lemma_passed(n as int);
                    // `delta` is what the values passed over add up to
                    lemma_running::<V, C>(s, vsum::<Get<'a, V>, AccOf<V>>(before::<V::X>(s, p)), delta, p, p + n);
                }
                Some(AccSeek { pos: self.inner.it.pos - 1, delta, pv })
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

    pub(super) fn suspend_impl(&self) -> (s: AccIterState<V>)
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
        AccIterState { inner: self.inner.suspend(), base_at: self.base_at, _phantom: PhantomData }
    }
}

impl<'a, V: Accumulated + ColumnValueRef<C>, C: Codec> AccIter<'a, V, C> {
    #[inline(always)]
    pub(super) fn next_impl(&mut self) -> (r: Option<AccValue<'a, V>>)
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
            // what Verus asks of every iterator's `next`
            old(self).items().len() > 0 ==> final(self).items() == old(self).items().drop_first() && r == Some(old(self).items()[0]),
            old(self).items().len() == 0 ==> final(self).items() == old(self).items() && r is None,
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
        *self = AccIter { ix, inner: Iter { it, slabs, col }, total: sum, base, base_at };
        proof {
            use_type_invariant(&*self);
        }
        match r {
            Some(value) => {
                let pv = AccValue { value, total: self.total.dup() };
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

    #[inline]
    pub(super) fn nth_impl(&mut self, n: usize) -> (r: Option<AccValue<'a, V>>)
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
        *self = AccIter { ix, inner: Iter { it, slabs, col }, total: sum, base, base_at };
        proof {
            use_type_invariant(&*self);
        }
        match r {
            Some(value) => {
                let pv = AccValue { value, total: self.total.dup() };
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
}

impl<'a, V: Accumulated + ColumnValueRef<C>, C: Codec> AccIter<'a, V, C> {
    pub(super) fn clone_impl(&self) -> (r: Self)
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
        let r = AccIter {
            ix: self.ix,
            inner: Iter { it: self.inner.it.dup(), slabs: self.inner.slabs, col: self.inner.col },
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

impl<V: Accumulated> AccIterState<V> {
    /// The sum counts from the iterator's place or before it.
    #[verifier::type_invariant]
    pub(super) open spec fn inv(&self) -> bool {
        self.base_at <= self.inner.pos
    }

    pub(super) proof fn lemma_fits_impl<C: Codec>(&self, column: &Column<V, C, AccIndex>) where V: ColumnValueRef<C>
        requires
            self.fits(column),
        ensures
            self.edits() == column.edits(),
    {
        self.plain().lemma_fits(column);
    }

    pub(super) proof fn lemma_base_impl(tracked &self)
        ensures
            self.base_pos() <= self.pos(),
    {
        use_type_invariant(self);
    }

    pub(super) fn try_resume_impl<'a, C: Codec>(&self, column: &'a Column<V, C, AccIndex>) -> (r: Result<
        AccIter<'a, V, C>,
        PackError,
    >) where V: ColumnValueRef<C>
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
        let it = AccIter { ix, inner, total: sum, base, base_at: self.base_at };
        proof {
            it.lemma_sums();
        }
        Ok(it)
    }
}

impl<V: Accumulated + ColumnValueRef<C>, C: Codec> Column<V, C, AccIndex> {
    #[inline(always)]
    pub(super) fn prefix_iter_impl(&self) -> (it: AccIter<'_, V, C>)
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
        let zero = <V::Acc as Accum>::new_zero();
        let base = <V::Acc as Accum>::new_zero();
        proof {
            use_type_invariant(&inner);
            let s = inner.slabs@;
            assert(s == self.core.slabs_spec());
            assert(before::<V::X>(s, 0) =~= Seq::<Get<'static, V>>::empty());
            <V::Acc as VGroup>::lemma_plus(zero, zero, zero);
        }
        AccIter { ix, inner, total: zero, base, base_at: 0 }
    }

    pub(super) fn prefix_iter_range_impl(&self, range: Range<usize>) -> (it: AccIter<'_, V, C>)
        ensures
            ({
                let (start, end) = range_in(range, self@.len());
                &&& it.rest() == self@.subrange(start, end)
                &&& it.column() == self@
                &&& it.source() == Some(*self)
                &&& it.pos() == start
                &&& it.end_pos() == end
                &&& it.base_pos() == 0
                &&& it.total() == accumulated::<V>(self@.take(start))
            }),
    {
        let (start, end) = range_in_exec(range, self.len());
        self.prefix_iter_within(start, end)
    }

    /// An iterator over the values `start..end` that keeps the running
    /// sum.
    fn prefix_iter_within(&self, start: usize, end: usize) -> (it: AccIter<'_, V, C>)
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

    pub(super) fn get_prefixed_impl(&self, index: usize) -> (r: Option<AccValue<'_, V>>)
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

    pub(super) fn delta_impl(&self, from: usize, to: usize) -> (r: Option<AccSeek<'_, V>>)
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

impl<'a, V: Accumulated + ColumnValueRef<C>, C: Codec> AccIter<'a, V, C> {
    /// What the iterator has left is a window of those.
    pub(super) proof fn lemma_accumulated_items(&self)
        ensures
            self.items() == window(self.accumulated_items(), self.inner.it.pos as int, self.inner.it.pos + self.inner.it.items_left),
    {
        assert(self.items() =~= window(self.accumulated_items(), self.inner.it.pos as int, self.inner.it.pos + self.inner.it.items_left));
    }

    /// A move that keeps the column and where the sums start keeps what
    /// the iterator ranges over.
    pub(super) proof fn lemma_same_accumulated_items(&self, o: Self)
        requires
            self.column() == o.column(),
            self.base_pos() == o.base_pos(),
        ensures
            self.accumulated_items() == o.accumulated_items(),
    {
        lemma_svs_inj(vals_of::<V::X>(self.inner.slabs@), vals_of::<V::X>(o.inner.slabs@));
        assert(self.accumulated_items() =~= o.accumulated_items());
    }
}

impl<'a, V: Accumulated + ColumnValueRef<C>, C: Codec> AccIter<'a, V, C> {
    pub(super) proof fn lemma_shift_impl(&self)
        ensures
            0 <= self.shift_at(),
            self.shift_left() == window(self.shift_vals(), self.shift_at(), self.shift_end()),
            self.shift_left() == concat(self.shift_items().map_values(|x: <Self as Iterator>::Item| Self::shift_spans(x))),
            forall|k: int|
                0 <= k < self.shift_items().len() ==> Self::shift_spans(#[trigger] self.shift_items()[k]).len() > 0,
    {
        let xs = self.items();
        lemma_singles(xs);
        assert(xs.map_values(|x: AccValue<'a, V>| Self::shift_spans(x)) =~= xs.map_values(
            |x: AccValue<'a, V>| seq![x],
        ));
    }

    #[inline(always)]
    pub(super) fn shift_step_impl(&mut self) -> (r: Option<AccValue<'a, V>>)
        ensures
            final(self).shift_vals() == old(self).shift_vals(),
            match r {
                Some(x) => {
                    &&& old(self).shift_items().len() > 0
                    &&& x == old(self).shift_items()[0]
                    &&& final(self).shift_items() == old(self).shift_items().skip(1)
                    &&& final(self).shift_at() == old(self).shift_at() + Self::shift_spans(x).len()
                },
                None => old(self).shift_items().len() == 0 && final(self).shift_items().len() == 0,
            },
    {
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
        }
        let r = self.nth(0);
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
            self.lemma_same_accumulated_items(*old(self));
        }
        r
    }

    #[inline(always)]
    pub(super) fn shift_nth_impl(&mut self, n: usize) -> (r: Option<AccValue<'a, V>>)
        ensures
            final(self).shift_vals() == old(self).shift_vals(),
            n < old(self).shift_items().len() ==> {
                &&& r == Some(old(self).shift_items()[n as int])
                &&& final(self).shift_items() == old(self).shift_items().skip(n + 1)
                &&& final(self).shift_at() == old(self).shift_at() + concat(
                    old(self).shift_items().take(n + 1).map_values(|x: <Self as Iterator>::Item| Self::shift_spans(x)),
                ).len()
            },
            n >= old(self).shift_items().len() ==> r is None && final(self).shift_items().len() == 0,
    {
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
        }
        let r = self.nth(n);
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
            self.lemma_same_accumulated_items(*old(self));
            if n < old(self).items().len() {
                let xs = old(self).items().take(n + 1);
                lemma_singles(xs);
                assert(xs.map_values(|x: AccValue<'a, V>| Self::shift_spans(x)) =~= xs.map_values(
                    |x: AccValue<'a, V>| seq![x],
                ));
            }
        }
        r
    }

    #[inline(always)]
    pub(super) fn dup_impl(&self) -> (r: Self)
        ensures
            r.shift_vals() == self.shift_vals(),
            r.shift_at() == self.shift_at(),
            r.shift_end() == self.shift_end(),
            r.shift_left() == self.shift_left(),
            r.shift_items() == self.shift_items(),
    {
        proof {
            use_type_invariant(self);
            use_type_invariant(&self.inner);
        }
        let inner = Iter { it: self.inner.it.dup(), slabs: self.inner.slabs, col: self.inner.col };
        AccIter { ix: self.ix, inner, total: self.total.dup(), base: self.base.dup(), base_at: self.base_at }
    }

    #[inline(always)]
    pub(super) fn get_pos_impl(&self) -> (r: usize)
        ensures
            r == self.shift_at(),
    {
        proof {
            use_type_invariant(self);
            self.lemma_sums();
        }
        self.pos()
    }

    #[inline(always)]
    pub(super) fn get_max_impl(&self) -> (r: usize)
        ensures
            r == self.shift_end(),
    {
        proof {
            use_type_invariant(self);
            self.lemma_sums();
        }
        self.end_pos()
    }

    #[inline(always)]
    pub(super) fn set_max_v_shiftable_impl(&mut self, end: usize)
        ensures
            final(self).shift_vals() == old(self).shift_vals(),
            final(self).shift_at() == old(self).shift_at(),
            final(self).shift_end() == if end >= old(self).shift_at() {
                end as int
            } else {
                old(self).shift_at()
            },
    {
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
        }
        AccIter::set_max(self, end);
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
            self.lemma_same_accumulated_items(*old(self));
        }
    }

    #[inline(always)]
    pub(super) fn advance_to_v_shiftable_impl(&mut self, target: usize)
        ensures
            final(self).shift_vals() == old(self).shift_vals(),
            final(self).shift_left() == past(old(self).shift_left(), target - old(self).shift_at()),
            target <= old(self).shift_at() ==> final(self).shift_at() == old(self).shift_at(),
            old(self).shift_at() < target <= old(self).shift_at() + old(self).shift_left().len()
                ==> final(self).shift_at() == target,
    {
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
            self.lemma_accumulated_items();
        }
        AccIter::advance_to(self, target);
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
            self.lemma_accumulated_items();
            self.lemma_same_accumulated_items(*old(self));
        }
    }

    #[inline(always)]
    pub(super) fn advance_by_v_shiftable_impl(&mut self, amount: usize)
        ensures
            final(self).shift_vals() == old(self).shift_vals(),
            final(self).shift_left() == past(old(self).shift_left(), amount as int),
            amount <= old(self).shift_left().len() ==> final(self).shift_at() == old(self).shift_at() + amount,
    {
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
            self.lemma_accumulated_items();
        }
        AccIter::advance_by(self, amount);
        proof {
            use_type_invariant(&*self);
            self.lemma_sums();
            self.lemma_accumulated_items();
            self.lemma_same_accumulated_items(*old(self));
        }
    }

    #[inline(always)]
    pub(super) fn seek_to_impl(&mut self, at: usize) -> (r: Option<AccValue<'a, V>>)
        ensures
            final(self).shift_vals() == old(self).shift_vals(),
            at < old(self).shift_at() ==> {
                &&& r is None
                &&& final(self).shift_left() == old(self).shift_left()
                &&& final(self).shift_at() == old(self).shift_at()
            },
            at >= old(self).shift_at() ==> {
                let (o, k) = (old(self).shift_left(), at - old(self).shift_at());
                &&& k < o.len() ==> (r matches Some(x) && {
                    let n = Self::shift_spans(x).len() as int;
                    &&& 0 < n <= o.len() - k
                    &&& Self::shift_spans(x) == o.skip(k).take(n)
                    &&& final(self).shift_left() == o.skip(k + n)
                    &&& final(self).shift_at() == at + n
                })
                &&& k < o.len() && Self::shift_unit() ==> {
                    &&& r == Some(o[k])
                    &&& final(self).shift_left() == o.skip(k + 1)
                    &&& final(self).shift_at() == at + 1
                }
                &&& k >= o.len() ==> r is None && final(self).shift_left().len() == 0
            },
    {
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
                self.lemma_same_accumulated_items(*old(self));
            }
            r
        } else {
            None
        }
    }

    #[inline(always)]
    pub(super) fn shift_v_shiftable_impl(&mut self, range: Range<usize>)
        ensures
            final(self).shift_vals() == old(self).shift_vals(),
            final(self).shift_left() == window(
                old(self).shift_vals(),
                if range.start <= old(self).shift_at() {
                    old(self).shift_at()
                } else {
                    range.start as int
                },
                range.end as int,
            ),
            range.start <= old(self).shift_at() ==> final(self).shift_at() == old(self).shift_at(),
            old(self).shift_at() < range.start <= range.end && range.start <= old(self).shift_vals().len()
                ==> final(self).shift_at() == range.start,
    {
        let ghost p = self.shift_at();
        proof {
            self.lemma_shift();
        }
        // the window's end, and then its start, as the iterator's own
        // `shift` moves
        Shiftable::set_max(self, range.end);
        let ghost p1 = self.shift_at();
        proof {
            self.lemma_shift();
        }
        Shiftable::advance_to(self, range.start);
        proof {
            self.lemma_shift();
            assert(p1 == p);
        }
    }

    #[inline(always)]
    pub(super) fn shift_next_v_shiftable_impl(&mut self, range: Range<usize>) -> (r: Option<AccValue<'a, V>>)
        requires
            range.start >= old(self).shift_at(),
        ensures
            final(self).shift_vals() == old(self).shift_vals(),
            ({
                let w = window(old(self).shift_vals(), range.start as int, range.end as int);
                &&& w.len() > 0 ==> (r matches Some(x) && {
                    let n = Self::shift_spans(x).len() as int;
                    &&& 0 < n <= w.len()
                    &&& Self::shift_spans(x) == w.take(n)
                    &&& final(self).shift_left() == w.skip(n)
                    &&& final(self).shift_at() == range.start + n
                })
                &&& w.len() == 0 ==> r is None && final(self).shift_left().len() == 0
            }),
    {
        let ghost (s, e) = (range.start as int, range.end as int);
        let ghost (v, p) = (self.accumulated_items(), self.shift_at());
        Shiftable::set_max(self, range.end);
        proof {
            self.lemma_shift();
        }
        let q = Shiftable::get_pos(self);
        let ghost o = self.shift_items();
        let r = Shiftable::shift_nth(self, range.start - q);
        proof {
            let w = window(v, s, e);
            let k = s - p;
            if w.len() > 0 {
                assert(o[k] == w[0]);
                let xs = o.take(k + 1);
                lemma_singles(xs);
                assert(xs.map_values(|x: AccValue<'a, V>| Self::shift_spans(x)) =~= xs.map_values(
                    |x: AccValue<'a, V>| seq![x],
                ));
            }
        }
        r
    }
}

impl<'a, V: Accumulated + ColumnValueRef<C>, C: Codec> AccIter<'a, V, C> {
    pub(super) proof fn lemma_shift_model_impl(&self)
        ensures
            self.shift_vals() == self.accumulated_items(),
            self.accumulated_items().len() == self.column().len(),
            forall|p: int| 0 <= p < self.column().len() ==> {
                &&& (#[trigger] self.accumulated_items()[p]).value.sv() == self.column()[p]
                &&& self.accumulated_items()[p].total == self.acc_to(p + 1)
            },
            self.shift_at() == self.pos(),
            self.shift_items() == self.items(),
            self.shift_left() == self.items(),
            Self::shift_unit(),
    {
        <Get<'a, V> as VGet>::lemma_svs(vals_of::<V::X>(self.inner.slabs@));
    }

    #[inline(always)]
    pub(super) fn unshift_impl(self) -> (r: Unshift<Self>)
        ensures
            r.unshift_vals() == self.accumulated_items(),
            r.unshift_left() == self.items(),
    {
        Unshift::new(self)
    }

    #[inline(always)]
    pub(super) fn ranges_impl<'r>(self, ranges: &'r [Range<usize>]) -> (r: Ranges<'r, Self>)
        ensures
            r.ranges_vals() == self.accumulated_items(),
            r.ranges_left() == windows(self.accumulated_items(), ranges@),
    {
        Ranges::new(self, ranges)
    }
}

} // verus!

impl<'a, V: Accumulated> std::fmt::Debug for AccValue<'a, V>
where
    Get<'a, V>: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccValue")
            .field("value", &self.value)
            .field("total", &self.total)
            .finish()
    }
}

impl<'a, V: Accumulated> PartialEq for AccValue<'a, V>
where
    Get<'a, V>: PartialEq,
    V::Acc: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.total == other.total
    }
}

impl<'a, V: Accumulated> std::fmt::Debug for AccSeek<'a, V>
where
    Get<'a, V>: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccSeek")
            .field("pos", &self.pos)
            .field("delta", &self.delta)
            .field("pv", &self.pv)
            .finish()
    }
}

impl<'a, V: Accumulated> PartialEq for AccSeek<'a, V>
where
    Get<'a, V>: PartialEq,
    V::Acc: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.pos == other.pos && self.delta == other.delta && self.pv == other.pv
    }
}

impl<'a, V: Accumulated + ColumnValueRef<C>, C: Codec> std::fmt::Debug for AccIter<'a, V, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccIter")
            .field("total", &self.total)
            .field("pos", &self.inner.pos())
            .finish()
    }
}
