//! Prefix columns of the verified public API: columns kept with the
//! sums of their values, which answer what the values before a position
//! add up to without reading them all.
//!
//! A prefix column is a [`VerifiedColumn`] kept with a [`SumIndex`]:
//! of each slab the index holds, besides the count of its values, what
//! they add up to (`summed`). What a column of the kind says of its
//! values, and its bytes when saved, are a plain column's. What
//! differs is that there is such a column only of values whose sums
//! fit the type they are kept in (`sums_fit`): a load refuses a saved
//! column of other values, and an edit that would leave other values
//! panics.
//!
//! The sums a prefix column answers with are of the type it keeps them
//! in, and what is proved of them is said of integers: the sum before
//! a position ([`get_prefix`](VerifiedColumn::get_prefix)) *is* what
//! the values before it add up to, for the sums of a prefix column fit.

use super::kind::{Kind, VIndex, VKeeps};
#[cfg(verus_keep_ghost)]
use super::sums::{
    lemma_nsum, lemma_sum_bound, lemma_sum_diff, lemma_sum_range, lemma_sums_fit_take,
    lemma_svs_slabs, lemma_vsum, summed, sums_fit,
};
use super::sums::{SumModel, SumOf, SumWeight, VCount, VSum, VSummed};
#[cfg(verus_keep_ghost)]
use super::{lemma_svs_sub, Get, Held, VGet};
use super::{VCodec, VValue, VerifiedColumn};
use crate::btree::SlabBTree;
#[cfg(verus_keep_ghost)]
use crate::btree::{PrefixAggregate, SlabAggregate};
#[cfg(verus_keep_ghost)]
use crate::column::Slab;
use crate::column_core::too_many_slabs;
#[cfg(verus_keep_ghost)]
use crate::cursor::{lemma_lens_ok, slab_lens, Slabs, Weigh};
#[cfg(verus_keep_ghost)]
use crate::fenwick::total;
#[cfg(verus_keep_ghost)]
use crate::index::{lemma_btree_index, IndexOps};
#[cfg(verus_keep_ghost)]
use crate::iter_core::{lens_ok, vals_of, Decoders};
#[cfg(verus_keep_ghost)]
use crate::prefix_core::{before, ix_ok, vsum, PrefixWeight};
use crate::prefix_core::{p_prefix, p_sum_range};
use crate::prefix_search::p_index_for_prefix;
#[cfg(verus_keep_ghost)]
use crate::prefix_search::{nsum, PrefixOrd};
use crate::Leb128;
use vstd::prelude::*;

verus! {

/// The index of a prefix column: of each slab it counts the values and
/// holds what they add up to.
pub struct SumIndex;

impl<V: VSummed + VValue<C>, C: VCodec> VKeeps<V, C> for SumIndex {
    /// The columns whose sums fit the type they are kept in.
    open spec fn keeps(vs: Seq<<V as super::VSpec>::S>) -> bool {
        sums_fit::<V>(vs)
    }
}

impl<V: VSummed + VValue<C>, C: VCodec> Kind<V, C> for SumIndex {
    type W = SumWeight<V::Sum>;

    type G = SumModel<V, C>;

    proof fn lemma_refuses(vs: Seq<Held<V, C>>) {
    }

    proof fn lemma_kept(s: Seq<Slab<<V::E as Slabs>::Tail>>) {
        lemma_svs_slabs::<V, C>(s);
    }
}

impl<V: VSummed + VValue<C>, C: VCodec> VIndex<V, C> for SumIndex {

}

/// A column kept with the sums of its values: a [`VerifiedColumn`]
/// with a [`SumIndex`].
pub type VerifiedPrefixColumn<V, C = Leb128> = VerifiedColumn<V, C, SumIndex>;

/// What a prefix column answers from its sums.
impl<V: VSummed + VValue<C>, C: VCodec> VerifiedColumn<V, C, SumIndex> {
    /// The index as the readers of the sums take it, with what they ask
    /// of it; none for a column an edit's panic left without its parts,
    /// which is empty.
    #[inline(always)]
    pub(super) fn sum_index(&self) -> (r: Option<&SlabBTree<SumWeight<V::Sum>>>)
        ensures
            sums_fit::<V>(self@),
            <Get<'static, V> as VGet>::svs(vals_of::<V::X>(self.core.slabs_spec())) == self@,
            vals_of::<V::X>(self.core.slabs_spec()).len() == self@.len(),
            self.core@.len() == self@.len(),
            self@.len() <= usize::MAX,
            match r {
                Some(ix) => {
                    &&& ix.wf()
                    &&& ix_ok::<V::X, SumOf<V>>(ix@, self.core.slabs_spec())
                    &&& lens_ok::<V::X>(self.core.slabs_spec())
                    &&& total(slab_lens(self.core.slabs_spec())) <= usize::MAX
                    &&& self.core.slabs_spec().len() < usize::MAX
                },
                None => self@.len() == 0 && self.core.slabs_spec().len() == 0,
            },
    {
        self.core.lemma_column();
        self.lemma_read();
        let r = self.core.index_opt();
        if self.core.slabs().len() == usize::MAX {
            too_many_slabs();
        }
        proof {
            let s = self.core.slabs_spec();
            <Get<'static, V> as VGet>::lemma_svs(vals_of::<V::X>(s));
            match r {
                Some(ix) => {
                    lemma_lens_ok::<V::E, V::X>(s);
                    lemma_btree_index(*ix);
                    assert forall|i: int| 0 <= i < s.len() implies {
                        &&& #[trigger] ix@[i].spec_len() == s[i].len
                        &&& ix@[i].spec_prefix() == vsum::<Get<'static, V>, SumOf<V>>(
                            <V::X as Decoders<'static>>::vals(s[i]),
                        )
                    } by {
                        assert(<SumModel<V, C> as Weigh<V::E, SumWeight<V::Sum>>>::wrel(ix.ws()[i], s[i]));
                    }
                    lemma_svs_slabs::<V, C>(s);
                },
                None => {
                    <SumModel<V, C> as Weigh<V::E, SumWeight<V::Sum>>>::lemma_dom_empty(s);
                },
            }
        }
        r
    }

    /// What the values before position `index` add up to; where `index`
    /// is past the end, what all the values do.
    ///
    /// The index gives the sum of the slabs before the one that holds
    /// the position, and of that slab the runs before it are read: no
    /// value of another slab is.
    pub fn get_prefix(&self, index: usize) -> (r: V::Sum)
        ensures
            index <= self@.len() ==> r.as_int() == summed::<V>(self@.take(index as int)),
            index >= self@.len() ==> r.as_int() == summed::<V>(self@),
    {
        let ghost k = if index <= self@.len() {
            index as int
        } else {
            self@.len() as int
        };
        proof {
            assert(self@.take(self@.len() as int) =~= self@);
        }
        match self.sum_index() {
            Some(ix) => {
                let r = p_prefix::<V::X, SumOf<V>>(self.core.slabs(), ix, index);
                proof {
                    self.lemma_before(index);
                    lemma_sums_fit_take::<V>(self@, k);
                    V::Sum::lemma_of_int(summed::<V>(self@.take(k)), 0);
                }
                r
            },
            None => {
                let r = V::Sum::zero();
                proof {
                    assert(self@.take(k) =~= self@);
                    V::Sum::lemma_as_int(r);
                    V::Sum::lemma_of_int(0, 0);
                }
                r
            },
        }
    }

    /// The sum the shared proofs give for the values before a position
    /// is what the first `index` of the column's values add up to (all
    /// of them, past the end), wrapped.
    proof fn lemma_before(&self, index: usize)
        requires
            <Get<'static, V> as VGet>::svs(vals_of::<V::X>(self.core.slabs_spec())) == self@,
            vals_of::<V::X>(self.core.slabs_spec()).len() == self@.len(),
        ensures
            vsum::<Get<'static, V>, SumOf<V>>(before::<V::X>(self.core.slabs_spec(), index as int)) == V::Sum::of_int(
                summed::<V>(
                    self@.take(
                        if index <= self@.len() {
                            index as int
                        } else {
                            self@.len() as int
                        },
                    ),
                ),
            ),
    {
        let k = if index <= self@.len() {
            index as int
        } else {
            self@.len() as int
        };
        let o = vals_of::<V::X>(self.core.slabs_spec());
        let b = before::<V::X>(self.core.slabs_spec(), index as int);
        assert(b =~= o.subrange(0, k));
        lemma_svs_sub(o, 0, k);
        assert(self@.subrange(0, k) =~= self@.take(k));
        lemma_vsum::<V>(b);
    }

    /// What the values through position `index` add up to, the one at
    /// `index` among them; where `index` is at or past the end, what
    /// all the values do. It is `get_prefix(index + 1)`.
    pub fn get_total(&self, index: usize) -> (r: V::Sum)
        ensures
            index < self@.len() ==> r.as_int() == summed::<V>(self@.take(index + 1)),
            index >= self@.len() ==> r.as_int() == summed::<V>(self@),
    {
        // a column is shorter than `usize::MAX`: where the addition
        // saturates, `index` is past the end
        self.core.lemma_column();
        self.lemma_read();
        self.get_prefix(index.saturating_add(1))
    }

    /// What the values at the positions from `start` up to, and not
    /// including, `end` add up to: of those of them that the column
    /// has, and nothing where there are none, as where `end` is not
    /// after `start`.
    ///
    /// It is the sum before `end` less the sum before `start`, found in
    /// one pass.
    pub fn sum_range(&self, start: usize, end: usize) -> (r: V::Sum)
        ensures
            start <= end <= self@.len() ==> r.as_int() == summed::<V>(self@.subrange(start as int, end as int)),
            start <= self@.len() <= end ==> r.as_int() == summed::<V>(
                self@.subrange(start as int, self@.len() as int),
            ),
            start >= end || start >= self@.len() ==> r.as_int() == 0,
    {
        let ghost n = self@.len() as int;
        let ix = match self.sum_index() {
            Some(ix) => ix,
            None => {
                proof {
                    V::Sum::lemma_as_int(V::Sum::of_int(0));
                    V::Sum::lemma_of_int(0, 0);
                    assert(self@.subrange(0, 0).len() == 0);
                }
                return V::Sum::zero();
            },
        };
        if start >= end {
            proof {
                V::Sum::lemma_as_int(V::Sum::of_int(0));
                V::Sum::lemma_of_int(0, 0);
                if start <= n {
                    assert(self@.subrange(start as int, start as int).len() == 0);
                    assert(self@.subrange(start as int, n).len() == 0 || start < n);
                }
            }
            return V::Sum::zero();
        }
        let len = self.core.len();
        let slabs = self.core.slabs();
        let mut core = self.reader(0, len);
        let mut total = V::Sum::zero();
        let mut base = V::Sum::zero();
        proof {
            assert(before::<V::X>(self.core.slabs_spec(), 0) =~= Seq::<Get<'static, V>>::empty());
            <SumWeight<V::Sum> as PrefixWeight>::lemma_prefix_add(base, base, base);
        }
        p_sum_range::<V::X, SumOf<V>>(&mut core, slabs, Some(ix), start, end, &mut total, &mut base);
        proof {
            let s0 = if start <= n {
                start as int
            } else {
                n
            };
            let e0 = if end <= n {
                end as int
            } else {
                n
            };
            self.lemma_before(start);
            self.lemma_before(end);
            lemma_sums_fit_take::<V>(self@, s0);
            lemma_sums_fit_take::<V>(self@, e0);
            lemma_sum_range::<V>(self@, s0, e0);
            lemma_sum_diff::<V::Sum>(base, total, summed::<V>(self@.take(s0)), summed::<V>(self@.take(e0)));
            if start >= n {
                assert(self@.subrange(n, n).len() == 0);
            }
        }
        total
    }
}

/// What a prefix column of sums that are never negative answers: where
/// its sums reach a target.
impl<V: VSummed + VValue<C>, C: VCodec> VerifiedColumn<V, C, SumIndex> where V::Sum: VCount {
    /// The first position that the values before it add up to `target`
    /// or more: no earlier position's do, and where the result is a
    /// position of the column or its end, that one's do. Where the
    /// values of the whole column add up to less, it is one past the
    /// column's end, `len() + 1`.
    ///
    /// The index finds the slab where the sums reach the target, and of
    /// that slab the runs are read: no value of another slab is.
    pub fn get_index_for_prefix(&self, target: V::Sum) -> (r: usize)
        ensures
            r <= self@.len() + 1,
            forall|i: int| 0 <= i < r && i <= self@.len() ==> summed::<V>(#[trigger] self@.take(i)) < target.as_int(),
            r <= self@.len() ==> target.as_int() <= summed::<V>(self@.take(r as int)),
    {
        match self.sum_index() {
            Some(ix) => {
                let r = p_index_for_prefix::<V::X, SumOf<V>>(self.core.slabs(), ix, target);
                proof {
                    let o = vals_of::<V::X>(self.core.slabs_spec());
                    V::Sum::lemma_count();
                    V::Sum::lemma_as_int(target);
                    // the sum of the column fits: the search's answer is exact
                    lemma_nsum::<V>(o);
                    lemma_sums_fit_take::<V>(self@, self@.len() as int);
                    assert(self@.take(self@.len() as int) =~= self@);
                    assert(nsum::<Get<'static, V>, SumOf<V>>(o) <= <SumOf<V> as PrefixOrd<Get<'static, V>>>::pmax());
                    assert forall|i: int| 0 <= i <= self@.len() implies nsum::<Get<'static, V>, SumOf<V>>(
                        #[trigger] o.take(i),
                    ) == summed::<V>(self@.take(i)) by {
                        lemma_nsum::<V>(o.take(i));
                        lemma_svs_sub(o, 0, i);
                        assert(o.take(i) =~= o.subrange(0, i));
                        assert(self@.take(i) =~= self@.subrange(0, i));
                    }
                    assert forall|i: int| 0 <= i < r && i <= self@.len() implies summed::<V>(#[trigger] self@.take(i))
                        < target.as_int() by {
                        assert(nsum::<Get<'static, V>, SumOf<V>>(o.take(i)) == summed::<V>(self@.take(i)));
                    }
                    if r <= self@.len() {
                        assert(nsum::<Get<'static, V>, SumOf<V>>(o.take(r as int)) == summed::<V>(
                            self@.take(r as int),
                        ));
                    }
                }
                r
            },
            None => {
                // an empty column: its end, or one past it
                proof {
                    V::Sum::lemma_count();
                    V::Sum::lemma_as_int(target);
                    V::Sum::lemma_as_int(V::Sum::of_int(0));
                    V::Sum::lemma_of_int(0, 0);
                    assert(self@.take(0).len() == 0);
                }
                if target.le(V::Sum::zero()) {
                    0
                } else {
                    1
                }
            },
        }
    }

    /// The first position that the values through it add up to
    /// `target` or more: no earlier position's do. Where the values of
    /// the whole column add up to less, it is the column's end,
    /// `len()`. It is `get_index_for_prefix(target)` less one, and zero
    /// for a target of zero.
    pub fn get_index_for_total(&self, target: V::Sum) -> (r: usize)
        ensures
            r <= self@.len(),
            forall|j: int| 0 < j <= r ==> summed::<V>(#[trigger] self@.take(j)) < target.as_int(),
            r < self@.len() ==> target.as_int() <= summed::<V>(self@.take(r + 1)),
    {
        let p = self.get_index_for_prefix(target);
        proof {
            if p == 0 {
                // a target of zero, which the sum through any value reaches
                assert(self@.take(0).len() == 0);
                if 0 < self@.len() {
                    lemma_sum_bound::<V>(self@.take(1));
                    V::Sum::lemma_count();
                }
            }
        }
        p.saturating_sub(1)
    }
}

} // verus!
