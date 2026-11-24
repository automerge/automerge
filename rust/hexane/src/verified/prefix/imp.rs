//! The implementation of `verified::prefix` and its proofs: what its
//! specification (`prefix.rs`) says. Each function named `..._impl` here has
//! the contract of the function there that calls it, word for word; a
//! function named after a trait, a method and a type (`vpack_pack_u64`)
//! is the body of that method in that type's impl of the trait, with the
//! trait's contract said of the type.

#![allow(unused_imports)]

#[cfg(verus_keep_ghost)]
use super::super::lemma_svs_sub;
#[cfg(verus_keep_ghost)]
use super::super::sums::{
    lemma_nsum, lemma_sum_bound, lemma_sum_diff, lemma_sum_range, lemma_sums_fit_take,
    lemma_svs_slabs, lemma_vsum,
};
use super::*;

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

impl<V: PrefixValue + ColumnValueRef<C>, C: Codec> Kind<V, C> for SumIndex {
    type W = SumWeight<V::Sum>;

    type I = SlabBTree<SumWeight<V::Sum>>;

    type G = SumModel<V, C>;

    proof fn lemma_refuses(vs: Seq<Held<V, C>>) {
    }

    proof fn lemma_kept(s: Seq<Slab<<V::E as Slabs>::Tail>>) {
        lemma_svs_slabs::<V, C>(s);
    }
}

impl<V: PrefixValue + ColumnValueRef<C>, C: Codec> Column<V, C, SumIndex> {
    /// The index as the readers of the sums take it, with what they ask
    /// of it; none for a column an edit's panic left without its parts,
    /// which is empty.
    #[inline(always)]
    pub(in crate::verified) fn sum_index(&self) -> (r: Option<&SlabBTree<SumWeight<V::Sum>>>)
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

    pub(super) fn get_prefix_impl(&self, index: usize) -> (r: V::Sum)
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
    pub(super) proof fn lemma_before(&self, index: usize)
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

    pub(super) fn get_total_impl(&self, index: usize) -> (r: V::Sum)
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

    pub(super) fn sum_range_impl(&self, start: usize, end: usize) -> (r: V::Sum)
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

impl<V: PrefixValue + ColumnValueRef<C>, C: Codec> Column<V, C, SumIndex> where V::Sum: UnsignedPrefix {
    pub(super) fn get_index_for_prefix_impl(&self, target: V::Sum) -> (r: usize)
        ensures
            r <= self@.len() + 1,
            forall|i: int| 0 <= i < r ==> summed::<V>(#[trigger] self@.take(i)) < target.as_int(),
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

    pub(super) fn get_index_for_total_impl(&self, target: V::Sum) -> (r: usize)
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
