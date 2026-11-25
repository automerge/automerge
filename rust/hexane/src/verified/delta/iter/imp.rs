//! The implementation of `verified::delta::iter` and its proofs: what its
//! specification (`iter.rs`) says. Each function named `..._impl` here has
//! the contract of the function there that calls it, word for word; a
//! function named after a trait, a method and a type (`vpack_pack_u64`)
//! is the body of that method in that type's impl of the trait, with the
//! trait's contract said of the type.

#![allow(unused_imports)]

#[cfg(verus_keep_ghost)]
use super::super::diffs::{
    delta_units_of_runs, lemma_delta_runs_units, lemma_delta_take, lemma_deltas_kept,
    lemma_vals_fit,
};
#[cfg(verus_keep_ghost)]
use super::super::{all_dom, delta_at, in_dom, lemma_bound, lemma_delta_run_at};
use super::*;
use crate::verified::imp::{bounds_exec, bounds_in_exec, range_in_exec};

#[cfg(verus_keep_ghost)]
use crate::btree::in_i64;
use crate::btree::{SlabAgg, SlabBTree};
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::slab_lens;
use crate::cursor::Slabs;
#[cfg(verus_keep_ghost)]
use crate::delta::agg::psum;
use crate::delta::agg::DeltaOps;
#[cfg(verus_keep_ghost)]
use crate::delta::ddomain::{dslabs, lemma_dslabs_own};
#[cfg(verus_keep_ghost)]
use crate::delta::dedit::rz;
#[cfg(verus_keep_ghost)]
use crate::delta::dfind::col_hit;
#[cfg(verus_keep_ghost)]
use crate::delta::diter::{at_run, dbefore, dindex_ok, dval_at, lemma_crop, lemma_realized_window};
use crate::delta::diter::{d_next, d_run, d_step};
#[cfg(verus_keep_ghost)]
use crate::delta::dscan::lemma_dval_rv;
use crate::delta::dscan::{d_scan, d_scan_null};
#[cfg(verus_keep_ghost)]
use crate::delta::dseek::{all_fit, below as dbelow, dfound, dsorted, lemma_search_start, rv};
use crate::delta::dseek::{d_seek, d_seek_null};
#[cfg(verus_keep_ghost)]
use crate::fenwick::total;
#[cfg(verus_keep_ghost)]
use crate::iter_core::{lemma_window_past, past, resumes, vals_of, window, Decode};
use crate::iter_core::{Decoders, ICore};
#[cfg(verus_keep_ghost)]
use crate::rle::writer::concat;
use crate::verified::codec::Codec;
#[cfg(verus_keep_ghost)]
use crate::verified::shift::{lemma_run_units, lemma_singles};
#[cfg(verus_keep_ghost)]
use crate::verified::{head_run, lemma_runs_of, runs_of, sorted};
use crate::verified::{invalid_resume, IterState};
use crate::verified::{Ranges, Shiftable, Unshift};
#[cfg(verus_keep_ghost)]
use crate::window::windows;
use crate::{DeltaRun, Leb128, PackError};
use std::ops::{Range, RangeBounds};
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use vstd::std_specs::range::RangeBoundsSpec;

verus! {

/// How the values of the column underneath are differences.
pub(super) type DB<T, C> = <T as DKind<C>>::B;

/// What a column of slabs `s` keeps at `i` is what a reader of its
/// differences gives there: the sum of those up to it.
pub(super) proof fn lemma_kept_at<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(s: Seq<Slab<X::Tail>>, i: int)
    requires
        0 <= i < vals_of::<X>(s).len(),
    ensures
        rz(0, dslabs::<X, B>(s)).len() == vals_of::<X>(s).len(),
        rz(0, dslabs::<X, B>(s))[i] == dval_at::<X, B>(s, i),
{
    let ds = dslabs::<X, B>(s);
    assert(ds.take(i) =~= dbefore::<X, B>(s, i));
}

/// Whether what a column keeps at a place, `d`, is a number from `lo`
/// to `hi`.
pub(in crate::verified::delta) open spec fn kept_in(d: Option<i64>, lo: i64, hi: i64) -> bool {
    match d {
        Some(n) => lo <= n <= hi,
        None => false,
    }
}

/// What a column of slabs `s` keeps at `i` is what the searches of its
/// differences say is there (`rv`): the same number, or a null.
pub(super) proof fn lemma_kept_rv<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(s: Seq<Slab<X::Tail>>, i: int)
    requires
        0 <= i < vals_of::<X>(s).len(),
        all_fit::<X, B>(s),
    ensures
        rz(0, dslabs::<X, B>(s)).len() == vals_of::<X>(s).len(),
        match rv::<X, B>(s, i) {
            Some(n) => in_i64(n) && rz(0, dslabs::<X, B>(s))[i] == Some(n as i64),
            None => rz(0, dslabs::<X, B>(s))[i] is None,
        },
        (B::dv(vals_of::<X>(s)[i]) is Some) == (rz(0, dslabs::<X, B>(s))[i] is Some),
{
    lemma_kept_at::<X, B>(s, i);
    lemma_dval_rv::<X, B>(s, i);
}

/// What a scan of the differences takes for a number from `lo` to `hi`
/// at `i` (`col_hit`) is what the column keeps as one there.
pub(in crate::verified::delta) proof fn lemma_kept_hit<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    s: Seq<Slab<X::Tail>>,
    lo: i64,
    hi: i64,
    i: int,
)
    requires
        0 <= i < vals_of::<X>(s).len(),
        all_fit::<X, B>(s),
    ensures
        col_hit::<X, B>(s, lo as int, hi as int, i) == kept_in(rz(0, dslabs::<X, B>(s))[i], lo, hi),
{
    lemma_kept_rv::<X, B>(s, i);
}

impl<'a, T: DeltaValue<C>, C: Codec> DeltaIter<'a, T, C> {
    /// The iterator stands in the slabs of its column, in the column or
    /// at its end, with the sum of the differences before it; and
    /// every value the column keeps is one of the type's.
    #[verifier::type_invariant]
    pub(super) open spec fn inv(&self) -> bool {
        &&& at_run::<DX<T, C>, DB<T, C>>(self.it, self.running, self.slabs@)
        &&& dindex_ok::<DX<T, C>, DB<T, C>>(self.ix, self.slabs@)
        &&& self.it.pos <= total(slab_lens(self.slabs@))
        &&& all_dom::<T>(self.kept())
        &&& match self.col {
            Some(c) => self.slabs@ == c.slabs_spec(),
            None => self.slabs@.len() == 0,
        }
    }

    /// What the column the iterator reads keeps: each value as an
    /// `i64`, or none for a null.
    pub(super) open spec fn kept(&self) -> Seq<Option<i64>> {
        rz(0, dslabs::<DX<T, C>, DB<T, C>>(self.slabs@))
    }

    pub(super) proof fn lemma_source_impl(tracked &self)
        ensures
            match self.source() {
                Some(c) => self.column() == c@,
                None => self.column().len() == 0,
            },
    {
        use_type_invariant(self);
        match self.col {
            Some(c) => {
                c.lemma_view();
                lemma_dslabs_own::<DE<T, C>, DX<T, C>, DB<T, C>>(c.slabs_spec());
            },
            None => {
                assert(vals_of::<DX<T, C>>(self.slabs@) =~= Seq::empty());
            },
        }
    }

    /// Where the iterator stands and where its window ends, as its
    /// reader holds them; how long the column is; and that the reader
    /// has as many values left as the window holds.
    pub(super) proof fn lemma_window(&self)
        requires
            self.it.wf::<DX<T, C>>(self.slabs@),
        ensures
            self.spec_pos() == self.it.pos,
            self.spec_end_pos() == self.it.pos + self.it.items_left,
            vals_of::<DX<T, C>>(self.slabs@).len() == total(slab_lens(self.slabs@)),
            self.kept().len() == total(slab_lens(self.slabs@)),
            self.column().len() == total(slab_lens(self.slabs@)),
            self.it.pos <= total(slab_lens(self.slabs@)) ==> {
                &&& self.rest().len() == self.it.out::<DX<T, C>>(self.slabs@).len()
                &&& self.it.pos + self.rest().len() <= total(slab_lens(self.slabs@))
            },
    {
        self.it.lemma_out_len::<DX<T, C>>(self.slabs@);
    }

    #[inline]
    pub(super) fn pos_impl(&self) -> (r: usize)
        ensures
            r == self.pos(),
            self.pos() <= self.end_pos(),
            self.pos() <= self.column().len(),
    {
        proof {
            use_type_invariant(self);
            self.lemma_window();
        }
        self.it.pos
    }

    #[inline]
    pub(super) fn end_pos_impl(&self) -> (r: usize)
        ensures
            r == self.end_pos(),
            self.pos() <= self.end_pos(),
            self.pos() <= self.column().len(),
    {
        proof {
            use_type_invariant(self);
            self.lemma_window();
        }
        self.it.pos + self.it.items_left
    }

    pub(super) fn set_max_impl(&mut self, end: usize)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
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
        }
        self.it.set_max::<DX<T, C>>(self.slabs, end);
        proof {
            self.lemma_window();
            assert(self.rest() =~= window(old(self).column(), old(self).pos() as int, end as int));
        }
    }

    /// Move to the `n`th of the values left, counted from 0, and past
    /// it, if there are that many, and give what the column keeps
    /// there: one of the type's values. If there are not, none is
    /// left. This is `DeltaIter::nth` before its value is made the
    /// type's.
    pub(super) fn step(&mut self, n: usize) -> (r: Option<Option<i64>>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            n < old(self).rest().len() ==> {
                &&& r matches Some(d) && in_dom::<T>(d) && T::of(d) == old(self).rest()[n as int]
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
    {
        let ghost me = *self;
        proof {
            use_type_invariant(&*self);
            me.lemma_window();
            me.it.lemma_out_window::<DX<T, C>>(me.slabs@);
        }
        // the reader and the sum move together, and are written back
        // together
        let mut it = self.it.dup();
        let mut running = self.running;
        let r = d_step::<DX<T, C>, DB<T, C>>(&mut it, self.slabs, self.ix, n, &mut running);
        *self = DeltaIter { it, slabs: self.slabs, running, ix: self.ix, col: self.col };
        proof {
            let s = me.slabs@;
            let (p, e) = (me.it.pos as int, me.it.pos + me.it.items_left);
            self.lemma_window();
            if n < me.rest().len() {
                // what the reader gives is what the column keeps there
                lemma_realized_window::<DX<T, C>, DB<T, C>>(s, p, e, n as int);
                lemma_kept_at::<DX<T, C>, DB<T, C>>(s, p + n);
                assert(r == Some(me.kept()[p + n]));
                assert(in_dom::<T>(me.kept()[p + n]));
                assert(me.rest()[n as int] == me.column()[p + n]);
                lemma_window_past(me.column(), p, e, n + 1);
            }
        }
        r
    }

    pub(super) fn advance_to_impl(&mut self, target: usize)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
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
        if target > self.it.pos {
            self.step(target - self.it.pos - 1);
            proof {
                assert(self.rest() =~= past(old(self).rest(), target - old(self).pos()));
            }
        }
    }

    pub(super) fn advance_by_impl(&mut self, amount: usize)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
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
        if amount > 0 {
            self.step(amount - 1);
            proof {
                assert(self.rest() =~= past(old(self).rest(), amount as int));
            }
        }
    }

    pub(super) fn shift_impl(&mut self, range: Range<usize>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
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
        let ghost (c, p) = (self.column(), self.it.pos as int);
        proof {
            use_type_invariant(&*self);
            self.lemma_window();
        }
        self.set_max(range.end);
        self.advance_to(range.start);
        proof {
            if range.start > p {
                lemma_window_past(c, p, range.end as int, range.start - p);
            }
        }
    }

    #[allow(clippy::manual_map)]
    pub(super) fn shift_next_impl(&mut self, range: Range<usize>) -> (r: Option<T>)
        requires
            range.start >= old(self).pos(),
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            ({
                let w = window(old(self).column(), range.start as int, range.end as int);
                &&& w.len() > 0 ==> {
                    &&& r == Some(w[0])
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
        let ghost (c, p) = (self.column(), self.it.pos as int);
        proof {
            use_type_invariant(&*self);
            self.lemma_window();
        }
        self.set_max(range.end);
        let ghost o1 = self.rest();
        let r = self.step(range.start - self.it.pos);
        proof {
            let (w, n) = (window(c, range.start as int, range.end as int), range.start - p);
            lemma_window_past(c, p, range.end as int, n);
            if n < o1.len() {
                assert(w =~= o1.skip(n));
                assert(w.skip(1) =~= o1.skip(n + 1));
            }
        }
        match r {
            Some(d) => Some(T::from_i64(d)),
            None => None,
        }
    }

    pub(super) fn next_run_impl(&mut self) -> (r: Option<DeltaRun>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            match r {
                Some(run) => {
                    &&& head_run(
                        window(deltas(old(self).column()), old(self).pos() as int, old(self).end_pos() as int),
                        run.delta,
                        run.count as int,
                    )
                    &&& run.prefix == delta_prefix(old(self).column(), old(self).pos() as int)
                    &&& delta_run_values::<T>(run) == old(self).rest().take(run.count as int)
                    &&& delta_run_fits::<T>(run)
                    &&& final(self).rest() == old(self).rest().skip(run.count as int)
                    &&& final(self).pos() == old(self).pos() + run.count
                    &&& final(self).end_pos() == old(self).end_pos()
                },
                None => {
                    &&& old(self).rest().len() == 0
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos()
                    &&& final(self).end_pos() == old(self).pos()
                },
            },
    {
        let ghost me = *self;
        proof {
            use_type_invariant(&*self);
            me.lemma_window();
            me.it.lemma_out_window::<DX<T, C>>(me.slabs@);
        }
        // the reader and the sum move together, and are written back
        // together
        let mut it = self.it.dup();
        let mut running = self.running;
        let r = d_run::<DX<T, C>, DB<T, C>>(&mut it, self.slabs, &mut running);
        *self = DeltaIter { it, slabs: self.slabs, running, ix: self.ix, col: self.col };
        match r {
            Some((prefix, run)) => {
                let delta = <DB<T, C> as DeltaOps<DIt<'a, T, C>>>::to_opt(run.value);
                proof {
                    let s = me.slabs@;
                    let (p, e) = (me.it.pos as int, me.it.pos + me.it.items_left);
                    let n = run.count as int;
                    let o = me.it.out::<DX<T, C>>(s);
                    let v = vals_of::<DX<T, C>>(s);
                    let ds = dslabs::<DX<T, C>, DB<T, C>>(s);
                    let col = me.column();
                    self.lemma_window();
                    // the sum before the run is the number before it
                    assert(ds.take(p) =~= dbefore::<DX<T, C>, DB<T, C>>(s, p));
                    lemma_deltas_kept::<T>(ds, p);
                    assert(prefix == delta_prefix(col, p));
                    // what the reader gives of the run are the column's
                    // differences there, and the one after it is another
                    let w = window(deltas(col), p, e);
                    assert(w.len() == o.len());
                    assert forall|k: int| 0 <= k < n implies #[trigger] w[k] == delta by {
                        lemma_deltas_kept::<T>(ds, p + k);
                        assert(o.take(n)[k] == run.value);
                        assert(o[k] == v[p + k]);
                    }
                    if n < w.len() {
                        lemma_deltas_kept::<T>(ds, p + n);
                        assert(o[n] == v[p + n]);
                        T::lemma_item(o[n], run.value);
                    }
                    // so the run's values are the column's there
                    lemma_vals_fit::<T>(me.kept());
                    assert forall|j: int| p <= j < p + n implies delta_at(col, j) == delta by {
                        assert(w[j - p] == delta);
                        assert(w[j - p] == deltas(col)[j]);
                    }
                    lemma_delta_run_at::<T>(col, p, run.count, delta);
                    assert(col.subrange(p, p + n) =~= me.rest().take(n));
                    lemma_window_past(col, p, e, n);
                }
                Some(DeltaRun { prefix, delta, count: run.count })
            },
            None => {
                proof {
                    self.lemma_window();
                }
                None
            },
        }
    }

    pub(super) fn runs_impl(self) -> (r: DeltaRuns<'a, T, C>)
        ensures
            r.iter() == self,
    {
        DeltaRuns(self)
    }

    pub(super) fn suspend_impl(&self) -> (s: DeltaIterState)
        ensures
            s.pos() == self.pos(),
            s.end_pos() == self.end_pos(),
            match self.source() {
                Some(c) => s.fits(&c),
                None => true,
            },
    {
        proof {
            use_type_invariant(self);
            self.lemma_window();
        }
        let (slab_idx, slab_consumed, pos, items_left) = self.it.suspended::<DX<T, C>>(self.slabs);
        let counter = match self.col {
            Some(c) => c.counter(),
            None => 0,
        };
        DeltaIterState {
            inner: IterState { counter, num_slabs: self.slabs.len(), slab_idx, pos, items_left, slab_consumed },
        }
    }

    /// What the iterator has left, value by value: each is what the
    /// column keeps at its place, as the type's value.
    pub(super) proof fn lemma_rest(&self)
        requires
            self.it.wf::<DX<T, C>>(self.slabs@),
            self.it.pos <= total(slab_lens(self.slabs@)),
        ensures
            self.column().len() == self.kept().len(),
            self.it.pos + self.rest().len() <= self.kept().len(),
            forall|j: int|
                0 <= j < self.rest().len() ==> #[trigger] self.rest()[j] == T::of(self.kept()[self.it.pos + j]),
    {
        self.lemma_window();
        assert forall|j: int| 0 <= j < self.rest().len() implies #[trigger] self.rest()[j] == T::of(
            self.kept()[self.it.pos + j],
        ) by {
            assert(self.rest()[j] == self.column()[self.it.pos + j]);
        }
    }

    /// Move on to the first of the values left that the column keeps as
    /// a number from `lo` to `hi`, and past it: given are its index in
    /// the column and the number. If there is none, the iterator gives
    /// all it has.
    pub(super) fn scan(&mut self, lo: i64, hi: i64) -> (r: Option<(usize, i64)>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            match r {
                Some((at, v)) => {
                    &&& old(self).pos() <= at < old(self).pos() + old(self).rest().len()
                    &&& old(self).kept()[at as int] == Some(v)
                    &&& lo <= v <= hi
                    &&& forall|i: int| old(self).pos() <= i < at ==> !kept_in(#[trigger] old(self).kept()[i], lo, hi)
                    &&& final(self).rest() == old(self).rest().skip(at - old(self).pos() + 1)
                    &&& final(self).pos() == at + 1
                    &&& final(self).end_pos() == old(self).end_pos()
                },
                None => {
                    &&& forall|i: int|
                        old(self).pos() <= i < old(self).pos() + old(self).rest().len() ==> !kept_in(
                            #[trigger] old(self).kept()[i],
                            lo,
                            hi,
                        )
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                    &&& final(self).end_pos() == final(self).pos()
                },
            },
    {
        let ghost me = *self;
        proof {
            use_type_invariant(&*self);
            me.lemma_window();
            me.it.lemma_out_window::<DX<T, C>>(me.slabs@);
            lemma_search_start::<DX<T, C>, DB<T, C>>(me.it, me.ix, me.slabs@);
        }
        // the reader and the sum move together, and are written back
        // together
        let mut it = self.it.dup();
        let mut running = self.running;
        let r = d_scan::<DX<T, C>, DB<T, C>>(&mut it, self.slabs, self.ix, &mut running, lo, hi);
        *self = DeltaIter { it, slabs: self.slabs, running, ix: self.ix, col: self.col };
        proof {
            let s = me.slabs@;
            let (p, e) = (me.it.pos as int, me.it.pos + me.it.items_left);
            self.lemma_window();
            match r {
                Some((h, v)) => {
                    lemma_kept_rv::<DX<T, C>, DB<T, C>>(s, h as int);
                    lemma_kept_hit::<DX<T, C>, DB<T, C>>(s, lo, hi, h as int);
                    assert forall|i: int| p <= i < h implies !kept_in(#[trigger] me.kept()[i], lo, hi) by {
                        lemma_kept_hit::<DX<T, C>, DB<T, C>>(s, lo, hi, i);
                    }
                    lemma_window_past(me.column(), p, e, h + 1 - p);
                    assert(self.rest() =~= me.rest().skip(h - p + 1));
                },
                None => {
                    assert forall|i: int| p <= i < p + me.rest().len() implies !kept_in(
                        #[trigger] me.kept()[i],
                        lo,
                        hi,
                    ) by {
                        lemma_kept_hit::<DX<T, C>, DB<T, C>>(s, lo, hi, i);
                    }
                },
            }
        }
        r
    }

    /// Move on to the first null of the values left, and past it: its
    /// index in the column is given. If there is none, the iterator
    /// gives all it has.
    pub(super) fn scan_null(&mut self) -> (r: Option<usize>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            match r {
                Some(at) => {
                    &&& old(self).pos() <= at < old(self).pos() + old(self).rest().len()
                    &&& old(self).kept()[at as int] is None
                    &&& forall|i: int| old(self).pos() <= i < at ==> (#[trigger] old(self).kept()[i]) is Some
                    &&& final(self).rest() == old(self).rest().skip(at - old(self).pos() + 1)
                    &&& final(self).pos() == at + 1
                    &&& final(self).end_pos() == old(self).end_pos()
                },
                None => {
                    &&& forall|i: int|
                        old(self).pos() <= i < old(self).pos() + old(self).rest().len() ==> (
                        #[trigger] old(self).kept()[i]) is Some
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                    &&& final(self).end_pos() == final(self).pos()
                },
            },
    {
        let ghost me = *self;
        proof {
            use_type_invariant(&*self);
            me.lemma_window();
            me.it.lemma_out_window::<DX<T, C>>(me.slabs@);
            lemma_search_start::<DX<T, C>, DB<T, C>>(me.it, me.ix, me.slabs@);
        }
        let mut it = self.it.dup();
        let mut running = self.running;
        let r = d_scan_null::<DX<T, C>, DB<T, C>>(&mut it, self.slabs, self.ix, &mut running);
        *self = DeltaIter { it, slabs: self.slabs, running, ix: self.ix, col: self.col };
        proof {
            let s = me.slabs@;
            let (p, e) = (me.it.pos as int, me.it.pos + me.it.items_left);
            self.lemma_window();
            match r {
                Some(h) => {
                    lemma_kept_rv::<DX<T, C>, DB<T, C>>(s, h as int);
                    assert forall|i: int| p <= i < h implies (#[trigger] me.kept()[i]) is Some by {
                        lemma_kept_rv::<DX<T, C>, DB<T, C>>(s, i);
                    }
                    lemma_window_past(me.column(), p, e, h + 1 - p);
                    assert(self.rest() =~= me.rest().skip(h - p + 1));
                },
                None => {
                    assert forall|i: int| p <= i < p + me.rest().len() implies (#[trigger] me.kept()[i]) is Some by {
                        lemma_kept_rv::<DX<T, C>, DB<T, C>>(s, i);
                    }
                },
            }
        }
        r
    }

    #[allow(clippy::manual_map)]  // a match is what the proof follows
    pub(super) fn scan_to_value_impl(&mut self, target: T) -> (r: Option<usize>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            match r {
                Some(at) => {
                    &&& old(self).pos() <= at < old(self).pos() + old(self).rest().len()
                    &&& old(self).rest()[at - old(self).pos()] == target
                    &&& forall|i: int| 0 <= i < at - old(self).pos() ==> #[trigger] old(self).rest()[i] != target
                    &&& final(self).rest() == old(self).rest().skip(at - old(self).pos() + 1)
                    &&& final(self).pos() == at + 1
                    &&& final(self).end_pos() == old(self).end_pos()
                },
                None => {
                    &&& forall|i: int| 0 <= i < old(self).rest().len() ==> #[trigger] old(self).rest()[i] != target
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                    &&& final(self).end_pos() == final(self).pos()
                },
            },
    {
        let ghost me = *self;
        let ghost p = self.it.pos as int;
        proof {
            use_type_invariant(&*self);
            me.lemma_rest();
            // how each value left stands to `target`
            assert forall|i: int| 0 <= i < me.rest().len() implies (#[trigger] me.rest()[i] == target) == (
            target.fits() && me.kept()[p + i] == target.dv()) by {
                lemma_bound::<T>(target, me.kept()[p + i]);
            }
        }
        match target.try_i64() {
            None => {
                // a column keeps no value that does not fit
                let end = self.end_pos();
                self.advance_to(end);
                None
            },
            Some(None) => self.scan_null(),
            Some(Some(t)) => match self.scan(t, t) {
                Some((at, _)) => Some(at),
                None => None,
            },
        }
    }

    pub(super) fn scan_to_range_impl<R: RangeBounds<T>>(&mut self, range: R) -> (r: Option<(usize, T)>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            ({
                let (lo, hi) = (range.spec_start_bound(), range.spec_end_bound());
                match r {
                    Some((at, v)) => {
                        &&& old(self).pos() <= at < old(self).pos() + old(self).rest().len()
                        &&& old(self).rest()[at - old(self).pos()] == v
                        &&& delta_within::<T>(lo, hi, v)
                        &&& forall|i: int|
                            0 <= i < at - old(self).pos() ==> !delta_within::<T>(lo, hi, #[trigger] old(self).rest()[i])
                        &&& final(self).rest() == old(self).rest().skip(at - old(self).pos() + 1)
                        &&& final(self).pos() == at + 1
                        &&& final(self).end_pos() == old(self).end_pos()
                    },
                    None => {
                        &&& forall|i: int|
                            0 <= i < old(self).rest().len() ==> !delta_within::<T>(lo, hi, #[trigger] old(self).rest()[i])
                        &&& final(self).rest().len() == 0
                        &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                        &&& final(self).end_pos() == final(self).pos()
                    },
                }
            }),
    {
        let ghost me = *self;
        let ghost p = self.it.pos as int;
        let ghost (lo, hi) = (range.spec_start_bound(), range.spec_end_bound());
        proof {
            use_type_invariant(&*self);
            me.lemma_rest();
        }
        let b = bounds_of::<T>(range.start_bound(), range.end_bound());
        proof {
            // whether each value left is within the bounds
            assert forall|i: int| 0 <= i < me.rest().len() implies delta_within::<T>(lo, hi, #[trigger] me.rest()[i])
                == match b {
                Some((x, y)) => kept_in(me.kept()[p + i], x, y),
                None => false,
            } by {
                assert(in_dom::<T>(me.kept()[p + i]));
                assert(me.rest()[i] == T::of(me.kept()[p + i]));
            }
        }
        match b {
            None => {
                // no value of a column is within the bounds
                let end = self.end_pos();
                self.advance_to(end);
                None
            },
            Some((x, y)) => match self.scan(x, y) {
                Some((at, v)) => {
                    proof {
                        assert(in_dom::<T>(me.kept()[at as int]));
                        assert(me.rest()[at - p] == T::of(me.kept()[p + (at - p)]));
                    }
                    Some((at, T::from_i64(Some(v))))
                },
                None => None,
            },
        }
    }

    /// A search of the differences for what `target` is kept as, over
    /// what the iterator has left, found `a..b` if the sums there are
    /// in order; then a search of the values for `target` finds `a..b`
    /// if they are in order.
    pub(super) proof fn lemma_sought(&self, target: T, a: int, b: int)
        requires
            self.it.wf::<DX<T, C>>(self.slabs@),
            self.it.pos <= total(slab_lens(self.slabs@)),
            all_dom::<T>(self.kept()),
            all_fit::<DX<T, C>, DB<T, C>>(self.slabs@),
            target.fits(),
            ({
                let (s, p) = (self.slabs@, self.it.pos as int);
                let t = match target.dv() {
                    Some(t) => Some(t as int),
                    None => None::<int>,
                };
                dsorted::<DX<T, C>, DB<T, C>>(s, p, p + self.rest().len()) ==> dfound::<DX<T, C>, DB<T, C>>(
                    s,
                    p,
                    p + self.rest().len(),
                    t,
                    a,
                    b,
                )
            }),
        ensures
            ({
                let (p, o) = (self.it.pos as int, self.rest());
                sorted::<T>(o) ==> forall|j: int|
                    0 <= j < o.len() ==> {
                        &&& (p + j < a) == T::below(#[trigger] o[j], target)
                        &&& (p + j < b) == !T::below(target, o[j])
                        &&& (a <= p + j < b) == (o[j] == target)
                    }
            }),
    {
        let (s, p, o, k) = (self.slabs@, self.it.pos as int, self.rest(), self.kept());
        let e = p + o.len();
        self.lemma_window();
        self.lemma_rest();
        if sorted::<T>(o) {
            // values in order are kept as sums in order
            assert forall|i: int, j: int| p <= i <= j < e implies !dbelow(
                #[trigger] rv::<DX<T, C>, DB<T, C>>(s, j),
                #[trigger] rv::<DX<T, C>, DB<T, C>>(s, i),
            ) by {
                lemma_kept_rv::<DX<T, C>, DB<T, C>>(s, i);
                lemma_kept_rv::<DX<T, C>, DB<T, C>>(s, j);
                assert(o[i - p] == T::of(k[p + (i - p)]));
                assert(o[j - p] == T::of(k[p + (j - p)]));
                lemma_bound::<T>(target, k[i]);
                lemma_bound::<T>(o[i - p], k[j]);
                assert(!T::below(o[j - p], o[i - p]));
            }
            assert(dsorted::<DX<T, C>, DB<T, C>>(s, p, e));
            assert forall|j: int| 0 <= j < o.len() implies {
                &&& (p + j < a) == T::below(#[trigger] o[j], target)
                &&& (p + j < b) == !T::below(target, o[j])
                &&& (a <= p + j < b) == (o[j] == target)
            } by {
                lemma_kept_rv::<DX<T, C>, DB<T, C>>(s, p + j);
                lemma_bound::<T>(target, k[p + j]);
                assert(o[j] == T::of(k[p + j]));
            }
        }
    }

    /// Search the values left for `target`, where they are in order,
    /// and move on to it: given are the first place in the column that
    /// holds it and the place after the last, and the iterator stands
    /// at the first. For a `target` that does not fit, which is after
    /// every value of a column, the iterator gives all it has.
    pub(super) fn seek_rest(&mut self, target: T) -> (r: (usize, usize))
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            ({
                let (p, o) = (old(self).pos() as int, old(self).rest());
                &&& p <= r.0 <= r.1 <= p + o.len()
                &&& final(self).pos() == r.0
                &&& final(self).rest() == o.skip(r.0 - p)
                &&& final(self).end_pos() == if target.fits() {
                    old(self).end_pos()
                } else {
                    final(self).pos()
                }
                &&& sorted::<T>(o) ==> forall|j: int|
                    0 <= j < o.len() ==> {
                        &&& (p + j < r.0) == T::below(#[trigger] o[j], target)
                        &&& (p + j < r.1) == !T::below(target, o[j])
                        &&& (r.0 <= p + j < r.1) == (o[j] == target)
                    }
            }),
    {
        let ghost me = *self;
        let ghost p = self.it.pos as int;
        proof {
            use_type_invariant(&*self);
            me.lemma_window();
            me.lemma_rest();
            me.it.lemma_out_window::<DX<T, C>>(me.slabs@);
            lemma_search_start::<DX<T, C>, DB<T, C>>(me.it, me.ix, me.slabs@);
            assert(me.rest().skip(0) =~= me.rest());
        }
        let here = self.it.pos;
        match target.try_i64() {
            None => {
                // after every value a column keeps
                let end = self.end_pos();
                self.advance_to(end);
                let at = self.pos();
                proof {
                    assert(self.rest() =~= me.rest().skip(at - p));
                    assert forall|j: int| 0 <= j < me.rest().len() implies {
                        &&& T::below(#[trigger] me.rest()[j], target)
                        &&& !T::below(target, me.rest()[j])
                        &&& me.rest()[j] != target
                    } by {
                        assert(in_dom::<T>(me.kept()[p + j]));
                        lemma_bound::<T>(target, me.kept()[p + j]);
                    }
                }
                (at, at)
            },
            Some(None) => {
                let run_end = d_seek_null::<DX<T, C>, DB<T, C>>(&self.it, self.slabs);
                proof {
                    me.lemma_sought(target, p, run_end as int);
                }
                (here, run_end)
            },
            Some(Some(t)) => {
                let mut it = self.it.dup();
                let mut running = self.running;
                let r = d_seek::<DX<T, C>, DB<T, C>>(&mut it, self.slabs, self.ix, &mut running, t);
                *self = DeltaIter { it, slabs: self.slabs, running, ix: self.ix, col: self.col };
                proof {
                    self.lemma_window();
                    me.lemma_sought(target, r.0 as int, r.1 as int);
                    lemma_window_past(me.column(), p, me.it.pos + me.it.items_left, r.0 - p);
                    assert(self.rest() =~= me.rest().skip(r.0 - p));
                }
                r
            },
        }
    }

    pub(super) fn seek_to_value_impl<R: RangeBounds<usize>>(&mut self, target: T, range: R) -> (r: Range<usize>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            ({
                // where the range starts and ends
                let (rs, re) = (
                    bound_start(range.spec_start_bound()),
                    bound_end(range.spec_start_bound(), range.spec_end_bound(), old(self).end_pos() as int),
                );
                let (p, c) = (old(self).pos() as int, old(self).column());
                // where the window is made to end
                let e = if rs <= re {
                    re as int
                } else {
                    rs as int
                };
                // the places searched are `s..t` of the column
                let t = if e <= p {
                    p
                } else if e <= c.len() {
                    e
                } else {
                    c.len() as int
                };
                let s = if rs <= p {
                    p
                } else if rs <= t {
                    rs as int
                } else {
                    t
                };
                &&& p <= s <= r.start <= r.end <= t <= c.len()
                &&& final(self).pos() == r.start
                &&& final(self).rest() == c.subrange(r.start as int, t)
                &&& final(self).end_pos() == if rs > t || !target.fits() {
                    r.start as int
                } else if e >= p {
                    e
                } else {
                    p
                }
                &&& sorted::<T>(c.subrange(s, t)) ==> forall|k: int|
                    s <= k < t ==> {
                        &&& (k < r.start) == T::below(#[trigger] c[k], target)
                        &&& (r.start <= k < r.end) == (c[k] == target)
                        &&& (r.end <= k) == T::below(target, c[k])
                    }
            }),
    {
        let (start, end) = bounds_exec(&range, self.end_pos());
        self.seek_within(target, start..end)
    }

    /// [`seek_to_value`](DeltaIter::seek_to_value) over the places
    /// `range.start..range.end`.
    fn seek_within(&mut self, target: T, range: Range<usize>) -> (r: Range<usize>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            ({
                let (p, c) = (old(self).pos() as int, old(self).column());
                // where the window is made to end
                let e = if range.start <= range.end {
                    range.end as int
                } else {
                    range.start as int
                };
                // the places searched are `s..t` of the column
                let t = if e <= p {
                    p
                } else if e <= c.len() {
                    e
                } else {
                    c.len() as int
                };
                let s = if range.start <= p {
                    p
                } else if range.start <= t {
                    range.start as int
                } else {
                    t
                };
                &&& p <= s <= r.start <= r.end <= t <= c.len()
                &&& final(self).pos() == r.start
                &&& final(self).rest() == c.subrange(r.start as int, t)
                &&& final(self).end_pos() == if range.start > t || !target.fits() {
                    r.start as int
                } else if e >= p {
                    e
                } else {
                    p
                }
                &&& sorted::<T>(c.subrange(s, t)) ==> forall|k: int|
                    s <= k < t ==> {
                        &&& (k < r.start) == T::below(#[trigger] c[k], target)
                        &&& (r.start <= k < r.end) == (c[k] == target)
                        &&& (r.end <= k) == T::below(target, c[k])
                    }
            }),
    {
        let ghost (p, c) = (self.pos() as int, self.column());
        proof {
            use_type_invariant(&*self);
            self.lemma_window();
        }
        let (start, end) = if range.start <= range.end {
            (range.start, range.end)
        } else {
            (range.start, range.start)
        };
        self.set_max(end);
        let ghost o = self.rest();
        if start > self.pos() {
            self.advance_to(start);
        }
        let here = self.pos();
        let ghost w = self.rest();
        let ghost t = p + o.len();
        proof {
            assert(o =~= c.subrange(p, t));
            assert(w =~= c.subrange(here as int, t));
        }
        if here >= end {
            return here..here;
        }
        let r = self.seek_rest(target);
        proof {
            assert(self.rest() =~= c.subrange(r.0 as int, t));
            lemma_found_in_column::<T, C>(c, here as int, t, r.0 as int, r.1 as int, target);
        }
        r.0..r.1
    }
}

/// What a search of the column `c` from `here` to `t` found, said of
/// the search's own values (`c.subrange(here, t)`), said of the
/// column's places.
pub(super) proof fn lemma_found_in_column<T: DeltaValue<C>, C: Codec>(c: Seq<T>, here: int, t: int, r0: int, r1: int, target: T)
    requires
        0 <= here <= t <= c.len(),
        sorted::<T>(c.subrange(here, t)) ==> forall|j: int|
            0 <= j < c.subrange(here, t).len() ==> {
                &&& (here + j < r0) == T::below(#[trigger] c.subrange(here, t)[j], target)
                &&& (here + j < r1) == !T::below(target, c.subrange(here, t)[j])
                &&& (r0 <= here + j < r1) == (c.subrange(here, t)[j] == target)
            },
    ensures
        sorted::<T>(c.subrange(here, t)) ==> forall|k: int|
            here <= k < t ==> {
                &&& (k < r0) == T::below(#[trigger] c[k], target)
                &&& (k < r1) == !T::below(target, c[k])
                &&& (r0 <= k < r1) == (c[k] == target)
            },
{
    let w = c.subrange(here, t);
    if sorted::<T>(w) {
        assert forall|k: int| here <= k < t implies {
            &&& (k < r0) == T::below(#[trigger] c[k], target)
            &&& (k < r1) == !T::below(target, c[k])
            &&& (r0 <= k < r1) == (c[k] == target)
        } by {
            assert(w[k - here] == c[k]);
        }
    }
}

impl<'a, T: DeltaValue<C>, C: Codec> DeltaIter<'a, T, C> {
    #[inline]
    pub(super) fn next_impl(&mut self) -> (r: Option<T>)
        ensures
            match r {
                Some(v) => {
                    &&& old(self).rest().len() > 0
                    &&& v == old(self).rest()[0]
                    &&& final(self).rest() == old(self).rest().skip(1)
                    &&& final(self).pos() == old(self).pos() + 1
                    &&& final(self).end_pos() == old(self).end_pos()
                },
                None => {
                    &&& old(self).rest().len() == 0
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos()
                    &&& final(self).end_pos() == old(self).pos()
                },
            },
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            // what Verus asks of every iterator's `next`
            old(self).rest().len() > 0 ==> final(self).rest() == old(self).rest().drop_first() && r == Some(old(self).rest()[0]),
            old(self).rest().len() == 0 ==> final(self).rest() == old(self).rest() && r is None,
    {
        let ghost me = *self;
        proof {
            use_type_invariant(&*self);
            me.lemma_window();
            me.it.lemma_out_window::<DX<T, C>>(me.slabs@);
        }
        // the reader and the sum move in place, together
        let r = d_next::<DX<T, C>, DB<T, C>>(&mut self.it, self.slabs, &mut self.running);
        proof {
            self.lemma_window();
        }
        match r {
            Some(d) => {
                proof {
                    let s = me.slabs@;
                    let (p, e) = (me.it.pos as int, me.it.pos + me.it.items_left);
                    // what the reader gives is what the column keeps
                    // there
                    lemma_kept_at::<DX<T, C>, DB<T, C>>(s, p);
                    assert(me.it.out::<DX<T, C>>(s)[0] == vals_of::<DX<T, C>>(s)[p]);
                    assert(d == me.kept()[p]);
                    assert(in_dom::<T>(me.kept()[p]));
                    assert(me.rest()[0] == me.column()[p]);
                    lemma_window_past(me.column(), p, e, 1);
                    assert(me.rest().skip(1) =~= me.rest().drop_first());
                }
                Some(T::from_i64(d))
            },
            None => {
                proof {
                    assert(self.rest() =~= me.rest());
                }
                None
            },
        }
    }

    #[inline]
    #[allow(clippy::manual_map)]
    pub(super) fn nth_impl(&mut self, n: usize) -> (r: Option<T>)
        ensures
            n < old(self).rest().len() ==> {
                &&& r == Some(old(self).rest()[n as int])
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
    {
        match self.step(n) {
            Some(d) => Some(T::from_i64(d)),
            None => None,
        }
    }

    #[inline]
    pub(super) fn size_hint_impl(&self) -> (r: (usize, Option<usize>))
        ensures
            r.0 == self.rest().len(),
            r.1 == Some(r.0),
    {
        proof {
            use_type_invariant(self);
            self.lemma_window();
            self.it.lemma_out_len::<DX<T, C>>(self.slabs@);
        }
        // the window can run past the column's end
        let left = match self.col {
            Some(col) => {
                col.lemma_column();
                self.it.items_left.min(col.len().saturating_sub(self.it.pos))
            },
            None => {
                proof {
                    assert(slab_lens(self.slabs@) =~= Seq::<nat>::empty());
                }
                0
            },
        };
        (left, Some(left))
    }
}

impl<'a, T: DeltaValue<C>, C: Codec> DeltaIter<'a, T, C> {
    pub(super) fn clone_impl(&self) -> (r: Self)
        ensures
            r.rest() == self.rest(),
            r.column() == self.column(),
            r.source() == self.source(),
            r.pos() == self.pos(),
            r.end_pos() == self.end_pos(),
    {
        proof {
            use_type_invariant(self);
        }
        DeltaIter {
            it: self.it.dup(),
            slabs: self.slabs,
            running: self.running,
            ix: self.ix,
            col: self.col,
        }
    }
}

impl<'a, T: DeltaValue<C>, C: Codec> DeltaIter<'a, T, C> {
    pub(super) fn default_impl() -> (r: Self)
        ensures
            r.rest().len() == 0,
            r.column().len() == 0,
            r.source() is None,
            r.pos() == 0,
            r.end_pos() == 0,
    {
        let it = ICore::empty::<DX<T, C>>();
        let slabs: &'a [Slab<<DE<T, C> as Slabs>::Tail>] = &[];
        proof {
            let s = slabs@;
            assert(s =~= Seq::empty());
            assert(slab_lens(s) =~= Seq::<nat>::empty());
            it.lemma_out_len::<DX<T, C>>(s);
            // no differences before the iterator, and their sum is 0
            assert(dbefore::<DX<T, C>, DB<T, C>>(s, 0).len() == 0);
            assert(psum(dbefore::<DX<T, C>, DB<T, C>>(s, 0)) == 0);
            lemma_crop(0);
        }
        DeltaIter { it, slabs, running: 0, ix: None, col: None }
    }
}

impl<T: DeltaValue<C>, C: Codec> DeltaColumn<T, C> {
    pub(super) fn iter_impl(&self) -> (it: DeltaIter<'_, T, C>)
        ensures
            it.rest() == self@,
            it.column() == self@,
            it.source() == Some(*self),
            it.pos() == 0,
            it.end_pos() == self@.len(),
    {
        proof {
            use_type_invariant(self);
        }
        let (slabs, ix) = self.core.delta_parts();
        let it = self.core.reader::<DX<T, C>>(0, self.core.len());
        self.core.lemma_column();
        proof {
            let s = self.core.slabs_spec();
            // no differences before the iterator, and their sum is 0
            assert(dbefore::<DX<T, C>, DB<T, C>>(s, 0).len() == 0);
            assert(psum(dbefore::<DX<T, C>, DB<T, C>>(s, 0)) == 0);
            lemma_crop(0);
            // what the reader will give is what the column keeps
            lemma_dslabs_own::<DE<T, C>, DX<T, C>, DB<T, C>>(s);
        }
        let r = DeltaIter { it, slabs, running: 0, ix: Some(ix), col: Some(&self.core) };
        proof {
            r.lemma_window();
            assert(r.rest() =~= self@);
        }
        r
    }

    pub(super) fn iter_range_impl(&self, range: Range<usize>) -> (it: DeltaIter<'_, T, C>)
        ensures
            ({
                let (start, end) = range_in(range, self@.len());
                &&& it.rest() == self@.subrange(start, end)
                &&& it.column() == self@
                &&& it.source() == Some(*self)
                &&& it.pos() == start
                &&& it.end_pos() == end
            }),
    {
        let (start, end) = range_in_exec(range, self.len());
        self.iter_within(start, end)
    }

    /// An iterator over the values `start..end`.
    fn iter_within(&self, start: usize, end: usize) -> (it: DeltaIter<'_, T, C>)
        requires
            start <= end <= self@.len(),
        ensures
            it.rest() == self@.subrange(start as int, end as int),
            it.column() == self@,
            it.source() == Some(*self),
            it.pos() == start,
            it.end_pos() == end,
    {
        let mut it = self.iter();
        it.set_max(end);
        it.advance_by(start);
        proof {
            lemma_window_past(self@, 0, end as int, start as int);
            assert(it.rest() =~= self@.subrange(start as int, end as int));
        }
        it
    }

    pub(super) fn scope_to_value_impl<R: RangeBounds<usize>>(&self, value: T, range: R) -> (r: Range<usize>)
        ensures
            ({
                let (start, end) = bounds_in(range.spec_start_bound(), range.spec_end_bound(), self@.len());
                &&& start <= r.start <= r.end <= end
                &&& sorted::<T>(self@.subrange(start, end)) ==> forall|k: int|
                    start <= k < end ==> {
                        &&& (k < r.start) == T::below(#[trigger] self@[k], value)
                        &&& (r.start <= k < r.end) == (self@[k] == value)
                        &&& (r.end <= k) == T::below(value, self@[k])
                    }
            }),
    {
        let (start, end) = bounds_in_exec(&range, self.len());
        self.scope_within(value, start, end)
    }

    /// [`scope_to_value`](DeltaColumn::scope_to_value) over the places
    /// `start..end`.
    fn scope_within(&self, value: T, start: usize, end: usize) -> (r: Range<usize>)
        requires
            start <= end <= self@.len(),
        ensures
            start <= r.start <= r.end <= end,
            sorted::<T>(self@.subrange(start as int, end as int)) ==> forall|k: int|
                start <= k < end ==> {
                    &&& (k < r.start) == T::below(#[trigger] self@[k], value)
                    &&& (r.start <= k < r.end) == (self@[k] == value)
                    &&& (r.end <= k) == T::below(value, self@[k])
                },
    {
        let mut it = self.iter();
        it.seek_within(value, start..end)
    }
}

impl<'a, T: DeltaValue<C>, C: Codec> DeltaRuns<'a, T, C> {
    #[inline(always)]
    pub(super) fn next_impl(&mut self) -> (r: Option<DeltaRun>)
        ensures
            final(self).iter().column() == old(self).iter().column(),
            final(self).iter().source() == old(self).iter().source(),
            match r {
                Some(run) => {
                    &&& head_run(
                        window(
                            deltas(old(self).iter().column()),
                            old(self).iter().pos() as int,
                            old(self).iter().end_pos() as int,
                        ),
                        run.delta,
                        run.count as int,
                    )
                    &&& run.prefix == delta_prefix(old(self).iter().column(), old(self).iter().pos() as int)
                    &&& delta_run_values::<T>(run) == old(self).iter().rest().take(run.count as int)
                    &&& delta_run_fits::<T>(run)
                    &&& final(self).iter().rest() == old(self).iter().rest().skip(run.count as int)
                    &&& final(self).iter().pos() == old(self).iter().pos() + run.count
                    &&& final(self).iter().end_pos() == old(self).iter().end_pos()
                },
                None => {
                    &&& old(self).iter().rest().len() == 0
                    &&& final(self).iter().rest().len() == 0
                    &&& final(self).iter().pos() == old(self).iter().pos()
                    &&& final(self).iter().end_pos() == old(self).iter().pos()
                },
            },
            // what Verus asks of every iterator's `next`
            delta_runs_at(
            old(self).iter().column(),
            old(self).iter().pos() as int,
            runs_of(window(deltas(old(self).iter().column()), old(self).iter().pos() as int, old(self).iter().end_pos() as int)),
        ).len() > 0 ==> delta_runs_at(
            final(self).iter().column(),
            final(self).iter().pos() as int,
            runs_of(window(deltas(final(self).iter().column()), final(self).iter().pos() as int, final(self).iter().end_pos() as int)),
        ) == delta_runs_at(
            old(self).iter().column(),
            old(self).iter().pos() as int,
            runs_of(window(deltas(old(self).iter().column()), old(self).iter().pos() as int, old(self).iter().end_pos() as int)),
        ).drop_first() && r == Some(delta_runs_at(
            old(self).iter().column(),
            old(self).iter().pos() as int,
            runs_of(window(deltas(old(self).iter().column()), old(self).iter().pos() as int, old(self).iter().end_pos() as int)),
        )[0]),
            delta_runs_at(
            old(self).iter().column(),
            old(self).iter().pos() as int,
            runs_of(window(deltas(old(self).iter().column()), old(self).iter().pos() as int, old(self).iter().end_pos() as int)),
        ).len() == 0 ==> delta_runs_at(
            final(self).iter().column(),
            final(self).iter().pos() as int,
            runs_of(window(deltas(final(self).iter().column()), final(self).iter().pos() as int, final(self).iter().end_pos() as int)),
        ) == delta_runs_at(
            old(self).iter().column(),
            old(self).iter().pos() as int,
            runs_of(window(deltas(old(self).iter().column()), old(self).iter().pos() as int, old(self).iter().end_pos() as int)),
        ) && r is None,
    {
        let ghost (col, p, e) = (self.0.column(), self.0.pos() as int, self.0.end_pos() as int);
        let ghost w = window(deltas(col), p, e);
        let r = self.0.next_run();
        proof {
            match r {
                Some(run) => {
                    // the runs left were this one and those left now
                    let n = run.count as int;
                    let w2 = window(deltas(col), p + n, e);
                    lemma_runs_of(w, run.delta, run.count);
                    lemma_window_past(deltas(col), p, e, n);
                    assert(w.skip(n) == w2);
                    assert(runs_of(w).drop_first() =~= runs_of(w2));
                    let before = delta_runs_at(col, p, runs_of(w));
                    let after = delta_runs_at(col, p + n, runs_of(w2));
                    assert(before =~= seq![run] + after);
                    assert(before.drop_first() =~= after);
                },
                None => {
                    assert(w.len() == 0);
                    assert(window(deltas(col), p, p).len() == 0);
                },
            }
        }
        r
    }
}

impl DeltaIterState {
    pub(super) proof fn lemma_fits_impl<T: DeltaValue<C>, C: Codec>(&self, column: &DeltaColumn<T, C>)
        requires
            self.fits(column),
        ensures
            self.edits() == column.edits(),
    {
    }

    pub(super) fn try_resume_impl<'a, T: DeltaValue<C>, C: Codec>(&self, column: &'a DeltaColumn<T, C>) -> (r: Result<
        DeltaIter<'a, T, C>,
        PackError,
    >)
        ensures
            r is Ok <==> self.fits(column),
            r is Ok ==> self.edits() == column.edits(),
            r matches Ok(it) ==> {
                &&& it.source() == Some(*column)
                &&& it.column() == column@
                &&& it.pos() == self.pos()
                &&& it.rest() == window(column@, self.pos() as int, self.end_pos() as int)
                &&& it.end_pos() == self.end_pos() || (self.pos() == column@.len() && it.end_pos() == self.pos())
            },
    {
        proof {
            use_type_invariant(column);
        }
        if self.inner.counter != column.core.counter() {
            return Err(invalid_resume());
        }
        let (slabs, ix) = column.core.delta_parts();
        if self.inner.num_slabs != slabs.len() {
            return Err(invalid_resume());
        }
        if self.inner.pos > column.core.len() {
            return Err(invalid_resume());
        }
        let it = match column.core.resume::<DX<T, C>>(
            self.inner.slab_idx,
            self.inner.slab_consumed,
            self.inner.pos,
            self.inner.items_left,
        ) {
            Some(it) => it,
            None => {
                return Err(invalid_resume());
            },
        };
        // the sum of the differences before the place is this column's:
        // a state taken of another column would have another's
        let mut before = column.iter();
        before.advance_by(self.inner.pos);
        column.core.lemma_column();
        proof {
            use_type_invariant(&before);
            // what the reader will give is what the column keeps
            lemma_dslabs_own::<DE<T, C>, DX<T, C>, DB<T, C>>(column.core.slabs_spec());
        }
        let r = DeltaIter { it, slabs, running: before.running, ix: Some(ix), col: Some(&column.core) };
        proof {
            r.lemma_window();
            assert(r.rest() =~= window(column@, self.pos() as int, self.end_pos() as int));
        }
        Ok(r)
    }
}

impl<'a, T: DeltaValue<C>, C: Codec> DeltaIter<'a, T, C> {
    pub(super) proof fn lemma_shift_impl(&self)
        ensures
            0 <= self.shift_at(),
            self.shift_left() == window(self.shift_vals(), self.shift_at(), self.shift_end()),
            self.shift_left() == concat(self.shift_items().map_values(|x: <Self as Iterator>::Item| Self::shift_spans(x))),
            forall|k: int|
                0 <= k < self.shift_items().len() ==> Self::shift_spans(#[trigger] self.shift_items()[k]).len() > 0,
    {
        let xs = self.rest();
        lemma_singles(xs);
        assert(xs.map_values(|x: T| Self::shift_spans(x)) =~= xs.map_values(|x: T| seq![x]));
    }

    #[inline(always)]
    pub(super) fn shift_nth_impl(&mut self, n: usize) -> (r: Option<T>)
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
        let r = self.nth(n);
        proof {
            if n < old(self).rest().len() {
                let xs = old(self).rest().take(n + 1);
                lemma_singles(xs);
                assert(xs.map_values(|x: T| Self::shift_spans(x)) =~= xs.map_values(|x: T| seq![x]));
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
        }
        DeltaIter { it: self.it.dup(), slabs: self.slabs, running: self.running, ix: self.ix, col: self.col }
    }

    #[inline(always)]
    pub(super) fn seek_to_impl(&mut self, at: usize) -> (r: Option<T>)
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
        let p = self.pos();
        if at >= p {
            self.nth(at - p)
        } else {
            None
        }
    }
}

impl<'a, T: DeltaValue<C>, C: Codec> DeltaIter<'a, T, C> {
    pub(super) proof fn lemma_shift_model_impl(&self)
        ensures
            self.shift_vals() == self.column(),
            self.shift_at() == self.pos(),
            self.shift_items() == self.rest(),
            self.shift_left() == self.rest(),
            Self::shift_unit(),
    {
    }

    #[inline(always)]
    pub(super) fn unshift_impl(self) -> (r: Unshift<Self>)
        ensures
            r.unshift_vals() == self.column(),
            r.unshift_left() == self.rest(),
    {
        Unshift::new(self)
    }

    #[inline(always)]
    pub(super) fn ranges_impl<'r>(self, ranges: &'r [Range<usize>]) -> (r: Ranges<'r, Self>)
        ensures
            r.ranges_vals() == self.column(),
            r.ranges_left() == windows(self.column(), ranges@),
    {
        Ranges::new(self, ranges)
    }
}

impl<'a, T: DeltaValue<C>, C: Codec> DeltaRuns<'a, T, C> {
    /// The first run of what is left, and the values it stands for.
    pub(super) fn take_run(&mut self) -> (r: Option<DeltaRun>)
        ensures
            final(self).shift_vals() == old(self).shift_vals(),
            old(self).shift_left().len() > 0 ==> (r matches Some(x) && {
                let n = x.count as int;
                &&& 0 < n <= old(self).shift_left().len()
                &&& old(self).shift_items().len() > 0
                &&& x == old(self).shift_items()[0]
                &&& final(self).shift_items() == old(self).shift_items().skip(1)
                &&& Self::shift_spans(x) == old(self).shift_left().take(n)
                &&& final(self).shift_left() == old(self).shift_left().skip(n)
                &&& final(self).shift_at() == old(self).shift_at() + n
            }),
            old(self).shift_left().len() == 0 ==> {
                &&& r is None
                &&& old(self).shift_items().len() == 0
                &&& final(self).shift_items().len() == 0
                &&& final(self).shift_left().len() == 0
            },
    {
        let ghost (vs, a, e, l) = (self.0.column(), self.0.pos() as int, self.0.end_pos() as int, self.0.rest().len() as int);
        let r = self.0.next_run();
        proof {
            if let Some(x) = r {
                assert(window(deltas(vs), a, e) =~= deltas(vs).subrange(a, a + l));
                lemma_delta_take(vs, a, l, x);
            }
        }
        r
    }
}

impl<'a, T: DeltaValue<C>, C: Codec> DeltaRuns<'a, T, C> {
    pub(super) proof fn lemma_shift_impl(&self)
        ensures
            0 <= self.shift_at(),
            self.shift_left() == window(self.shift_vals(), self.shift_at(), self.shift_end()),
            self.shift_left() == concat(self.shift_items().map_values(|x: <Self as Iterator>::Item| Self::shift_spans(x))),
            forall|k: int|
                0 <= k < self.shift_items().len() ==> Self::shift_spans(#[trigger] self.shift_items()[k]).len() > 0,
    {
        let (vs, a) = (self.0.column(), self.0.spec_pos() as int);
        let b = if a + self.0.rest().len() <= vs.len() {
            a + self.0.rest().len()
        } else {
            vs.len() as int
        };
        let w = window(deltas(vs), a, a + self.0.rest().len());
        if a < vs.len() {
            assert(w =~= deltas(vs).subrange(a, b));
            lemma_run_units(w);
            lemma_delta_runs_units(vs, a, b, runs_of(w));
        }
        assert(delta_runs_at(vs, a, runs_of(w)).map_values(|x: DeltaRun| Self::shift_spans(x)) =~= delta_units_of_runs(
            delta_runs_at(vs, a, runs_of(w)),
        ));
    }

    #[inline(always)]
    pub(super) fn shift_nth_impl(&mut self, n: usize) -> (r: Option<DeltaRun>)
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
        let ghost (o, a) = (self.shift_items(), self.shift_at());
        let mut i: usize = 0;
        loop
            invariant
                self.shift_vals() == old(self).shift_vals(),
                o == old(self).shift_items(),
                a == old(self).shift_at(),
                i <= n,
                i <= o.len(),
                self.shift_items() == o.skip(i as int),
                self.shift_at() == a + concat(o.take(i as int).map_values(|x: DeltaRun| Self::shift_spans(x))).len(),
            decreases n - i,
        {
            let r = self.take_run();
            match r {
                Some(x) => {
                    proof {
                        let m = o.take(i as int).map_values(|x: DeltaRun| Self::shift_spans(x));
                        let m1 = o.take(i + 1).map_values(|x: DeltaRun| Self::shift_spans(x));
                        assert(m1.drop_last() =~= m);
                    }
                    if i == n {
                        return Some(x);
                    }
                    i = i + 1;
                },
                None => {
                    return None;
                },
            }
        }
    }

    #[inline(always)]
    pub(super) fn seek_to_impl(&mut self, at: usize) -> (r: Option<DeltaRun>)
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
        let p = Shiftable::get_pos(&self.0);
        if at < p {
            return None;
        }
        // the runs' own move, whose model is said of the runs
        Shiftable::advance_to(self, at);
        self.take_run()
    }

    #[inline(always)]
    pub(super) fn shift_next_impl(&mut self, range: Range<usize>) -> (r: Option<DeltaRun>)
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
        Shiftable::shift(self, range);
        self.take_run()
    }
}

impl<'a, T: DeltaValue<C>, C: Codec> DeltaRuns<'a, T, C> {
    pub(super) proof fn lemma_shift_model_impl(&self)
        ensures
            self.shift_vals() == delta_units(self.iter().column()),
            self.shift_at() == self.iter().pos(),
            self.shift_items() == delta_runs_at(
                self.iter().column(),
                self.iter().pos() as int,
                runs_of(window(deltas(self.iter().column()), self.iter().pos() as int, self.iter().end_pos() as int)),
            ),
            self.shift_left() == window(
                delta_units(self.iter().column()),
                self.iter().pos() as int,
                self.iter().end_pos() as int,
            ),
            !Self::shift_unit(),
    {
    }

    #[inline(always)]
    pub(super) fn unshift_impl(self) -> (r: Unshift<Self>)
        ensures
            r.unshift_vals() == self.shift_vals(),
            r.unshift_left() == self.shift_items(),
    {
        Unshift::new(self)
    }

    #[inline(always)]
    pub(super) fn ranges_impl<'r>(self, ranges: &'r [Range<usize>]) -> (r: Ranges<'r, Self>)
        ensures
            r.ranges_vals() == self.shift_vals(),
            r.ranges_left() == windows(self.shift_vals(), ranges@),
    {
        Ranges::new(self, ranges)
    }
}

} // verus!

impl<T: DeltaValue<C>, C: Codec> std::fmt::Debug for DeltaRuns<'_, T, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DeltaRuns").field(&self.0).finish()
    }
}

impl<T: DeltaValue<C>, C: Codec> std::fmt::Debug for DeltaIter<'_, T, C> {
    /// Where the iterator stands, the sum it keeps and how many values
    /// it has left, as `DeltaIter` prints itself.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaIter")
            .field("pos", &self.it.pos)
            .field("running", &self.running)
            .field("items_left", &self.len())
            .finish()
    }
}
