//! The iterator of a delta column: the column's values, each the sum
//! of the differences the column keeps up to it.
//!
//! [`iter`](VerifiedDeltaColumn::iter) and
//! [`iter_range`](VerifiedDeltaColumn::iter_range) of a
//! [`VerifiedDeltaColumn`] give a [`VerifiedDeltaIter`]. It moves as a
//! [`VerifiedIter`](crate::verified::VerifiedIter) does and says the
//! same of each move: what it has left (`rest`) is the column's values
//! from where it stands ([`pos`](VerifiedDeltaIter::pos)) to where its
//! window ends ([`end_pos`](VerifiedDeltaIter::end_pos)), as far as the
//! column goes.
//!
//! It keeps as it goes the sum of the differences before it, so that
//! the next value is one addition away. The moves are
//! [`DeltaIter`](crate::DeltaIter)'s, the shared ones of
//! `delta/diter.rs` (`d_next`, `d_step`): a long move asks the column's
//! index for the slab to go to and for the sum of the differences
//! passed over, and within a slab a whole run is passed over at once.
//!
//! What is proved of every value the iterator gives is that it is the
//! column's value at that place. That it is one of the type's is the
//! column's invariant, which the iterator keeps with the borrow of the
//! column: no value is checked as it is read.

#[cfg(verus_keep_ghost)]
use super::diffs::{
    delta_run_units, delta_units, delta_units_of_runs, lemma_delta_runs_units, lemma_delta_take,
    lemma_deltas_kept, lemma_vals_fit,
};
#[cfg(verus_keep_ghost)]
use super::{
    all_dom, delta_at, delta_prefix, delta_run_fits, delta_run_values, delta_runs_at, delta_within,
    deltas, in_dom, lemma_bound, lemma_delta_run_at, vals,
};
use super::{bounds_of, DCore, DIt, DKind, VDeltaValue, VerifiedDeltaColumn, DE, DX};
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
use crate::verified::codec::VCodec;
#[cfg(verus_keep_ghost)]
use crate::verified::shift::{lemma_run_units, lemma_singles};
#[cfg(verus_keep_ghost)]
use crate::verified::{head_run, lemma_runs_of, runs_of, sorted};
use crate::verified::{invalid_resume, VerifiedIterState};
use crate::verified::{VShiftable, VerifiedRanges, VerifiedUnshift};
#[cfg(verus_keep_ghost)]
use crate::window::windows;
use crate::{DeltaRun, Leb128, PackError};
use std::ops::{Range, RangeBounds};
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use vstd::std_specs::range::RangeBoundsSpec;

verus! {

/// How the values of the column underneath are differences.
type DB<T, C> = <T as DKind<C>>::B;

/// What a column of slabs `s` keeps at `i` is what a reader of its
/// differences gives there: the sum of those up to it.
proof fn lemma_kept_at<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(s: Seq<Slab<X::Tail>>, i: int)
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
pub(super) open spec fn kept_in(d: Option<i64>, lo: i64, hi: i64) -> bool {
    match d {
        Some(n) => lo <= n <= hi,
        None => false,
    }
}

/// What a column of slabs `s` keeps at `i` is what the searches of its
/// differences say is there (`rv`): the same number, or a null.
proof fn lemma_kept_rv<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(s: Seq<Slab<X::Tail>>, i: int)
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
pub(super) proof fn lemma_kept_hit<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
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


/// An iterator over the values of a [`VerifiedDeltaColumn`], or over
/// those in a range of it.
///
/// It stands at an index of the column ([`pos`](Self::pos)) and has a
/// window that ends at another ([`end_pos`](Self::end_pos)): what it has
/// left to give (`rest`) is the column's values from the one to the
/// other, as far as the column goes. Each method says what
/// it gives, what is left after it, and where the iterator then stands.
pub struct VerifiedDeltaIter<'a, T: VDeltaValue<C>, C: VCodec = Leb128> {
    it: ICore<<DX<T, C> as Decoders<'a>>::D>,
    slabs: &'a [Slab<<DE<T, C> as Slabs>::Tail>],
    /// The sum of the differences before the iterator, wrapping as an
    /// `i64` does.
    running: i64,
    /// The column's index, for long moves; a default iterator has none.
    ix: Option<&'a SlabBTree<SlabAgg>>,
    /// The column the slabs are of; a default iterator has none.
    col: Option<&'a DCore<T, C>>,
}

impl<'a, T: VDeltaValue<C>, C: VCodec> VerifiedDeltaIter<'a, T, C> {
    /// The iterator stands in the slabs of its column, in the column or
    /// at its end, with the sum of the differences before it; and
    /// every value the column keeps is one of the type's.
    #[verifier::type_invariant]
    spec fn inv(&self) -> bool {
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
    spec fn kept(&self) -> Seq<Option<i64>> {
        rz(0, dslabs::<DX<T, C>, DB<T, C>>(self.slabs@))
    }

    /// The values of the column the iterator reads: all of them,
    /// whatever the iterator's window.
    pub closed spec fn column(&self) -> Seq<T> {
        vals::<T>(self.kept())
    }

    /// The values the iterator has left to give: the column's from
    /// where it stands to where its window ends, as far as the column
    /// goes.
    pub open spec fn rest(&self) -> Seq<T> {
        window(self.column(), self.pos() as int, self.end_pos() as int)
    }

    /// The column the iterator reads, as it was when the iterator was
    /// made of it; a default iterator reads none. No method of the
    /// iterator changes it.
    pub closed spec fn source(&self) -> Option<VerifiedDeltaColumn<T, C>> {
        match self.col {
            Some(c) => Some(VerifiedDeltaColumn { core: *c }),
            None => None,
        }
    }

    /// [`pos`](Self::pos), as a specification sees it: there it is
    /// written `pos()`.
    pub closed spec fn spec_pos(&self) -> usize {
        self.it.pos
    }

    /// [`end_pos`](Self::end_pos), as a specification sees it: there it
    /// is written `end_pos()`.
    pub closed spec fn spec_end_pos(&self) -> usize {
        (self.it.pos + self.it.items_left) as usize
    }

    /// What the iterator reads is what its column holds: of `source`
    /// and `column`, the first tells the second.
    pub proof fn lemma_source(tracked &self)
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
    proof fn lemma_window(&self)
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

    /// The index in the column of the value `next` gives, if it gives
    /// one.
    ///
    /// What the iterator has left is the column's values from here up to
    /// [`end_pos`](Self::end_pos), as far as the column goes. The
    /// iterator stands in the column or at its end.
    #[verifier::when_used_as_spec(spec_pos)]
    #[inline]
    pub fn pos(&self) -> (r: usize)
        ensures
            r == self.pos(),
            self.pos() <= self.end_pos(),
            self.pos() <= self.column().len(),
            self.rest() == window(self.column(), self.pos() as int, self.end_pos() as int),
    {
        proof {
            use_type_invariant(self);
            self.lemma_window();
        }
        self.it.pos
    }

    /// Where the iterator's window ends: the index in the column that
    /// its last value comes before. It can lie past the column's end
    /// ([`set_max`](Self::set_max)), and the iterator then gives what the
    /// column has. Once the iterator has given all it has, its window
    /// ends where it stands.
    #[verifier::when_used_as_spec(spec_end_pos)]
    #[inline]
    pub fn end_pos(&self) -> (r: usize)
        ensures
            r == self.end_pos(),
            self.pos() <= self.end_pos(),
            self.pos() <= self.column().len(),
            self.rest() == window(self.column(), self.pos() as int, self.end_pos() as int),
    {
        proof {
            use_type_invariant(self);
            self.lemma_window();
        }
        self.it.pos + self.it.items_left
    }

    /// Make the window end at `end`: the iterator then gives the
    /// column's values from where it stands up to `end`, and none if it
    /// stands at `end` or past it.
    ///
    /// This can widen the window as well as narrow it. With an `end`
    /// past the column's, the iterator gives what the column has.
    pub fn set_max(&mut self, end: usize)
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
    fn step(&mut self, n: usize) -> (r: Option<Option<i64>>)
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
        *self = VerifiedDeltaIter { it, slabs: self.slabs, running, ix: self.ix, col: self.col };
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

    /// Move on so that the value at index `target` of the column comes
    /// next. The iterator moves only forwards: at `target` or past it
    /// already, it stays. With a `target` past what it has left, it
    /// gives all it has, and stands at the end of that with nothing left.
    ///
    /// Few of the values passed over are looked at
    /// ([`nth`](Self::nth)).
    pub fn advance_to(&mut self, target: usize)
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

    /// Move `amount` values on: past as many as the iterator has left,
    /// if it has fewer.
    pub fn advance_by(&mut self, amount: usize)
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

    /// Move the window to `range`: the iterator then gives the column's
    /// values there. It moves only forwards: if `range` starts before
    /// the iterator, the window starts where the iterator stands.
    ///
    /// If `range` starts past its own end or the column's, the iterator
    /// has nothing left, and stands where `range` ends or the column
    /// does, whichever comes first, if that is ahead of it.
    pub fn shift(&mut self, range: Range<usize>)
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

    /// Move the window to `range` and give its first value, if it has
    /// one. `range` is to start at the iterator or after it.
    ///
    /// If the window has no value, the iterator has nothing left, and
    /// stands where [`shift`](Self::shift) leaves it then.
    #[allow(clippy::manual_map)]
    pub fn shift_next(&mut self, range: Range<usize>) -> (r: Option<T>)
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

    /// The next run of the differences the column keeps: how much the
    /// value the iterator gives next differs from the number before it
    /// (`delta`), how many values in a row differ by that much each from
    /// the one before (`count`), and the number before the first of
    /// them (`prefix`). A run of nulls has no difference. The iterator
    /// moves past the whole run.
    ///
    /// The run says which values the iterator moved past
    /// (`delta_run_values`), and they are the type's
    /// (`delta_run_fits`): it is what
    /// [`splice_runs`](VerifiedDeltaColumn::splice_runs) takes.
    ///
    /// A run is as long as the differences make it, up to the window's
    /// end: how the column lies in memory does not cut it short.
    pub fn next_run(&mut self) -> (r: Option<DeltaRun>)
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
        *self = VerifiedDeltaIter { it, slabs: self.slabs, running, ix: self.ix, col: self.col };
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

    /// An iterator of the runs of differences the iterator has left to
    /// give ([`next_run`](Self::next_run)): what
    /// [`splice_runs`](VerifiedDeltaColumn::splice_runs) takes.
    pub fn runs(self) -> (r: VerifiedDeltaRuns<'a, T, C>)
        ensures
            r.iter() == self,
    {
        VerifiedDeltaRuns(self)
    }

    /// Where the iterator stands and where its window ends, to keep
    /// without the borrow of the column:
    /// [`VerifiedDeltaIterState::try_resume`] gives an iterator from it.
    ///
    /// The state fits the column the iterator reads: resumed on that
    /// column, as it is, it gives an iterator that stands where this one
    /// does and has the same values left.
    pub fn suspend(&self) -> (s: VerifiedDeltaIterState)
        ensures
            s.pos() == self.pos(),
            s.end_pos() == self.end_pos(),
            self.rest() == window(self.column(), s.pos() as int, s.end_pos() as int),
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
        VerifiedDeltaIterState {
            inner: VerifiedIterState { counter, num_slabs: self.slabs.len(), slab_idx, pos, items_left, slab_consumed },
        }
    }

    /// What the iterator has left, value by value: each is what the
    /// column keeps at its place, as the type's value.
    proof fn lemma_rest(&self)
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
    fn scan(&mut self, lo: i64, hi: i64) -> (r: Option<(usize, i64)>)
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
        *self = VerifiedDeltaIter { it, slabs: self.slabs, running, ix: self.ix, col: self.col };
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
    fn scan_null(&mut self) -> (r: Option<usize>)
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
        *self = VerifiedDeltaIter { it, slabs: self.slabs, running, ix: self.ix, col: self.col };
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

    /// Move on to the first of the values left that is `target`, and
    /// past it: its index in the column is given. If none of them is
    /// `target`, the iterator gives all it has, as
    /// [`advance_to`](Self::advance_to) leaves it then.
    ///
    /// The values need not be in order. Few of them are looked at where
    /// the column's index can tell that a part of the column holds no
    /// `target`; a search for the null reads the values a run at a
    /// time.
    #[allow(clippy::manual_map)]  // a match is what the proof follows
    pub fn scan_to_value(&mut self, target: T) -> (r: Option<usize>)
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

    /// Move on to the first of the values left that is within the
    /// bounds of `range`, and past it: given are its index in the
    /// column and the value. If none of them is, the iterator gives
    /// all it has, as [`advance_to`](Self::advance_to) leaves it then.
    ///
    /// The bounds are in the order of the type's values
    /// (`VSpec::below`), and a null is within no bounds
    /// (`delta_within`). A bound can be any value of the type, also
    /// one that no delta column holds: a null as a bound comes before
    /// every number, and a number that does not fit comes after every
    /// value a column holds.
    ///
    /// The values need not be in order. Few of them are looked at where
    /// the column's index can tell that a part of the column holds
    /// none within the bounds.
    pub fn scan_to_range<R: RangeBounds<T>>(&mut self, range: R) -> (r: Option<(usize, T)>)
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
    proof fn lemma_sought(&self, target: T, a: int, b: int)
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
    fn seek_rest(&mut self, target: T) -> (r: (usize, usize))
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
                *self = VerifiedDeltaIter { it, slabs: self.slabs, running, ix: self.ix, col: self.col };
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

    /// Search the values of the column in `range` for `target`, where
    /// they are in order, and move on to it: given is the range of the
    /// places that hold it, and the iterator gives the first of them
    /// next. If no value there is `target`, the range is the empty one
    /// at the place where it would stand, and the iterator stands
    /// there.
    ///
    /// The search is over `range` as far as that lies ahead of the
    /// iterator and in the column. The window is made to end where
    /// `range` ends ([`set_max`](Self::set_max)), whether that is
    /// before its end or after it, and the iterator moves on to where
    /// `range` starts ([`advance_to`](Self::advance_to)); a `range`
    /// that starts past its end is taken as the empty one at its
    /// start. `0..usize::MAX` is all the column has ahead of the
    /// iterator.
    ///
    /// Few of the values are looked at: the column's index tells the
    /// part of the column to look in. The values searched are taken to
    /// be in order (`sorted`); where they are not, the range is some
    /// range of the places searched. A `target` that does not fit comes
    /// after every value of a column: the iterator then gives all it
    /// has, and its window ends where it stands.
    pub fn seek_to_value(&mut self, target: T, range: Range<usize>) -> (r: Range<usize>)
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
                        &&& (k < r.end) == !T::below(target, c[k])
                        &&& (r.start <= k < r.end) == (c[k] == target)
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
proof fn lemma_found_in_column<T: VDeltaValue<C>, C: VCodec>(c: Seq<T>, here: int, t: int, r0: int, r1: int, target: T)
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

#[cfg(verus_keep_ghost)]
impl<'a, T: VDeltaValue<C>, C: VCodec> vstd::std_specs::iter::IteratorSpecImpl for VerifiedDeltaIter<'a, T, C> {
    /// The iterator keeps to what Verus asks of an iterator: `next` gives
    /// `remaining()`, one value after another, and then `None`.
    open spec fn obeys_prophetic_iter_laws(&self) -> bool {
        true
    }

    open spec fn remaining(&self) -> Seq<T> {
        self.rest()
    }

    open spec fn will_return_none(&self) -> bool {
        true
    }

    open spec fn decrease(&self) -> Option<nat> {
        Some(self.rest().len())
    }

    open spec fn peek(&self, index: int) -> Option<T> {
        if 0 <= index < self.rest().len() {
            Some(self.rest()[index])
        } else {
            None
        }
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> Iterator for VerifiedDeltaIter<'a, T, C> {
    type Item = T;

    /// The next value, if one is left.
    #[inline]
    fn next(&mut self) -> (r: Option<T>)
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
        let r = d_next::<DX<T, C>, DB<T, C>>(&mut it, self.slabs, &mut running);
        *self = VerifiedDeltaIter { it, slabs: self.slabs, running, ix: self.ix, col: self.col };
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

    /// The `n`th of the values left, counted from 0, if there are that
    /// many: it and the values before it are then given. If there are
    /// not, none is left.
    ///
    /// Few of the values passed over are looked at: a long move asks the
    /// column's index for the slab to go to and for the sum of the
    /// differences passed over, and within a slab a whole run is passed
    /// over at once.
    #[inline]
    #[allow(clippy::manual_map)]
    fn nth(&mut self, n: usize) -> (r: Option<T>)
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

    /// How many values are left: exactly.
    #[inline]
    fn size_hint(&self) -> (r: (usize, Option<usize>))
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

#[cfg(verus_keep_ghost)]
impl<'a, T: VDeltaValue<C>, C: VCodec> vstd::std_specs::iter::ExactSizeIteratorSpecImpl for VerifiedDeltaIter<'a, T, C> {
    open spec fn exact_len(&self) -> usize {
        self.rest().len() as usize
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> ExactSizeIterator for VerifiedDeltaIter<'a, T, C> {
    /// How many values are left.
    #[inline]
    fn len(&self) -> (len: usize)
        ensures
            len == self.rest().len(),
    {
        self.size_hint().0
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> Clone for VerifiedDeltaIter<'a, T, C> {
    /// An iterator of its own with the same values left.
    fn clone(&self) -> (r: Self)
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
        VerifiedDeltaIter {
            it: self.it.dup(),
            slabs: self.slabs,
            running: self.running,
            ix: self.ix,
            col: self.col,
        }
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> Default for VerifiedDeltaIter<'a, T, C> {
    /// An iterator with nothing to give.
    fn default() -> (r: Self)
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
        VerifiedDeltaIter { it, slabs, running: 0, ix: None, col: None }
    }
}

impl<T: VDeltaValue<C>, C: VCodec> VerifiedDeltaColumn<T, C> {
    /// An iterator over the values.
    pub fn iter(&self) -> (it: VerifiedDeltaIter<'_, T, C>)
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
        let r = VerifiedDeltaIter { it, slabs, running: 0, ix: Some(ix), col: Some(&self.core) };
        proof {
            r.lemma_window();
            assert(r.rest() =~= self@);
        }
        r
    }

    /// An iterator over the values `start..end`.
    ///
    /// The iterator moves from the column's start to `start` as
    /// [`advance_by`](VerifiedDeltaIter::advance_by) does: the column's
    /// index gives the sum of the differences before `start`.
    pub fn iter_range(&self, start: usize, end: usize) -> (it: VerifiedDeltaIter<'_, T, C>)
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

    /// The range of the places in `start..end` that hold `value`, where
    /// the values there are in order: the first such place and the
    /// place after the last. If no value there is `value`, the range is
    /// the empty one at the place where it would stand.
    ///
    /// Few of the values are looked at: the column's index tells the
    /// part of the column to look in. The values of `start..end` are
    /// taken to be in order (`sorted`); where they are not, the range is
    /// some range of the places in `start..end`. A `value` that does not
    /// fit comes after every value of a column.
    pub fn scope_to_value(&self, value: T, start: usize, end: usize) -> (r: Range<usize>)
        requires
            start <= end <= self@.len(),
        ensures
            start <= r.start <= r.end <= end,
            sorted::<T>(self@.subrange(start as int, end as int)) ==> forall|k: int|
                start <= k < end ==> {
                    &&& (k < r.start) == T::below(#[trigger] self@[k], value)
                    &&& (k < r.end) == !T::below(value, self@[k])
                    &&& (r.start <= k < r.end) == (self@[k] == value)
                },
    {
        let mut it = self.iter();
        it.seek_to_value(value, start..end)
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> IntoIterator for &'a VerifiedDeltaColumn<T, C> {
    type Item = T;

    type IntoIter = VerifiedDeltaIter<'a, T, C>;

    /// An iterator over the values ([`VerifiedDeltaColumn::iter`]).
    #[inline(always)]
    fn into_iter(self) -> (it: VerifiedDeltaIter<'a, T, C>)
        ensures
            it.rest() == self@,
            it.column() == self@,
            it.source() == Some(*self),
            it.pos() == 0,
            it.end_pos() == self@.len(),
    {
        self.iter()
    }
}

/// An iterator of the runs of differences of a delta column's values,
/// which [`VerifiedDeltaIter::runs`] gives: each run is of as many
/// equal differences as lie together in what the iterator had left
/// (`delta_runs_at` of their `runs_of`), with the number before it.
pub struct VerifiedDeltaRuns<'a, T: VDeltaValue<C>, C: VCodec = Leb128>(VerifiedDeltaIter<'a, T, C>);

impl<'a, T: VDeltaValue<C>, C: VCodec> VerifiedDeltaRuns<'a, T, C> {
    /// The iterator of values whose runs these are, where it stands now.
    pub closed spec fn iter(&self) -> VerifiedDeltaIter<'a, T, C> {
        self.0
    }
}

#[cfg(verus_keep_ghost)]
impl<'a, T: VDeltaValue<C>, C: VCodec> vstd::std_specs::iter::IteratorSpecImpl for VerifiedDeltaRuns<'a, T, C> {
    /// The iterator keeps to what Verus asks of an iterator: `next` gives
    /// `remaining()`, one run after another, and then `None`.
    open spec fn obeys_prophetic_iter_laws(&self) -> bool {
        true
    }

    open spec fn remaining(&self) -> Seq<DeltaRun> {
        delta_runs_at(
            self.iter().column(),
            self.iter().pos() as int,
            runs_of(window(deltas(self.iter().column()), self.iter().pos() as int, self.iter().end_pos() as int)),
        )
    }

    open spec fn will_return_none(&self) -> bool {
        true
    }

    open spec fn decrease(&self) -> Option<nat> {
        Some(self.iter().rest().len())
    }

    open spec fn peek(&self, index: int) -> Option<DeltaRun> {
        let rs = delta_runs_at(
            self.iter().column(),
            self.iter().pos() as int,
            runs_of(window(deltas(self.iter().column()), self.iter().pos() as int, self.iter().end_pos() as int)),
        );
        if 0 <= index < rs.len() {
            Some(rs[index])
        } else {
            None
        }
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> Iterator for VerifiedDeltaRuns<'a, T, C> {
    type Item = DeltaRun;

    /// The next run, if one is left ([`VerifiedDeltaIter::next_run`]).
    #[inline(always)]
    fn next(&mut self) -> (r: Option<DeltaRun>)
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

/// Where a [`VerifiedDeltaIter`] stood and where its window ended, kept
/// without the borrow of the column: [`VerifiedDeltaIter::suspend`]
/// gives it, and [`try_resume`](Self::try_resume) an iterator from it.
///
/// It holds the iterator's place in the column's slabs, and how many
/// slabs and edits the column counted then, as a
/// [`VerifiedIterState`] does. It does not hold the sum of the
/// differences before the place: `try_resume` takes that from the
/// column it resumes on. It is of no value type: a state fits any delta
/// column whose slabs it is a place in.
pub struct VerifiedDeltaIterState {
    inner: VerifiedIterState,
}

impl VerifiedDeltaIterState {
    /// Where the iterator stood: its `pos()`.
    pub closed spec fn pos(&self) -> usize {
        self.inner.pos
    }

    /// Where the iterator's window ended: its `end_pos()`.
    pub closed spec fn end_pos(&self) -> usize {
        (self.inner.pos + self.inner.items_left) as usize
    }

    /// The count of edits of the column the state was taken of
    /// (`VerifiedDeltaColumn::edits`).
    pub closed spec fn edits(&self) -> usize {
        self.inner.counter
    }

    /// The state is a place in `column` as it is: the column counts the
    /// edits and the slabs that the state does, the state's place is one
    /// in those slabs, and it is in the column or at its end.
    /// [`try_resume`](Self::try_resume) gives an iterator exactly then.
    pub closed spec fn fits<T: VDeltaValue<C>, C: VCodec>(&self, column: &VerifiedDeltaColumn<T, C>) -> bool {
        let lens = slab_lens(column.core.slabs_spec());
        &&& self.inner.counter == column.core.count()
        &&& self.inner.num_slabs == lens.len()
        &&& resumes(lens, self.inner.slab_idx, self.inner.slab_consumed, self.inner.pos, self.inner.items_left)
        &&& self.inner.pos <= total(lens)
    }

    /// A state fits only a column that counts the edits it does. A
    /// column that has been edited since counts others
    /// (`lemma_after_edits`), so the state does not fit it, and
    /// [`try_resume`](Self::try_resume) refuses it.
    pub proof fn lemma_fits<T: VDeltaValue<C>, C: VCodec>(&self, column: &VerifiedDeltaColumn<T, C>)
        requires
            self.fits(column),
        ensures
            self.edits() == column.edits(),
    {
    }

    /// An iterator of `column` that stands where the suspended one stood,
    /// with its window, if the state fits the column (`fits`); an error
    /// ([`PackError::InvalidResume`]) if it does not.
    ///
    /// A state fits the column it was taken of, as long as that is not
    /// edited ([`VerifiedDeltaIter::suspend`]). The iterator given is
    /// one of `column` whatever column the state was taken of: it gives
    /// the values of `column` in the state's window, and the sum it
    /// keeps is of that column's differences. Where the state stands at
    /// the column's end, the iterator's window ends there.
    ///
    /// An edit moves the column's count of edits
    /// (`VerifiedDeltaColumn::edits`), as each method that edits says,
    /// so a state taken before an edit is refused after it
    /// (`lemma_fits`), and after more edits unless they are
    /// `usize::MAX + 1` or more (`lemma_after_edits`).
    ///
    /// The sum of the differences before the place is found as
    /// [`advance_by`](VerifiedDeltaIter::advance_by) finds it, with the
    /// column's index.
    pub fn try_resume<'a, T: VDeltaValue<C>, C: VCodec>(&self, column: &'a VerifiedDeltaColumn<T, C>) -> (r: Result<
        VerifiedDeltaIter<'a, T, C>,
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
        let r = VerifiedDeltaIter { it, slabs, running: before.running, ix: Some(ix), col: Some(&column.core) };
        proof {
            r.lemma_window();
            assert(r.rest() =~= window(column@, self.pos() as int, self.end_pos() as int));
        }
        Ok(r)
    }
}


// ── Repositioning (`VShiftable`) ───────────────────────────────────────────


/// The iterator repositioned as any of the verified ones is: its model is
/// its column's values, from where it stands to the end of its window,
/// each value an item.
impl<'a, T: VDeltaValue<C>, C: VCodec> VShiftable for VerifiedDeltaIter<'a, T, C> {
    closed spec fn shift_vals(&self) -> Seq<T> {
        self.column()
    }

    closed spec fn shift_at(&self) -> int {
        self.spec_pos() as int
    }

    closed spec fn shift_end(&self) -> int {
        self.spec_end_pos() as int
    }

    closed spec fn shift_left(&self) -> Seq<T> {
        self.rest()
    }

    closed spec fn shift_items(&self) -> Seq<T> {
        self.rest()
    }

    open spec fn shift_spans(x: T) -> Seq<T> {
        seq![x]
    }

    open spec fn shift_unit() -> bool {
        true
    }

    proof fn lemma_shift(&self) {
        let xs = self.rest();
        lemma_singles(xs);
        assert(xs.map_values(|x: T| Self::shift_spans(x)) =~= xs.map_values(|x: T| seq![x]));
    }

    proof fn lemma_shift_unit(x: T) {
    }

    #[inline(always)]
    fn shift_step(&mut self) -> (r: Option<T>) {
        self.nth(0)
    }

    #[inline(always)]
    fn shift_nth(&mut self, n: usize) -> (r: Option<T>) {
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
    fn dup(&self) -> (r: Self) {
        proof {
            use_type_invariant(self);
        }
        VerifiedDeltaIter { it: self.it.dup(), slabs: self.slabs, running: self.running, ix: self.ix, col: self.col }
    }

    #[inline(always)]
    fn get_pos(&self) -> (r: usize) {
        self.pos()
    }

    #[inline(always)]
    fn get_max(&self) -> (r: usize) {
        self.end_pos()
    }

    #[inline(always)]
    fn set_max(&mut self, end: usize) {
        VerifiedDeltaIter::set_max(self, end)
    }

    #[inline(always)]
    fn advance_to(&mut self, target: usize) {
        VerifiedDeltaIter::advance_to(self, target)
    }

    #[inline(always)]
    fn advance_by(&mut self, amount: usize) {
        VerifiedDeltaIter::advance_by(self, amount)
    }

    #[inline(always)]
    fn seek_to(&mut self, at: usize) -> (r: Option<T>) {
        let p = self.pos();
        if at >= p {
            self.nth(at - p)
        } else {
            None
        }
    }

    #[inline(always)]
    fn shift(&mut self, range: Range<usize>) {
        VerifiedDeltaIter::shift(self, range)
    }

    #[inline(always)]
    fn shift_next(&mut self, range: Range<usize>) -> (r: Option<T>) {
        VerifiedDeltaIter::shift_next(self, range)
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> VerifiedDeltaIter<'a, T, C> {
    /// The iterator's repositioning model ([`VShiftable`]) in the terms
    /// of its own specification: the values it ranges over are its
    /// column's; it stands at `pos()`; what it has left is `rest()`,
    /// each item a value.
    pub proof fn lemma_shift_model(&self)
        ensures
            self.shift_vals() == self.column(),
            self.shift_at() == self.pos(),
            self.shift_items() == self.rest(),
            self.shift_left() == self.rest(),
            Self::shift_unit(),
    {
    }

    /// A lookahead: the iterator wrapped so that the value it gives next
    /// can be looked at first ([`VerifiedUnshift::peek`]), as the old
    /// API's `Shiftable::unshift` gives.
    #[inline(always)]
    pub fn unshift(self) -> (r: VerifiedUnshift<Self>)
        ensures
            r.unshift_vals() == self.column(),
            r.unshift_left() == self.rest(),
    {
        VerifiedUnshift::new(self)
    }

    /// The values of each of `ranges` of the column, one range after
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
            r.ranges_vals() == self.column(),
            r.ranges_left() == windows(self.column(), ranges@),
    {
        VerifiedRanges::new(self, ranges)
    }
}

// ── The runs of a delta column, repositioned ───────────────────────────────


impl<'a, T: VDeltaValue<C>, C: VCodec> VerifiedDeltaRuns<'a, T, C> {
    /// The first run of what is left, and the values it stands for.
    fn take_run(&mut self) -> (r: Option<DeltaRun>)
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

/// The runs of a delta column, repositioned as its values are: the model
/// is each value as a delta run of one, with the number before it, and
/// an item, a run, stands for as many of those as it counts.
impl<'a, T: VDeltaValue<C>, C: VCodec> VShiftable for VerifiedDeltaRuns<'a, T, C> {
    closed spec fn shift_vals(&self) -> Seq<DeltaRun> {
        delta_units(self.0.column())
    }

    closed spec fn shift_at(&self) -> int {
        self.0.spec_pos() as int
    }

    closed spec fn shift_end(&self) -> int {
        self.0.spec_end_pos() as int
    }

    closed spec fn shift_left(&self) -> Seq<DeltaRun> {
        window(delta_units(self.0.column()), self.0.spec_pos() as int, self.0.spec_pos() + self.0.rest().len())
    }

    closed spec fn shift_items(&self) -> Seq<DeltaRun> {
        delta_runs_at(
            self.0.column(),
            self.0.spec_pos() as int,
            runs_of(window(deltas(self.0.column()), self.0.spec_pos() as int, self.0.spec_pos() + self.0.rest().len())),
        )
    }

    open spec fn shift_spans(x: DeltaRun) -> Seq<DeltaRun> {
        delta_run_units(x)
    }

    open spec fn shift_unit() -> bool {
        false
    }

    proof fn lemma_shift(&self) {
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

    proof fn lemma_shift_unit(x: DeltaRun) {
    }

    #[inline(always)]
    fn shift_step(&mut self) -> (r: Option<DeltaRun>) {
        self.take_run()
    }

    #[inline(always)]
    fn shift_nth(&mut self, n: usize) -> (r: Option<DeltaRun>) {
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
    fn dup(&self) -> (r: Self) {
        VerifiedDeltaRuns(VShiftable::dup(&self.0))
    }

    #[inline(always)]
    fn get_pos(&self) -> (r: usize) {
        VShiftable::get_pos(&self.0)
    }

    #[inline(always)]
    fn get_max(&self) -> (r: usize) {
        VShiftable::get_max(&self.0)
    }

    #[inline(always)]
    fn set_max(&mut self, end: usize) {
        VShiftable::set_max(&mut self.0, end)
    }

    #[inline(always)]
    fn advance_to(&mut self, target: usize) {
        VShiftable::advance_to(&mut self.0, target)
    }

    #[inline(always)]
    fn advance_by(&mut self, amount: usize) {
        VShiftable::advance_by(&mut self.0, amount)
    }

    #[inline(always)]
    fn seek_to(&mut self, at: usize) -> (r: Option<DeltaRun>) {
        let p = VShiftable::get_pos(&self.0);
        if at < p {
            return None;
        }
        // the runs' own move, whose model is said of the runs
        VShiftable::advance_to(self, at);
        self.take_run()
    }

    #[inline(always)]
    fn shift(&mut self, range: Range<usize>) {
        VShiftable::shift(&mut self.0, range)
    }

    #[inline(always)]
    fn shift_next(&mut self, range: Range<usize>) -> (r: Option<DeltaRun>) {
        VShiftable::shift(self, range);
        self.take_run()
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> VerifiedDeltaRuns<'a, T, C> {
    /// The runs' repositioning model ([`VShiftable`]) in the terms of
    /// their own specification: they range over the column's values, each
    /// as a delta run of one; they stand where their iterator of values
    /// does; their items are the runs of the differences that has left,
    /// each with the number before it.
    pub proof fn lemma_shift_model(&self)
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

    /// A lookahead: the runs wrapped so that the run they give next can
    /// be looked at first ([`VerifiedUnshift::peek`]), as the old API's
    /// `Shiftable::unshift` gives. (It cannot be moved to a window: its
    /// items are not each one value.)
    #[inline(always)]
    pub fn unshift(self) -> (r: VerifiedUnshift<Self>)
        ensures
            r.unshift_vals() == self.shift_vals(),
            r.unshift_left() == self.shift_items(),
    {
        VerifiedUnshift::new(self)
    }

    /// The runs of each of `ranges` of the column, one range after
    /// another, as the old API's `Shiftable::ranges` gives them: a run
    /// that crosses the edge of a range is cut there, and each says the
    /// number before it. The ranges ascend and do not overlap (they may
    /// touch), and start where the iterator stands or later.
    ///
    /// # Panics
    ///
    /// When it comes to a range that starts before where it stands.
    #[inline(always)]
    pub fn ranges<'r>(self, ranges: &'r [Range<usize>]) -> (r: VerifiedRanges<'r, Self>)
        ensures
            r.ranges_vals() == self.shift_vals(),
            r.ranges_left() == windows(self.shift_vals(), ranges@),
    {
        VerifiedRanges::new(self, ranges)
    }
}

} // verus!

impl<T: VDeltaValue<C>, C: VCodec> std::fmt::Debug for VerifiedDeltaRuns<'_, T, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("VerifiedDeltaRuns").field(&self.0).finish()
    }
}

impl<T: VDeltaValue<C>, C: VCodec> std::fmt::Debug for VerifiedDeltaIter<'_, T, C> {
    /// Where the iterator stands, the sum it keeps and how many values
    /// it has left, as `DeltaIter` prints itself.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedDeltaIter")
            .field("pos", &self.it.pos)
            .field("running", &self.running)
            .field("items_left", &self.len())
            .finish()
    }
}
