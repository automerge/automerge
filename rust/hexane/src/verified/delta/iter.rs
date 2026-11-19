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
use super::{all_dom, in_dom, vals};
use super::{DCore, DKind, VDeltaValue, VerifiedDeltaColumn, DE, DX};
use crate::btree::{SlabAgg, SlabBTree};
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::slab_lens;
use crate::cursor::Slabs;
#[cfg(verus_keep_ghost)]
use crate::delta::agg::{psum, DeltaOps};
#[cfg(verus_keep_ghost)]
use crate::delta::ddomain::{dslabs, lemma_dslabs_own};
#[cfg(verus_keep_ghost)]
use crate::delta::dedit::rz;
#[cfg(verus_keep_ghost)]
use crate::delta::diter::{at_run, dbefore, dindex_ok, dval_at, lemma_crop, lemma_realized_window};
use crate::delta::diter::{d_next, d_step};
#[cfg(verus_keep_ghost)]
use crate::fenwick::total;
#[cfg(verus_keep_ghost)]
use crate::iter_core::{lemma_window_past, past, vals_of, window, Decode};
use crate::iter_core::{Decoders, ICore};
use crate::verified::codec::VCodec;
use crate::Leb128;
use std::ops::Range;
use vstd::prelude::*;

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

} // verus!

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
