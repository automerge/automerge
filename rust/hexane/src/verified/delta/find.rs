//! The finds of a delta column: the places of the values within some
//! bounds, found by the column's index.
//!
//! [`find_by_range`](VerifiedDeltaColumn::find_by_range) and
//! [`find_by_value`](VerifiedDeltaColumn::find_by_value) of a
//! [`VerifiedDeltaColumn`] give a [`VerifiedFindByRange`]: an iterator
//! over the places of the column whose values are within the bounds,
//! from the first on. [`find_first`](VerifiedDeltaColumn::find_first)
//! gives the first place of a value. The values need be in no order.
//!
//! The search is [`FindByRange`](crate::FindByRange)'s, the shared one
//! of `delta/dfind.rs` (`d_find_new`, `d_find_next`): the column's index
//! keeps the least and the greatest value of each slab, so a slab that
//! can hold no value within the bounds is passed over whole, and the
//! others are read through.
//!
//! What is proved is that the places a finder gives are those of
//! `delta_finds`, one after another: every place of the column whose
//! value is within the bounds, each once, in order
//! (`lemma_delta_finds`, `lemma_delta_finds_first`).

#[cfg(verus_keep_ghost)]
use super::iter::{kept_in, lemma_kept_hit};
use super::{bounds_of, DKind, VDeltaValue, VerifiedDeltaColumn, DE, DX};
#[cfg(verus_keep_ghost)]
use super::{delta_below, delta_within, in_dom, lemma_bound, vals, VDelta};
#[cfg(verus_keep_ghost)]
use crate::btree::in_i64;
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::slab_lens;
use crate::cursor::Slabs;
#[cfg(verus_keep_ghost)]
use crate::delta::ddomain::{dslabs, lemma_dslabs_own};
#[cfg(verus_keep_ghost)]
use crate::delta::dedit::rz;
#[cfg(verus_keep_ghost)]
use crate::delta::dfind::{col_hit, find_ok};
use crate::delta::dfind::{d_find_new, d_find_next, DFind};
#[cfg(verus_keep_ghost)]
use crate::delta::dseek::{all_fit, lemma_sums_fit, rsum};
#[cfg(verus_keep_ghost)]
use crate::fenwick::total;
#[cfg(verus_keep_ghost)]
use crate::iter_core::vals_of;
use crate::iter_core::Decoders;
use crate::verified::codec::VCodec;
use crate::Leb128;
use std::ops::{Bound, RangeBounds};
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use vstd::std_specs::range::RangeBoundsSpec;

verus! {

/// How the values of the column underneath are differences.
type DB<T, C> = <T as DKind<C>>::B;

/// The places of the values `vs`, from `from` on, whose values are
/// within the bounds `lo` and `hi` ([`delta_within`]), in order: what a
/// find over the bounds gives ([`VerifiedDeltaColumn::find_by_range`]).
/// [`lemma_delta_finds`] says which places they are, and
/// [`lemma_delta_finds_first`] which is the first.
pub open spec fn delta_finds<T: VDelta>(vs: Seq<T>, lo: Bound<&T>, hi: Bound<&T>, from: int) -> Seq<usize>
    decreases vs.len() - from,
{
    if from < 0 || from >= vs.len() {
        Seq::empty()
    } else if delta_within::<T>(lo, hi, vs[from]) {
        seq![from as usize] + delta_finds::<T>(vs, lo, hi, from + 1)
    } else {
        delta_finds::<T>(vs, lo, hi, from + 1)
    }
}

/// The finds of values `vs` from `from` on are places from `from` on
/// whose values are within the bounds; they come in order, so none
/// comes twice; and every such place is one of them.
pub proof fn lemma_delta_finds<T: VDelta>(vs: Seq<T>, lo: Bound<&T>, hi: Bound<&T>, from: int)
    requires
        0 <= from,
        vs.len() <= usize::MAX,
    ensures
        forall|k: int|
            0 <= k < delta_finds::<T>(vs, lo, hi, from).len() ==> {
                let i = #[trigger] delta_finds::<T>(vs, lo, hi, from)[k];
                from <= i < vs.len() && delta_within::<T>(lo, hi, vs[i as int])
            },
        forall|j: int, k: int|
            0 <= j < k < delta_finds::<T>(vs, lo, hi, from).len() ==> #[trigger] delta_finds::<T>(vs, lo, hi, from)[j]
                < #[trigger] delta_finds::<T>(vs, lo, hi, from)[k],
        forall|i: int|
            from <= i < vs.len() && delta_within::<T>(lo, hi, #[trigger] vs[i]) ==> delta_finds::<T>(
                vs,
                lo,
                hi,
                from,
            ).contains(i as usize),
    decreases vs.len() - from,
{
    let fs = delta_finds::<T>(vs, lo, hi, from);
    if from < vs.len() {
        let rest = delta_finds::<T>(vs, lo, hi, from + 1);
        lemma_delta_finds::<T>(vs, lo, hi, from + 1);
        if delta_within::<T>(lo, hi, vs[from]) {
            // `from`, then the finds after it
            assert(fs == seq![from as usize] + rest);
            assert(fs[0] == from as usize);
            assert forall|k: int| 0 <= k < fs.len() implies ({
                let i = #[trigger] fs[k];
                from <= i < vs.len() && delta_within::<T>(lo, hi, vs[i as int])
            }) by {
                if k > 0 {
                    assert(fs[k] == rest[k - 1]);
                }
            }
            assert forall|j: int, k: int| 0 <= j < k < fs.len() implies #[trigger] fs[j] < #[trigger] fs[k] by {
                assert(fs[k] == rest[k - 1]);
                if j > 0 {
                    assert(fs[j] == rest[j - 1]);
                }
            }
            assert forall|i: int| from <= i < vs.len() && delta_within::<T>(lo, hi, #[trigger] vs[i]) implies fs.contains(
                i as usize,
            ) by {
                if i == from {
                    assert(fs[0] == i as usize);
                } else {
                    assert(rest.contains(i as usize));
                    let w = choose|w: int| 0 <= w < rest.len() && rest[w] == i as usize;
                    assert(fs[w + 1] == i as usize);
                }
            }
        } else {
            assert(fs == rest);
        }
    }
}

/// The first of the finds of values `vs` from `from` on is the first
/// place from `from` on whose value is within the bounds, and the finds
/// after it are those from the next place on; and where there is no
/// find, no value from `from` on is within the bounds.
pub proof fn lemma_delta_finds_first<T: VDelta>(vs: Seq<T>, lo: Bound<&T>, hi: Bound<&T>, from: int)
    requires
        0 <= from,
        vs.len() <= usize::MAX,
    ensures
        ({
            let fs = delta_finds::<T>(vs, lo, hi, from);
            if fs.len() > 0 {
                let i = fs[0] as int;
                &&& from <= i < vs.len()
                &&& delta_within::<T>(lo, hi, vs[i])
                &&& forall|j: int| from <= j < i ==> !delta_within::<T>(lo, hi, #[trigger] vs[j])
                &&& fs.drop_first() == delta_finds::<T>(vs, lo, hi, i + 1)
            } else {
                forall|j: int| from <= j < vs.len() ==> !delta_within::<T>(lo, hi, #[trigger] vs[j])
            }
        }),
    decreases vs.len() - from,
{
    let fs = delta_finds::<T>(vs, lo, hi, from);
    if from < vs.len() {
        let rest = delta_finds::<T>(vs, lo, hi, from + 1);
        if delta_within::<T>(lo, hi, vs[from]) {
            assert(fs == seq![from as usize] + rest);
            assert(fs.drop_first() =~= rest);
        } else {
            lemma_delta_finds_first::<T>(vs, lo, hi, from + 1);
            assert(fs == rest);
        }
    }
}

/// A value `v` that a delta column can hold is within the bounds that
/// include `t` and nothing else exactly if it is `t`, and no null.
pub proof fn lemma_delta_within_one<T: VDelta>(t: T, v: T)
    requires
        v.fits(),
    ensures
        delta_within::<T>(Bound::Included(&t), Bound::Included(&t), v) == (v == t && v.dv() is Some),
{
    T::lemma_delta(v, None);
    assert(in_dom::<T>(v.dv()));
    lemma_bound::<T>(t, v.dv());
    if t.fits() {
        // neither before the other: kept as the same
        assert(!delta_below(v.dv(), t.dv()) && !delta_below(t.dv(), v.dv()) && v.dv() is Some ==> v.dv() == t.dv());
    }
}

/// The places of `ds`, what a column keeps, from `from` on, that hold a
/// number from `lo` to `hi`, in order.
spec fn hits_from(ds: Seq<Option<i64>>, lo: i64, hi: i64, from: int) -> Seq<usize>
    decreases ds.len() - from,
{
    if from < 0 || from >= ds.len() {
        Seq::empty()
    } else if kept_in(ds[from], lo, hi) {
        seq![from as usize] + hits_from(ds, lo, hi, from + 1)
    } else {
        hits_from(ds, lo, hi, from + 1)
    }
}

/// Where no place from `from` on holds a number from `lo` to `hi`,
/// there is none to give.
proof fn lemma_hits_none(ds: Seq<Option<i64>>, lo: i64, hi: i64, from: int)
    requires
        0 <= from,
        forall|j: int| from <= j < ds.len() ==> !kept_in(#[trigger] ds[j], lo, hi),
    ensures
        hits_from(ds, lo, hi, from) == Seq::<usize>::empty(),
    decreases ds.len() - from,
{
    if from < ds.len() {
        assert(!kept_in(ds[from], lo, hi));
        lemma_hits_none(ds, lo, hi, from + 1);
    }
}

/// Where `i` is the first place from `from` on that holds a number from
/// `lo` to `hi`, the places to give are `i` and then those after it.
proof fn lemma_hits_next(ds: Seq<Option<i64>>, lo: i64, hi: i64, from: int, i: int)
    requires
        0 <= from <= i < ds.len(),
        kept_in(ds[i], lo, hi),
        forall|j: int| from <= j < i ==> !kept_in(#[trigger] ds[j], lo, hi),
    ensures
        hits_from(ds, lo, hi, from) == seq![i as usize] + hits_from(ds, lo, hi, i + 1),
    decreases i - from,
{
    if from < i {
        assert(!kept_in(ds[from], lo, hi));
        lemma_hits_next(ds, lo, hi, from + 1, i);
    }
}

/// Where the values within the bounds `lo` and `hi` are those kept as
/// a number from `a` to `b`, the places of the one are the finds of the
/// other.
proof fn lemma_hits_finds<T: VDelta>(ds: Seq<Option<i64>>, lo: Bound<&T>, hi: Bound<&T>, a: i64, b: i64, from: int)
    requires
        forall|i: int| 0 <= i < ds.len() ==> in_dom::<T>(#[trigger] ds[i]),
        forall|d: Option<i64>|
            #![trigger T::of(d)]
            in_dom::<T>(d) ==> delta_within::<T>(lo, hi, T::of(d)) == kept_in(d, a, b),
    ensures
        hits_from(ds, a, b, from) == delta_finds::<T>(vals::<T>(ds), lo, hi, from),
    decreases ds.len() - from,
{
    if 0 <= from < ds.len() {
        lemma_hits_finds::<T>(ds, lo, hi, a, b, from + 1);
        assert(in_dom::<T>(ds[from]));
        assert(vals::<T>(ds)[from] == T::of(ds[from]));
    }
}

/// The places of a [`VerifiedDeltaColumn`] whose values are within some
/// bounds, one after another from the first:
/// [`find_by_range`](VerifiedDeltaColumn::find_by_range) and
/// [`find_by_value`](VerifiedDeltaColumn::find_by_value) give one.
///
/// It is an iterator, and says what it has left to give (`finds`):
/// `next` gives the first of them and leaves the rest.
pub struct VerifiedFindByRange<'a, T: VDeltaValue<C>, C: VCodec = Leb128> {
    slabs: &'a [Slab<<DE<T, C> as Slabs>::Tail>],
    /// The search of the column's index and slabs; none where there is
    /// nothing to find.
    find: Option<DFind<'a, <DX<T, C> as Decoders<'a>>::D>>,
}

impl<'a, T: VDeltaValue<C>, C: VCodec> VerifiedFindByRange<'a, T, C> {
    /// The search, where there is one, is a search of the slabs that
    /// has given every match before the place it has come to, a place
    /// in the column or its end; and the sums of the column's
    /// differences are numbers.
    #[verifier::type_invariant]
    spec fn inv(&self) -> bool {
        let s = self.slabs@;
        match self.find {
            Some(fd) => {
                &&& find_ok::<DX<T, C>, DB<T, C>>(fd, s, fd.from@)
                &&& 0 <= fd.from@ <= total(slab_lens(s))
                &&& vals_of::<DX<T, C>>(s).len() == total(slab_lens(s))
                &&& self.kept().len() == total(slab_lens(s))
                &&& all_fit::<DX<T, C>, DB<T, C>>(s)
            },
            None => true,
        }
    }

    /// What the column of the slabs keeps: each value as an `i64`, or
    /// none for a null.
    spec fn kept(&self) -> Seq<Option<i64>> {
        rz(0, dslabs::<DX<T, C>, DB<T, C>>(self.slabs@))
    }

    /// The places the finder has left to give, in the order it gives
    /// them.
    pub closed spec fn finds(&self) -> Seq<usize> {
        match self.find {
            Some(fd) => hits_from(self.kept(), fd.lo, fd.hi, fd.from@),
            None => Seq::empty(),
        }
    }
}

#[cfg(verus_keep_ghost)]
impl<'a, T: VDeltaValue<C>, C: VCodec> vstd::std_specs::iter::IteratorSpecImpl for VerifiedFindByRange<'a, T, C> {
    /// The finder keeps to what Verus asks of an iterator: `next` gives
    /// `remaining()`, one place after another, and then `None`.
    open spec fn obeys_prophetic_iter_laws(&self) -> bool {
        true
    }

    open spec fn remaining(&self) -> Seq<usize> {
        self.finds()
    }

    open spec fn will_return_none(&self) -> bool {
        true
    }

    open spec fn decrease(&self) -> Option<nat> {
        Some(self.finds().len())
    }

    open spec fn peek(&self, index: int) -> Option<usize> {
        if 0 <= index < self.finds().len() {
            Some(self.finds()[index])
        } else {
            None
        }
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> Iterator for VerifiedFindByRange<'a, T, C> {
    type Item = usize;

    /// The next place, if one is left: the first of `finds()`, which
    /// leaves the rest of them.
    fn next(&mut self) -> (r: Option<usize>) {
        proof {
            use_type_invariant(&*self);
        }
        let ghost s = self.slabs@;
        let ghost ds = self.kept();
        match self.find.take() {
            None => None,
            Some(mut fd) => {
                let ghost lo = fd.lo;
                let ghost hi = fd.hi;
                let ghost f0 = fd.from@;
                let r = d_find_next::<DX<T, C>, DB<T, C>>(&mut fd, self.slabs);
                proof {
                    // a match of the search is a number from `lo` to `hi` that the column keeps
                    assert forall|j: int| 0 <= j < ds.len() implies col_hit::<DX<T, C>, DB<T, C>>(
                        s,
                        lo as int,
                        hi as int,
                        j,
                    ) == kept_in(#[trigger] ds[j], lo, hi) by {
                        lemma_kept_hit::<DX<T, C>, DB<T, C>>(s, lo, hi, j);
                    }
                    match r {
                        Some(i) => {
                            lemma_hits_next(ds, lo, hi, f0, i as int);
                            assert(hits_from(ds, lo, hi, f0).drop_first() =~= hits_from(ds, lo, hi, i + 1));
                        },
                        None => {
                            lemma_hits_none(ds, lo, hi, f0);
                        },
                    }
                }
                self.find = Some(fd);
                r
            },
        }
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> Default for VerifiedFindByRange<'a, T, C> {
    /// A finder with nothing to give.
    fn default() -> (r: Self)
        ensures
            r.finds() == Seq::<usize>::empty(),
    {
        let slabs: &'a [Slab<<DE<T, C> as Slabs>::Tail>] = &[];
        VerifiedFindByRange { slabs, find: None }
    }
}

impl<T: VDeltaValue<C>, C: VCodec> VerifiedDeltaColumn<T, C> {
    /// A finder of the places whose values are within the bounds `lo`
    /// and `hi`.
    fn find_within(&self, lo: Bound<&T>, hi: Bound<&T>) -> (r: VerifiedFindByRange<'_, T, C>)
        ensures
            r.finds() == delta_finds::<T>(self@, lo, hi, 0),
    {
        proof {
            use_type_invariant(self);
        }
        match bounds_of::<T>(lo, hi) {
            None => {
                proof {
                    // no value of a column is within the bounds
                    lemma_hits_finds::<T>(self.kept(), lo, hi, 1, 0, 0);
                    lemma_hits_none(self.kept(), 1, 0, 0);
                }
                VerifiedFindByRange::default()
            },
            Some((a, b)) => {
                let (slabs, ix) = self.core.delta_parts();
                self.core.lemma_column();
                proof {
                    let s = self.core.slabs_spec();
                    // what the search will read is what the column keeps
                    lemma_dslabs_own::<DE<T, C>, DX<T, C>, DB<T, C>>(s);
                    assert(rz(0, dslabs::<DX<T, C>, DB<T, C>>(s)) == self.kept());
                    // and the sums of its differences are numbers
                    assert forall|n: int| 0 <= n <= vals_of::<DX<T, C>>(s).len() implies in_i64(
                        #[trigger] rsum::<DX<T, C>, DB<T, C>>(s, n),
                    ) by {
                        lemma_sums_fit::<DX<T, C>, DB<T, C>>(ix@, s, n);
                    }
                    lemma_hits_finds::<T>(self.kept(), lo, hi, a, b, 0);
                }
                let fd = d_find_new::<DX<T, C>, DB<T, C>>(ix, a, b, slabs);
                VerifiedFindByRange { slabs, find: Some(fd) }
            },
        }
    }

    /// The places of the values within `range`, from the first on.
    ///
    /// The bounds are values of the column's type, and a value is
    /// within them as `delta_within` says: a null is within no
    /// bounds, though a bound may be one. The finder gives the places of
    /// `delta_finds`. The column's index tells it which slabs can
    /// hold such a value, and it reads through those alone.
    pub fn find_by_range<R: RangeBounds<T>>(&self, range: R) -> (r: VerifiedFindByRange<'_, T, C>)
        ensures
            r.finds() == delta_finds::<T>(self@, range.spec_start_bound(), range.spec_end_bound(), 0),
    {
        self.find_within(range.start_bound(), range.end_bound())
    }

    /// The places of the values that are `target`, from the first on.
    ///
    /// A null is found nowhere, as with
    /// [`DeltaColumn::find_by_value`](crate::DeltaColumn::find_by_value):
    /// the places are those of the values within the bounds that
    /// include `target` and nothing else (`lemma_delta_within_one`).
    pub fn find_by_value(&self, target: T) -> (r: VerifiedFindByRange<'_, T, C>)
        ensures
            r.finds() == delta_finds::<T>(self@, Bound::Included(&target), Bound::Included(&target), 0),
    {
        self.find_within(Bound::Included(&target), Bound::Included(&target))
    }

    /// The first place of a value that is `target`, if there is one.
    ///
    /// A null is found nowhere: for a `target` that is one the answer
    /// is `None` whatever the column holds.
    pub fn find_first(&self, target: T) -> (r: Option<usize>)
        ensures
            match r {
                Some(i) => {
                    &&& i < self@.len()
                    &&& self@[i as int] == target
                    &&& target.dv() is Some
                    &&& forall|j: int| 0 <= j < i ==> #[trigger] self@[j] != target
                },
                None => target.dv() is Some ==> forall|j: int| 0 <= j < self@.len() ==> #[trigger] self@[j] != target,
            },
    {
        let mut finder = self.find_by_value(target);
        let r = finder.next();
        self.core.lemma_column();
        proof {
            self.lemma_fits();
            assert(self@.len() == self.core@.len());
            lemma_delta_finds_first::<T>(self@, Bound::Included(&target), Bound::Included(&target), 0);
            assert forall|j: int| 0 <= j < self@.len() implies delta_within::<T>(
                Bound::Included(&target),
                Bound::Included(&target),
                #[trigger] self@[j],
            ) == (self@[j] == target && target.dv() is Some) by {
                lemma_delta_within_one::<T>(target, self@[j]);
            }
        }
        r
    }
}

} // verus!
