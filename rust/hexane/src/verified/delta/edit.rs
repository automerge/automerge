//! Edits of a [`DeltaColumn`] at a cursor: [`DeltaEdit`],
//! which [`DeltaColumn::edit`] and
//! [`DeltaColumn::edit_at`] open.
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([Reviewing the specification](crate#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`edit/imp.rs`), which has
//! its contract word for word and is proved there.

#[cfg(verus_keep_ghost)]
use super::{all_dom, delta_runs_fit, delta_runs_values, vals, Delta, DE, DX};
use super::{DKind, DeltaColumn, DeltaValue, ECore};
#[cfg(verus_keep_ghost)]
use crate::delta::ddomain::DeltaWrite;
#[cfg(verus_keep_ghost)]
use crate::rle::lift::Lift;
#[cfg(verus_keep_ghost)]
use crate::rle::model::{RleOps, WriteOps};
#[cfg(verus_keep_ghost)]
use crate::verified::after_call;
use crate::verified::codec::Codec;
use crate::{DeltaRun, Leb128};
use vstd::prelude::*;

mod imp;
#[allow(unused_imports)]
use imp::*;

verus! {

/// An edit of a [`DeltaColumn`] in progress: a cursor that walks
/// the column once, front to back, and writes the edited column as it
/// goes. [`DeltaColumn::edit`] and
/// [`DeltaColumn::edit_at`] open one.
///
/// A cursor passes over values ([`seek`](Self::seek),
/// [`advance`](Self::advance)), which keeps them; puts values before the
/// one it stands at ([`insert`](Self::insert),
/// [`insert_run`](Self::insert_run), [`insert_runs`](Self::insert_runs));
/// drops values from where it stands ([`delete`](Self::delete)); looks
/// at the value it stands at ([`peek`](Self::peek)) and puts another in
/// its place ([`replace_with`](Self::replace_with)). Many edits made
/// through one cursor cost one pass over the slabs they touch, where as
/// many calls of `splice` would each find their place anew.
///
/// The steps return the cursor, so they can be written one after
/// another, and [`finish`](Self::finish) hands the edited column back:
///
/// ```
/// use hexane::DeltaColumn;
///
/// let mut col = DeltaColumn::<u64>::from_values(&[10, 20, 30, 40, 50]);
/// let mut e = col.edit();
/// e.seek(1).delete(2).insert(25).seek(4).replace_with(45);
/// e.finish();
/// assert_eq!(col.to_vec(), [10, 25, 40, 45]);
/// ```
///
/// # An edit must be finished
///
/// While a cursor is open it holds the column's slabs, and `finish` is
/// what gives them back. A cursor dropped without it leaves the column
/// without its values: an empty column. Verus shows of a verified caller
/// that it did call `finish` wherever it says anything of the column
/// afterwards, since only `finish` says what the column is (`result()`).
///
/// # What a specification says of a cursor
///
/// A cursor `e` has the values of the column as it was opened
/// (`e.orig()`), the place among them it stands at (`e.at()`), and the
/// values it has written (`e.written()`). The column it will give is
/// `e.now()`: what is written, then the original values from `e.at()`
/// on. Every step says what it does to these three, and that the rest
/// stays as it was (`kept`).
///
/// The values a cursor writes are ones a delta column of `T` can keep
/// (`fits`), as [`splice`](DeltaColumn::splice) asks of its
/// values. What no step can ask is that the edited column's values lie
/// within 2^63 of each other, which a delta column's must:
/// [`finish`](Self::finish) panics where they do not.
#[must_use = "an edit gives the column back when `finish` is called"]
pub struct DeltaEdit<'a, T: DeltaValue<C>, C: Codec = Leb128> {
    col: &'a mut DeltaColumn<T, C>,
    /// the column's slabs, which the cursor has while it is open
    held: ECore<T, C>,
    core: ECur<T, C>,
    /// the column's count of edits when the cursor was opened
    counter: usize,
    /// a step has deleted or inserted a value
    changed: bool,
}

impl<T: DeltaValue<C>, C: Codec> DeltaColumn<T, C> {
    /// A cursor to edit the column with, standing at its first value.
    /// See [`DeltaEdit`].
    #[inline(always)]
    pub fn edit(&mut self) -> (e: DeltaEdit<'_, T, C>)
        ensures
            e.wf(),
            e.orig() == old(self)@,
            e.at() == 0,
            e.written() == Seq::<T>::empty(),
            !e.changed(),
            e.began() == old(self).edits(),
            e.result() == *final(self),
    {
        self.edit_impl()
    }

    /// A cursor to edit the column with, standing at the value at `at`
    /// (at the end, where `at` is the column's length), with the values
    /// before it kept. See [`DeltaEdit`].
    ///
    /// The cursor's `result()` is the column as it will be when the
    /// borrow of it ends: what `finish` says of that is what is known of
    /// the column afterwards.
    ///
    /// # Panics
    ///
    /// If `at` is past the column's end, which the `requires` rules out
    /// of a verified caller.
    #[inline(always)]
    pub fn edit_at(&mut self, at: usize) -> (e: DeltaEdit<'_, T, C>)
        requires
            at <= old(self)@.len(),
        ensures
            e.wf(),
            e.orig() == old(self)@,
            e.at() == at,
            e.written() == old(self)@.take(at as int),
            !e.changed(),
            e.began() == old(self).edits(),
            e.result() == *final(self),
    {
        self.edit_at_impl(at)
    }
}

impl<'a, T: DeltaValue<C>, C: Codec> DeltaEdit<'a, T, C> {
    /// The cursor is one a column gave, or one a step of such a cursor
    /// left: every step asks for this and keeps it.
    pub closed spec fn wf(&self) -> bool {
        &&& self.core.inv()
        &&& self.core.edit.held()
        &&& all_dom::<T>(self.core.rorig())
        &&& all_dom::<T>(self.core.rwritten())
        &&& self.core.rorig().len() <= usize::MAX
    }

    /// The values of the column as it was when the cursor was opened.
    pub closed spec fn orig(&self) -> Seq<T> {
        vals::<T>(self.core.rorig())
    }

    /// The place the cursor stands at, among the original values: the
    /// ones before it have been kept or deleted.
    pub closed spec fn at(&self) -> int {
        self.core.at()
    }

    /// The values written so far: the original values the cursor has
    /// kept and the ones inserted, in their order.
    pub closed spec fn written(&self) -> Seq<T> {
        vals::<T>(self.core.rwritten())
    }

    /// What the column holds if the edit is finished now: what is
    /// written, then the original values from the cursor's place on.
    pub open spec fn now(&self) -> Seq<T> {
        self.written() + self.orig().skip(self.at())
    }

    /// A step has deleted a value or inserted one.
    pub closed spec fn changed(&self) -> bool {
        self.changed
    }

    /// The count of edits the column had when the cursor was opened.
    pub closed spec fn began(&self) -> usize {
        self.counter
    }

    /// The column as it will be when the edit is over and its borrow
    /// ends. [`finish`](Self::finish) says what it holds.
    #[verifier::prophetic]
    pub closed spec fn result(&self) -> DeltaColumn<T, C> {
        *final(self.col)
    }

    /// How far a step over `n` values goes: over `n`, or to the end of
    /// the original values where fewer are left.
    pub open spec fn step(&self, n: usize) -> int {
        let left = self.orig().len() - self.at();
        if n <= left {
            n as int
        } else {
            left
        }
    }

    /// What every step keeps of the cursor `old` it was called on.
    #[verifier::prophetic]
    pub open spec fn kept(&self, old: Self) -> bool {
        &&& self.wf()
        &&& self.orig() == old.orig()
        &&& self.began() == old.began()
        &&& self.result() == old.result()
    }

    /// The cursor is as `old` was: nothing moved, nothing written.
    #[verifier::prophetic]
    pub open spec fn unmoved(&self, old: Self) -> bool {
        &&& self.kept(old)
        &&& self.at() == old.at()
        &&& self.written() == old.written()
        &&& self.changed() == old.changed()
    }

    /// The cursor stands among the original values, or at their end;
    /// and they are no more than a `usize` counts, so that a seek to
    /// `usize::MAX` is a seek to the end.
    pub proof fn lemma_at(&self)
        requires
            self.wf(),
        ensures
            0 <= self.at() <= self.orig().len() <= usize::MAX,
    {
        self.lemma_at_impl()
    }

    /// The cursor's place among the original values.
    #[inline(always)]
    pub fn pos(&self) -> (r: usize)
        ensures
            r == self.at(),
    {
        self.pos_impl()
    }

    /// The cursor's place in the column being written: the number of
    /// values written so far.
    #[inline(always)]
    pub fn out_pos(&self) -> (r: usize)
        requires
            self.wf(),
        ensures
            r == self.written().len(),
    {
        self.out_pos_impl()
    }

    /// Keep the next `n` values: the cursor passes over them, and they
    /// are written as they are. Where fewer than `n` are left, it keeps
    /// those and stands at the end.
    #[inline(always)]
    pub fn advance(&mut self, n: usize) -> (r: &mut Self)
        requires
            old(self).wf(),
        ensures
            *final(self) == *final(r),
            r.kept(*old(self)),
            r.at() == old(self).at() + old(self).step(n),
            r.written() == old(self).written() + old(self).orig().subrange(
                old(self).at(),
                old(self).at() + old(self).step(n),
            ),
            r.changed() == old(self).changed(),
    {
        self.advance_impl(n)
    }

    /// Keep the values up to the one at `to` of the original column, and
    /// stand there; at the end, where `to` is past it. A cursor only
    /// goes forward: where `to` is before the place it stands at, it
    /// stays there.
    #[inline(always)]
    pub fn seek(&mut self, to: usize) -> (r: &mut Self)
        requires
            old(self).wf(),
        ensures
            *final(self) == *final(r),
            r.kept(*old(self)),
            r.at() == if to <= old(self).at() {
                old(self).at()
            } else if to <= old(self).orig().len() {
                to as int
            } else {
                old(self).orig().len() as int
            },
            r.written() == old(self).written() + old(self).orig().subrange(old(self).at(), r.at()),
            r.changed() == old(self).changed(),
    {
        self.seek_impl(to)
    }

    /// Write `value` before the value the cursor stands at.
    #[inline(always)]
    pub fn insert(&mut self, value: T) -> (r: &mut Self)
        requires
            old(self).wf(),
            value.fits(),
        ensures
            *final(self) == *final(r),
            r.kept(*old(self)),
            r.at() == old(self).at(),
            r.written() == old(self).written().push(value),
            r.changed(),
    {
        self.insert_impl(value)
    }

    /// Write `n` copies of `value` before the value the cursor stands
    /// at; none, where `n` is 0.
    #[inline(always)]
    pub fn insert_run(&mut self, value: T, n: usize) -> (r: &mut Self)
        requires
            old(self).wf(),
            value.fits(),
        ensures
            *final(self) == *final(r),
            r.kept(*old(self)),
            r.at() == old(self).at(),
            r.written() == old(self).written() + Seq::new(n as nat, |_i: int| value),
            r.changed() == (old(self).changed() || n > 0),
    {
        self.insert_run_impl(value, n)
    }

    /// Write, before the value the cursor stands at, the values of the
    /// runs `runs` (`delta_runs_values`): of each run of numbers its
    /// first value and then each a step from the one before, of a run of
    /// nulls its nulls. A run says its values itself (its `prefix` is
    /// the value before its first), whatever the cursor stands after.
    ///
    /// Each run's values are ones the column can keep
    /// (`delta_runs_fit`; [`DeltaColumn::run_fits`] is a check
    /// that shows it). The cursor has changed the column if a value was
    /// written.
    #[inline(always)]
    pub fn insert_runs(&mut self, runs: &[DeltaRun]) -> (r: &mut Self)
        requires
            old(self).wf(),
            delta_runs_fit::<T>(runs@),
        ensures
            *final(self) == *final(r),
            r.kept(*old(self)),
            r.at() == old(self).at(),
            r.written() == old(self).written() + delta_runs_values::<T>(runs@),
            r.changed() == (old(self).changed() || r.written().len() != old(self).written().len()),
    {
        self.insert_runs_impl(runs)
    }

    /// The value the cursor stands at, or `None` at the end. The cursor
    /// stays where it is.
    // a `map` would take a closure that asks the value be in the domain
    #[allow(clippy::manual_map)]
    #[inline(always)]
    pub fn peek(&mut self) -> (r: Option<T>)
        requires
            old(self).wf(),
        ensures
            final(self).unmoved(*old(self)),
            match r {
                Some(v) => old(self).at() < old(self).orig().len() && v == old(self).orig()[old(self).at()],
                None => old(self).at() == old(self).orig().len(),
            },
    {
        self.peek_impl()
    }

    /// Put `value` in the place of the value the cursor stands at, and
    /// stand after it. At the end, where there is no value to replace,
    /// this does nothing.
    ///
    /// Where the value there is `value` already it is kept, and the
    /// cursor has not changed the column by this.
    #[inline(always)]
    pub fn replace_with(&mut self, value: T) -> (r: &mut Self)
        requires
            old(self).wf(),
            value.fits(),
        ensures
            *final(self) == *final(r),
            r.kept(*old(self)),
            if old(self).at() < old(self).orig().len() {
                &&& r.at() == old(self).at() + 1
                &&& r.written() == old(self).written().push(value)
                &&& r.changed() == (old(self).changed() || old(self).orig()[old(self).at()] != value)
            } else {
                &&& r.at() == old(self).at()
                &&& r.written() == old(self).written()
                &&& r.changed() == old(self).changed()
            },
    {
        self.replace_with_impl(value)
    }

    /// Drop the next `n` values: the cursor passes over them, and they
    /// are not written. Where fewer than `n` are left, it drops those
    /// and stands at the end.
    #[inline(always)]
    pub fn delete(&mut self, n: usize) -> (r: &mut Self)
        requires
            old(self).wf(),
        ensures
            *final(self) == *final(r),
            r.kept(*old(self)),
            r.at() == old(self).at() + old(self).step(n),
            r.written() == old(self).written(),
            r.changed() == (old(self).changed() || old(self).step(n) > 0),
    {
        self.delete_impl(n)
    }

    /// End the edit: the column holds what the cursor has written and
    /// then the original values from the cursor's place on (`now()`).
    ///
    /// The column counts one edit more than it did when the cursor was
    /// opened if a step deleted a value or inserted one, however many
    /// did; and what it did, if none did.
    ///
    /// # Panics
    ///
    /// Where the edited column's values do not lie within 2^63 of each
    /// other, as an edit by `splice` does.
    #[inline(always)]
    pub fn finish(self)
        requires
            self.wf(),
        ensures
            self.result()@ == self.now(),
            self.result().edits() == after_call(self.began(), self.changed()),
    {
        self.finish_impl()
    }
}

} // verus!
