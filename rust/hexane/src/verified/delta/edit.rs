//! Edits of a [`VerifiedDeltaColumn`] at a cursor: [`VerifiedDeltaEdit`],
//! which [`VerifiedDeltaColumn::edit`] and
//! [`VerifiedDeltaColumn::edit_at`] open.

#[cfg(verus_keep_ghost)]
use super::{
    all_dom, delta_run_kept, delta_runs_fit, delta_runs_kept, delta_runs_values, in_dom,
    lemma_delta_runs_dom, vals, VDelta, DE, DX,
};
use super::{DKind, ECore, VDeltaValue, VerifiedDeltaColumn};
#[cfg(verus_keep_ghost)]
use crate::delta::agg::reps;
#[cfg(verus_keep_ghost)]
use crate::delta::ddomain::rvals;
use crate::delta::ddomain::DeltaWrite;
#[cfg(verus_keep_ghost)]
use crate::delta::dedit::run_vals;
use crate::delta::dedit_core::DeltaEditCore;
use crate::rle::dec::RleReads;
use crate::rle::edit::RleModel;
use crate::rle::lift::Lift;
use crate::rle::model::{RleOps, WriteOps};
#[cfg(verus_keep_ghost)]
use crate::verified::after_call;
use crate::verified::codec::VCodec;
use crate::{DeltaRun, Leb128};
use vstd::prelude::*;

verus! {

/// The shared cursor of an edit of a delta column of `T`.
type ECur<T, C> = DeltaEditCore<<T as DKind<C>>::L, <T as DKind<C>>::O, <T as DKind<C>>::W>;

/// An edit of a [`VerifiedDeltaColumn`] in progress: a cursor that walks
/// the column once, front to back, and writes the edited column as it
/// goes. [`VerifiedDeltaColumn::edit`] and
/// [`VerifiedDeltaColumn::edit_at`] open one.
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
/// use hexane::verified::VerifiedDeltaColumn;
///
/// let mut col = VerifiedDeltaColumn::<u64>::from_values(&[10, 20, 30, 40, 50]);
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
/// (`fits`), as [`splice`](VerifiedDeltaColumn::splice) asks of its
/// values. What no step can ask is that the edited column's values lie
/// within 2^63 of each other, which a delta column's must:
/// [`finish`](Self::finish) panics where they do not.
#[must_use = "an edit gives the column back when `finish` is called"]
pub struct VerifiedDeltaEdit<'a, T: VDeltaValue<C>, C: VCodec = Leb128> {
    col: &'a mut VerifiedDeltaColumn<T, C>,
    /// the column's slabs, which the cursor has while it is open
    held: ECore<T, C>,
    core: ECur<T, C>,
    /// the column's count of edits when the cursor was opened
    counter: usize,
    /// a step has deleted or inserted a value
    changed: bool,
}

/// The values of what is kept, of one sequence after another.
proof fn lemma_vals_add<T: VDelta>(a: Seq<Option<i64>>, b: Seq<Option<i64>>)
    ensures
        vals::<T>(a + b) == vals::<T>(a) + vals::<T>(b),
        all_dom::<T>(a) && all_dom::<T>(b) ==> all_dom::<T>(a + b),
{
    assert(vals::<T>(a + b) =~= vals::<T>(a) + vals::<T>(b));
    if all_dom::<T>(a) && all_dom::<T>(b) {
        assert forall|i: int| 0 <= i < (a + b).len() implies in_dom::<T>(#[trigger] (a + b)[i]) by {
            if i < a.len() {
                assert((a + b)[i] == a[i]);
            } else {
                assert((a + b)[i] == b[i - a.len()]);
            }
        }
    }
}

/// The values of what is kept, of a part of a sequence.
proof fn lemma_vals_sub<T: VDelta>(a: Seq<Option<i64>>, i: int, j: int)
    requires
        0 <= i <= j <= a.len(),
    ensures
        vals::<T>(a.subrange(i, j)) == vals::<T>(a).subrange(i, j),
        all_dom::<T>(a) ==> all_dom::<T>(a.subrange(i, j)),
{
    assert(vals::<T>(a.subrange(i, j)) =~= vals::<T>(a).subrange(i, j));
    if all_dom::<T>(a) {
        assert forall|k: int| 0 <= k < a.subrange(i, j).len() implies in_dom::<T>(#[trigger] a.subrange(i, j)[k]) by {
            assert(a.subrange(i, j)[k] == a[i + k]);
        }
    }
}

/// Give the cursor `core` each of the runs `runs`: of a run of numbers
/// its first value and its step, of a run of nulls its count.
fn insert_all<L, O, B>(core: &mut DeltaEditCore<L, O, B>, runs: &[DeltaRun]) where
    L: for <'t>Lift<'t, W = L> + for <'t>WriteOps<'t, R = O>,
    O: for <'b>RleOps<'b>,
    B: for <'a>DeltaWrite<'a, RleModel<L>, RleReads<O>>,

    requires
        old(core).inv(),
        old(core).edit.held(),
    ensures
        final(core).inv(),
        final(core).edit.held(),
        final(core).same(*old(core)),
        final(core).at() == old(core).at(),
        final(core).rwritten() == old(core).rwritten() + delta_runs_kept(runs@),
{
    let ghost w0 = core.rwritten();
    proof {
        assert(w0 =~= w0 + delta_runs_kept(runs@.take(0)));
    }
    let mut i: usize = 0;
    while i < runs.len()
        invariant
            i <= runs.len(),
            core.inv(),
            core.edit.held(),
            core.same(*old(core)),
            core.at() == old(core).at(),
            core.rwritten() == w0 + delta_runs_kept(runs@.take(i as int)),
        decreases runs.len() - i,
    {
        let run = runs[i];
        if run.count > 0 {
            match run.delta {
                None => core.insert_nulls(run.count),
                Some(d) => core.insert_run(run.prefix, d, run.count),
            }
        }
        proof {
            let done = runs@.take(i as int);
            assert(runs@.take(i + 1) =~= done.push(run));
            assert(done.push(run).drop_last() =~= done);
            let one = delta_run_kept(run);
            match run.delta {
                None => {
                    assert(one =~= reps(run.count as nat, None));
                },
                Some(d) => {
                    assert(one =~= run_vals(run.prefix as int, d, run.count as nat));
                },
            }
            assert(w0 + (delta_runs_kept(done) + one) =~= (w0 + delta_runs_kept(done)) + one);
        }
        i += 1;
    }
    proof {
        assert(runs@.take(runs@.len() as int) =~= runs@);
    }
}

impl<T: VDeltaValue<C>, C: VCodec> VerifiedDeltaColumn<T, C> {
    /// A cursor to edit the column with, standing at its first value.
    /// See [`VerifiedDeltaEdit`].
    #[inline(always)]
    pub fn edit(&mut self) -> (e: VerifiedDeltaEdit<'_, T, C>)
        ensures
            e.wf(),
            e.orig() == old(self)@,
            e.at() == 0,
            e.written() == Seq::<T>::empty(),
            !e.changed(),
            e.began() == old(self).edits(),
            e.result() == *final(self),
    {
        let e = self.edit_at(0);
        proof {
            assert(old(self)@.take(0) =~= Seq::<T>::empty());
        }
        e
    }

    /// A cursor to edit the column with, standing at the value at `at`
    /// (at the end, where `at` is the column's length), with the values
    /// before it kept. See [`VerifiedDeltaEdit`].
    ///
    /// The cursor's `result()` is the column as it will be when the
    /// borrow of it ends: what `finish` says of that is what is known of
    /// the column afterwards.
    ///
    /// # Panics
    ///
    /// If `at` is past the column's end, which the `requires` rules out
    /// of a verified caller.
    pub fn edit_at(&mut self, at: usize) -> (e: VerifiedDeltaEdit<'_, T, C>)
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
        proof {
            use_type_invariant(&*self);
        }
        // a caller that is not verified gets the old cursor's panic, and
        // the column as it was
        let _left = self.len().checked_sub(at).expect("edit_at out of bounds");
        let ghost k0 = self.kept();
        self.core.lemma_column();
        // the column's values are kept in bounds while it is whole: the
        // cursor has its slabs, and leaves an empty column in their place
        let mut me = Self::new();
        core::mem::swap(self, &mut me);
        let store = me.core;
        let counter = store.counter();
        let mut held = T::to_edit(store);
        let core = DeltaEditCore::begin(&mut held, at);
        proof {
            assert(core.rorig() == k0);
            assert(k0.len() == old(self).core@.len());
            lemma_vals_sub::<T>(k0, 0, at as int);
        }
        VerifiedDeltaEdit { col: self, held, core, counter, changed: false }
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> VerifiedDeltaEdit<'a, T, C> {
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
    pub closed spec fn result(&self) -> VerifiedDeltaColumn<T, C> {
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
        self.lemma_lens();
    }

    /// The values the cursor speaks of are as many as those its core
    /// has, and the cursor stands among them.
    proof fn lemma_lens(&self)
        requires
            self.wf(),
        ensures
            self.orig().len() == self.core.rorig().len() == self.core.edit.orig_vals().len(),
            self.written().len() == self.core.rwritten().len() == self.core.edit.written().len(),
            0 <= self.core.at() <= self.core.rorig().len(),
    {
        self.core.edit.lemma_at();
    }

    /// The cursor's place among the original values.
    #[inline(always)]
    pub fn pos(&self) -> (r: usize)
        ensures
            r == self.at(),
    {
        self.core.edit.pos()
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
        proof {
            self.lemma_lens();
        }
        self.core.edit.out_pos()
    }

    /// One step: keep the next `n` values.
    #[inline(always)]
    fn step_advance(&mut self, n: usize)
        requires
            old(self).wf(),
        ensures
            final(self).kept(*old(self)),
            final(self).at() == old(self).at() + old(self).step(n),
            final(self).written() == old(self).written() + old(self).orig().subrange(
                old(self).at(),
                old(self).at() + old(self).step(n),
            ),
            final(self).changed() == old(self).changed(),
    {
        proof {
            self.lemma_lens();
        }
        self.core.advance(n);
        proof {
            let o = old(self).core;
            let (a, b) = (o.at(), o.past(n));
            let passed = o.rorig().subrange(a, b);
            lemma_vals_sub::<T>(o.rorig(), a, b);
            lemma_vals_add::<T>(o.rwritten(), passed);
        }
    }

    /// One step: write `n` copies of `value`.
    #[inline(always)]
    fn step_insert(&mut self, value: T, n: usize)
        requires
            old(self).wf(),
            value.fits(),
        ensures
            final(self).kept(*old(self)),
            final(self).at() == old(self).at(),
            final(self).written() == old(self).written() + Seq::new(n as nat, |_i: int| value),
            final(self).changed() == (old(self).changed() || n > 0),
    {
        if n > 0 {
            match value.to_i64() {
                None => self.core.insert_nulls(n),
                Some(v) => self.core.insert_value(v, n),
            }
            self.changed = true;
        }
        proof {
            T::lemma_delta(value, None);
            let w = old(self).core.rwritten();
            let run = reps(n as nat, value.dv());
            assert(self.core.rwritten() =~= w + run);
            assert(all_dom::<T>(run));
            lemma_vals_add::<T>(w, run);
            assert(vals::<T>(run) =~= Seq::new(n as nat, |_i: int| value));
        }
    }

    /// One step: drop the next `n` values.
    #[inline(always)]
    fn step_delete(&mut self, n: usize)
        requires
            old(self).wf(),
        ensures
            final(self).kept(*old(self)),
            final(self).at() == old(self).at() + old(self).step(n),
            final(self).written() == old(self).written(),
            final(self).changed() == (old(self).changed() || old(self).step(n) > 0),
    {
        proof {
            self.lemma_lens();
        }
        if n > 0 {
            let left = self.core.edit.remaining();
            self.core.delete(n);
            self.changed = self.changed || left > 0;
        }
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
        self.step_advance(n);
        self
    }

    /// Keep the values up to the one at `to` of the original column, and
    /// stand there; at the end, where `to` is past it. A cursor only
    /// goes forward: where `to` is before the place it stands at, it
    /// stays there (as the old API's cursor of a delta column does).
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
        proof {
            self.lemma_at();
        }
        let n = to.saturating_sub(self.core.edit.pos());
        self.step_advance(n);
        self
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
        self.step_insert(value, 1);
        proof {
            assert(self.written() =~= old(self).written().push(value));
        }
        self
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
        self.step_insert(value, n);
        self
    }

    /// Write, before the value the cursor stands at, the values of the
    /// runs `runs` (`delta_runs_values`): of each run of numbers its
    /// first value and then each a step from the one before, of a run of
    /// nulls its nulls. A run says its values itself (its `prefix` is
    /// the value before its first), whatever the cursor stands after.
    ///
    /// Each run's values are ones the column can keep
    /// (`delta_runs_fit`; [`VerifiedDeltaColumn::run_fits`] is a check
    /// that shows it). The cursor has changed the column if a value was
    /// written.
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
        proof {
            self.lemma_lens();
        }
        let before = self.core.edit.out_pos();
        insert_all(&mut self.core, runs);
        proof {
            lemma_delta_runs_dom::<T>(runs@);
            lemma_vals_add::<T>(old(self).core.rwritten(), delta_runs_kept(runs@));
            self.lemma_lens();
        }
        let after = self.core.edit.out_pos();
        self.changed = self.changed || after != before;
        self
    }

    /// The value the cursor stands at, or `None` at the end. The cursor
    /// stays where it is.
    // a `map` would take a closure that asks the value be in the domain
    #[allow(clippy::manual_map)]
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
        proof {
            self.lemma_lens();
        }
        match self.core.peek_value() {
            None => None,
            Some(d) => Some(T::from_i64(d)),
        }
    }

    /// Put `value` in the place of the value the cursor stands at, and
    /// stand after it. At the end, where there is no value to replace,
    /// this does nothing.
    ///
    /// Where the value there is `value` already it is kept, and the
    /// cursor has not changed the column by this.
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
        proof {
            self.lemma_lens();
        }
        let want = value.to_i64();
        let (there, same) = match self.core.peek_value() {
            Some(cur) => {
                proof {
                    // two values a column can keep are one where they
                    // are kept as one
                    T::lemma_delta(value, cur);
                }
                (
                    true,
                    match (cur, want) {
                        (Some(a), Some(b)) => a == b,
                        (None, None) => true,
                        _ => false,
                    },
                )
            },
            None => (false, false),
        };
        if there {
            if same {
                self.step_advance(1);
                proof {
                    let o = *old(self);
                    assert(o.orig().subrange(o.at(), o.at() + 1) =~= seq![value]);
                    assert(self.written() =~= o.written().push(value));
                }
            } else {
                self.step_delete(1);
                self.step_insert(value, 1);
                proof {
                    assert(self.written() =~= old(self).written().push(value));
                }
            }
        }
        self
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
        self.step_delete(n);
        self
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
    pub fn finish(self)
        requires
            self.wf(),
        ensures
            self.result()@ == self.now(),
            self.result().edits() == after_call(self.began(), self.changed()),
    {
        proof {
            self.lemma_lens();
            let (w, o, a) = (self.core.rwritten(), self.core.rorig(), self.core.at());
            lemma_vals_sub::<T>(o, a, o.len() as int);
            lemma_vals_add::<T>(w, o.skip(a));
        }
        let VerifiedDeltaEdit { col: column, held: mut slabs, core: mut cursor, counter: before, changed: edit } = self;
        cursor.end(&mut slabs, T::lo(), T::hi());
        let mut store = T::of_edit(slabs);
        // the shared cursor moves the count as it writes, by a number
        // that depends on the edit: this makes it one an edit
        store.set_counter(
            if edit {
                before.wrapping_add(1)
            } else {
                before
            },
        );
        *column = VerifiedDeltaColumn { core: store };
    }
}

} // verus!
