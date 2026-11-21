//! The implementation of `verified::delta::edit` and its proofs: what its
//! specification (`edit.rs`) says. Each function named `..._impl` here has
//! the contract of the function there that calls it, word for word; a
//! function named after a trait, a method and a type (`vpack_pack_u64`)
//! is the body of that method in that type's impl of the trait, with the
//! trait's contract said of the type.

#![allow(unused_imports)]

#[cfg(verus_keep_ghost)]
use super::super::{delta_run_kept, delta_runs_kept, in_dom, lemma_delta_runs_dom};
use super::*;

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
pub(super) type ECur<T, C> = DeltaEditCore<<T as DKind<C>>::L, <T as DKind<C>>::O, <T as DKind<C>>::W>;

/// The values of what is kept, of one sequence after another.
pub(super) proof fn lemma_vals_add<T: VDelta>(a: Seq<Option<i64>>, b: Seq<Option<i64>>)
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
pub(super) proof fn lemma_vals_sub<T: VDelta>(a: Seq<Option<i64>>, i: int, j: int)
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
pub(super) fn insert_all<L, O, B>(core: &mut DeltaEditCore<L, O, B>, runs: &[DeltaRun]) where
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
    #[inline(always)]
    pub(super) fn edit_impl(&mut self) -> (e: VerifiedDeltaEdit<'_, T, C>)
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

    pub(super) fn edit_at_impl(&mut self, at: usize) -> (e: VerifiedDeltaEdit<'_, T, C>)
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
    pub(super) proof fn lemma_at_impl(&self)
        requires
            self.wf(),
        ensures
            0 <= self.at() <= self.orig().len() <= usize::MAX,
    {
        self.lemma_lens();
    }

    /// The values the cursor speaks of are as many as those its core
    /// has, and the cursor stands among them.
    pub(super) proof fn lemma_lens(&self)
        requires
            self.wf(),
        ensures
            self.orig().len() == self.core.rorig().len() == self.core.edit.orig_vals().len(),
            self.written().len() == self.core.rwritten().len() == self.core.edit.written().len(),
            0 <= self.core.at() <= self.core.rorig().len(),
    {
        self.core.edit.lemma_at();
    }

    #[inline(always)]
    pub(super) fn pos_impl(&self) -> (r: usize)
        ensures
            r == self.at(),
    {
        self.core.edit.pos()
    }

    #[inline(always)]
    pub(super) fn out_pos_impl(&self) -> (r: usize)
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
    pub(super) fn step_advance(&mut self, n: usize)
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
    pub(super) fn step_insert(&mut self, value: T, n: usize)
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
    pub(super) fn step_delete(&mut self, n: usize)
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

    #[inline(always)]
    pub(super) fn advance_impl(&mut self, n: usize) -> (r: &mut Self)
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

    #[inline(always)]
    pub(super) fn seek_impl(&mut self, to: usize) -> (r: &mut Self)
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

    #[inline(always)]
    pub(super) fn insert_impl(&mut self, value: T) -> (r: &mut Self)
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

    #[inline(always)]
    pub(super) fn insert_run_impl(&mut self, value: T, n: usize) -> (r: &mut Self)
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

    pub(super) fn insert_runs_impl(&mut self, runs: &[DeltaRun]) -> (r: &mut Self)
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

    #[allow(clippy::manual_map)]
    pub(super) fn peek_impl(&mut self) -> (r: Option<T>)
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

    pub(super) fn replace_with_impl(&mut self, value: T) -> (r: &mut Self)
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

    #[inline(always)]
    pub(super) fn delete_impl(&mut self, n: usize) -> (r: &mut Self)
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

    pub(super) fn finish_impl(self)
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
