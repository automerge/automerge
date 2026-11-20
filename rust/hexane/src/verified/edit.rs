//! Edits of a [`VerifiedColumn`] at a cursor: [`VerifiedEdit`], which
//! [`VerifiedColumn::edit`] and [`VerifiedColumn::edit_at`] open.

use super::kind::Kind;
use super::model::Model;
use super::seal;
#[cfg(verus_keep_ghost)]
use super::{after_edit, lemma_held_runs, lemma_svs_add, lemma_svs_sub, run_values, Held, VGet};
use super::{Get, LenIndex, RunsFeed, VCodec, VIndex, VValue, VerifiedColumn};
#[cfg(verus_keep_ghost)]
use crate::cursor::{Insertable, Slabs};
use crate::cursor::{ReadsAgree, RunFeed};
use crate::edit_core::EditCore;
use crate::{Leb128, Run};
use std::marker::PhantomData;
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use vstd::std_specs::iter::IteratorSpec;

verus! {

/// The shared cursor of an edit of a column of `V` kept with an index of
/// kind `K`.
type KEdit<V, C, K> = EditCore<
    <V as seal::Sealed<C>>::E,
    <K as Kind<V, C>>::W,
    <K as Kind<V, C>>::I,
    <K as Kind<V, C>>::G,
>;

/// An edit of a [`VerifiedColumn`] in progress: a cursor that walks the
/// column once, front to back, and writes the edited column as it goes.
/// [`VerifiedColumn::edit`] and [`VerifiedColumn::edit_at`] open one.
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
/// use hexane::verified::VerifiedColumn;
///
/// let mut col = VerifiedColumn::<u64>::from_values(&[1, 2, 3, 4, 5]);
/// let mut e = col.edit();
/// e.seek(1).delete(2).insert(9).seek(4).replace_with(7);
/// e.finish();
/// assert_eq!(col.to_vec(), [1, 9, 4, 7]);
/// ```
///
/// # An edit must be finished
///
/// While a cursor is open the column's slabs are the cursor's, and
/// `finish` is what gives them back. A cursor dropped without it leaves
/// the column without its values, as a panic in the middle of an edit
/// does: the column then reads as an empty one, and an edit of it
/// panics. Verus shows of a verified caller that it did call `finish`
/// wherever it says anything of the column afterwards, since only
/// `finish` says what the column is (`result()`).
///
/// # What a specification says of a cursor
///
/// A cursor `e` has the values of the column as it was opened
/// (`e.orig()`), the place among them it stands at (`e.at()`), and the
/// values it has written (`e.written()`). The column it will give is
/// `e.now()`: what is written, then the original values from `e.at()`
/// on. Every step says what it does to these three, and that the rest
/// stays as it was (`kept`).
#[must_use = "an edit gives the column back when `finish` is called"]
pub struct VerifiedEdit<'a, V: VValue<C>, C: VCodec = Leb128, K: VIndex<V, C> = LenIndex> {
    col: &'a mut VerifiedColumn<V, C, K>,
    core: KEdit<V, C, K>,
    /// the column's count of edits when the cursor was opened
    counter: usize,
    /// a step has deleted or inserted a value
    changed: bool,
}

/// Give the cursor `core` every run `feed` has, until it has no more.
#[verifier::exec_allows_no_decreases_clause]
#[inline(always)]
fn insert_feed<V: VValue<C>, C: VCodec, K: VIndex<V, C>, R: RunFeed<V::E>>(core: &mut KEdit<V, C, K>, feed: &mut R)
    requires
        old(core).inv(),
        old(core).held(),
        old(feed).inv(),
        old(feed).given() == Seq::<Held<V, C>>::empty(),
    ensures
        final(core).kept(*old(core)),
        final(core).at() == old(core).at(),
        final(core).written() == old(core).written() + final(feed).given(),
        final(feed).inv(),
        final(feed).key() == old(feed).key(),
        final(feed).ended(),
{
    let ghost w0 = core.written();
    proof {
        assert(w0 =~= w0 + feed.given());
    }
    loop
        invariant
            core.kept(*old(core)),
            core.at() == old(core).at(),
            core.written() == w0 + feed.given(),
            feed.inv(),
            feed.key() == old(feed).key(),
        ensures
            core.kept(*old(core)),
            core.at() == old(core).at(),
            core.written() == w0 + feed.given(),
            feed.inv(),
            feed.key() == old(feed).key(),
            feed.ended(),
    {
        let ghost g0 = feed.given();
        match feed.next_run() {
            None => {
                break ;
            },
            Some((x, n)) => {
                let ghost run = Seq::new(n as nat, |_i: int| x.val());
                core.insert(x, n);
                proof {
                    assert(w0 + (g0 + run) =~= (w0 + g0) + run);
                }
            },
        }
    }
}

impl<V: VValue<C>, C: VCodec, K: VIndex<V, C>> VerifiedColumn<V, C, K> {
    /// A cursor to edit the column with, standing at its first value.
    /// See [`VerifiedEdit`].
    #[inline(always)]
    pub fn edit(&mut self) -> (e: VerifiedEdit<'_, V, C, K>)
        ensures
            e.wf(),
            e.orig() == old(self)@,
            e.at() == 0,
            e.written() == Seq::<V::S>::empty(),
            !e.changed(),
            e.began() == old(self).edits(),
            e.result() == *final(self),
    {
        let e = self.edit_at(0);
        proof {
            assert(old(self)@.take(0) =~= Seq::<V::S>::empty());
        }
        e
    }

    /// A cursor to edit the column with, standing at the value at `at`
    /// (at the end, where `at` is the column's length), with the values
    /// before it kept. See [`VerifiedEdit`].
    ///
    /// The cursor's `result()` is the column as it will be when the
    /// borrow of it ends: what `finish` says of that is what is known of
    /// the column afterwards.
    pub fn edit_at(&mut self, at: usize) -> (e: VerifiedEdit<'_, V, C, K>)
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
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        self.core.lemma_column();
        let counter = self.core.counter();
        let core = EditCore::begin(&mut self.core, at);
        proof {
            lemma_svs_sub(old(self).core@, 0, at as int);
        }
        VerifiedEdit { col: self, core, counter, changed: false }
    }
}

impl<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>> VerifiedEdit<'a, V, C, K> {
    /// The cursor is one a column gave, or one a step of such a cursor
    /// left: every step asks for this and keeps it.
    pub closed spec fn wf(&self) -> bool {
        self.core.inv() && self.core.held() && self.core.orig_vals().len() <= usize::MAX
    }

    /// The values of the column as it was when the cursor was opened.
    pub closed spec fn orig(&self) -> Seq<V::S> {
        <Held<V, C> as VGet>::svs(self.core.orig_vals())
    }

    /// The place the cursor stands at, among the original values: the
    /// ones before it have been kept or deleted.
    pub closed spec fn at(&self) -> int {
        self.core.at()
    }

    /// The values written so far: the original values the cursor has
    /// kept and the ones inserted, in their order.
    pub closed spec fn written(&self) -> Seq<V::S> {
        <Held<V, C> as VGet>::svs(self.core.written())
    }

    /// What the column holds if the edit is finished now: what is
    /// written, then the original values from the cursor's place on.
    pub open spec fn now(&self) -> Seq<V::S> {
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
    pub closed spec fn result(&self) -> VerifiedColumn<V, C, K> {
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
        self.core.lemma_at();
        <Held<V, C> as VGet>::lemma_svs(self.core.orig_vals());
    }

    /// The cursor's place among the original values.
    #[inline(always)]
    pub fn pos(&self) -> (r: usize)
        ensures
            r == self.at(),
    {
        self.core.pos()
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
            <Held<V, C> as VGet>::lemma_svs(self.core.written());
        }
        self.core.out_pos()
    }

    /// The number of slabs the edit has rewritten so far.
    #[doc(hidden)]
    #[inline(always)]
    pub fn rebuilds(&self) -> usize {
        self.core.rebuilds()
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
            self.lemma_at();
            <Held<V, C> as VGet>::lemma_svs(self.core.orig_vals());
        }
        let mut f = <V::E as Model<V::X>>::blind();
        let _jumped = self.core.advance_with(n, &mut f);
        proof {
            let o = old(self).core;
            let s = o.step_len(n);
            lemma_svs_add(o.written(), o.orig_vals().subrange(o.at(), o.at() + s));
            lemma_svs_sub(o.orig_vals(), o.at(), o.at() + s);
        }
    }

    /// One step: write `n` copies of `value`.
    #[inline(always)]
    fn step_insert(&mut self, value: Get<'_, V>, n: usize)
        requires
            old(self).wf(),
        ensures
            final(self).kept(*old(self)),
            final(self).at() == old(self).at(),
            final(self).written() == old(self).written() + Seq::new(n as nat, |_i: int| value.sv()),
            final(self).changed() == (old(self).changed() || n > 0),
    {
        let x = <V::E as ReadsAgree<'_, V::X>>::ins(value);
        let ghost run = Seq::new(n as nat, |_i: int| x.val());
        self.core.insert(x, n);
        proof {
            V::lemma_own(value);
            lemma_svs_add(old(self).core.written(), run);
            <Held<V, C> as VGet>::lemma_svs(run);
            assert(<Held<V, C> as VGet>::svs(run) =~= Seq::new(n as nat, |_i: int| value.sv()));
        }
        self.changed = self.changed || n > 0;
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
            self.lemma_at();
            <Held<V, C> as VGet>::lemma_svs(self.core.orig_vals());
        }
        let left = self.core.remaining();
        let mut f = <V::E as Model<V::X>>::blind();
        self.core.delete_with(n, &mut f);
        self.changed = self.changed || (n > 0 && left > 0);
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
    /// goes forward: `to` is not before the place it stands at.
    ///
    /// # Panics
    ///
    /// If `to` is before the cursor, which the `requires` rules out of a
    /// verified caller.
    #[inline(always)]
    pub fn seek(&mut self, to: usize) -> (r: &mut Self)
        requires
            old(self).wf(),
            old(self).at() <= to,
        ensures
            *final(self) == *final(r),
            r.kept(*old(self)),
            r.at() == if to <= old(self).orig().len() {
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
        let n = to.checked_sub(self.core.pos()).expect("VerifiedEdit::seek is forward-only");
        self.step_advance(n);
        self
    }

    /// Write `value` before the value the cursor stands at.
    #[inline(always)]
    pub fn insert(&mut self, value: Get<'_, V>) -> (r: &mut Self)
        requires
            old(self).wf(),
        ensures
            *final(self) == *final(r),
            r.kept(*old(self)),
            r.at() == old(self).at(),
            r.written() == old(self).written().push(value.sv()),
            r.changed(),
    {
        self.step_insert(value, 1);
        proof {
            assert(self.written() =~= old(self).written().push(value.sv()));
        }
        self
    }

    /// Write `n` copies of `value` before the value the cursor stands
    /// at; none, where `n` is 0.
    #[inline(always)]
    pub fn insert_run(&mut self, value: Get<'_, V>, n: usize) -> (r: &mut Self)
        requires
            old(self).wf(),
        ensures
            *final(self) == *final(r),
            r.kept(*old(self)),
            r.at() == old(self).at(),
            r.written() == old(self).written() + Seq::new(n as nat, |_i: int| value.sv()),
            r.changed() == (old(self).changed() || n > 0),
    {
        self.step_insert(value, n);
        self
    }

    /// Write, before the value the cursor stands at, the runs that
    /// `runs` gives, as an iterator: of each run its value, as many
    /// times as its count says (`run_values`).
    ///
    /// The specification speaks of the iterator as
    /// [`splice_runs`](VerifiedColumn::splice_runs)'s does. The cursor
    /// has changed the column if a value was written.
    pub fn insert_runs<'v, I: IntoIterator<Item = Run<Get<'v, V>>>>(&mut self, runs: I) -> (r: &mut Self)
        requires
            old(self).wf(),
        ensures
            *final(self) == *final(r),
            r.kept(*old(self)),
            r.at() == old(self).at(),
            exists|it: I::IntoIter| #[trigger]
                (I::into_iter).ensures((runs,), it) && (it.obeys_prophetic_iter_laws() ==> r.written() == old(
                    self,
                ).written() + run_values(it.remaining())),
            r.changed() == (old(self).changed() || r.written().len() != old(self).written().len()),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core.written());
        }
        let before = self.core.out_pos();
        let it = runs.into_iter();
        let ghost it0 = it;
        let mut feed = RunsFeed::<V, C, I::IntoIter> {
            it,
            init: Ghost(it0),
            given: Ghost(Seq::empty()),
            ended: Ghost(false),
            _c: PhantomData,
        };
        proof {
            assert(feed.given@ + feed.it.remaining() =~= it0.remaining());
        }
        insert_feed::<V, C, K, _>(&mut self.core, &mut feed);
        let after = self.core.out_pos();
        proof {
            if it0.obeys_prophetic_iter_laws() {
                let rs = it0.remaining();
                assert(feed.given@ =~= feed.given@ + feed.it.remaining());
                lemma_held_runs::<V, C>(rs);
            }
            lemma_svs_add(old(self).core.written(), feed.given());
            <Held<V, C> as VGet>::lemma_svs(self.core.written());
        }
        self.changed = self.changed || after != before;
        self
    }

    /// The value the cursor stands at, or `None` at the end. The cursor
    /// stays where it is.
    pub fn peek(&mut self) -> (r: Option<Get<'_, V>>)
        requires
            old(self).wf(),
        ensures
            final(self).unmoved(*old(self)),
            match r {
                Some(v) => old(self).at() < old(self).orig().len() && v.sv() == old(self).orig()[old(self).at()],
                None => old(self).at() == old(self).orig().len(),
            },
    {
        let ghost (ov, a) = (self.core.orig_vals(), self.core.at());
        proof {
            self.core.lemma_at();
            <Held<V, C> as VGet>::lemma_svs(ov);
        }
        let r = if self.core.dirty() {
            self.core.rebuild_reader()
        } else {
            self.core.reader()
        };
        match r {
            Some((st, slab)) => {
                let ghost k = choose|k: nat|
                    #![trigger <V::E as Slabs>::st_at(slab.data@, *st, k)]
                    k < <V::E as Slabs>::vals(*slab).len() && <V::E as Slabs>::st_at(slab.data@, *st, k) && <V::E as Slabs>::vals(
                        *slab,
                    )[k as int] == ov[a];
                let v = <V::E as Model<V::X>>::peek(slab, st, Ghost(k));
                proof {
                    V::lemma_own(v);
                }
                Some(v)
            },
            None => None,
        }
    }

    /// Put `value` in the place of the value the cursor stands at, and
    /// stand after it. At the end, where there is no value to replace,
    /// this does nothing.
    ///
    /// Where the value there is `value` already it is kept, and the
    /// cursor has not changed the column by this.
    pub fn replace_with(&mut self, value: Get<'_, V>) -> (r: &mut Self)
        requires
            old(self).wf(),
        ensures
            *final(self) == *final(r),
            r.kept(*old(self)),
            if old(self).at() < old(self).orig().len() {
                &&& r.at() == old(self).at() + 1
                &&& r.written() == old(self).written().push(value.sv())
                &&& r.changed() == (old(self).changed() || old(self).orig()[old(self).at()] != value.sv())
            } else {
                &&& r.at() == old(self).at()
                &&& r.written() == old(self).written()
                &&& r.changed() == old(self).changed()
            },
    {
        let (there, same) = match self.peek() {
            Some(cur) => (true, V::same(cur, value)),
            None => (false, false),
        };
        if there {
            if same {
                self.step_advance(1);
                proof {
                    let o = *old(self);
                    o.lemma_at();
                    assert(o.orig().subrange(o.at(), o.at() + 1) =~= seq![value.sv()]);
                    assert(self.written() =~= o.written().push(value.sv()));
                }
            } else {
                self.step_delete(1);
                self.step_insert(value, 1);
                proof {
                    assert(self.written() =~= old(self).written().push(value.sv()));
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
    /// Where the column's kind of index is not kept for the edited
    /// values (`VKeeps::keeps`), as an edit by `splice` does.
    pub fn finish(self)
        requires
            self.wf(),
        ensures
            self.result()@ == self.now(),
            self.result().edits() == if self.changed() {
                after_edit(self.began())
            } else {
                self.began()
            },
    {
        let VerifiedEdit { col: column, core: cursor, counter: before, changed: edit } = self;
        proof {
            cursor.lemma_at();
            let a = cursor.orig_vals();
            <Held<V, C> as VGet>::lemma_svs(a);
            lemma_svs_add(cursor.written(), a.skip(cursor.at()));
            lemma_svs_sub(a, cursor.at(), a.len() as int);
        }
        cursor.end(&mut column.core);
        column.count_call(before, edit);
    }
}

} // verus!
