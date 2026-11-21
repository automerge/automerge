//! The implementation of `verified::edit` and its proofs: what its
//! specification (`edit.rs`) says. Each function named `..._impl` here has
//! the contract of the function there that calls it, word for word; a
//! function named after a trait, a method and a type (`vpack_pack_u64`)
//! is the body of that method in that type's impl of the trait, with the
//! trait's contract said of the type.

#![allow(unused_imports)]

#[cfg(verus_keep_ghost)]
use super::super::{lemma_held_runs, lemma_svs_add, lemma_svs_sub};
use super::*;

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
pub(super) type KEdit<V, C, K> = EditCore<
    <V as seal::Sealed<C>>::E,
    <K as Kind<V, C>>::W,
    <K as Kind<V, C>>::I,
    <K as Kind<V, C>>::G,
>;

/// Give the cursor `core` every run `feed` has, until it has no more.
#[verifier::exec_allows_no_decreases_clause]
#[inline(always)]
pub(super) fn insert_feed<V: VValue<C>, C: VCodec, K: VIndex<V, C>, R: RunFeed<V::E>>(core: &mut KEdit<V, C, K>, feed: &mut R)
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
    #[inline(always)]
    pub(super) fn edit_impl(&mut self) -> (e: VerifiedEdit<'_, V, C, K>)
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

    pub(super) fn edit_at_impl(&mut self, at: usize) -> (e: VerifiedEdit<'_, V, C, K>)
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
    pub(super) proof fn lemma_at_impl(&self)
        requires
            self.wf(),
        ensures
            0 <= self.at() <= self.orig().len() <= usize::MAX,
    {
        self.core.lemma_at();
        <Held<V, C> as VGet>::lemma_svs(self.core.orig_vals());
    }

    #[inline(always)]
    pub(super) fn pos_impl(&self) -> (r: usize)
        ensures
            r == self.at(),
    {
        self.core.pos()
    }

    #[inline(always)]
    pub(super) fn out_pos_impl(&self) -> (r: usize)
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

    #[inline(always)]
    pub(super) fn rebuilds_impl(&self) -> usize
    {
        self.core.rebuilds()
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
    pub(super) fn step_insert(&mut self, value: Get<'_, V>, n: usize)
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
            self.lemma_at();
            <Held<V, C> as VGet>::lemma_svs(self.core.orig_vals());
        }
        let left = self.core.remaining();
        let mut f = <V::E as Model<V::X>>::blind();
        self.core.delete_with(n, &mut f);
        self.changed = self.changed || (n > 0 && left > 0);
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

    #[inline(always)]
    pub(super) fn insert_impl(&mut self, value: Get<'_, V>) -> (r: &mut Self)
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

    #[inline(always)]
    pub(super) fn insert_run_impl(&mut self, value: Get<'_, V>, n: usize) -> (r: &mut Self)
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

    pub(super) fn insert_runs_impl<'v, I: IntoIterator<Item = Run<Get<'v, V>>>>(&mut self, runs: I) -> (r: &mut Self)
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

    pub(super) fn peek_impl(&mut self) -> (r: Option<Get<'_, V>>)
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

    pub(super) fn replace_with_impl(&mut self, value: Get<'_, V>) -> (r: &mut Self)
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
