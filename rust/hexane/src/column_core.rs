//! A column's slabs, index and counts, with the column invariant as a
//! type invariant, verified with Verus.
//!
//! [`Column`](crate::Column) keeps its state in a [`ColumnCore`]: the
//! slabs, the index over them, the item count, the segment budget and the
//! edit counter. The core's fields are private to this module, and its
//! type invariant is the column invariant (`col_ok`, with room for three
//! more items, as the edit cursor counts). Every way of making or changing
//! a core is verified here and keeps the invariant, so verified code that
//! holds a core has it without being given it (`use_type_invariant`).
//! The core's view is the column's values, one after another, and the
//! operations are specified against it: a splice replaces exactly the
//! range given, `copy_ranges` applies its splices, and an iterator over a
//! range (`reader`, `resume`) gives exactly the values there. The readers
//! see slabs through their own model of the encoding; that it agrees with
//! the edit model (`cursor::ReadsAgree`) is proved for both encodings, so
//! their preconditions come from the type invariant too.
//!
//! Trusted: what the constructors are given (well-formed slabs and their
//! weights, from the loaders, fills and encoders); and the parts the
//! public edit cursor ([`crate::edit::Edit`]) hands back
//! ([`ColumnCore::restore`]), which its verified steps keep a column but
//! its glue chains.

// Verus has no spec for a slice's `is_empty`.
#![allow(clippy::len_zero)]

use crate::column::Slab;
use crate::copy::{copy_direct, copy_invert, RangeFeed};
#[cfg(verus_keep_ghost)]
use crate::cursor::{all_ok, col_ok, lemma_vals_total, slab_lens, vals_of, ws_ok};
use crate::cursor::{
    col_index, splice_feed, ReadsAgree, RunFeed, Slabs, Visitor, Weigh, WeightSink,
};
#[cfg(verus_keep_ghost)]
use crate::cursor::{lemma_lens_ok, lemma_vals_own};
#[cfg(verus_keep_ghost)]
use crate::fenwick::total;
#[cfg(verus_keep_ghost)]
use crate::index::wlens;
use crate::index::IndexOps;
#[cfg(verus_keep_ghost)]
use crate::iter_core::Decode;
use crate::iter_core::{Decoders, ICore};
#[cfg(verus_keep_ghost)]
use crate::splices::{applied, normal};
use crate::Splice;
use std::marker::PhantomData;
use vstd::prelude::*;

verus! {

/// Panics: a segment budget below 2, which no column can keep.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn budget_too_small()
    ensures
        false,
{
    panic!("max_segments must be at least 2")
}

/// Panics: a column used after a panic in the middle of an edit, which
/// left it without its parts.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn poisoned()
    ensures
        false,
{
    panic!("column used after a panic during an edit")
}

/// Panics: a column of `usize::MAX` slabs, which no memory holds (each
/// slab takes some), but which Verus does not rule out.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn too_many_slabs()
    ensures
        false,
{
    panic!("a column of usize::MAX slabs")
}

/// A column's parts: its slabs, the index over them, its item count and
/// its edit counter.
pub(crate) struct Parts<T: Copy + Clone + std::fmt::Debug + Default, I> {
    pub(crate) slabs: Vec<Slab<T>>,
    pub(crate) index: I,
    pub(crate) total_len: usize,
    pub(crate) counter: usize,
}

/// A column's state: its parts and its segment budget, the parts always
/// a column (`inv`). Only while an edit has them out are they missing,
/// and a column left so by a panic panics when used again.
pub(crate) struct ColumnCore<E: Slabs, W, I: IndexOps<W>, G: Weigh<E::Tail, W>> {
    state: Option<Parts<E::Tail, I>>,
    max_segments: usize,
    _p: PhantomData<(E, W, G)>,
}

/// The parts make a column with budget `max_segments`, with room for
/// three more items, as the edit cursor counts.
pub(crate) open spec fn parts_ok<E: Slabs, W, I: IndexOps<W>, G: Weigh<E::Tail, W>>(
    p: Parts<E::Tail, I>,
    max_segments: usize,
) -> bool {
    &&& col_ok::<E, W, I, G>(p.slabs@, p.index, p.total_len, max_segments)
    &&& p.total_len + 3 <= usize::MAX
}

impl<E: Slabs, W, I: IndexOps<W>, G: Weigh<E::Tail, W>> ColumnCore<E, W, I, G> {
    /// The column invariant.
    #[verifier::type_invariant]
    spec fn inv(&self) -> bool {
        &&& self.max_segments >= 2
        &&& self.state matches Some(p) ==> parts_ok::<E, W, I, G>(p, self.max_segments)
    }

    /// The core holds its parts (no edit has them out).
    pub(crate) closed spec fn ready(&self) -> bool {
        self.state is Some
    }

    /// The column's values, one after another.
    pub(crate) closed spec fn view(&self) -> Seq<E::V> {
        match self.state {
            Some(p) => vals_of::<E>(p.slabs@),
            None => Seq::empty(),
        }
    }

    pub(crate) closed spec fn slabs_spec(&self) -> Seq<Slab<E::Tail>> {
        match self.state {
            Some(p) => p.slabs@,
            None => Seq::empty(),
        }
    }

    pub(crate) closed spec fn index_spec(&self) -> I {
        self.state->Some_0.index
    }

    pub(crate) closed spec fn budget(&self) -> usize {
        self.max_segments
    }

    pub(crate) closed spec fn count(&self) -> usize {
        match self.state {
            Some(p) => p.counter,
            None => 0,
        }
    }

    /// What the invariant says of the parts the accessors hand out: the
    /// slabs are a column, indexed, holding the view.
    #[inline(always)]
    pub(crate) fn lemma_column(&self)
        ensures
            self.ready() ==> col_ok::<E, W, I, G>(self.slabs_spec(), self.index_spec(), self@.len() as usize, self.budget()),
            self@ == vals_of::<E>(self.slabs_spec()),
            self@.len() == total(slab_lens(self.slabs_spec())),
            self@.len() + 3 <= usize::MAX,
            self.budget() >= 2,
    {
        proof {
            use_type_invariant(self);
            if self.state is Some {
                lemma_vals_total::<E>(self.state->Some_0.slabs@);
            } else {
                assert(vals_of::<E>(Seq::<Slab<E::Tail>>::empty()) =~= Seq::<E::V>::empty());
                assert(slab_lens(Seq::<Slab<E::Tail>>::empty()) =~= Seq::<nat>::empty());
            }
        }
    }

    /// An empty column with segment budget `max_segments`; panics if
    /// that is below 2.
    pub(crate) fn new(max_segments: usize) -> (r: Self)
        ensures
            r.ready(),
            r@.len() == 0,
            r.budget() == max_segments,
            r.count() == 0,
    {
        if max_segments < 2 {
            budget_too_small();
        }
        let slabs: Vec<Slab<E::Tail>> = Vec::new();
        let index = I::ix_empty();
        proof {
            I::lemma_plain();
            assert(slabs@ =~= Seq::<Slab<E::Tail>>::empty());
            assert(slab_lens(slabs@) =~= Seq::<nat>::empty());
            assert(wlens::<W, I>(index.ws()) =~= Seq::<nat>::empty());
            assert(vals_of::<E>(slabs@) =~= Seq::<E::V>::empty());
        }
        ColumnCore { state: Some(Parts { slabs, index, total_len: 0, counter: 0 }), max_segments, _p: PhantomData }
    }

    /// The column of well-formed slabs `slabs`, each weighed by `ws`:
    /// `None` if a weight does not count its slab's items, or the items
    /// are more than an edit can count (`usize::MAX - 3`).
    pub(crate) fn from_slabs(slabs: Vec<Slab<E::Tail>>, ws: Vec<W>, max_segments: usize) -> (r: Option<Self>)
        requires
            all_ok::<E>(slabs@),
            ws_ok::<E::Tail, W, G>(ws@, slabs@),
        ensures
            r matches Some(c) ==> c.ready() && c@ == vals_of::<E>(slabs@) && c.budget() == max_segments
                && c.count() == 0,
    {
        if max_segments < 2 {
            budget_too_small();
        }
        proof {
            I::lemma_plain();
        }
        let (index, total_len) = col_index::<E::Tail, W, I>(slabs.as_slice(), ws)?;
        if total_len > usize::MAX - 3 {
            return None;
        }
        Some(ColumnCore { state: Some(Parts { slabs, index, total_len, counter: 0 }), max_segments, _p: PhantomData })
    }

    #[inline(always)]
    pub(crate) fn len(&self) -> (n: usize)
        ensures
            n == self@.len(),
    {
        self.lemma_column();
        proof {
            use_type_invariant(self);
        }
        match &self.state {
            Some(p) => p.total_len,
            None => 0,
        }
    }

    #[inline(always)]
    pub(crate) fn slabs(&self) -> (r: &[Slab<E::Tail>])
        ensures
            r@ == self.slabs_spec(),
    {
        match &self.state {
            Some(p) => p.slabs.as_slice(),
            None => &[],
        }
    }

    /// The index over the slabs; panics if an edit's panic left the
    /// column without its parts.
    #[inline(always)]
    pub(crate) fn index(&self) -> (r: &I)
        ensures
            self.ready() && *r == self.index_spec(),
    {
        match &self.state {
            Some(p) => &p.index,
            None => {
                poisoned();
                unreached()
            },
        }
    }

    #[inline(always)]
    pub(crate) fn max_segments(&self) -> (n: usize)
        ensures
            n == self.budget(),
    {
        self.max_segments
    }

    #[inline(always)]
    pub(crate) fn counter(&self) -> (n: usize)
        ensures
            n == self.count(),
    {
        match &self.state {
            Some(p) => p.counter,
            None => 0,
        }
    }

    /// An iterator over items `start..end` of the column, through a read
    /// model that agrees with the edit model: it stands at `start` with
    /// `end - start` to give, exactly the column's values there. The
    /// readers' preconditions come from the type invariant here.
    #[inline(always)]
    pub(crate) fn reader<'a, X: Decoders<'a, Tail = E::Tail>>(&'a self, start: usize, end: usize) -> (it: ICore<X::D>)
        where E: ReadsAgree<'a, X>
        requires
            start <= end <= self@.len(),
        ensures
            it.wf::<X>(self.slabs_spec()),
            it.pos == start,
            it.out::<X>(self.slabs_spec()) == crate::iter_core::vals_of::<X>(self.slabs_spec()).subrange(start as int, end as int),
            it.out::<X>(self.slabs_spec()).map_values(|v: <X::D as Decode>::Item| E::own(v))
                == self@.subrange(start as int, end as int),
    {
        self.lemma_column();
        let slabs = self.slabs();
        proof {
            lemma_lens_ok::<E, X>(slabs@);
            lemma_vals_own::<E, X>(slabs@);
            let r = crate::iter_core::vals_of::<X>(slabs@);
            assert(r.subrange(start as int, end as int).map_values(|v: <X::D as Decode>::Item| E::own(v))
                =~= r.map_values(|v: <X::D as Decode>::Item| E::own(v)).subrange(start as int, end as int));
        }
        if slabs.len() == usize::MAX {
            too_many_slabs();
        }
        let len = self.len();
        if start >= len || slabs.len() == 0 {
            let it = ICore::idle::<X>(slabs, start);
            proof {
                it.lemma_here::<X>(slabs@);
                let v = crate::iter_core::vals_of::<X>(slabs@);
                assert(v.subrange(start as int, end as int) =~= Seq::<<X::D as Decode>::Item>::empty());
            }
            return it;
        }
        proof {
            use_type_invariant(self);
        }
        let (si, offset) = self.index().ix_find(start);
        let it = ICore::at::<X>(slabs, si, offset, start, end - start);
        proof {
            it.lemma_here::<X>(slabs@);
        }
        it
    }

    /// Resume an iterator at a suspended position (`ICore::resume`), the
    /// readers' preconditions from the type invariant: `Some` exactly
    /// when the position is one of the column's.
    #[inline(always)]
    pub(crate) fn resume<'a, X: Decoders<'a, Tail = E::Tail>>(
        &'a self,
        si: usize,
        consumed: usize,
        pos: usize,
        n: usize,
    ) -> (r: Option<ICore<X::D>>)
        where E: ReadsAgree<'a, X>
        ensures
            r is Some <==> crate::iter_core::resumes(slab_lens(self.slabs_spec()), si, consumed, pos, n),
            r matches Some(it) ==> it.wf::<X>(self.slabs_spec()) && it.pos == pos,
    {
        self.lemma_column();
        let slabs = self.slabs();
        proof {
            lemma_lens_ok::<E, X>(slabs@);
            use_type_invariant(self);
        }
        if slabs.len() == usize::MAX {
            too_many_slabs();
        }
        ICore::resume::<X, W, I>(slabs, self.index(), si, consumed, pos, n)
    }

    /// Take the column's parts out, for an edit; the column has none until
    /// they are put back ([`Self::restore`]). Panics if an edit's panic
    /// left it without them.
    #[inline(always)]
    pub(crate) fn take_parts(&mut self) -> (r: Parts<E::Tail, I>)
        ensures
            parts_ok::<E, W, I, G>(r, old(self).budget()),
            vals_of::<E>(r.slabs@) == old(self)@,
            r.counter == old(self).count(),
            !final(self).ready(),
            final(self).budget() == old(self).budget(),
    {
        proof {
            use_type_invariant(&*self);
        }
        match self.state.take() {
            Some(p) => p,
            None => {
                poisoned();
                unreached()
            },
        }
    }

    /// Put back parts, as an edit left them.
    #[inline(always)]
    pub(crate) fn restore(&mut self, p: Parts<E::Tail, I>)
        requires
            parts_ok::<E, W, I, G>(p, old(self).budget()),
        ensures
            final(self).ready(),
            final(self)@ == vals_of::<E>(p.slabs@),
            final(self).budget() == old(self).budget(),
            final(self).count() == p.counter,
    {
        proof {
            use_type_invariant(&*self);
        }
        self.state = Some(p);
    }

    /// Set the edit counter.
    pub(crate) fn set_counter(&mut self, counter: usize)
        ensures
            final(self)@ == old(self)@,
            final(self).budget() == old(self).budget(),
            final(self).count() == counter,
            final(self).ready(),
    {
        let mut p = self.take_parts();
        p.counter = counter;
        self.restore(p);
    }

    /// Exchange two columns' segment budgets, keeping their values.
    pub(crate) fn swap_budgets(&mut self, other: &mut Self)
        ensures
            final(self)@ == old(self)@,
            final(other)@ == old(other)@,
            final(self).budget() == old(other).budget(),
            final(other).budget() == old(self).budget(),
            final(self).count() == old(self).count(),
            final(other).count() == old(other).count(),
            final(self).ready(),
            final(other).ready(),
    {
        let (a, b) = (self.max_segments, other.max_segments);
        proof {
            use_type_invariant(&*self);
            use_type_invariant(&*other);
        }
        let pa = self.take_parts();
        let pb = other.take_parts();
        *self = ColumnCore { state: Some(pa), max_segments: b, _p: PhantomData };
        *other = ColumnCore { state: Some(pb), max_segments: a, _p: PhantomData };
    }

    /// Delete `del` items at `at` and insert every run `feed` gives there,
    /// in one verified chain of cursor steps (`cursor::splice_feed`):
    /// the column holds its values before `at`, the values inserted, then
    /// its values from `at + del`.
    #[inline(always)]
    pub(crate) fn splice<F: Visitor<E>, S: WeightSink<W>, R: RunFeed<E>>(
        &mut self,
        at: usize,
        del: usize,
        f: &mut F,
        g: &mut S,
        feed: &mut R,
    ) -> (ins: Ghost<Seq<E::V>>)
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
        ensures
            final(self)@ == old(self)@.take(at as int) + ins@ + old(self)@.skip(at + del),
            final(feed).given() == old(feed).given() + ins@,
            final(feed).ended(),
            final(self).budget() == old(self).budget(),
            final(self).ready(),
    {
        let max_segments = self.max_segments;
        let mut p = self.take_parts();
        proof {
            lemma_vals_total::<E>(p.slabs@);
        }
        let ins = splice_feed::<E, W, I, G, F, S, R>(
            &mut p.slabs,
            &mut p.index,
            &mut p.total_len,
            max_segments,
            &mut p.counter,
            at,
            del,
            f,
            g,
            feed,
        );
        self.restore(p);
        ins
    }

    /// `copy_ranges`, directly: apply the normalized splices `sps` to this
    /// column, the source's items from `feed` (`copy::copy_direct`).
    #[inline(always)]
    pub(crate) fn copy_direct<F: Visitor<E>, S: WeightSink<W>, R: RangeFeed<E>>(
        &mut self,
        sps: &[Splice],
        f: &mut F,
        g: &mut S,
        feed: &mut R,
    )
        requires
            normal(sps@, old(feed).src().len() as int, old(self)@.len() as int),
            old(feed).at() == 0,
        ensures
            final(self)@ == applied(old(self)@, old(feed).src(), sps@),
            final(self).budget() == old(self).budget(),
            final(self).ready(),
    {
        let max_segments = self.max_segments;
        let mut p = self.take_parts();
        proof {
            lemma_vals_total::<E>(p.slabs@);
        }
        copy_direct::<E, W, I, G, F, S, R>(&mut p.slabs, &mut p.index, &mut p.total_len, max_segments, &mut p.counter, sps, f, g, feed);
        self.restore(p);
    }

    /// `copy_ranges`, inverted: this column holds the source; keep the
    /// normalized splices `sps`' ranges of it and put the destination's
    /// items, from `feed`, back between them (`copy::copy_invert`).
    #[inline(always)]
    pub(crate) fn copy_invert<F: Visitor<E>, S: WeightSink<W>, R: RangeFeed<E>>(
        &mut self,
        sps: &[Splice],
        dst_len: usize,
        f: &mut F,
        g: &mut S,
        feed: &mut R,
    )
        requires
            normal(sps@, old(self)@.len() as int, dst_len as int),
            old(feed).src().len() == dst_len,
            old(feed).at() == 0,
        ensures
            final(self)@ == applied(old(feed).src(), old(self)@, sps@),
            final(self).budget() == old(self).budget(),
            final(self).ready(),
    {
        let max_segments = self.max_segments;
        let mut p = self.take_parts();
        proof {
            lemma_vals_total::<E>(p.slabs@);
        }
        copy_invert::<E, W, I, G, F, S, R>(
            &mut p.slabs,
            &mut p.index,
            &mut p.total_len,
            max_segments,
            &mut p.counter,
            sps,
            dst_len,
            f,
            g,
            feed,
        );
        self.restore(p);
    }
}

/// Never returns: after a call that does not.
#[verifier::external_body]
#[inline(always)]
fn unreached<A>() -> A
    requires
        false,
{
    unreachable!()
}

} // verus!

// Outside `verus!`: a clone is the same column (std's `Vec::clone` and the
// index's own `Clone`), trusted like the derived clones of verified state.
impl<T: Copy + Clone + std::fmt::Debug + Default, I: Clone> Clone for Parts<T, I> {
    fn clone(&self) -> Self {
        Parts {
            slabs: self.slabs.clone(),
            index: self.index.clone(),
            total_len: self.total_len,
            counter: self.counter,
        }
    }
}

impl<E: Slabs, W, I: IndexOps<W> + Clone, G: Weigh<E::Tail, W>> Clone for ColumnCore<E, W, I, G> {
    fn clone(&self) -> Self {
        ColumnCore {
            state: self.state.clone(),
            max_segments: self.max_segments,
            _p: PhantomData,
        }
    }
}
