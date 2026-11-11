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
//! A column's weight model can ask more of its values as a whole
//! (`Weigh::dom_ok`): a delta column's, that every step between two of
//! its running sums fits `i64`, which its readers need. That is in the
//! invariant too: an edit checks it before it puts the parts back, a
//! delta splice cheaply, from its plan's bound on the values
//! ([`ColumnCore::splice_delta`]), and so the delta readers' invariant
//! follows from the type invariant ([`ColumnCore::delta_parts`]).
//!
//! A load builds its core in verified code (`load_core::LoadCore::finish`),
//! so a loaded column's invariant is proved from its bytes.
//!
//! The public edit cursor takes a column's parts and hands them back
//! checked to be in the domain in verified code too
//! (`edit_core::EditCore`), each of its steps keeping them a column.
//!
//! Trusted: that the slabs a fill or an encoder hands
//! [`ColumnCore::from_cut`] are well-formed, as those verified functions
//! promise (it weighs them and checks the domain itself); that the edit
//! cursor's glue chains its verified steps; for a delta column, that
//! `DeltaEdit`'s domain check, which spares its cursor the one on handing
//! back, is of the column it leaves; and a clone.

// Verus has no spec for a slice's `is_empty`.
#![allow(clippy::len_zero)]

#[cfg(verus_keep_ghost)]
use crate::btree::{
    axiom_prefix_add, lemma_prefix_len, lemma_prefix_sum, prefix_add, prefix_zero, PrefixAggregate,
    SlabAggregate,
};
use crate::btree::{SlabAgg, SlabBTree};
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::copy::RangeFeed;
use crate::copy::{copy_direct, copy_invert, SrcFeed};
#[cfg(verus_keep_ghost)]
use crate::cursor::{all_ok, col_ok, lemma_vals_total, slab_lens, vals_of, ws_ok};
use crate::cursor::{col_index, splice_feed, ReadsAgree, RunFeed, Slabs, Visitor, Weigh};
#[cfg(verus_keep_ghost)]
use crate::cursor::{lemma_lens_ok, lemma_vals_own};
#[cfg(verus_keep_ghost)]
use crate::delta::agg::{agg_of, deltas_of, reps, steps_fit};
#[cfg(verus_keep_ghost)]
use crate::delta::dbound::{bounds_of, lemma_spliced_vals, span_ok, spliced};
use crate::delta::dbound::{root_bound, SpanLedger, SpanPlan, ToDeltas};
use crate::delta::dcopy::DSrc;
#[cfg(verus_keep_ghost)]
use crate::delta::ddomain::{
    dslabs, items_vals, lemma_dix_ok, lemma_dslabs_own, lemma_span_steps, lemma_splice_deltas,
    rvals,
};
use crate::delta::ddomain::{
    slabs_fit, DeltaFeed, DeltaWrite, SpliceItem, SpliceItems, ValueItems, ValuesFeed,
};
use crate::delta::dedit::d_boundaries;
use crate::delta::diter::d_nth;
#[cfg(verus_keep_ghost)]
use crate::delta::diter::{crop, dindex_ok, lemma_crop};
#[cfg(verus_keep_ghost)]
use crate::fenwick::total;
#[cfg(verus_keep_ghost)]
use crate::index::lemma_btree_index;
#[cfg(verus_keep_ghost)]
use crate::index::wlens;
use crate::index::IndexOps;
#[cfg(verus_keep_ghost)]
use crate::iter_core::lens_ok;
use crate::iter_core::{Decode, Decoders, ICore};
use crate::prefix::PrefixSlabWeight;
#[cfg(verus_keep_ghost)]
use crate::prefix_core::{before, ix_ok, vsum};
use crate::prefix_core::{p_prefix, p_sum_range, PrefixOps};
#[cfg(verus_keep_ghost)]
use crate::prefix_search::found_at;
use crate::prefix_search::{p_index_for_prefix, PrefixOrd};
#[cfg(verus_keep_ghost)]
use crate::splices::applied;
use crate::splices::{wrong_columns, Normalized};
use crate::weights::{DeltaModel, PrefixModel};
use std::marker::PhantomData;
use std::ops::{Add, AddAssign, SubAssign};
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

/// Panics: an edit that left a column's values outside its domain (a
/// delta column's spanning 2^63 or more), which no column can hold. The
/// column is left without its parts.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn out_of_domain()
    ensures
        false,
{
    panic!("delta values out of domain: they would span 2^63 or more")
}

/// Panics: a delta column of more than `i64::MAX` items, whose steps
/// cannot all fit `i64`. The column is left without its parts.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn too_many_deltas()
    ensures
        false,
{
    panic!("a delta column of more than i64::MAX items")
}

/// Panics: a column of more items than an edit can count.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn too_many_items()
    ensures
        false,
{
    panic!("a column of more than usize::MAX - 3 items")
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
pub(crate) struct ColumnCore<E: Slabs, W, I: IndexOps<W>, G: Weigh<E, W>> {
    state: Option<Parts<E::Tail, I>>,
    max_segments: usize,
    _p: PhantomData<(E, W, G)>,
}

/// The parts make a column with budget `max_segments`, with room for
/// three more items, as the edit cursor counts, in its weight model's
/// domain (only a delta column's asks anything).
pub(crate) open spec fn parts_ok<E: Slabs, W, I: IndexOps<W>, G: Weigh<E, W>>(
    p: Parts<E::Tail, I>,
    max_segments: usize,
) -> bool {
    &&& col_ok::<E, W, I, G>(p.slabs@, p.index, p.total_len, max_segments)
    &&& p.total_len + 3 <= usize::MAX
    &&& G::dom_ok(p.slabs@)
}

impl<E: Slabs, W, I: IndexOps<W>, G: Weigh<E, W>> ColumnCore<E, W, I, G> {
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
            self.ready() ==> G::dom_ok(self.slabs_spec()),
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
            G::lemma_dom_empty(slabs@);
        }
        ColumnCore { state: Some(Parts { slabs, index, total_len: 0, counter: 0 }), max_segments, _p: PhantomData }
    }

    /// The column of well-formed slabs `slabs`, each weighed by `ws`, in
    /// the weight model's domain (as a delta column's loader checks):
    /// `None` if a weight does not count its slab's items, or the items
    /// are more than an edit can count (`usize::MAX - 3`).
    pub(crate) fn from_slabs(slabs: Vec<Slab<E::Tail>>, ws: Vec<W>, max_segments: usize) -> (r: Option<Self>)
        requires
            all_ok::<E>(slabs@),
            ws_ok::<E, W, G>(ws@, slabs@),
            G::dom_ok(slabs@),
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

    /// The column of well-formed slabs `slabs` (cut by a verified encoder
    /// or fill), each weighed by its weight model, with segment budget
    /// `max_segments`. Panics if the items are more than an edit can count
    /// (`usize::MAX - 3`), or the values are outside the weight model's
    /// domain (`Weigh::dom_check`).
    pub(crate) fn from_cut(slabs: Vec<Slab<E::Tail>>, max_segments: usize) -> (r: Self)
        requires
            all_ok::<E>(slabs@),
        ensures
            r.ready(),
            r@ == vals_of::<E>(slabs@),
            r.budget() == max_segments,
    {
        let ghost s = slabs@;
        let mut ws: Vec<W> = Vec::with_capacity(slabs.len());
        let mut len: usize = 0;
        let mut k: usize = 0;
        while k < slabs.len()
            invariant
                s == slabs@,
                all_ok::<E>(s),
                0 <= k <= s.len(),
                ws@.len() == k,
                len == crate::fenwick::sum(slab_lens(s), 0, k as int),
                forall|j: int| 0 <= j < k ==> G::wrel(#[trigger] ws@[j], s[j]),
            decreases s.len() - k,
        {
            proof {
                crate::fenwick::lemma_sum_split(slab_lens(s), 0, k as int, k + 1);
                crate::fenwick::lemma_sum_one(slab_lens(s), k as int);
            }
            if slabs[k].len > usize::MAX - 3 || len > usize::MAX - 3 - slabs[k].len {
                too_many_items();
            }
            len += slabs[k].len;
            ws.push(G::compute::<I>(&slabs[k]));
            k += 1;
        }
        if !G::dom_check(&slabs, len) {
            out_of_domain();
        }
        match Self::from_slabs(slabs, ws, max_segments) {
            Some(c) => c,
            None => {
                too_many_items();
                unreached()
            },
        }
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
            it.items_left == end - start,
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
            old(self).ready(),
            r.slabs@ == old(self).slabs_spec(),
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

    /// Put back parts, as an edit left them, with the edit's segment
    /// budget.
    #[inline(always)]
    pub(crate) fn restore_budget(&mut self, p: Parts<E::Tail, I>, max_segments: usize)
        requires
            parts_ok::<E, W, I, G>(p, max_segments),
            max_segments >= 2,
        ensures
            final(self).ready(),
            final(self)@ == vals_of::<E>(p.slabs@),
            final(self).budget() == max_segments,
            final(self).count() == p.counter,
    {
        *self = ColumnCore { state: Some(p), max_segments, _p: PhantomData };
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
    pub(crate) fn splice<F: Visitor<E>, R: RunFeed<E>>(
        &mut self,
        at: usize,
        del: usize,
        f: &mut F,
        feed: &mut R,
    ) -> (ins: Ghost<Seq<E::V>>)
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
            old(feed).inv(),
        ensures
            final(self)@ == old(self)@.take(at as int) + ins@ + old(self)@.skip(at + del),
            final(feed).given() == old(feed).given() + ins@,
            final(feed).ended(),
            final(feed).inv(),
            final(feed).key() == old(feed).key(),
            final(self).budget() == old(self).budget(),
            final(self).ready(),
    {
        let max_segments = self.max_segments;
        let mut p = self.take_parts();
        proof {
            lemma_vals_total::<E>(p.slabs@);
        }
        let ins = splice_feed::<E, W, I, G, F, R>(
            &mut p.slabs,
            &mut p.index,
            &mut p.total_len,
            max_segments,
            &mut p.counter,
            at,
            del,
            f,
            feed,
        );
        if !G::dom_check(&p.slabs, p.total_len) {
            out_of_domain();
        }
        self.restore(p);
        ins
    }

    /// The column's items, a range at a time, for `copy_ranges`: a reader
    /// moved by the column's own index (`copy::SrcFeed`), at the start.
    #[inline(always)]
    fn src_feed<'a, X: Decoders<'a, Tail = E::Tail>>(&'a self) -> (r: SrcFeed<'a, E, W, I, X>)
        where E: ReadsAgree<'a, X>
        ensures
            r.inv(),
            r.src() == self@,
            r.at() == 0,
    {
        let index = self.index();
        let slabs = self.slabs();
        let core = self.reader::<X>(0, 0);
        self.lemma_column();
        proof {
            use_type_invariant(self);
            let v = crate::iter_core::vals_of::<X>(slabs@);
            lemma_lens_ok::<E, X>(slabs@);
            lemma_vals_own::<E, X>(slabs@);
            assert(v.subrange(0, 0) =~= Seq::<<X::D as Decode>::Item>::empty());
        }
        SrcFeed { slabs, index, core, end: 0, _p: PhantomData }
    }

    /// `copy_ranges`, directly: apply the normalized splices `sps` to this
    /// column, the items of column `src` (`copy::copy_direct`): it gives
    /// what the splices `list` was made from give one by one.
    #[inline(always)]
    pub(crate) fn copy_direct<'a, X: Decoders<'a, Tail = E::Tail>, F: Visitor<E>>(
        &mut self,
        list: &Normalized,
        src: &'a Self,
        f: &mut F,
    )
        where E: ReadsAgree<'a, X>
        ensures
            final(self)@ == applied(old(self)@, src@, list.seen()),
            final(self).budget() == old(self).budget(),
            final(self).ready(),
    {
        if list.src_len() != src.len() || list.dst_len() != self.len() {
            wrong_columns();
        }
        let sps = list.splices();
        proof {
            list.lemma_applied(self@, src@);
        }
        let mut feed = src.src_feed::<X>();
        let max_segments = self.max_segments;
        let mut p = self.take_parts();
        proof {
            lemma_vals_total::<E>(p.slabs@);
        }
        copy_direct::<E, W, I, G, F, SrcFeed<'a, E, W, I, X>>(
            &mut p.slabs,
            &mut p.index,
            &mut p.total_len,
            max_segments,
            &mut p.counter,
            sps,
            f,
            &mut feed,
        );
        if !G::dom_check(&p.slabs, p.total_len) {
            out_of_domain();
        }
        self.restore(p);
    }

    /// `copy_ranges`, inverted: this column holds the source; keep the
    /// normalized splices' ranges of it and put the items of column `dst`
    /// back between them (`copy::copy_invert`): it gives what the splices
    /// `list` was made from give one by one.
    #[inline(always)]
    pub(crate) fn copy_invert<'a, X: Decoders<'a, Tail = E::Tail>, F: Visitor<E>>(
        &mut self,
        list: &Normalized,
        dst: &'a Self,
        f: &mut F,
    )
        where E: ReadsAgree<'a, X>
        ensures
            final(self)@ == applied(dst@, old(self)@, list.seen()),
            final(self).budget() == old(self).budget(),
            final(self).ready(),
    {
        if list.src_len() != self.len() || list.dst_len() != dst.len() {
            wrong_columns();
        }
        let sps = list.splices();
        proof {
            list.lemma_applied(dst@, self@);
        }
        let mut feed = dst.src_feed::<X>();
        let dst_len = dst.len();
        let max_segments = self.max_segments;
        let mut p = self.take_parts();
        proof {
            lemma_vals_total::<E>(p.slabs@);
        }
        copy_invert::<E, W, I, G, F, SrcFeed<'a, E, W, I, X>>(
            &mut p.slabs,
            &mut p.index,
            &mut p.total_len,
            max_segments,
            &mut p.counter,
            sps,
            dst_len,
            f,
            &mut feed,
        );
        if !G::dom_check(&p.slabs, p.total_len) {
            out_of_domain();
        }
        self.restore(p);
    }
}

/// A prefix column's core: its weights hold each slab's item count and
/// sum (`weights::PrefixModel`), so the prefix readers' invariant
/// (`ix_ok`) follows from the type invariant.
impl<E, X, B, P> ColumnCore<E, PrefixSlabWeight<P>, SlabBTree<PrefixSlabWeight<P>>, PrefixModel<X, B>> where
    E: Slabs + for<'a> ReadsAgree<'a, X>,
    X: for<'a> Decoders<'a, Tail = E::Tail>,
    B: for<'a> PrefixOps<<<X as Decoders<'a>>::D as Decode>::Item, P = P>,
    P: Clone + Default + std::fmt::Debug + AddAssign + SubAssign + Add<Output = P> + for<'b> AddAssign<&'b P>,
 {
    /// The prefix readers' view of the column invariant, from the type
    /// invariant: the index is well-formed and each weight holds its
    /// slab's item count and the sum of its values.
    #[inline(always)]
    fn prefix_index(&self) -> (ix: &SlabBTree<PrefixSlabWeight<P>>)
        ensures
            ix.wf(),
            ix_ok::<X, B>(ix@, self.slabs_spec()),
            lens_ok::<X>(self.slabs_spec()),
            total(slab_lens(self.slabs_spec())) <= usize::MAX,
            self.slabs_spec().len() < usize::MAX,
    {
        let ix = self.index();
        if self.slabs().len() == usize::MAX {
            too_many_slabs();
        }
        proof {
            use_type_invariant(self);
            let s = self.slabs_spec();
            lemma_lens_ok::<E, X>(s);
            lemma_btree_index(*ix);
            assert forall|i: int| 0 <= i < s.len() implies {
                &&& #[trigger] ix@[i].spec_len() == s[i].len
                &&& ix@[i].spec_prefix() == vsum::<_, B>(<X as Decoders<'static>>::vals(s[i]))
            } by {
                lemma_prefix_len(ix@[i]);
                lemma_prefix_sum(ix@[i]);
                assert(<PrefixModel<X, B> as Weigh<E, PrefixSlabWeight<P>>>::wrel(ix.ws()[i], s[i]));
            }
        }
        ix
    }

    /// The sum of the values before item `index` (all of them, past the
    /// end): `p_prefix`, from the type invariant.
    #[inline(always)]
    pub(crate) fn prefix_before(&self, index: usize) -> (r: P)
        ensures
            r == vsum::<_, B>(before::<X>(self.slabs_spec(), index as int)),
    {
        let ix = self.prefix_index();
        p_prefix::<X, B>(self.slabs(), ix, index)
    }

    /// The sum of the values in `start..end` (`start < end`): what, added
    /// to the sum before `start`, gives the sum before `end`. `p_sum_range`
    /// over an iterator on the whole column, from the type invariant.
    #[inline(always)]
    pub(crate) fn sum_range(&self, start: usize, end: usize) -> (r: P)
        requires
            start < end,
        ensures
            prefix_add(vsum::<_, B>(before::<X>(self.slabs_spec(), start as int)), r)
                == vsum::<_, B>(before::<X>(self.slabs_spec(), end as int)),
    {
        let ix = self.prefix_index();
        let len = self.len();
        let mut core = self.reader::<X>(0, len);
        let mut total = <B as PrefixOps<<<X as Decoders<'static>>::D as Decode>::Item>>::zero();
        let mut base = <B as PrefixOps<<<X as Decoders<'static>>::D as Decode>::Item>>::zero();
        self.lemma_column();
        proof {
            core.lemma_here::<X>(self.slabs_spec());
            let v = crate::iter_core::vals_of::<X>(self.slabs_spec());
            assert(v.take(0) =~= Seq::<<X::D as Decode>::Item>::empty());
            axiom_prefix_add(prefix_zero::<P>(), prefix_zero::<P>(), prefix_zero::<P>());
        }
        p_sum_range::<X, B>(&mut core, self.slabs(), Some(ix), start, end, &mut total, &mut base);
        total
    }

    /// The first position whose sum before it reaches `target`:
    /// `p_index_for_prefix`, from the type invariant.
    #[inline(always)]
    pub(crate) fn index_for_prefix(&self, target: P) -> (r: usize)
        where B: for<'a> PrefixOrd<<<X as Decoders<'a>>::D as Decode>::Item>, P: Ord
        ensures
            found_at::<_, B>(crate::iter_core::vals_of::<X>(self.slabs_spec()), target, r as int),
    {
        let ix = self.prefix_index();
        p_index_for_prefix::<X, B>(self.slabs(), ix, target)
    }
}

/// A delta column's core: its weights hold each slab's deltas' aggregate
/// wherever their steps fit (`weights::DeltaModel`), and its domain, in
/// the type invariant, is that every step fits, so the delta readers'
/// invariant (`dix_ok`) follows from the type invariant; and its splice
/// keeps that domain.
impl<E, X, B> ColumnCore<E, SlabAgg, SlabBTree<SlabAgg>, DeltaModel<X, B>> where
    E: Slabs + for<'a> ReadsAgree<'a, X>,
    X: for<'a> Decoders<'a, Tail = E::Tail>,
    B: for<'a> DeltaWrite<'a, E, X>,
 {
    /// The column's slabs and index as the delta readers take them, with
    /// their invariant (`dindex_ok`), from the type invariant.
    #[inline(always)]
    pub(crate) fn delta_parts(&self) -> (r: (&[Slab<E::Tail>], &SlabBTree<SlabAgg>))
        ensures
            r.0@ == self.slabs_spec(),
            dindex_ok::<X, B>(Some(r.1), r.0@),
            lens_ok::<X>(r.0@),
            total(slab_lens(r.0@)) == self@.len(),
            self@.len() + 3 <= usize::MAX,
            crate::iter_core::vals_of::<X>(r.0@).len() == self@.len(),
    {
        let ix = self.index();
        let slabs = self.slabs();
        self.lemma_column();
        proof {
            use_type_invariant(self);
            let s = self.slabs_spec();
            lemma_lens_ok::<E, X>(s);
            lemma_vals_own::<E, X>(s);
            lemma_ix_exact::<E, X, B>(*ix, s);
        }
        (slabs, ix)
    }

    /// The column's values, a range at a time, for a delta column's
    /// `copy_ranges` (`dcopy::DSrc`): a reader at the start, with its
    /// running sum, moved by the column's own index.
    #[inline(always)]
    pub(crate) fn dsrc<'a>(&'a self) -> (r: DSrc<'a, X, B>)
        ensures
            r.inv(),
            r.src() == rvals::<E, X, B>(self@),
            r.at() == 0,
    {
        let (slabs, index) = self.delta_parts();
        let core = self.reader::<X>(0, 0);
        self.lemma_column();
        proof {
            let s = self.slabs_spec();
            let v = crate::iter_core::vals_of::<X>(s);
            assert(v.subrange(0, 0) =~= Seq::<<X::D as Decode>::Item>::empty());
            assert(crate::prefix_core::before::<X>(s, 0) =~= Seq::<<X::D as Decode>::Item>::empty());
            assert(deltas_of::<_, B>(Seq::<<X::D as Decode>::Item>::empty()) =~= Seq::<Option<i64>>::empty());
            lemma_crop(0);
            lemma_dslabs_own::<E, X, B>(s);
        }
        DSrc { slabs, index, core, running: 0, end: 0, _p: PhantomData }
    }

    /// The ledger of a delta edit cursor opening at item `at`: the
    /// realized value before it (an index jump), the bound on the
    /// column's values (from the index's root), and the column's deltas,
    /// those before `at` read. The ledger widens the bound by each value
    /// the edit writes: the root aggregate after an edit is no bound, as
    /// the index merges with wrapping arithmetic.
    pub(crate) fn delta_ledger(&self, at: usize) -> (l: SpanLedger)
        requires
            at <= self@.len(),
        ensures
            l.inv(),
            l.orig@ == dslabs::<X, B>(self.slabs_spec()),
            l.l.read@ == l.orig@.take(at as int),
            l.l.written@ == l.orig@.take(at as int),
            l.tracking@,
            l.l.adjust == 0,
            !l.l.closed@,
    {
        let len = self.len();
        let ghost s0 = self.slabs_spec();
        let ghost orig = dslabs::<X, B>(s0);
        let ghost v = crate::iter_core::vals_of::<X>(s0);
        let (slabs, ix) = self.delta_parts();
        let (lo, hi) = root_bound::<X, B>(slabs, ix);
        let mut running: i64 = 0;
        proof {
            assert(before::<X>(s0, 0) =~= Seq::<<X::D as Decode>::Item>::empty());
            assert(deltas_of::<_, B>(Seq::<<X::D as Decode>::Item>::empty()) =~= Seq::<Option<i64>>::empty());
            lemma_crop(0);
        }
        if at > 0 {
            let mut core = self.reader::<X>(0, len);
            proof {
                assert(v.subrange(0, len as int) =~= v);
            }
            d_nth::<X, B>(&mut core, slabs, Some(ix), at - 1, &mut running);
        }
        proof {
            assert(deltas_of::<_, B>(v.take(at as int)) =~= orig.take(at as int));
            if at == 0 {
                assert(orig.take(0) =~= Seq::<Option<i64>>::empty());
            }
        }
        SpanLedger::new(running, lo, hi, true, Ghost(orig.take(at as int)), Ghost(orig))
    }

    /// Delete `del` items at `at` and insert the values of `items` (if any) there,
    /// the values after them kept (the delta after the deleted items, and
    /// any nulls before it, re-written to reach the same value): panics
    /// if the values would leave `min..=max` or span 2^63 or more, or the
    /// column hold more than `i64::MAX` items. The splice's plan keeps a
    /// bound on the values (`SpanPlan`), so the column is read again only
    /// where that bound is too wide.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn splice_delta<F: Visitor<E>, S: SpliceItems>(
        &mut self,
        at: usize,
        del: usize,
        items: Option<S>,
        min: i64,
        max: i64,
        f: &mut F,
    ) -> (taken: Ghost<Seq<SpliceItem>>)
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
        ensures
            final(self).budget() == old(self).budget(),
            final(self).ready(),
            rvals::<E, X, B>(final(self)@) == rvals::<E, X, B>(old(self)@).take(at as int) + items_vals(taken@)
                + rvals::<E, X, B>(old(self)@).skip(at + del),
            items is None ==> taken@.len() == 0,
    {
        // the bound on the column's values, and the running sum, the nulls
        // and the value around the splice
        let len = self.len();
        let ghost s0 = self.slabs_spec();
        let ghost orig = dslabs::<X, B>(s0);
        let (lo, hi, prev, skip, next_val) = {
            let (slabs, ix) = self.delta_parts();
            let (lo, hi) = root_bound::<X, B>(slabs, ix);
            let mut core = self.reader::<X>(0, len);
            let mut running: i64 = 0;
            proof {
                let v = crate::iter_core::vals_of::<X>(s0);
                assert(v.subrange(0, len as int) =~= v);
                assert(crate::prefix_core::before::<X>(s0, 0) =~= Seq::<<X::D as Decode>::Item>::empty());
                assert(deltas_of::<_, B>(Seq::<<X::D as Decode>::Item>::empty()) =~= Seq::<Option<i64>>::empty());
                lemma_crop(0);
            }
            let (prev, skip, next_val) = d_boundaries::<X, B>(&mut core, slabs, Some(ix), &mut running, at, del);
            proof {
                // what it found around the splice, of the column's deltas
                let a = orig.take(at as int);
                let x = orig.subrange(at as int, at + del);
                let tail = orig.skip(at + del);
                let e = at + del + skip;
                assert(tail.take(skip as int) =~= reps(skip as nat, None)) by {
                    assert forall|i: int| 0 <= i < skip implies tail.take(skip as int)[i] == reps(skip as nat, None)[i] by {
                        assert(orig[at + del + i] is None);
                    }
                }
                if next_val is Some {
                    assert(a + x + tail.take(skip + 1int) =~= orig.take(e + 1));
                    assert(tail[skip as int] == orig[e]);
                } else {
                    assert forall|i: int| 0 <= i < tail.len() implies (#[trigger] tail[i]) is None by {
                        assert(orig[at + del + i] is None);
                    }
                }
                assert(bounds_of(a, x, tail, prev as int, skip as nat, next_val));
            }
            (lo, hi, prev, skip, next_val)
        };
        let plan = SpanPlan::new(prev, skip, next_val, lo, hi, true, Ghost(orig));
        let mut feed = DeltaFeed::<S, E, X, B> {
            items,
            plan,
            track: true,
            given: Ghost(Seq::empty()),
            taken: Ghost(Seq::empty()),
            _p: PhantomData,
        };
        proof {
            assert(Seq::<E::V>::empty().map_values(|o: E::V| <B as DeltaWrite<'static, E, X>>::dvo(o)) =~= feed.plan.p.emitted@);
            assert(feed.plan.p.wanted@ =~= items_vals(feed.taken@));
        }
        // with a value after the deleted items and their nulls, those are
        // written again
        let total_del = if next_val.is_some() { del + skip + 1 } else { del };
        let max_segments = self.max_segments;
        let mut p = self.take_parts();
        proof {
            lemma_vals_total::<E>(p.slabs@);
        }
        let _ins = splice_feed::<E, SlabAgg, SlabBTree<SlabAgg>, DeltaModel<X, B>, F, DeltaFeed<S, E, X, B>>(
            &mut p.slabs,
            &mut p.index,
            &mut p.total_len,
            max_segments,
            &mut p.counter,
            at,
            total_del,
            f,
            &mut feed,
        );
        let fits = p.total_len as u64 <= i64::MAX as u64 && feed.plan.fits(min, max);
        proof {
            // the deltas the splice leaves, and the values they realize:
            // the items' before the original ones after the deleted items
            let a = orig.take(at as int);
            let x = orig.subrange(at as int, at + del);
            let tail = orig.skip(at + del);
            assert(orig =~= a + x + tail);
            assert(bounds_of(a, x, tail, feed.plan.p.prev@, feed.plan.p.skip as nat, feed.plan.p.next_val));
            assert(_ins@ =~= feed.given@);
            lemma_vals_total::<E>(p.slabs@);
            lemma_splice_deltas::<E, X, B>(
                s0,
                p.slabs@,
                at as int,
                del as int,
                total_del as int,
                _ins@,
                feed.plan.p.emitted@,
                skip as nat,
                next_val,
            );
            lemma_spliced_vals(orig, a, x, tail, feed.plan.p.emitted@, feed.plan.p.wanted@, prev, skip as nat, next_val);
            lemma_dslabs_own::<E, X, B>(s0);
            lemma_dslabs_own::<E, X, B>(p.slabs@);
            if fits {
                // the plan's bound holds the values of the column it leaves
                lemma_span_steps(dslabs::<X, B>(p.slabs@), min as int, max as int);
            }
        }
        if !fits {
            proof {
                lemma_lens_ok::<E, X>(p.slabs@);
            }
            if p.total_len as u64 > i64::MAX as u64 {
                too_many_deltas();
            }
            if !slabs_fit::<X, B>(p.slabs.as_slice(), p.total_len, min, max) {
                out_of_domain();
            }
        }
        self.restore(p);
        Ghost(feed.taken@)
    }

    /// Fill this empty column with the values `items` gives, each written
    /// as the delta that realizes it after those before (`from_values`):
    /// panics if the values would leave `min..=max` or span 2^63 or more,
    /// or number more than `i64::MAX`. The deltas keep a bound on the
    /// values (`ToDeltas`), so the column is read again only where that
    /// bound is too wide.
    pub(crate) fn fill_delta<F: Visitor<E>, S: ValueItems>(
        &mut self,
        items: S,
        min: i64,
        max: i64,
        f: &mut F,
    ) -> (taken: Ghost<Seq<Option<i64>>>)
        requires
            old(self)@.len() == 0,
        ensures
            final(self).budget() == old(self).budget(),
            final(self).ready(),
            rvals::<E, X, B>(final(self)@) == taken@,
    {
        let mut feed = ValuesFeed::<S, E, X, B> {
            items,
            deltas: ToDeltas::new(true),
            given: Ghost(Seq::empty()),
            ended: Ghost(false),
            taken: Ghost(Seq::empty()),
            _p: PhantomData,
        };
        proof {
            assert(Seq::<E::V>::empty().map_values(|o: E::V| <B as DeltaWrite<'static, E, X>>::dvo(o))
                =~= feed.deltas.ds@);
            assert(feed.deltas.vals@ =~= feed.taken@);
        }
        let max_segments = self.max_segments;
        let mut p = self.take_parts();
        proof {
            lemma_vals_total::<E>(p.slabs@);
        }
        let _ins = splice_feed::<E, SlabAgg, SlabBTree<SlabAgg>, DeltaModel<X, B>, F, ValuesFeed<S, E, X, B>>(
            &mut p.slabs,
            &mut p.index,
            &mut p.total_len,
            max_segments,
            &mut p.counter,
            0,
            0,
            f,
            &mut feed,
        );
        let fits = p.total_len as u64 <= i64::MAX as u64 && feed.deltas.fits(min, max);
        proof {
            // the column holds just the values written, whose deltas
            // realize the values taken
            let v = vals_of::<E>(p.slabs@);
            assert(v =~= feed.given@);
            lemma_dslabs_own::<E, X, B>(p.slabs@);
            lemma_vals_total::<E>(p.slabs@);
            if fits {
                // the bound holds them
                lemma_span_steps(dslabs::<X, B>(p.slabs@), min as int, max as int);
            }
        }
        if !fits {
            proof {
                lemma_lens_ok::<E, X>(p.slabs@);
            }
            if p.total_len as u64 > i64::MAX as u64 {
                too_many_deltas();
            }
            if !slabs_fit::<X, B>(p.slabs.as_slice(), p.total_len, min, max) {
                out_of_domain();
            }
        }
        self.restore(p);
        Ghost(feed.taken@)
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

/// The delta readers' invariant for slabs `s` indexed by `ix`, whose
/// weights are the slabs' (as a column's are) and whose running sums'
/// steps fit `i64`.
pub(crate) proof fn lemma_ix_exact<E, X, B>(ix: SlabBTree<SlabAgg>, s: Seq<Slab<E::Tail>>)
    where
        E: Slabs + for<'b> ReadsAgree<'b, X>,
        X: for<'b> Decoders<'b, Tail = E::Tail>,
        B: for<'b> DeltaWrite<'b, E, X>,
    requires
        all_ok::<E>(s),
        ix.ok(),
        ix.ws().len() == s.len(),
        ws_ok::<E, SlabAgg, DeltaModel<X, B>>(ix.ws(), s),
        steps_fit(dslabs::<X, B>(s)),
        total(slab_lens(s)) <= usize::MAX,
    ensures
        dindex_ok::<X, B>(Some(&ix), s),
{
    crate::delta::ddomain::lemma_dslabs_len::<E, X, B>(s);
    lemma_btree_index(ix);
    assert forall|k: int| 0 <= k < s.len() implies (steps_fit(deltas_of::<_, B>(X::vals(s[k]))) ==> agg_of(
        #[trigger] ix@[k],
        deltas_of::<_, B>(X::vals(s[k])),
    )) by {
        assert(<DeltaModel<X, B> as Weigh<E, SlabAgg>>::wrel(ix.ws()[k], s[k]));
    }
    lemma_dix_ok::<X, B>(ix@, s);
}

/// The running value before item `pos` of the delta column of `slabs`
/// indexed by `ix`, whose weights are its slabs' (as a column's are): an
/// index jump where its running sums' steps fit `i64` (`exact`, so the
/// index is), else a decode from the start. For an edit, which holds a
/// column's parts.
pub(crate) fn delta_running_at<'a, E, X, B>(
    slabs: &'a [Slab<E::Tail>],
    ix: &SlabBTree<SlabAgg>,
    len: usize,
    pos: usize,
    exact: bool,
) -> (r: i64)
    where
        E: Slabs + for<'b> ReadsAgree<'b, X>,
        X: for<'b> Decoders<'b, Tail = E::Tail>,
        B: for<'b> DeltaWrite<'b, E, X>,
    requires
        all_ok::<E>(slabs@),
        ix.ok(),
        ix.ws().len() == slabs@.len(),
        ws_ok::<E, SlabAgg, DeltaModel<X, B>>(ix.ws(), slabs@),
        exact ==> steps_fit(dslabs::<X, B>(slabs@)),
        len == total(slab_lens(slabs@)),
        0 < pos <= len,
        slabs@.len() < usize::MAX,
    ensures
        r == crop(crate::delta::agg::psum(deltas_of::<_, B>(crate::iter_core::vals_of::<X>(slabs@).take(pos as int)))),
{
    let ghost s = slabs@;
    proof {
        lemma_lens_ok::<E, X>(s);
        crate::delta::ddomain::lemma_dslabs_len::<E, X, B>(s);
    }
    if exact {
        delta_running_exact::<E, X, B>(slabs, ix, len, pos)
    } else {
        delta_running_decode::<E, X, B>(slabs, len, pos)
    }
}

/// [`delta_running_at`] through the index.
fn delta_running_exact<'a, E, X, B>(slabs: &'a [Slab<E::Tail>], ix: &SlabBTree<SlabAgg>, len: usize, pos: usize) -> (r: i64)
    where
        E: Slabs + for<'b> ReadsAgree<'b, X>,
        X: for<'b> Decoders<'b, Tail = E::Tail>,
        B: for<'b> DeltaWrite<'b, E, X>,
    requires
        all_ok::<E>(slabs@),
        ix.ok(),
        ix.ws().len() == slabs@.len(),
        ws_ok::<E, SlabAgg, DeltaModel<X, B>>(ix.ws(), slabs@),
        steps_fit(dslabs::<X, B>(slabs@)),
        len == total(slab_lens(slabs@)),
        0 < pos <= len,
        slabs@.len() < usize::MAX,
    ensures
        r == crop(crate::delta::agg::psum(deltas_of::<_, B>(crate::iter_core::vals_of::<X>(slabs@).take(pos as int)))),
{
    let ghost s = slabs@;
    proof {
        lemma_lens_ok::<E, X>(s);
        crate::delta::ddomain::lemma_dslabs_len::<E, X, B>(s);
        lemma_ix_exact::<E, X, B>(*ix, s);
    }
    let mut core = ICore::at::<X>(slabs, 0, 0, 0, len);
    let mut running: i64 = 0;
    proof {
        core.lemma_here::<X>(s);
        assert(crate::prefix_core::before::<X>(s, 0) =~= Seq::<<X::D as Decode>::Item>::empty());
        assert(deltas_of::<_, B>(crate::prefix_core::before::<X>(s, 0)) =~= Seq::<Option<i64>>::empty());
        lemma_crop(0);
    }
    d_nth::<X, B>(&mut core, slabs, Some(ix), pos - 1, &mut running);
    proof {
        assert(crate::prefix_core::before::<X>(s, pos as int) == crate::iter_core::vals_of::<X>(s).take(pos as int));
    }
    running
}

/// [`delta_running_at`] by decoding from the start.
#[allow(unused_variables)] // `len` is for the spec
fn delta_running_decode<'a, E, X, B>(slabs: &'a [Slab<E::Tail>], len: usize, pos: usize) -> (r: i64)
    where
        E: Slabs + for<'b> ReadsAgree<'b, X>,
        X: for<'b> Decoders<'b, Tail = E::Tail>,
        B: for<'b> DeltaWrite<'b, E, X>,
    requires
        all_ok::<E>(slabs@),
        len == total(slab_lens(slabs@)),
        0 < pos <= len,
        slabs@.len() < usize::MAX,
    ensures
        r == crop(crate::delta::agg::psum(deltas_of::<_, B>(crate::iter_core::vals_of::<X>(slabs@).take(pos as int)))),
{
    let ghost s = slabs@;
    proof {
        lemma_lens_ok::<E, X>(s);
    }
    let mut core = ICore::at::<X>(slabs, 0, 0, 0, pos);
    let mut running: i64 = 0;
    proof {
        core.lemma_here::<X>(s);
        assert(crate::prefix_core::before::<X>(s, 0) =~= Seq::<<X::D as Decode>::Item>::empty());
        assert(deltas_of::<_, B>(crate::prefix_core::before::<X>(s, 0)) =~= Seq::<Option<i64>>::empty());
        lemma_crop(0);
    }
    while core.pos < pos
        invariant
            s == slabs@,
            crate::delta::diter::at_run::<X, B>(core, running, s),
            core.pos <= pos,
            core.out::<X>(s).len() == pos - core.pos,
        decreases pos - core.pos,
    {
        // the window is not empty, so a run comes
        let _ = crate::delta::diter::d_next_run::<X, B>(&mut core, slabs, usize::MAX, &mut running);
    }
    proof {
        assert(crate::prefix_core::before::<X>(s, pos as int) == crate::iter_core::vals_of::<X>(s).take(pos as int));
    }
    running
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

impl<E: Slabs, W, I: IndexOps<W> + Clone, G: Weigh<E, W>> Clone for ColumnCore<E, W, I, G> {
    fn clone(&self) -> Self {
        ColumnCore {
            state: self.state.clone(),
            max_segments: self.max_segments,
            _p: PhantomData,
        }
    }
}
