//! A delta column's edit cursor ([`DeltaEdit`]), verified with Verus:
//! the column cursor over its storage ([`EditCore`]) and the ledger of
//! the values it writes (`dbound::SpanLedger`).
//!
//! The column cursor shows its visitor every item it carries or deletes
//! (`cursor::Visitor::seen`). [`DSumVisit`] is that visitor for a delta
//! column's RLE storage: it runs the rebuild's verified walk with
//! [`DSum`], which adds up the deltas of the runs the walk shows it, and
//! weighs a slab dropped whole (`slab_agg`, whose total is the slab's
//! deltas' sum, wrapping). Its type invariant: the sum is that of the
//! deltas of all it has seen (`dvos`), wrapping. So the ledger hears
//! exactly what the cursor carried, deleted or jumped over.
//!
//! Proved, of [`DeltaEditCore`]: its invariant (the ledger's original
//! deltas are the column's, those it has passed are the cursor's, and
//! those it has written are the edit's) holds from `begin` through every
//! step; each step is stated by the column's values as its iterators read
//! them (`ddomain::rvals`): the original values (`rorig`) and those
//! written before the cursor (`rwritten`). An insert writes the values
//! given, a carry the original values passed, a delete nothing, and a
//! peek reads the original value at the cursor; `end` pays what is owed,
//! checks the domain and hands the column back holding the values
//! written, then the original values after the cursor.
//!
//! Trusted: what the delta bridge `DeltaWrite` says of an owned value's
//! delta (`dvo`, and that a value read holds its owned form's delta); and
//! that the glue (`DeltaEdit`) chains the steps, using none after `end`
//! or after one that panicked.
//!
//! [`DeltaEdit`]: crate::delta::DeltaEdit

use crate::btree::{SlabAgg, SlabBTree};
use crate::column::Slab;
use crate::column_core::ColumnCore;
use crate::cursor::Visitor;
use crate::delta::dbound::SpanLedger;
use crate::delta::ddomain::slabs_fit;

use crate::edit_core::EditCore;

#[cfg(verus_keep_ghost)]
use crate::cursor::{lemma_lens_ok, vals_of, Insertable, ReadsAgree, Slabs, Weigh};
#[cfg(verus_keep_ghost)]
use crate::delta::agg::{agg_of, steps_fit};
#[cfg(verus_keep_ghost)]
use crate::delta::agg::{deltas_of, has_null, psum, reps};
use crate::delta::agg::{slab_dsum, DeltaOps};
#[cfg(verus_keep_ghost)]
use crate::delta::dbound::lemma_rz_sub;
use crate::delta::ddomain::DeltaWrite;
#[cfg(verus_keep_ghost)]
use crate::delta::ddomain::{dslabs, lemma_dix_ok, lemma_dslabs_own, lemma_span_steps, rvals};
use crate::delta::dedit::run_sum;
#[cfg(verus_keep_ghost)]
use crate::delta::dedit::{lemma_rest_unchanged, lemma_rz_nulls, run_vals, rz};
#[cfg(verus_keep_ghost)]
use crate::delta::diter::{crop, lemma_crop_add, lemma_psum_cat, lemma_wrapping_add};
#[cfg(verus_keep_ghost)]
use crate::index::{lemma_btree_index, IndexOps};
#[cfg(verus_keep_ghost)]
use crate::prefix_core::before;
use crate::rle::dec::RleReads;
use crate::rle::edit::{discard, pass, RleModel, RleRebuild, Visit};
#[cfg(verus_keep_ghost)]
use crate::rle::lift::lift_seq;
use crate::rle::lift::Lift;
use crate::rle::model::{RleOps, WriteOps};
#[cfg(verus_keep_ghost)]
use crate::rle::state::copies;
use crate::rle::state::VOf;
use crate::rle::RleTail;
use crate::weights::DeltaModel;
use std::marker::PhantomData;
use vstd::prelude::*;

verus! {

/// The deltas of the runs a walk shows it, summed, wrapping.
pub(crate) struct DSum<V: Copy, B: DeltaOps<V>> {
    sum: i64,
    /// whether one of them is null
    null: bool,
    #[allow(dead_code)]
    seen: Ghost<Seq<V>>,
    _p: PhantomData<B>,
}

impl<V: Copy, B: DeltaOps<V>> DSum<V, B> {
    #[verifier::type_invariant]
    spec fn inv(&self) -> bool {
        &&& self.sum == crop(psum(deltas_of::<V, B>(self.seen@)))
        &&& self.null == has_null(deltas_of::<V, B>(self.seen@))
    }

    #[inline(always)]
    pub(crate) fn new() -> (d: Self)
        ensures
            d.seen() == Seq::<V>::empty(),
    {
        proof {
            assert(deltas_of::<V, B>(Seq::<V>::empty()) =~= Seq::<Option<i64>>::empty());
            crate::delta::diter::lemma_crop(0);
        }
        DSum { sum: 0, null: false, seen: Ghost(Seq::empty()), _p: PhantomData }
    }

    /// The values it has been shown.
    pub(crate) closed spec fn seen(&self) -> Seq<V> {
        self.seen@
    }

    /// Their deltas' sum, wrapping.
    #[inline(always)]
    pub(crate) fn total(&self) -> (s: i64)
        ensures
            s == crop(psum(deltas_of::<V, B>(self.seen()))),
    {
        proof {
            use_type_invariant(self);
        }
        self.sum
    }

    /// Whether one of their deltas is null.
    #[inline(always)]
    pub(crate) fn saw_null(&self) -> (b: bool)
        ensures
            b == has_null(deltas_of::<V, B>(self.seen())),
    {
        proof {
            use_type_invariant(self);
        }
        self.null
    }
}

/// A null among `a` then `b` is one among either.
proof fn lemma_null_cat(a: Seq<Option<i64>>, b: Seq<Option<i64>>)
    ensures
        has_null(a + b) == (has_null(a) || has_null(b)),
{
    let c = a + b;
    if has_null(a) {
        let i = choose|i: int| 0 <= i < a.len() && #[trigger] a[i] is None;
        assert(c[i] is None);
    }
    if has_null(b) {
        let i = choose|i: int| 0 <= i < b.len() && #[trigger] b[i] is None;
        assert(c[a.len() + i] is None);
    }
    if has_null(c) {
        let i = choose|i: int| 0 <= i < c.len() && #[trigger] c[i] is None;
        if i < a.len() {
            assert(a[i] is None);
        } else {
            assert(b[i - a.len()] is None);
        }
    }
}

/// A run holds a null when it is of nulls and not empty.
proof fn lemma_null_reps(n: nat, d: Option<i64>)
    ensures
        has_null(reps(n, d)) == (n > 0 && d is None),
{
    if n > 0 && d is None {
        assert(reps(n, d)[0] is None);
    }
}

/// The deltas of `vs` and then a run of `n` copies of `v`.
proof fn lemma_deltas_run<V: Copy, B: DeltaOps<V>>(vs: Seq<V>, n: nat, v: V)
    ensures
        deltas_of::<V, B>(vs + copies(n, v)) == deltas_of::<V, B>(vs) + reps(n, B::dv(v)),
{
    assert(deltas_of::<V, B>(vs + copies(n, v)) =~= deltas_of::<V, B>(vs) + reps(n, B::dv(v)));
}

/// The sum of `ds` then `x`, wrapping, from that of `ds`.
proof fn lemma_crop_cat(ds: Seq<Option<i64>>, x: Seq<Option<i64>>, s: i64, t: i64)
    requires
        s == crop(psum(ds)),
        t == crop(psum(x)),
    ensures
        crop(s + t) == crop(psum(ds + x)),
{
    lemma_psum_cat(ds, x);
    lemma_crop_add(psum(ds), t as int);
    lemma_crop_add(psum(x), psum(ds));
}

impl<V: Copy, B: DeltaOps<V>> Visit<V> for DSum<V, B> {
    closed spec fn seen(&self) -> Seq<V> {
        self.seen@
    }

    #[inline(always)]
    fn visit(&mut self, v: V, n: usize) {
        proof {
            use_type_invariant(&*self);
        }
        let ghost s0 = self.seen@;
        let dv = B::to_opt(v);
        let r = run_sum(dv, n);
        proof {
            lemma_deltas_run::<V, B>(s0, n as nat, v);
            lemma_crop_cat(deltas_of::<V, B>(s0), reps(n as nat, B::dv(v)), self.sum, r);
            lemma_wrapping_add(self.sum, r);
            lemma_null_cat(deltas_of::<V, B>(s0), reps(n as nat, dv));
            lemma_null_reps(n as nat, dv);
        }
        let null = self.null || (n > 0 && dv.is_none());
        *self = DSum { sum: self.sum.wrapping_add(r), null, seen: Ghost(s0 + copies(n as nat, v)), _p: PhantomData };
    }
}

/// The deltas of owned values `vs`, as `B` reads them.
pub(crate) open spec fn dvos<'a, L, O, B>(vs: Seq<VOf<'static, L>>) -> Seq<Option<i64>> where
    L: for<'t> Lift<'t, W = L> + for<'t> WriteOps<'t, R = O>,
    O: for<'b> RleOps<'b>,
    B: DeltaWrite<'a, RleModel<L>, RleReads<O>>,
 {
    vs.map_values(|o: VOf<'static, L>| B::dvo(o))
}

/// A delta column's cursor visitor: the sum of the deltas of everything
/// the cursor has shown it, wrapping.
#[verifier::reject_recursive_types(L)]
#[verifier::reject_recursive_types(O)]
#[verifier::reject_recursive_types(B)]
pub(crate) struct DSumVisit<L, O, B> where
    L: for<'t> Lift<'t, W = L> + for<'t> WriteOps<'t, R = O>,
    O: for<'b> RleOps<'b>,
    B: for<'a> DeltaWrite<'a, RleModel<L>, RleReads<O>>,
 {
    sum: i64,
    /// whether one of them is null
    null: bool,
    #[allow(dead_code)]
    seen: Ghost<Seq<VOf<'static, L>>>,
    _p: PhantomData<(O, B)>,
}

impl<L, O, B> DSumVisit<L, O, B> where
    L: for<'t> Lift<'t, W = L> + for<'t> WriteOps<'t, R = O>,
    O: for<'b> RleOps<'b>,
    B: for<'a> DeltaWrite<'a, RleModel<L>, RleReads<O>>,
 {
    #[verifier::type_invariant]
    spec fn inv(&self) -> bool {
        &&& self.sum == crop(psum(dvos::<'static, L, O, B>(self.seen@)))
        &&& self.null == has_null(dvos::<'static, L, O, B>(self.seen@))
    }

    #[inline(always)]
    pub(crate) fn new() -> (d: Self)
        ensures
            d.seen_vals() == Seq::<VOf<'static, L>>::empty(),
    {
        proof {
            assert(dvos::<'static, L, O, B>(Seq::empty()) =~= Seq::<Option<i64>>::empty());
            crate::delta::diter::lemma_crop(0);
        }
        DSumVisit { sum: 0, null: false, seen: Ghost(Seq::empty()), _p: PhantomData }
    }

    pub(crate) closed spec fn seen_vals(&self) -> Seq<VOf<'static, L>> {
        self.seen@
    }

    /// The deltas' sum of all it has seen, wrapping.
    #[inline(always)]
    pub(crate) fn total(&self) -> (s: i64)
        ensures
            s == crop(psum(dvos::<'static, L, O, B>(self.seen_vals()))),
    {
        proof {
            use_type_invariant(self);
        }
        self.sum
    }

    /// Whether one of the deltas it has seen is null.
    #[inline(always)]
    pub(crate) fn saw_null(&self) -> (b: bool)
        ensures
            b == has_null(dvos::<'static, L, O, B>(self.seen_vals())),
    {
        proof {
            use_type_invariant(self);
        }
        self.null
    }

    /// Add what a walk over `s` showed `d`: the slab's values `a..b`.
    #[inline(always)]
    #[allow(unused_variables)] // `s`, `a` and `b` are for the spec
    fn absorb<'s>(&mut self, s: &'s Slab<RleTail>, a: Ghost<int>, b: Ghost<int>, d: &DSum<VOf<'s, L>, B>)
        requires
            0 <= a@ <= b@ <= RleModel::<L>::vals(*s).len(),
            d.seen() == crate::rle::dec::slab_values::<<L as WriteOps<'s>>::R>(s.data@).subrange(a@, b@),
            lift_seq::<L>(crate::rle::dec::slab_values::<<L as WriteOps<'s>>::R>(s.data@).subrange(a@, b@))
                == RleModel::<L>::vals(*s).subrange(a@, b@),
        ensures
            final(self).seen_vals() == old(self).seen_vals() + RleModel::<L>::vals(*s).subrange(a@, b@),
    {
        proof {
            use_type_invariant(&*self);
        }
        let t = d.total();
        let dn = d.saw_null();
        let ghost bv = d.seen();
        let ghost ov = RleModel::<L>::vals(*s).subrange(a@, b@);
        proof {
            // an owned value holds its read form's delta
            assert(dvos::<'static, L, O, B>(ov) =~= deltas_of::<VOf<'s, L>, B>(bv)) by {
                assert forall|i: int| 0 <= i < bv.len() implies dvos::<'static, L, O, B>(ov)[i]
                    == deltas_of::<VOf<'s, L>, B>(bv)[i] by {
                    <B as DeltaWrite<'s, RleModel<L>, RleReads<O>>>::axiom_own(bv[i]);
                    assert(ov[i] == lift_seq::<L>(bv)[i]);
                }
            }
            assert(dvos::<'static, L, O, B>(self.seen@ + ov) =~= dvos::<'static, L, O, B>(self.seen@) + dvos::<'static, L, O, B>(ov));
            lemma_crop_cat(dvos::<'static, L, O, B>(self.seen@), dvos::<'static, L, O, B>(ov), self.sum, t);
            lemma_wrapping_add(self.sum, t);
            lemma_null_cat(dvos::<'static, L, O, B>(self.seen@), dvos::<'static, L, O, B>(ov));
        }
        *self = DSumVisit { sum: self.sum.wrapping_add(t), null: self.null || dn, seen: Ghost(self.seen@ + ov), _p: PhantomData };
    }
}

impl<L, O, B> Visitor<RleModel<L>> for DSumVisit<L, O, B> where
    L: for<'t> Lift<'t, W = L> + for<'t> WriteOps<'t, R = O>,
    O: for<'b> RleOps<'b>,
    B: for<'a> DeltaWrite<'a, RleModel<L>, RleReads<O>>,
 {
    closed spec fn seen(&self) -> Seq<VOf<'static, L>> {
        self.seen@
    }

    #[inline(always)]
    fn pass<'s>(&mut self, e: &mut RleRebuild<L>, s: &'s Slab<RleTail>, n: usize, runs: usize) -> (left: usize) {
        let ghost a = RleModel::<L>::rb_read(*e) as int;
        let mut d = DSum::<VOf<'s, L>, B>::new();
        let left = pass::<L, DSum<VOf<'s, L>, B>>(e, s, n, runs, &mut d);
        proof {
            <RleModel<L> as Slabs>::lemma_len(*s);
            assert(d.seen() =~= Seq::<VOf<'s, L>>::empty() + crate::rle::dec::slab_values::<<L as WriteOps<'s>>::R>(
                s.data@,
            ).subrange(a, a + (n - left)));
            crate::rle::edit::lemma_vals_read::<L>(*s, a, a + (n - left));
        }
        self.absorb(s, Ghost(a), Ghost(a + (n - left)), &d);
        left
    }

    #[inline(always)]
    fn discard<'s>(&mut self, e: &mut RleRebuild<L>, s: &'s Slab<RleTail>, n: usize) {
        let ghost a = RleModel::<L>::rb_read(*e) as int;
        let mut d = DSum::<VOf<'s, L>, B>::new();
        discard::<L, DSum<VOf<'s, L>, B>>(e, s, n, &mut d);
        proof {
            <RleModel<L> as Slabs>::lemma_len(*s);
            assert(d.seen() =~= Seq::<VOf<'s, L>>::empty() + crate::rle::dec::slab_values::<<L as WriteOps<'s>>::R>(
                s.data@,
            ).subrange(a, a + n));
            crate::rle::edit::lemma_vals_read::<L>(*s, a, a + n);
        }
        self.absorb(s, Ghost(a), Ghost(a + n), &d);
    }

    /// A slab dropped whole: its deltas' sum, and whether one is null,
    /// by decoding it (`slab_dsum`).
    #[inline(always)]
    fn dropped(&mut self, s: &Slab<RleTail>) {
        proof {
            use_type_invariant(&*self);
            <RleModel<L> as ReadsAgree<'static, RleReads<O>>>::lemma_reads(*s);
        }
        let (t, dn) = slab_dsum::<RleReads<O>, B>(s);
        let ghost xv = <RleReads<O> as crate::iter_core::Decoders<'static>>::vals(*s);
        let ghost ov = RleModel::<L>::vals(*s);
        proof {
            assert(dvos::<'static, L, O, B>(ov) =~= deltas_of::<_, B>(xv)) by {
                assert forall|i: int| 0 <= i < xv.len() implies dvos::<'static, L, O, B>(ov)[i] == deltas_of::<_, B>(xv)[i] by {
                    <B as DeltaWrite<'static, RleModel<L>, RleReads<O>>>::axiom_own(xv[i]);
                }
            }
            assert(dvos::<'static, L, O, B>(self.seen@ + ov) =~= dvos::<'static, L, O, B>(self.seen@) + dvos::<'static, L, O, B>(ov));
            lemma_crop_cat(dvos::<'static, L, O, B>(self.seen@), dvos::<'static, L, O, B>(ov), self.sum, t);
            lemma_wrapping_add(self.sum, t);
            lemma_null_cat(dvos::<'static, L, O, B>(self.seen@), dvos::<'static, L, O, B>(ov));
        }
        *self = DSumVisit { sum: self.sum.wrapping_add(t), null: self.null || dn, seen: Ghost(self.seen@ + ov), _p: PhantomData };
    }
}

/// A delta column's storage, as its edit cursor sees it.
pub(crate) type DCol<L, O, B> = ColumnCore<RleModel<L>, SlabAgg, SlabBTree<SlabAgg>, DeltaModel<RleReads<O>, B>>;

/// A delta column's edit: the column cursor over its storage
/// ([`EditCore`]) and the ledger of the realized values it writes
/// (`SpanLedger`), which keeps the deltas after the cursor decoding to
/// their values.
#[verifier::reject_recursive_types(L)]
#[verifier::reject_recursive_types(O)]
#[verifier::reject_recursive_types(B)]
pub(crate) struct DeltaEditCore<L, O, B> where
    L: for<'t> Lift<'t, W = L> + for<'t> WriteOps<'t, R = O>,
    O: for<'b> RleOps<'b>,
    B: for<'a> DeltaWrite<'a, RleModel<L>, RleReads<O>>,
 {
    pub(crate) edit: EditCore<RleModel<L>, SlabAgg, SlabBTree<SlabAgg>, DeltaModel<RleReads<O>, B>>,
    pub(crate) ledger: SpanLedger,
}

/// Panics: a column of `usize::MAX` slabs, which no memory holds.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn too_many_slabs()
    ensures
        false,
{
    panic!("a column of usize::MAX slabs")
}

/// Panics: a delta column of more than `i64::MAX` items.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn too_long()
    ensures
        false,
{
    panic!("a delta column of more than i64::MAX items")
}

/// Panics: an edit that left a delta column's values outside its domain.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn out_of_domain()
    ensures
        false,
{
    panic!("delta values out of domain: they would span 2^63 or more")
}

/// Panics: a delta edit opened past the column's end.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn out_of_bounds()
    ensures
        false,
{
    panic!("edit_at out of bounds")
}

impl<L, O, B> DeltaEditCore<L, O, B> where
    L: for<'t> Lift<'t, W = L> + for<'t> WriteOps<'t, R = O>,
    O: for<'b> RleOps<'b>,
    B: for<'a> DeltaWrite<'a, RleModel<L>, RleReads<O>>,
 {
    /// The ledger accounts for the edit: its original deltas are the
    /// column's, those it has passed are the cursor's, and those it has
    /// written are the edit's.
    pub(crate) open spec fn inv(&self) -> bool {
        &&& self.edit.inv()
        &&& self.ledger.inv()
        &&& self.ledger.tracking@
        &&& self.edit.held() ==> {
            &&& self.ledger.orig@ == dvos::<'static, L, O, B>(self.edit.orig_vals())
            &&& self.ledger.l.read@ == self.ledger.orig@.take(self.edit.at())
            &&& self.ledger.l.written@ == dvos::<'static, L, O, B>(self.edit.written())
        }
    }

    /// The values the column held when the edit began, as its iterators
    /// read them: at each delta the running sum through it, and nulls.
    pub(crate) open spec fn rorig(&self) -> Seq<Option<i64>> {
        rz(0, self.ledger.orig@)
    }

    /// The values the edit has written before the cursor.
    pub(crate) open spec fn rwritten(&self) -> Seq<Option<i64>> {
        rz(0, self.ledger.l.written@)
    }

    /// The cursor's position among the original items.
    pub(crate) open spec fn at(&self) -> int {
        self.edit.at()
    }

    /// Where a step over `n` items from the cursor ends: `n` on, or the
    /// end.
    pub(crate) open spec fn past(&self, n: usize) -> int {
        self.at() + self.edit.step_len(n)
    }

    /// The same edit of the same column as `o`, perhaps further on.
    pub(crate) open spec fn same(&self, o: Self) -> bool {
        &&& self.ledger.orig == o.ledger.orig
        &&& self.edit.orig_vals() == o.edit.orig_vals()
    }

    /// Open a delta edit of `col` at item `at`.
    pub(crate) fn begin(col: &mut DCol<L, O, B>, at: usize) -> (e: Self)
        ensures
            e.inv(),
            e.edit.held(),
            e.edit.orig_vals() == old(col)@,
            e.edit.at() == at,
            e.rorig() == rvals::<RleModel<L>, RleReads<O>, B>(old(col)@),
            e.rwritten() == e.rorig().take(at as int),
    {
        if at > col.len() {
            out_of_bounds();
        }
        col.lemma_column();
        let ghost c0 = *col;
        let ledger = col.delta_ledger(at);
        let edit = EditCore::begin(col, at);
        proof {
            lemma_dslabs_own::<RleModel<L>, RleReads<O>, B>(c0.slabs_spec());
            assert(dvos::<'static, L, O, B>(c0@) =~= dslabs::<RleReads<O>, B>(c0.slabs_spec()));
            assert(dvos::<'static, L, O, B>(c0@.take(at as int)) =~= dvos::<'static, L, O, B>(c0@).take(at as int));
            lemma_rz_sub(ledger.orig@, at as int, at as int);
            assert(dvos::<'static, L, O, B>(c0@) =~= c0@.map_values(
                |o: VOf<'static, L>| <B as DeltaWrite<'static, RleModel<L>, RleReads<O>>>::dvo(o),
            ));
        }
        DeltaEditCore { edit, ledger }
    }

    /// The original delta at the cursor, or `None` at the end.
    pub(crate) fn peek_delta(&mut self) -> (r: Option<Option<i64>>)
        requires
            old(self).inv(),
            old(self).edit.held(),
        ensures
            final(self).inv(),
            final(self).edit.held(),
            final(self).edit.orig_vals() == old(self).edit.orig_vals(),
            final(self).edit.at() == old(self).edit.at(),
            final(self).edit.written() == old(self).edit.written(),
            final(self).ledger == old(self).ledger,
            final(self).edit.rebuilding() == old(self).edit.rebuilding(),
            r matches Some(d) ==> old(self).edit.at() < old(self).edit.orig_vals().len() && d == old(self).ledger.orig@[old(
                self,
            ).edit.at()],
            r is None ==> old(self).edit.at() == old(self).edit.orig_vals().len(),
    {
        let ghost (ov, a) = (self.edit.orig_vals(), self.edit.at());
        proof {
            self.edit.lemma_at();
        }
        let read = if self.edit.dirty() {
            let r = self.edit.rebuild_reader();
            proof {
                if r is None {
                    assert(self.edit.at() == self.edit.orig_vals().len());
                }
            }
            r
        } else {
            self.edit.reader()
        };
        match read {
            Some((st, slab)) => {
                let ghost k = choose|k: nat|
                    #![trigger <RleModel<L> as Slabs>::st_at(slab.data@, *st, k)]
                    k < <RleModel<L> as Slabs>::vals(*slab).len() && <RleModel<L> as Slabs>::st_at(slab.data@, *st, k)
                        && <RleModel<L> as Slabs>::vals(*slab)[k as int] == ov[a];
                let v = crate::rle::edit::peek_at::<L>(slab, st, Ghost(k));
                let d = B::to_opt(v);
                proof {
                    <B as DeltaWrite<'static, RleModel<L>, RleReads<O>>>::axiom_own(v);
                    assert(<L as Lift<'static>>::lift(v) == ov[a]);
                    assert(dvos::<'static, L, O, B>(ov)[a] == <B as DeltaWrite<'static, RleModel<L>, RleReads<O>>>::dvo(ov[a]));
                }
                Some(d)
            },
            None => None,
        }
    }

    /// The realized value before the cursor, read from the column, no
    /// rebuild open and the debt paid: an index jump where the ledger's
    /// bound, or else a walk, shows the column in its domain (and so its
    /// index exact); else a decode from the start.
    pub(crate) fn running_now(&self) -> (r: i64)
        requires
            self.edit.inv(),
            self.edit.held(),
            !self.edit.rebuilding(),
            self.ledger.inv(),
            self.ledger.tracking@,
            self.ledger.l.adjust == 0 || self.ledger.l.closed@,
            self.ledger.l.written@ + self.ledger.rest() == dvos::<'static, L, O, B>(self.edit.now()),
        ensures
            r == crop(psum(dvos::<'static, L, O, B>(self.edit.written()))),
    {
        let (slabs, index, len) = self.edit.clean_parts();
        let out = self.edit.out_pos();
        let sl = slabs.as_slice();
        let ghost s = sl@;
        let ghost v = crate::iter_core::vals_of::<RleReads<O>>(s);
        let ghost now = self.edit.now();
        let ghost w = dvos::<'static, L, O, B>(self.edit.written());
        proof {
            lemma_lens_ok::<RleModel<L>, RleReads<O>>(s);
            lemma_dslabs_own::<RleModel<L>, RleReads<O>, B>(s);
            self.edit.lemma_at();
            assert(dvos::<'static, L, O, B>(now) =~= dslabs::<RleReads<O>, B>(s));
            assert(now =~= self.edit.written() + self.edit.orig_vals().skip(self.edit.at()));
            assert(dvos::<'static, L, O, B>(now).take(out as int) =~= w);
            assert(dslabs::<RleReads<O>, B>(s).take(out as int) =~= deltas_of::<_, B>(v.take(out as int)));
        }
        if out == 0 {
            proof {
                assert(w =~= Seq::<Option<i64>>::empty());
                crate::delta::diter::lemma_crop(0);
            }
            return 0;
        }
        if sl.len() == usize::MAX {
            too_many_slabs();
        }
        let small = len as u64 <= i64::MAX as u64;
        let by_bound = small && self.ledger.fits(i64::MIN, i64::MAX);
        let fits = by_bound || (small && slabs_fit::<RleReads<O>, B>(sl, len, i64::MIN, i64::MAX));
        proof {
            if by_bound {
                lemma_span_steps(dslabs::<RleReads<O>, B>(s), i64::MIN as int, i64::MAX as int);
            }
        }
        // verified (`delta_running_at`): an index jump where the index is
        // exact, else a decode from the start
        crate::column_core::delta_running_at::<RleModel<L>, RleReads<O>, B>(sl, index, len, out, fits)
    }

    /// The deltas of `n` copies of an inserted value holding `d`.
    proof fn lemma_ins(ws: Seq<VOf<'static, L>>, x: VOf<'static, L>, n: nat, d: Option<i64>)
        requires
            <B as DeltaWrite<'static, RleModel<L>, RleReads<O>>>::dvo(x) == d,
        ensures
            dvos::<'static, L, O, B>(ws + Seq::new(n, |_i: int| x)) == dvos::<'static, L, O, B>(ws) + reps(n, d),
    {
        assert(dvos::<'static, L, O, B>(ws + Seq::new(n, |_i: int| x)) =~= dvos::<'static, L, O, B>(ws) + reps(n, d));
    }

    /// Write `n` copies of the value holding delta `d`.
    #[inline(always)]
    fn put(&mut self, d: Option<i64>, n: usize)
        requires
            old(self).edit.inv(),
            old(self).edit.held(),
        ensures
            final(self).edit.inv(),
            final(self).edit.held(),
            final(self).ledger == old(self).ledger,
            final(self).edit.orig_vals() == old(self).edit.orig_vals(),
            final(self).edit.at() == old(self).edit.at(),
            dvos::<'static, L, O, B>(final(self).edit.written()) == dvos::<'static, L, O, B>(old(self).edit.written()) + reps(
                n as nat,
                d,
            ),
    {
        let x = <B as DeltaWrite<'static, RleModel<L>, RleReads<O>>>::ins(d);
        let ghost xv = x.val();
        proof {
            Self::lemma_ins(self.edit.written(), xv, n as nat, d);
        }
        self.edit.insert(x, n);
    }

    /// Insert `n` copies of the realized value `v`: a delta to reach it,
    /// then zeroes.
    pub(crate) fn insert_value(&mut self, v: i64, n: usize)
        requires
            old(self).inv(),
            old(self).edit.held(),
            n >= 1,
        ensures
            final(self).inv(),
            final(self).edit.held(),
            final(self).same(*old(self)),
            final(self).at() == old(self).at(),
            final(self).rwritten() == old(self).rwritten() + reps(n as nat, Some(v)),
    {
        let ghost w0 = dvos::<'static, L, O, B>(self.edit.written());
        let d = self.ledger.insert_value(v, n, true);
        self.put(Some(d), 1);
        if n > 1 {
            self.put(Some(0), n - 1);
        } else {
            proof {
                assert(reps(0, Some(0i64)) =~= Seq::<Option<i64>>::empty());
            }
        }
        proof {
            assert(reps(1, Some(d)) =~= seq![Some(d)]);
            assert(w0 + reps(1, Some(d)) + reps((n - 1) as nat, Some(0i64)) =~= w0 + (seq![Some(d)] + reps((n - 1) as nat, Some(0i64))));
        }
    }

    /// Insert `n` nulls.
    pub(crate) fn insert_nulls(&mut self, n: usize)
        requires
            old(self).inv(),
            old(self).edit.held(),
        ensures
            final(self).inv(),
            final(self).edit.held(),
            final(self).same(*old(self)),
            final(self).at() == old(self).at(),
            final(self).rwritten() == old(self).rwritten() + reps(n as nat, None),
    {
        self.ledger.insert_nulls(n);
        self.put(None, n);
    }

    /// Insert the run of `n` values from `prefix` by steps of `d`: its
    /// first delta re-anchored to the running value, and the rest kept.
    pub(crate) fn insert_run(&mut self, prefix: i64, d: i64, n: usize)
        requires
            old(self).inv(),
            old(self).edit.held(),
            n >= 1,
        ensures
            final(self).inv(),
            final(self).edit.held(),
            final(self).same(*old(self)),
            final(self).at() == old(self).at(),
            final(self).rwritten() == old(self).rwritten() + run_vals(prefix as int, d, n as nat),
    {
        let ghost w0 = dvos::<'static, L, O, B>(self.edit.written());
        let first = self.ledger.insert_run(prefix, d, n, true);
        if first == d {
            self.put(Some(d), n);
            proof {
                assert(seq![Some(first)] + reps((n - 1) as nat, Some(d)) =~= reps(n as nat, Some(d)));
            }
        } else {
            self.put(Some(first), 1);
            if n > 1 {
                self.put(Some(d), n - 1);
            } else {
                proof {
                    assert(reps(0, Some(d)) =~= Seq::<Option<i64>>::empty());
                }
            }
            proof {
                assert(reps(1, Some(first)) =~= seq![Some(first)]);
                assert(w0 + reps(1, Some(first)) + reps((n - 1) as nat, Some(d)) =~= w0 + (seq![Some(first)] + reps((n - 1) as nat, Some(d))));
            }
        }
    }

    /// Pay the debt into the item at the cursor: carry a null on, or
    /// rewrite the next delta to absorb it. False, the ledger closed, if
    /// nothing follows.
    pub(crate) fn pay_step(&mut self) -> (paid: bool)
        requires
            old(self).inv(),
            old(self).edit.held(),
        ensures
            final(self).inv(),
            final(self).edit.held(),
            final(self).same(*old(self)),
            paid ==> final(self).at() == old(self).at() + 1,
            paid ==> final(self).rwritten() == old(self).rwritten() + old(self).rorig().subrange(
                old(self).at(),
                old(self).at() + 1,
            ),
            !paid ==> {
                &&& final(self).at() == old(self).at() == old(self).edit.orig_vals().len()
                &&& final(self).ledger.l.adjust == 0
                &&& final(self).rwritten() == old(self).rwritten()
            },
    {
        let ghost a = self.edit.at();
        proof {
            self.edit.lemma_at();
        }
        if self.edit.remaining() == 0 {
            // nothing follows, so nothing decodes against the cursor
            self.ledger.close();
            return false;
        }
        proof {
            self.lemma_orig_range(a, a + 1);
        }
        // take the item out, seeing its delta
        let mut v = DSumVisit::<L, O, B>::new();
        self.edit.delete_with(1, &mut v);
        let t = v.total();
        let null = v.saw_null();
        let ghost x = self.ledger.orig@.subrange(a, a + 1);
        proof {
            assert(Seq::<VOf<'static, L>>::empty() + self.edit.orig_vals().subrange(a, a + 1) =~= self.edit.orig_vals().subrange(a, a + 1));
            assert(x =~= seq![x[0]]);
        }
        if null {
            // a null holds no delta: it goes back as it was
            proof {
                assert(x[0] is None);
                assert(x =~= reps(1, None::<i64>));
                crate::delta::diter::lemma_crop(0);
                crate::delta::agg::lemma_run_sum(Seq::empty(), 1, None);
                assert(Seq::<Option<i64>>::empty() + reps(1, None::<i64>) =~= x);
            }
            self.ledger.delete(0, Ghost(x));
            self.ledger.insert_nulls(1);
            self.put(None, 1);
            proof {
                assert(self.ledger.l.read@ =~= self.ledger.orig@.take(self.edit.at()));
                assert(dvos::<'static, L, O, B>(self.edit.written()) =~= self.ledger.l.written@);
            }
        } else {
            // the delta absorbs the debt
            proof {
                assert(x[0] is Some);
                crate::delta::agg::lemma_run_sum(Seq::empty(), 1, x[0]);
                assert(Seq::<Option<i64>>::empty() + reps(1, x[0]) =~= x);
                crate::delta::diter::lemma_crop(psum(x));
            }
            let nd = self.ledger.pay(t);
            self.put(Some(nd), 1);
            proof {
                assert(self.ledger.l.read@ =~= self.ledger.orig@.take(self.edit.at()));
                assert(reps(1, Some(nd)) =~= seq![Some(nd)]);
                assert(dvos::<'static, L, O, B>(self.edit.written()) =~= self.ledger.l.written@);
            }
        }
        true
    }

    /// Move forward `n` items: the debt paid out of the move (into the
    /// first non-null item carried), then the rest carried, the ledger
    /// keeping the running value (the visitor's sum of the deltas
    /// carried, or after a jump the column's).
    pub(crate) fn advance(&mut self, n: usize)
        requires
            old(self).inv(),
            old(self).edit.held(),
        ensures
            final(self).inv(),
            final(self).edit.held(),
            final(self).same(*old(self)),
            final(self).at() == old(self).past(n),
            final(self).rwritten() == old(self).rwritten() + old(self).rorig().subrange(old(self).at(), final(self).at()),
    {
        let ghost (a0, v0, o0) = (self.at(), self.rwritten(), self.rorig());
        proof {
            self.edit.lemma_at();
            assert(o0.len() == self.edit.orig_vals().len());
            assert(v0 =~= v0 + o0.subrange(a0, a0));
        }
        let mut want = n;
        while want > 0 && self.ledger.l.adjust != 0
            invariant
                self.inv(),
                self.edit.held(),
                self.same(*old(self)),
                want <= n,
                0 <= a0 == old(self).at(),
                v0 == old(self).rwritten(),
                o0 == old(self).rorig(),
                o0.len() == self.edit.orig_vals().len(),
                self.at() == a0 + (n - want),
                self.at() <= self.edit.orig_vals().len(),
                self.rwritten() == v0 + o0.subrange(a0, self.at()),
            decreases want,
        {
            let ghost (a1, w1) = (self.at(), self.rwritten());
            if !self.pay_step() {
                return;
            }
            proof {
                self.edit.lemma_at();
                assert(w1 + o0.subrange(a1, a1 + 1) =~= v0 + o0.subrange(a0, self.at()));
            }
            want -= 1;
        }
        if want == 0 || self.edit.remaining() == 0 {
            return;
        }
        proof {
            self.edit.lemma_at();
        }
        let ghost (a, k) = (self.edit.at(), self.edit.step_len(want));
        let ghost w0 = self.ledger.l.written@;
        let ghost x = dvos::<'static, L, O, B>(self.edit.orig_vals().subrange(a, a + k));
        proof {
            self.lemma_orig_range(a, a + k);
            // the values carried are the original ones there
            lemma_rz_sub(self.ledger.orig@, a, a + k);
        }
        let mut v = DSumVisit::<L, O, B>::new();
        let jumped = self.edit.advance_with(want, &mut v);
        let r = if jumped > 0 {
            proof {
                let ov = self.edit.orig_vals();
                assert(self.edit.now() =~= self.edit.written() + ov.skip(a + k));
                assert(ov.skip(a) =~= ov.subrange(a, a + k) + ov.skip(a + k));
                assert(dvos::<'static, L, O, B>(self.edit.now()) =~= w0 + dvos::<'static, L, O, B>(ov.skip(a)));
                assert(self.ledger.rest() =~= dvos::<'static, L, O, B>(ov.skip(a)));
                assert(dvos::<'static, L, O, B>(self.edit.written()) =~= w0 + x);
            }
            self.running_now()
        } else {
            let t = v.total();
            proof {
                assert(Seq::<VOf<'static, L>>::empty() + self.edit.orig_vals().subrange(a, a + k) =~= self.edit.orig_vals().subrange(a, a + k));
                lemma_crop_cat(w0, x, self.ledger.l.running, t);
                lemma_wrapping_add(self.ledger.l.running, t);
            }
            self.ledger.l.running.wrapping_add(t)
        };
        self.ledger.jump(r, Ghost(x));
        proof {
            assert(self.ledger.l.read@ =~= self.ledger.orig@.take(self.edit.at()));
            assert(dvos::<'static, L, O, B>(self.edit.written()) =~= self.ledger.l.written@);
            assert(self.rwritten() =~= v0 + o0.subrange(a0, self.at()));
        }
    }

    /// Pay the debt wherever it falls, so every realized value after the
    /// cursor is what it was.
    pub(crate) fn settle(&mut self)
        requires
            old(self).inv(),
            old(self).edit.held(),
        ensures
            final(self).inv(),
            final(self).edit.held(),
            final(self).same(*old(self)),
            final(self).ledger.l.adjust == 0,
            old(self).at() <= final(self).at() <= final(self).edit.orig_vals().len(),
            final(self).rwritten() == old(self).rwritten() + old(self).rorig().subrange(old(self).at(), final(self).at()),
    {
        let ghost (a0, v0, o0) = (self.at(), self.rwritten(), self.rorig());
        proof {
            self.edit.lemma_at();
            assert(o0.len() == self.edit.orig_vals().len());
            assert(v0 =~= v0 + o0.subrange(a0, a0));
        }
        while self.ledger.l.adjust != 0
            invariant
                self.inv(),
                self.edit.held(),
                self.same(*old(self)),
                0 <= a0 == old(self).at(),
                v0 == old(self).rwritten(),
                o0 == old(self).rorig(),
                o0.len() == self.edit.orig_vals().len(),
                a0 <= self.at() <= self.edit.orig_vals().len(),
                self.rwritten() == v0 + o0.subrange(a0, self.at()),
            ensures
                self.inv(),
                self.edit.held(),
                self.same(*old(self)),
                self.ledger.l.adjust == 0,
                a0 <= self.at() <= self.edit.orig_vals().len(),
                self.rwritten() == v0 + o0.subrange(a0, self.at()),
            decreases self.edit.orig_vals().len() - self.edit.at(),
        {
            proof {
                self.edit.lemma_at();
            }
            let ghost (a1, w1) = (self.at(), self.rwritten());
            if !self.pay_step() {
                break;
            }
            proof {
                self.edit.lemma_at();
                assert(w1 + o0.subrange(a1, a1 + 1) =~= v0 + o0.subrange(a0, self.at()));
            }
        }
    }

    /// The realized value at the cursor: `None` at the end, `Some(None)`
    /// for a null.
    pub(crate) fn peek_value(&mut self) -> (r: Option<Option<i64>>)
        requires
            old(self).inv(),
            old(self).edit.held(),
        ensures
            final(self).inv(),
            final(self).edit.held(),
            final(self).same(*old(self)),
            final(self).at() == old(self).at(),
            final(self).rwritten() == old(self).rwritten(),
            r is None <==> old(self).at() == old(self).edit.orig_vals().len(),
            r matches Some(v) ==> v == old(self).rorig()[old(self).at()],
    {
        match self.peek_delta() {
            None => None,
            Some(None) => {
                proof {
                    self.edit.lemma_at();
                }
                Some(None)
            },
            Some(Some(d)) => {
                proof {
                    self.edit.lemma_at();
                    assert(self.ledger.l.read@.len() == self.edit.at());
                    assert(self.ledger.orig@.len() == self.edit.orig_vals().len());
                }
                Some(Some(self.ledger.next_value(d)))
            },
        }
    }

    /// End the edit: pay the debt, close the cursor, check the values in
    /// `min..=max` within less than 2^63 (the ledger's bound shows it, or
    /// a walk), and hand `col` its parts back.
    pub(crate) fn end(&mut self, col: &mut DCol<L, O, B>, min: i64, max: i64)
        requires
            old(self).inv(),
            old(self).edit.held(),
        ensures
            rvals::<RleModel<L>, RleReads<O>, B>(final(col)@) == old(self).rwritten() + old(self).rorig().skip(old(self).at()),
    {
        let ghost (a0, v0, o0) = (self.at(), self.rwritten(), self.rorig());
        proof {
            self.edit.lemma_at();
            assert(o0.len() == self.edit.orig_vals().len());
        }
        self.settle();
        self.edit.close();
        let (slabs, _, len) = self.edit.clean_parts();
        let ghost s = slabs@;
        proof {
            lemma_lens_ok::<RleModel<L>, RleReads<O>>(s);
        }
        if len as u64 > i64::MAX as u64 {
            too_long();
        }
        let by_bound = self.ledger.fits(min, max);
        if !by_bound && !slabs_fit::<RleReads<O>, B>(slabs.as_slice(), len, min, max) {
            out_of_domain();
        }
        proof {
            lemma_lens_ok::<RleModel<L>, RleReads<O>>(s);
            lemma_dslabs_own::<RleModel<L>, RleReads<O>, B>(s);
            self.edit.lemma_at();
            let ov = self.edit.orig_vals();
            assert(self.edit.now() =~= self.edit.written() + ov.skip(self.edit.at()));
            assert(dvos::<'static, L, O, B>(self.edit.now()) =~= dslabs::<RleReads<O>, B>(s));
            // the values it leaves: those written, then the original ones
            // after the cursor, as they were
            let l = self.ledger.l;
            let rest = self.ledger.rest();
            assert(rest =~= dvos::<'static, L, O, B>(ov.skip(self.edit.at())));
            assert(dvos::<'static, L, O, B>(self.edit.now()) =~= l.written@ + rest);
            lemma_rest_unchanged(l, rest);
            assert(l.read@ + rest =~= self.ledger.orig@);
            assert(o0.subrange(a0, self.at()) + o0.skip(self.at()) =~= o0.skip(a0));
            assert(rz(0, l.written@ + rest) =~= v0 + o0.skip(a0));
            let now = self.edit.now();
            assert(dvos::<'static, L, O, B>(now) =~= now.map_values(
                |o: VOf<'static, L>| <B as DeltaWrite<'static, RleModel<L>, RleReads<O>>>::dvo(o),
            ));
            if by_bound {
                assert(self.ledger.rest() =~= dvos::<'static, L, O, B>(ov.skip(self.edit.at())));
                assert(self.ledger.l.written@ + self.ledger.rest() =~= dslabs::<RleReads<O>, B>(s));
                lemma_span_steps(dslabs::<RleReads<O>, B>(s), min as int, max as int);
            }
        }
        self.edit.end_in_domain(col);
    }

    /// The original deltas of items `a..b`.
    proof fn lemma_orig_range(&self, a: int, b: int)
        requires
            self.inv(),
            self.edit.held(),
            0 <= a <= b <= self.edit.orig_vals().len(),
        ensures
            self.ledger.orig@.subrange(a, b) == dvos::<'static, L, O, B>(self.edit.orig_vals().subrange(a, b)),
    {
        assert(self.ledger.orig@.subrange(a, b) =~= dvos::<'static, L, O, B>(self.edit.orig_vals().subrange(a, b)));
    }

    /// Delete `n` items at the cursor (all that are left, if fewer): the
    /// ledger owes the deleted deltas' sum to the next value.
    pub(crate) fn delete(&mut self, n: usize)
        requires
            old(self).inv(),
            old(self).edit.held(),
        ensures
            final(self).inv(),
            final(self).edit.held(),
            final(self).same(*old(self)),
            final(self).at() == old(self).past(n),
            final(self).rwritten() == old(self).rwritten(),
    {
        if self.edit.remaining() == 0 {
            return;
        }
        proof {
            self.edit.lemma_at();
        }
        let ghost (a, k) = (self.edit.at(), self.edit.step_len(n));
        proof {
            self.lemma_orig_range(a, a + k);
        }
        let mut v = DSumVisit::<L, O, B>::new();
        self.edit.delete_with(n, &mut v);
        let t = v.total();
        proof {
            assert(Seq::<VOf<'static, L>>::empty() + self.edit.orig_vals().subrange(a, a + k) =~= self.edit.orig_vals().subrange(a, a + k));
        }
        self.ledger.delete(t, Ghost(dvos::<'static, L, O, B>(self.edit.orig_vals().subrange(a, a + k))));
        proof {
            assert(self.ledger.l.read@ =~= self.ledger.orig@.take(self.edit.at()));
        }
    }
}

} // verus!
