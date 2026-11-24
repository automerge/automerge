//! The weight a column's index keeps for each slab, verified with Verus.
//!
//! The edit cursor keeps every slab's weight related to the slab by the
//! column's weight model (`cursor::Weigh::wrel`), and each model's
//! `compute` is the verified computation:
//!
//! - [`LenModel`], a plain column's: the slab's item count.
//! - the prefix and accumulator columns' (`crate::verified::sums`,
//!   `crate::verified::acc`): the item count and the sum of the slab's
//!   values (`vsum`), computed by `prefix_core::p_slab_sum`.
//! - [`DeltaModel`], a delta column's: the item count and, where the
//!   slab's running sums fit `i64`, the aggregate of its deltas
//!   (`agg_of`), computed by `delta::agg::slab_agg`. Its domain
//!   (`Weigh::dom_ok`), part of the column's invariant, is that every step
//!   between two of the column's running sums fits, so every slab's
//!   weight is its aggregate (`delta::ddomain`).
//!
//! A load uses each model too ([`LoadWeigh`]): it accumulates the weight
//! of a slab's values as their runs stream past (`attrib::Accumulate`:
//! the item count, the checked sum, the exact delta aggregate), which is
//! proved to be the slab's weight, and makes the model's load checks,
//! which give its domain (a delta column's walk over the aggregates, and
//! where they span too much, over the values).
//!
//! The models read slabs through their encoding's read model, which
//! agrees with the edit model (`cursor::ReadsAgree`). Trusted: the value
//! types' contributions and deltas (`PrefixOps`, `DeltaOps`), and the
//! prefix columns' run-time overflow checks, which specify nothing as they
//! can only panic.

use crate::attrib::Accumulate;
use crate::btree::SlabAgg;
#[cfg(verus_keep_ghost)]
use crate::btree::{exact_clone, in_i64, SlabAggregate};
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::{all_ok, lemma_lens_ok, lemma_vals_own, slab_lens, vals_of};
use crate::cursor::{ReadsAgree, Slabs, Weigh};
#[cfg(verus_keep_ghost)]
use crate::delta::agg::{agg_of, deltas_of, pdiff, psum, reps, steps_fit};
use crate::delta::agg::{agg_run, slab_agg, DeltaOps, DomainWalk};
#[cfg(verus_keep_ghost)]
use crate::delta::dbound::{in_bound, lemma_sums_bound};
#[cfg(verus_keep_ghost)]
use crate::delta::ddomain::{
    dslabs, lemma_dslabs_cat, lemma_dslabs_len, lemma_dslabs_own, lemma_dslabs_unit,
    lemma_walk_steps,
};
use crate::delta::ddomain::{slabs_fit, DeltaWrite};
#[cfg(verus_keep_ghost)]
use crate::delta::dedit::rz;
#[cfg(verus_keep_ghost)]
use crate::delta::diter::{crop, lemma_crop};
#[cfg(verus_keep_ghost)]
use crate::fenwick::total;
use crate::index::IndexOps;
use crate::iter_core::{Decode, Decoders};
#[cfg(verus_keep_ghost)]
use crate::prefix_core::{lemma_vsum_cat, lemma_vsum_run, vsum};
#[cfg(verus_keep_ghost)]
use crate::prefix_core::{p_slab_sum, PrefixOps};
use crate::PackError;
use std::marker::PhantomData;
use vstd::prelude::*;

pub(crate) use crate::trusted::count_overflows;
pub(crate) use crate::trusted::delta_out_of_domain;
use crate::trusted::delta_sum_overflows;
use crate::trusted::delta_too_long;
use crate::trusted::delta_too_wide;
pub(crate) use crate::trusted::prefix_sum_overflows;
pub(crate) use crate::trusted::sum_overflows;
verus! {








/// A weight model as a load uses it: weights accumulated as the runs
/// stream past (`Accumulate`), each proved to be its slab's weight
/// (`lemma_wrel`), and the checks a load makes of them, which give the
/// model's domain.
pub(crate) trait LoadWeigh<'a, E: ReadsAgree<'a, X>, X: Decoders<'a, Tail = <E as Slabs>::Tail>, W>: Weigh<E, W> + Accumulate<
    <X::D as Decode>::Item,
    W,
> {
    /// Whether a load accumulates the weights as the runs pass, rather
    /// than computing each slab's (`Weigh::compute`).
    spec fn accumulates() -> bool;

    fn accumulates_exec() -> (r: bool)
        ensures
            r == Self::accumulates(),
    ;

    /// A load of this model may refuse a column of the values `vs`: it
    /// fails only where the loader does, where the column is too long,
    /// or where this holds.
    spec fn may_refuse(vs: Seq<E::V>) -> bool;

    /// A model that accumulates the weights may refuse any column: its
    /// checks are made as the runs pass, and nothing is said of them.
    proof fn lemma_may_refuse(vs: Seq<E::V>)
        ensures
            Self::accumulates() ==> Self::may_refuse(vs),
    ;

    /// What the checks of a load say of the slabs `s` they pass, made
    /// with the domain `lo..=hi`: of a delta column, that its values
    /// lie in it.
    spec fn bounded(s: Seq<Slab<E::Tail>>, lo: i64, hi: i64) -> bool;

    /// The weight accumulated from a slab's values is its weight.
    proof fn lemma_wrel(w: W, s: Slab<E::Tail>)
        requires
            E::ok(s),
            Self::wvals(w, X::vals(s)),
        ensures
            Self::wrel(w, s),
    ;

    /// The checks a load makes of the weights of `slabs`, `len` items
    /// (`lo..=hi`, a delta column's domain): passing, the column is in
    /// the model's domain, and is what the checks say (`bounded`).
    fn load_check(ws: &[W], slabs: &[Slab<E::Tail>], len: usize, lo: i64, hi: i64) -> (r: Result<(), PackError>)
        requires
            all_ok::<E>(slabs@),
            ws@.len() == slabs@.len(),
            len == total(slab_lens(slabs@)),
            Self::accumulates() ==> forall|i: int| 0 <= i < ws@.len() ==> Self::wvals(#[trigger] ws@[i], X::vals(slabs@[i])),
        ensures
            r is Ok ==> Self::dom_ok(slabs@) && Self::bounded(slabs@, lo, hi),
            r is Err ==> Self::may_refuse(vals_of::<E>(slabs@)),
    ;
}

/// A weight model whose columns can be copied: a clone of a weight is the
/// weight, and what the model says of a slab and of a column it says of
/// their clones (`Slab::same`).
pub(crate) trait CopyWeigh<E: Slabs, W: Clone>: Weigh<E, W> {
    proof fn lemma_copy_weight()
        ensures
            exact_clone::<W>(),
    ;

    proof fn lemma_copy_wrel(w: W, a: Slab<E::Tail>, b: Slab<E::Tail>)
        requires
            E::ok(a),
            a.same(b),
            Self::wrel(w, a),
        ensures
            Self::wrel(w, b),
    ;

    proof fn lemma_copy_dom(s: Seq<Slab<E::Tail>>, t: Seq<Slab<E::Tail>>)
        requires
            all_ok::<E>(s),
            t.len() == s.len(),
            forall|j: int| 0 <= j < s.len() ==> (#[trigger] s[j]).same(t[j]),
            Self::dom_ok(s),
        ensures
            Self::dom_ok(t),
    ;
}

/// A plain column's weights: each slab's item count.
pub struct LenModel;

impl<E: Slabs> Weigh<E, usize> for LenModel {
    open spec fn wrel(w: usize, s: Slab<E::Tail>) -> bool {
        w == s.len
    }

    #[inline(always)]
    fn compute<I: IndexOps<usize>>(s: &Slab<E::Tail>) -> (w: usize) {
        proof {
            I::lemma_wlen(s.len);
        }
        s.len
    }

    #[inline(always)]
    fn check_set<I: IndexOps<usize>>(_index: &I, _i: usize, _w: &usize, _len: usize) {
    }

    #[inline(always)]
    fn check_splice<I: IndexOps<usize>>(_index: &I, _start: usize, _end: usize, _new: &Vec<usize>, _len: usize) {
    }

    open spec fn dom_ok(s: Seq<Slab<E::Tail>>) -> bool {
        true
    }

    #[inline(always)]
    fn dom_check(_slabs: &Vec<Slab<E::Tail>>, _len: usize) -> bool {
        true
    }

    type Pre = ();

    open spec fn pre_ok(p: (), s: Seq<Slab<E::Tail>>) -> bool {
        true
    }

    #[inline(always)]
    fn dom_check_spliced<I: IndexOps<usize>>(
        _pre: (),
        _slabs: &Vec<Slab<E::Tail>>,
        _index: &I,
        _len: usize,
        _at: usize,
        _ins: usize,
        _old: Ghost<Seq<Slab<E::Tail>>>,
        _del: Ghost<int>,
    ) -> bool {
        true
    }

    #[inline(always)]
    fn total_ok(_ws: &[usize], _len: usize) -> bool {
        true
    }

    proof fn lemma_dom_empty(s: Seq<Slab<E::Tail>>) {
    }
}

impl<E: Slabs> CopyWeigh<E, usize> for LenModel {
    proof fn lemma_copy_weight() {
    }

    proof fn lemma_copy_wrel(w: usize, a: Slab<E::Tail>, b: Slab<E::Tail>) {
    }

    proof fn lemma_copy_dom(s: Seq<Slab<E::Tail>>, t: Seq<Slab<E::Tail>>) {
    }
}

impl<V> Accumulate<V, usize> for LenModel {
    open spec fn wvals(w: usize, vs: Seq<V>) -> bool {
        w == vs.len()
    }

    #[inline(always)]
    fn new_zero() -> usize {
        0
    }

    #[inline(always)]
    fn accumulate_run(w: &mut usize, n: usize, _v: V, Ghost(vs): Ghost<Seq<V>>) -> Result<(), PackError> {
        if *w > usize::MAX - n {
            return Err(count_overflows());
        }
        *w += n;
        Ok(())
    }
}

impl<'a, E: Slabs + ReadsAgree<'a, X>, X: Decoders<'a, Tail = E::Tail>> LoadWeigh<'a, E, X, usize> for LenModel {
    /// A slab's item count is in its metadata: a load computes it.
    open spec fn accumulates() -> bool {
        false
    }

    #[inline(always)]
    fn accumulates_exec() -> bool {
        false
    }

    open spec fn may_refuse(vs: Seq<E::V>) -> bool {
        false
    }

    open spec fn bounded(s: Seq<Slab<E::Tail>>, lo: i64, hi: i64) -> bool {
        true
    }

    proof fn lemma_may_refuse(vs: Seq<E::V>) {
    }

    proof fn lemma_wrel(w: usize, s: Slab<E::Tail>) {
        E::lemma_reads(s);
    }

    #[inline(always)]
    fn load_check(_ws: &[usize], _slabs: &[Slab<E::Tail>], _len: usize, _lo: i64, _hi: i64) -> Result<(), PackError> {
        Ok(())
    }
}


/// A delta column's weights: each slab's item count, the sum of its deltas
/// (wrapping), and, where its running sums fit `i64`, their aggregate,
/// through read model `X` and deltas `B`.
pub struct DeltaModel<X, B>(PhantomData<(X, B)>);

impl<E, X, B> Weigh<E, SlabAgg> for DeltaModel<X, B> where
    E: Slabs + for<'a> ReadsAgree<'a, X>,
    X: for<'a> Decoders<'a, Tail = E::Tail>,
    B: for<'a> DeltaOps<<<X as Decoders<'a>>::D as Decode>::Item>,
 {
    open spec fn wrel(w: SlabAgg, s: Slab<E::Tail>) -> bool {
        let ds = deltas_of::<_, B>(<X as Decoders<'static>>::vals(s));
        &&& w.len == s.len
        &&& w.total == crop(psum(ds))
        &&& steps_fit(ds) ==> agg_of(w, ds)
    }

    #[inline(always)]
    fn compute<I: IndexOps<SlabAgg>>(s: &Slab<E::Tail>) -> (w: SlabAgg) {
        proof {
            <E as ReadsAgree<'static, X>>::lemma_reads(*s);
        }
        let w = slab_agg::<X, B>(s);
        proof {
            I::lemma_wlen(w);
        }
        w
    }

    #[inline(always)]
    fn check_set<I: IndexOps<SlabAgg>>(_index: &I, _i: usize, _w: &SlabAgg, _len: usize) {
    }

    #[inline(always)]
    fn check_splice<I: IndexOps<SlabAgg>>(_index: &I, _start: usize, _end: usize, _new: &Vec<SlabAgg>, _len: usize) {
    }

    /// Every step between two of the column's running sums fits `i64`:
    /// then each slab's weight is its deltas' aggregate, and the index
    /// valid (`ddomain::lemma_dix_ok`), as the readers ask.
    open spec fn dom_ok(s: Seq<Slab<E::Tail>>) -> bool {
        steps_fit(dslabs::<X, B>(s))
    }

    fn dom_check(slabs: &Vec<Slab<E::Tail>>, len: usize) -> bool {
        proof {
            lemma_lens_ok::<E, X>(slabs@);
        }
        slabs_fit::<X, B>(slabs.as_slice(), len, i64::MIN, i64::MAX)
    }

    type Pre = ();

    open spec fn pre_ok(p: (), s: Seq<Slab<E::Tail>>) -> bool {
        true
    }

    /// The whole check: a delta column's own splices check what they
    /// put in themselves (`ColumnCore::splice_delta`).
    fn dom_check_spliced<I: IndexOps<SlabAgg>>(
        _pre: (),
        slabs: &Vec<Slab<E::Tail>>,
        _index: &I,
        len: usize,
        _at: usize,
        _ins: usize,
        _old: Ghost<Seq<Slab<E::Tail>>>,
        _del: Ghost<int>,
    ) -> bool {
        <Self as Weigh<E, SlabAgg>>::dom_check(slabs, len)
    }

    /// The domain (`dom_check`) bounds the sums: nothing more to check.
    #[inline(always)]
    fn total_ok(_ws: &[SlabAgg], _len: usize) -> bool {
        true
    }

    proof fn lemma_dom_empty(s: Seq<Slab<E::Tail>>) {
        lemma_vals_own::<E, X>(s);
        let d = dslabs::<X, B>(s);
        assert(d =~= Seq::<Option<i64>>::empty());
        assert forall|i: int, j: int| 0 <= i <= j <= d.len() implies in_i64(#[trigger] pdiff(d, i, j)) by {
            assert(d.take(0) =~= Seq::<Option<i64>>::empty());
        }
    }
}

/// A copy of a delta column's slabs keeps their weights and their
/// domain: a slab's deltas are those of its owned values
/// (`DeltaWrite::axiom_own`), which a copy holds too.
impl<E, X, B> CopyWeigh<E, SlabAgg> for DeltaModel<X, B> where
    E: Slabs + for<'a> ReadsAgree<'a, X>,
    X: for<'a> Decoders<'a, Tail = E::Tail>,
    B: for<'a> DeltaWrite<'a, E, X>,
 {
    proof fn lemma_copy_weight() {
    }

    proof fn lemma_copy_wrel(w: SlabAgg, a: Slab<E::Tail>, b: Slab<E::Tail>) {
        E::lemma_copy(a, b);
        <E as ReadsAgree<'static, X>>::lemma_reads(a);
        <E as ReadsAgree<'static, X>>::lemma_reads(b);
        let xa = <X as Decoders<'static>>::vals(a);
        let xb = <X as Decoders<'static>>::vals(b);
        let da = deltas_of::<_, B>(xa);
        let db = deltas_of::<_, B>(xb);
        assert forall|i: int| 0 <= i < da.len() implies (#[trigger] da[i]) == db[i] by {
            <B as DeltaWrite<'static, E, X>>::axiom_own(xa[i]);
            <B as DeltaWrite<'static, E, X>>::axiom_own(xb[i]);
            assert(E::vals(a)[i] == <E as ReadsAgree<'static, X>>::own(xa[i]));
            assert(E::vals(b)[i] == <E as ReadsAgree<'static, X>>::own(xb[i]));
        }
        assert(da =~= db);
    }

    proof fn lemma_copy_dom(s: Seq<Slab<E::Tail>>, t: Seq<Slab<E::Tail>>) {
        let f = |x: Slab<E::Tail>| E::vals(x);
        assert forall|j: int| 0 <= j < t.len() implies E::ok(#[trigger] t[j]) && f(t[j]) == f(s[j]) by {
            E::lemma_copy(s[j], t[j]);
        }
        assert(t.map_values(f) =~= s.map_values(f));
        lemma_dslabs_own::<E, X, B>(s);
        lemma_dslabs_own::<E, X, B>(t);
    }
}

impl<'a, X, B> Accumulate<<X::D as Decode>::Item, SlabAgg> for DeltaModel<X, B> where
    X: Decoders<'a>,
    B: DeltaOps<<X::D as Decode>::Item>,
 {
    /// Exactly the aggregate of the values' deltas.
    open spec fn wvals(w: SlabAgg, vs: Seq<<X::D as Decode>::Item>) -> bool {
        agg_of(w, deltas_of::<_, B>(vs))
    }

    #[inline(always)]
    fn new_zero() -> SlabAgg {
        let w = SlabAgg { len: 0, total: 0, min_offset: 0, max_offset: 0 };
        proof {
            assert(deltas_of::<_, B>(Seq::<<X::D as Decode>::Item>::empty()) =~= Seq::<Option<i64>>::empty());
        }
        w
    }

    /// Exact (`agg_run`): a running sum that leaves `i64` is an error, as
    /// the delta domain rules those out.
    #[inline(always)]
    fn accumulate_run(
        w: &mut SlabAgg,
        n: usize,
        v: <X::D as Decode>::Item,
        Ghost(vs): Ghost<Seq<<X::D as Decode>::Item>>,
    ) -> Result<(), PackError> {
        if w.len > usize::MAX - n {
            return Err(delta_sum_overflows());
        }
        let ghost ds = deltas_of::<_, B>(vs);
        if agg_run(w, n, B::to_opt(v), Ghost(ds)) {
            proof {
                assert(deltas_of::<_, B>(vs + Seq::new(n as nat, |_i: int| v)) =~= ds + reps(n as nat, B::dv(v)));
            }
            Ok(())
        } else {
            Err(delta_sum_overflows())
        }
    }
}

impl<'a, E, X, B> LoadWeigh<'a, E, X, SlabAgg> for DeltaModel<X, B> where
    E: Slabs + for<'b> ReadsAgree<'b, X>,
    X: for<'b> Decoders<'b, Tail = E::Tail>,
    B: for<'b> DeltaOps<<<X as Decoders<'b>>::D as Decode>::Item>,
 {
    open spec fn accumulates() -> bool {
        true
    }

    #[inline(always)]
    fn accumulates_exec() -> bool {
        true
    }

    open spec fn may_refuse(vs: Seq<E::V>) -> bool {
        true
    }

    open spec fn bounded(s: Seq<Slab<E::Tail>>, lo: i64, hi: i64) -> bool {
        in_bound(rz(0, dslabs::<X, B>(s)), lo as int, hi as int)
    }

    proof fn lemma_may_refuse(vs: Seq<E::V>) {
    }

    proof fn lemma_wrel(w: SlabAgg, s: Slab<E::Tail>) {
        <E as ReadsAgree<'a, X>>::lemma_reads(s);
    }

    /// The domain: walking the slabs' exact aggregates, every running
    /// sum lies in `lo..=hi` (`DomainWalk`); where those sums span more
    /// than `i64` holds, the values themselves are read (`slabs_fit`,
    /// since nulls hold the running sum, which can be a 0 that is no
    /// value); and the column holds at most `i64::MAX` items.
    fn load_check(ws: &[SlabAgg], slabs: &[Slab<E::Tail>], len: usize, lo: i64, hi: i64) -> Result<(), PackError> {
        let ghost s = slabs@;
        proof {
            lemma_lens_ok::<E, X>(s);
        }
        let mut walk = DomainWalk::new();
        let mut i: usize = 0;
        proof {
            assert(s.take(0) =~= Seq::<Slab<E::Tail>>::empty());
            lemma_dslabs_unit::<X, B>(s[0]);
            assert(walk.ds@ =~= dslabs::<X, B>(s.take(0)));
        }
        while i < ws.len()
            invariant
                s == slabs@,
                ws@.len() == s.len(),
                forall|k: int| 0 <= k < ws@.len() ==> Self::wvals(#[trigger] ws@[k], X::vals(s[k])),
                0 <= i <= s.len(),
                walk.inv(lo, hi),
                walk.ds@ == dslabs::<X, B>(s.take(i as int)),
            decreases ws.len() - i,
        {
            if !walk.slab(&ws[i], lo, hi, Ghost(deltas_of::<_, B>(X::vals(s[i as int])))) {
                return Err(delta_out_of_domain());
            }
            proof {
                assert(s.take(i + 1) =~= s.take(i as int) + seq![s[i as int]]);
                lemma_dslabs_cat::<X, B>(s.take(i as int), seq![s[i as int]]);
                lemma_dslabs_unit::<X, B>(s[i as int]);
            }
            i += 1;
        }
        proof {
            assert(s.take(i as int) =~= s);
        }
        if len as u64 > i64::MAX as u64 {
            return Err(delta_too_long(len));
        }
        if walk.too_wide(Ghost(lo), Ghost(hi)) {
            if !slabs_fit::<X, B>(slabs, len, lo, hi) {
                return Err(delta_too_wide());
            }
        } else {
            proof {
                lemma_dslabs_len::<E, X, B>(s);
                lemma_walk_steps(dslabs::<X, B>(s), lo as int, hi as int);
            }
        }
        proof {
            // the least and the greatest of the sums walked bound the
            // values
            let ds = dslabs::<X, B>(s);
            if ds.len() > 0 {
                lemma_sums_bound(ds);
            }
        }
        Ok(())
    }
}

} // verus!
