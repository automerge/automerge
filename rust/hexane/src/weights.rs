//! The weight a column's index keeps for each slab, verified with Verus.
//!
//! The edit cursor keeps every slab's weight related to the slab by the
//! column's weight model (`cursor::Weigh::wrel`), and each weight
//! function names its model (`WeightFn::Model`), so the relation is
//! concrete, and each model's `compute` is the verified computation:
//!
//! - [`LenModel`], a plain column's: the slab's item count.
//! - [`PrefixModel`], a prefix column's: the item count and the sum of the
//!   slab's values (`vsum`), computed by `prefix_core::p_slab_sum` (or its
//!   checked twin, which panics where the sum would overflow).
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
use crate::btree::{in_i64, lemma_prefix_len, SlabAggregate};
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::{all_ok, lemma_lens_ok, lemma_vals_own, slab_lens};
use crate::cursor::{ReadsAgree, Slabs, Weigh};
#[cfg(verus_keep_ghost)]
use crate::delta::agg::{agg_of, deltas_of, pdiff, psum, reps, steps_fit};
use crate::delta::agg::{agg_run, slab_agg, DeltaOps, DomainWalk};
use crate::delta::ddomain::slabs_fit;
#[cfg(verus_keep_ghost)]
use crate::delta::ddomain::{
    dslabs, lemma_dslabs_cat, lemma_dslabs_len, lemma_dslabs_unit, lemma_walk_steps,
};
#[cfg(verus_keep_ghost)]
use crate::delta::diter::{crop, lemma_crop};
#[cfg(verus_keep_ghost)]
use crate::fenwick::total;
use crate::index::IndexOps;
use crate::iter_core::{Decode, Decoders};
use crate::prefix::PrefixSlabWeight;
#[cfg(verus_keep_ghost)]
use crate::prefix_core::{lemma_vsum_cat, lemma_vsum_run, vsum};
use crate::prefix_core::{p_slab_sum, p_slab_sum_checked, PrefixOps};
use crate::{PackError, Run};
use std::marker::PhantomData;
use std::ops::{AddAssign, SubAssign};
use vstd::prelude::*;

verus! {

/// Panics: a prefix sum that overflows its type.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn sum_overflows()
    ensures
        false,
{
    panic!("prefix sum overflows its type")
}

/// The error a load reports for a slab whose item count overflows.
#[verifier::external_body]
#[cold]
fn count_overflows() -> PackError {
    PackError::BadFormat
}

/// The error a load reports for a prefix sum that overflows its type.
#[verifier::external_body]
#[cold]
fn prefix_sum_overflows() -> PackError {
    PackError::InvalidValue("prefix sum overflows its type".into())
}

/// The error a load reports for a delta running sum that overflows.
#[verifier::external_body]
#[cold]
fn delta_sum_overflows() -> PackError {
    PackError::InvalidValue("delta running sum overflows i64".into())
}

/// The error a load reports for a delta value outside its type's domain.
#[verifier::external_body]
#[cold]
fn delta_out_of_domain() -> PackError {
    PackError::InvalidValue("delta value out of domain".into())
}

/// The error a load reports for delta values spanning 2^63 or more.
#[verifier::external_body]
#[cold]
fn delta_too_wide() -> PackError {
    PackError::InvalidValue("delta values out of domain: they span 2^63 or more".into())
}

/// The error a load reports for a delta column of more than `i64::MAX`
/// items.
#[verifier::external_body]
#[cold]
fn delta_too_long(len: usize) -> PackError {
    PackError::InvalidLength(len, i64::MAX as usize)
}

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
    /// the model's domain.
    fn load_check(ws: &[W], slabs: &[Slab<E::Tail>], len: usize, lo: i64, hi: i64) -> (r: Result<(), PackError>)
        requires
            all_ok::<E>(slabs@),
            ws@.len() == slabs@.len(),
            len == total(slab_lens(slabs@)),
            Self::accumulates() ==> forall|i: int| 0 <= i < ws@.len() ==> Self::wvals(#[trigger] ws@[i], X::vals(slabs@[i])),
        ensures
            r is Ok ==> Self::dom_ok(slabs@),
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

    proof fn lemma_dom_empty(s: Seq<Slab<E::Tail>>) {
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

    proof fn lemma_wrel(w: usize, s: Slab<E::Tail>) {
        E::lemma_reads(s);
    }

    #[inline(always)]
    fn load_check(_ws: &[usize], _slabs: &[Slab<E::Tail>], _len: usize, _lo: i64, _hi: i64) -> Result<(), PackError> {
        Ok(())
    }
}

/// A prefix column's weights: each slab's item count and the sum of its
/// values, through read model `X` and prefix arithmetic `B`.
pub struct PrefixModel<X, B>(PhantomData<(X, B)>);

impl<E, X, B, P> Weigh<E, PrefixSlabWeight<P>> for PrefixModel<X, B> where
    E: Slabs + for<'a> ReadsAgree<'a, X>,
    X: for<'a> Decoders<'a, Tail = E::Tail>,
    B: for<'a> PrefixOps<<<X as Decoders<'a>>::D as Decode>::Item, P = P>,
    P: Clone + Default + std::fmt::Debug + AddAssign + SubAssign,
 {
    open spec fn wrel(w: PrefixSlabWeight<P>, s: Slab<E::Tail>) -> bool {
        &&& w.len == s.len
        &&& w.prefix == vsum::<_, B>(<X as Decoders<'static>>::vals(s))
    }

    #[inline(always)]
    fn compute<I: IndexOps<PrefixSlabWeight<P>>>(s: &Slab<E::Tail>) -> (w: PrefixSlabWeight<P>) {
        proof {
            <E as ReadsAgree<'static, X>>::lemma_reads(*s);
        }
        let prefix = if s.len <= <B as PrefixOps<<<X as Decoders<'static>>::D as Decode>::Item>>::max_unchecked_len() {
            p_slab_sum::<X, B>(s)
        } else {
            match p_slab_sum_checked::<X, B>(s) {
                Some(p) => p,
                None => {
                    sum_overflows();
                    unreached()
                },
            }
        };
        let w = PrefixSlabWeight { len: s.len, prefix };
        proof {
            I::lemma_wlen(w);
            lemma_prefix_len(w);
        }
        w
    }

    #[verifier::external_body]
    #[inline(always)]
    fn check_set<I: IndexOps<PrefixSlabWeight<P>>>(index: &I, i: usize, w: &PrefixSlabWeight<P>, len: usize) {
        check_total::<I, B, P, <<X as Decoders<'static>>::D as Decode>::Item>(index, i, i + 1, std::slice::from_ref(w), len)
    }

    #[verifier::external_body]
    #[inline(always)]
    fn check_splice<I: IndexOps<PrefixSlabWeight<P>>>(
        index: &I,
        start: usize,
        end: usize,
        new: &Vec<PrefixSlabWeight<P>>,
        len: usize,
    ) {
        check_total::<I, B, P, <<X as Decoders<'static>>::D as Decode>::Item>(index, start, end, new, len)
    }

    open spec fn dom_ok(s: Seq<Slab<E::Tail>>) -> bool {
        true
    }

    #[inline(always)]
    fn dom_check(_slabs: &Vec<Slab<E::Tail>>, _len: usize) -> bool {
        true
    }

    proof fn lemma_dom_empty(s: Seq<Slab<E::Tail>>) {
    }
}

impl<'a, X, B, P> Accumulate<<X::D as Decode>::Item, PrefixSlabWeight<P>> for PrefixModel<X, B> where
    X: Decoders<'a>,
    B: PrefixOps<<X::D as Decode>::Item, P = P>,
    P: Clone + Default + std::fmt::Debug + AddAssign + SubAssign,
 {
    open spec fn wvals(w: PrefixSlabWeight<P>, vs: Seq<<X::D as Decode>::Item>) -> bool {
        &&& w.len == vs.len()
        &&& w.prefix == vsum::<_, B>(vs)
    }

    #[inline(always)]
    fn new_zero() -> PrefixSlabWeight<P> {
        PrefixSlabWeight { len: 0, prefix: B::zero() }
    }

    /// The run's sum checked (`try_acc_run`): data whose sums the prefix
    /// type can't hold is rejected rather than wrapped.
    #[inline(always)]
    fn accumulate_run(
        w: &mut PrefixSlabWeight<P>,
        n: usize,
        v: <X::D as Decode>::Item,
        Ghost(vs): Ghost<Seq<<X::D as Decode>::Item>>,
    ) -> Result<(), PackError> {
        if w.len > usize::MAX - n {
            return Err(count_overflows());
        }
        if !B::try_acc_run(&mut w.prefix, &Run { count: n, value: v }) {
            return Err(prefix_sum_overflows());
        }
        w.len += n;
        proof {
            lemma_vsum_cat::<_, B>(vs, Seq::new(n as nat, |_i: int| v));
            lemma_vsum_run::<_, B>(n as nat, v);
        }
        Ok(())
    }
}

impl<'a, E, X, B, P> LoadWeigh<'a, E, X, PrefixSlabWeight<P>> for PrefixModel<X, B> where
    E: Slabs + for<'b> ReadsAgree<'b, X>,
    X: for<'b> Decoders<'b, Tail = E::Tail>,
    B: for<'b> PrefixOps<<<X as Decoders<'b>>::D as Decode>::Item, P = P>,
    P: Clone + Default + std::fmt::Debug + AddAssign + SubAssign,
 {
    open spec fn accumulates() -> bool {
        true
    }

    #[inline(always)]
    fn accumulates_exec() -> bool {
        true
    }

    proof fn lemma_wrel(w: PrefixSlabWeight<P>, s: Slab<E::Tail>) {
        <E as ReadsAgree<'a, X>>::lemma_reads(s);
    }

    /// A column longer than one whose sums cannot overflow: its total, the
    /// weights' sum, checked (`try_add`). Its values add nonnegative
    /// amounts, so the total bounds every prefix sum. Not part of the
    /// proof: it can only fail.
    #[inline(always)]
    fn load_check(ws: &[PrefixSlabWeight<P>], _slabs: &[Slab<E::Tail>], len: usize, _lo: i64, _hi: i64) -> Result<(), PackError> {
        if len > <B as PrefixOps<<<X as Decoders<'a>>::D as Decode>::Item>>::max_unchecked_len() {
            let mut total = <B as PrefixOps<<<X as Decoders<'a>>::D as Decode>::Item>>::zero();
            let mut i: usize = 0;
            while i < ws.len()
                decreases ws.len() - i,
            {
                if !<B as PrefixOps<<<X as Decoders<'a>>::D as Decode>::Item>>::try_add(&mut total, &ws[i].prefix) {
                    return Err(prefix_sum_overflows());
                }
                i += 1;
            }
        }
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

    proof fn lemma_dom_empty(s: Seq<Slab<E::Tail>>) {
        lemma_vals_own::<E, X>(s);
        let d = dslabs::<X, B>(s);
        assert(d =~= Seq::<Option<i64>>::empty());
        assert forall|i: int, j: int| 0 <= i <= j <= d.len() implies in_i64(#[trigger] pdiff(d, i, j)) by {
            assert(d.take(0) =~= Seq::<Option<i64>>::empty());
        }
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
        Ok(())
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

/// Panic unless the column `index` indexes, with `new` in place of the
/// weights of slabs `start..end`, sums within the prefix type, for a
/// column of `len` items longer than one whose sums cannot overflow. Its
/// values add nonnegative amounts, so the total bounds every prefix sum.
#[inline(always)]
fn check_total<I, B, P, V>(
    index: &I,
    start: usize,
    end: usize,
    new: &[PrefixSlabWeight<P>],
    len: usize,
) where
    I: IndexOps<PrefixSlabWeight<P>>,
    B: PrefixOps<V, P = P>,
    V: Copy,
    P: Clone + Default + std::fmt::Debug + AddAssign + SubAssign,
{
    if len > B::max_unchecked_len() {
        check_spliced_total::<I, B, P, V>(index, start, end, new);
    }
}

#[cold]
#[inline(never)]
fn check_spliced_total<I, B, P, V>(index: &I, start: usize, end: usize, new: &[PrefixSlabWeight<P>])
where
    I: IndexOps<PrefixSlabWeight<P>>,
    B: PrefixOps<V, P = P>,
    V: Copy,
    P: Clone + Default + std::fmt::Debug + AddAssign + SubAssign,
{
    // the other slabs' total: it fits, as the column's did
    let mut total = index.ix_total().prefix;
    for i in start..end {
        total -= index.ix_weight(i).prefix;
    }
    for w in new {
        if !B::try_add(&mut total, &w.prefix) {
            panic!("prefix sum overflows its type");
        }
    }
}
