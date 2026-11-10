//! A delta column's domain, verified with Verus: the invariant its readers
//! need, and why it holds.
//!
//! The readers of a delta column ask that each slab's weight be the
//! aggregate of its deltas, and that every merge of those aggregates be
//! representable (`diter::dix_ok`). Both follow from one property of the
//! column's deltas: every step between two of its running sums fits
//! `i64` (`agg::steps_fit`). The column keeps that as part of its type
//! invariant (`weights::DeltaModel::dom_ok`), so a reader's precondition
//! comes from the column itself (`lemma_dix_ok`).
//!
//! Proved: steps that fit over the whole column fit over any stretch of it
//! (`lemma_steps_sub`); where they fit, the index's aggregate of any run
//! of slabs is that of their deltas (`lemma_fold_steps`), and any two
//! adjacent ones merge exactly (`lemma_merge_ok`), so the index is valid;
//! and values within `i64` and less than 2^63 apart, the bound an edit
//! checks (`dbound::span_ok`), have steps that fit (`lemma_span_steps`).
//!
//! A splice keeps the domain (`ColumnCore::splice_delta`): the cursor
//! takes its runs from a [`DeltaFeed`], which passes the items given
//! through the splice's plan (`dbound::SpanPlan`) and writes each delta
//! the plan emits as the value type writes it ([`DeltaWrite`]). Once the
//! splice is done, the plan's bound on the values shows the column it
//! leaves is in the domain (`lemma_splice_dom`); where the bound is too
//! wide, the column is read again ([`slabs_fit`]), and a splice that
//! would leave the domain panics. Building a column from values
//! (`ColumnCore::fill_delta`) is the same, through a [`ValuesFeed`],
//! which turns each value into the delta that realizes it
//! (`dbound::ToDeltas`).
//!
//! Trusted: [`DeltaWrite`], the value types' bridge (a delta written
//! reads back as itself, and a value read holds the delta its owned form
//! does); and, for a delta column's other ways in, the glue: that a
//! `DeltaEdit`'s checks (`dbound::SpanLedger`, then [`slabs_fit`]) are of
//! the column it leaves. A load's are verified with the load itself
//! (`load_core`, `weights::DeltaModel`).

#[cfg(verus_keep_ghost)]
use crate::btree::{fold, in_i64, SlabAgg, SlabAggregate};
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::{all_ok, lemma_vals_own, lemma_vals_total, vals_of as cvals_of};
#[cfg(verus_keep_ghost)]
use crate::cursor::{lemma_concat_append, slab_lens};
use crate::cursor::{Insertable, ReadsAgree, RunFeed, Slabs};
#[cfg(verus_keep_ghost)]
use crate::delta::agg::{
    agg_of, deltas_of, lemma_merge, lemma_take_run, pdiff, pmax, pmin, psum, reps, steps_fit, vmax,
    vmin,
};
use crate::delta::agg::{AnySpan, DeltaOps};
#[cfg(verus_keep_ghost)]
use crate::delta::dbound::{lemma_psum_within, span_ok, spliced};
use crate::delta::dbound::{SpanPlan, ToDeltas};
use crate::delta::dedit::SplicePhase;
#[cfg(verus_keep_ghost)]
use crate::delta::diter::{dix_ok, lemma_psum_cat};
#[cfg(verus_keep_ghost)]
use crate::fenwick::{lemma_sum_le, lemma_sum_one, lemma_sum_split, sum, total};
#[cfg(verus_keep_ghost)]
use crate::iter_core::{each, lens_ok, vals_of};
use crate::iter_core::{Decode, Decoders};
#[cfg(verus_keep_ghost)]
use crate::rle::writer::concat;
use std::marker::PhantomData;
use vstd::prelude::*;

verus! {

/// The deltas of slabs `s`, one slab's after another's.
pub(crate) open spec fn dslabs<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    s: Seq<Slab<X::Tail>>,
) -> Seq<Option<i64>> {
    deltas_of::<_, B>(vals_of::<X>(s))
}

/// The deltas of two runs of slabs, one after the other.
pub(crate) proof fn lemma_dslabs_cat<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    a: Seq<Slab<X::Tail>>,
    b: Seq<Slab<X::Tail>>,
)
    ensures
        dslabs::<X, B>(a + b) == dslabs::<X, B>(a) + dslabs::<X, B>(b),
{
    assert(each::<X>(a + b) =~= each::<X>(a) + each::<X>(b));
    lemma_concat_append(each::<X>(a), each::<X>(b));
    assert(deltas_of::<_, B>(vals_of::<X>(a) + vals_of::<X>(b)) =~= deltas_of::<_, B>(vals_of::<X>(a))
        + deltas_of::<_, B>(vals_of::<X>(b)));
}

/// The deltas of one slab, and of none.
pub(crate) proof fn lemma_dslabs_unit<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(x: Slab<X::Tail>)
    ensures
        dslabs::<X, B>(seq![x]) == deltas_of::<_, B>(X::vals(x)),
        dslabs::<X, B>(Seq::<Slab<X::Tail>>::empty()) == Seq::<Option<i64>>::empty(),
{
    let e = Seq::<Seq<<X::D as Decode>::Item>>::empty();
    assert(each::<X>(seq![x]) =~= e.push(X::vals(x)));
    assert(e.push(X::vals(x)).drop_last() =~= e);
    assert(concat(e) == Seq::<<X::D as Decode>::Item>::empty());
    assert(concat(e.push(X::vals(x))) =~= X::vals(x));
    assert(each::<X>(Seq::<Slab<X::Tail>>::empty()) =~= e);
    assert(deltas_of::<_, B>(Seq::<<X::D as Decode>::Item>::empty()) =~= Seq::<Option<i64>>::empty());
}

/// Steps that fit over deltas `ds` fit over any stretch of them.
pub(crate) proof fn lemma_steps_sub(ds: Seq<Option<i64>>, i: int, j: int)
    requires
        steps_fit(ds),
        0 <= i <= j <= ds.len(),
    ensures
        steps_fit(ds.subrange(i, j)),
{
    let t = ds.subrange(i, j);
    assert forall|a: int, b: int| 0 <= a <= b <= t.len() implies in_i64(#[trigger] pdiff(t, a, b)) by {
        assert(ds.take(i + a) =~= ds.take(i) + t.take(a));
        assert(ds.take(i + b) =~= ds.take(i) + t.take(b));
        lemma_psum_cat(ds.take(i), t.take(a));
        lemma_psum_cat(ds.take(i), t.take(b));
        assert(pdiff(ds, i + a, i + b) == pdiff(t, a, b));
    }
}

/// The running sum after deltas `ds`: 0 where they hold no value, else
/// within their values' sums.
proof fn lemma_last_value(ds: Seq<Option<i64>>)
    ensures
        (vmin(ds) > vmax(ds) && psum(ds) == 0) || (vmin(ds) <= psum(ds) <= vmax(ds)),
    decreases ds.len(),
{
    if ds.len() > 0 {
        lemma_last_value(ds.drop_last());
    }
}

/// Fewer deltas hold fewer values: their least is no less, their greatest
/// no greater.
proof fn lemma_vrange_take(ds: Seq<Option<i64>>, k: int)
    requires
        0 <= k <= ds.len(),
    ensures
        vmin(ds) <= vmin(ds.take(k)),
        vmax(ds.take(k)) <= vmax(ds),
    decreases ds.len(),
{
    if k < ds.len() {
        let d0 = ds.drop_last();
        assert(d0.take(k) =~= ds.take(k));
        lemma_vrange_take(d0, k);
    } else {
        assert(ds.take(k) =~= ds);
    }
}

/// Values within `i64`, less than 2^63 apart: every step between two
/// running sums fits, as a running sum is either a value's or the 0
/// before the first.
pub(crate) proof fn lemma_span_steps(ds: Seq<Option<i64>>, min: int, max: int)
    requires
        span_ok(ds, min, max),
        i64::MIN <= min,
        max <= i64::MAX,
        ds.len() <= i64::MAX,
    ensures
        steps_fit(ds),
{
    assert forall|i: int, j: int| 0 <= i <= j <= ds.len() implies in_i64(#[trigger] pdiff(ds, i, j)) by {
        let (a, b) = (ds.take(i), ds.take(j));
        lemma_last_value(a);
        lemma_last_value(b);
        lemma_vrange_take(ds, i);
        lemma_vrange_take(ds, j);
        assert(b.take(i) =~= a);
        lemma_vrange_take(b, i);
    }
}

/// Running sums after one or more deltas within `lo..=hi`, a range less
/// than 2^63 wide inside `i64` (what a load's walk over the slab
/// aggregates checks): every step between two running sums fits, the 0
/// before the first included.
pub(crate) proof fn lemma_walk_steps(ds: Seq<Option<i64>>, lo: int, hi: int)
    requires
        ds.len() > 0 ==> lo <= pmin(ds) && pmax(ds) <= hi && pmax(ds) - pmin(ds) <= i64::MAX,
        i64::MIN <= lo,
        hi <= i64::MAX,
        ds.len() <= i64::MAX,
    ensures
        steps_fit(ds),
{
    assert forall|i: int, j: int| 0 <= i <= j <= ds.len() implies in_i64(#[trigger] pdiff(ds, i, j)) by {
        if i < j {
            lemma_psum_within(ds, j);
            if i == 0 {
                assert(ds.take(0) =~= Seq::<Option<i64>>::empty());
            } else {
                lemma_psum_within(ds, i);
            }
        }
    }
}

/// Every running sum after deltas `ds`, shifted by `c`, fits `i64`: so do
/// their least and greatest.
proof fn lemma_pminmax_in(ds: Seq<Option<i64>>, c: int)
    requires
        ds.len() >= 1,
        forall|t: int| 1 <= t <= ds.len() ==> in_i64(c + #[trigger] psum(ds.take(t))),
    ensures
        in_i64(c + pmin(ds)),
        in_i64(c + pmax(ds)),
    decreases ds.len(),
{
    assert(ds.take(ds.len() as int) =~= ds);
    assert(in_i64(c + psum(ds.take(ds.len() as int))));
    if ds.len() > 1 {
        let d0 = ds.drop_last();
        assert forall|t: int| 1 <= t <= d0.len() implies in_i64(c + #[trigger] psum(d0.take(t))) by {
            assert(d0.take(t) =~= ds.take(t));
        }
        lemma_pminmax_in(d0, c);
    }
}

/// The aggregates of two stretches of deltas whose steps fit together
/// merge exactly.
pub(crate) proof fn lemma_merge_ok(a: SlabAgg, da: Seq<Option<i64>>, b: SlabAgg, db: Seq<Option<i64>>)
    requires
        agg_of(a, da),
        agg_of(b, db),
        steps_fit(da + db),
        da.len() + db.len() <= usize::MAX,
    ensures
        SlabAgg::merge_ok(a, b),
{
    if da.len() > 0 && db.len() > 0 {
        let d = da + db;
        let n = da.len() as int;
        assert(d.take(0) =~= Seq::<Option<i64>>::empty());
        assert(d.take(d.len() as int) =~= d);
        lemma_psum_cat(da, db);
        assert(pdiff(d, 0, d.len() as int) == psum(da) + psum(db));
        assert forall|t: int| 1 <= t <= db.len() implies in_i64(psum(da) + #[trigger] psum(db.take(t))) by {
            assert(d.take(n + t) =~= da + db.take(t));
            lemma_psum_cat(da, db.take(t));
            assert(pdiff(d, 0, n + t) == psum(da) + psum(db.take(t)));
        }
        lemma_pminmax_in(db, psum(da));
    }
}

/// Where the steps of the deltas of slabs `i..j` fit, the index's
/// aggregate of those slabs is that of their deltas.
pub(crate) proof fn lemma_fold_steps<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    ws: Seq<SlabAgg>,
    s: Seq<Slab<X::Tail>>,
    i: int,
    j: int,
)
    requires
        ws.len() == s.len(),
        0 <= i <= j <= s.len(),
        forall|k: int| 0 <= k < s.len() ==> agg_of(#[trigger] ws[k], deltas_of::<_, B>(X::vals(s[k]))),
        steps_fit(dslabs::<X, B>(s.subrange(i, j))),
        dslabs::<X, B>(s.subrange(i, j)).len() <= usize::MAX,
    ensures
        agg_of(fold(ws.subrange(i, j)), dslabs::<X, B>(s.subrange(i, j))),
    decreases j - i,
{
    if j == i {
        assert(ws.subrange(i, j) =~= Seq::<SlabAgg>::empty());
        assert(s.subrange(i, j) =~= Seq::<Slab<X::Tail>>::empty());
        lemma_dslabs_unit::<X, B>(s[0]);
    } else {
        let a = s.subrange(i, j - 1);
        assert(s.subrange(i, j) =~= a + seq![s[j - 1]]);
        lemma_dslabs_cat::<X, B>(a, seq![s[j - 1]]);
        lemma_dslabs_unit::<X, B>(s[j - 1]);
        let da = dslabs::<X, B>(a);
        let db = deltas_of::<_, B>(X::vals(s[j - 1]));
        assert(da =~= (da + db).subrange(0, da.len() as int));
        lemma_steps_sub(da + db, 0, da.len() as int);
        lemma_fold_steps::<X, B>(ws, s, i, j - 1);
        assert(ws.subrange(i, j).drop_last() =~= ws.subrange(i, j - 1));
        assert(ws.subrange(i, j).last() == ws[j - 1]);
        lemma_merge_ok(fold(ws.subrange(i, j - 1)), da, ws[j - 1], db);
        lemma_merge(fold(ws.subrange(i, j - 1)), da, ws[j - 1], db);
    }
}

/// The readers' invariant (`dix_ok`), from the column's: where the steps
/// of all its deltas fit, each slab's weight, which is its deltas'
/// aggregate wherever their steps fit, is, and every merge of the index's
/// aggregates is representable.
pub(crate) proof fn lemma_dix_ok<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    ws: Seq<SlabAgg>,
    s: Seq<Slab<X::Tail>>,
)
    requires
        ws.len() == s.len(),
        forall|k: int|
            0 <= k < s.len() ==> (steps_fit(deltas_of::<_, B>(X::vals(s[k]))) ==> agg_of(
                #[trigger] ws[k],
                deltas_of::<_, B>(X::vals(s[k])),
            )),
        steps_fit(dslabs::<X, B>(s)),
        dslabs::<X, B>(s).len() <= usize::MAX,
    ensures
        dix_ok::<X, B>(ws, s),
{
    let d = dslabs::<X, B>(s);
    // each run of slabs' deltas is a stretch of the column's
    assert forall|i: int, j: int| 0 <= i <= j <= s.len() implies steps_fit(
        #[trigger] dslabs::<X, B>(s.subrange(i, j)),
    ) && dslabs::<X, B>(s.subrange(i, j)).len() <= d.len() by {
        assert(s =~= (s.take(i) + s.subrange(i, j)) + s.skip(j));
        lemma_dslabs_cat::<X, B>(s.take(i) + s.subrange(i, j), s.skip(j));
        lemma_dslabs_cat::<X, B>(s.take(i), s.subrange(i, j));
        let p = dslabs::<X, B>(s.take(i));
        let m = dslabs::<X, B>(s.subrange(i, j));
        assert(m =~= d.subrange(p.len() as int, (p.len() + m.len()) as int));
        lemma_steps_sub(d, p.len() as int, (p.len() + m.len()) as int);
    }
    assert forall|k: int| 0 <= k < s.len() implies agg_of(#[trigger] ws[k], deltas_of::<_, B>(X::vals(s[k]))) by {
        assert(s.subrange(k, k + 1) =~= seq![s[k]]);
        lemma_dslabs_unit::<X, B>(s[k]);
        assert(steps_fit(dslabs::<X, B>(s.subrange(k, k + 1))));
    }
    assert forall|i: int, j: int, k: int| 0 <= i <= j <= k <= ws.len() implies #[trigger] SlabAgg::merge_ok(
        fold(ws.subrange(i, j)),
        fold(ws.subrange(j, k)),
    ) by {
        assert(steps_fit(dslabs::<X, B>(s.subrange(i, j))));
        assert(steps_fit(dslabs::<X, B>(s.subrange(j, k))));
        assert(steps_fit(dslabs::<X, B>(s.subrange(i, k))));
        lemma_fold_steps::<X, B>(ws, s, i, j);
        lemma_fold_steps::<X, B>(ws, s, j, k);
        assert(s.subrange(i, k) =~= s.subrange(i, j) + s.subrange(j, k));
        lemma_dslabs_cat::<X, B>(s.subrange(i, j), s.subrange(j, k));
        lemma_merge_ok(
            fold(ws.subrange(i, j)),
            dslabs::<X, B>(s.subrange(i, j)),
            fold(ws.subrange(j, k)),
            dslabs::<X, B>(s.subrange(j, k)),
        );
    }
}

/// Walk the runs of `slab` into `span`.
// a Verus loop with an invariant: `loop` + `match`
#[allow(clippy::while_let_loop)]
fn slab_span<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(span: &mut AnySpan, slab: &'a Slab<X::Tail>)
    requires
        old(span).inv(),
        X::ok(*slab),
        old(span).ds@.len() + X::vals(*slab).len() <= usize::MAX,
    ensures
        final(span).inv(),
        final(span).ds@ == old(span).ds@ + deltas_of::<_, B>(X::vals(*slab)),
{
    let ghost v = X::vals(*slab);
    let ghost d0 = span.ds@;
    let mut d = X::open(slab);
    let ghost mut k: int = 0;
    proof {
        assert(v.skip(0) =~= v);
        assert(d0 + deltas_of::<_, B>(v.take(0)) =~= d0);
    }
    loop
        invariant
            v == X::vals(*slab),
            d.inv(),
            0 <= k <= v.len(),
            d.rest() == v.skip(k),
            span.inv(),
            span.ds@ == d0 + deltas_of::<_, B>(v.take(k)),
            d0.len() + v.len() <= usize::MAX,
        ensures
            span.inv(),
            span.ds@ == d0 + deltas_of::<_, B>(v),
        decreases d.rest().len(),
    {
        match d.d_run_max(usize::MAX) {
            Some(run) => {
                proof {
                    lemma_take_run::<_, B>(v, k, run.count as int, run.value);
                }
                span.run(run.count, B::to_opt(run.value));
                proof {
                    assert(d0 + deltas_of::<_, B>(v.take(k)) + reps(run.count as nat, B::dv(run.value)) =~= d0
                        + deltas_of::<_, B>(v.take(k + run.count)));
                    k = k + run.count;
                }
            },
            None => {
                proof {
                    assert(v.take(k) =~= v);
                }
                break;
            },
        }
    }
}

/// Whether the values of the deltas of `slabs`, `len` items, lie in
/// `min..=max`, within a range less than 2^63 wide, reading every run;
/// then every step between two running sums fits `i64` (the delta
/// column's domain).
pub(crate) fn slabs_fit<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    slabs: &'a [Slab<X::Tail>],
    len: usize,
    min: i64,
    max: i64,
) -> (r: bool)
    requires
        lens_ok::<X>(slabs@),
        len == total(slab_lens(slabs@)),
    ensures
        r ==> steps_fit(dslabs::<X, B>(slabs@)),
{
    let ghost s = slabs@;
    let ghost lens = slab_lens(s);
    let mut span = AnySpan::new();
    let mut i: usize = 0;
    proof {
        assert(s.take(0) =~= Seq::<Slab<X::Tail>>::empty());
        lemma_dslabs_unit::<X, B>(s[0]);
        assert(span.ds@ =~= Seq::<Option<i64>>::empty());
    }
    while i < slabs.len()
        invariant
            s == slabs@,
            lens == slab_lens(s),
            lens_ok::<X>(s),
            len == total(lens),
            len <= usize::MAX,
            0 <= i <= s.len(),
            span.inv(),
            span.ds@ == dslabs::<X, B>(s.take(i as int)),
            span.ds@.len() == sum(lens, 0, i as int),
        decreases s.len() - i,
    {
        proof {
            lemma_sum_split(lens, 0, i as int, i + 1);
            lemma_sum_one(lens, i as int);
            lemma_sum_le(lens, 0, i + 1, 0, s.len() as int);
            assert(X::vals(s[i as int]).len() == s[i as int].len);
        }
        slab_span::<X, B>(&mut span, &slabs[i]);
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
    let r = len as u64 <= i64::MAX as u64 && span.fits(min, max);
    proof {
        if r {
            assert(span.ds@.len() == len) by {
                assert(sum(lens, 0, s.len() as int) == total(lens));
            }
            lemma_span_steps(span.ds@, min as int, max as int);
        }
    }
    r
}

/// A column's deltas, one per item.
pub(crate) proof fn lemma_dslabs_len<E, X, B>(s: Seq<Slab<E::Tail>>)
    where
        E: Slabs + for<'a> ReadsAgree<'a, X>,
        X: for<'a> Decoders<'a, Tail = E::Tail>,
        B: for<'a> DeltaOps<<<X as Decoders<'a>>::D as Decode>::Item>,
    requires
        all_ok::<E>(s),
    ensures
        dslabs::<X, B>(s).len() == total(slab_lens(s)),
{
    lemma_vals_own::<E, X>(s);
    lemma_vals_total::<E>(s);
}

/// A column's deltas, read, are its owned values' (`DeltaWrite::dvo`).
pub(crate) proof fn lemma_dslabs_own<E, X, B>(s: Seq<Slab<E::Tail>>)
    where
        E: Slabs + for<'a> ReadsAgree<'a, X>,
        X: for<'a> Decoders<'a, Tail = E::Tail>,
        B: for<'a> DeltaWrite<'a, E, X>,
    requires
        all_ok::<E>(s),
    ensures
        dslabs::<X, B>(s) == cvals_of::<E>(s).map_values(|o: E::V| <B as DeltaWrite<'static, E, X>>::dvo(o)),
{
    lemma_vals_own::<E, X>(s);
    let xv = vals_of::<X>(s);
    let f = |o: E::V| <B as DeltaWrite<'static, E, X>>::dvo(o);
    assert forall|i: int| 0 <= i < xv.len() implies #[trigger] dslabs::<X, B>(s)[i] == f(cvals_of::<E>(s)[i]) by {
        <B as DeltaWrite<'static, E, X>>::axiom_own(xv[i]);
    }
    assert(dslabs::<X, B>(s) =~= cvals_of::<E>(s).map_values(f));
}

/// A delta splice keeps the column's domain: its deltas are the
/// original ones before `at`, the plan's (`e`, holding the values `ins`
/// written), then the original ones after the deleted items and, where a
/// value followed them, after its nulls and it; where the plan's bound
/// shows those values lie in `min..=max`, within a range less than 2^63
/// wide (`span_ok` of `dbound::spliced`), every step fits.
pub(crate) proof fn lemma_splice_dom<E, X, B>(
    s0: Seq<Slab<E::Tail>>,
    s1: Seq<Slab<E::Tail>>,
    at: int,
    del: int,
    tdel: int,
    ins: Seq<E::V>,
    e: Seq<Option<i64>>,
    skip: nat,
    next_val: Option<i64>,
    min: int,
    max: int,
)
    where
        E: Slabs + for<'a> ReadsAgree<'a, X>,
        X: for<'a> Decoders<'a, Tail = E::Tail>,
        B: for<'a> DeltaWrite<'a, E, X>,
    requires
        all_ok::<E>(s0),
        all_ok::<E>(s1),
        0 <= at,
        0 <= del,
        tdel == if next_val is Some { del + skip + 1 } else { del },
        at + tdel <= cvals_of::<E>(s0).len(),
        cvals_of::<E>(s1) == cvals_of::<E>(s0).take(at) + ins + cvals_of::<E>(s0).skip(at + tdel),
        ins.map_values(|o: E::V| <B as DeltaWrite<'static, E, X>>::dvo(o)) == e,
        span_ok(
            spliced(dslabs::<X, B>(s0).take(at), e, dslabs::<X, B>(s0).skip(at + del), skip, next_val),
            min,
            max,
        ),
        i64::MIN <= min,
        max <= i64::MAX,
        cvals_of::<E>(s1).len() <= i64::MAX,
    ensures
        steps_fit(dslabs::<X, B>(s1)),
{
    let f = |o: E::V| <B as DeltaWrite<'static, E, X>>::dvo(o);
    lemma_dslabs_own::<E, X, B>(s0);
    lemma_dslabs_own::<E, X, B>(s1);
    let (v0, v1) = (cvals_of::<E>(s0), cvals_of::<E>(s1));
    let d0 = dslabs::<X, B>(s0);
    let tail = d0.skip(at + del);
    assert(d0.take(at) =~= v0.take(at).map_values(f));
    assert(v1.map_values(f) =~= v0.take(at).map_values(f) + ins.map_values(f) + v0.skip(at + tdel).map_values(f));
    assert(v0.skip(at + tdel).map_values(f) =~= d0.skip(at + tdel));
    if next_val is Some {
        assert(tail.skip(skip + 1int) =~= d0.skip(at + tdel));
    }
    assert(dslabs::<X, B>(s1) =~= spliced(d0.take(at), e, tail, skip, next_val));
    lemma_span_steps(dslabs::<X, B>(s1), min, max);
}

/// An item a delta splice inserts: a value (or a null), or a run of
/// `count` deltas `delta` (or nulls) whose values follow the running sum
/// `prefix`.
pub(crate) enum SpliceItem {
    Value(Option<i64>),
    Run(i64, Option<i64>, usize),
}

/// The items a delta splice inserts, from outside: nothing is assumed of
/// them.
pub(crate) trait SpliceItems {
    fn next_item(&mut self) -> Option<SpliceItem>;
}

/// The values a delta column is built from, from outside (`from_values`):
/// nothing is assumed of them.
pub(crate) trait ValueItems {
    fn next_value(&mut self) -> Option<Option<i64>>;
}

/// A delta column's values, as the cursor takes them ([`RunFeed`]) to
/// build the column: each turned into the delta that realizes it after
/// those before (`dbound::ToDeltas`), which keeps a bound on them, and
/// written as `B` writes it.
pub(crate) struct ValuesFeed<S, E: Slabs, M, B> {
    pub(crate) items: S,
    pub(crate) deltas: ToDeltas,
    /// the values written so far, and whether the items have run out
    #[allow(dead_code)]
    pub(crate) given: Ghost<Seq<E::V>>,
    #[allow(dead_code)]
    pub(crate) ended: Ghost<bool>,
    pub(crate) _p: PhantomData<(M, B)>,
}

impl<S: ValueItems, E, M, B> RunFeed<E> for ValuesFeed<S, E, M, B> where
    E: Slabs + for<'a> ReadsAgree<'a, M>,
    M: for<'a> Decoders<'a, Tail = E::Tail>,
    B: for<'a> DeltaWrite<'a, E, M>,
 {
    type X = <B as DeltaWrite<'static, E, M>>::Ins;

    type K = ();

    /// The converter's, keeping its bound, and the values written hold
    /// the deltas it gave.
    open spec fn inv(&self) -> bool {
        &&& self.deltas.inv()
        &&& self.deltas.tracking@
        &&& self.given@.map_values(|o: E::V| <B as DeltaWrite<'static, E, M>>::dvo(o)) == self.deltas.ds@
    }

    open spec fn key(&self) -> () {
        ()
    }

    open spec fn given(&self) -> Seq<E::V> {
        self.given@
    }

    open spec fn ended(&self) -> bool {
        self.ended@
    }

    #[inline(always)]
    fn next_run(&mut self) -> (r: Option<(Self::X, usize)>) {
        match self.items.next_value() {
            Some(v) => {
                let d = self.deltas.next(v, true);
                let x = <B as DeltaWrite<'static, E, M>>::ins(d);
                proof {
                    let g0 = self.given@;
                    let run = Seq::new(1, |_i: int| x.val());
                    let f = |o: E::V| <B as DeltaWrite<'static, E, M>>::dvo(o);
                    assert((g0 + run).map_values(f) =~= g0.map_values(f).push(d));
                    self.given = Ghost(g0 + run);
                }
                Some((x, 1))
            },
            None => {
                proof {
                    self.ended = Ghost(true);
                }
                None
            },
        }
    }
}

/// A delta column's values as written: the trusted bridge to
/// `DeltaInner::from_opt` (user code), which writes a delta, and to the
/// edit model's owned values, whose deltas those read back are.
pub(crate) trait DeltaWrite<'a, E: ReadsAgree<'a, X>, X: Decoders<'a, Tail = <E as Slabs>::Tail>>: DeltaOps<
    <X::D as Decode>::Item,
> {
    type Ins: Insertable<E>;

    /// The delta an owned value holds.
    spec fn dvo(o: E::V) -> Option<i64>;

    /// A value read holds its owned form's delta.
    proof fn axiom_own(v: <X::D as Decode>::Item)
        ensures
            Self::dv(v) == Self::dvo(E::own(v)),
    ;

    /// The value that holds delta `d`.
    fn ins(d: Option<i64>) -> (x: Self::Ins)
        ensures
            Self::dvo(x.val()) == d,
    ;
}

/// A delta splice's runs, as the cursor takes them ([`RunFeed`]): the
/// items given, through the splice's plan, which re-anchors them and then
/// the item after the deleted ones (`dbound::SpanPlan`), each delta
/// written as `B` writes it.
pub(crate) struct DeltaFeed<S, E: Slabs, M, B> {
    pub(crate) items: S,
    pub(crate) plan: SpanPlan,
    /// whether the plan keeps a bound on the values (always, for the
    /// column's domain; the field keeps the call sites' constant)
    pub(crate) track: bool,
    /// the values written so far
    #[allow(dead_code)]
    pub(crate) given: Ghost<Seq<E::V>>,
    pub(crate) _p: PhantomData<(M, B)>,
}

impl<S: SpliceItems, E, M, B> RunFeed<E> for DeltaFeed<S, E, M, B> where
    E: Slabs + for<'a> ReadsAgree<'a, M>,
    M: for<'a> Decoders<'a, Tail = E::Tail>,
    B: for<'a> DeltaWrite<'a, E, M>,
 {
    type X = <B as DeltaWrite<'static, E, M>>::Ins;

    type K = (Ghost<int>, usize, Option<i64>, Ghost<Seq<Option<i64>>>, Ghost<bool>);

    /// The plan's, and the values written hold the deltas it emitted.
    open spec fn inv(&self) -> bool {
        &&& self.plan.inv()
        &&& self.plan.tracking@ == self.track
        &&& self.given@.map_values(|o: E::V| <B as DeltaWrite<'static, E, M>>::dvo(o)) == self.plan.p.emitted@
    }

    /// The splice: around it, and the column's original deltas.
    open spec fn key(&self) -> Self::K {
        (self.plan.p.prev, self.plan.p.skip, self.plan.p.next_val, self.plan.orig, self.plan.tracking)
    }

    open spec fn given(&self) -> Seq<E::V> {
        self.given@
    }

    open spec fn ended(&self) -> bool {
        self.plan.p.phase is Done
    }

    #[verifier::exec_allows_no_decreases_clause]
    #[inline(always)]
    fn next_run(&mut self) -> (r: Option<(Self::X, usize)>) {
        let ghost e0 = self.plan.p.emitted@;
        let ghost plan0 = self.plan;
        loop
            invariant
                self.inv(),
                self.plan.fixed(plan0),
                self.plan.p.emitted@ == e0,
                self.given == old(self).given,
                e0 == old(self).plan.p.emitted@,
                plan0 == old(self).plan,
                self.track == old(self).track,
        {
            let (d, n) = match self.plan.p.phase {
                SplicePhase::Values => match self.plan.take_pending() {
                    Some(o) => o,
                    None => {
                        proof {
                            assert(e0 + Seq::<Option<i64>>::empty() =~= e0);
                        }
                        match self.items.next_item() {
                            Some(SpliceItem::Value(v)) => self.plan.value(v, self.track),
                            // an empty run inserts nothing
                            Some(SpliceItem::Run(prefix, delta, count)) => {
                                if count == 0 {
                                    continue;
                                }
                                self.plan.run(prefix, delta, count, self.track)
                            },
                            None => {
                                self.plan.end_values();
                                continue;
                            },
                        }
                    },
                },
                SplicePhase::NullRun => match self.plan.nulls() {
                    Some(o) => o,
                    None => {
                        proof {
                            assert(e0 + Seq::<Option<i64>>::empty() =~= e0);
                        }
                        continue;
                    },
                },
                SplicePhase::Boundary => self.plan.boundary(),
                SplicePhase::Done => return None,
            };
            let x = <B as DeltaWrite<'static, E, M>>::ins(d);
            proof {
                let g0 = self.given@;
                let run = Seq::new(n as nat, |_i: int| x.val());
                let f = |o: E::V| <B as DeltaWrite<'static, E, M>>::dvo(o);
                assert((g0 + run).map_values(f) =~= g0.map_values(f) + reps(n as nat, d));
                self.given = Ghost(g0 + run);
            }
            return Some((x, n));
        }
    }
}

} // verus!
