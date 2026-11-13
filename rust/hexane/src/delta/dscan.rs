//! A delta column's value scan, verified with Verus.
//!
//! [`DeltaIter::scan_to_range`](crate::DeltaIter::scan_to_range) finds the
//! iterator's next item whose value lies in a range, with the values in
//! any order. It reads the rest of the slab the iterator is in a run at a
//! time (the items of a run of equal deltas that lie in the range are
//! found by division, `run_hits`), then jumps to the next slab the index
//! says may hold a value in the range (the B-tree's value search),
//! skipping the slabs between.
//!
//! Proved: the result is the first item of the iterator's window, from its
//! position, whose value lies in the range, with the iterator just past
//! it; or there is none, and the iterator has used up its window. Trusted
//! as for reading (`diter`): the value type's `to_opt`, and that the index
//! holds each slab's aggregate.

// Verus proofs read more plainly with `x = x + n`; ghost arguments count
// towards clippy's limit.
#![allow(clippy::assign_op_pattern, clippy::too_many_arguments)]

#[cfg(verus_keep_ghost)]
use crate::btree::in_i64;
use crate::btree::{SlabAgg, SlabBTree, ValueSearch};
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::slab_lens;
use crate::delta::agg::DeltaOps;
#[cfg(verus_keep_ghost)]
use crate::delta::agg::{deltas_of, dval, psum};
use crate::delta::dfind::run_hits;
#[cfg(verus_keep_ghost)]
use crate::delta::dfind::{col_hit, kth, lemma_skipped};
use crate::delta::diter::d_nth;
#[cfg(verus_keep_ghost)]
use crate::delta::diter::{
    at_run, dbefore, dindex_ok, lemma_crop, lemma_found, lemma_psum_cat, realize, realized_at,
};
use crate::delta::dseek::d_step;
#[cfg(verus_keep_ghost)]
use crate::delta::dseek::{
    all_fit, lemma_out_skip, lemma_run_rv, lemma_sums_fit, lemma_vals_len, lemma_window, probe_at,
    rsum,
};
#[cfg(verus_keep_ghost)]
use crate::fenwick::{lemma_sum_le, lemma_sum_one, lemma_sum_split, sum, total};
#[cfg(verus_keep_ghost)]
use crate::iter_core::vals_of;
use crate::iter_core::{Decode, Decoders, ICore};
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// None of the column's items `a..b` has a value in `[lo, hi]`.
pub(crate) open spec fn clear<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    s: Seq<Slab<X::Tail>>,
    lo: int,
    hi: int,
    a: int,
    b: int,
) -> bool {
    forall|j: int| a <= j < b ==> !#[trigger] col_hit::<X, B>(s, lo, hi, j)
}

/// Moving forward, the iterator's slab never goes back.
proof fn lemma_slab_mono<'a, D: Decode, X: Decoders<'a, D = D>>(c0: ICore<D>, c1: ICore<D>, s: Seq<Slab<X::Tail>>)
    requires
        c0.wf::<X>(s),
        c1.wf::<X>(s),
        c0.pos < c1.pos,
    ensures
        c0.slab_idx <= c1.slab_idx,
{
    if c1.slab_idx < c0.slab_idx {
        let lens = slab_lens(s);
        lemma_sum_split(lens, 0, c1.slab_idx as int, c1.slab_idx + 1);
        lemma_sum_one(lens, c1.slab_idx as int);
        lemma_sum_le(lens, 0, c1.slab_idx + 1, 0, c0.slab_idx as int);
        if c0.slab_idx == s.len() {
            assert(lens.len() == s.len());
        }
    }
}

/// At the end of a slab, the iterator stands at the next slab's first
/// item.
proof fn lemma_slab_end<'a, D: Decode, X: Decoders<'a, D = D>>(c: ICore<D>, s: Seq<Slab<X::Tail>>)
    requires
        c.wf::<X>(s),
        c.slab_remaining == 0,
        c.slab_idx < s.len(),
    ensures
        c.pos == sum(slab_lens(s), 0, c.slab_idx + 1),
{
    let lens = slab_lens(s);
    lemma_sum_split(lens, 0, c.slab_idx as int, c.slab_idx + 1);
    lemma_sum_one(lens, c.slab_idx as int);
}

/// The running sum after the first `b` deltas is that after `a` and those
/// between.
proof fn lemma_rsum_split<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    s: Seq<Slab<X::Tail>>,
    a: int,
    b: int,
)
    requires
        0 <= a <= b <= vals_of::<X>(s).len(),
    ensures
        rsum::<X, B>(s, b) == rsum::<X, B>(s, a) + psum(deltas_of::<_, B>(vals_of::<X>(s).subrange(a, b))),
{
    let vv = vals_of::<X>(s);
    assert(vv.take(b) =~= vv.take(a) + vv.subrange(a, b));
    assert(deltas_of::<_, B>(vv.take(b)) =~= deltas_of::<_, B>(vv.take(a)) + deltas_of::<_, B>(vv.subrange(a, b)));
    lemma_psum_cat(deltas_of::<_, B>(vv.take(a)), deltas_of::<_, B>(vv.subrange(a, b)));
}

/// Scan the rest of the slab the iterator is in (and any its runs lead
/// into), up to `end`: the first item with a value in `[lo, hi]`, or none,
/// and where the scan stopped: the window's end, or a slab's.
fn d_scan_slab<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    running: &mut i64,
    lo: i64,
    hi: i64,
    end: usize,
    Ghost(o): Ghost<Seq<<X::D as Decode>::Item>>,
    Ghost(p): Ghost<int>,
) -> (r: (Option<usize>, usize))
    requires
        probe_at::<X, B>(*old(core), *old(running), slabs@, o, p, old(core).pos as int),
        p <= old(core).pos <= p + o.len() <= end,
        all_fit::<X, B>(slabs@),
    ensures
        match r.0 {
            Some(h) => {
                &&& old(core).pos <= h < p + o.len()
                &&& col_hit::<X, B>(slabs@, lo as int, hi as int, h as int)
                &&& clear::<X, B>(slabs@, lo as int, hi as int, old(core).pos as int, h as int)
            },
            None => {
                &&& old(core).pos <= r.1 <= p + o.len()
                &&& clear::<X, B>(slabs@, lo as int, hi as int, old(core).pos as int, r.1 as int)
                &&& r.1 == p + o.len() || {
                    &&& old(core).slab_idx <= final(core).slab_idx < slabs@.len()
                    &&& r.1 == sum(slab_lens(slabs@), 0, final(core).slab_idx + 1)
                }
            },
        },
{
    let ghost s = slabs@;
    let ghost e = p + o.len();
    let ghost q0 = core.pos as int;
    let ghost si0 = core.slab_idx;
    let mut pos = core.pos;
    let mut hit: Option<usize> = None;
    while core.slab_remaining > 0 && pos < end
        invariant_except_break
            hit is None,
            probe_at::<X, B>(*core, *running, s, o, p, pos as int),
            si0 <= core.slab_idx,
        invariant
            s == slabs@,
            e == p + o.len(),
            e <= end,
            p <= q0 <= pos <= e,
            all_fit::<X, B>(s),
            hit is None ==> clear::<X, B>(s, lo as int, hi as int, q0, pos as int),
        ensures
            match hit {
                Some(h) => {
                    &&& q0 <= h < e
                    &&& col_hit::<X, B>(s, lo as int, hi as int, h as int)
                    &&& clear::<X, B>(s, lo as int, hi as int, q0, h as int)
                },
                None => {
                    &&& q0 <= pos <= e
                    &&& clear::<X, B>(s, lo as int, hi as int, q0, pos as int)
                    &&& pos == e || {
                        &&& core.slab_remaining == 0
                        &&& probe_at::<X, B>(*core, *running, s, o, p, pos as int)
                        &&& si0 <= core.slab_idx
                    }
                },
            },
        decreases e - pos,
    {
        let a = *running;
        let ghost c1 = *core;
        let Some(run) = d_step::<X, B>(core, slabs, running, Ghost(o), Ghost(p), Ghost(pos as int)) else {
            break;
        };
        let count = run.count;
        let ghost c = count as int;
        proof {
            lemma_slab_mono::<X::D, X>(c1, *core, s);
        }
        match B::to_opt(run.value) {
            None => {
                proof {
                    assert forall|j: int| q0 <= j < pos + c implies !#[trigger] col_hit::<X, B>(s, lo as int, hi as int, j) by {
                        if j >= pos {
                            lemma_run_rv::<X, B>(s, pos as int, c, run.value, j - pos + 1);
                        }
                    }
                }
                pos = pos + count;
            },
            Some(d) => {
                // the first of the run's items in range, counting from 1
                // (0: none)
                let j: usize = if d == 0 {
                    if lo <= a && a <= hi {
                        1
                    } else {
                        0
                    }
                } else {
                    proof {
                        lemma_run_rv::<X, B>(s, pos as int, c, run.value, 1);
                        lemma_run_rv::<X, B>(s, pos as int, c, run.value, c);
                    }
                    let (ks, ke) = run_hits(a, d, count, lo, hi);
                    if ks <= ke {
                        ks
                    } else {
                        0
                    }
                };
                proof {
                    // the run's items before the `j`th are out of range,
                    // and the `j`th is in
                    assert forall|kk: int| 1 <= kk <= c && (j == 0 || kk < j) implies !#[trigger] col_hit::<X, B>(
                        s,
                        lo as int,
                        hi as int,
                        pos + kk - 1,
                    ) by {
                        lemma_run_rv::<X, B>(s, pos as int, c, run.value, kk);
                        assert(kth(a as int, d as int, kk) == a + kk * d);
                    }
                    if j > 0 {
                        lemma_run_rv::<X, B>(s, pos as int, c, run.value, j as int);
                        assert(kth(a as int, d as int, j as int) == a + j * d);
                    }
                    assert forall|i: int| q0 <= i < pos + (if j > 0 { j - 1 } else { c }) implies !#[trigger] col_hit::<X, B>(
                        s,
                        lo as int,
                        hi as int,
                        i,
                    ) by {
                        if i >= pos {
                            let kk = i - pos + 1;
                            assert(!col_hit::<X, B>(s, lo as int, hi as int, pos + kk - 1));
                        }
                    }
                }
                if j > 0 {
                    hit = Some(pos + j - 1);
                    break;
                }
                pos = pos + count;
            },
        }
    }
    proof {
        if hit is None && pos < e {
            lemma_vals_len::<X>(s);
            if core.slab_idx >= s.len() {
                assert(core.out::<X>(s).len() == 0);
                assert(o.skip(pos - p).len() == e - pos);
            } else {
                lemma_slab_end::<X::D, X>(*core, s);
            }
        }
    }
    (hit, pos)
}

/// Use up the window: the iterator stands at its end.
fn d_park<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&SlabBTree<SlabAgg>>,
    running: &mut i64,
    end: usize,
)
    requires
        at_run::<X, B>(*old(core), *old(running), slabs@),
        dindex_ok::<X, B>(ix, slabs@),
        end == old(core).pos + old(core).items_left,
    ensures
        at_run::<X, B>(*final(core), *final(running), slabs@),
        final(core).out::<X>(slabs@).len() == 0,
{
    proof {
        lemma_window::<X::D, X>(*core, slabs@);
    }
    let n = end - core.pos;
    if n > 0 {
        d_nth::<X, B>(core, slabs, ix, n - 1, running);
    }
}

/// The value item `h` realizes, read from `n` items before it: its
/// running sum.
proof fn lemma_value_at<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    s: Seq<Slab<X::Tail>>,
    w: Seq<<X::D as Decode>::Item>,
    q: int,
    h: int,
)
    requires
        0 <= q <= h < vals_of::<X>(s).len(),
        w.len() >= h - q + 1,
        forall|k: int| 0 <= k < w.len() ==> #[trigger] w[k] == vals_of::<X>(s)[q + k],
        B::dv(vals_of::<X>(s)[h]) is Some,
    ensures
        realized_at::<_, B>(w, rsum::<X, B>(s, q), h - q) == Some(
            crate::delta::diter::crop(rsum::<X, B>(s, h + 1)),
        ),
{
    let vv = vals_of::<X>(s);
    assert(w.take(h - q) =~= vv.subrange(q, h));
    lemma_rsum_split::<X, B>(s, q, h);
    lemma_rsum_split::<X, B>(s, h, h + 1);
    assert(vv.subrange(h, h + 1) =~= seq![vv[h]]);
    assert(deltas_of::<_, B>(vv.subrange(h, h + 1)) =~= seq![B::dv(vv[h])]);
    assert(seq![B::dv(vv[h])].drop_last() =~= Seq::<Option<i64>>::empty());
    assert(psum(Seq::<Option<i64>>::empty()) == 0);
    assert(psum(seq![B::dv(vv[h])]) == dval(B::dv(vv[h])));
    assert(w[h - q] == vv[h]);
    assert(deltas_of::<_, B>(w.take(h - q)) == deltas_of::<_, B>(vv.subrange(q, h)));
}

/// `scan_to_range` for `[lo, hi]`: the iterator's first item, in its
/// window, whose value lies in `[lo, hi]`, and that value, with the
/// iterator just past it; or none, with the window used up.
#[verifier::spinoff_prover]
pub(crate) fn d_scan<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&'a SlabBTree<SlabAgg>>,
    running: &mut i64,
    lo: i64,
    hi: i64,
) -> (r: Option<(usize, i64)>)
    requires
        at_run::<X, B>(*old(core), *old(running), slabs@),
        dindex_ok::<X, B>(ix, slabs@),
    ensures
        at_run::<X, B>(*final(core), *final(running), slabs@),
        ({
            let p = old(core).pos as int;
            let o = old(core).out::<X>(slabs@);
            let e = p + o.len();
            match r {
                Some((h, v)) => {
                    &&& p <= h < e
                    &&& col_hit::<X, B>(slabs@, lo as int, hi as int, h as int)
                    &&& v == rsum::<X, B>(slabs@, h + 1)
                    &&& clear::<X, B>(slabs@, lo as int, hi as int, p, h as int)
                    &&& final(core).pos == h + 1
                    &&& final(core).out::<X>(slabs@) == o.skip(h + 1 - p)
                },
                None => {
                    &&& clear::<X, B>(slabs@, lo as int, hi as int, p, e)
                    &&& final(core).out::<X>(slabs@).len() == 0
                },
            }
        }),
{
    let ghost s = slabs@;
    let ghost p = core.pos as int;
    let ghost o = core.out::<X>(s);
    let ghost e = p + o.len();
    let ghost vv = vals_of::<X>(s);
    let ghost lens = slab_lens(s);
    proof {
        lemma_window::<X::D, X>(*core, s);
        lemma_vals_len::<X>(s);
        assert forall|n: int| 0 <= n <= vv.len() implies in_i64(#[trigger] rsum::<X, B>(s, n)) by {
            match ix {
                Some(ix) => lemma_sums_fit::<X, B>(ix@, s, n),
                None => {
                    assert(vv.take(n) =~= Seq::<<X::D as Decode>::Item>::empty());
                    assert(deltas_of::<_, B>(vv.take(n)) =~= Seq::<Option<i64>>::empty());
                },
            }
        }
        assert(o.skip(0) =~= o);
    }
    let end = core.pos + core.items_left;
    let Some(tree) = ix else {
        // no index, no slabs: nothing to find
        proof {
            assert(vv.len() == 0);
        }
        d_park::<X, B>(core, slabs, ix, running, end);
        return None;
    };
    let ghost ws = tree@;
    proof {
        lemma_found::<X, B>(ws, s, 0);
    }
    let mut search = ValueSearch::new(tree, lo, hi);
    proof {
        assert(ws.len() == s.len());
    }
    loop
        invariant
            s == slabs@,
            p == old(core).pos,
            o == old(core).out::<X>(slabs@),
            vv == vals_of::<X>(s),
            lens == slab_lens(s),
            e == p + o.len(),
            e <= end,
            ix == Some(tree),
            dindex_ok::<X, B>(ix, s),
            ws == tree@,
            all_fit::<X, B>(s),
            probe_at::<X, B>(*core, *running, s, o, p, core.pos as int),
            core.pos + core.items_left == end,
            p <= core.pos <= e,
            clear::<X, B>(s, lo as int, hi as int, p, core.pos as int),
            search.inv(),
            search.view_tree() == ws,
            search.spec_lo() == lo,
            search.spec_hi() == hi,
            0 <= search.pos() <= ws.len(),
            search.pos() <= core.slab_idx + 1,
            ws.len() == s.len(),
            o.len() > 0 ==> e <= vv.len(),
            e < end ==> e >= vv.len(),
        decreases ws.len() - search.pos(),
    {
        let mark = core.dup();
        let mark_running = *running;
        let ghost q0 = core.pos as int;
        let ghost sp0 = search.pos();
        let (hit, scanned) = d_scan_slab::<X, B>(core, slabs, running, lo, hi, end, Ghost(o), Ghost(p));
        let ghost k = core.slab_idx as int;
        // back to the slab's start: to step onto a hit, or to move on
        *core = mark;
        *running = mark_running;
        proof {
            lemma_window::<X::D, X>(*core, s);
        }
        if let Some(h) = hit {
            proof {
                assert(dbefore::<X, B>(s, q0) == deltas_of::<_, B>(vv.take(q0)));
                lemma_crop(rsum::<X, B>(s, q0));
                lemma_value_at::<X, B>(s, core.out::<X>(s), q0, h as int);
                lemma_crop(rsum::<X, B>(s, h + 1));
                assert(o.skip(q0 - p).skip(h - q0 + 1) =~= o.skip(h + 1 - p));
            }
            let n = h - core.pos;
            let ghost c2 = *core;
            let v = d_nth::<X, B>(core, slabs, ix, n, running);
            proof {
                assert(n < c2.out::<X>(s).len());
                assert(core.pos == h + 1);
                assert(core.out::<X>(s) == o.skip(h + 1 - p));
                assert(v == Some(Some(crate::delta::diter::crop(rsum::<X, B>(s, h + 1)))));
                assert(in_i64(rsum::<X, B>(s, h + 1)));
                assert forall|j: int| p <= j < h implies !#[trigger] col_hit::<X, B>(s, lo as int, hi as int, j) by {
                    if j >= q0 {
                    }
                }
            }
            match v {
                Some(Some(v)) => {
                    proof {
                        assert(p <= h < e);
                        assert(col_hit::<X, B>(s, lo as int, hi as int, h as int));
                        assert(v == rsum::<X, B>(s, h + 1));
                        assert(clear::<X, B>(s, lo as int, hi as int, p, h as int));
                        assert(at_run::<X, B>(*core, *running, s));
                        assert(core.pos == h + 1);
                        assert(core.out::<X>(s) == o.skip(h + 1 - p));
                    }
                    return Some((h, v));
                },
                _ => {
                    proof {
                        assert(false);
                    }
                    return None;
                },
            }
        }
        let ghost q = scanned as int;
        proof {
            assert(ws.len() == s.len());
            if q < e {
                assert(0 <= k < s.len());
                assert(mark.slab_idx <= k);
                assert(search.pos() <= mark.slab_idx + 1);
                lemma_sum_le(lens, 0, search.pos(), 0, k + 1);
            }
        }
        // the next slab the index says may hold a value in range
        loop
            invariant_except_break
                core.pos == q0,
                q == e || {
                    &&& 0 <= k < s.len()
                    &&& q == sum(lens, 0, k + 1)
                    &&& search.pos() <= k + 1
                },
            invariant
                s == slabs@,
                p == old(core).pos,
                o == old(core).out::<X>(slabs@),
                q == scanned,
                vv == vals_of::<X>(s),
                lens == slab_lens(s),
                e == p + o.len(),
                e <= end,
                ix == Some(tree),
                dindex_ok::<X, B>(ix, s),
                ws == tree@,
                all_fit::<X, B>(s),
                probe_at::<X, B>(*core, *running, s, o, p, core.pos as int),
                core.pos + core.items_left == end,
                p <= core.pos <= e,
                q0 <= q <= e,
                clear::<X, B>(s, lo as int, hi as int, p, q),
                clear::<X, B>(s, lo as int, hi as int, p, core.pos as int),
                search.inv(),
                search.view_tree() == ws,
                search.spec_lo() == lo,
                search.spec_hi() == hi,
                0 <= search.pos() <= ws.len(),
                ws.len() == s.len(),
                o.len() > 0 ==> e <= vv.len(),
                e < end ==> e >= vv.len(),
                sp0 <= search.pos(),
            ensures
                sp0 < search.pos(),
                search.pos() <= core.slab_idx + 1,
            decreases ws.len() - search.pos(),
        {
            let ghost pos0 = search.pos();
            proof {
                if q < e {
                    lemma_sum_le(lens, 0, pos0, 0, k + 1);
                }
            }
            let Some((si, items_before, prefix)) = search.next_hit() else {
                proof {
                    if q < e {
                        lemma_skipped::<X, B>(ws, s, lo as int, hi as int, pos0, s.len() as int, sum(lens, 0, pos0) as int);
                        assert(lens.len() == s.len());
                        lemma_vals_len::<X>(s);
                        assert(sum(lens, 0, s.len() as int) == total(lens));
                        assert(e <= vv.len());
                        assert forall|j: int| p <= j < e implies !#[trigger] col_hit::<X, B>(s, lo as int, hi as int, j) by {
                            if j >= q {
                                assert(sum(lens, 0, pos0) <= j < sum(lens, 0, s.len() as int));
                            }
                        }
                    }
                }
                d_park::<X, B>(core, slabs, ix, running, end);
                return None;
            };
            proof {
                lemma_found::<X, B>(ws, s, si as int);
                lemma_sum_split(lens, 0, si as int, si + 1);
                lemma_sum_one(lens, si as int);
                lemma_sum_le(lens, 0, si + 1, 0, s.len() as int);
                assert(lens.len() == s.len());
                assert(ws[si as int].len == s[si as int].len);
            }
            if items_before >= end {
                proof {
                    if q < e {
                        lemma_skipped::<X, B>(ws, s, lo as int, hi as int, pos0, si as int, sum(lens, 0, pos0) as int);
                        assert forall|j: int| p <= j < e implies !#[trigger] col_hit::<X, B>(s, lo as int, hi as int, j) by {
                            if j >= q {
                                assert(sum(lens, 0, pos0) <= j < sum(lens, 0, si as int));
                            }
                        }
                    }
                }
                d_park::<X, B>(core, slabs, ix, running, end);
                return None;
            }
            if items_before < scanned {
                // a slab the scan has passed
                proof {
                    assert(items_before == sum(lens, 0, si as int));
                    assert(search.pos() == si + 1);
                    if q < e && si > k {
                        lemma_sum_le(lens, 0, k + 1, 0, si as int);
                        assert(false);
                    }
                }
                continue;
            }
            *running = prefix;
            let ghost c1 = *core;
            proof {
                lemma_skipped::<X, B>(ws, s, lo as int, hi as int, pos0, si as int, sum(lens, 0, pos0) as int);
                lemma_crop(prefix as int);
            }
            proof {
                assert(items_before == sum(lens, 0, si as int));
                assert(core.pos == q0);
                assert(q0 <= items_before);
                assert(items_before - q0 <= core.items_left);
                lemma_vals_len::<X>(s);
                assert(items_before <= e);
            }
            if !core.advance_to_slab::<X>(slabs, si, items_before) {
                proof {
                    assert(false);
                }
                d_park::<X, B>(core, slabs, ix, running, end);
                return None;
            }
            proof {
                lemma_out_skip::<X::D, X>(c1, *core, s, items_before - q0);
                assert(o.skip(q0 - p).skip(items_before - q0) =~= o.skip(items_before - p));
            }
            break;
        }
    }
}


/// `scan_to_value` for null: the iterator's first null item, in its
/// window, with the iterator just past it; or none, with the window used
/// up.
pub(crate) fn d_scan_null<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&SlabBTree<SlabAgg>>,
    running: &mut i64,
) -> (r: Option<usize>)
    requires
        at_run::<X, B>(*old(core), *old(running), slabs@),
        dindex_ok::<X, B>(ix, slabs@),
    ensures
        at_run::<X, B>(*final(core), *final(running), slabs@),
        ({
            let p = old(core).pos as int;
            let o = old(core).out::<X>(slabs@);
            let e = p + o.len();
            let vv = vals_of::<X>(slabs@);
            match r {
                Some(h) => {
                    &&& p <= h < e
                    &&& B::dv(vv[h as int]) is None
                    &&& forall|j: int| p <= j < h ==> B::dv(#[trigger] vv[j]) is Some
                    &&& final(core).pos == h + 1
                    &&& final(core).out::<X>(slabs@) == o.skip(h + 1 - p)
                },
                None => {
                    &&& forall|j: int| p <= j < e ==> B::dv(#[trigger] vv[j]) is Some
                    &&& final(core).out::<X>(slabs@).len() == 0
                },
            }
        }),
{
    let ghost s = slabs@;
    let ghost p = core.pos as int;
    let ghost o = core.out::<X>(s);
    let ghost e = p + o.len();
    let ghost vv = vals_of::<X>(s);
    proof {
        lemma_window::<X::D, X>(*core, s);
        lemma_vals_len::<X>(s);
        assert forall|n: int| 0 <= n <= vv.len() implies in_i64(#[trigger] rsum::<X, B>(s, n)) by {
            match ix {
                Some(ix) => lemma_sums_fit::<X, B>(ix@, s, n),
                None => {
                    assert(vv.take(n) =~= Seq::<<X::D as Decode>::Item>::empty());
                    assert(deltas_of::<_, B>(vv.take(n)) =~= Seq::<Option<i64>>::empty());
                },
            }
        }
        assert(o.skip(0) =~= o);
    }
    let end = core.pos + core.items_left;
    let mark = core.dup();
    let mark_running = *running;
    let mut pos = core.pos;
    let mut hit: Option<usize> = None;
    while pos < end
        invariant_except_break
            hit is None,
            probe_at::<X, B>(*core, *running, s, o, p, pos as int),
        invariant
            s == slabs@,
            vv == vals_of::<X>(s),
            e == p + o.len(),
            e <= end,
            p <= pos <= e,
            all_fit::<X, B>(s),
            forall|j: int| p <= j < pos ==> B::dv(#[trigger] vv[j]) is Some,
        ensures
            match hit {
                Some(h) => p <= h < e && B::dv(vv[h as int]) is None && forall|j: int| p <= j < h ==> B::dv(
                    #[trigger] vv[j],
                ) is Some,
                None => pos == e,
            },
        decreases e - pos,
    {
        let Some(run) = d_step::<X, B>(core, slabs, running, Ghost(o), Ghost(p), Ghost(pos as int)) else {
            break;
        };
        let ghost c = run.count as int;
        proof {
            lemma_run_rv::<X, B>(s, pos as int, c, run.value, 1);
        }
        if B::to_opt(run.value).is_none() {
            hit = Some(pos);
            break;
        }
        proof {
            assert forall|j: int| p <= j < pos + c implies B::dv(#[trigger] vv[j]) is Some by {
                if j >= pos {
                    lemma_run_rv::<X, B>(s, pos as int, c, run.value, j - pos + 1);
                }
            }
        }
        pos = pos + run.count;
    }
    // back to the start: step onto the hit, or use up the window
    *core = mark;
    *running = mark_running;
    match hit {
        Some(h) => {
            proof {
                lemma_window::<X::D, X>(*core, s);
                assert(o.skip(h + 1 - p) =~= o.skip(0).skip(h - p + 1));
            }
            d_nth::<X, B>(core, slabs, ix, h - core.pos, running);
            Some(h)
        },
        None => {
            d_park::<X, B>(core, slabs, ix, running, end);
            None
        },
    }
}

} // verus!
