//! Merging one slab onto the end of another, verified with Verus.
//!
//! The first slab's last group (found by its tail record) and the second
//! slab's first run meet in a writer, which flushes them as one; the
//! rest of the second slab's bytes follow unchanged. [`merge_plan`] does
//! that while both slabs are borrowed, [`merge_apply`] then writes the
//! result into the first slab's bytes. Together: the merged bytes are the
//! canonical encoding of the first slab's values then the second's, with
//! the segment count and tail record that encoding has.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero)]

// proof functions exist only when Verus compiles the crate
#[cfg(verus_keep_ghost)]
use super::dec::*;
use super::dec::{head_postfix, tail_state};
use super::lift::*;
use super::model::*;
use super::postfix::*;
#[cfg(verus_keep_ghost)]
use super::read::{lemma_segments_le, lit_tail_of, tail_bytes_of};
use super::state::*;
use super::writer::*;
use super::RleTail;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// How merging changes the first slab's bytes.
pub(crate) struct MergePlan {
    /// Where the first slab's last group starts: its bytes from here on
    /// are replaced.
    pub tail_pos: usize,
    /// The flush of that group then the second slab's first run.
    pub f: FlushState,
    /// Where the second slab's bytes after its first run start.
    pub b_bytes: usize,
    /// The merged slab's segments.
    pub segments: usize,
    /// The first slab's last literal header, if the flush rewrites it.
    #[allow(dead_code)] // for the spec
    pub h0: Ghost<i64>,
}

/// The merged bytes: the first slab's before its last group (its header
/// rewritten), the flushed `buf`, then the second slab's after its first
/// run.
pub(crate) open spec fn merge_bytes<'a, W: WriteOps<'a>>(p: MergePlan, a: Seq<u8>, buf: Seq<u8>, b: Seq<u8>) -> Seq<u8> {
    apply::<W>(p.f.rewrite, a.take(p.tail_pos as int), p.h0@) + buf + b.skip(p.b_bytes as int)
}

/// The merged slab's tail record, from the second slab's (`tb`, `tl`).
pub(crate) open spec fn merge_tail_of<'a, W: WriteOps<'a>>(p: MergePlan, a: Seq<u8>, buf: Seq<u8>, b: Seq<u8>, tb: int, tl: int) -> (int, int) {
    tail_after(p.f.wpos, apply::<W>(p.f.rewrite, a.take(p.tail_pos as int), p.h0@).len() as int, 0,
        buf.len() as int, b.len() - p.b_bytes, tb, tl)
}

/// The slab `data` with its metadata as recorded: `segs` segments and
/// tail record `t`.
pub(crate) open spec fn slab_meta<'a, O: RleOps<'a>>(data: Seq<u8>, segs: int, t: RleTail) -> bool {
    &&& valid_slab::<O>(data)
    &&& segs == groups_segments(slab_groups::<O>(data))
    &&& tail_b(t) == tail_bytes_of::<O>(slab_groups::<O>(data))
    &&& tail_l(t) == lit_tail_of::<O>(slab_groups::<O>(data))
}

/// The pending group's values are among those written.
proof fn lemma_plen_le<'a, W: WriteOps<'a>>(s: State<'a, W>, pre: Seq<u8>, buf: Seq<u8>, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64)
    requires rel(s, pre, buf, 0, gs, ws, h0),
    ensures s.plen() <= values_of(s, gs, ws).len(),
{
    if !(s is Empty) {
        let g = pend_group(s, ws)->Some_0;
        lemma_push::<W::R>(gs, g);
        lemma_group_values_len(g, <W::R as RleOps<'a>>::null());
    }
}

/// An encoding has no more segments than values.
proof fn lemma_encodes_segs<'a, O: RleOps<'a>>(x: Seq<u8>, vals: Seq<O::V>, segs: int)
    requires encodes::<O>(x, vals, segs),
    ensures 0 <= segs <= vals.len(),
{
    let gs = choose|gs: Seq<Group<O::V>>| #[trigger] canonical::<O>(gs) && groups_bytes::<O>(gs) == x
        && groups_values(gs, O::null()) == vals && groups_segments(gs) == segs;
    lemma_segments_le::<O>(gs);
    lemma_groups_values_concat(gs, Seq::empty(), O::null());
}

/// Owned copies of two sequences of values.
proof fn lemma_lift_concat<'s, L: Lift<'s>>(x: Seq<VOf<'s, L>>, y: Seq<VOf<'s, L>>)
    ensures lift_seq::<L>(x + y) == lift_seq::<L>(x) + lift_seq::<L>(y),
{
    assert(lift_seq::<L>(x + y) =~= lift_seq::<L>(x) + lift_seq::<L>(y));
}

/// Plan merging slab `b` onto the end of slab `a` (both read with
/// borrowed values, specified with owned ones): flush `a`'s last group
/// and `b`'s first run into `buf`.
#[verifier::spinoff_prover]
#[verifier::rlimit(60)]
#[allow(unused_variables)] // b_tail is for the spec
pub(crate) fn merge_plan<'s, L: Lift<'s>>(a: &'s [u8], a_segs: usize, a_tail: &RleTail, b: &'s [u8], b_segs: usize, b_tail: &RleTail, buf: &mut Vec<u8>) -> (p: MergePlan)
    requires
        slab_meta::<WR<'s, L>>(a@, a_segs as int, *a_tail), a@.len() < i64::MAX,
        slab_meta::<WR<'s, L>>(b@, b_segs as int, *b_tail),
        slab_values::<WR<'s, L>>(a@).len() + slab_values::<WR<'s, L>>(b@).len() + 2 < usize::MAX,
    ensures
        p.tail_pos <= a@.len(), p.b_bytes <= b@.len(),
        rw_at::<L::W>(p.f.rewrite, a@.take(p.tail_pos as int), p.h0@),
        encodes::<WR<'s, L>>(merge_bytes::<L::W>(p, a@, final(buf)@, b@),
            slab_values::<WR<'s, L>>(a@) + slab_values::<WR<'s, L>>(b@), p.segments as int),
        ({
            let t = merge_tail_of::<L::W>(p, a@, final(buf)@, b@, tail_b(*b_tail) as int, tail_l(*b_tail) as int);
            tail_ok::<WR<'s, L>>(merge_bytes::<L::W>(p, a@, final(buf)@, b@), t.0, t.1) && 0 <= t.1 <= t.0
        }),
{
    proof {
        lemma_unlift_meta::<L>(a@);
        lemma_unlift_meta::<L>(b@);
        lemma_slab::<L::R>(a@);
        lemma_slab::<L::R>(b@);
    }
    let ghost va = slab_values::<L::R>(a@);
    let ghost vb = slab_values::<L::R>(b@);
    let (mut s, tail_pos, delta) = tail_state::<L>(a, tail_run_len(a_tail), tail_lit_len(a_tail));
    let ghost s0 = s;
    let ghost pre = a@.take(tail_pos as int);
    let ghost (gs, ws, h0) = choose|gs: Seq<Group<VOf<'s, L>>>, ws: Seq<VOf<'s, L>>, h0: i64|
        #[trigger] rel(s0, pre, Seq::empty(), 0, gs, ws, h0)
        && values_of(s0, gs, ws) == va.take(va.len() as int) && acc(s0, gs, ws) == a_segs - delta;
    let (q, b_bytes) = head_postfix::<L::R>(b);
    buf.clear();
    proof {
        assert(buf@ =~= Seq::<u8>::empty());
        assert(va.take(va.len() as int) =~= va);
        lemma_rel_wf(s0, pre, buf@, 0, gs, ws, h0);
        lemma_plen_le(s0, pre, buf@, gs, ws, h0);
        lemma_plen_bound::<L::R>(b@, 0, q, b_bytes as int);
        lemma_postfix_nulls::<L::R>(b@, 0, q, b_bytes as int);
        lemma_rest_bound::<L::R>(b@, 0, q, b_bytes as int);
        lemma_lift_slab::<L>(a@);
        lemma_lift_slab::<L>(b@);
    }
    let pend = match &q {
        Some(r) => r.pending(),
        None => 0,
    };
    let postfix = cow_postfix::<L>(q);
    proof {
        lemma_pmap::<L>(postfix);
    }
    let f = s.flush_postfix(buf, postfix);
    let ghost b1 = buf@;
    let ghost x = apply::<L>(f.rewrite, pre, h0) + b1.skip(0) + b@.skip(b_bytes as int);
    let ghost segs = (a_segs - delta) + f.segments + rest_segs::<L::R>(b@, 0, q, b_bytes as int);
    proof {
        // what the flush wrote, then `b`'s bytes after its first run
        assert(rel(s0, pre, Seq::empty(), 0, gs, ws, h0));
        assert(postfix_ok::<L::R>(b@, 0, q, b_bytes as int));
        assert(vb.skip(0) =~= vb);
        assert(encodes::<L::R>(x, va + vb, segs));
        lemma_encodes_segs::<L::R>(x, va + vb, segs);
        // the rewrite and the encoding read the same with owned values
        lemma_rw_at::<L>(s0, pre, Seq::empty(), 0, gs, ws, h0, f.rewrite);
        L::axiom_lift_codec(h0, 0);
        if f.rewrite is Some {
            L::axiom_lift_codec(-(f.rewrite->Some_0.count as int) as i64, 0);
        }
        assert(apply::<L>(f.rewrite, pre, h0) == apply::<L::W>(f.rewrite, pre, h0));
        assert(b1.skip(0) =~= b1);
        lemma_lift_encodes::<L>(x, va + vb, segs);
        lemma_lift_concat::<L>(va, vb);
    }
    let segments = (a_segs - delta) + f.segments + (b_segs - pend);
    let p = MergePlan { tail_pos, f, b_bytes, segments, h0: Ghost(h0) };
    proof {
        let t = merge_tail_of::<L::W>(p, a@, b1, b@, tail_b(*b_tail) as int, tail_l(*b_tail) as int);
        assert(tail_ok::<L::R>(x, t.0, t.1));
        lemma_tail_ok_bounds::<L::R>(x, t.0, t.1);
        lemma_lift_tail_ok::<L>(x, t.0, t.1);
        assert(x == merge_bytes::<L::W>(p, a@, b1, b@));
    }
    p
}

/// Write the planned merge into the first slab's bytes, which start at
/// `start` in `a`: its bytes from the plan's tail position replaced by
/// `buf` (its header rewritten first), then `b`'s after its first run.
/// Returns the tail record.
pub(crate) fn merge_apply<'a, W: WriteOps<'a>>(a: &mut Vec<u8>, start: usize, p: &MergePlan, buf: &[u8], b: &[u8], b_tail: &RleTail) -> (t: RleTail)
    requires
        start <= old(a)@.len(), p.tail_pos <= old(a)@.len() - start, p.b_bytes <= b@.len(),
        rw_at::<W>(p.f.rewrite, old(a)@.skip(start as int).take(p.tail_pos as int), p.h0@),
        ({
            let t = merge_tail_of::<W>(*p, old(a)@.skip(start as int), buf@, b@, tail_b(*b_tail) as int, tail_l(*b_tail) as int);
            0 <= t.1 <= t.0
        }),
    ensures
        final(a)@ == old(a)@.take(start as int) + merge_bytes::<W>(*p, old(a)@.skip(start as int), buf@, b@),
        ({
            let t2 = merge_tail_of::<W>(*p, old(a)@.skip(start as int), buf@, b@, tail_b(*b_tail) as int, tail_l(*b_tail) as int);
            tail_b(t) == t2.0 && tail_l(t) == t2.1
        }),
{
    let ghost a0 = a@;
    let ghost head = a0.take(start as int);
    let ghost pre = a0.skip(start as int).take(p.tail_pos as int);
    let ghost h0 = p.h0@;
    a.truncate(start + p.tail_pos);
    assert(a@ =~= head + pre);
    if let Some(rw) = p.f.rewrite {
        let ghost h = <W::R as RleOps<'a>>::enc_signed(h0);
        proof {
            assert(a@.skip(start + rw.pos) =~= pre.skip(rw.pos as int));
            assert(h.is_prefix_of(a@.skip(start + rw.pos)));
        }
        let _ = W::rewrite_lit_header(a, start + rw.pos, rw.count);
        proof {
            assert(head + pre == a0.take(start + p.tail_pos));
            assert(a@ =~= head + apply::<W>(p.f.rewrite, pre, h0));
        }
    }
    let a_len = a.len();
    let ghost a1 = a@;
    a.extend_from_slice(buf);
    let ghost a2 = a@;
    let rest = &b[p.b_bytes..];
    a.extend_from_slice(rest);
    proof {
        assert(a2 =~= a1 + buf@) by {
            assert forall|i: int| 0 <= i < a2.len() implies a2[i] == (a1 + buf@)[i] by {
                if i >= a1.len() {
                    assert(cloned::<u8>(buf@[i - a1.len()], a2[i]));
                }
            }
        }
        assert(a@ =~= a2 + b@.skip(p.b_bytes as int)) by {
            assert forall|i: int| 0 <= i < a@.len() implies a@[i] == (a2 + b@.skip(p.b_bytes as int))[i] by {
                if i >= a2.len() {
                    assert(cloned::<u8>(rest@[i - a2.len()], a@[i]));
                }
            }
        }
    }
    let _total = a.len();
    merge_tail(p.f.wpos, a_len - start, buf.len(), b.len() - p.b_bytes, b_tail)
}

} // verus!
