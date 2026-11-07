//! Progressive editing of a single RLE slab.
//!
//! [`RleEdit`] is the slab half of a column cursor: it opens a rebuild at
//! one item, takes any mix of *pass* (carry the slab's own items
//! through), *insert* and *delete* moving forward, and closes by carrying
//! the untouched tail wholesale.
//!
//! One splice is `reset` + `delete` + `insert` + `close`. What a cursor
//! adds is staying open across several of them, so N edits landing in one
//! slab cost one partition, one buffer, one byte splice and one index
//! update rather than N of each — the shape real workloads have (94% of
//! the splices in an automerge fragment chain re-open the slab the
//! previous one just rebuilt).
//!
//! The cursor owns its encoder state, where a one-shot splice borrows the
//! slab, because it lives alongside a `&mut Column`. For a `Copy` value
//! that is a register move; string columns pay a clone per run carried.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use crate::cursor::Slabs;
#[cfg(verus_keep_ghost)]
use crate::cursor::{lemma_concat_append, lemma_vals_cat, lemma_vals_unit, vals_of};
use crate::edit::{EditSlab, SlabEdit};
#[cfg(verus_keep_ghost)]
use crate::rle::cut::{lemma_lit_tail_le, slab_ok};
#[cfg(verus_keep_ghost)]
use crate::rle::dec::*;
use crate::rle::dec::{partition_at, postfix_at, Dec, RleDecoderState};
use crate::rle::lift::*;
use crate::rle::merge::{merge_apply, merge_plan};
#[cfg(verus_keep_ghost)]
use crate::rle::merge::{merge_bytes, slab_meta};
use crate::rle::model::*;
#[cfg(verus_keep_ghost)]
use crate::rle::postfix::*;
#[cfg(verus_keep_ghost)]
use crate::rle::read::{lit_tail_of, tail_bytes_of};
use crate::rle::state::*;
#[cfg(verus_keep_ghost)]
use crate::rle::writer::*;
use crate::rle::writer::{add, merge_tail, tail_rec, Rebuilt, Splice};
use crate::rle::{RleTail, Slab};
use crate::{AsColumnRef, Codec, RleValue};
use std::marker::PhantomData;
#[allow(unused_imports)]
use vstd::prelude::*;
use vstd::slice::slice_subrange;

verus! {

/// A range of a sequence, split.
proof fn lemma_subrange_concat<V>(s: Seq<V>, a: int, b: int, c: int)
    requires 0 <= a <= b <= c <= s.len(),
    ensures s.subrange(a, c) == s.subrange(a, b) + s.subrange(b, c),
{
    assert(s.subrange(a, c) =~= s.subrange(a, b) + s.subrange(b, c));
}

/// What a walk shows each run it moves over (a closure, outside Verus):
/// anything goes, as far as the rebuild is concerned.
pub(crate) trait Visit<V> {
    fn visit(&mut self, v: V, n: usize);
}

/// A rebuild in progress within one slab, verified with Verus: the
/// values written so far (`vals`), and how far the slab
/// has been read ([`read`](Edit::read)). Closing it leaves the slab, and
/// any it spills into, the canonical encoding of the values written then
/// the slab's from where reading stopped.
///
/// `M` writes owned values; each call reads the slab through a
/// borrowed-value model `L` that [`Lift`]s to it.
pub(crate) struct Edit<M: WriteOps<'static>> {
    /// the shared encoder — the same one a one-shot splice drives
    w: Splice<'static, M>,
    /// Where reading stopped — every walk starts here and `close`
    /// describes the tail from here, so nothing is read twice. The same
    /// reader the cursor carries, so a rebuild picks up where the cursor
    /// stands rather than re-finding it.
    dec: RleDecoderState,
    /// Complete wire segments behind the reader. The writer needs it to
    /// describe the tail it carries; a decoder has no use for it, which
    /// is why it is counted here rather than inside one.
    read_segments: usize,
    read: usize,
    written: usize,
    /// the slab's bytes
    data: Ghost<Seq<u8>>,
}

impl<M: WriteOps<'static>> Edit<M> {
    /// The slab's bytes.
    pub(crate) closed spec fn data(&self) -> Seq<u8> {
        self.data@
    }

    /// The values written: the slab's before the edit, then what the
    /// edit carried through and inserted.
    pub(crate) closed spec fn vals(&self) -> Seq<VOf<'static, M>> {
        self.w.vals()
    }

    /// How many of the slab's values have been read.
    pub(crate) closed spec fn read(&self) -> int {
        self.read as int
    }

    pub(crate) proof fn lemma_read(&self)
        ensures self.read() >= 0,
    {
    }

    /// Room for `n` more values.
    pub(crate) closed spec fn room(&self, n: nat) -> bool {
        self.w.room(n)
    }

    /// What room is measured against.
    pub(crate) closed spec fn used(&self) -> nat {
        self.w.used()
    }

    pub(crate) proof fn lemma_room(&self, n: nat)
        ensures self.room(n) == (self.used() + n + 2 < usize::MAX),
    {
        self.w.lemma_used(n);
    }

    /// The slab's bytes before the edit, and the count in its literal
    /// header there.
    pub(crate) closed spec fn pre(&self) -> Seq<u8> {
        self.w.pre()
    }

    pub(crate) closed spec fn h0(&self) -> i64 {
        self.w.h0()
    }

    closed spec fn core(&self) -> bool {
        &&& valid_slab::<M::R>(self.data@) && self.data@.len() < i64::MAX
        &&& self.w.inv()
        &&& self.w.pre().len() <= self.data@.len()
        &&& self.w.pre() == self.data@.take(self.w.pre().len() as int)
        &&& self.read <= slab_values::<M::R>(self.data@).len()
    }

    /// Open, between walks.
    pub(crate) closed spec fn inv(&self) -> bool {
        &&& self.core()
        &&& state_ok::<M::R>(self.data@, self.dec) && state_pos::<M::R>(self.data@, self.dec) == self.read
        &&& self.w.pre().len() <= sbyte(self.dec)
        &&& self.read_segments == rsegs::<M::R>(self.data@, self.dec)
    }

    /// Open, walking with the reader `d`.
    pub(crate) closed spec fn walking<'s, O: RleOps<'s>>(&self, d: Dec<'s, O>) -> bool {
        &&& self.core()
        &&& d.inv() && d.data() == self.data@ && d.pos() == self.read
        &&& self.w.pre().len() <= sbyte(d.dstate())
        &&& self.read_segments == d.dsegs()
    }

    pub(crate) fn new() -> (r: Self) {
        Edit {
            w: Splice::empty(),
            dec: RleDecoderState::start(),
            read_segments: 0,
            read: 0,
            written: 0,
            data: Ghost(Seq::empty()),
        }
    }

    /// The reader's position.
    pub(crate) fn dec(&self) -> (r: &RleDecoderState)
        ensures self.inv() ==> state_ok::<M::R>(self.data(), *r) && state_pos::<M::R>(self.data(), *r) == self.read(),
    {
        &self.dec
    }

    /// The slab in hand holds no more values than were written.
    pub(crate) proof fn lemma_used_le(&self)
        ensures self.used() <= self.vals().len(),
    {
        self.w.lemma_used_le();
    }

    /// Open a rebuild of the valid slab `data` at item `at`. One walk,
    /// both halves: the run the cut lands in for the writer to re-emit,
    /// and a reader standing at the cut.
    pub(crate) fn reset<'s, L: Lift<'s, W = M>>(&mut self, data: &'s [u8], at: usize, max_segments: usize)
        requires
            valid_slab::<L::R>(data@), data@.len() < i64::MAX,
            at <= slab_values::<L::R>(data@).len(),
        ensures
            final(self).inv(), final(self).data() == data@, final(self).read() == at,
            final(self).vals() == slab_values::<M::R>(data@).take(at as int),
    {
        let p = partition_at::<L>(data, at);
        let ghost bs = p.byte_start as int;
        let ghost pre = data@.take(bs);
        let ghost (gs, ws, h0) = choose|gs: Seq<Group<VOf<'s, L>>>, ws: Seq<VOf<'s, L>>, h0: i64|
            #[trigger] rel::<L>(p.state, pre, Seq::empty(), 0, gs, ws, h0)
            && values_of::<L>(p.state, gs, ws) == slab_values::<L::R>(data@).take(at as int)
            && acc::<L>(p.state, gs, ws) == p.segments;
        let state = own_state::<L>(p.state);
        let ghost vals = slab_values::<M::R>(data@).take(at as int);
        proof {
            lemma_lift_rel::<L>(p.state, state, pre, Seq::empty(), 0, gs, ws, h0);
            lemma_lift_slab::<L>(data@);
            assert(lift_seq::<L>(slab_values::<L::R>(data@).take(at as int)) =~= vals);
        }
        self.w.reset(state, p.segments, at, p.byte_start, max_segments, Ghost(pre), Ghost(h0), Ghost(vals));
        self.dec = p.read_state;
        self.read_segments = p.read_segments;
        self.read = at;
        self.written = 0;
        self.data = Ghost(data@);
        proof {
            lemma_lift_state::<L>(data@, p.read_state);
            lemma_slab::<M::R>(data@);
        }
    }

    /// Start walking: a reader where reading stopped.
    pub(crate) fn walk_begin<'s, L: Lift<'s, W = M>>(&self, data: &'s [u8]) -> (d: Dec<'s, L::R>)
        requires self.inv(), data@ == self.data(),
        ensures self.walking(d),
    {
        proof {
            lemma_unlift_slab::<L>(data@);
            lemma_lift_state::<L>(data@, self.dec);
        }
        let d = Dec::resume(data, &self.dec);
        proof {
            d.lemma_dstate();
        }
        d
    }

    /// Read the next run, of at most `want` values, carrying it through
    /// the rebuild if `emit`, else dropping it. `None` at the end of the
    /// slab.
    pub(crate) fn walk_step<'s, L: Lift<'s, W = M>>(&mut self, d: &mut Dec<'s, L::R>, want: usize, emit: bool) -> (r: Option<crate::Run<VOf<'s, L>>>)
        requires old(self).walking(*old(d)), want >= 1, emit ==> old(self).room(want as nat),
        ensures
            final(self).walking(*final(d)), final(self).data() == old(self).data(),
            final(self).pre() == old(self).pre(), final(self).h0() == old(self).h0(),
            match r {
                Some(run) => {
                    &&& 1 <= run.count <= want
                    &&& final(self).read() == old(self).read() + run.count
                    &&& slab_values::<L::R>(old(self).data()).subrange(old(self).read(), final(self).read())
                        == copies(run.count as nat, run.value)
                    &&& final(self).vals() == old(self).vals()
                        + if emit { copies(run.count as nat, L::lift(run.value)) } else { Seq::empty() }
                    &&& final(self).used() <= old(self).used() + if emit { run.count as nat } else { 0 }
                },
                None => {
                    &&& final(self).read() == old(self).read() && final(self).vals() == old(self).vals()
                    &&& old(self).read() == slab_values::<M::R>(old(self).data()).len()
                },
            },
    {
        let ghost d0 = *d;
        let ghost data = self.data@;
        proof {
            lemma_unlift_slab::<L>(data);
            lemma_lift_slab::<L>(data);
            d0.lemma_dstate();
            lemma_slab::<L::R>(data);
        }
        let run = match d.next_run_max(want) {
            Some(run) => run,
            None => {
                proof {
                    d.lemma_dstate();
                    lemma_sbyte_end::<L::R>(data, d.dstate());
                }
                return None;
            },
        };
        let ghost lv = slab_values::<L::R>(data);
        proof {
            assert(lv.subrange(self.read as int, self.read + run.count) =~= copies(run.count as nat, run.value));
            d.lemma_dstate();
            lemma_sbyte_mono::<L::R>(data, d0.dstate(), d.dstate());
            lemma_rsegs_le::<L::R>(data, d.dstate());
        }
        if emit {
            proof {
                L::axiom_lift(run.value);
                L::axiom_lift_codec(0, 0);
                if <L::R as RleOps<'s>>::is_null(run.value) {
                    assert(lv[self.read as int] == run.value);
                    lemma_slab_null::<L::R>(data, self.read as int);
                }
            }
            proof {
                self.w.lemma_room_le(run.count as nat, want as nat);
            }
            self.w.push_cow(L::own(L::cow_ref(run.value)), run.count);
            self.written = add(self.written, run.count);
        }
        self.read = self.read + run.count;
        // a literal is one segment per value; a repeat or null run is
        // one segment however many items it holds, so it counts only
        // once the decoder has given up the last of them
        if d.is_literal() || d.left() == 0 {
            self.read_segments = self.read_segments + 1;
        }
        Some(run)
    }

    /// Move the reader `n` values on, carrying them through the rebuild
    /// if `emit` (spending at most `budget` runs), else dropping them;
    /// `f` sees every run either way. Returns how many it did not move:
    /// only an emitting walk stops short, when its budget runs out.
    ///
    /// Reading starts where the last walk stopped, so nothing is decoded
    /// twice.
    pub(crate) fn walk<'s, L: Lift<'s, W = M>, G: Visit<VOf<'s, L>>>(&mut self, data: &'s [u8], n: usize, emit: bool, budget: usize, f: &mut G) -> (left: usize)
        requires
            old(self).inv(), data@ == old(self).data(),
            old(self).read() + n <= slab_values::<M::R>(data@).len(),
            emit ==> old(self).room(n as nat),
        ensures
            final(self).inv(), final(self).data() == old(self).data(),
            final(self).pre() == old(self).pre(), final(self).h0() == old(self).h0(),
            left <= n, !emit ==> left == 0,
            final(self).read() == old(self).read() + (n - left),
            final(self).vals() == old(self).vals() + if emit {
                lift_seq::<L>(slab_values::<L::R>(data@).subrange(old(self).read(), final(self).read()))
            } else {
                Seq::empty()
            },
            emit ==> final(self).used() + left <= old(self).used() + n,
            !emit ==> final(self).used() <= old(self).used(),
    {
        let ghost r0 = self.read();
        let ghost v0 = self.vals();
        let ghost u0 = self.used();
        let ghost lv = slab_values::<L::R>(data@);
        proof {
            lemma_unlift_slab::<L>(data@);
            lemma_lift_slab::<L>(data@);
            self.lemma_room(n as nat);
            self.lemma_read();
            assert(lv.subrange(r0, r0) =~= Seq::<VOf<'s, L>>::empty());
            assert(lift_seq::<L>(Seq::<VOf<'s, L>>::empty()) =~= Seq::<VOf<'static, M>>::empty());
            assert(v0 + Seq::<VOf<'static, M>>::empty() =~= v0);
        }
        let mut d = self.walk_begin::<L>(data);
        let mut want = n;
        let mut pushes: usize = 0;
        while want > 0
            invariant
                self.walking(d), self.data() == data@,
                self.pre() == old(self).pre(), self.h0() == old(self).h0(),
                want <= n, self.read() == r0 + (n - want), 0 <= r0, r0 + n <= lv.len(),
                lv == slab_values::<L::R>(data@), lv.len() == slab_values::<M::R>(data@).len(),
                self.vals() == v0 + if emit { lift_seq::<L>(lv.subrange(r0, self.read())) } else { Seq::empty() },
                emit ==> self.used() + want <= u0 + n && u0 + n + 2 < usize::MAX,
                !emit ==> self.used() <= u0,
                emit ==> pushes <= budget,
            ensures !emit ==> want == 0,
            decreases want,
        {
            if emit && pushes == budget {
                break;
            }
            proof {
                if emit {
                    self.lemma_room(want as nat);
                }
            }
            let ghost read0 = self.read();
            let ghost vals0 = self.vals();
            let run = match self.walk_step::<L>(&mut d, want, emit) {
                Some(run) => run,
                None => break,
            };
            f.visit(run.value, run.count);
            if emit {
                pushes = pushes + 1;
            }
            want = want - run.count;
            proof {
                assert(r0 <= read0 <= self.read() <= lv.len());
                let a = lv.subrange(r0, read0);
                let b = lv.subrange(read0, self.read());
                lemma_subrange_concat(lv, r0, read0, self.read());
                if emit {
                    assert(lift_seq::<L>(a + b) =~= lift_seq::<L>(a) + lift_seq::<L>(b));
                    assert(lift_seq::<L>(b) =~= copies(run.count as nat, L::lift(run.value)));
                    assert(self.vals() =~= v0 + lift_seq::<L>(lv.subrange(r0, self.read())));
                }
            }
        }
        self.walk_end::<L>(d);
        want
    }

    /// Stop walking: keep where reading stopped.
    pub(crate) fn walk_end<'s, L: Lift<'s, W = M>>(&mut self, d: Dec<'s, L::R>)
        requires old(self).walking(d),
        ensures
            final(self).inv(), final(self).data() == old(self).data(), final(self).read() == old(self).read(),
            final(self).vals() == old(self).vals(), final(self).used() == old(self).used(),
            final(self).pre() == old(self).pre(), final(self).h0() == old(self).h0(),
    {
        self.dec = d.suspend();
        proof {
            d.lemma_dstate();
            lemma_unlift_slab::<L>(self.data@);
            lemma_lift_state::<L>(self.data@, self.dec);
        }
    }

    /// Insert `n` copies of `value` at the cursor.
    pub(crate) fn insert_cow(&mut self, value: M::Cow, n: usize)
        requires
            old(self).inv(), old(self).room(n as nat),
            <M::R as RleOps<'static>>::is_null(M::val(value)) ==> <M::R as RleOps<'static>>::nullable(),
        ensures
            final(self).inv(), final(self).data() == old(self).data(), final(self).read() == old(self).read(),
            final(self).vals() == old(self).vals() + copies(n as nat, M::val(value)),
    {
        if n == 0 {
            assert(old(self).vals() + copies(0, M::val(value)) =~= old(self).vals());
            return;
        }
        self.w.push_cow(value, n);
        self.written = add(self.written, n);
    }

    /// Room to close: for the slab's values not yet read.
    pub(crate) closed spec fn room_close(&self) -> bool {
        self.w.room((slab_values::<M::R>(self.data@).len() - self.read) as nat)
    }

    /// Close: carry the slab's values from where reading stopped (the
    /// first run as values, the rest as bytes) and finish. Only the
    /// first run after the cursor is read — a close never re-reads what
    /// it already walked.
    pub(crate) fn close_rebuild<'s, L: Lift<'s, W = M>>(&mut self, data: &'s [u8], slab_segments: usize, slab_len: usize, slab_tail: RleTail) -> (r: Rebuilt)
        requires
            old(self).inv(), data@ == old(self).data(), old(self).room_close(),
            slab_len == slab_values::<M::R>(data@).len(),
            slab_segments == groups_segments(slab_groups::<M::R>(data@)),
            tail_b(slab_tail) == tail_bytes_of::<M::R>(slab_groups::<M::R>(data@)),
            tail_l(slab_tail) == lit_tail_of::<M::R>(slab_groups::<M::R>(data@)),
        ensures
            old(self).pre() == data@.take(old(self).pre().len() as int),
            r.h0@ == old(self).h0(),
            rebuilt_ok::<M>(r, old(self).pre(), old(self).h0(), data@,
                old(self).vals() + slab_values::<M::R>(data@).skip(old(self).read())),
    {
        proof {
            lemma_unlift_slab::<L>(data@);
            lemma_lift_state::<L>(data@, self.dec);
        }
        let (p, end) = postfix_at::<L::R>(data, &self.dec);
        proof {
            lemma_lift_slab::<L>(data@);
            lemma_lift_groups::<L>(slab_groups::<L::R>(data@));
            lemma_lift_postfix::<L>(data@, self.read as int, p, end as int);
            lemma_rest_bound::<M::R>(data@, self.read as int, plift::<L>(p), end as int);
        }
        let postfix_segments = match &p {
            Some(q) => slab_segments - self.read_segments - q.pending(),
            None => 0,
        };
        let ghost lp = p;
        let postfix = own_postfix::<L>(p);
        let tail_count = slab_len - self.read;
        proof {
            lemma_lift_postfix::<L>(data@, self.read as int, lp, end as int);
            lemma_plen_bound::<M::R>(data@, self.read as int, plift::<L>(lp), end as int);
            lemma_pmap::<M>(postfix);
            self.w.lemma_room_le(plen_of(postfix), (slab_values::<M::R>(data@).len() - self.read) as nat);
            self.w.lemma_room_le(tail_count as nat, (slab_values::<M::R>(data@).len() - self.read) as nat);
        }
        self.w.set_range_end(end);
        let tail = &data[end..];
        assert(tail@ == data@.skip(end as int));
        let r = self.w.finish(postfix, postfix_segments, tail_count, tail, data.len(), slab_tail);
        proof {
            let ghost w0 = old(self).w;
            assert(postfix_ok::<M::R>(data@, self.read as int, pmap::<M>(postfix), end as int));
        }
        r
    }
}


// ── The cursor's slab model ────────────────────────────────────────────────

/// An RLE slab rebuild in progress: RLE columns' `SlabEdit::Edit`
/// (`RleEdit`).
pub struct RleRebuild<M: WriteOps<'static>> {
    pub(crate) e: Edit<M>,
}

/// RLE slabs as the column cursor sees them (`Slabs`), verified against
/// the RLE format: a slab is well-formed when `slab_ok` (its bytes a
/// canonical encoding, with its metadata), and holds the values its
/// bytes encode, owned. `L` is the column's writer ops: one type for
/// every lifetime a slab is borrowed for, owning its values as itself.
///
/// Public only because RLE columns' `SlabEdit::Model` names it; this
/// module is not.
pub struct RleModel<L>(PhantomData<L>);

/// A slab too long for the format's byte arithmetic: no slab is, as a
/// slab's bytes are bounded by its segment budget, but the specifications
/// do not say so.
#[cold]
#[inline(never)]
#[verifier::external_body]
fn slab_too_long()
    ensures false,
{
    panic!("RLE slab too long")
}

/// A canonical encoding of `vals`, whose recorded length, segments and
/// tail match it, is a well-formed slab holding them.
proof fn lemma_slab_of<'a, O: RleOps<'a>>(s: Slab, vals: Seq<O::V>)
    requires
        encodes::<O>(slab_bytes(s), vals, slab_segments(s) as int),
        tail_ok::<O>(slab_bytes(s), tail_b(slab_tail_of(s)) as int, tail_l(slab_tail_of(s)) as int),
        slab_len(s) == vals.len(),
    ensures
        slab_ok::<O>(s),
        slab_values::<O>(slab_bytes(s)) == vals,
{
    lemma_meta_of::<O>(slab_bytes(s), vals, slab_segments(s) as int, slab_tail_of(s));
}

/// A slab of `len` copies of `value` (`ColumnEncoding::fill`): what a
/// fresh writer writes for them.
#[inline]
pub(crate) fn fill<'s, L>(value: <L as WriteOps<'s>>::Cow, len: usize) -> (s: Slab)
    where L: for<'x> Lift<'x, W = L>,
    requires
        1 <= len <= i64::MAX,
        len < usize::MAX - 1,
    ensures
        RleModel::<L>::ok(s),
        RleModel::<L>::vals(s) == copies(len as nat, <L as Lift<'s>>::lift(<L as WriteOps<'s>>::val(value))),
        s.len == len,
{
    if <L as WriteOps<'s>>::cow_is_null(&value) && !<<L as WriteOps<'s>>::R as RleOps<'s>>::nullable_exec() {
        null_in_column(Ghost(true));
    }
    let ghost v = <L as WriteOps<'s>>::val(value);
    let mut buf = Vec::new();
    let mut state = State::<'s, L>::empty();
    let f1 = state.append_cow(&mut buf, value, len);
    let ghost s1 = state;
    let ghost b1 = buf@;
    let f2 = state.flush(&mut buf);
    let tail0 = tail_rec(0, 0);
    let tail = merge_tail(f2.wpos, 0, buf.len(), 0, &tail0);
    let segments = f1.segments + f2.segments;
    let s = Slab { data: buf, len, segments, tail };
    proof {
        let eg = Seq::<Group<VOf<'s, L>>>::empty();
        let ev = Seq::<VOf<'s, L>>::empty();
        let eb = Seq::<u8>::empty();
        let h0 = 0i64;
        let s0 = State::<'s, L>::Empty;
        assert(eb.skip(0) =~= eb);
        assert(groups_values(eg, <<L as WriteOps<'s>>::R as RleOps<'s>>::null()) =~= ev);
        assert(rel(s0, eb, eb, 0, eg, ev, h0));
        // starting empty, no header is pending, so nothing is rewritten
        assert(f1.rewrite is None);
        assert(f2.rewrite is None);
        assert(apply::<L>(f1.rewrite, eb, h0) == eb);
        let (gs, ws) = choose|gs: Seq<Group<VOf<'s, L>>>, ws: Seq<VOf<'s, L>>| #[trigger] rel(s1, eb, b1, 0, gs, ws, h0)
            && values_of(s1, gs, ws) == values_of(s0, eg, ev) + copies(len as nat, v)
            && acc(s1, gs, ws) == acc(s0, eg, ev) + f1.segments;
        assert(s.data@.skip(0) =~= s.data@);
        assert(eb + s.data@ =~= s.data@);
        assert(values_of(s0, eg, ev) + copies(len as nat, v) =~= copies(len as nat, v));
        assert(encodes::<<L as WriteOps<'s>>::R>(s.data@, copies(len as nat, v), segments as int));
        lemma_flush_tail::<L>(s1, eb, b1, gs, ws, h0, s.data@, f2.rewrite, f2.segments as int, f2.wpos);
        let t = tail_after(f2.wpos, 0, 0, s.data@.len() as int, 0, tail_b(tail0) as int, tail_l(tail0) as int);
        assert(tail_ok::<<L as WriteOps<'s>>::R>(s.data@, t.0, t.1));
        lemma_written::<L>(s, copies(len as nat, v), f2.wpos, tail_b(tail0) as int, tail_l(tail0) as int);
        assert(lift_seq::<L>(copies(len as nat, v)) =~= copies(len as nat, <L as Lift<'s>>::lift(v)));
    }
    s
}

/// A slab a writer wrote from empty and flushed — the canonical encoding
/// of `vals`, its segments counted, and the tail record `merge_tail`
/// makes of the flush's position `w` — is well-formed, and holds their
/// owned copies.
pub(crate) proof fn lemma_written<'s, L>(s: Slab, vals: Seq<VOf<'s, L>>, w: WPos, tb: int, tl: int)
    where L: for<'x> Lift<'x, W = L>,
    requires
        encodes::<<L as WriteOps<'s>>::R>(slab_bytes(s), vals, slab_segments(s) as int),
        ({
            let t = tail_after(w, 0, 0, slab_bytes(s).len() as int, 0, tb, tl);
            &&& tail_ok::<<L as WriteOps<'s>>::R>(slab_bytes(s), t.0, t.1)
            &&& 0 <= t.1 <= t.0 ==> tail_b(slab_tail_of(s)) == t.0 && tail_l(slab_tail_of(s)) == t.1
        }),
        slab_len(s) == vals.len(),
    ensures
        RleModel::<L>::ok(s),
        RleModel::<L>::vals(s) == lift_seq::<L>(vals),
{
    let data = slab_bytes(s);
    let t = tail_after(w, 0, 0, data.len() as int, 0, tb, tl);
    let gt = choose|gt: Seq<Group<VOf<'s, L>>>| #[trigger] canonical::<<L as WriteOps<'s>>::R>(gt)
        && groups_bytes::<<L as WriteOps<'s>>::R>(gt) == data
        && t.0 == tail_bytes_of::<<L as WriteOps<'s>>::R>(gt) && t.1 == lit_tail_of::<<L as WriteOps<'s>>::R>(gt);
    lemma_lit_tail_le::<<L as WriteOps<'s>>::R>(gt);
    lemma_lift_encodes::<L>(data, vals, slab_segments(s) as int);
    lemma_lift_tail_ok::<L>(data, t.0, t.1);
    lemma_slab_of::<<L as WriteOps<'static>>::R>(s, lift_seq::<L>(vals));
}

/// Bytes that are the canonical encoding of `vals` with `segs` segments
/// are a valid slab of them.
proof fn lemma_valid_of<'a, O: RleOps<'a>>(data: Seq<u8>, vals: Seq<O::V>, segs: int)
    requires
        encodes::<O>(data, vals, segs),
        vals.len() <= usize::MAX,
    ensures
        valid_slab::<O>(data),
        slab_values::<O>(data) == vals,
        groups_segments(slab_groups::<O>(data)) == segs,
{
    let gs = choose|gs: Seq<Group<O::V>>| #[trigger] canonical::<O>(gs) && groups_bytes::<O>(gs) == data
        && groups_values(gs, O::null()) == vals && groups_segments(gs) == segs;
    lemma_groups_values_concat(gs, Seq::empty(), O::null());
    reveal(parses);
    assert(parses::<O>(gs, data));
    let gc = slab_groups::<O>(data);
    lemma_groups_unique::<O>(gs, gc);
}

/// ... and with the tail record that encoding has, a well-formed slab's
/// bytes and metadata.
proof fn lemma_meta_of<'a, O: RleOps<'a>>(data: Seq<u8>, vals: Seq<O::V>, segs: int, t: RleTail)
    requires
        encodes::<O>(data, vals, segs),
        tail_ok::<O>(data, tail_b(t) as int, tail_l(t) as int),
        vals.len() <= usize::MAX,
    ensures
        slab_meta::<O>(data, segs, t),
        slab_values::<O>(data) == vals,
{
    lemma_valid_of::<O>(data, vals, segs);
    let gt = choose|gs: Seq<Group<O::V>>| #[trigger] canonical::<O>(gs) && groups_bytes::<O>(gs) == data
        && tail_b(t) as int == tail_bytes_of::<O>(gs) && tail_l(t) as int == lit_tail_of::<O>(gs);
    reveal(parses);
    lemma_groups_unique::<O>(gt, slab_groups::<O>(data));
}

/// No bytes: the valid slab of no values.
proof fn lemma_valid_empty<'a, O: RleOps<'a>>()
    ensures
        valid_slab::<O>(Seq::empty()),
        slab_values::<O>(Seq::empty()) == Seq::<O::V>::empty(),
{
    let eg = Seq::<Group<O::V>>::empty();
    assert(groups_bytes::<O>(eg) =~= Seq::<u8>::empty());
    assert(groups_values(eg, O::null()) =~= Seq::<O::V>::empty());
    assert(canonical::<O>(eg));
    lemma_valid_of::<O>(Seq::empty(), Seq::empty(), 0);
}

/// The canonical encoding of the slabs' values, appended to `out`
/// (`Column::save_to`): the first slab's bytes, then each of the others
/// merged on. `total` is their item count, as a column records it.
#[inline]
pub(crate) fn save_slabs<L>(slabs: &[Slab], total: usize, out: &mut Vec<u8>)
    where L: for<'x> Lift<'x, W = L>,
    requires
        crate::cursor::all_ok::<RleModel<L>>(slabs@),
        total == crate::fenwick::total(crate::cursor::slab_lens(slabs@)),
    ensures
        final(out)@.take(old(out)@.len() as int) == old(out)@,
        ({
            let x = final(out)@.skip(old(out)@.len() as int);
            &&& valid_slab::<<L as WriteOps<'static>>::R>(x)
            &&& slab_values::<<L as WriteOps<'static>>::R>(x) == vals_of::<RleModel<L>>(slabs@)
        }),
{
    let start = out.len();
    let ghost s = slabs@;
    let ghost o0 = out@;
    if slabs.len() == 0 {
        proof {
            assert(out@.take(start as int) =~= o0);
            assert(out@.skip(start as int) =~= Seq::<u8>::empty());
            lemma_vals_unit::<RleModel<L>>(arbitrary());
            lemma_valid_empty::<<L as WriteOps<'static>>::R>();
        }
        return;
    }
    // the merges' value counts: no column comes near
    if total > usize::MAX - 3 {
        slab_too_long();
    }
    out.extend_from_slice(slabs[0].data.as_slice());
    let mut tail = slabs[0].tail;
    let mut segments = slabs[0].segments;
    let ghost mut n = slabs[0].len as int;
    let mut buf = Vec::new();
    proof {
        assert(out@.take(start as int) =~= o0);
        assert(out@.skip(start as int) =~= slabs[0].data@);
        assert(s.take(1) =~= seq![s[0]]);
        lemma_vals_unit::<RleModel<L>>(s[0]);
        assert(RleModel::<L>::ok(s[0]));
        crate::cursor::lemma_vals_total::<RleModel<L>>(s);
    }
    let mut i = 1;
    while i < slabs.len()
        invariant
            s == slabs@, crate::cursor::all_ok::<RleModel<L>>(s),
            total == vals_of::<RleModel<L>>(s).len(), total <= usize::MAX - 3,
            1 <= i <= s.len(),
            start == o0.len(), start <= out@.len(),
            out@.take(start as int) == o0,
            slab_meta::<<L as WriteOps<'static>>::R>(out@.skip(start as int), segments as int, tail),
            slab_values::<<L as WriteOps<'static>>::R>(out@.skip(start as int)) == vals_of::<RleModel<L>>(s.take(i as int)),
            n == slab_values::<<L as WriteOps<'static>>::R>(out@.skip(start as int)).len(),
        decreases s.len() - i,
    {
        let ghost x0 = out@.skip(start as int);
        let ghost va = slab_values::<<L as WriteOps<'static>>::R>(x0);
        let ghost vb = slab_values::<<L as WriteOps<'static>>::R>(slabs[i as int].data@);
        proof {
            assert(RleModel::<L>::ok(s[i as int]));
            assert(s.take(i + 1) =~= s.take(i as int) + seq![s[i as int]]);
            lemma_vals_cat::<RleModel<L>>(s.take(i as int), seq![s[i as int]]);
            lemma_vals_unit::<RleModel<L>>(s[i as int]);
            // the values so far are among all of them
            crate::cursor::lemma_vals_split::<RleModel<L>>(s, i + 1);
        }
        let b = &slabs[i];
        if b.len > 0 {
            // the merge's byte arithmetic: no slab comes near
            if out.len() - start >= i64::MAX as usize {
                slab_too_long();
            }
            let a = slice_subrange(out.as_slice(), start, out.len());
            let p = merge_plan::<L>(a, segments, &tail, b.data.as_slice(), b.segments, &b.tail, &mut buf);
            let ghost o1 = out@;
            tail = merge_apply::<L>(out, start, &p, buf.as_slice(), b.data.as_slice(), &b.tail);
            segments = p.segments;
            proof {
                n = n + b.len;
                assert(o1.skip(start as int) =~= x0);
                let m = merge_bytes::<L>(p, x0, buf@, b.data@);
                assert(out@.take(start as int) =~= o0);
                assert(out@.skip(start as int) =~= m);
                lemma_meta_of::<<L as WriteOps<'static>>::R>(m, va + vb, segments as int, tail);
            }
        } else {
            proof {
                assert(vb =~= Seq::<VOf<'static, L>>::empty());
                assert(va + vb =~= va);
            }
        }
        i = i + 1;
    }
    proof {
        assert(s.take(s.len() as int) =~= s);
    }
}

/// A null value in a column that isn't nullable: none of the value types
/// has one.  Trusted only in that a panic does not return.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn null_in_column(Ghost(why): Ghost<bool>)
    requires
        why,
    ensures
        false,
{
    panic!("null value in a column that is not nullable")
}

/// A concatenation, split after its first part.
proof fn lemma_concat_first<V>(cs: Seq<Seq<V>>)
    requires cs.len() > 0,
    ensures concat(cs) == cs[0] + concat(cs.skip(1)),
{
    let one = seq![cs[0]];
    lemma_concat_append(one, cs.skip(1));
    assert(one + cs.skip(1) =~= cs);
    assert(one.drop_last() =~= Seq::<Seq<V>>::empty());
    assert(concat(one.drop_last()) =~= Seq::<V>::empty());
    assert(concat(one) =~= cs[0]);
}

/// A close written back: the slab and those it spilled into are
/// well-formed, and hold what the rebuild wrote, then the slab's values
/// it had not read.
proof fn lemma_close_ok<L>(r: Rebuilt, pre: Seq<u8>, h0: i64, data: Seq<u8>, out: Seq<VOf<'static, L>>, tail0: RleTail, s: Slab, spilled: Seq<Slab>)
    where L: for<'s> Lift<'s, W = L>,
    requires
        slab_meta::<<L as WriteOps<'static>>::R>(data, groups_segments(slab_groups::<<L as WriteOps<'static>>::R>(data)) as int, tail0),
        pre == data.take(pre.len() as int),
        rebuilt_ok::<L>(r, pre, h0, data, out),
        ({
            let a = apply::<L>(r.rewrite, data.take(r.start as int), h0);
            let t = tail_after(r.wpos, a.len() as int, 0, r.bytes@.len() as int, data.len() - r.end,
                tail_b(tail0) as int, tail_l(tail0) as int);
            &&& s.data@ == a + r.bytes@ + data.skip(r.end as int)
            &&& 0 <= t.1 <= t.0 ==> tail_b(s.tail) == t.0 && tail_l(s.tail) == t.1
        }),
        s.len == r.len,
        s.segments == r.segments,
        spilled == r.overflow@,
    ensures
        slab_ok::<<L as WriteOps<'static>>::R>(s),
        forall|i: int| 0 <= i < spilled.len() ==> #[trigger] slab_ok::<<L as WriteOps<'static>>::R>(spilled[i]),
        slab_values::<<L as WriteOps<'static>>::R>(slab_bytes(s)) + concat(spilled.map_values(
            |x: Slab| slab_values::<<L as WriteOps<'static>>::R>(slab_bytes(x)),
        )) == out,
{
    let cs = choose|cs: Seq<Seq<VOf<'static, L>>>| #[trigger] concat(cs) == out && cs.len() == r.overflow@.len() + 1
        && encodes::<<L as WriteOps<'static>>::R>(edited::<L>(r, pre, h0, data), cs[0], r.segments as int) && r.len == cs[0].len()
        && forall|k: int| 0 <= k < r.overflow@.len()
            ==> encodes::<<L as WriteOps<'static>>::R>(#[trigger] slab_bytes(r.overflow@[k]), cs[k + 1], slab_segments(r.overflow@[k]) as int)
            && slab_len(r.overflow@[k]) == cs[k + 1].len();
    let a = apply::<L>(r.rewrite, pre, h0);
    let t = tail_after(r.wpos, a.len() as int, 0, r.bytes@.len() as int, data.len() - r.end,
        tail_bytes_of::<<L as WriteOps<'static>>::R>(slab_groups::<<L as WriteOps<'static>>::R>(data)) as int,
        lit_tail_of::<<L as WriteOps<'static>>::R>(slab_groups::<<L as WriteOps<'static>>::R>(data)) as int);
    // the tail record the close leaves names the edited bytes' last run
    let gt = choose|gs: Seq<Group<VOf<'static, L>>>| #[trigger] canonical::<<L as WriteOps<'static>>::R>(gs)
        && groups_bytes::<<L as WriteOps<'static>>::R>(gs) == edited::<L>(r, pre, h0, data)
        && t.0 == tail_bytes_of::<<L as WriteOps<'static>>::R>(gs) && t.1 == lit_tail_of::<<L as WriteOps<'static>>::R>(gs);
    lemma_lit_tail_le::<<L as WriteOps<'static>>::R>(gt);
    assert(slab_bytes(s) == edited::<L>(r, pre, h0, data));
    lemma_slab_of::<<L as WriteOps<'static>>::R>(s, cs[0]);
    assert forall|i: int| #![trigger spilled[i]] 0 <= i < spilled.len() implies slab_ok::<<L as WriteOps<'static>>::R>(spilled[i])
        && slab_values::<<L as WriteOps<'static>>::R>(slab_bytes(spilled[i])) == cs[i + 1] by {
        lemma_slab_of::<<L as WriteOps<'static>>::R>(spilled[i], cs[i + 1]);
    }
    assert(spilled.map_values(|x: Slab| slab_values::<<L as WriteOps<'static>>::R>(slab_bytes(x))) =~= cs.skip(1));
    lemma_concat_first(cs);
}

/// The reader state at value `at` of a valid slab.
#[inline(always)]
fn state_in<'s, L: Lift<'s>>(data: &'s [u8], at: usize) -> (st: RleDecoderState)
    requires
        valid_slab::<L::R>(data@),
        at <= slab_values::<L::R>(data@).len(),
    ensures
        state_ok::<L::R>(data@, st),
        state_pos::<L::R>(data@, st) == at,
{
    let mut d = Dec::<'s, L::R>::new(data);
    d.advance_by(at);
    d.suspend()
}

/// A reader state `n` values on.
#[inline(always)]
fn state_on<'s, L: Lift<'s>>(data: &'s [u8], st: &mut RleDecoderState, n: usize)
    requires
        state_ok::<L::R>(data@, *old(st)),
        state_pos::<L::R>(data@, *old(st)) + n <= slab_values::<L::R>(data@).len(),
    ensures
        state_ok::<L::R>(data@, *final(st)),
        state_pos::<L::R>(data@, *final(st)) == state_pos::<L::R>(data@, *old(st)) + n,
{
    let mut d = Dec::<'s, L::R>::resume(data, st);
    d.advance_by(n);
    *st = d.suspend();
}

impl<L> Slabs for RleModel<L> where L: for<'s> Lift<'s, W = L> {
    type Tail = RleTail;
    type Rebuild = RleRebuild<L>;
    type State = RleDecoderState;
    type V = VOf<'static, L>;

    closed spec fn ok(s: Slab) -> bool {
        slab_ok::<<L as WriteOps<'static>>::R>(s)
    }

    closed spec fn vals(s: Slab) -> Seq<VOf<'static, L>> {
        slab_values::<<L as WriteOps<'static>>::R>(slab_bytes(s))
    }

    proof fn lemma_len(s: Slab) {
    }

    closed spec fn rb_inv(e: RleRebuild<L>) -> bool {
        e.e.inv()
    }

    closed spec fn rb_data(e: RleRebuild<L>) -> Seq<u8> {
        e.e.data()
    }

    closed spec fn rb_vals(e: RleRebuild<L>) -> Seq<VOf<'static, L>> {
        e.e.vals()
    }

    closed spec fn rb_read(e: RleRebuild<L>) -> nat {
        e.e.read() as nat
    }

    closed spec fn st_at(data: Seq<u8>, st: RleDecoderState, pos: nat) -> bool {
        state_ok::<<L as WriteOps<'static>>::R>(data, st) && state_pos::<<L as WriteOps<'static>>::R>(data, st) == pos
    }

    #[verifier::external_body]
    #[inline(always)]
    fn carry_budget() -> usize {
        crate::edit::RUN_BUDGET
    }

    #[inline(always)]
    fn idle() -> RleRebuild<L> {
        RleRebuild { e: Edit::new() }
    }

    fn empty_slab() -> (s: Slab) {
        let s = Slab { data: Vec::new(), len: 0, segments: 0, tail: tail_rec(0, 0) };
        proof {
            let e = Seq::<Group<VOf<'static, L>>>::empty();
            assert(groups_bytes::<<L as WriteOps<'static>>::R>(e) =~= s.data@);
            assert(canonical::<<L as WriteOps<'static>>::R>(e));
            assert(groups_values(e, <<L as WriteOps<'static>>::R as RleOps<'static>>::null()) =~= Seq::<VOf<'static, L>>::empty());
            lemma_slab_of::<<L as WriteOps<'static>>::R>(s, Seq::empty());
        }
        s
    }

    fn reset(e: &mut RleRebuild<L>, s: &Slab, at: usize, max_segments: usize) {
        if s.data.len() >= i64::MAX as usize {
            slab_too_long();
        }
        proof {
            lemma_unlift_slab::<L>(s.data@);
            lemma_unlift_meta::<L>(s.data@);
        }
        e.e.reset::<L>(s.data.as_slice(), at, max_segments);
    }

    fn close(e: &mut RleRebuild<L>, s: &mut Slab) -> (spilled: Vec<Slab>) {
        let ghost data = s.data@;
        let ghost e0 = e.e;
        let ghost out = e0.vals() + slab_values::<<L as WriteOps<'static>>::R>(data).skip(e0.read());
        proof {
            e.e.lemma_used_le();
            e.e.lemma_room((slab_values::<<L as WriteOps<'static>>::R>(data).len() - e0.read()) as nat);
        }
        let mut rebuilt = e.e.close_rebuild::<L>(s.data.as_slice(), s.segments, s.len, s.tail);
        let ghost r0 = rebuilt;
        let ghost pre = e0.pre();
        let ghost h0 = e0.h0();
        let (spilled, bytes) = rebuilt.apply_to::<L>(s);
        e.e.w.recycle(bytes);
        proof {
            e0.lemma_read();
            lemma_close_ok::<L>(r0, pre, h0, data, out, old(s).tail, *s, spilled@);
            assert(spilled@.map_values(|x: Slab| Self::vals(x)) =~= spilled@.map_values(
                |x: Slab| slab_values::<<L as WriteOps<'static>>::R>(slab_bytes(x)),
            ));
        }
        spilled
    }

    fn merge(a: &mut Slab, b: Slab) {
        if a.len == 0 {
            proof {
                assert(slab_values::<<L as WriteOps<'static>>::R>(slab_bytes(*a)) =~= Seq::<VOf<'static, L>>::empty());
                assert(Seq::<VOf<'static, L>>::empty() + slab_values::<<L as WriteOps<'static>>::R>(slab_bytes(b))
                    =~= slab_values::<<L as WriteOps<'static>>::R>(slab_bytes(b)));
            }
            *a = b;
        } else if b.len > 0 {
            // the merge's byte and count arithmetic: no slab comes near
            if a.data.len() >= i64::MAX as usize || a.len + b.len > usize::MAX - 3 {
                slab_too_long();
            }
            let ghost a0 = *a;
            let ghost va = slab_values::<<L as WriteOps<'static>>::R>(slab_bytes(a0));
            let ghost vb = slab_values::<<L as WriteOps<'static>>::R>(slab_bytes(b));
            let mut buf = Vec::new();
            let p = merge_plan::<L>(a.data.as_slice(), a.segments, &a.tail, b.data.as_slice(), b.segments, &b.tail, &mut buf);
            proof {
                assert(a0.data@.skip(0) =~= a0.data@);
            }
            let tail = merge_apply::<L>(&mut a.data, 0, &p, buf.as_slice(), b.data.as_slice(), &b.tail);
            a.segments = p.segments;
            a.tail = tail;
            a.len = a.len + b.len;
            proof {
                assert(a0.data@.take(0) + merge_bytes::<L>(p, a0.data@, buf@, b.data@) =~= merge_bytes::<L>(
                    p,
                    a0.data@,
                    buf@,
                    b.data@,
                ));
                lemma_slab_of::<<L as WriteOps<'static>>::R>(*a, va + vb);
            }
        } else {
            proof {
                assert(slab_values::<<L as WriteOps<'static>>::R>(slab_bytes(b)) =~= Seq::<VOf<'static, L>>::empty());
                assert(slab_values::<<L as WriteOps<'static>>::R>(slab_bytes(*a)) + Seq::<VOf<'static, L>>::empty()
                    =~= slab_values::<<L as WriteOps<'static>>::R>(slab_bytes(*a)));
            }
        }
    }

    #[inline(always)]
    fn read_state(e: &RleRebuild<L>) -> &RleDecoderState {
        e.e.dec()
    }

    fn state_at(s: &Slab, at: usize) -> (st: RleDecoderState) {
        proof {
            lemma_unlift_meta::<L>(s.data@);
        }
        let st = state_in::<L>(s.data.as_slice(), at);
        proof {
            lemma_lift_state::<L>(s.data@, st);
        }
        st
    }

    #[allow(unused_variables)]
    fn state_advance(s: &Slab, st: &mut RleDecoderState, n: usize, pos: Ghost<nat>) {
        proof {
            lemma_unlift_meta::<L>(s.data@);
            lemma_lift_state::<L>(s.data@, *st);
        }
        state_on::<L>(s.data.as_slice(), st, n);
        proof {
            lemma_lift_state::<L>(s.data@, *st);
        }
    }
}

/// Carry `n` of the slab's values through the rebuild, at most `runs`
/// pushes, telling `f` each run: `Visitor::pass` for RLE slabs, which the
/// encoding's `EditSlab::pass` forwards to.
#[inline(always)]
pub(crate) fn pass<'s, L, G>(e: &mut RleRebuild<L>, s: &'s Slab, n: usize, runs: usize, f: &mut G) -> (left: usize)
    where L: for<'t> Lift<'t, W = L>, G: Visit<VOf<'s, L>>,
    requires
        RleModel::<L>::rb_inv(*old(e)),
        RleModel::<L>::ok(*s),
        RleModel::<L>::rb_data(*old(e)) == s.data@,
        RleModel::<L>::rb_read(*old(e)) + n <= s.len,
        RleModel::<L>::rb_vals(*old(e)).len() + n + 3 <= usize::MAX,
    ensures
        RleModel::<L>::rb_inv(*final(e)),
        RleModel::<L>::rb_data(*final(e)) == s.data@,
        left <= n,
        RleModel::<L>::rb_read(*final(e)) == RleModel::<L>::rb_read(*old(e)) + (n - left),
        RleModel::<L>::rb_vals(*final(e)) == RleModel::<L>::rb_vals(*old(e)) + RleModel::<L>::vals(*s).subrange(
            RleModel::<L>::rb_read(*old(e)) as int,
            RleModel::<L>::rb_read(*old(e)) + (n - left),
        ),
{
    let ghost e0 = e.e;
    proof {
        e.e.lemma_used_le();
        e.e.lemma_room(n as nat);
        e.e.lemma_read();
        lemma_unlift_slab::<L>(s.data@);
        lemma_lift_slab::<L>(s.data@);
    }
    let left = e.e.walk::<L, G>(s.data.as_slice(), n, true, runs, f);
    proof {
        let lv = slab_values::<<L as WriteOps<'s>>::R>(s.data@);
        assert(lift_seq::<L>(lv.subrange(e0.read(), e.e.read())) =~= slab_values::<<L as WriteOps<'static>>::R>(
            s.data@,
        ).subrange(e0.read(), e.e.read()));
    }
    left
}

/// Drop `n` of the slab's values from the rebuild, telling `f` each run:
/// `Visitor::discard` for RLE slabs.
#[inline(always)]
pub(crate) fn discard<'s, L, G>(e: &mut RleRebuild<L>, s: &'s Slab, n: usize, f: &mut G)
    where L: for<'t> Lift<'t, W = L>, G: Visit<VOf<'s, L>>,
    requires
        RleModel::<L>::rb_inv(*old(e)),
        RleModel::<L>::ok(*s),
        RleModel::<L>::rb_data(*old(e)) == s.data@,
        RleModel::<L>::rb_read(*old(e)) + n <= s.len,
    ensures
        RleModel::<L>::rb_inv(*final(e)),
        RleModel::<L>::rb_data(*final(e)) == s.data@,
        RleModel::<L>::rb_read(*final(e)) == RleModel::<L>::rb_read(*old(e)) + n,
        RleModel::<L>::rb_vals(*final(e)) == RleModel::<L>::rb_vals(*old(e)),
{
    let ghost v0 = e.e.vals();
    proof {
        e.e.lemma_read();
    }
    e.e.walk::<L, G>(s.data.as_slice(), n, false, usize::MAX, f);
    assert(e.e.vals() =~= v0);
}

} // verus!

// Outside `verus!`: a closure is a visitor that calls itself.
impl<V, F: FnMut(V, usize)> Visit<V> for F {
    #[inline(always)]
    fn visit(&mut self, v: V, n: usize) {
        self(v, n)
    }
}

/// A rebuild in progress within one RLE slab, verified with Verus (`Edit`,
/// and what the column cursor does with it: `RleModel`).
pub type RleEdit<T, C> = RleRebuild<WOps<T, T, C>>;

impl<M: WriteOps<'static>> Default for RleRebuild<M> {
    fn default() -> Self {
        RleRebuild { e: Edit::new() }
    }
}

impl<T: RleValue, C: Codec> RleEdit<T, C> {
    /// An idle rebuild pointed at item `at`, for tests with no cursor to
    /// hold one across slabs.
    #[cfg(test)]
    pub(crate) fn open(slab: &Slab, at: usize) -> Self {
        let mut e = Self::default();
        e.reset(slab, at, usize::MAX);
        e
    }
}

// Forwards to the verified RLE slab model; the column cursor calls the
// model directly, and reaches `pass`, `insert` and `delete` through here.
impl<T: RleValue, C: Codec> EditSlab for RleEdit<T, C> {
    type Tail = RleTail;
    type Value = T;
    type State = RleDecoderState;

    fn reset(&mut self, slab: &Slab, at: usize, max_segments: usize) {
        <RleModel<WOps<T, T, C>> as Slabs>::reset(self, slab, at, max_segments)
    }

    /// Carry `n` of the slab's items through the rebuild unchanged,
    /// spending at most `runs` pushes. Returns what it declined to carry;
    /// closing the rebuild is the only sensible answer to that.
    ///
    /// The budget is in runs because that is what the work is: carrying a
    /// 10,000-item run costs one push, ten literals cost ten.
    fn pass<F>(&mut self, slab: &Slab, n: usize, runs: usize, f: &mut F) -> usize
    where
        F: FnMut(T::Get<'_>, usize),
    {
        pass::<WOps<T, T, C>, F>(self, slab, n, runs, f)
    }

    /// Insert `n` copies of `value` at the cursor.
    fn insert<V: AsColumnRef<T>>(&mut self, value: V, n: usize) {
        if n == 0 {
            return;
        }
        let value = T::to_owned(value.as_column_ref());
        self.e.insert_cow(RleCow::Owned(value), n);
    }

    /// Drop `n` of the slab's items at the cursor.
    fn delete<F>(&mut self, slab: &Slab, n: usize, f: &mut F)
    where
        F: FnMut(T::Get<'_>, usize),
    {
        debug_assert!(n <= slab.len, "delete past the end of the slab");
        discard::<WOps<T, T, C>, F>(self, slab, n, f)
    }

    fn read_state(&self) -> &RleDecoderState {
        self.e.dec()
    }

    /// Finish: carry the slab's remaining bytes through wholesale, write
    /// the rebuilt region back, and return any slabs the rebuild spilled
    /// into (to be inserted after this one).
    fn close(&mut self, slab: &mut Slab) -> Vec<Slab> {
        <RleModel<WOps<T, T, C>> as Slabs>::close(self, slab)
    }
}

/// The slab-level operations a column cursor drives. Implemented here
/// for the RLE encoding; an encoding without an implementation simply
/// has no cursor (nothing outside this module requires one yet).
impl<T, C> SlabEdit for crate::rle::RleEncoding<T, C>
where
    T: RleValue,
    C: Codec,
    T: crate::ColumnValueRef<Encoding<C> = Self>,
{
    type Edit = RleEdit<T, C>;
    type Model = RleModel<WOps<T, T, C>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::Leb128;
    use crate::encoding::EncoderApi;
    use crate::rle::RleDecoder;
    use crate::Encoder;

    fn slab_of(vals: &[u64]) -> Slab {
        Encoder::<u64>::encode_slab(vals.iter().copied())
    }

    fn values(slab: &Slab) -> Vec<u64> {
        RleDecoder::<u64, Leb128>::new(&slab.data).collect()
    }

    /// open → pass → insert → close, against the same edit expressed as
    /// a plain `Vec` splice.
    fn check(input: &[u64], script: impl Fn(&mut RleEdit<u64, Leb128>, &Slab), want: Vec<u64>) {
        let mut slab = slab_of(input);
        let orig = slab.clone();
        let mut e = RleEdit::<u64, Leb128>::open(&orig, 0);
        script(&mut e, &orig);
        e.close(&mut slab);
        assert_eq!(values(&slab), want, "values");
        assert_eq!(slab.len, want.len(), "len");
        // the encoding must be canonical: re-encoding the same values
        // from scratch produces the same bytes
        let fresh = slab_of(&want);
        assert_eq!(slab.data, fresh.data, "bytes");
        assert_eq!(slab.segments, fresh.segments, "segments");
    }

    #[test]
    fn insert_at_front() {
        check(&[1, 1, 2, 3], |e, _s| e.insert(9u64, 1), {
            let mut v = vec![9u64];
            v.extend([1, 1, 2, 3]);
            v
        });
    }

    #[test]
    fn pass_then_insert() {
        check(
            &[1, 1, 2, 3],
            |e, s| {
                e.pass(s, 2, usize::MAX, &mut |_, _| {});
                e.insert(9u64, 1);
            },
            vec![1, 1, 9, 2, 3],
        );
    }

    #[test]
    fn two_inserts_one_pass() {
        check(
            &[1, 1, 1, 1, 1, 1],
            |e, s| {
                e.pass(s, 1, usize::MAX, &mut |_, _| {});
                e.insert(7u64, 1);
                e.pass(s, 2, usize::MAX, &mut |_, _| {});
                e.insert(8u64, 2);
            },
            vec![1, 7, 1, 1, 8, 8, 1, 1, 1],
        );
    }

    #[test]
    fn delete_then_insert() {
        check(
            &[1, 2, 3, 4, 5],
            |e, s| {
                e.pass(s, 1, usize::MAX, &mut |_, _| {});
                e.delete(s, 2, &mut |_, _| {});
                e.insert(9u64, 1);
            },
            vec![1, 9, 4, 5],
        );
    }

    #[test]
    fn extends_a_run() {
        check(
            &[5, 5, 5, 5],
            |e, s| {
                e.pass(s, 2, usize::MAX, &mut |_, _| {});
                e.insert(5u64, 3);
            },
            vec![5, 5, 5, 5, 5, 5, 5],
        );
    }

    #[test]
    fn append_at_end() {
        let mut slab = slab_of(&[1, 2, 3]);
        let orig = slab.clone();
        let mut e = RleEdit::<u64, Leb128>::open(&orig, 3);
        e.insert(4u64, 1);
        e.close(&mut slab);
        assert_eq!(values(&slab), vec![1, 2, 3, 4]);
    }

    #[test]
    fn delete_only() {
        check(
            &[1, 2, 3, 4],
            |e, s| {
                e.pass(s, 1, usize::MAX, &mut |_, _| {});
                e.delete(s, 2, &mut |_, _| {});
            },
            vec![1, 4],
        );
    }

    /// Every single edit must land on the canonical encoding of the values
    /// it leaves: bytes, length and segments, as encoding them afresh would.
    #[test]
    fn matches_the_canonical_encoding() {
        let input: Vec<u64> = vec![1, 1, 1, 2, 3, 3, 4, 5, 5, 5, 6, 7, 8, 8];
        for at in 0..=input.len() {
            for del in 0..=(input.len() - at).min(3) {
                for ins in [vec![], vec![9u64], vec![9, 9], vec![3, 3, 3]] {
                    let mut a = slab_of(&input);
                    let orig = a.clone();
                    let mut e = RleEdit::<u64, Leb128>::open(&orig, at);
                    e.delete(&orig, del, &mut |_, _| {});
                    for v in &ins {
                        e.insert(*v, 1);
                    }
                    e.close(&mut a);

                    let mut want = input.clone();
                    want.splice(at..at + del, ins.iter().copied());
                    let b = slab_of(&want);
                    assert_eq!(values(&a), want, "values at={at} del={del} ins={ins:?}");
                    assert_eq!(a.data, b.data, "bytes at={at} del={del} ins={ins:?}");
                    assert_eq!(a.len, b.len, "len at={at} del={del} ins={ins:?}");
                    assert_eq!(
                        a.segments, b.segments,
                        "segments at={at} del={del} ins={ins:?}"
                    );
                }
            }
        }
    }
}
