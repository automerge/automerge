//! The RLE column encoder, verified with Verus.
//!
//! [`REncoder`] appends values with the RLE writer ([`State`]) into a
//! buffer of its own. Given a segment budget, it cuts that buffer into
//! slabs as it goes. Every slab it cuts, or hands out whole, is a
//! well-formed slab of its share of the values (`RleModel::ok`). Together
//! the slabs and the buffer hold the values appended, in order. The bytes
//! it hands out unsliced are the canonical encoding of the values since
//! the last cut.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

#[cfg(verus_keep_ghost)]
use crate::cursor::{all_ok, lemma_vals_cat, lemma_vals_unit, vals_of, Slabs};
#[cfg(verus_keep_ghost)]
use crate::rle::dec::lemma_group_values_len;
#[cfg(verus_keep_ghost)]
use crate::rle::edit::lemma_written;
use crate::rle::edit::null_in_column;
use crate::rle::edit::RleModel;
use crate::rle::lift::*;
use crate::rle::model::*;
#[cfg(verus_keep_ghost)]
use crate::rle::postfix::*;
use crate::rle::state::*;
#[cfg(verus_keep_ghost)]
use crate::rle::writer::*;
use crate::rle::writer::{merge_tail, new_slab, tail_rec};
use crate::rle::Slab;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// Appending would take an encoder past `usize::MAX - 2` values, which
/// it can't count.  Trusted only in that a panic does not return.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn too_many_values(Ghost(why): Ghost<bool>)
    requires
        why,
    ensures
        false,
{
    panic!("encoder too long to append to")
}

/// A state the writer may go on from: what it wrote from empty into
/// `buf` from `base`, standing for `vals` and accounting `segs` segments.
pub(crate) open spec fn written<'a, W: WriteOps<'a>>(s: State<'a, W>, buf: Seq<u8>, base: int, vals: Seq<VOf<'a, W>>, segs: int) -> bool {
    &&& wf(s, buf)
    &&& !pre_lit(s)
    &&& exists|gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>| #[trigger] rel(s, Seq::empty(), buf, base, gs, ws, 0)
        && values_of(s, gs, ws) == vals && acc(s, gs, ws) == segs
}

/// The writer from empty: nothing written after `buf`.
proof fn lemma_written_empty<'a, W: WriteOps<'a>>(buf: Seq<u8>)
    ensures written(State::<'a, W>::Empty, buf, buf.len() as int, Seq::empty(), 0),
{
    let eg = Seq::<Group<VOf<'a, W>>>::empty();
    let ev = Seq::<VOf<'a, W>>::empty();
    assert(buf.skip(buf.len() as int) =~= Seq::<u8>::empty());
    assert(groups_values(eg, <W::R as RleOps<'a>>::null()) =~= ev);
    assert(rel(State::<'a, W>::Empty, Seq::empty(), buf, buf.len() as int, eg, ev, 0));
}

/// What a state has written: the pending values are among them, as are
/// the segments accounted and those a flush adds.
proof fn lemma_written_room<'a, W: WriteOps<'a>>(s: State<'a, W>, buf: Seq<u8>, base: int, vals: Seq<VOf<'a, W>>, segs: int)
    requires written(s, buf, base, vals, segs),
    ensures
        s.plen() <= vals.len(),
        0 <= segs, segs + s.pseg() <= vals.len(),
        0 <= base <= buf.len(),
        vals.len() == 0 ==> s is Empty && buf.len() == base,
{
    let (gs, ws) = choose|gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>| #[trigger] rel(s, Seq::empty(), buf, base, gs, ws, 0)
        && values_of(s, gs, ws) == vals && acc(s, gs, ws) == segs;
    lemma_acc::<W>(s, Seq::empty(), buf, base, gs, ws, 0);
    if !(s is Empty) {
        let g = pend_group(s, ws)->Some_0;
        lemma_push::<W::R>(gs, g);
        lemma_group_values_len(g, <W::R as RleOps<'a>>::null());
    }
}

/// Owned copies, as many as the values.
proof fn lemma_lift_len<'s, L: Lift<'s>>(vs: Seq<VOf<'s, L>>)
    ensures lift_seq::<L>(vs).len() == vs.len(),
{
}

/// The writer's state and output buffer, and the slabs cut from it.
#[doc(hidden)]
pub struct REncoder<'a, L: WriteOps<'a>> {
    pub(crate) data: Vec<u8>,
    pub(crate) state: State<'a, L>,
    /// segments accounted since the last cut
    pub(crate) segments: usize,
    /// values the encoder holds: in `slabs`, and since the last cut
    pub(crate) len: usize,
    /// slabs cut; only with a segment budget
    pub(crate) slabs: Vec<Slab>,
    pub(crate) max_segments: Option<usize>,
    /// values appended before the last cut
    pub(crate) len_flushed: usize,
    /// the values appended since the last cut
    #[allow(dead_code)]
    pub(crate) cur: Ghost<Seq<VOf<'a, L>>>,
}

impl<'a, L> REncoder<'a, L> where L: for<'x> Lift<'x, W = L> {
    /// All of [`inv`](Self::inv) but the count of values appended.
    pub(crate) open spec fn inv_parts(&self) -> bool {
        &&& written(self.state, self.data@, 0, self.cur@, self.segments as int)
        &&& all_ok::<RleModel<L>>(self.slabs@)
        &&& vals_of::<RleModel<L>>(self.slabs@).len() == self.len_flushed
        &&& self.len + 2 <= usize::MAX
    }

    pub(crate) open spec fn inv(&self) -> bool {
        &&& self.inv_parts()
        &&& self.len == self.len_flushed + self.cur@.len()
    }

    /// The values the encoder holds, owned: the slabs', then those since
    /// the last cut.
    pub(crate) open spec fn values(&self) -> Seq<VOf<'static, L>> {
        vals_of::<RleModel<L>>(self.slabs@) + lift_seq::<L>(self.cur@)
    }

    pub(crate) fn new() -> (e: Self)
        ensures e.inv(), e.values().len() == 0, e.cur@.len() == 0, e.slabs@.len() == 0, e.max_segments is None,
    {
        let e = REncoder {
            data: Vec::new(),
            state: State::empty(),
            segments: 0,
            len: 0,
            slabs: Vec::new(),
            max_segments: None,
            len_flushed: 0,
            cur: Ghost(Seq::empty()),
        };
        proof {
            lemma_written_empty::<'a, L>(Seq::empty());
            assert(e.data@ =~= Seq::<u8>::empty());
            lemma_vals_unit::<RleModel<L>>(arbitrary());
            assert(lift_seq::<L>(e.cur@) =~= Seq::<VOf<'static, L>>::empty());
        }
        e
    }

    /// Flush, and take what was written since the last cut as a slab,
    /// whose values the caller accounts for.
    /// `presize`: give the next buffer the capacity of this one.
    #[inline]
    fn seal(&mut self, presize: bool) -> (s: Slab)
        requires old(self).inv(),
        ensures
            final(self).inv_parts(), final(self).cur@.len() == 0, final(self).slabs@ == old(self).slabs@,
            final(self).len == old(self).len, final(self).len_flushed == old(self).len_flushed,
            final(self).max_segments == old(self).max_segments,
            RleModel::<L>::ok(s), RleModel::<L>::vals(s) == lift_seq::<L>(old(self).cur@),
            encodes::<<L as WriteOps<'a>>::R>(slab_bytes(s), old(self).cur@, slab_segments(s) as int),
    {
        let ghost s0 = self.state;
        let ghost b0 = self.data@;
        let ghost cur0 = self.cur@;
        let ghost (gs, ws) = choose|gs: Seq<Group<VOf<'a, L>>>, ws: Seq<VOf<'a, L>>| #[trigger] rel(s0, Seq::empty(), b0, 0, gs, ws, 0)
            && values_of(s0, gs, ws) == cur0 && acc(s0, gs, ws) == self.segments;
        proof {
            lemma_written_room::<L>(s0, b0, 0, cur0, self.segments as int);
        }
        let f = self.state.flush(&mut self.data);
        let segments = self.segments + f.segments;
        let len = self.len - self.len_flushed;
        let tail0 = tail_rec(0, 0);
        let tail = merge_tail(f.wpos, 0, self.data.len(), 0, &tail0);
        // successive slabs are similar-sized: pre-size the next buffer
        // (one allocation per slab, like load)
        let capacity = if presize { self.data.len() } else { 0 };
        let mut data = Vec::with_capacity(capacity);
        core::mem::swap(&mut self.data, &mut data);
        let s = new_slab(data, len, segments, tail);
        self.segments = 0;
        proof {
            let eb = Seq::<u8>::empty();
            let b1 = slab_bytes(s);
            // a state written from empty has no header pending in the slab
            assert(f.rewrite is None);
            assert(b1.skip(0) =~= b1);
            assert(eb + b1 =~= b1);
            lemma_acc::<L>(s0, eb, b0, 0, gs, ws, 0);
            assert(encodes::<<L as WriteOps<'a>>::R>(b1, cur0, segments as int));
            lemma_flush_tail::<L>(s0, eb, b0, gs, ws, 0, b1, f.rewrite, f.segments as int, f.wpos);
            let t = tail_after(f.wpos, 0, 0, b1.len() as int, 0, tail_b(tail0) as int, tail_l(tail0) as int);
            assert(tail_ok::<<L as WriteOps<'a>>::R>(b1, t.0, t.1));
            lemma_written::<L>(s, cur0, f.wpos, tail_b(tail0) as int, tail_l(tail0) as int);
            self.cur = Ghost(Seq::empty());
            lemma_written_empty::<'a, L>(Seq::empty());
            assert(self.data@ =~= Seq::<u8>::empty());
        }
        s
    }

    /// Cut the slab in hand, if it holds any values.
    fn cut(&mut self, presize: bool)
        requires old(self).inv(),
        ensures
            final(self).inv(), final(self).values() == old(self).values(), final(self).len == old(self).len,
            final(self).cur@.len() == 0, final(self).max_segments == old(self).max_segments,
    {
        if self.len == self.len_flushed {
            proof {
                lemma_written_room::<L>(self.state, self.data@, 0, self.cur@, self.segments as int);
                assert(self.cur@ =~= Seq::<VOf<'a, L>>::empty());
            }
            return;
        }
        let ghost s0 = self.slabs@;
        let ghost v0 = old(self).values();
        let s = self.seal(presize);
        self.slabs.push(s);
        self.len_flushed = self.len;
        proof {
            assert forall|i: int| 0 <= i < self.slabs@.len() implies #[trigger] RleModel::<L>::ok(self.slabs@[i]) by {
                if i < s0.len() {
                    assert(self.slabs@[i] == s0[i]);
                }
            }
            lemma_vals_cat::<RleModel<L>>(s0, seq![s]);
            lemma_vals_unit::<RleModel<L>>(s);
            assert(s0 + seq![s] =~= self.slabs@);
            lemma_lift_len::<L>(old(self).cur@);
            assert(lift_seq::<L>(self.cur@) =~= Seq::<VOf<'static, L>>::empty());
            assert(self.values() =~= v0);
        }
    }

    /// Cut the slab in hand once the segment budget is spent (no-op
    /// without a budget). Cuts at half the budget (half-full slabs) to
    /// mirror `load`'s split policy, leaving room for later inserts.
    #[inline]
    fn maybe_cut(&mut self)
        requires old(self).inv(),
        ensures
            final(self).inv(), final(self).values() == old(self).values(), final(self).len == old(self).len,
            final(self).cur@.len() <= old(self).cur@.len(), final(self).max_segments == old(self).max_segments,
    {
        let max = match self.max_segments {
            Some(m) => m,
            None => return,
        };
        let target = max / 2;
        proof {
            lemma_written_room::<L>(self.state, self.data@, 0, self.cur@, self.segments as int);
        }
        if target == 0 || self.segments + self.state.pending_segments() < target {
            return;
        }
        self.cut(true);
    }

    /// Append `n` copies of `value`.
    #[inline(always)]
    pub(crate) fn append(&mut self, value: <L as WriteOps<'a>>::Cow, n: usize)
        requires old(self).inv(),
        ensures
            final(self).inv(),
            final(self).values() == old(self).values() + lift_seq::<L>(copies(n as nat, <L as WriteOps<'a>>::val(value))),
            final(self).len == old(self).len + n, final(self).max_segments == old(self).max_segments,
    {
        if n > usize::MAX - 2 - self.len {
            too_many_values(Ghost(true));
        }
        if <L as WriteOps<'a>>::cow_is_null(&value) && !<<L as WriteOps<'a>>::R as RleOps<'a>>::nullable_exec() {
            null_in_column(Ghost(true));
        }
        self.maybe_cut();
        let ghost s0 = self.state;
        let ghost b0 = self.data@;
        let ghost cur0 = self.cur@;
        let ghost v0 = self.values();
        let ghost (gs, ws) = choose|gs: Seq<Group<VOf<'a, L>>>, ws: Seq<VOf<'a, L>>| #[trigger] rel(s0, Seq::empty(), b0, 0, gs, ws, 0)
            && values_of(s0, gs, ws) == cur0 && acc(s0, gs, ws) == self.segments;
        proof {
            lemma_written_room::<L>(s0, b0, 0, cur0, self.segments as int);
        }
        let f = self.state.append_cow(&mut self.data, value, n);
        let ghost added = copies(n as nat, <L as WriteOps<'a>>::val(value));
        proof {
            // a state written from empty has no header pending in the slab
            assert(f.rewrite is None);
            let (gs2, ws2) = choose|gs2: Seq<Group<VOf<'a, L>>>, ws2: Seq<VOf<'a, L>>| #[trigger] rel(self.state, apply::<L>(f.rewrite, Seq::empty(), 0), self.data@, 0, gs2, ws2, 0)
                && values_of(self.state, gs2, ws2) == values_of(s0, gs, ws) + added
                && acc(self.state, gs2, ws2) == acc(s0, gs, ws) + f.segments;
            assert(rel(self.state, Seq::empty(), self.data@, 0, gs2, ws2, 0));
            self.cur = Ghost(cur0 + added);
            lemma_written_room::<L>(self.state, self.data@, 0, self.cur@, acc(self.state, gs2, ws2));
        }
        self.segments = self.segments + f.segments;
        self.len = self.len + n;
        proof {
            assert(lift_seq::<L>(cur0 + added) =~= lift_seq::<L>(cur0) + lift_seq::<L>(added));
            assert(self.values() =~= v0 + lift_seq::<L>(added));
        }
    }

    /// Flush, and take the bytes written since the last cut: the
    /// canonical encoding of the values since, which are all the values
    /// if there was no cut.
    pub(crate) fn take_bytes(&mut self) -> (r: Vec<u8>)
        requires old(self).inv(),
        ensures
            final(self).inv(), final(self).slabs@ == old(self).slabs@,
            exists|segs: int| encodes::<<L as WriteOps<'a>>::R>(r@, old(self).cur@, segs),
            old(self).slabs@.len() == 0 ==> lift_seq::<L>(old(self).cur@) == old(self).values(),
    {
        proof {
            lemma_vals_unit::<RleModel<L>>(arbitrary());
            assert(Seq::<VOf<'static, L>>::empty() + lift_seq::<L>(old(self).cur@) =~= lift_seq::<L>(old(self).cur@));
        }
        let s = self.seal(false);
        self.len = self.len_flushed;
        s.data
    }

    /// Flush, and take what was written since the last cut as one slab:
    /// all the values if there was no cut.
    pub(crate) fn take_slab(&mut self) -> (s: Slab)
        requires old(self).inv(),
        ensures
            final(self).inv(), final(self).slabs@ == old(self).slabs@,
            RleModel::<L>::ok(s), RleModel::<L>::vals(s) == lift_seq::<L>(old(self).cur@),
            old(self).slabs@.len() == 0 ==> RleModel::<L>::vals(s) == old(self).values(),
    {
        proof {
            lemma_vals_unit::<RleModel<L>>(arbitrary());
            assert(Seq::<VOf<'static, L>>::empty() + lift_seq::<L>(old(self).cur@) =~= lift_seq::<L>(old(self).cur@));
        }
        let s = self.seal(false);
        self.len = self.len_flushed;
        s
    }

    /// Flush, and take every slab: those cut, then the last. Their item
    /// count is the encoder's `len`.
    pub(crate) fn take_slabs(&mut self) -> (v: Vec<Slab>)
        requires old(self).inv(),
        ensures
            final(self).inv(),
            all_ok::<RleModel<L>>(v@), vals_of::<RleModel<L>>(v@) == old(self).values(),
            crate::fenwick::total(crate::cursor::slab_lens(v@)) == old(self).len,
    {
        self.cut(false);
        let mut v = Vec::new();
        core::mem::swap(&mut self.slabs, &mut v);
        proof {
            assert(vals_of::<RleModel<L>>(v@) + Seq::<VOf<'static, L>>::empty() =~= vals_of::<RleModel<L>>(v@));
            assert(lift_seq::<L>(self.cur@) =~= Seq::<VOf<'static, L>>::empty());
            crate::cursor::lemma_vals_total::<RleModel<L>>(v@);
            lemma_vals_unit::<RleModel<L>>(arbitrary());
        }
        self.len_flushed = 0;
        self.len = 0;
        v
    }

    /// True only if every value appended is `value` (or there are none).
    #[inline]
    pub(crate) fn single_run_of(&self, value: &<L as WriteOps<'a>>::Cow) -> (r: bool)
        requires self.inv(),
        ensures
            r ==> self.slabs@.len() == 0
                && self.cur@ == copies(self.cur@.len(), <L as WriteOps<'a>>::val(*value)),
    {
        let r = self.slabs.len() == 0 && self.segments == 0 && self.state.single_run_of(value);
        proof {
            if r {
                lemma_single_run::<L>(self.state, self.data@, 0, self.cur@, *value);
            }
        }
        r
    }
}

/// The RLE encoder as `remap` drives it (`remap::Encodes`): what it
/// appends is a value in the writer's form, owned or borrowed.
impl<'a, L> crate::remap::Encodes<RleModel<L>> for REncoder<'a, L> where L: for<'x> Lift<'x, W = L> {
    type In = <L as WriteOps<'a>>::Cow;

    open spec fn e_inv(&self) -> bool {
        self.inv()
    }

    open spec fn e_values(&self) -> Seq<VOf<'static, L>> {
        self.values()
    }

    open spec fn val(x: <L as WriteOps<'a>>::Cow) -> VOf<'static, L> {
        <L as Lift<'a>>::lift(<L as WriteOps<'a>>::val(x))
    }

    #[inline(always)]
    fn with_budget(max: usize) -> (e: Self) {
        let mut e = REncoder::new();
        e.max_segments = Some(max);
        e
    }

    #[inline(always)]
    fn put(&mut self, x: <L as WriteOps<'a>>::Cow, n: usize) {
        self.append(x, n);
        proof {
            assert(lift_seq::<L>(copies(n as nat, <L as WriteOps<'a>>::val(x))) =~= Seq::new(
                n as nat,
                |_i: int| <L as Lift<'a>>::lift(<L as WriteOps<'a>>::val(x)),
            ));
        }
    }

    #[inline(always)]
    fn take(&mut self) -> (v: Vec<Slab>) {
        self.take_slabs()
    }
}

/// The RLE writer appending to a caller's buffer, after the bytes the
/// buffer held when the first value came (`head`): `encode_to` and
/// `RleEncoderState`.
pub(crate) struct RStream<'a, L: WriteOps<'a>> {
    pub(crate) state: State<'a, L>,
    /// segments accounted
    pub(crate) segments: usize,
    /// values appended
    pub(crate) len: usize,
    /// the caller's bytes before the stream's, and the values appended
    #[allow(dead_code)]
    pub(crate) head: Ghost<Seq<u8>>,
    #[allow(dead_code)]
    pub(crate) cur: Ghost<Seq<VOf<'a, L>>>,
}

impl<'a, L: WriteOps<'a>> RStream<'a, L> {
    /// Nothing appended yet: the stream may start on any buffer.
    pub(crate) open spec fn fresh(&self) -> bool {
        &&& self.state is Empty && self.segments == 0 && self.len == 0 && self.cur@.len() == 0
    }

    /// The caller's bytes in `buf`, before the stream's.
    pub(crate) open spec fn head_of(&self, buf: Seq<u8>) -> Seq<u8> {
        if self.fresh() { buf } else { self.head@ }
    }

    /// The stream, writing into `buf`.
    pub(crate) open spec fn on(&self, buf: Seq<u8>) -> bool {
        &&& self.len == self.cur@.len() && self.len + 2 <= usize::MAX
        &&& self.fresh() || {
            &&& written(self.state, buf, self.head@.len() as int, self.cur@, self.segments as int)
            &&& buf.take(self.head@.len() as int) == self.head@
        }
    }

    pub(crate) fn new() -> (r: Self)
        ensures r.fresh(), forall|buf: Seq<u8>| r.on(buf),
    {
        RStream { state: State::empty(), segments: 0, len: 0, head: Ghost(Seq::empty()), cur: Ghost(Seq::empty()) }
    }

    /// Append `n` copies of `value`.
    #[inline(always)]
    pub(crate) fn append(&mut self, buf: &mut Vec<u8>, value: L::Cow, n: usize)
        requires old(self).on(old(buf)@),
        ensures
            final(self).on(final(buf)@),
            final(self).head_of(final(buf)@) == old(self).head_of(old(buf)@),
            final(self).cur@ == old(self).cur@ + copies(n as nat, L::val(value)),
            final(self).len == old(self).len + n,
    {
        if n > usize::MAX - 2 - self.len {
            too_many_values(Ghost(true));
        }
        if L::cow_is_null(&value) && !<L::R as RleOps<'a>>::nullable_exec() {
            null_in_column(Ghost(true));
        }
        let ghost h = self.head_of(buf@);
        let ghost s0 = self.state;
        let ghost b0 = buf@;
        let ghost cur0 = self.cur@;
        let ghost base = h.len() as int;
        proof {
            if self.fresh() {
                lemma_written_empty::<'a, L>(b0);
                assert(b0.take(base) =~= b0);
                assert(self.cur@ =~= Seq::<VOf<'a, L>>::empty());
            }
            lemma_written_room::<L>(s0, b0, base, cur0, self.segments as int);
        }
        let ghost (gs, ws) = choose|gs: Seq<Group<VOf<'a, L>>>, ws: Seq<VOf<'a, L>>| #[trigger] rel(s0, Seq::empty(), b0, base, gs, ws, 0)
            && values_of(s0, gs, ws) == cur0 && acc(s0, gs, ws) == self.segments;
        let f = self.state.append_cow(buf, value, n);
        let ghost added = copies(n as nat, L::val(value));
        proof {
            // a state written from empty has no header pending in the slab
            assert(f.rewrite is None);
            let (gs2, ws2) = choose|gs2: Seq<Group<VOf<'a, L>>>, ws2: Seq<VOf<'a, L>>| #[trigger] rel(self.state, apply::<L>(f.rewrite, Seq::empty(), 0), buf@, base, gs2, ws2, 0)
                && values_of(self.state, gs2, ws2) == values_of(s0, gs, ws) + added
                && acc(self.state, gs2, ws2) == acc(s0, gs, ws) + f.segments;
            assert(rel(self.state, Seq::empty(), buf@, base, gs2, ws2, 0));
            self.cur = Ghost(cur0 + added);
            self.head = Ghost(h);
            lemma_written_room::<L>(self.state, buf@, base, self.cur@, acc(self.state, gs2, ws2));
            // the caller's bytes are before the writer's
            assert(base <= wlo(s0, b0));
            assert(buf@.take(base) =~= h);
        }
        self.segments = self.segments + f.segments;
        self.len = self.len + n;
        proof {
            if n == 0 && old(self).fresh() {
                assert(self.cur@ =~= Seq::<VOf<'a, L>>::empty());
                assert(buf@ == b0);
            }
        }
    }

    /// Flush: the bytes after the caller's are the canonical encoding of
    /// the values appended.
    #[allow(unused_variables)] // the flush's result is for the proof
    pub(crate) fn finish(&mut self, buf: &mut Vec<u8>)
        requires old(self).on(old(buf)@),
        ensures ({
            let h = old(self).head_of(old(buf)@);
            &&& h.len() <= final(buf)@.len()
            &&& final(buf)@.take(h.len() as int) == h
            &&& exists|segs: int| encodes::<L::R>(final(buf)@.skip(h.len() as int), old(self).cur@, segs)
        }),
    {
        let ghost h = self.head_of(buf@);
        let ghost s0 = self.state;
        let ghost b0 = buf@;
        let ghost base = h.len() as int;
        proof {
            if self.fresh() {
                lemma_written_empty::<'a, L>(b0);
                assert(b0.take(base) =~= b0);
                assert(self.cur@ =~= Seq::<VOf<'a, L>>::empty());
            }
            lemma_written_room::<L>(s0, b0, base, self.cur@, self.segments as int);
        }
        let ghost (gs, ws) = choose|gs: Seq<Group<VOf<'a, L>>>, ws: Seq<VOf<'a, L>>| #[trigger] rel(s0, Seq::empty(), b0, base, gs, ws, 0)
            && values_of(s0, gs, ws) == self.cur@ && acc(s0, gs, ws) == self.segments;
        let f = self.state.flush(buf);
        proof {
            assert(f.rewrite is None);
            lemma_acc::<L>(s0, Seq::empty(), b0, base, gs, ws, 0);
            let x = buf@.skip(base);
            assert(Seq::<u8>::empty() + x =~= x);
            assert(encodes::<L::R>(x, old(self).cur@, groups_segments(all_groups(s0, gs, ws)) as int));
            assert(base <= wlo(s0, b0));
            assert(buf@.take(base) =~= h);
        }
    }

    /// True only if every value appended is `value` (or there are none).
    #[inline]
    pub(crate) fn single_run_of(&self, value: &L::Cow) -> (r: bool)
        requires exists|buf: Seq<u8>| self.on(buf),
        ensures r ==> self.cur@ == copies(self.cur@.len(), L::val(*value)),
    {
        let r = self.segments == 0 && self.state.single_run_of(value);
        proof {
            if r {
                let buf = choose|buf: Seq<u8>| self.on(buf);
                if self.fresh() {
                    assert(self.cur@ =~= copies(0, L::val(*value)));
                } else {
                    lemma_single_run::<L>(self.state, buf, self.head@.len() as int, self.cur@, *value);
                }
            }
        }
        r
    }
}

/// A state written from empty that has emitted nothing and holds one
/// run of `value`, or nothing, stands for copies of `value`.
proof fn lemma_single_run<'a, W: WriteOps<'a>>(s: State<'a, W>, buf: Seq<u8>, base: int, vals: Seq<VOf<'a, W>>, value: W::Cow)
    requires
        written(s, buf, base, vals, 0),
        match s {
            State::Empty => true,
            State::Lone(v) => W::val(v) == W::val(value),
            State::Run(_, v) => W::val(v) == W::val(value),
            State::Null(_) => <W::R as RleOps<'a>>::is_null(W::val(value)),
            State::Lit { .. } => false,
        },
    ensures vals == copies(vals.len(), W::val(value)),
{
    let (gs, ws) = choose|gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>| #[trigger] rel(s, Seq::empty(), buf, base, gs, ws, 0)
        && values_of(s, gs, ws) == vals && acc(s, gs, ws) == 0;
    let null = <W::R as RleOps<'a>>::null();
    // nothing emitted: every group emitted counts a segment
    if gs.len() > 0 {
        let a = all_groups(s, gs, ws);
        if !(s is Empty) {
            lemma_canonical_split::<W::R>(gs, pend_group(s, ws)->Some_0);
        }
        assert(group_ok::<W::R>(gs[gs.len() - 1]));
        assert(groups_segments(gs) >= group_segments(gs.last()));
    }
    assert(gs =~= Seq::<Group<VOf<'a, W>>>::empty());
    if !(s is Empty) {
        let g = pend_group(s, ws)->Some_0;
        lemma_push::<W::R>(gs, g);
        assert(groups_values(gs, null) =~= Seq::<VOf<'a, W>>::empty());
        assert(group_ok::<W::R>(all_groups(s, gs, ws)[0]));
        if s is Null {
            <W::R as RleOps<'a>>::axiom_null(W::val(value));
        }
    }
    assert(vals =~= copies(vals.len(), W::val(value)));
}

} // verus!
