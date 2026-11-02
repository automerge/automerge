//! The rebuild an edit drives, verified with Verus.
//!
//! A [`SpliceWriter`] re-encodes a region of one slab: it opens on the
//! writer state for the slab's values before the edit point, takes the
//! values the edit carries through or inserts, and closes on the postfix
//! (the run after the edit) and the slab's bytes after it. When the
//! region outgrows the slab's segment budget it cuts, and the rest goes
//! into new slabs. What [`finish`](Splice::finish) returns, written back
//! by [`Rebuilt::apply`], leaves the edited slab and each new one the
//! canonical encoding of its share of the values, which run on from one
//! slab to the next ([`rebuilt_ok`]).
//!
//! Only bytes and values are specified. The slabs' lengths, segment
//! counts and tail records are computed as before, unspecified.

// Verus proofs read more plainly with `x = x + n`.
#![allow(
    clippy::len_zero,
    clippy::assign_op_pattern,
    clippy::too_many_arguments
)]

// spec functions exist only when Verus compiles the crate
#[cfg(verus_keep_ghost)]
use crate::rle::dec::*;
use crate::rle::model::*;
use crate::rle::postfix::*;
use crate::rle::state::*;
use crate::rle::{RleTail, Slab};
use crate::{Codec, RleValue};
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

#[verifier::external_type_specification]
#[verifier::external_body]
#[verifier::reject_recursive_types(T)]
#[allow(dead_code)]
pub struct ExSlab<T: Copy + Clone + core::fmt::Debug + Default>(crate::column::Slab<T>);

#[verifier::external_type_specification]
#[verifier::external_body]
#[allow(dead_code)]
pub struct ExRleTail(RleTail);

/// A slab's bytes.
pub uninterp spec fn slab_bytes(s: Slab) -> Seq<u8>;

/// Slab metadata arithmetic (lengths, segment counts): unspecified, and
/// computed exactly as before.
#[verifier::external_body]
#[inline(always)]
pub(crate) fn add(a: usize, b: usize) -> usize {
    a + b
}

/// The same, for `a - b`.
#[verifier::external_body]
#[inline(always)]
pub(crate) fn sub(a: usize, b: usize) -> usize {
    a - b
}

/// A slab the rebuild spilled into.
#[verifier::external_body]
#[inline(always)]
fn new_slab(data: Vec<u8>, len: usize, segments: usize, wpos: WPos) -> (s: Slab)
    ensures slab_bytes(s) == data@,
{
    let tail = wpos.as_tail(0, data.len());
    Slab { data, len, segments, tail }
}

/// The last slab the rebuild spilled into, which carries the edited
/// slab's bytes after the postfix.
#[verifier::external_body]
#[inline(always)]
fn last_slab(mut data: Vec<u8>, tail_bytes: &[u8], len: usize, segments: usize, wpos: WPos, slab_tail: RleTail) -> (s: Slab)
    ensures slab_bytes(s) == data@ + tail_bytes@,
{
    let tail = wpos.merge(0, data.len(), tail_bytes.len(), slab_tail);
    data.extend_from_slice(tail_bytes);
    Slab { data, len, segments, tail }
}

/// `vec[index..index + del] = bytes`.
#[verifier::external_body]
#[inline(always)]
fn splice_bytes(vec: &mut Vec<u8>, index: usize, del: usize, bytes: &[u8])
    requires index + del <= old(vec).len(),
    ensures final(vec)@ == old(vec)@.take(index as int) + bytes@ + old(vec)@.skip(index + del),
{
    crate::column::splice_bytes(vec, index, del, bytes)
}

/// The values of slabs one after another.
pub open spec fn concat<V>(cs: Seq<Seq<V>>) -> Seq<V>
    decreases cs.len(),
{
    if cs.len() == 0 { Seq::empty() } else { concat(cs.drop_last()) + cs.last() }
}

proof fn lemma_concat_push<V>(cs: Seq<Seq<V>>, c: Seq<V>)
    ensures concat(cs.push(c)) == concat(cs) + c, concat(Seq::<Seq<V>>::empty()) == Seq::<V>::empty(),
{
    assert(cs.push(c).drop_last() =~= cs);
}

/// Where a rewrite applies: the header of the slab's literal, `h0`, in
/// the bytes before the edit.
pub(crate) open spec fn rw_at<'a, W: WriteOps<'a>>(rw: Option<RewriteHeader>, pre: Seq<u8>, h0: i64) -> bool {
    rw is Some ==> {
        let r = rw->Some_0;
        &&& r.pos + <W::R as RleOps<'a>>::enc_signed(h0).len() <= pre.len()
        &&& <W::R as RleOps<'a>>::enc_signed(h0).is_prefix_of(pre.skip(r.pos as int))
        &&& 1 <= r.count <= i64::MAX
    }
}

/// A rewrite from closing the slab's literal applies to its header.
proof fn lemma_rw_at<'a, W: WriteOps<'a>>(s: State<'a, W>, pre: Seq<u8>, b: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64, rw: Option<RewriteHeader>)
    requires rel(s, pre, b, base, gs, ws, h0), rw_from(s, rw),
    ensures rw_at::<W>(rw, pre, h0),
{
    if rw is Some {
        let gb = groups_bytes::<W::R>(gs);
        let h = <W::R as RleOps<'a>>::enc_signed(h0);
        let rest = encs::<W::R>(ws.take(s->Lit_count - s->Lit_local));
        assert(pre.skip(gb.len() as int) =~= h + rest);
        assert((h + rest).take(h.len() as int) =~= h);
    }
}

/// The edited slab's bytes, `pre` with the rewrite applied, then the
/// region rebuilt, then the slab's from `end`.
pub(crate) open spec fn edited<'a, W: WriteOps<'a>>(r: Rebuilt, pre: Seq<u8>, h0: i64, data: Seq<u8>) -> Seq<u8> {
    apply::<W>(r.rewrite, pre, h0) + r.bytes@ + data.skip(r.end as int)
}

/// The rebuild `r` of `data`, whose bytes before the edit are `pre`:
/// the edited slab, then the slabs it spilled into, encode `vals`.
pub(crate) open spec fn rebuilt_ok<'a, W: WriteOps<'a>>(r: Rebuilt, pre: Seq<u8>, h0: i64, data: Seq<u8>, vals: Seq<VOf<'a, W>>) -> bool {
    &&& r.start == pre.len() && r.start <= r.end <= data.len()
    &&& rw_at::<W>(r.rewrite, pre, h0)
    &&& exists|cs: Seq<Seq<VOf<'a, W>>>| #[trigger] concat(cs) == vals && cs.len() == r.overflow@.len() + 1
        && encodes::<W::R>(edited::<W>(r, pre, h0, data), cs[0])
        && forall|k: int| 0 <= k < r.overflow@.len() ==> encodes::<W::R>(#[trigger] slab_bytes(r.overflow@[k]), cs[k + 1])
}

/// A slab region rebuilt: the bytes between `start` and `end` replaced
/// by `bytes`, with the slab's literal header rewritten, and the slabs
/// it spilled into.
pub(crate) struct Rebuilt {
    pub(crate) bytes: Vec<u8>,
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) len: usize,
    pub(crate) segments: usize,
    pub(crate) rewrite: Option<RewriteHeader>,
    pub(crate) overflow: Vec<Slab>,
    pub(crate) wpos: WPos,
    /// the count in the literal header [`rewrite`](Self::rewrite) replaces
    pub(crate) h0: Ghost<i64>,
}

impl Rebuilt {
    fn new(start: usize, end: usize) -> (r: Self)
        ensures r.start == start, r.end == end, r.rewrite is None, r.overflow@.len() == 0, r.bytes@.len() == 0,
    {
        Rebuilt {
            bytes: Vec::new(),
            start,
            end,
            len: 0,
            segments: 0,
            rewrite: None,
            overflow: Vec::new(),
            wpos: FlushState::none().wpos,
            h0: Ghost(0),
        }
    }

    /// Write the rebuilt bytes into the slab's, `data`, and apply the
    /// header rewrite. Returns the rewrite's change in length.
    pub(crate) fn apply_bytes<'a, W: WriteOps<'a>>(&self, data: &mut Vec<u8>) -> (d: i64)
        requires
            self.start <= self.end <= old(data).len(),
            rw_at::<W>(self.rewrite, old(data)@.take(self.start as int), self.h0@),
        ensures
            final(data)@ == apply::<W>(self.rewrite, old(data)@.take(self.start as int), self.h0@) + self.bytes@
                + old(data)@.skip(self.end as int),
    {
        let ghost h0 = self.h0@;
        let ghost d0 = data@;
        let ghost pre = d0.take(self.start as int);
        splice_bytes(data, self.start, self.end - self.start, &self.bytes);
        let ghost d1 = data@;
        proof {
            assert(d1 =~= pre + (self.bytes@ + d0.skip(self.end as int)));
        }
        match self.rewrite {
            Some(rw) => {
                let ghost h = <W::R as RleOps<'a>>::enc_signed(h0);
                let ghost post = self.bytes@ + d0.skip(self.end as int);
                proof {
                    assert(d1.skip(rw.pos as int) =~= pre.skip(rw.pos as int) + post);
                    assert(pre.skip(rw.pos as int).take(h.len() as int) == h);
                    assert(d1.skip(rw.pos as int).take(h.len() as int) =~= h);
                    assert(h.is_prefix_of(d1.skip(rw.pos as int)));
                }
                let d = W::rewrite_lit_header(data, rw.pos, rw.count);
                proof {
                    assert(d1.take(rw.pos as int) =~= pre.take(rw.pos as int));
                    assert(d1.skip(rw.pos + h.len()) =~= pre.skip(rw.pos + h.len()) + post);
                    assert(data@ =~= apply::<W>(self.rewrite, pre, h0) + self.bytes@ + d0.skip(self.end as int));
                }
                d
            },
            None => 0,
        }
    }
}

/// The rebuild of one slab region.
pub(crate) struct Splice<'a, W: WriteOps<'a>> {
    state: State<'a, W>,
    f: FlushState,
    buf: Vec<u8>,
    max_segments: usize,
    target: usize,
    starting_segments: usize,
    prefix_segments: usize,
    head_len: usize,
    overflowed: bool,
    inserted: usize,
    out: Rebuilt,
    /// The slab's bytes before the edit, and the count in its literal
    /// header there, if the edit opened inside a literal.
    pre: Ghost<Seq<u8>>,
    h0: Ghost<i64>,
    /// The values of the slabs finished: the edited one, then those it
    /// spilled into.
    done: Ghost<Seq<Seq<VOf<'a, W>>>>,
    /// The values written since.
    cur: Ghost<Seq<VOf<'a, W>>>,
}

impl<'a, W: WriteOps<'a>> Splice<'a, W> {
    /// The slab's bytes before the edit.
    pub(crate) closed spec fn pre(&self) -> Seq<u8> {
        self.pre@
    }

    /// The header count of the slab's literal the edit opened in.
    pub(crate) closed spec fn h0(&self) -> i64 {
        self.h0@
    }

    /// The values written: the slab's before the edit, and those since.
    pub(crate) closed spec fn vals(&self) -> Seq<VOf<'a, W>> {
        concat(self.done@) + self.cur@
    }

    /// Where the slab's bytes after the region start.
    pub(crate) closed spec fn end(&self) -> usize {
        self.out.end
    }

    /// The bytes the current slab starts with: the edited slab's before
    /// the edit, or none for a slab spilled into.
    closed spec fn cur_pre(&self) -> Seq<u8> {
        if self.overflowed { Seq::empty() } else { self.pre@ }
    }

    /// Room for `n` more values in the slab in hand.
    pub(crate) closed spec fn room(&self, n: nat) -> bool {
        self.cur@.len() + n + 2 < usize::MAX
    }

    /// Room for more is room for fewer.
    pub(crate) proof fn lemma_room_le(&self, a: nat, b: nat)
        requires self.room(b), a <= b,
        ensures self.room(a),
    {
    }

    /// The pending group is among the values written.
    proof fn lemma_room(&self)
        requires self.inv(),
        ensures self.state.plen() <= self.cur@.len(),
    {
        let (gs, ws) = choose|gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>|
            #[trigger] rel(self.state, apply::<W>(self.f.rewrite, self.cur_pre(), self.h0@), self.buf@, 0, gs, ws, self.h0@)
            && values_of(self.state, gs, ws) == self.cur@;
        if !(self.state is Empty) {
            let g = pend_group(self.state, ws)->Some_0;
            lemma_push::<W::R>(gs, g);
            lemma_group_values_len(g, <W::R as RleOps<'a>>::null());
        }
    }

    pub(crate) closed spec fn inv(&self) -> bool {
        let rw = self.f.rewrite;
        &&& wf(self.state, self.buf@)
        &&& exists|gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>|
            #[trigger] rel(self.state, apply::<W>(rw, self.cur_pre(), self.h0@), self.buf@, 0, gs, ws, self.h0@)
            && values_of(self.state, gs, ws) == self.cur@
        &&& rw_at::<W>(rw, self.cur_pre(), self.h0@)
        &&& rw is Some ==> !pre_lit(self.state)
        &&& self.out.start == self.pre@.len()
        &&& if !self.overflowed {
            self.done@.len() == 0 && self.out.overflow@.len() == 0
        } else {
            &&& rw is None && !pre_lit(self.state)
            &&& self.done@.len() == self.out.overflow@.len() + 1
            &&& encodes::<W::R>(apply::<W>(self.out.rewrite, self.pre@, self.h0@) + self.out.bytes@, self.done@[0])
            &&& rw_at::<W>(self.out.rewrite, self.pre@, self.h0@)
            &&& forall|k: int| 0 <= k < self.out.overflow@.len()
                ==> encodes::<W::R>(#[trigger] slab_bytes(self.out.overflow@[k]), self.done@[k + 1])
        }
    }

    pub(crate) fn empty() -> (r: Self) {
        Splice {
            state: State::empty(),
            f: FlushState::none(),
            buf: Vec::new(),
            max_segments: 0,
            target: 0,
            starting_segments: 0,
            prefix_segments: 0,
            head_len: 0,
            overflowed: false,
            inserted: 0,
            out: Rebuilt::new(0, 0),
            pre: Ghost(Seq::empty()),
            h0: Ghost(0),
            done: Ghost(Seq::empty()),
            cur: Ghost(Seq::empty()),
        }
    }

    /// Open on `state`, which stands for the values `vals` of a slab
    /// whose bytes before the edit, at `start`, are `pre`.
    pub(crate) fn reset(&mut self, state: State<'a, W>, prefix_segments: usize, head_len: usize, start: usize, max_segments: usize, Ghost(pre): Ghost<Seq<u8>>, Ghost(h0): Ghost<i64>, Ghost(vals): Ghost<Seq<VOf<'a, W>>>)
        requires
            pre.len() == start,
            exists|gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>| #[trigger] rel(state, pre, Seq::empty(), 0, gs, ws, h0)
                && values_of(state, gs, ws) == vals,
        ensures
            final(self).inv(), final(self).vals() == vals, final(self).pre() == pre, final(self).h0() == h0,
            final(self).end() == start,
    {
        self.state = state;
        self.f = FlushState::none();
        self.buf.clear();
        self.max_segments = max_segments;
        self.target = max_segments;
        self.starting_segments = prefix_segments;
        self.prefix_segments = prefix_segments;
        self.head_len = head_len;
        self.overflowed = false;
        self.inserted = 0;
        self.out = Rebuilt::new(start, start);
        self.pre = Ghost(pre);
        self.h0 = Ghost(h0);
        self.done = Ghost(Seq::empty());
        self.cur = Ghost(vals);
        proof {
            let (gs, ws) = choose|gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>| #[trigger] rel(state, pre, Seq::empty(), 0, gs, ws, h0)
                && values_of(state, gs, ws) == vals;
            lemma_rel_wf::<W>(state, pre, Seq::empty(), 0, gs, ws, h0);
            assert(self.buf@ =~= Seq::<u8>::empty());
            assert(rel(self.state, apply::<W>(None, self.cur_pre(), h0), self.buf@, 0, gs, ws, h0));
            assert(concat(self.done@) =~= Seq::<VOf<'a, W>>::empty());
            assert(self.vals() =~= vals);
        }
    }

    /// Where the slab's bytes after the region start.
    pub(crate) fn set_range_end(&mut self, end: usize)
        requires old(self).inv(),
        ensures
            final(self).inv(), final(self).vals() == old(self).vals(), final(self).pre() == old(self).pre(),
            final(self).h0() == old(self).h0(), final(self).end() == end,
            forall|n: nat| #[trigger] final(self).room(n) == old(self).room(n),
    {
        let ghost rw = self.f.rewrite;
        let ghost p = apply::<W>(rw, self.cur_pre(), self.h0@);
        let ghost (gs, ws) = choose|gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>| #[trigger] rel(self.state, p, self.buf@, 0, gs, ws, self.h0@)
            && values_of(self.state, gs, ws) == self.cur@;
        self.out.end = end;
        proof {
            assert(rel(self.state, apply::<W>(self.f.rewrite, self.cur_pre(), self.h0@), self.buf@, 0, gs, ws, self.h0@));
        }
    }

    /// Append `count` copies of `value`, cutting to a new slab first if
    /// the segment budget is spent.
    pub(crate) fn push_cow(&mut self, value: W::Cow, count: usize)
        requires
            old(self).inv(), old(self).room(count as nat),
            <W::R as RleOps<'a>>::is_null(W::val(value)) ==> <W::R as RleOps<'a>>::nullable(),
        ensures
            final(self).inv(), final(self).vals() == old(self).vals() + copies(count as nat, W::val(value)),
            final(self).pre() == old(self).pre(), final(self).h0() == old(self).h0(),
            final(self).end() == old(self).end(),
    {
        if add(add(self.starting_segments, self.f.segments), self.state.pending_segments()) >= self.target {
            self.cut();
        }
        proof {
            self.lemma_room();
        }
        let ghost s0 = self.state;
        let ghost b0 = self.buf@;
        let ghost rw0 = self.f.rewrite;
        let ghost cur0 = self.cur@;
        let f = self.state.append_cow(&mut self.buf, value, count);
        add_flush(&mut self.f, f);
        // counted once appended, so a run that panics as too long leaves
        // the rebuild as it was for the cursor's drop to close
        self.inserted = add(self.inserted, count);
        proof {
            let added = copies(count as nat, W::val(value));
            self.cur = Ghost(cur0 + added);
            let p0 = apply::<W>(rw0, self.cur_pre(), self.h0@);
            let (gs, ws) = choose|gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>| #[trigger] rel(s0, p0, b0, 0, gs, ws, self.h0@)
                && values_of(s0, gs, ws) == cur0;
            lemma_apply_comb::<W>(rw0, f.rewrite, self.cur_pre(), self.h0@);
            lemma_rw_at::<W>(s0, p0, b0, 0, gs, ws, self.h0@, f.rewrite);
            let (gs2, ws2) = choose|gs2: Seq<Group<VOf<'a, W>>>, ws2: Seq<VOf<'a, W>>| #[trigger] rel(self.state, apply::<W>(f.rewrite, p0, self.h0@), self.buf@, 0, gs2, ws2, self.h0@)
                && values_of(self.state, gs2, ws2) == values_of(s0, gs, ws) + added;
            assert(rel(self.state, apply::<W>(self.f.rewrite, self.cur_pre(), self.h0@), self.buf@, 0, gs2, ws2, self.h0@));
            assert(self.vals() =~= old(self).vals() + added);
        }
    }

    /// Close the slab in hand and start a new one.
    fn cut(&mut self)
        requires old(self).inv(),
        ensures
            final(self).inv(), final(self).vals() == old(self).vals(), final(self).state is Empty, final(self).overflowed,
            final(self).pre() == old(self).pre(), final(self).h0() == old(self).h0(),
            final(self).end() == old(self).end(),
    {
        let ghost s0 = self.state;
        let ghost b0 = self.buf@;
        let ghost rw0 = self.f.rewrite;
        let ghost p0 = apply::<W>(rw0, self.cur_pre(), self.h0@);
        let f = self.state.flush(&mut self.buf);
        add_flush(&mut self.f, f);
        let ghost bytes = self.buf@;
        proof {
            let (gs, ws) = choose|gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>| #[trigger] rel(s0, p0, b0, 0, gs, ws, self.h0@)
                && values_of(s0, gs, ws) == self.cur@;
            lemma_apply_comb::<W>(rw0, f.rewrite, self.cur_pre(), self.h0@);
            lemma_rw_at::<W>(s0, p0, b0, 0, gs, ws, self.h0@, f.rewrite);
            let a = all_groups(s0, gs, ws);
            assert(bytes.skip(0) =~= bytes);
            assert(apply::<W>(self.f.rewrite, self.cur_pre(), self.h0@) + bytes == groups_bytes::<W::R>(a));
            assert(encodes::<W::R>(apply::<W>(self.f.rewrite, self.cur_pre(), self.h0@) + bytes, self.cur@));
        }
        let mut taken = Vec::new();
        core::mem::swap(&mut self.buf, &mut taken);
        if !self.overflowed {
            self.overflowed = true;
            self.target = self.max_segments / 2;
            self.out.wpos = self.f.wpos;
            self.out.bytes = taken;
            self.out.len = add(self.head_len, self.inserted);
            self.out.segments = add(self.prefix_segments, self.f.segments);
            self.out.rewrite = self.f.rewrite;
            proof {
                assert(self.out.bytes@ == bytes);
                self.done = Ghost(seq![self.cur@]);
                lemma_concat_push(Seq::<Seq<VOf<'a, W>>>::empty(), self.cur@);
                assert(Seq::<Seq<VOf<'a, W>>>::empty().push(self.cur@) =~= self.done@);
            }
        } else {
            let s = new_slab(taken, self.inserted, self.f.segments, self.f.wpos);
            self.out.overflow.push(s);
            proof {
                let d0 = self.done@;
                self.done = Ghost(d0.push(self.cur@));
                lemma_concat_push(d0, self.cur@);
                assert(Seq::<u8>::empty() + bytes =~= bytes);
                assert(slab_bytes(s) == bytes);
                assert forall|k: int| 0 <= k < self.out.overflow@.len()
                    implies encodes::<W::R>(#[trigger] slab_bytes(self.out.overflow@[k]), self.done@[k + 1]) by {
                    if k < self.out.overflow@.len() - 1 {
                        assert(self.out.overflow@[k] == old(self).out.overflow@[k]);
                        assert(self.done@[k + 1] == d0[k + 1]);
                    }
                }
                assert(self.out.overflow@.last() == s);
            }
        }
        self.state = State::empty();
        self.f = FlushState::none();
        self.inserted = 0;
        self.starting_segments = 0;
        proof {
            let e = Seq::<Group<VOf<'a, W>>>::empty();
            let ghost cur = self.cur@;
            assert(self.buf@ =~= Seq::<u8>::empty());
            assert(rel(self.state, apply::<W>(self.f.rewrite, self.cur_pre(), self.h0@), self.buf@, 0, e, Seq::empty(), self.h0@));
            assert(concat(self.done@).len() >= 0);
            assert(concat(self.done@) =~= concat(old(self).done@) + cur);
            self.cur = Ghost(Seq::empty());
            assert(values_of(self.state, e, Seq::empty()) =~= self.cur@);
            assert(self.vals() =~= old(self).vals());
            assert(wf(self.state, self.buf@));
            assert(rw_at::<W>(self.f.rewrite, self.cur_pre(), self.h0@));
            assert(self.out.start == self.pre@.len());
            assert(self.done@.len() == self.out.overflow@.len() + 1);
            assert(encodes::<W::R>(apply::<W>(self.out.rewrite, self.pre@, self.h0@) + self.out.bytes@, self.done@[0]));
            assert(rw_at::<W>(self.out.rewrite, self.pre@, self.h0@));
        }
    }

    /// What a finish promises, for any slab `data` the postfix and
    /// `tail_bytes` came from.
    pub(crate) open spec fn finished(&self, r: Rebuilt, postfix: Option<Postfix<W::Cow>>, tail_bytes: Seq<u8>, data_len: usize) -> bool {
        forall|data: Seq<u8>, at: int|
            #[trigger] postfix_ok::<W::R>(data, at, pmap::<W>(postfix), self.end() as int)
            && tail_bytes == data.skip(self.end() as int) && data_len == data.len()
            && self.pre().len() <= self.end() <= data.len()
            ==> rebuilt_ok::<W>(r, self.pre(), self.h0(), data, self.vals() + slab_values::<W::R>(data).skip(at))
    }

    /// Close on the postfix (the run after the region, if any) and the
    /// slab's bytes after it, `tail_bytes`.
    pub(crate) fn finish(&mut self, postfix: Option<Postfix<W::Cow>>, postfix_segments: usize, tail_count: usize, tail_bytes: &[u8], data_len: usize, slab_tail: RleTail) -> (r: Rebuilt)
        requires old(self).inv(), nulls_ok::<W>(postfix), old(self).room(plen_of(postfix)),
        ensures old(self).finished(r, postfix, tail_bytes@, data_len), r.h0@ == old(self).h0(),
    {
        let postfix_pending = match &postfix {
            Some(p) => p.pending(),
            None => 0,
        };
        if !self.overflowed {
            let pending = add(self.state.pending_segments(), postfix_pending);
            if add(add(add(self.prefix_segments, pending), self.f.segments), postfix_segments) <= self.max_segments {
                // it all fits in the one slab
                return self.finish_in_place(postfix, postfix_segments, tail_count);
            }
            // spill: the edited slab keeps what is built so far
            self.cut();
        }
        // if the postfix will not fit on the slab in hand, close it
        let pending = add(self.state.pending_segments(), postfix_pending);
        if add(add(pending, self.f.segments), postfix_segments) > self.max_segments {
            self.cut();
        }
        self.finish_spilled(postfix, postfix_segments, tail_count, tail_bytes, data_len, slab_tail)
    }

    /// The postfix joins the edited slab.
    fn finish_in_place(&mut self, postfix: Option<Postfix<W::Cow>>, postfix_segments: usize, tail_count: usize) -> (r: Rebuilt)
        requires old(self).inv(), !old(self).overflowed, nulls_ok::<W>(postfix), old(self).room(plen_of(postfix)),
        ensures
            forall|tail_bytes: Seq<u8>, data_len: usize| #[trigger] old(self).finished(r, postfix, tail_bytes, data_len),
            r.h0@ == old(self).h0(),
    {
        hide(canonical);
        let ghost s0 = self.state;
        let ghost b0 = self.buf@;
        let ghost rw0 = self.f.rewrite;
        let ghost pre = self.pre@;
        let ghost h0 = self.h0@;
        let ghost p0 = apply::<W>(rw0, pre, h0);
        proof {
            self.lemma_room();
        }
        let f = self.state.flush_postfix(&mut self.buf, postfix);
        add_flush(&mut self.f, f);
        let ghost b1 = self.buf@;
        let mut taken = Vec::new();
        core::mem::swap(&mut self.buf, &mut taken);
        self.out.bytes = taken;
        self.out.wpos = self.f.wpos;
        self.out.rewrite = self.f.rewrite;
        self.out.len = add(add(self.head_len, self.inserted), tail_count);
        self.out.segments = add(add(self.prefix_segments, self.f.segments), postfix_segments);
        self.out.h0 = Ghost(self.h0@);
        let mut r = Rebuilt::new(0, 0);
        core::mem::swap(&mut self.out, &mut r);
        proof {
            let (gs, ws) = choose|gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>| #[trigger] rel(s0, p0, b0, 0, gs, ws, h0)
                && values_of(s0, gs, ws) == old(self).cur@;
            lemma_apply_comb::<W>(rw0, f.rewrite, pre, h0);
            lemma_rw_at::<W>(s0, p0, b0, 0, gs, ws, h0, f.rewrite);
            assert(concat(old(self).done@) =~= Seq::<VOf<'a, W>>::empty());
            assert(old(self).vals() =~= old(self).cur@);
            assert(b1.skip(0) =~= b1);
            assert forall|tail_bytes: Seq<u8>, data_len: usize| #[trigger] old(self).finished(r, postfix, tail_bytes, data_len) by {
                assert forall|data: Seq<u8>, at: int|
                    #[trigger] postfix_ok::<W::R>(data, at, pmap::<W>(postfix), old(self).end() as int)
                    && tail_bytes == data.skip(old(self).end() as int) && data_len == data.len()
                    && old(self).pre().len() <= old(self).end() <= data.len()
                    implies rebuilt_ok::<W>(r, pre, h0, data, old(self).vals() + slab_values::<W::R>(data).skip(at)) by {
                    let vals = old(self).vals() + slab_values::<W::R>(data).skip(at);
                    let cs = seq![vals];
                    lemma_concat_push(Seq::<Seq<VOf<'a, W>>>::empty(), vals);
                    assert(Seq::<Seq<VOf<'a, W>>>::empty().push(vals) =~= cs);
                    assert(edited::<W>(r, pre, h0, data) == apply::<W>(f.rewrite, p0, h0) + b1.skip(0) + data.skip(old(self).end() as int));
                }
            }
        }
        r
    }

    /// The postfix goes on a slab the edit spilled into, with the edited
    /// slab's bytes after it: the edited slab ends at the edit.
    fn finish_spilled(&mut self, postfix: Option<Postfix<W::Cow>>, postfix_segments: usize, tail_count: usize, tail_bytes: &[u8], data_len: usize, slab_tail: RleTail) -> (r: Rebuilt)
        requires old(self).inv(), old(self).overflowed, nulls_ok::<W>(postfix), old(self).room(plen_of(postfix)),
        ensures old(self).finished(r, postfix, tail_bytes@, data_len), r.h0@ == old(self).h0(),
    {
        hide(canonical);
        let ghost s0 = self.state;
        let ghost b0 = self.buf@;
        let ghost h0 = self.h0@;
        proof {
            self.lemma_room();
        }
        let f = self.state.flush_postfix(&mut self.buf, postfix);
        add_flush(&mut self.f, f);
        let ghost b1 = self.buf@;
        self.out.end = data_len;
        let len = add(self.inserted, tail_count);
        let segments = add(self.f.segments, postfix_segments);
        let mut taken = Vec::new();
        core::mem::swap(&mut self.buf, &mut taken);
        let s = last_slab(taken, tail_bytes, len, segments, self.f.wpos, slab_tail);
        self.out.overflow.push(s);
        self.out.h0 = Ghost(self.h0@);
        let mut r = Rebuilt::new(0, 0);
        core::mem::swap(&mut self.out, &mut r);
        proof {
            let pre = self.pre@;
            let e = Seq::<u8>::empty();
            let (gs, ws) = choose|gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>| #[trigger] rel(s0, apply::<W>(None, e, h0), b0, 0, gs, ws, h0)
                && values_of(s0, gs, ws) == old(self).cur@;
            assert(f.rewrite is None);
            assert(apply::<W>(None, e, h0) =~= e);
            assert(b1.skip(0) =~= b1);
            let n = old(self).out.overflow@.len();
            assert(r.overflow@ == old(self).out.overflow@.push(s));
            assert forall|data: Seq<u8>, at: int|
                #[trigger] postfix_ok::<W::R>(data, at, pmap::<W>(postfix), old(self).end() as int)
                && tail_bytes@ == data.skip(old(self).end() as int) && data_len == data.len()
                && old(self).pre().len() <= old(self).end() <= data.len()
                implies rebuilt_ok::<W>(r, pre, h0, data, old(self).vals() + slab_values::<W::R>(data).skip(at)) by {
                let rest = slab_values::<W::R>(data).skip(at);
                let d0 = old(self).done@;
                let cs = d0.push(old(self).cur@ + rest);
                lemma_concat_push(d0, old(self).cur@ + rest);
                assert(concat(cs) =~= old(self).vals() + rest);
                assert(e + b1.skip(0) + data.skip(old(self).end() as int) =~= slab_bytes(s));
                assert(data.skip(data_len as int) =~= e);
                assert(edited::<W>(r, pre, h0, data) =~= apply::<W>(old(self).out.rewrite, pre, h0) + old(self).out.bytes@);
                assert(cs[0] == d0[0]);
                assert forall|k: int| 0 <= k < r.overflow@.len() implies encodes::<W::R>(#[trigger] slab_bytes(r.overflow@[k]), cs[k + 1]) by {
                    if k < n {
                        assert(r.overflow@[k] == old(self).out.overflow@[k]);
                        assert(cs[k + 1] == d0[k + 1]);
                    }
                }
            }
        }
        r
    }
}

} // verus!

impl std::fmt::Debug for Rebuilt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rebuilt")
            .field("bytes", &self.bytes)
            .field("start", &self.start)
            .field("end", &self.end)
            .field("len", &self.len)
            .field("segments", &self.segments)
            .field("rewrite", &self.rewrite)
            .field("overflow", &self.overflow)
            .field("wpos", &self.wpos)
            .finish()
    }
}

impl Rebuilt {
    /// Write the rebuild back into `slab`; returns the slabs it spilled
    /// into (to go after it) and its byte buffer, for reuse.
    // T is only read by the debug-build slab validation below
    #[cfg_attr(not(debug_assertions), allow(clippy::extra_unused_type_parameters))]
    pub(crate) fn apply<T: RleValue, C: Codec>(&mut self, slab: &mut Slab) -> (Vec<Slab>, Vec<u8>) {
        let mut prefix = self.start as i64;
        let middle = self.bytes.len();
        let postfix = slab.data.len() - self.end;

        // verified with Verus: the splice, then the header rewrite
        prefix += self.apply_bytes::<WOps<'static, T, T, C>>(&mut slab.data);

        slab.tail = self.wpos.merge(prefix as usize, middle, postfix, slab.tail);
        slab.len = self.len;
        slab.segments = self.segments;

        #[cfg(debug_assertions)]
        crate::rle::validate_rle_slab::<T, C>(slab);
        #[cfg(debug_assertions)]
        for s in &self.overflow {
            crate::rle::validate_rle_slab::<T, C>(s);
        }

        let mut bytes = std::mem::take(&mut self.bytes);
        bytes.clear();
        (std::mem::take(&mut self.overflow), bytes)
    }
}

impl<T: RleValue, C: Codec> Splice<'static, WOps<'static, T, T, C>> {
    pub(crate) fn recycle(&mut self, mut bytes: Vec<u8>) {
        if bytes.capacity() > self.buf.capacity() {
            bytes.clear();
            self.buf = bytes;
        }
    }
}
