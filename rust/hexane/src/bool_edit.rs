//! The bool edit cursor's core, verified with Verus.
//!
//! A rebuild of one bool slab: the slab's head before the edit point is
//! kept as bytes, values are carried through or inserted, and the slab's
//! tail after the reader is carried as bytes again. The writer's state is
//! one pending run (`cur_value`, `cur_count`) after the counts of the
//! chunk in hand (`BEdit::chunk`); a chunk that reaches its segment
//! target is cut into a slab of its own.

// Verus proofs read more plainly with `x = x + n`.
#![allow(
    clippy::len_zero,
    clippy::assign_op_pattern,
    clippy::too_many_arguments,
    clippy::manual_unwrap_or_default,
    clippy::manual_unwrap_or,
    clippy::manual_is_multiple_of,
    clippy::single_match
)]

use crate::bool_wire::*;
use crate::cursor::Slabs;
#[cfg(verus_keep_ghost)]
use crate::cursor::{lemma_vals_cat, lemma_vals_unit, vals_of};
use crate::rle::edit::Visit;
#[cfg(verus_keep_ghost)]
use crate::rle::writer::concat;
use crate::{PackError, Run};
use std::marker::PhantomData;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// The values of chunks of counts, in order.
pub(crate) open spec fn chunks_values(cs: Seq<Seq<u64>>) -> Seq<bool>
    decreases cs.len(),
{
    if cs.len() == 0 { Seq::empty() } else { chunks_values(cs.drop_last()) + vals_at(cs.last(), 0) }
}

/// A run's values.
pub(crate) open spec fn run_of(n: nat, v: bool) -> Seq<bool> {
    Seq::new(n, |_j: int| v)
}

/// Counts that may make a slab once a last run is added: only the first
/// may be zero.
pub(crate) open spec fn counts_pre(cs: Seq<u64>) -> bool {
    forall|i: int| 1 <= i < cs.len() ==> #[trigger] cs[i] > 0
}

/// Values from counts number their counts' total.
pub(crate) proof fn lemma_vals_len(cs: Seq<u64>, p: int)
    ensures vals_at(cs, p).len() == counts_len(cs),
    decreases cs.len(),
{
    if cs.len() > 0 {
        lemma_vals_len(cs.drop_last(), p);
    }
}

/// The chunks cut, once the first is: the first (with the head's counts)
/// as bytes after the head and metadata, the rest as slabs.
#[verifier::opaque]
pub(crate) open spec fn out_ok<K: CountOps>(overflowed: bool, base: Seq<u64>, done: Seq<Seq<u64>>, out_cb: Seq<u64>, out_bytes: Seq<u8>,
    out_emitted: usize, out_segments: usize, out_tail: u8, overflow: Seq<BSlab>) -> bool {
    &&& !overflowed ==> done.len() == 0 && overflow.len() == 0
    &&& overflowed ==> {
        &&& done.len() >= 1
        &&& done[0] == base + out_cb
        &&& out_bytes == counts_bytes::<K>(out_cb)
        &&& out_emitted == counts_len(out_cb) && out_segments == done[0].len()
        &&& counts_ok(done[0]) && out_cb.len() > 0
        &&& out_tail == K::enc(done[0].last()).len()
        &&& overflow.len() + 1 == done.len()
        &&& forall|i: int| 0 <= i < overflow.len() ==> {
            let s = #[trigger] overflow[i];
            &&& bslab_bytes(s) == counts_bytes::<K>(done[i + 1])
            &&& counts_ok(done[i + 1]) && done[i + 1].len() > 0
            &&& bslab_len(s) == counts_len(done[i + 1])
            &&& bslab_segments(s) == done[i + 1].len()
            &&& bslab_tail(s) == K::enc(done[i + 1].last()).len()
        }
    }
}

/// A run appended to counts.
pub(crate) proof fn lemma_vals_push(cs: Seq<u64>, c: u64)
    ensures vals_at(cs.push(c), 0) == vals_at(cs, 0) + run_of(c as nat, run_value(cs.len() as int)),
{
    assert(cs.push(c).drop_last() =~= cs);
}

/// The writer half of a bool slab rebuild.
pub(crate) struct BWriter {
    /// the run being written: its value, and items in it so far
    pub(crate) cur_value: bool,
    pub(crate) cur_count: usize,
    pub(crate) buf: Vec<u8>,
    /// segments written into the chunk in hand
    pub(crate) segments: usize,
    /// items flushed into `buf` — the pending run is not counted
    pub(crate) emitted: usize,
    /// byte length of the last count written into `buf`
    pub(crate) tail_byte: u8,
    /// segments before `buf` in the chunk in hand: the slab's head
    /// before the first cut, nothing after
    pub(crate) standing: usize,
    pub(crate) max_segments: usize,
    /// budget for the chunk in hand — halves after the first cut, so a
    /// spill leaves room for later growth rather than filling to the brim
    pub(crate) target: usize,
    pub(crate) overflowed: bool,
    /// the first chunk, once cut: its bytes after the head, and metadata
    pub(crate) out_bytes: Vec<u8>,
    pub(crate) out_emitted: usize,
    pub(crate) out_segments: usize,
    pub(crate) out_tail: u8,
    /// the chunks cut after it, as slabs
    pub(crate) overflow: Vec<BSlab>,
    /// the head's counts (ghost), the counts in `buf`, and the counts of
    /// the chunks cut (the first with the head's)
    #[allow(dead_code)]
    pub(crate) base: Ghost<Seq<u64>>,
    #[allow(dead_code)]
    pub(crate) cb: Ghost<Seq<u64>>,
    #[allow(dead_code)]
    pub(crate) done: Ghost<Seq<Seq<u64>>>,
    /// the first chunk's counts after the head, once cut
    #[allow(dead_code)]
    pub(crate) out_cb: Ghost<Seq<u64>>,
}

impl BWriter {
    /// The counts of the chunk in hand, before the pending run.
    pub(crate) open spec fn chunk(&self) -> Seq<u64> {
        (if self.overflowed { Seq::empty() } else { self.base@ }) + self.cb@
    }

    /// The values written: the chunks cut, the chunk in hand, the pending
    /// run.
    pub(crate) open spec fn vals(&self) -> Seq<bool> {
        chunks_values(self.done@) + vals_at(self.chunk(), 0) + run_of(self.cur_count as nat, self.cur_value)
    }

    /// Between operations: the pending run is empty only before anything
    /// at all.
    pub(crate) open spec fn inv<K: CountOps>(&self) -> bool {
        &&& self.inv_w::<K>()
        &&& self.cur_count == 0 ==> self.chunk().len() == 0 && !self.overflowed
    }

    /// The writer's state, the pending run aside.
    pub(crate) open spec fn inv_w<K: CountOps>(&self) -> bool {
        let c = self.chunk();
        &&& self.buf@ == counts_bytes::<K>(self.cb@)
        &&& self.segments == self.cb@.len() && self.emitted == counts_len(self.cb@)
        &&& self.cb@.len() > 0 ==> self.tail_byte == K::enc(self.cb@.last()).len()
        &&& self.cb@.len() == 0 ==> self.tail_byte == 0
        &&& counts_pre(c)
        &&& self.cur_value == run_value(c.len() as int)
        &&& self.standing == if self.overflowed { 0 } else { self.base@.len() }
        &&& !self.overflowed ==> self.target == self.max_segments && self.max_segments >= 2
        &&& self.overflowed ==> self.target >= 1
        &&& out_ok::<K>(self.overflowed, self.base@, self.done@, self.out_cb@, self.out_bytes@, self.out_emitted,
            self.out_segments, self.out_tail, self.overflow@)
        &&& self.vals().len() <= usize::MAX
    }

    /// An empty writer, after the head `base` of `head` values in
    /// `base_bytes`, pending `cur_count` of `cur_value`.
    pub(crate) fn new<K: CountOps>(cur_value: bool, cur_count: usize, standing: usize, max_segments: usize, buf: Vec<u8>,
        Ghost(base): Ghost<Seq<u64>>) -> (w: Self)
        requires
            counts_pre(base), cur_value == run_value(base.len() as int), cur_count == 0 ==> base.len() == 0,
            standing == base.len(), max_segments >= 2,
            counts_len(base) + cur_count <= usize::MAX,
        ensures
            w.inv::<K>(), w.base@ == base, w.done@.len() == 0, !w.overflowed,
            w.vals() == vals_at(base, 0) + run_of(cur_count as nat, cur_value),
    {
        let mut buf = buf;
        buf.clear();
        let w = BWriter {
            cur_value,
            cur_count,
            buf,
            segments: 0,
            emitted: 0,
            tail_byte: 0,
            standing,
            max_segments,
            target: max_segments,
            overflowed: false,
            out_bytes: Vec::new(),
            out_emitted: 0,
            out_segments: 0,
            out_tail: 0,
            overflow: Vec::new(),
            base: Ghost(base),
            cb: Ghost(Seq::empty()),
            done: Ghost(Seq::empty()),
            out_cb: Ghost(Seq::empty()),
        };
        proof {
            reveal(out_ok);
            assert(w.buf@ =~= counts_bytes::<K>(Seq::<u64>::empty()));
            assert(w.chunk() =~= base);
            assert(chunks_values(w.done@) =~= Seq::<bool>::empty());
            assert(w.vals() =~= vals_at(base, 0) + run_of(cur_count as nat, cur_value));
            lemma_vals_at_zero(base);
            lemma_values_len(base);
        }
        w
    }

    /// Write the pending run out (nothing for an empty one).
    #[inline(always)]
    #[verifier::spinoff_prover]
    fn flush_run<K: CountOps>(&mut self)
        requires old(self).inv_w::<K>(),
        ensures
            final(self).inv_w::<K>(), final(self).vals() == old(self).vals(),
            final(self).cur_count == 0 || final(self).cur_count == old(self).cur_count,
            old(self).cur_count > 0 ==> {
                &&& final(self).cb@ == old(self).cb@.push(old(self).cur_count as u64)
                &&& final(self).cur_value == !old(self).cur_value && final(self).cur_count == 0
            },
            old(self).cur_count == 0 ==> *final(self) == *old(self),
            final(self).base@ == old(self).base@, final(self).done@ == old(self).done@,
            final(self).overflowed == old(self).overflowed, final(self).target == old(self).target,
            final(self).standing == old(self).standing, final(self).max_segments == old(self).max_segments,
    {
        hide(BWriter::inv_w);
        hide(BWriter::vals);
        if self.cur_count > 0 {
            let ghost x = self.cur_count as u64;
            let n0 = self.buf.len();
            K::put_count(self.cur_count, &mut self.buf);
            let n1 = self.buf.len();
            proof {
                lemma_flush_fits::<K>(*old(self));
            }
            self.tail_byte = (n1 - n0) as u8;
            self.emitted = self.emitted + self.cur_count;
            self.segments = self.segments + 1;
            self.cur_value = !self.cur_value;
            self.cur_count = 0;
            proof {
                self.cb = Ghost(self.cb@.push(x));
                lemma_flush_run::<K>(*old(self), *self);
            }
        }
    }

    /// Close the chunk in hand and start a new slab.
    #[verifier::spinoff_prover]
    fn cut<K: CountOps>(&mut self)
        requires
            old(self).inv_w::<K>(), old(self).cur_count > 0,
            old(self).cb@.len() > 0, old(self).cb@.last() > 0,
        ensures
            final(self).inv::<K>(), final(self).vals() == old(self).vals(), final(self).overflowed,
            final(self).base@ == old(self).base@, final(self).max_segments == old(self).max_segments,
            final(self).cur_value == old(self).cur_value, final(self).cur_count == old(self).cur_count,
    {
        let ghost c0 = self.chunk();
        let ghost d0 = self.done@;
        proof {
            reveal(out_ok);
            assert(c0.last() == self.cb@.last());
            assert(counts_ok(c0));
            lemma_vals_len(c0, 0);
            lemma_chunk_len(c0, self.cur_count as nat, self.vals().len(), chunks_values(d0).len());
            lemma_concat::<K>(if self.overflowed { Seq::empty() } else { self.base@ }, self.cb@);
        }
        let mut bytes = Vec::new();
        core::mem::swap(&mut self.buf, &mut bytes);
        if !self.overflowed {
            self.overflowed = true;
            self.target = if self.max_segments / 2 < 1 { 1 } else { self.max_segments / 2 };
            self.out_bytes = bytes;
            self.out_emitted = self.emitted;
            self.out_segments = self.standing + self.segments;
            self.out_tail = self.tail_byte;
            self.standing = 0;
            proof {
                self.out_cb = self.cb;
            }
        } else {
            let s = new_bslab_vec(bytes, self.emitted, self.segments, self.tail_byte);
            let ghost o0 = self.overflow@;
            self.overflow.push(s);
            proof {
                assert(c0 =~= self.cb@);
                assert forall|i: int| 0 <= i < self.overflow@.len() implies {
                    let s = #[trigger] self.overflow@[i];
                    &&& bslab_bytes(s) == counts_bytes::<K>(d0.push(c0)[i + 1])
                    &&& counts_ok(d0.push(c0)[i + 1]) && d0.push(c0)[i + 1].len() > 0
                    &&& bslab_len(s) == counts_len(d0.push(c0)[i + 1])
                    &&& bslab_segments(s) == d0.push(c0)[i + 1].len()
                    &&& bslab_tail(s) == K::enc(d0.push(c0)[i + 1].last()).len()
                } by {
                    if i < o0.len() {
                        assert(self.overflow@[i] == o0[i]);
                        assert(d0.push(c0)[i + 1] == d0[i + 1]);
                    }
                }
            }
        }
        self.segments = 0;
        self.emitted = 0;
        self.tail_byte = 0;
        proof {
            self.done = Ghost(d0.push(c0));
            self.cb = Ghost(Seq::empty());
            assert(d0.push(c0).drop_last() =~= d0);
            assert(self.done@[0] == if d0.len() == 0 { c0 } else { d0[0] });
            assert(self.buf@ =~= counts_bytes::<K>(self.cb@));
        }
        // a fresh slab starts on a false run
        if self.cur_value {
            let n0 = self.buf.len();
            K::put_count(0, &mut self.buf);
            let n1 = self.buf.len();
            proof {
                K::axiom_enc(0, 0, Seq::empty(), Seq::empty());
                lemma_bytes_push::<K>(Seq::empty(), 0);
                assert(Seq::<u64>::empty().push(0) =~= seq![0u64]);
            }
            self.tail_byte = (n1 - n0) as u8;
            self.segments = 1;
            proof {
                self.cb = Ghost(seq![0u64]);
                assert(counts_bytes::<K>(Seq::<u64>::empty()) =~= Seq::<u8>::empty());
                assert(self.buf@ =~= counts_bytes::<K>(self.cb@));
            }
        }
        proof {
            reveal(out_ok);
            assert(self.chunk() =~= self.cb@);
            assert(vals_at(self.chunk(), 0) =~= Seq::<bool>::empty()) by {
                reveal_with_fuel(vals_at, 2);
                if self.cb@.len() == 1 {
                    assert(self.cb@.drop_last() =~= Seq::<u64>::empty());
                }
            }
            assert(vals_at(Seq::<u64>::empty(), 0) =~= Seq::<bool>::empty());
            assert(self.vals() =~= old(self).vals());
            lemma_counts_len_le(c0);
            assert(self.buf@ == counts_bytes::<K>(self.cb@));
            assert(self.segments == self.cb@.len() && self.emitted == counts_len(self.cb@));
            assert(self.cb@.len() > 0 ==> self.tail_byte == K::enc(self.cb@.last()).len());
            assert(counts_pre(self.chunk()));
            assert(self.cur_value == run_value(self.chunk().len() as int));
            assert(self.standing == 0);
            assert(self.target >= 1);
            assert(out_ok::<K>(self.overflowed, self.base@, self.done@, self.out_cb@, self.out_bytes@, self.out_emitted,
                self.out_segments, self.out_tail, self.overflow@));
        }
    }

    /// Append `n` items of `value`.
    #[verifier::spinoff_prover]
    pub(crate) fn push<K: CountOps>(&mut self, value: bool, n: usize)
        requires old(self).inv::<K>(), old(self).vals().len() + n <= usize::MAX,
        ensures
            final(self).inv::<K>(), final(self).vals() == old(self).vals() + run_of(n as nat, value),
            final(self).base@ == old(self).base@, final(self).max_segments == old(self).max_segments,
            old(self).overflowed ==> final(self).overflowed,
            n > 0 ==> final(self).cur_value == value && final(self).cur_count > 0,
            n == 0 ==> *final(self) == *old(self),
    {
        hide(BWriter::inv_w);
        hide(BWriter::vals);
        if n == 0 {
            proof {
                lemma_run_none(old(self).vals(), value);
            }
            return;
        }
        if value == self.cur_value {
            proof {
                lemma_push_same::<K>(*old(self), n);
            }
            self.cur_count = self.cur_count + n;
            return;
        }
        proof {
            lemma_pending::<K>(*old(self));
        }
        if self.cur_count == 0 && self.segments == 0 && !self.overflowed {
            // the rebuild starts the slab and its first run is true, so
            // the slab needs a zero-count false pad to start on
            let n0 = self.buf.len();
            K::put_count(0, &mut self.buf);
            let n1 = self.buf.len();
            proof {
                K::axiom_enc(0, 0, Seq::empty(), Seq::empty());
            }
            self.tail_byte = (n1 - n0) as u8;
            self.segments = self.segments + 1;
            proof {
                self.cb = Ghost(seq![0u64]);
                lemma_push_pad::<K>(*old(self), *self);
            }
        } else {
            self.flush_run::<K>();
            proof {
                assert(self.cb@.last() == old(self).cur_count as u64);
                assert(BWriter { cur_value: value, ..*self } == *self);
            }
        }
        let ghost w1 = BWriter { cur_value: value, ..*self };
        self.cur_value = value;
        self.cur_count = n;
        proof {
            lemma_push_start::<K>(w1, n);
        }
        if self.standing + self.segments >= self.target {
            self.cut::<K>();
        }
    }
}

/// Counts where only the first may be zero stay so with a run added.
proof fn lemma_pre_push(c: Seq<u64>, x: u64)
    requires counts_pre(c), x > 0,
    ensures counts_pre(c.push(x)),
{
    assert forall|i: int| 1 <= i < c.push(x).len() implies #[trigger] c.push(x)[i] > 0 by {
        if i < c.len() {
            assert(c.push(x)[i] == c[i]);
        }
    }
}

/// A pending run written into the chunk leaves the values as they were.
proof fn lemma_vals_flush(d: Seq<bool>, c: Seq<u64>, n: usize, v: bool)
    requires v == run_value(c.len() as int),
    ensures d + vals_at(c.push(n as u64), 0) + run_of(0, !v) == d + vals_at(c, 0) + run_of(n as nat, v),
{
    lemma_vals_push(c, n as u64);
    assert(run_of(0, !v) =~= Seq::<bool>::empty());
    assert(d + vals_at(c.push(n as u64), 0) + run_of(0, !v) =~= d + vals_at(c, 0) + run_of(n as nat, v));
}

/// Before a pending run is written out: its counters have room.
proof fn lemma_flush_fits<K: CountOps>(w: BWriter)
    requires w.inv_w::<K>(),
    ensures
        w.emitted + w.cur_count <= usize::MAX,
        w.segments <= w.buf@.len(),
        0 < K::enc(w.cur_count as u64).len() <= 10,
{
    let x = w.cur_count as u64;
    let h = if w.overflowed { Seq::<u64>::empty() } else { w.base@ };
    K::axiom_enc(x, x, Seq::empty(), Seq::empty());
    lemma_bytes_len::<K>(w.cb@);
    lemma_vals_len(w.chunk(), 0);
    lemma_concat::<K>(h, w.cb@);
    assert(w.vals().len() == chunks_values(w.done@).len() + counts_len(w.chunk()) + w.cur_count);
}

/// A pending run written out: the writer `w0` after `flush_run`'s
/// updates, `w`.
proof fn lemma_flush_run<K: CountOps>(w0: BWriter, w: BWriter)
    requires
        w0.inv_w::<K>(),
        w0.cur_count > 0,
        w0.segments < usize::MAX,
        w.buf@ == w0.buf@ + K::enc(w0.cur_count as u64),
        w == (BWriter {
            buf: w.buf,
            tail_byte: K::enc(w0.cur_count as u64).len() as u8,
            emitted: (w0.emitted + w0.cur_count) as usize,
            segments: (w0.segments + 1) as usize,
            cur_value: !w0.cur_value,
            cur_count: 0,
            cb: Ghost(w0.cb@.push(w0.cur_count as u64)),
            ..w0
        }),
    ensures
        w.inv_w::<K>(),
        w.vals() == w0.vals(),
{
    hide(counts_bytes);
    hide(counts_len);
    hide(counts_pre);
    hide(vals_at);
    hide(chunks_values);
    hide(run_of);
    let x = w0.cur_count as u64;
    let c0 = w0.chunk();
    lemma_flush_fits::<K>(w0);
    assert(w.chunk() =~= c0.push(x));
    lemma_bytes_push::<K>(w0.cb@, x);
    lemma_pre_push(c0, x);
    lemma_vals_flush(chunks_values(w0.done@), c0, w0.cur_count, w0.cur_value);
    assert(w0.cb@.push(x).last() == x);
}

/// Nothing appended.
proof fn lemma_run_none(a: Seq<bool>, v: bool)
    ensures a + run_of(0, v) == a,
{
    assert(a + run_of(0, v) =~= a);
}

/// A writer with nothing pending has written nothing.
proof fn lemma_pending<K: CountOps>(w: BWriter)
    requires w.inv::<K>(),
    ensures w.cur_count == 0 ==> w.segments == 0 && !w.overflowed,
{
}

/// More of the pending run's value joins it.
proof fn lemma_push_same<K: CountOps>(w0: BWriter, n: usize)
    requires
        w0.inv::<K>(),
        w0.vals().len() + n <= usize::MAX,
        n > 0,
    ensures
        w0.cur_count + n <= usize::MAX,
        ({
            let w = BWriter { cur_count: (w0.cur_count + n) as usize, ..w0 };
            w.inv::<K>() && w.vals() == w0.vals() + run_of(n as nat, w0.cur_value)
        }),
{
    let w = BWriter { cur_count: (w0.cur_count + n) as usize, ..w0 };
    assert(w.chunk() == w0.chunk());
    assert(run_of(w0.cur_count as nat, w0.cur_value) + run_of(n as nat, w0.cur_value) =~= run_of(
        (w0.cur_count + n) as nat,
        w0.cur_value,
    ));
    assert(w.vals() =~= w0.vals() + run_of(n as nat, w0.cur_value));
}

/// A zero-count false pad written into an empty writer `w0`, giving `w`:
/// a `true` run may start on it.
proof fn lemma_push_pad<K: CountOps>(w0: BWriter, w: BWriter)
    requires
        w0.inv::<K>(),
        w0.cur_count == 0,
        w.buf@ == w0.buf@ + K::enc(0),
        w == (BWriter { buf: w.buf, tail_byte: K::enc(0).len() as u8, segments: 1, cb: Ghost(seq![0u64]), ..w0 }),
    ensures
        !w0.cur_value,
        ({
            let w1 = BWriter { cur_value: true, ..w };
            w1.inv_w::<K>() && w1.vals() == w0.vals()
        }),
        w.standing + w.segments < w.target,
        !w.overflowed,
        w.cur_count == 0,
{
    let w1 = BWriter { cur_value: true, ..w };
    K::axiom_enc(0, 0, Seq::empty(), Seq::empty());
    assert(w0.base@ =~= Seq::<u64>::empty());
    assert(w0.cb@ =~= Seq::<u64>::empty());
    assert(w0.chunk() =~= Seq::<u64>::empty());
    assert(w1.chunk() =~= seq![0u64]);
    lemma_bytes_push::<K>(Seq::empty(), 0);
    assert(Seq::<u64>::empty().push(0) =~= seq![0u64]);
    assert(counts_bytes::<K>(Seq::<u64>::empty()) =~= Seq::<u8>::empty());
    assert(w1.buf@ =~= counts_bytes::<K>(w1.cb@));
    lemma_vals_push(Seq::empty(), 0);
    assert(vals_at(Seq::<u64>::empty(), 0) =~= Seq::<bool>::empty());
    assert(run_of(0, run_value(0)) =~= Seq::<bool>::empty());
    assert(vals_at(seq![0u64], 0) =~= Seq::<bool>::empty());
    assert(w1.vals() =~= w0.vals());
}

/// A run of `n` starts pending on a writer with none.
proof fn lemma_push_start<K: CountOps>(w1: BWriter, n: usize)
    requires
        w1.inv_w::<K>(),
        w1.cur_count == 0,
        n > 0,
        w1.vals().len() + n <= usize::MAX,
    ensures
        ({
            let w = BWriter { cur_count: n, ..w1 };
            &&& w.inv_w::<K>()
            &&& w.vals() == w1.vals() + run_of(n as nat, w1.cur_value)
            &&& w.standing + w.segments <= usize::MAX
        }),
{
    let w = BWriter { cur_count: n, ..w1 };
    assert(w.chunk() == w1.chunk());
    assert(run_of(0, w1.cur_value) =~= Seq::<bool>::empty());
    assert(w.vals() =~= w1.vals() + run_of(n as nat, w1.cur_value));
    lemma_vals_len(w.chunk(), 0);
    lemma_chunk_len(w.chunk(), n as nat, w.vals().len(), chunks_values(w.done@).len());
}

/// Counts where only the first may be zero number at most one more than
/// their total.
pub(crate) proof fn lemma_pre_len(cs: Seq<u64>)
    requires counts_pre(cs),
    ensures cs.len() <= counts_len(cs) + 1,
    decreases cs.len(),
{
    if cs.len() > 1 {
        assert(counts_pre(cs.drop_last())) by {
            assert forall|i: int| 1 <= i < cs.drop_last().len() implies #[trigger] cs.drop_last()[i] > 0 by {
                assert(cs.drop_last()[i] == cs[i]);
            }
        }
        lemma_pre_len(cs.drop_last());
        assert(cs[cs.len() - 1] > 0);
    }
}

/// The writer's chunk numbers no more segments than values written.
proof fn lemma_chunk_len(c: Seq<u64>, n: nat, vals_len: nat, done_len: nat)
    requires counts_pre(c), n >= 1, vals_len == done_len + counts_len(c) + n,
    ensures c.len() <= vals_len,
{
    lemma_pre_len(c);
}

/// A count is no more than counts' total.
pub(crate) proof fn lemma_counts_len_le(cs: Seq<u64>)
    ensures forall|i: int| 0 <= i < cs.len() ==> #[trigger] cs[i] <= counts_len(cs),
{
    assert forall|i: int| 0 <= i < cs.len() implies #[trigger] cs[i] <= counts_len(cs) by {
        lemma_count_le(cs, i);
    }
}

/// `v` then `s`.
#[inline(always)]
fn extend_vec(v: &mut Vec<u8>, s: &[u8])
    ensures final(v)@ == old(v)@ + s@,
{
    let ghost v0 = v@;
    v.extend_from_slice(s);
    proof {
        assert forall|i: int| 0 <= i < v@.len() implies #[trigger] v@[i] == (v0 + s@)[i] by {
            if i >= v0.len() {
                assert(cloned::<u8>(s@[i - v0.len()], v@[i]));
            }
        }
        assert(v@ =~= v0 + s@);
    }
}

/// A slab of `data`.
#[inline(always)]
pub(crate) fn new_bslab_vec(data: Vec<u8>, len: usize, segments: usize, tail: u8) -> (s: BSlab)
    ensures bslab_bytes(s) == data@, bslab_len(s) == len, bslab_segments(s) == segments, bslab_tail(s) == tail,
{
    proof {
        reveal(bslab_bytes);
        reveal(bslab_len);
        reveal(bslab_segments);
        reveal(bslab_tail);
    }
    crate::column::Slab { data, len, segments, tail }
}

// ── The rebuild ─────────────────────────────────────────────────────────────

/// Values up to the end of run `i`: the first `i` counts' values.
pub(crate) proof fn lemma_values_take(cs: Seq<u64>, i: int)
    requires 0 <= i <= cs.len(),
    ensures
        counts_len(cs.take(i)) <= counts_values(cs).len(),
        counts_values(cs).take(counts_len(cs.take(i)) as int) == vals_at(cs.take(i), 0),
{
    assert(cs =~= cs.take(i) + cs.skip(i));
    lemma_values_prefix(cs.take(i), cs.skip(i));
    lemma_values_len(cs.take(i));
    lemma_vals_at_zero(cs.take(i));
}

/// Run `i` in values, `off` into it.
pub(crate) proof fn lemma_values_run_take(cs: Seq<u64>, i: int, off: int)
    requires 0 <= i < cs.len(), 0 <= off <= cs[i],
    ensures
        counts_len(cs.take(i)) + off <= counts_values(cs).len(),
        counts_values(cs).take(counts_len(cs.take(i)) + off)
            == vals_at(cs.take(i), 0) + run_of(off as nat, run_value(i)),
{
    lemma_values_take(cs, i + 1);
    lemma_values_take(cs, i);
    assert(cs.take(i + 1).drop_last() =~= cs.take(i));
    let a = counts_len(cs.take(i)) as int;
    let v = counts_values(cs);
    assert(v.take(a + off) =~= v.take(counts_len(cs.take(i + 1)) as int).take(a + off));
    assert(vals_at(cs.take(i + 1), 0) == vals_at(cs.take(i), 0) + run_of(cs[i] as nat, run_value(i)));
    lemma_vals_len(cs.take(i), 0);
    assert(v.take(a + off) =~= vals_at(cs.take(i), 0) + run_of(off as nat, run_value(i)));
}

/// A slab's values, split after `k` runs.
pub(crate) proof fn lemma_values_split(cs: Seq<u64>, k: int)
    requires 0 <= k <= cs.len(),
    ensures counts_values(cs) == vals_at(cs.take(k), 0) + vals_at(cs.skip(k), k),
{
    assert(cs =~= cs.take(k) + cs.skip(k));
    lemma_vals_at_concat(cs.take(k), cs.skip(k), 0);
    lemma_vals_at_zero(cs);
}

/// Counts' values, split after their first run.
pub(crate) proof fn lemma_vals_first(cs: Seq<u64>, p: int)
    requires cs.len() > 0,
    ensures vals_at(cs, p) == run_of(cs[0] as nat, run_value(p)) + vals_at(cs.skip(1), p + 1),
{
    assert(cs =~= seq![cs[0]] + cs.skip(1));
    lemma_vals_at_concat(seq![cs[0]], cs.skip(1), p);
    lemma_vals_single(cs[0], p);
}

/// What follows the reader: the rest of the run it stands in (`partial`
/// of `value`), then the slab's counts from run `j` on, as bytes.
pub(crate) struct BTail {
    pub(crate) value: bool,
    pub(crate) partial: usize,
    /// where the carried bytes begin
    pub(crate) start: usize,
    /// items in the carried bytes, not counting `partial`
    pub(crate) items: usize,
    pub(crate) segments: usize,
}

pub(crate) open spec fn tail_ok<K: CountOps>(data: Seq<u8>, read: int, t: BTail, j: int) -> bool {
    let cs = bool_counts::<K>(data);
    let rc = cs.skip(j);
    &&& 0 <= j <= cs.len()
    &&& t.start == counts_bytes::<K>(cs.take(j)).len()
    &&& t.items == counts_len(rc) && t.segments == rc.len()
    &&& t.partial == 0 ==> j == cs.len()
    &&& t.partial > 0 ==> j >= 1 && run_value(j - 1) == t.value
    &&& rc.len() > 0 ==> j >= 1 && rc[0] > 0
    &&& forall|i: int| 0 <= i < rc.len() ==> #[trigger] rc[i] > 0
    &&& 0 <= read <= bool_values::<K>(data).len()
    &&& bool_values::<K>(data).skip(read) == run_of(t.partial as nat, t.value) + vals_at(rc, j)
}

/// Chunks' values: the first's, then the rest's.
pub(crate) proof fn lemma_chunks_first(d: Seq<Seq<u64>>)
    requires d.len() > 0,
    ensures chunks_values(d) == vals_at(d[0], 0) + chunks_values(d.skip(1)),
    decreases d.len(),
{
    if d.len() == 1 {
        assert(d.drop_last() =~= Seq::<Seq<u64>>::empty());
        assert(d.skip(1) =~= Seq::<Seq<u64>>::empty());
        reveal_with_fuel(chunks_values, 2);
        assert(chunks_values(d) =~= vals_at(d[0], 0) + chunks_values(d.skip(1)));
    } else {
        lemma_chunks_first(d.drop_last());
        assert(d.drop_last().skip(1) =~= d.skip(1).drop_last());
        assert(d.skip(1).last() == d.last());
        assert(chunks_values(d) =~= vals_at(d[0], 0) + chunks_values(d.skip(1)));
    }
}

/// A chunk's values are among all of the chunks'.
pub(crate) proof fn lemma_chunk_le(d: Seq<Seq<u64>>, i: int)
    requires 0 <= i < d.len(),
    ensures vals_at(d[i], 0).len() <= chunks_values(d).len(),
    decreases d.len(),
{
    if i < d.len() - 1 {
        lemma_chunk_le(d.drop_last(), i);
        assert(d.drop_last()[i] == d[i]);
    }
}

/// Slabs whose bytes encode chunks hold the chunks' values.
pub(crate) proof fn lemma_slabs_chunks<K: CountOps>(s: Seq<BSlab>, d: Seq<Seq<u64>>)
    requires
        s.len() == d.len(),
        forall|i: int| 0 <= i < s.len() ==> bslab_bytes(#[trigger] s[i]) == counts_bytes::<K>(d[i])
            && counts_ok(d[i]) && counts_len(d[i]) <= usize::MAX,
    ensures bslabs_values::<K>(s) == chunks_values(d),
    decreases s.len(),
{
    if s.len() > 0 {
        let n = s.len() - 1;
        lemma_slabs_chunks::<K>(s.drop_last(), d.drop_last());
        assert(bslab_bytes(s[n]) == counts_bytes::<K>(d[n]));
        lemma_counts::<K>(d[n], bslab_bytes(s[n]));
        lemma_vals_at_zero(d[n]);
        assert(s.last() == s[n] && d.last() == d[n]);
    }
}

/// A rebuild closed: the bytes replacing `start..end` in the slab, its
/// metadata after, and the slabs it spilled into.
pub(crate) struct BClosed {
    pub(crate) bytes: Vec<u8>,
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) len: usize,
    pub(crate) segments: usize,
    pub(crate) tail: u8,
    pub(crate) overflow: Vec<BSlab>,
}

/// `r` rebuilds the slab `data` into slabs holding `out`.
pub(crate) open spec fn closed_ok<K: CountOps>(r: BClosed, data: Seq<u8>, out: Seq<bool>) -> bool {
    let rs = data.take(r.start as int) + r.bytes@ + data.skip(r.end as int);
    let cr = bool_counts::<K>(rs);
    &&& r.start <= r.end <= data.len()
    &&& bool_valid::<K>(rs)
    &&& r.len == counts_len(cr) && r.segments == cr.len() && r.tail == counts_tail::<K>(cr)
    &&& forall|i: int| 0 <= i < r.overflow@.len() ==> bslab_ok::<K>(#[trigger] r.overflow@[i])
    &&& bool_values::<K>(rs) + bslabs_values::<K>(r.overflow@) == out
}

/// Counts all above zero number no more than their total.
pub(crate) proof fn lemma_rc_len(rc: Seq<u64>)
    requires forall|i: int| 0 <= i < rc.len() ==> #[trigger] rc[i] > 0,
    ensures rc.len() <= counts_len(rc),
    decreases rc.len(),
{
    if rc.len() > 0 {
        assert forall|i: int| 0 <= i < rc.drop_last().len() implies #[trigger] rc.drop_last()[i] > 0 by {
            assert(rc.drop_last()[i] == rc[i]);
        }
        lemma_rc_len(rc.drop_last());
        assert(rc[rc.len() - 1] > 0);
    }
}

/// The tail as a slab of its own: a false pad if it would start true,
/// the partial run, then the carried bytes. `None` if it holds nothing.
#[verifier::spinoff_prover]
fn into_slab<K: CountOps>(t: &BTail, data: &[u8], old_tail: u8, Ghost(read): Ghost<int>, Ghost(j): Ghost<int>) -> (r: Option<BSlab>)
    requires
        tail_ok::<K>(data@, read, *t, j), bool_valid::<K>(data@),
        old_tail == counts_tail::<K>(bool_counts::<K>(data@)),
    ensures
        match r {
            None => bool_values::<K>(data@).skip(read) == Seq::<bool>::empty(),
            Some(s) => bslab_ok::<K>(s) && bool_values::<K>(bslab_bytes(s)) == bool_values::<K>(data@).skip(read),
        },
{
    let ghost cs = bool_counts::<K>(data@);
    let ghost rc = cs.skip(j);
    proof {
        lemma_counts::<K>(cs, data@);
        lemma_values_len(cs);
        lemma_vals_len(rc, j);
        assert(cs =~= cs.take(j) + rc);
        lemma_concat::<K>(cs.take(j), rc);
        lemma_take_bytes_le::<K>(cs, j);
        lemma_rc_len(rc);
    }
    let mut buf: Vec<u8> = Vec::new();
    let mut segments = 0usize;
    let mut tail = 0u8;
    let ghost mut sc = Seq::<u64>::empty();
    if t.value && t.partial > 0 {
        K::put_count(0, &mut buf);
        segments = segments + 1;
        proof {
            K::axiom_enc(0, 0, Seq::empty(), Seq::empty());
            lemma_bytes_push::<K>(sc, 0);
            sc = sc.push(0);
        }
        tail = buf.len() as u8;
    }
    if t.partial > 0 {
        let n0 = buf.len();
        K::put_count(t.partial, &mut buf);
        let n1 = buf.len();
        proof {
            K::axiom_enc(t.partial as u64, t.partial as u64, Seq::empty(), Seq::empty());
            lemma_bytes_push::<K>(sc, t.partial as u64);
            sc = sc.push(t.partial as u64);
        }
        tail = (n1 - n0) as u8;
        segments = segments + 1;
    }
    if t.segments > 0 {
        tail = old_tail;
    }
    let rest = &data[t.start..];
    assert(rest@ == data@.skip(t.start as int));
    proof {
        assert(rest@ =~= counts_bytes::<K>(rc));
        assert(counts_bytes::<K>(Seq::<u64>::empty()) =~= Seq::<u8>::empty());
    }
    extend_vec(&mut buf, rest);
    let _total = buf.len();
    proof {
        let v = bool_values::<K>(data@).skip(read);
        assert(v.len() == t.partial + counts_len(rc));
        lemma_concat::<K>(sc, rc);
        lemma_bytes_len::<K>(sc + rc);
    }
    let len = t.partial + t.items;
    if len == 0 {
        proof {
            assert(rc.len() == 0) by {
                lemma_rc_len(rc);
            }
            assert(bool_values::<K>(data@).skip(read) =~= Seq::<bool>::empty());
        }
        return None;
    }
    let ghost full = sc + rc;
    proof {
        lemma_concat::<K>(sc, rc);
        let p = sc.len() as int;
        // the partial run is run `p - 1` of the new slab, so the carried
        // runs keep their parity
        if t.partial > 0 {
            assert(run_value(p - 1) == t.value);
            lemma_vals_push(sc.drop_last(), t.partial as u64);
            assert(sc.drop_last().push(t.partial as u64) =~= sc);
            if t.value {
                assert(sc.drop_last() =~= seq![0u64]);
                lemma_vals_single(0, 0);
                assert(vals_at(seq![0u64], 0) =~= Seq::<bool>::empty());
            } else {
                assert(sc.drop_last() =~= Seq::<u64>::empty());
            }
            assert(vals_at(sc, 0) == run_of(t.partial as nat, t.value));
            if rc.len() > 0 {
                lemma_vals_at_parity(rc, p, j);
            }
        } else {
            assert(rc.len() == 0);
        }
        if rc.len() == 0 {
            assert(vals_at(rc, p) =~= Seq::<bool>::empty());
            assert(vals_at(rc, j) =~= Seq::<bool>::empty());
            assert(full =~= sc);
        }
        lemma_vals_at_concat(sc, rc, 0);
        assert(counts_ok(full)) by {
            assert forall|i: int| 1 <= i < full.len() implies #[trigger] full[i] > 0 by {
                if i >= sc.len() {
                    assert(full[i] == rc[i - sc.len()]);
                } else {
                    assert(full[i] == sc[i]);
                }
            }
            if rc.len() > 0 {
                assert(full.last() == rc.last());
            }
        }
        lemma_vals_len(full, 0);
        lemma_vals_at_zero(full);
        lemma_counts::<K>(full, buf@);
        if rc.len() > 0 {
            assert(full.last() == rc.last());
            assert(rc.last() == cs.last());
        }
    }
    let s = new_bslab_vec(buf, len, segments + t.segments, tail);
    proof {
        lemma_counts::<K>(full, bslab_bytes(s));
    }
    Some(s)
}

/// The writer's values number the chunks' and the pending run's.
pub(crate) proof fn lemma_chunks_len(d: Seq<Seq<u64>>, c: Seq<u64>, n: nat, v: bool)
    ensures (chunks_values(d) + vals_at(c, 0) + run_of(n, v)).len() == chunks_values(d).len() + counts_len(c) + n,
{
    lemma_vals_len(c, 0);
}

/// A slab's bytes, split where run `j` starts.
pub(crate) proof fn lemma_split_at<K: CountOps>(data: Seq<u8>, cs: Seq<u64>, j: int)
    requires bool_parses::<K>(cs, data), 0 <= j <= cs.len(),
    ensures
        ({
            let n = counts_bytes::<K>(cs.take(j)).len() as int;
            &&& n <= data.len()
            &&& data.take(n) == counts_bytes::<K>(cs.take(j))
            &&& data.skip(n) == counts_bytes::<K>(cs.skip(j))
            &&& (n < data.len()) == (j < cs.len())
            &&& counts_len(cs) == counts_len(cs.take(j)) + counts_len(cs.skip(j))
        }),
{
    let a = counts_bytes::<K>(cs.take(j));
    let b = counts_bytes::<K>(cs.skip(j));
    assert(cs =~= cs.take(j) + cs.skip(j));
    lemma_concat::<K>(cs.take(j), cs.skip(j));
    lemma_bytes_len::<K>(cs.skip(j));
    assert(data == a + b);
    assert(data.take(a.len() as int) =~= a);
    assert(data.skip(a.len() as int) =~= b);
}

/// Counts written with counts carried after them are canonical, when
/// the last written is above zero.
proof fn lemma_join_ok(f: Seq<u64>, rc: Seq<u64>)
    requires
        counts_pre(f),
        forall|i: int| 0 <= i < rc.len() ==> #[trigger] rc[i] > 0,
        f.len() > 0 ==> f.last() > 0,
    ensures
        counts_ok(f + rc),
        rc.len() > 0 ==> (f + rc).last() == rc.last(),
        rc.len() == 0 ==> f + rc == f,
{
    let fr = f + rc;
    assert forall|i: int| 1 <= i < fr.len() implies #[trigger] fr[i] > 0 by {
        if i >= f.len() {
            assert(fr[i] == rc[i - f.len()]);
        } else {
            assert(fr[i] == f[i]);
        }
    }
    if rc.len() == 0 {
        assert(fr =~= f);
    } else {
        assert(fr.last() == rc[rc.len() - 1]);
    }
}

/// Their values: the written counts', then the carried counts' as they
/// were from run `j` on.
proof fn lemma_join_vals(f: Seq<u64>, rc: Seq<u64>, j: int)
    requires 0 <= j, rc.len() > 0 ==> run_value(f.len() as int) == run_value(j),
    ensures counts_values(f + rc) == vals_at(f, 0) + vals_at(rc, j),
{
    lemma_vals_at_zero(f + rc);
    lemma_vals_at_concat(f, rc, 0);
    if rc.len() > 0 {
        lemma_vals_at_parity(rc, f.len() as int, j);
    }
}

/// The head's counts, the counts written and the counts carried number
/// no more than their values and one.
proof fn lemma_first_sizes<K: CountOps>(base: Seq<u64>, cb: Seq<u64>, rc: Seq<u64>)
    requires
        counts_pre(base + cb),
        forall|i: int| 0 <= i < rc.len() ==> #[trigger] rc[i] > 0,
    ensures
        counts_len(base + cb) == counts_len(base) + counts_len(cb),
        base.len() + cb.len() + rc.len() <= counts_len(base) + counts_len(cb) + counts_len(rc) + 1,
{
    lemma_concat::<K>(base, cb);
    lemma_pre_len(base + cb);
    lemma_rc_len(rc);
}

/// The slab rebuilt when nothing has spilled: the head, the counts
/// written after it, and the slab's counts from run `j` on.
proof fn lemma_sealed_first<K: CountOps>(r: BClosed, data: Seq<u8>, j: int, base: Seq<u64>, cb: Seq<u64>, tail_byte: u8,
    slab_tail: u8)
    requires
        bool_valid::<K>(data),
        ({
            let cs = bool_counts::<K>(data);
            let rc = cs.skip(j);
            let f = base + cb;
            &&& 0 <= j <= cs.len()
            &&& base.len() <= j && base == cs.take(base.len() as int)
            &&& counts_pre(f)
            &&& forall|i: int| 0 <= i < rc.len() ==> #[trigger] rc[i] > 0
            &&& rc.len() > 0 ==> run_value(f.len() as int) == run_value(j)
            &&& f.len() > 0 ==> f.last() > 0 && cb.len() > 0
            &&& cb.len() > 0 ==> tail_byte == K::enc(cb.last()).len()
            &&& cb.len() == 0 ==> tail_byte == 0
            &&& slab_tail == counts_tail::<K>(cs)
            &&& counts_len(f) + counts_len(rc) <= usize::MAX
            &&& r.start == counts_bytes::<K>(base).len()
            &&& r.end == counts_bytes::<K>(cs.take(j)).len()
            &&& r.bytes@ == counts_bytes::<K>(cb)
            &&& r.len == counts_len(base) + counts_len(cb) + counts_len(rc)
            &&& r.segments == base.len() + cb.len() + rc.len()
            &&& r.tail == if r.end < data.len() { slab_tail } else { tail_byte }
            &&& r.overflow@.len() == 0
        }),
    ensures
        closed_ok::<K>(r, data, vals_at(base + cb, 0) + vals_at(bool_counts::<K>(data).skip(j), j)),
{
    let cs = bool_counts::<K>(data);
    let rc = cs.skip(j);
    let f = base + cb;
    let fr = f + rc;
    let rs = data.take(r.start as int) + r.bytes@ + data.skip(r.end as int);
    lemma_split_at::<K>(data, cs, j);
    lemma_split_at::<K>(data, cs, base.len() as int);
    if base.len() < j {
        lemma_take_bytes_mono::<K>(cs, base.len() as int, j);
    }
    lemma_concat::<K>(base, cb);
    lemma_concat::<K>(f, rc);
    assert(rs == counts_bytes::<K>(fr));
    lemma_join_ok(f, rc);
    lemma_counts::<K>(fr, rs);
    lemma_join_vals(f, rc, j);
    if f.len() > 0 {
        assert(f.last() == cb.last());
    }
    assert(r.tail == counts_tail::<K>(fr));
    assert(bool_values::<K>(rs) + bslabs_values::<K>(r.overflow@) =~= vals_at(f, 0) + vals_at(rc, j));
}

/// The first chunk cut: the head's counts and those written after
/// them, a slab's worth.
proof fn lemma_out_first<K: CountOps>(base: Seq<u64>, d: Seq<Seq<u64>>, out_cb: Seq<u64>, out_bytes: Seq<u8>, out_emitted: usize,
    out_segments: usize, out_tail: u8, o: Seq<BSlab>)
    requires out_ok::<K>(true, base, d, out_cb, out_bytes, out_emitted, out_segments, out_tail, o),
    ensures
        d.len() >= 1,
        d[0] == base + out_cb,
        counts_ok(d[0]),
        out_bytes == counts_bytes::<K>(out_cb),
        out_emitted == counts_len(out_cb),
        out_segments == d[0].len(),
        out_tail == counts_tail::<K>(d[0]),
        counts_len(d[0]) == counts_len(base) + counts_len(out_cb),
        counts_len(d[0]) <= chunks_values(d).len(),
{
    reveal(out_ok);
    lemma_concat::<K>(base, out_cb);
    lemma_chunk_le(d, 0);
    lemma_vals_len(d[0], 0);
}

/// The chunks cut after the first are valid slabs, which hold those
/// chunks' values.
proof fn lemma_cut_slabs<K: CountOps>(base: Seq<u64>, d: Seq<Seq<u64>>, out_cb: Seq<u64>, out_bytes: Seq<u8>, out_emitted: usize,
    out_segments: usize, out_tail: u8, o: Seq<BSlab>)
    requires
        out_ok::<K>(true, base, d, out_cb, out_bytes, out_emitted, out_segments, out_tail, o),
        chunks_values(d).len() <= usize::MAX,
    ensures
        forall|i: int| 0 <= i < o.len() ==> bslab_ok::<K>(#[trigger] o[i]),
        bslabs_values::<K>(o) == chunks_values(d.skip(1)),
{
    reveal(out_ok);
    let ds = d.skip(1);
    assert forall|i: int| 0 <= i < o.len() implies bslab_bytes(#[trigger] o[i]) == counts_bytes::<K>(ds[i]) && counts_ok(ds[i])
        && counts_len(ds[i]) <= usize::MAX && bslab_ok::<K>(o[i]) by {
        assert(ds[i] == d[i + 1]);
        lemma_chunk_le(d, i + 1);
        lemma_vals_len(d[i + 1], 0);
        lemma_counts::<K>(ds[i], bslab_bytes(o[i]));
    }
    lemma_slabs_chunks::<K>(o, ds);
}

/// The first chunk in the slab's place: the head's bytes, then the
/// bytes of the chunk's counts after the head.
proof fn lemma_first_chunk<K: CountOps>(data: Seq<u8>, base: Seq<u64>, out_cb: Seq<u64>)
    requires
        bool_valid::<K>(data),
        base.len() <= bool_counts::<K>(data).len(),
        base == bool_counts::<K>(data).take(base.len() as int),
        counts_ok(base + out_cb),
        counts_len(base + out_cb) <= usize::MAX,
    ensures
        ({
            let n = counts_bytes::<K>(base).len() as int;
            let rs = data.take(n) + counts_bytes::<K>(out_cb) + data.skip(data.len() as int);
            &&& n <= data.len()
            &&& bool_valid::<K>(rs)
            &&& bool_counts::<K>(rs) == base + out_cb
        }),
{
    let cs = bool_counts::<K>(data);
    let n = counts_bytes::<K>(base).len() as int;
    let rs = data.take(n) + counts_bytes::<K>(out_cb) + data.skip(data.len() as int);
    lemma_split_at::<K>(data, cs, base.len() as int);
    lemma_concat::<K>(base, out_cb);
    assert(rs =~= counts_bytes::<K>(base + out_cb));
    lemma_counts::<K>(base + out_cb, rs);
}

/// The chunk in hand with the slab's counts from run `j` on, as a
/// slab of its own.
proof fn lemma_last_slab<K: CountOps>(s: BSlab, data: Seq<u8>, j: int, cb: Seq<u64>, tail_byte: u8, slab_tail: u8)
    requires
        bool_valid::<K>(data),
        ({
            let cs = bool_counts::<K>(data);
            let rc = cs.skip(j);
            let n = counts_bytes::<K>(cs.take(j)).len();
            &&& 0 <= j <= cs.len()
            &&& counts_pre(cb) && cb.len() > 0 && cb.last() > 0
            &&& forall|i: int| 0 <= i < rc.len() ==> #[trigger] rc[i] > 0
            &&& rc.len() > 0 ==> run_value(cb.len() as int) == run_value(j)
            &&& tail_byte == K::enc(cb.last()).len()
            &&& slab_tail == counts_tail::<K>(cs)
            &&& counts_len(cb) + counts_len(rc) <= usize::MAX
            &&& bslab_bytes(s) == counts_bytes::<K>(cb) + data.skip(n as int)
            &&& bslab_len(s) == counts_len(cb) + counts_len(rc)
            &&& bslab_segments(s) == cb.len() + rc.len()
            &&& bslab_tail(s) == if n < data.len() { slab_tail } else { tail_byte }
        }),
    ensures
        bslab_ok::<K>(s),
        bool_values::<K>(bslab_bytes(s)) == vals_at(cb, 0) + vals_at(bool_counts::<K>(data).skip(j), j),
{
    let cs = bool_counts::<K>(data);
    let rc = cs.skip(j);
    let fr = cb + rc;
    lemma_split_at::<K>(data, cs, j);
    lemma_concat::<K>(cb, rc);
    lemma_join_ok(cb, rc);
    lemma_counts::<K>(fr, bslab_bytes(s));
    lemma_join_vals(cb, rc, j);
    assert(bslab_tail(s) == counts_tail::<K>(fr));
}

/// The slab rebuilt when chunks have spilled: the first chunk in the
/// slab's place from the head on, the chunks cut since as slabs, and
/// the chunk in hand with the slab's counts from run `j` on as the
/// last slab `s`.
proof fn lemma_sealed_spilled<K: CountOps>(r: BClosed, data: Seq<u8>, j: int, w: BWriter, s: BSlab, slab_tail: u8)
    requires
        w.overflowed,
        w.inv_w::<K>(),
        bool_valid::<K>(data),
        ({
            let cs = bool_counts::<K>(data);
            let rc = cs.skip(j);
            let n = counts_bytes::<K>(cs.take(j)).len();
            &&& 0 <= j <= cs.len()
            &&& forall|i: int| 0 <= i < rc.len() ==> #[trigger] rc[i] > 0
            &&& rc.len() > 0 ==> run_value(w.chunk().len() as int) == run_value(j)
            &&& w.cb@.len() > 0 && w.chunk().last() > 0
            &&& w.base@.len() <= cs.len() && w.base@ == cs.take(w.base@.len() as int)
            &&& slab_tail == counts_tail::<K>(cs)
            &&& chunks_values(w.done@).len() + counts_len(w.chunk()) + counts_len(rc) <= usize::MAX
            &&& bslab_bytes(s) == w.buf@ + data.skip(n as int)
            &&& bslab_len(s) == w.emitted + counts_len(rc)
            &&& bslab_segments(s) == w.segments + rc.len()
            &&& bslab_tail(s) == if n < data.len() { slab_tail } else { w.tail_byte }
            &&& r.start == counts_bytes::<K>(w.base@).len()
            &&& r.end == data.len()
            &&& r.bytes@ == w.out_bytes@
            &&& r.len == counts_len(w.base@) + w.out_emitted
            &&& r.segments == w.out_segments
            &&& r.tail == w.out_tail
            &&& r.overflow@ == w.overflow@.push(s)
        }),
    ensures
        closed_ok::<K>(r, data, chunks_values(w.done@) + vals_at(w.chunk(), 0) + vals_at(bool_counts::<K>(data).skip(j), j)),
{
    let rc = bool_counts::<K>(data).skip(j);
    let d = w.done@;
    let base = w.base@;
    let cb = w.cb@;
    let f = w.chunk();
    let o0 = w.overflow@;
    let o = r.overflow@;
    let rs = data.take(r.start as int) + r.bytes@ + data.skip(r.end as int);
    assert(f =~= cb);
    // the first chunk, in the slab's place
    lemma_out_first::<K>(base, d, w.out_cb@, w.out_bytes@, w.out_emitted, w.out_segments, w.out_tail, o0);
    lemma_first_chunk::<K>(data, base, w.out_cb@);
    lemma_vals_at_zero(d[0]);
    // the slabs: those cut after the first, then the last
    lemma_cut_slabs::<K>(base, d, w.out_cb@, w.out_bytes@, w.out_emitted, w.out_segments, w.out_tail, o0);
    lemma_last_slab::<K>(s, data, j, cb, w.tail_byte, slab_tail);
    assert(o.drop_last() =~= o0);
    assert forall|i: int| 0 <= i < o.len() implies bslab_ok::<K>(#[trigger] o[i]) by {
        if i < o0.len() {
            assert(o[i] == o0[i]);
        }
    }
    lemma_chunks_first(d);
    assert(bool_values::<K>(rs) + bslabs_values::<K>(o) =~= chunks_values(d) + vals_at(f, 0) + vals_at(rc, j));
}

/// Where a reader stands in a slab: `k` runs started, the bytes of as
/// many counts behind it.
proof fn lemma_reader_at<K: CountOps>(data: Seq<u8>, st: BoolDecoderState)
    requires bstate_ok::<K>(data, st),
    ensures
        ({
            let cs = bool_counts::<K>(data);
            let k = bstate_k::<K>(data, st);
            &&& bool_parses::<K>(cs, data)
            &&& dstate_at::<K>(cs, st, k)
            &&& bool_values::<K>(data).len() == counts_len(cs)
            &&& counts_len(cs) == counts_len(cs.take(k)) + counts_len(cs.skip(k))
            &&& st.remaining <= counts_len(cs.take(k))
            &&& st.byte_pos <= data.len()
            &&& (st.byte_pos < data.len()) == (k < cs.len())
        }),
{
    let cs = bool_counts::<K>(data);
    let k = bstate_k::<K>(data, st);
    assert(bool_parses::<K>(cs, data));
    assert(dstate_at::<K>(cs, st, k));
    lemma_values_len(cs);
    lemma_split_at::<K>(data, cs, k);
    if k > 0 {
        lemma_count_le(cs.take(k), k - 1);
    }
}

/// Values that end in a run, with more after: from `m` before the
/// run's end on, `m` of the run's value and what is after.
proof fn lemma_skip_run(a: Seq<bool>, n: nat, m: nat, v: bool, b: Seq<bool>)
    requires m <= n,
    ensures (a + run_of(n, v) + b).skip(a.len() + n - m) == run_of(m, v) + b,
{
    assert((a + run_of(n, v) + b).skip(a.len() + n - m) =~= run_of(m, v) + b);
}

/// A slab's counts from run `j` on are above zero, when `j` is not
/// the first.
proof fn lemma_rest_positive(cs: Seq<u64>, j: int)
    requires counts_ok(cs), 1 <= j <= cs.len(),
    ensures forall|i: int| 0 <= i < cs.skip(j).len() ==> #[trigger] cs.skip(j)[i] > 0,
{
    let rc = cs.skip(j);
    assert forall|i: int| 0 <= i < rc.len() implies #[trigger] rc[i] > 0 by {
        assert(rc[i] == cs[j + i]);
    }
}

/// What follows a reader inside a run: the rest of the run, then the
/// runs after it.
proof fn lemma_tail_inside<K: CountOps>(data: Seq<u8>, st: BoolDecoderState, t: BTail)
    requires
        bstate_ok::<K>(data, st),
        st.remaining > 0,
        t.value == st.value && t.partial == st.remaining && t.start == st.byte_pos,
        ({
            let cs = bool_counts::<K>(data);
            let k = bstate_k::<K>(data, st);
            t.items == counts_len(cs.skip(k)) && t.segments == cs.len() - k
        }),
    ensures
        tail_ok::<K>(data, bstate_pos::<K>(data, st), t, bstate_k::<K>(data, st)),
{
    let cs = bool_counts::<K>(data);
    let k = bstate_k::<K>(data, st);
    let rc = cs.skip(k);
    lemma_reader_at::<K>(data, st);
    lemma_rest_positive(cs, k);
    lemma_values_split(cs, k);
    let h = cs.take(k - 1);
    assert(cs.take(k) =~= h.push(cs[k - 1]));
    lemma_vals_push(h, cs[k - 1]);
    lemma_bytes_push::<K>(h, cs[k - 1]);
    lemma_vals_len(h, 0);
    lemma_skip_run(vals_at(h, 0), cs[k - 1] as nat, st.remaining as nat, st.value, vals_at(rc, k));
}

/// A reader between runs with bytes ahead of it stands before run `k`,
/// whose count is the next thing in the bytes.
proof fn lemma_tail_next<K: CountOps>(data: Seq<u8>, st: BoolDecoderState)
    requires bstate_ok::<K>(data, st), st.byte_pos < data.len(),
    ensures
        ({
            let cs = bool_counts::<K>(data);
            let k = bstate_k::<K>(data, st);
            &&& k < cs.len() && cs[k] <= usize::MAX
            &&& K::enc(cs[k]).is_prefix_of(data.skip(st.byte_pos as int))
        }),
{
    let cs = bool_counts::<K>(data);
    let k = bstate_k::<K>(data, st);
    lemma_reader_at::<K>(data, st);
    lemma_next_count::<K>(data, cs, k, st.byte_pos as int);
}

/// The count read there is run `k`'s, and the reader is then past
/// `k + 1` counts.
proof fn lemma_tail_count<K: CountOps>(data: Seq<u8>, st: BoolDecoderState, cb: int, count: u64)
    requires
        bstate_ok::<K>(data, st),
        st.byte_pos < data.len(),
        0 <= cb <= data.len() - st.byte_pos,
        data.skip(st.byte_pos as int).take(cb) == K::enc(count),
    ensures
        ({
            let cs = bool_counts::<K>(data);
            let k = bstate_k::<K>(data, st);
            &&& k < cs.len()
            &&& count == cs[k]
            &&& st.byte_pos + cb == counts_bytes::<K>(cs.take(k + 1)).len()
            &&& counts_len(cs) == counts_len(cs.take(k)) + count + counts_len(cs.skip(k + 1))
        }),
{
    let cs = bool_counts::<K>(data);
    let k = bstate_k::<K>(data, st);
    let rest = data.skip(st.byte_pos as int);
    lemma_reader_at::<K>(data, st);
    lemma_next_count::<K>(data, cs, k, st.byte_pos as int);
    assert(rest =~= rest.take(cb) + rest.skip(cb));
    K::axiom_enc(count, cs[k], rest.skip(cb), counts_bytes::<K>(cs.skip(k + 1)));
    lemma_split_at::<K>(data, cs, k + 1);
    assert(cs.take(k + 1) =~= cs.take(k).push(cs[k]));
    lemma_bytes_push::<K>(cs.take(k), cs[k]);
}

/// What follows a reader between runs, once run `k`'s count is read:
/// that run, then the runs after it.
proof fn lemma_tail_between<K: CountOps>(data: Seq<u8>, st: BoolDecoderState, t: BTail)
    requires
        bstate_ok::<K>(data, st),
        st.remaining == 0,
        ({
            let cs = bool_counts::<K>(data);
            let k = bstate_k::<K>(data, st);
            &&& 1 <= k < cs.len()
            &&& t.value == !st.value
            &&& t.partial == cs[k]
            &&& t.start == counts_bytes::<K>(cs.take(k + 1)).len()
            &&& t.items == counts_len(cs.skip(k + 1))
            &&& t.segments == cs.len() - k - 1
        }),
    ensures
        tail_ok::<K>(data, bstate_pos::<K>(data, st), t, bstate_k::<K>(data, st) + 1),
{
    let cs = bool_counts::<K>(data);
    let k = bstate_k::<K>(data, st);
    let rk = cs.skip(k);
    let rc = cs.skip(k + 1);
    let a = vals_at(cs.take(k), 0);
    lemma_reader_at::<K>(data, st);
    lemma_rest_positive(cs, k + 1);
    lemma_values_split(cs, k);
    lemma_vals_len(cs.take(k), 0);
    lemma_vals_first(rk, k);
    assert(rk.skip(1) =~= rc);
    assert(rk[0] == cs[k]);
    assert((a + vals_at(rk, k)).skip(a.len() as int) =~= vals_at(rk, k));
}

/// Nothing follows a reader at the slab's end.
proof fn lemma_tail_end<K: CountOps>(data: Seq<u8>, st: BoolDecoderState, t: BTail)
    requires
        bstate_ok::<K>(data, st),
        st.remaining == 0,
        st.byte_pos >= data.len(),
        t.partial == 0 && t.start == data.len() && t.items == 0 && t.segments == 0,
    ensures
        tail_ok::<K>(data, bstate_pos::<K>(data, st), t, bstate_k::<K>(data, st)),
{
    let cs = bool_counts::<K>(data);
    let k = bstate_k::<K>(data, st);
    let vs = bool_values::<K>(data);
    lemma_reader_at::<K>(data, st);
    assert(cs.take(k) =~= cs);
    assert(cs.skip(k) =~= Seq::<u64>::empty());
    assert(vs.skip(vs.len() as int) =~= run_of(0, t.value) + vals_at(cs.skip(k), k));
}

/// A rebuild of one bool slab: the writer, the head it keeps, and the
/// reader standing where the edit has read to.
pub(crate) struct BEdit {
    pub(crate) w: BWriter,
    /// items of the slab before `byte_start`, which stay put
    pub(crate) head_len: usize,
    /// where the rebuilt bytes replace the original
    pub(crate) byte_start: usize,
    /// where reading stopped — the same reader the cursor carries
    pub(crate) dec: BoolDecoderState,
    /// complete runs behind the reader
    pub(crate) read_segments: usize,
    pub(crate) read: usize,
    /// the slab's bytes
    #[allow(dead_code)]
    pub(crate) data: Ghost<Seq<u8>>,
}

/// A valid slab's counts are its parse, and none of them has no bytes
/// and no values.
proof fn lemma_scan_start<K: CountOps>(data: Seq<u8>)
    requires bool_valid::<K>(data),
    ensures
        ({
            let cs = bool_counts::<K>(data);
            &&& bool_parses::<K>(cs, data)
            &&& counts_bytes::<K>(cs.take(0)).len() == 0
            &&& counts_len(cs.take(0)) == 0
            &&& bool_values::<K>(data).len() == counts_len(cs)
        }),
{
    let cs = bool_counts::<K>(data);
    lemma_values_len(cs);
    assert(cs.take(0) =~= Seq::<u64>::empty());
}

/// Counts whose bytes reach the slab's end are all of the slab's.
proof fn lemma_scan_end<K: CountOps>(data: Seq<u8>, cs: Seq<u64>, i: int)
    requires
        bool_parses::<K>(cs, data),
        0 <= i <= cs.len(),
        counts_bytes::<K>(cs.take(i)).len() >= data.len(),
    ensures
        i == cs.len(),
        cs.take(i) == cs,
{
    if i < cs.len() {
        lemma_take_bytes_mono::<K>(cs, i, cs.len() as int);
    }
    assert(cs.take(cs.len() as int) =~= cs);
}

/// The count read where run `i` starts is run `i`'s: `cb` bytes, which
/// end where run `i + 1` starts.
proof fn lemma_scan_count<K: CountOps>(data: Seq<u8>, cs: Seq<u64>, i: int, cb: int, c: u64)
    requires
        bool_parses::<K>(cs, data),
        0 <= i <= cs.len(),
        counts_bytes::<K>(cs.take(i)).len() < data.len(),
        0 <= cb <= data.len() - counts_bytes::<K>(cs.take(i)).len(),
        data.skip(counts_bytes::<K>(cs.take(i)).len() as int).take(cb) == K::enc(c),
    ensures
        i < cs.len(),
        c == cs[i],
        cb == K::enc(cs[i]).len(),
        c == 0 ==> i == 0,
        counts_bytes::<K>(cs.take(i + 1)).len() == counts_bytes::<K>(cs.take(i)).len() + cb,
        counts_bytes::<K>(cs.take(i + 1)).len() <= data.len(),
        counts_len(cs.take(i + 1)) == counts_len(cs.take(i)) + c,
        counts_len(cs.take(i + 1)) <= counts_len(cs),
        counts_len(cs) <= usize::MAX,
        cs.len() <= data.len(),
{
    let bp = counts_bytes::<K>(cs.take(i)).len() as int;
    let rest = data.skip(bp);
    lemma_bytes_len::<K>(cs);
    lemma_next_count::<K>(data, cs, i, bp);
    assert(rest =~= rest.take(cb) + rest.skip(cb));
    K::axiom_enc(c, cs[i], rest.skip(cb), counts_bytes::<K>(cs.skip(i + 1)));
    lemma_split_at::<K>(data, cs, i + 1);
    assert(cs.take(i + 1) =~= cs.take(i).push(cs[i]));
    lemma_bytes_push::<K>(cs.take(i), cs[i]);
}

/// Where a rebuild opened at item `at` starts, the scan having stopped
/// `off` items into run `i`, `prev` the run before if it has items: after
/// the first `p` runs, with `cur_count` items of the next pending. That
/// is run `i` and its items before `at`, or, at a run's start, the run
/// before and all its items.
proof fn lemma_open_head<K: CountOps>(data: Seq<u8>, cs: Seq<u64>, i: int, at: int, off: int, prev: Option<(usize, usize)>,
    byte_start: int, p: int, head_len: int, cur_value: bool, cur_count: int)
    requires
        bool_parses::<K>(cs, data),
        0 <= i <= cs.len(),
        prev matches Some((pb, pc)) ==> i >= 1 && pc == cs[i - 1] && pc > 0
            && pb == counts_bytes::<K>(cs.take(i - 1)).len(),
        prev is None ==> i == 0 || (i == 1 && cs[0] == 0),
        i < cs.len() ==> 0 <= off < cs[i] && at == counts_len(cs.take(i)) + off,
        i == cs.len() ==> off == 0 && at == counts_len(cs.take(i)),
        off > 0 ==> p == i && cur_value == run_value(i) && cur_count == off && head_len == at - off
            && byte_start == counts_bytes::<K>(cs.take(i)).len(),
        off == 0 ==> match prev {
            Some((pb, pc)) => p == i - 1 && cur_value == !run_value(i) && cur_count == pc && head_len == at - pc
                && byte_start == pb,
            None => p == 0 && !cur_value && cur_count == 0 && head_len == 0 && byte_start == 0,
        },
    ensures
        0 <= p <= i,
        p < i || i < cs.len() || p == 0,
        counts_pre(cs.take(p)),
        cur_value == run_value(p),
        cur_count >= 0,
        cur_count == 0 ==> p == 0,
        counts_len(cs.take(p)) + cur_count == at,
        head_len == counts_len(cs.take(p)),
        byte_start == counts_bytes::<K>(cs.take(p)).len(),
        at <= bool_values::<K>(data).len(),
        bool_values::<K>(data).take(at) == vals_at(cs.take(p), 0) + run_of(cur_count as nat, cur_value),
{
    lemma_counts::<K>(cs, data);
    let base = cs.take(p);
    assert(0 <= p <= i);
    assert(counts_pre(base)) by {
        assert forall|j: int| 1 <= j < base.len() implies #[trigger] base[j] > 0 by {
            assert(base[j] == cs[j]);
        }
    }
    if off > 0 {
        lemma_values_run_take(cs, i, off);
    } else if prev is Some {
        assert(cs.take(i).drop_last() =~= cs.take(i - 1));
        assert(cs.take(i).last() == cs[i - 1]);
        lemma_values_run_take(cs, i - 1, cs[i - 1] as int);
    } else {
        assert(base =~= Seq::<u64>::empty());
        assert(counts_len(cs.take(0)) == 0);
        assert(counts_len(cs.take(i)) == 0) by {
            if i == 1 {
                assert(cs.take(1).drop_last() =~= cs.take(0));
                assert(cs.take(1).last() == cs[0]);
            }
        }
        lemma_values_take(cs, 0);
        assert(run_of(0, cur_value) =~= Seq::<bool>::empty());
        assert(counts_values(cs).take(0) =~= vals_at(base, 0) + run_of(0, cur_value));
    }
}

/// A rebuild put together where the scan stopped is open at item `at`:
/// its reader is inside run `i`, or past the last run.
proof fn lemma_opened<K: CountOps>(e: BEdit, data: Seq<u8>, cs: Seq<u64>, i: int, p: int, off: int)
    requires
        bool_parses::<K>(cs, data),
        data.len() <= usize::MAX,
        0 <= p <= i <= cs.len(),
        p < i || i < cs.len() || p == 0,
        e.data@ == data,
        e.w.inv::<K>(),
        e.w.base@ == cs.take(p),
        e.byte_start == counts_bytes::<K>(cs.take(p)).len(),
        e.head_len == counts_len(cs.take(p)),
        e.read_segments == i,
        i < cs.len() ==> {
            &&& 0 <= off < cs[i]
            &&& e.read == counts_len(cs.take(i)) + off
            &&& e.dec.byte_pos == counts_bytes::<K>(cs.take(i)).len() + K::enc(cs[i]).len()
            &&& e.dec.remaining == cs[i] - off
            &&& e.dec.value == run_value(i)
        },
        i == cs.len() ==> {
            &&& e.read == counts_len(cs.take(i))
            &&& e.dec.byte_pos == counts_bytes::<K>(cs.take(i)).len()
            &&& e.dec.remaining == 0
            &&& e.dec.value == !run_value(i)
        },
    ensures e.inv::<K>(),
{
    let k = if i < cs.len() { i + 1 } else { i };
    lemma_counts::<K>(cs, data);
    if i < cs.len() {
        assert(cs.take(i + 1) =~= cs.take(i).push(cs[i]));
        lemma_bytes_push::<K>(cs.take(i), cs[i]);
    }
    assert(dstate_at::<K>(cs, e.dec, k));
    lemma_k_unique::<K>(cs, e.dec, bstate_k::<K>(data, e.dec), k);
}

/// A tail's values number its partial run's items and its carried
/// counts'.
proof fn lemma_tail_len<K: CountOps>(data: Seq<u8>, read: int, t: BTail, j: int)
    requires tail_ok::<K>(data, read, t, j),
    ensures bool_values::<K>(data).skip(read).len() == t.partial + counts_len(bool_counts::<K>(data).skip(j)),
{
    lemma_vals_len(bool_counts::<K>(data).skip(j), j);
}

/// A writer `w0` whose pending run is flushed, giving `w`: the chunk in
/// hand ends with that run's count, and is empty if there was none.
proof fn lemma_flushed<K: CountOps>(w0: BWriter, w: BWriter)
    requires
        w0.inv::<K>(),
        w.inv_w::<K>(),
        w.cur_count == 0,
        w.vals() == w0.vals(),
        w0.cur_count > 0 ==> w.cb@ == w0.cb@.push(w0.cur_count as u64),
        w0.cur_count == 0 ==> w.cb@ == w0.cb@,
        w.base@ == w0.base@,
        w.done@ == w0.done@,
        w.overflowed == w0.overflowed,
    ensures
        w.chunk().len() > 0 ==> w.chunk().last() > 0 && w.cb@.len() > 0,
        w.overflowed ==> w.cb@.len() > 0,
        w0.cur_count > 0 ==> run_value(w.chunk().len() as int) == !w0.cur_value,
        chunks_values(w.done@) + vals_at(w.chunk(), 0) == w0.vals(),
        chunks_values(w.done@).len() + counts_len(w.chunk()) == w0.vals().len(),
{
    let f = w.chunk();
    if w0.cur_count > 0 {
        assert(f =~= w0.chunk().push(w0.cur_count as u64));
        assert(f.last() == w0.cur_count as u64);
    } else {
        assert(f =~= w0.chunk());
    }
    assert(run_of(0, w.cur_value) =~= Seq::<bool>::empty());
    assert(w.vals() =~= chunks_values(w.done@) + vals_at(f, 0));
    lemma_vals_len(f, 0);
}

/// A rebuild's head ends inside its slab's bytes.
proof fn lemma_head_bytes<K: CountOps>(e: BEdit)
    requires e.inv::<K>(),
    ensures e.byte_start <= e.data@.len(),
{
    lemma_take_bytes_le::<K>(bool_counts::<K>(e.data@), e.w.base@.len() as int);
}

/// A writer `w0` on the slab `data`, flushed (then `w`), as `seal`
/// takes it to carry nothing after it: what `seal` requires, and the
/// values it then holds.
proof fn lemma_seal_whole<K: CountOps>(data: Seq<u8>, w0: BWriter, w: BWriter)
    requires
        bool_valid::<K>(data),
        w0.inv::<K>(),
        w0.vals().len() + 3 <= usize::MAX,
        w.inv_w::<K>(),
        w.cur_count == 0,
        w.vals() == w0.vals(),
        w0.cur_count > 0 ==> w.cb@ == w0.cb@.push(w0.cur_count as u64),
        w0.cur_count == 0 ==> w.cb@ == w0.cb@,
        w.base@ == w0.base@,
        w.done@ == w0.done@,
        w.overflowed == w0.overflowed,
    ensures
        ({
            let cs = bool_counts::<K>(data);
            let n = cs.len() as int;
            let rc = cs.skip(n);
            let f = w.chunk();
            &&& data.len() == counts_bytes::<K>(cs.take(n)).len()
            &&& rc.len() == 0 && counts_len(rc) == 0
            &&& f.len() > 0 ==> f.last() > 0 && w.cb@.len() > 0
            &&& w.overflowed ==> w.cb@.len() > 0
            &&& chunks_values(w.done@).len() + counts_len(f) + counts_len(rc) + 1 <= usize::MAX
            &&& chunks_values(w.done@) + vals_at(f, 0) + vals_at(rc, n) == w0.vals()
        }),
{
    hide(BWriter::inv);
    hide(BWriter::inv_w);
    hide(BWriter::vals);
    let cs = bool_counts::<K>(data);
    let n = cs.len() as int;
    let rc = cs.skip(n);
    let f = w.chunk();
    lemma_flushed::<K>(w0, w);
    assert(cs.take(n) =~= cs);
    assert(rc =~= Seq::<u64>::empty());
    assert(vals_at(rc, n) =~= Seq::<bool>::empty());
    assert(chunks_values(w.done@) + vals_at(f, 0) + vals_at(rc, n) =~= chunks_values(w.done@) + vals_at(f, 0));
}

/// A rebuild closed that spills into one slab more holds that slab's
/// values after its own.
proof fn lemma_closed_push<K: CountOps>(r0: BClosed, r: BClosed, data: Seq<u8>, out: Seq<bool>, s: BSlab)
    requires
        closed_ok::<K>(r0, data, out),
        bslab_ok::<K>(s),
        r.bytes@ == r0.bytes@,
        r.start == r0.start,
        r.end == r0.end,
        r.len == r0.len,
        r.segments == r0.segments,
        r.tail == r0.tail,
        r.overflow@ == r0.overflow@.push(s),
    ensures closed_ok::<K>(r, data, out + bool_values::<K>(bslab_bytes(s))),
{
    let o0 = r0.overflow@;
    let o = r.overflow@;
    let rs = data.take(r.start as int) + r.bytes@ + data.skip(r.end as int);
    assert(o.drop_last() =~= o0);
    assert(o.last() == s);
    assert forall|i: int| 0 <= i < o.len() implies bslab_ok::<K>(#[trigger] o[i]) by {
        if i < o0.len() {
            assert(o[i] == o0[i]);
        }
    }
    assert(bool_values::<K>(rs) + bslabs_values::<K>(o) =~= out + bool_values::<K>(bslab_bytes(s)));
}

/// A writer that held `v0`, given the partial run of the tail `t` of the
/// slab `data` (then `w1`) and flushed (then `w`), as `seal` takes it to
/// carry the tail's counts: what `seal` requires, and the values it then
/// holds.
proof fn lemma_seal_tail<K: CountOps>(data: Seq<u8>, read: int, v0: Seq<bool>, w1: BWriter, w: BWriter, t: BTail, j: int)
    requires
        tail_ok::<K>(data, read, t, j),
        v0.len() + bool_values::<K>(data).skip(read).len() + 3 <= usize::MAX,
        w1.inv::<K>(),
        w1.vals() == v0 + run_of(t.partial as nat, t.value),
        t.partial > 0 ==> w1.cur_value == t.value && w1.cur_count > 0,
        w.inv_w::<K>(),
        w.cur_count == 0,
        w.vals() == w1.vals(),
        w1.cur_count > 0 ==> w.cb@ == w1.cb@.push(w1.cur_count as u64),
        w1.cur_count == 0 ==> w.cb@ == w1.cb@,
        w.base@ == w1.base@,
        w.done@ == w1.done@,
        w.overflowed == w1.overflowed,
    ensures
        ({
            let rc = bool_counts::<K>(data).skip(j);
            let f = w.chunk();
            &&& rc.len() > 0 ==> run_value(f.len() as int) == run_value(j)
            &&& f.len() > 0 ==> f.last() > 0 && w.cb@.len() > 0
            &&& w.overflowed ==> w.cb@.len() > 0
            &&& chunks_values(w.done@).len() + counts_len(f) + counts_len(rc) + 1 <= usize::MAX
            &&& chunks_values(w.done@) + vals_at(f, 0) + vals_at(rc, j) == v0 + bool_values::<K>(data).skip(read)
        }),
{
    hide(BWriter::inv);
    hide(BWriter::inv_w);
    hide(BWriter::vals);
    let rc = bool_counts::<K>(data).skip(j);
    let run = run_of(t.partial as nat, t.value);
    lemma_flushed::<K>(w1, w);
    lemma_tail_len::<K>(data, read, t, j);
    if t.partial == 0 {
        assert(rc.len() == 0);
    }
    assert((v0 + run).len() == v0.len() + t.partial);
    assert(v0 + run + vals_at(rc, j) =~= v0 + (run + vals_at(rc, j)));
}

impl BEdit {
    /// The values written: the head's, then what was carried and inserted.
    pub(crate) open spec fn vals(&self) -> Seq<bool> {
        self.w.vals()
    }

    pub(crate) open spec fn inv<K: CountOps>(&self) -> bool {
        let data = self.data@;
        let cs = bool_counts::<K>(data);
        let k = bstate_k::<K>(data, self.dec);
        &&& bool_valid::<K>(data) && data.len() <= usize::MAX
        &&& self.w.inv::<K>()
        &&& self.w.base@.len() <= cs.len() && self.w.base@ == cs.take(self.w.base@.len() as int)
        &&& self.byte_start == counts_bytes::<K>(self.w.base@).len()
        &&& self.head_len == counts_len(self.w.base@)
        &&& bstate_ok::<K>(data, self.dec) && bstate_pos::<K>(data, self.dec) == self.read
        &&& self.read_segments == if self.dec.remaining > 0 { k - 1 } else { k }
        &&& k >= 1 || cs.len() == 0
        &&& self.w.base@.len() < k || self.w.base@.len() == 0
    }

    /// Insert `n` copies of `value` at the cursor.
    #[inline(always)]
    pub(crate) fn insert<K: CountOps>(&mut self, value: bool, n: usize)
        requires old(self).inv::<K>(), old(self).vals().len() + n <= usize::MAX,
        ensures
            final(self).inv::<K>(), final(self).data@ == old(self).data@, final(self).read == old(self).read,
            final(self).vals() == old(self).vals() + run_of(n as nat, value),
    {
        self.w.push::<K>(value, n);
    }

    /// Move the read cursor `n` items forward, keeping them when `emit`;
    /// `f` sees every run either way.
    #[verifier::spinoff_prover]
    pub(crate) fn walk<K: CountOps, G: Visit<bool>>(&mut self, data: &[u8], n: usize, emit: bool, f: &mut G) -> (left: usize)
        requires
            old(self).inv::<K>(), data@ == old(self).data@,
            old(self).read + n <= bool_values::<K>(data@).len(),
            emit ==> old(self).vals().len() + n <= usize::MAX,
        ensures
            final(self).inv::<K>(), final(self).data@ == old(self).data@, left == 0,
            final(self).read == old(self).read + n,
            final(self).vals() == old(self).vals() + if emit {
                bool_values::<K>(data@).subrange(old(self).read as int, old(self).read + n)
            } else {
                Seq::empty()
            },
    {
        let ghost r0 = self.read as int;
        let ghost v0 = self.vals();
        let ghost vs = bool_values::<K>(data@);
        let ghost cs = bool_counts::<K>(data@);
        let mut dec = BDec::resume::<K>(data, &self.dec);
        let mut want = n;
        proof {
            assert(vs.subrange(r0, r0) =~= Seq::<bool>::empty());
            assert(v0 + Seq::<bool>::empty() =~= v0);
            lemma_counts::<K>(cs, data@);
            lemma_values_len(cs);
        }
        while want > 0
            invariant
                self.w.inv::<K>(), self.data@ == data@, dec.inv::<K>(), dec.data() == data@,
                self.w.base@ == old(self).w.base@,
                dec.pos::<K>() == self.read, self.read == r0 + (n - want), want <= n, 0 <= r0,
                r0 + n <= vs.len(), vs.len() <= usize::MAX, vs == bool_values::<K>(data@), cs == bool_counts::<K>(data@),
                self.read_segments == if dec.state().remaining > 0 { dec.k::<K>() - 1 } else { dec.k::<K>() },
                dec.k::<K>() >= 1 || cs.len() == 0,
                self.w.base@.len() < dec.k::<K>() || self.w.base@.len() == 0,
                self.vals() == v0 + if emit { vs.subrange(r0, self.read as int) } else { Seq::empty() },
                emit ==> v0.len() + n <= usize::MAX,
                self.byte_start == old(self).byte_start, self.head_len == old(self).head_len,
            ensures
                self.w.inv::<K>(), self.data@ == data@, dec.inv::<K>(), dec.data() == data@,
                self.w.base@ == old(self).w.base@, want == 0,
                dec.pos::<K>() == self.read, self.read == r0 + n,
                self.read_segments == if dec.state().remaining > 0 { dec.k::<K>() - 1 } else { dec.k::<K>() },
                dec.k::<K>() >= 1 || cs.len() == 0,
                self.w.base@.len() < dec.k::<K>() || self.w.base@.len() == 0,
                self.vals() == v0 + if emit { vs.subrange(r0, self.read as int) } else { Seq::empty() },
                self.byte_start == old(self).byte_start, self.head_len == old(self).head_len,
            decreases want,
        {
            let ghost st0 = dec.state();
            let ghost k0 = dec.k::<K>();
            let ghost read0 = self.read as int;
            let run = match dec.next_run_max::<K>(want) {
                Some(run) => run,
                None => break,
            };
            f.visit(run.value, run.count);
            proof {
                let b = vs.subrange(read0, read0 + run.count);
                assert(vs.subrange(r0, read0 + run.count) =~= vs.subrange(r0, read0) + b);
                assert(b =~= run_of(run.count as nat, run.value));
            }
            if emit {
                self.w.push::<K>(run.value, run.count);
            }
            self.read = self.read + run.count;
            want = want - run.count;
            proof {
                if k0 == 0 {
                    // the reader stands at the start of a nonempty slab only
                    // before it has read anything — never, once opened
                    assert(cs.len() == 0);
                }
            }
            proof {
                lemma_bytes_len::<K>(cs);
                lemma_counts::<K>(cs, data@);
                assert(dstate_at::<K>(cs, dec.state(), dec.k::<K>()));
            }
            if dec.remaining() == 0 {
                self.read_segments = self.read_segments + 1;
            }
        }
        dec.suspend_into(&mut self.dec);
        want
    }

    /// What follows the reader. Reads one count — the run the cursor
    /// stands in; everything past it is bytes.
    #[verifier::spinoff_prover]
    pub(crate) fn tail_of<K: CountOps>(&self, data: &[u8], slab_len: usize, slab_segments: usize) -> (r: (BTail, Ghost<int>))
        requires
            self.inv::<K>(), data@ == self.data@,
            slab_len == bool_values::<K>(data@).len(), slab_segments == bool_counts::<K>(data@).len(),
        ensures
            tail_ok::<K>(data@, self.read as int, r.0, r.1@),
            self.w.base@.len() <= r.1@,
    {
        // the writer's invariant isn't needed here, and its slab vector
        // gives Z3 terms to loop on
        hide(BWriter::inv);
        let ghost cs = bool_counts::<K>(data@);
        let ghost k = bstate_k::<K>(data@, self.dec);
        let data_len = data.len();
        proof {
            lemma_reader_at::<K>(data@, self.dec);
        }
        if self.dec.remaining > 0 {
            let partial = self.dec.remaining;
            let t = BTail {
                value: self.dec.value,
                partial,
                start: self.dec.byte_pos,
                items: slab_len - self.read - partial,
                segments: slab_segments - self.read_segments - 1,
            };
            proof {
                lemma_tail_inside::<K>(data@, self.dec, t);
            }
            return (t, Ghost(k));
        }
        if self.dec.byte_pos < data_len {
            proof {
                lemma_tail_next::<K>(data@, self.dec);
            }
            let rest = &data[self.dec.byte_pos..];
            assert(rest@ == data@.skip(self.dec.byte_pos as int));
            let (cb, count) = match K::read_count(rest) {
                Some(v) => v,
                None => (0, 0),
            };
            proof {
                lemma_tail_count::<K>(data@, self.dec, cb as int, count as u64);
            }
            let t = BTail {
                value: !self.dec.value,
                partial: count,
                start: self.dec.byte_pos + cb,
                items: slab_len - self.read - count,
                segments: slab_segments - self.read_segments - 1,
            };
            proof {
                lemma_tail_between::<K>(data@, self.dec, t);
            }
            return (t, Ghost(k + 1));
        }
        proof {
            let t = BTail { value: self.dec.value, partial: 0, start: data_len, items: 0, segments: 0 };
            lemma_tail_end::<K>(data@, self.dec, t);
        }
        (BTail { value: self.dec.value, partial: 0, start: data_len, items: 0, segments: 0 }, Ghost(k))
    }

    /// Finish the chunk in hand with the slab's counts from run `j` on
    /// (carried as bytes from `tail_start`): the edited slab's
    /// replacement when nothing has spilled, another new slab when
    /// something has.
    #[inline(always)]
    fn seal<K: CountOps>(&mut self, data: &[u8], tail_start: usize, raw_items: usize, raw_segs: usize, slab_tail: u8,
        Ghost(j): Ghost<int>) -> (r: BClosed)
        requires
            old(self).w.inv_w::<K>(), old(self).w.cur_count == 0, data@ == old(self).data@,
            bool_valid::<K>(data@), data@.len() <= usize::MAX,
            slab_tail == counts_tail::<K>(bool_counts::<K>(data@)),
            ({
                let cs = bool_counts::<K>(data@);
                let rc = cs.skip(j);
                let f = old(self).w.chunk();
                &&& 0 <= j <= cs.len()
                &&& tail_start == counts_bytes::<K>(cs.take(j)).len()
                &&& raw_items == counts_len(rc) && raw_segs == rc.len()
                &&& forall|i: int| 0 <= i < rc.len() ==> #[trigger] rc[i] > 0
                &&& rc.len() > 0 ==> run_value(f.len() as int) == run_value(j)
                &&& f.len() > 0 ==> f.last() > 0 && old(self).w.cb@.len() > 0
                &&& old(self).w.overflowed ==> old(self).w.cb@.len() > 0
                &&& !old(self).w.overflowed ==> {
                    &&& old(self).w.base@ == cs.take(old(self).w.base@.len() as int)
                    &&& old(self).w.base@.len() <= j
                    &&& old(self).byte_start == counts_bytes::<K>(old(self).w.base@).len()
                    &&& old(self).head_len == counts_len(old(self).w.base@)
                }
                &&& old(self).w.overflowed ==> old(self).byte_start <= data@.len()
                    && old(self).w.base@.len() <= cs.len()
                    && old(self).w.base@ == cs.take(old(self).w.base@.len() as int)
                    && old(self).byte_start == counts_bytes::<K>(old(self).w.base@).len()
                    && old(self).head_len == counts_len(old(self).w.base@)
                &&& chunks_values(old(self).w.done@).len() + counts_len(f) + counts_len(rc) + 1 <= usize::MAX
            }),
        ensures
            closed_ok::<K>(r, data@, chunks_values(old(self).w.done@) + vals_at(old(self).w.chunk(), 0)
                + vals_at(bool_counts::<K>(data@).skip(j), j)),
    {
        if !self.w.overflowed {
            self.seal_first::<K>(data, tail_start, raw_items, raw_segs, slab_tail, Ghost(j))
        } else {
            self.seal_spilled::<K>(data, tail_start, raw_items, raw_segs, slab_tail, Ghost(j))
        }
    }

    #[verifier::spinoff_prover]
    fn seal_first<K: CountOps>(&mut self, data: &[u8], tail_start: usize, raw_items: usize, raw_segs: usize, slab_tail: u8,
        Ghost(j): Ghost<int>) -> (r: BClosed)
        requires
            !old(self).w.overflowed, old(self).w.inv_w::<K>(), old(self).w.cur_count == 0, data@ == old(self).data@,
            bool_valid::<K>(data@), data@.len() <= usize::MAX,
            slab_tail == counts_tail::<K>(bool_counts::<K>(data@)),
            ({
                let cs = bool_counts::<K>(data@);
                let rc = cs.skip(j);
                let f = old(self).w.chunk();
                &&& 0 <= j <= cs.len()
                &&& tail_start == counts_bytes::<K>(cs.take(j)).len()
                &&& raw_items == counts_len(rc) && raw_segs == rc.len()
                &&& forall|i: int| 0 <= i < rc.len() ==> #[trigger] rc[i] > 0
                &&& rc.len() > 0 ==> run_value(f.len() as int) == run_value(j)
                &&& f.len() > 0 ==> f.last() > 0 && old(self).w.cb@.len() > 0
                &&& old(self).w.overflowed ==> old(self).w.cb@.len() > 0
                &&& !old(self).w.overflowed ==> {
                    &&& old(self).w.base@ == cs.take(old(self).w.base@.len() as int)
                    &&& old(self).w.base@.len() <= j
                    &&& old(self).byte_start == counts_bytes::<K>(old(self).w.base@).len()
                    &&& old(self).head_len == counts_len(old(self).w.base@)
                }
                &&& old(self).w.overflowed ==> old(self).byte_start <= data@.len()
                    && old(self).w.base@.len() <= cs.len()
                    && old(self).w.base@ == cs.take(old(self).w.base@.len() as int)
                    && old(self).byte_start == counts_bytes::<K>(old(self).w.base@).len()
                    && old(self).head_len == counts_len(old(self).w.base@)
                &&& chunks_values(old(self).w.done@).len() + counts_len(f) + counts_len(rc) + 1 <= usize::MAX
            }),
        ensures
            closed_ok::<K>(r, data@, chunks_values(old(self).w.done@) + vals_at(old(self).w.chunk(), 0)
                + vals_at(bool_counts::<K>(data@).skip(j), j)),
    {
        let ghost rc = bool_counts::<K>(data@).skip(j);
        let ghost f = self.w.chunk();
        let ghost d = self.w.done@;
        let ghost base = self.w.base@;
        let ghost cb = self.w.cb@;
        let ghost tail_byte = self.w.tail_byte;
        let data_len = data.len();
        proof {
            reveal(out_ok);
            lemma_first_sizes::<K>(base, cb, rc);
        }
        // the slab's last count is unchanged when any tail bytes remain
        let tail = if tail_start < data_len { slab_tail } else { self.w.tail_byte };
        let mut bytes = Vec::new();
        core::mem::swap(&mut self.w.buf, &mut bytes);
        let r = BClosed {
            bytes,
            start: self.byte_start,
            end: tail_start,
            len: self.head_len + self.w.emitted + raw_items,
            segments: self.w.standing + self.w.segments + raw_segs,
            tail,
            overflow: Vec::new(),
        };
        proof {
            lemma_sealed_first::<K>(r, data@, j, base, cb, tail_byte, slab_tail);
            assert(chunks_values(d) + vals_at(f, 0) + vals_at(rc, j) =~= vals_at(f, 0) + vals_at(rc, j));
        }
        r
    }

    #[verifier::spinoff_prover]
    fn seal_spilled<K: CountOps>(&mut self, data: &[u8], tail_start: usize, raw_items: usize, raw_segs: usize, slab_tail: u8,
        Ghost(j): Ghost<int>) -> (r: BClosed)
        requires
            old(self).w.overflowed, old(self).w.inv_w::<K>(), old(self).w.cur_count == 0, data@ == old(self).data@,
            bool_valid::<K>(data@), data@.len() <= usize::MAX,
            slab_tail == counts_tail::<K>(bool_counts::<K>(data@)),
            ({
                let cs = bool_counts::<K>(data@);
                let rc = cs.skip(j);
                let f = old(self).w.chunk();
                &&& 0 <= j <= cs.len()
                &&& tail_start == counts_bytes::<K>(cs.take(j)).len()
                &&& raw_items == counts_len(rc) && raw_segs == rc.len()
                &&& forall|i: int| 0 <= i < rc.len() ==> #[trigger] rc[i] > 0
                &&& rc.len() > 0 ==> run_value(f.len() as int) == run_value(j)
                &&& f.len() > 0 ==> f.last() > 0 && old(self).w.cb@.len() > 0
                &&& old(self).w.overflowed ==> old(self).w.cb@.len() > 0
                &&& !old(self).w.overflowed ==> {
                    &&& old(self).w.base@ == cs.take(old(self).w.base@.len() as int)
                    &&& old(self).w.base@.len() <= j
                    &&& old(self).byte_start == counts_bytes::<K>(old(self).w.base@).len()
                    &&& old(self).head_len == counts_len(old(self).w.base@)
                }
                &&& old(self).w.overflowed ==> old(self).byte_start <= data@.len()
                    && old(self).w.base@.len() <= cs.len()
                    && old(self).w.base@ == cs.take(old(self).w.base@.len() as int)
                    && old(self).byte_start == counts_bytes::<K>(old(self).w.base@).len()
                    && old(self).head_len == counts_len(old(self).w.base@)
                &&& chunks_values(old(self).w.done@).len() + counts_len(f) + counts_len(rc) + 1 <= usize::MAX
            }),
        ensures
            closed_ok::<K>(r, data@, chunks_values(old(self).w.done@) + vals_at(old(self).w.chunk(), 0)
                + vals_at(bool_counts::<K>(data@).skip(j), j)),
    {
        let ghost cs = bool_counts::<K>(data@);
        let ghost rc = cs.skip(j);
        let ghost w0 = self.w;
        let data_len = data.len();
        proof {
            lemma_out_first::<K>(w0.base@, w0.done@, w0.out_cb@, w0.out_bytes@, w0.out_emitted, w0.out_segments, w0.out_tail,
                w0.overflow@);
            lemma_first_sizes::<K>(Seq::empty(), w0.cb@, rc);
            lemma_split_at::<K>(data@, cs, j);
        }
        // the slab's last count is unchanged when any tail bytes remain
        let tail = if tail_start < data_len { slab_tail } else { self.w.tail_byte };
        let mut bytes = Vec::new();
        core::mem::swap(&mut self.w.buf, &mut bytes);
        // the chunk in hand and the carried tail are a new slab
        let rest = &data[tail_start..];
        assert(rest@ == data@.skip(tail_start as int));
        let mut last = bytes;
        extend_vec(&mut last, rest);
        let s = new_bslab_vec(last, self.w.emitted + raw_items, self.w.segments + raw_segs, tail);
        let ghost s0 = s;
        let mut overflow = Vec::new();
        core::mem::swap(&mut self.w.overflow, &mut overflow);
        overflow.push(s);
        let mut first = Vec::new();
        core::mem::swap(&mut self.w.out_bytes, &mut first);
        let r = BClosed {
            bytes: first,
            start: self.byte_start,
            end: data_len,
            len: self.head_len + self.w.out_emitted,
            segments: self.w.out_segments,
            tail: self.w.out_tail,
            overflow,
        };
        proof {
            lemma_sealed_spilled::<K>(r, data@, j, w0, s0, slab_tail);
        }
        r
    }

    /// Write the rebuild back, carrying the slab's remaining bytes
    /// through untouched (or, if they would overfill the slab, as a slab
    /// of their own).
    #[verifier::spinoff_prover]
    pub(crate) fn close<K: CountOps>(&mut self, data: &[u8], slab_len: usize, slab_segments: usize, slab_tail: u8) -> (r: BClosed)
        requires
            old(self).inv::<K>(), data@ == old(self).data@,
            slab_len == bool_values::<K>(data@).len(), slab_segments == bool_counts::<K>(data@).len(),
            slab_tail == counts_tail::<K>(bool_counts::<K>(data@)),
            old(self).vals().len() + (slab_len - old(self).read) + 3 <= usize::MAX,
        ensures
            closed_ok::<K>(r, data@, old(self).vals() + bool_values::<K>(data@).skip(old(self).read as int)),
    {
        let ghost cs = bool_counts::<K>(data@);
        let (t, Ghost(j)) = self.tail_of::<K>(data, slab_len, slab_segments);
        proof {
            let rc = cs.skip(j);
            lemma_counts::<K>(cs, data@);
            lemma_values_len(cs);
            lemma_vals_len(rc, j);
            lemma_pre_len(self.w.chunk());
            lemma_vals_len(self.w.chunk(), 0);
            lemma_rc_len(rc);
            lemma_chunks_len(self.w.done@, self.w.chunk(), self.w.cur_count as nat, self.w.cur_value);
            let vs = bool_values::<K>(data@);
            assert(vs.skip(self.read as int).len() == t.partial + counts_len(rc));
            assert(self.w.chunk().len() == self.w.standing + self.w.segments);
        }
        // the partial run costs a segment unless it merges into the
        // pending one
        let joins = t.partial > 0 && t.value == self.w.cur_value;
        let need = (if self.w.cur_count > 0 { 1usize } else { 0 }) + t.segments + (if t.partial > 0 && !joins { 1usize } else { 0 });
        if self.w.standing + self.w.segments + need > self.w.max_segments {
            self.close_spill::<K>(data, slab_tail, &t, Ghost(j))
        } else {
            self.close_fit::<K>(data, slab_tail, &t, Ghost(j))
        }
    }

    /// The tail does not fit: it becomes a slab of its own.
    #[verifier::spinoff_prover]
    fn close_spill<K: CountOps>(&mut self, data: &[u8], slab_tail: u8, t: &BTail, Ghost(j): Ghost<int>) -> (r: BClosed)
        requires
            old(self).inv::<K>(), data@ == old(self).data@,
            slab_tail == counts_tail::<K>(bool_counts::<K>(data@)),
            tail_ok::<K>(data@, old(self).read as int, *t, j), old(self).w.base@.len() <= j,
            old(self).vals().len() + bool_values::<K>(data@).skip(old(self).read as int).len() + 3 <= usize::MAX,
        ensures
            closed_ok::<K>(r, data@, old(self).vals() + bool_values::<K>(data@).skip(old(self).read as int)),
    {
        let ghost cs = bool_counts::<K>(data@);
        let ghost vs = bool_values::<K>(data@);
        let ghost read = self.read as int;
        let ghost v0 = self.vals();
        let ghost e0 = *self;
        self.w.flush_run::<K>();
        self.w.cur_count = 0;
        proof {
            lemma_head_bytes::<K>(e0);
            lemma_seal_whole::<K>(data@, e0.w, self.w);
        }
        let r0 = self.seal::<K>(data, data.len(), 0, 0, slab_tail, Ghost(cs.len() as int));
        let spill = into_slab::<K>(t, data, slab_tail, Ghost(read), Ghost(j));
        let mut r = r0;
        let ghost r1 = r;
        proof {
            if spill is None {
                assert(v0 + vs.skip(read) =~= v0);
            }
        }
        match spill {
            Some(s) => {
                let ghost s0 = s;
                r.overflow.push(s);
                proof {
                    lemma_closed_push::<K>(r1, r, data@, v0, s0);
                }
            },
            None => {},
        }
        r
    }

    /// The tail fits: the partial run goes through the encoder like any
    /// other, which merges it with the pending run when the parities
    /// agree, and pads or cuts when they do not.
    #[verifier::spinoff_prover]
    fn close_fit<K: CountOps>(&mut self, data: &[u8], slab_tail: u8, t: &BTail, Ghost(j): Ghost<int>) -> (r: BClosed)
        requires
            old(self).inv::<K>(), data@ == old(self).data@,
            slab_tail == counts_tail::<K>(bool_counts::<K>(data@)),
            tail_ok::<K>(data@, old(self).read as int, *t, j), old(self).w.base@.len() <= j,
            old(self).vals().len() + bool_values::<K>(data@).skip(old(self).read as int).len() + 3 <= usize::MAX,
        ensures
            closed_ok::<K>(r, data@, old(self).vals() + bool_values::<K>(data@).skip(old(self).read as int)),
    {
        let ghost e0 = *self;
        proof {
            lemma_tail_len::<K>(data@, self.read as int, *t, j);
        }
        self.w.push::<K>(t.value, t.partial);
        let ghost w1 = self.w;
        self.w.flush_run::<K>();
        self.w.cur_count = 0;
        proof {
            lemma_head_bytes::<K>(e0);
            lemma_seal_tail::<K>(data@, e0.read as int, e0.vals(), w1, self.w, *t, j);
        }
        self.seal::<K>(data, t.start, t.items, t.segments, slab_tail, Ghost(j))
    }

    /// Open a rebuild of the valid slab `data` at item `at`.
    #[verifier::spinoff_prover]
    #[allow(unused_assignments)] // the scan's results are set where it stops
    pub(crate) fn open<K: CountOps>(data: &[u8], at: usize, max_segments: usize, buf: Vec<u8>) -> (e: Self)
        requires
            bool_valid::<K>(data@), at <= bool_values::<K>(data@).len(), max_segments >= 2,
        ensures
            e.inv::<K>(), e.data@ == data@, e.read == at, e.vals() == bool_values::<K>(data@).take(at as int),
    {
        hide(BEdit::inv);
        hide(BWriter::inv);
        hide(BWriter::vals);
        hide(bool_parses);
        hide(bool_values);
        hide(counts_bytes);
        hide(counts_len);
        hide(vals_at);
        hide(run_of);
        let ghost cs = bool_counts::<K>(data@);
        let len = data.len();
        proof {
            lemma_scan_start::<K>(data@);
        }
        let mut byte = 0usize;
        let mut item = 0usize;
        let mut value = false;
        let mut segments = 0usize;
        // the run before the one `at` falls in, if it has items
        let mut prev: Option<(usize, usize)> = None; // (byte, count)
        let ghost mut i: int = 0;
        let mut header = 0usize;
        let mut off = 0usize;
        let mut cbytes = 0usize;
        let mut count = 0usize;
        let mut found = false;
        loop
            invariant_except_break !found,
            invariant
                0 <= i <= cs.len(), segments == i, len == data@.len(),
                byte == counts_bytes::<K>(cs.take(i)).len(), item == counts_len(cs.take(i)),
                value == run_value(i), item <= at, bool_parses::<K>(cs, data@),
                prev matches Some((pb, pc)) ==> i >= 1 && pc == cs[i - 1] && pc > 0
                    && pb == counts_bytes::<K>(cs.take(i - 1)).len(),
                prev is None ==> i == 0 || (i == 1 && cs[0] == 0),
                at <= bool_values::<K>(data@).len(), bool_values::<K>(data@).len() == counts_len(cs),
            ensures
                0 <= i <= cs.len(), segments == i,
                byte == counts_bytes::<K>(cs.take(i)).len(), item == counts_len(cs.take(i)),
                value == run_value(i), item <= at,
                prev matches Some((pb, pc)) ==> i >= 1 && pc == cs[i - 1] && pc > 0
                    && pb == counts_bytes::<K>(cs.take(i - 1)).len(),
                prev is None ==> i == 0 || (i == 1 && cs[0] == 0),
                header == byte, found == (i < cs.len()), found ==> header + cbytes <= len,
                i < cs.len() ==> off == at - item && off < count && count == cs[i]
                    && cbytes == K::enc(cs[i]).len(),
                i == cs.len() ==> off == 0 && cbytes == 0 && count == 0 && item == at,
            decreases cs.len() - i,
        {
            if byte >= len {
                proof {
                    lemma_scan_end::<K>(data@, cs, i);
                }
                header = byte;
                off = 0;
                cbytes = 0;
                count = 0;
                break;
            }
            proof {
                lemma_next_count::<K>(data@, cs, i, byte as int);
            }
            let rest = &data[byte..];
            assert(rest@ == data@.skip(byte as int));
            let (cb, c) = match K::read_count(rest) {
                Some(v) => v,
                None => (0, 0),
            };
            proof {
                lemma_scan_count::<K>(data@, cs, i, cb as int, c as u64);
            }
            if item + c > at {
                header = byte;
                off = at - item;
                cbytes = cb;
                count = c;
                found = true;
                break;
            }
            if c > 0 {
                prev = Some((byte, c));
            }
            item = item + c;
            byte = byte + cb;
            value = !value;
            segments = segments + 1;
            proof {
                i = i + 1;
            }
        }
        let ghost i = i;
        proof {
            if i >= 1 {
                lemma_count_le(cs.take(i), i - 1);
            }
        }
        // The pending run must not be empty, or a push of the opposite
        // parity would have to merge into a run already encoded in the
        // head: on a run boundary, back up one run and re-encode it.
        let (byte_start, prefix_segments, head_len, cur_value, cur_count) = if off > 0 {
            (header, segments, at - off, value, off)
        } else if let Some((pbyte, pcount)) = prev {
            (pbyte, segments - 1, at - pcount, !value, pcount)
        } else {
            (0, 0, 0, false, 0)
        };
        let ghost base = cs.take(prefix_segments as int);
        proof {
            lemma_open_head::<K>(data@, cs, i, at as int, off as int, prev, byte_start as int, prefix_segments as int,
                head_len as int, cur_value, cur_count as int);
        }
        let w = BWriter::new::<K>(cur_value, cur_count, prefix_segments, max_segments, buf, Ghost(base));
        // the reader: inside the run `at` falls in, or past the last run
        let dec = if found {
            BoolDecoderState { byte_pos: header + cbytes, remaining: count - off, value }
        } else {
            BoolDecoderState { byte_pos: header, remaining: 0, value: !value }
        };
        let e = BEdit { w, head_len, byte_start, dec, read_segments: segments, read: at, data: Ghost(data@) };
        proof {
            lemma_opened::<K>(e, data@, cs, i, prefix_segments as int, off as int);
        }
        e
    }
}

impl BClosed {
    /// Write the rebuilt bytes into the slab's, `data`.
    pub(crate) fn apply_bytes(&self, data: &mut Vec<u8>)
        requires self.start <= self.end <= old(data)@.len(),
        ensures final(data)@ == old(data)@.take(self.start as int) + self.bytes@ + old(data)@.skip(self.end as int),
    {
        crate::rle::writer::splice_bytes(data, self.start, self.end - self.start, self.bytes.as_slice());
    }
}

impl BEdit {
    /// A rebuild not yet opened on any slab.
    pub(crate) fn idle() -> (e: Self) {
        BEdit {
            w: BWriter {
                cur_value: false,
                cur_count: 0,
                buf: Vec::new(),
                segments: 0,
                emitted: 0,
                tail_byte: 0,
                standing: 0,
                max_segments: 2,
                target: 2,
                overflowed: false,
                out_bytes: Vec::new(),
                out_emitted: 0,
                out_segments: 0,
                out_tail: 0,
                overflow: Vec::new(),
                base: Ghost(Seq::empty()),
                cb: Ghost(Seq::empty()),
                done: Ghost(Seq::empty()),
                out_cb: Ghost(Seq::empty()),
            },
            head_len: 0,
            byte_start: 0,
            dec: BoolDecoderState::start(),
            read_segments: 0,
            read: 0,
            data: Ghost(Seq::empty()),
        }
    }
}


// ── The cursor's slab model ────────────────────────────────────────────────

/// A rebuild in progress within one boolean slab.
///
/// Same contract as the RLE cursor: the head before the edit and the tail
/// after it are carried as raw bytes and never re-encoded, no byte is
/// decoded twice, and N edits landing in one slab cost one partition, one
/// byte splice and one index update.
///
/// The rebuild is verified with Verus (`BEdit`), and so is what the
/// column cursor does with it (`BoolModel`): opening, walking,
/// inserting and closing leave the slab, and any it spills into, the
/// canonical encoding of the values written then the slab's from where
/// reading stopped, with their metadata.
pub struct BoolEdit<C = crate::Leb128> {
    pub(crate) e: BEdit,
    pub(crate) _phantom: PhantomData<C>,
}

/// Bool slabs as the column cursor sees them ([`Slabs`]), verified
/// against the bool format: a slab is well-formed when `bslab_ok`, and
/// holds the values its bytes encode.
///
/// Public only because `BoolEncoding<C>`'s `SlabEdit::Model` names it;
/// this module is not.
pub struct BoolModel<C>(PhantomData<C>);

/// The rebuild's write buffer, to reuse: nothing is claimed of it, as
/// opening a rebuild takes any buffer. (`mem::take` is what the
/// original did; a swap with a new `Vec` compiles to a slower open.)
#[verifier::external_body]
#[inline(always)]
fn take_buf(v: &mut Vec<u8>) -> Vec<u8> {
    std::mem::take(v)
}

/// Slabs' values one after another are their values in order.
proof fn lemma_concat_bslabs<C: CountsOf>(s: Seq<BSlab>)
    ensures
        concat(s.map_values(|x: BSlab| BoolModel::<C>::vals(x))) == bslabs_values::<C::K>(s),
    decreases s.len(),
{
    reveal(bslab_bytes);
    if s.len() > 0 {
        lemma_concat_bslabs::<C>(s.drop_last());
        assert(s.drop_last().map_values(|x: BSlab| BoolModel::<C>::vals(x)) =~= s.map_values(
            |x: BSlab| BoolModel::<C>::vals(x),
        ).drop_last());
    } else {
        assert(s.map_values(|x: BSlab| BoolModel::<C>::vals(x)) =~= Seq::<Seq<bool>>::empty());
    }
}

/// The bool encoder as `remap` drives it (`remap::Encodes`), with column
/// codec `C`'s counts.
#[doc(hidden)]
#[verifier::reject_recursive_types(C)]
pub struct BoolRecoder<C>(pub(crate) crate::bool_enc::BEncoder, pub(crate) PhantomData<C>);

impl<C: CountsOf> crate::remap::Encodes<BoolModel<C>> for BoolRecoder<C> {
    type In = bool;

    open spec fn e_inv(&self) -> bool {
        self.0.inv::<C::K>()
    }

    open spec fn e_values(&self) -> Seq<bool> {
        self.0.values::<C::K>()
    }

    open spec fn val(x: bool) -> bool {
        x
    }

    #[inline(always)]
    fn with_budget(max: usize) -> (e: Self) {
        BoolRecoder(crate::bool_enc::BEncoder::new::<C::K>(Some(max)), PhantomData)
    }

    #[inline(always)]
    fn put(&mut self, x: bool, n: usize) {
        self.0.append::<C::K>(x, n);
        proof {
            assert(run_of(n as nat, x) =~= Seq::new(n as nat, |_i: int| x));
        }
    }

    #[inline(always)]
    fn take(&mut self) -> (v: Vec<BSlab>) {
        let v = self.0.take_slabs::<C::K>();
        proof {
            lemma_concat_bslabs::<C>(v@);
            assert forall|i: int| 0 <= i < v@.len() implies BoolModel::<C>::ok(#[trigger] v@[i]) by {}
        }
        v
    }
}

/// A well-formed slab's facts, unwrapped from the opaque views.
proof fn lemma_bslab<K: CountOps>(s: BSlab)
    requires
        bslab_ok::<K>(s),
    ensures
        bool_valid::<K>(s.data@),
        bool_values::<K>(s.data@).len() == s.len,
        s.segments == bool_counts::<K>(s.data@).len(),
        s.tail == counts_tail::<K>(bool_counts::<K>(s.data@)),
        counts_len(bool_counts::<K>(s.data@)) == s.len,
{
    reveal(bslab_bytes);
    reveal(bslab_len);
    reveal(bslab_segments);
    reveal(bslab_tail);
    lemma_values_len(bool_counts::<K>(s.data@));
}

/// Metadata that matches valid bytes make a well-formed slab.
proof fn lemma_bslab_ok<K: CountOps>(s: BSlab)
    requires
        bool_valid::<K>(s.data@),
        s.len == counts_len(bool_counts::<K>(s.data@)),
        s.segments == bool_counts::<K>(s.data@).len(),
        s.tail == counts_tail::<K>(bool_counts::<K>(s.data@)),
    ensures
        bslab_ok::<K>(s),
{
    reveal(bslab_bytes);
    reveal(bslab_len);
    reveal(bslab_segments);
    reveal(bslab_tail);
}

/// The canonical encoding of the slabs' values, appended to `out`
/// (`Column::save_to`): the first slab's bytes, then each of the others
/// merged on. `total` is their item count, as a column records it.
#[inline]
#[allow(unused_variables)] // `total` is for the spec: it bounds the merges' counts
pub(crate) fn save_slabs<C: CountsOf>(slabs: &[BSlab], total: usize, out: &mut Vec<u8>)
    requires
        crate::cursor::all_ok::<BoolModel<C>>(slabs@),
        total == crate::fenwick::total(crate::cursor::slab_lens(slabs@)),
    ensures
        old(out)@.len() <= final(out)@.len(),
        final(out)@.take(old(out)@.len() as int) == old(out)@,
        ({
            let x = final(out)@.skip(old(out)@.len() as int);
            &&& bool_valid::<C::K>(x)
            &&& bool_values::<C::K>(x) == vals_of::<BoolModel<C>>(slabs@)
        }),
{
    let start = out.len();
    let ghost s = slabs@;
    let ghost o0 = out@;
    if slabs.len() == 0 {
        proof {
            assert(out@.take(start as int) =~= o0);
            assert(out@.skip(start as int) =~= Seq::<u8>::empty());
            lemma_vals_unit::<BoolModel<C>>(arbitrary());
            let e = Seq::<u64>::empty();
            assert(counts_bytes::<C::K>(e) =~= Seq::<u8>::empty());
            lemma_counts::<C::K>(e, Seq::empty());
            assert(counts_values(e) =~= Seq::<bool>::empty());
        }
        return;
    }
    out.extend_from_slice(slabs[0].data.as_slice());
    let mut tail = slabs[0].tail;
    let mut segments = slabs[0].segments;
    let ghost mut n = slabs[0].len as int;
    proof {
        assert(out@.take(start as int) =~= o0);
        assert(out@.skip(start as int) =~= slabs[0].data@);
        assert(s.take(1) =~= seq![s[0]]);
        lemma_vals_unit::<BoolModel<C>>(s[0]);
        assert(BoolModel::<C>::ok(s[0]));
        lemma_bslab::<C::K>(s[0]);
        crate::cursor::lemma_vals_total::<BoolModel<C>>(s);
    }
    let mut i = 1;
    while i < slabs.len()
        invariant
            s == slabs@, crate::cursor::all_ok::<BoolModel<C>>(s),
            total == vals_of::<BoolModel<C>>(s).len(),
            1 <= i <= s.len(),
            start == o0.len(), start <= out@.len(),
            out@.take(start as int) == o0,
            bool_valid::<C::K>(out@.skip(start as int)),
            segments == bool_counts::<C::K>(out@.skip(start as int)).len(),
            tail == counts_tail::<C::K>(bool_counts::<C::K>(out@.skip(start as int))),
            bool_values::<C::K>(out@.skip(start as int)) == vals_of::<BoolModel<C>>(s.take(i as int)),
            n == counts_len(bool_counts::<C::K>(out@.skip(start as int))),
        decreases s.len() - i,
    {
        let ghost x0 = out@.skip(start as int);
        let ghost va = bool_values::<C::K>(x0);
        let ghost vb = bool_values::<C::K>(slabs[i as int].data@);
        proof {
            assert(BoolModel::<C>::ok(s[i as int]));
            lemma_bslab::<C::K>(s[i as int]);
            assert(s.take(i + 1) =~= s.take(i as int) + seq![s[i as int]]);
            lemma_vals_cat::<BoolModel<C>>(s.take(i as int), seq![s[i as int]]);
            lemma_vals_unit::<BoolModel<C>>(s[i as int]);
            // the values so far are among all of them
            crate::cursor::lemma_vals_split::<BoolModel<C>>(s, i + 1);
            lemma_values_len(bool_counts::<C::K>(x0));
        }
        let b = &slabs[i];
        if b.len == 0 || b.data.len() == 0 {
            proof {
                if b.data@.len() == 0 {
                    assert(b.data@ =~= Seq::<u8>::empty());
                    let e = Seq::<u64>::empty();
                    assert(counts_bytes::<C::K>(e) =~= Seq::<u8>::empty());
                    lemma_counts::<C::K>(e, b.data@);
                }
                lemma_values_len(bool_counts::<C::K>(b.data@));
                assert(vb =~= Seq::<bool>::empty());
                assert(va + vb =~= va);
            }
        } else {
            let ghost o1 = out@;
            let r = merge::<C::K>(out, start, tail, segments, b.data.as_slice(), b.segments, b.tail);
            segments = r.0;
            tail = r.1;
            proof {
                n = n + b.len;
                assert(o1.skip(start as int) =~= x0);
                let m = out@.skip(start as int);
                lemma_values_len(bool_counts::<C::K>(x0));
                lemma_values_len(bool_counts::<C::K>(b.data@));
                lemma_values_len(bool_counts::<C::K>(m));
            }
        }
        i = i + 1;
    }
    proof {
        assert(s.take(s.len() as int) =~= s);
    }
}

/// A slab of `len` copies of `value`: a count of `len`, after a zero
/// count of `false`s if `value` is `true` (`ColumnEncoding::fill`).
#[inline]
pub(crate) fn fill<C: CountsOf>(len: usize, value: bool) -> (s: BSlab)
    requires
        len > 0,
    ensures
        BoolModel::<C>::ok(s),
        BoolModel::<C>::vals(s) == Seq::new(len as nat, |_i: int| value),
        s.len == len,
{
    let mut data = Vec::new();
    let segments = if value {
        C::K::put_count(0, &mut data);
        2
    } else {
        1
    };
    let ghost d0 = data@;
    let t = data.len();
    C::K::put_count(len, &mut data);
    proof {
        C::K::axiom_enc(len as u64, len as u64, Seq::empty(), Seq::empty());
    }
    let tail = (data.len() - t) as u8;
    let s = new_bslab_vec(data, len, segments, tail);
    proof {
        reveal(bslab_bytes);
        reveal(bslab_len);
        reveal(bslab_segments);
        reveal(bslab_tail);
        let cs = if value { seq![0u64, len as u64] } else { seq![len as u64] };
        let e = Seq::<u64>::empty();
        assert(counts_bytes::<C::K>(e) == Seq::<u8>::empty());
        assert(counts_len(e) == 0);
        if value {
            let z = seq![0u64];
            assert(cs.drop_last() =~= z);
            assert(z.drop_last() =~= e);
            assert(counts_bytes::<C::K>(z) == counts_bytes::<C::K>(e) + C::K::enc(0));
            assert(counts_len(z) == counts_len(e) + 0);
            assert(d0 =~= C::K::enc(0));
            assert(counts_bytes::<C::K>(cs) == counts_bytes::<C::K>(z) + C::K::enc(len as u64));
            assert(counts_len(cs) == counts_len(z) + len);
        } else {
            assert(cs.drop_last() =~= e);
            assert(d0 =~= Seq::<u8>::empty());
            assert(counts_bytes::<C::K>(cs) == counts_bytes::<C::K>(e) + C::K::enc(len as u64));
            assert(counts_len(cs) == counts_len(e) + len);
        }
        assert(counts_bytes::<C::K>(cs) =~= s.data@);
        assert(bool_parses::<C::K>(cs, s.data@));
        lemma_unique::<C::K>(cs, bool_counts::<C::K>(s.data@));
        lemma_bslab_ok::<C::K>(s);
        assert(counts_values(e) =~= Seq::<bool>::empty());
        if value {
            let z = seq![0u64];
            assert(counts_values(z) =~= Seq::<bool>::empty());
            assert(counts_values(cs) == counts_values(z) + Seq::new(len as nat, |_j: int| run_value(1)));
        } else {
            assert(counts_values(cs) == counts_values(e) + Seq::new(len as nat, |_j: int| run_value(0)));
        }
        assert(counts_values(cs) =~= Seq::new(len as nat, |_i: int| value));
    }
    s
}

impl<'a, C: CountsOf> crate::cursor::ReadsAgree<'a, crate::bool_wire::BoolReads<C>> for BoolModel<C> {
    open spec fn own(v: bool) -> bool {
        v
    }

    proof fn lemma_reads(s: BSlab) {
        crate::bool_wire::lemma_reads_are::<C>(s);
        lemma_bslab::<C::K>(s);
        let x = bool_values::<C::K>(s.data@);
        assert(x.map_values(|v: bool| v) =~= x);
    }

    type Ins = BoolIns;

    #[inline(always)]
    fn ins(v: bool) -> (x: BoolIns) {
        BoolIns(v)
    }
}

/// A well-formed bool slab is one of the edit model's.
pub(crate) proof fn lemma_model_ok<C: CountsOf>(s: BSlab)
    requires
        bslab_ok::<C::K>(s),
    ensures
        <BoolModel<C> as Slabs>::ok(s),
{
}

/// A value read from a bool column, to insert into a rebuild.
pub struct BoolIns(pub(crate) bool);

impl<C: CountsOf> crate::cursor::Insertable<BoolModel<C>> for BoolIns {
    open spec fn val(&self) -> bool {
        self.0
    }

    #[inline(always)]
    fn insert_into(self, e: &mut BoolEdit<C>, n: usize) {
        e.e.insert::<C::K>(self.0, n);
    }
}

impl<C: CountsOf> Slabs for BoolModel<C> {
    type Tail = u8;
    type Rebuild = BoolEdit<C>;
    type State = BoolDecoderState;
    type V = bool;

    closed spec fn ok(s: BSlab) -> bool {
        bslab_ok::<C::K>(s)
    }

    closed spec fn vals(s: BSlab) -> Seq<bool> {
        bool_values::<C::K>(s.data@)
    }

    proof fn lemma_len(s: BSlab) {
        lemma_bslab::<C::K>(s);
    }

    closed spec fn rb_inv(e: BoolEdit<C>) -> bool {
        e.e.inv::<C::K>()
    }

    closed spec fn rb_data(e: BoolEdit<C>) -> Seq<u8> {
        e.e.data@
    }

    closed spec fn rb_vals(e: BoolEdit<C>) -> Seq<bool> {
        e.e.vals()
    }

    closed spec fn rb_read(e: BoolEdit<C>) -> nat {
        e.e.read as nat
    }

    closed spec fn st_at(data: Seq<u8>, st: BoolDecoderState, pos: nat) -> bool {
        bstate_ok::<C::K>(data, st) && bstate_pos::<C::K>(data, st) == pos
    }

    #[verifier::external_body]
    #[inline(always)]
    fn carry_budget() -> usize {
        crate::edit::RUN_BUDGET
    }

    #[inline(always)]
    fn idle() -> BoolEdit<C> {
        BoolEdit { e: BEdit::idle(), _phantom: PhantomData }
    }

    fn empty_slab() -> (s: BSlab) {
        let s = new_bslab_vec(Vec::new(), 0, 0, 0);
        proof {
            reveal(bslab_bytes);
            reveal(bslab_len);
            reveal(bslab_segments);
            reveal(bslab_tail);
            let e = Seq::<u64>::empty();
            assert(counts_bytes::<C::K>(e) =~= s.data@);
            lemma_counts::<C::K>(e, s.data@);
            assert(counts_values(e) =~= Seq::<bool>::empty());
            lemma_bslab_ok::<C::K>(s);
        }
        s
    }

    proof fn lemma_copy(a: BSlab, b: BSlab) {
        reveal(bslab_bytes);
        reveal(bslab_len);
        reveal(bslab_segments);
        reveal(bslab_tail);
    }

    #[inline(always)]
    fn reset(e: &mut BoolEdit<C>, s: &BSlab, at: usize, max_segments: usize) {
        proof {
            lemma_bslab::<C::K>(*s);
        }
        e.e = BEdit::open::<C::K>(s.data.as_slice(), at, max_segments, take_buf(&mut e.e.w.buf));
    }

    fn close(e: &mut BoolEdit<C>, s: &mut BSlab) -> (spilled: Vec<BSlab>) {
        proof {
            lemma_bslab::<C::K>(*s);
        }
        let ghost data = s.data@;
        let ghost out = e.e.vals() + bool_values::<C::K>(data).skip(e.e.read as int);
        let r = e.e.close::<C::K>(s.data.as_slice(), s.len, s.segments, s.tail);
        r.apply_bytes(&mut s.data);
        s.len = r.len;
        s.segments = r.segments;
        s.tail = r.tail;
        proof {
            lemma_bslab_ok::<C::K>(*s);
            lemma_concat_bslabs::<C>(r.overflow@);
        }
        // hand the buffer back for the next slab this cursor edits
        e.e.w.buf = r.bytes;
        r.overflow
    }

    fn merge(a: &mut BSlab, b: BSlab) {
        proof {
            lemma_bslab::<C::K>(*a);
            lemma_bslab::<C::K>(b);
        }
        if a.len == 0 {
            proof {
                assert(bool_values::<C::K>(a.data@) =~= Seq::<bool>::empty());
                assert(Seq::<bool>::empty() + bool_values::<C::K>(b.data@) =~= bool_values::<C::K>(b.data@));
            }
            *a = b;
        } else if b.len > 0 {
            let ghost a0 = a.data@;
            proof {
                assert(a0.skip(0) =~= a0);
            }
            let (segments, tail) = merge::<C::K>(&mut a.data, 0, a.tail, a.segments, b.data.as_slice(), b.segments, b.tail);
            a.len = a.len + b.len;
            a.segments = segments;
            a.tail = tail;
            proof {
                assert(a.data@.skip(0) =~= a.data@);
                lemma_values_len(bool_counts::<C::K>(a.data@));
                lemma_bslab_ok::<C::K>(*a);
            }
        } else {
            proof {
                assert(bool_values::<C::K>(b.data@) =~= Seq::<bool>::empty());
                assert(bool_values::<C::K>(a.data@) + Seq::<bool>::empty() =~= bool_values::<C::K>(a.data@));
            }
        }
    }

    #[inline(always)]
    fn read_state(e: &BoolEdit<C>) -> &BoolDecoderState {
        &e.e.dec
    }

    // out of line: a reader made afresh is the rare case, and its decode
    // loop inline would cost the common read (`EditCore::reader`) a frame
    #[inline(never)]
    fn state_at(s: &BSlab, at: usize) -> (st: BoolDecoderState) {
        proof {
            lemma_bslab::<C::K>(*s);
        }
        let mut d = BDec::new::<C::K>(s.data.as_slice());
        if at > 0 {
            d.nth::<C::K>(at - 1);
        }
        d.suspend()
    }

    #[inline(always)]
    fn state_advance(s: &BSlab, st: &mut BoolDecoderState, n: usize, pos: Ghost<nat>) {
        proof {
            lemma_bslab::<C::K>(*s);
        }
        let mut d = BDec::resume::<C::K>(s.data.as_slice(), st);
        if d.skip_in_run::<C::K>(n) {
            d.suspend_into(st);
            return;
        }
        advance_long::<C>(s, st, n, pos);
    }

    #[inline(always)]
    #[allow(unused_variables)]
    fn state_skip(s: &BSlab, st: &mut BoolDecoderState, n: usize, pos: Ghost<nat>) -> (r: bool) {
        proof {
            lemma_bslab::<C::K>(*s);
        }
        let mut d = BDec::resume::<C::K>(s.data.as_slice(), st);
        if d.skip_in_run::<C::K>(n) {
            d.suspend_into(st);
            return true;
        }
        false
    }
}

/// [`Slabs::state_advance`] past the end of the run in hand: out of
/// line, so a step inside it needs no frame.
#[inline(never)]
#[allow(unused_variables)]
fn advance_long<C: CountsOf>(s: &BSlab, st: &mut BoolDecoderState, n: usize, pos: Ghost<nat>)
    requires
        BoolModel::<C>::ok(*s),
        BoolModel::<C>::st_at(s.data@, *old(st), pos@),
        pos@ + n <= s.len,
    ensures
        BoolModel::<C>::st_at(s.data@, *final(st), (pos@ + n) as nat),
{
    proof {
        lemma_bslab::<C::K>(*s);
    }
    let mut d = BDec::resume::<C::K>(s.data.as_slice(), st);
    if n > 0 {
        d.nth::<C::K>(n - 1);
    }
    d.suspend_into(st);
}

/// Carry `n` of the slab's values through the rebuild, telling `f` each
/// run: [`Visitor::pass`](crate::cursor::Visitor::pass) for bool slabs,
/// which the encoding's `EditSlab::pass` forwards to.
#[inline(always)]
pub(crate) fn pass<C: CountsOf, G: Visit<bool>>(e: &mut BoolEdit<C>, s: &BSlab, n: usize, f: &mut G) -> (left: usize)
    requires
        BoolModel::<C>::rb_inv(*old(e)),
        BoolModel::<C>::ok(*s),
        BoolModel::<C>::rb_data(*old(e)) == s.data@,
        BoolModel::<C>::rb_read(*old(e)) + n <= s.len,
        BoolModel::<C>::rb_vals(*old(e)).len() + n + 3 <= usize::MAX,
    ensures
        BoolModel::<C>::rb_inv(*final(e)),
        BoolModel::<C>::rb_data(*final(e)) == s.data@,
        left <= n,
        BoolModel::<C>::rb_read(*final(e)) == BoolModel::<C>::rb_read(*old(e)) + (n - left),
        BoolModel::<C>::rb_vals(*final(e)) == BoolModel::<C>::rb_vals(*old(e)) + BoolModel::<C>::vals(*s).subrange(
            BoolModel::<C>::rb_read(*old(e)) as int,
            BoolModel::<C>::rb_read(*old(e)) + (n - left),
        ),
{
    proof {
        lemma_bslab::<C::K>(*s);
    }
    e.e.walk::<C::K, G>(s.data.as_slice(), n, true, f)
}

/// Drop `n` of the slab's values from the rebuild, telling `f` each run:
/// [`Visitor::discard`](crate::cursor::Visitor::discard) for bool slabs.
#[inline(always)]
pub(crate) fn discard<C: CountsOf, G: Visit<bool>>(e: &mut BoolEdit<C>, s: &BSlab, n: usize, f: &mut G)
    requires
        BoolModel::<C>::rb_inv(*old(e)),
        BoolModel::<C>::ok(*s),
        BoolModel::<C>::rb_data(*old(e)) == s.data@,
        BoolModel::<C>::rb_read(*old(e)) + n <= s.len,
    ensures
        BoolModel::<C>::rb_inv(*final(e)),
        BoolModel::<C>::rb_data(*final(e)) == s.data@,
        BoolModel::<C>::rb_read(*final(e)) == BoolModel::<C>::rb_read(*old(e)) + n,
        BoolModel::<C>::rb_vals(*final(e)) == BoolModel::<C>::rb_vals(*old(e)),
{
    proof {
        lemma_bslab::<C::K>(*s);
    }
    let ghost v0 = e.e.vals();
    e.e.walk::<C::K, G>(s.data.as_slice(), n, false, f);
    assert(e.e.vals() =~= v0);
}

/// The values of slabs as the read model sees them: the loader's.
pub(crate) proof fn lemma_read_bslabs<C: CountsOf>(ss: Seq<BSlab>)
    ensures
        crate::iter_core::vals_of::<BoolReads<C>>(ss) == bslabs_values::<C::K>(ss),
    decreases ss.len(),
{
    let e = crate::iter_core::each::<BoolReads<C>>(ss);
    if ss.len() == 0 {
        assert(e =~= Seq::<Seq<bool>>::empty());
    } else {
        lemma_read_bslabs::<C>(ss.drop_last());
        crate::bool_wire::lemma_reads_are::<C>(ss.last());
        reveal(bslab_bytes);
        assert(e.drop_last() =~= crate::iter_core::each::<BoolReads<C>>(ss.drop_last()));
        assert(e.last() == <BoolReads<C> as crate::iter_core::Decoders<'static>>::vals(ss.last()));
    }
}

/// Well-formed bool slabs are the edit model's.
proof fn lemma_bmodel_all<C: CountsOf>(ss: Seq<BSlab>)
    requires
        forall|i: int| 0 <= i < ss.len() ==> bslab_ok::<C::K>(#[trigger] ss[i]),
    ensures
        crate::cursor::all_ok::<BoolModel<C>>(ss),
{
    assert forall|i: int| 0 <= i < ss.len() implies <BoolModel<C> as crate::cursor::Slabs>::ok(#[trigger] ss[i]) by {
        lemma_model_ok::<C>(ss[i]);
    }
}



/// A bool column's loader for codec `C` (`BoolLoad`), as [`crate::load_core::Loads`].
pub struct BoolLoader<'a, C> {
    pub(crate) l: BoolLoad<'a>,
    pub(crate) _p: PhantomData<C>,
}

impl<'a, C: CountsOf> crate::load_core::Loads<'a, BoolModel<C>, BoolReads<C>> for BoolLoader<'a, C> {
    open spec fn inv(&self) -> bool {
        self.l.inv::<C::K>()
    }

    #[inline(always)]
    fn start(data: &'a [u8], max_segments: usize) -> (l: Self) {
        BoolLoader { l: BoolLoad::new::<C::K>(data, max_segments), _p: PhantomData }
    }

    open spec fn pulled(&self) -> Seq<bool> {
        self.l.values_read()
    }

    open spec fn done_slabs(&self) -> Seq<BSlab> {
        self.l.done_slabs()
    }

    open spec fn at_end(&self) -> bool {
        self.l.at_end()
    }

    open spec fn left(&self) -> int {
        self.l.left()
    }

    open spec fn data(&self) -> Seq<u8> {
        self.l.data()
    }

    open spec fn values_of(d: Seq<u8>) -> Seq<bool> {
        crate::bool_wire::bool_values::<C::K>(d)
    }

    open spec fn valid(d: Seq<u8>) -> bool {
        crate::bool_wire::bool_valid::<C::K>(d)
    }

    open spec fn accepts(d: Seq<u8>) -> bool {
        crate::bool_wire::bool_valid::<C::K>(d)
    }

    proof fn lemma_done(&self) {
        self.l.lemma_done_prefix::<C::K>();
        lemma_read_bslabs::<C>(self.l.done_slabs());
        self.l.lemma_done_ok::<C::K>();
        lemma_bmodel_all::<C>(self.l.done_slabs());
    }

    #[inline(always)]
    fn try_next_run(&mut self) -> (r: Result<Option<Run<bool>>, PackError>) {
        let r = self.l.try_next_run::<C::K>();
        proof {
            lemma_read_bslabs::<C>(self.l.done_slabs());
        }
        r
    }

    #[inline(always)]
    fn slabs_completed(&self) -> (n: usize) {
        self.l.slabs_completed()
    }

    #[inline(always)]
    fn finalize(self) -> (r: Result<Vec<BSlab>, PackError>) {
        let mut l = self.l;
        let r = l.finalize::<C::K>();
        proof {
            if r is Ok {
                lemma_read_bslabs::<C>(r->Ok_0@);
                lemma_bmodel_all::<C>(r->Ok_0@);
            }
        }
        r
    }
}

} // verus!
