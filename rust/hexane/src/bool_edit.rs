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
        if self.cur_count > 0 {
            let ghost c0 = self.chunk();
            let ghost x = self.cur_count as u64;
            let n0 = self.buf.len();
            K::put_count(self.cur_count, &mut self.buf);
            let n1 = self.buf.len();
            proof {
                K::axiom_enc(x, x, Seq::empty(), Seq::empty());
                lemma_bytes_push::<K>(self.cb@, x);
                lemma_vals_push(c0, x);
                lemma_vals_at_zero(self.cb@);
                lemma_values_len(self.cb@);
                lemma_bytes_len::<K>(self.cb@.push(x));
                lemma_vals_len(c0, 0);
                lemma_concat::<K>(if self.overflowed { Seq::empty() } else { self.base@ }, self.cb@);
                assert(c0 == (if self.overflowed { Seq::<u64>::empty() } else { self.base@ }) + self.cb@);
            }
            self.tail_byte = (n1 - n0) as u8;
            self.emitted = self.emitted + self.cur_count;
            self.segments = self.segments + 1;
            self.cur_value = !self.cur_value;
            self.cur_count = 0;
            proof {
                self.cb = Ghost(self.cb@.push(x));
                assert(self.chunk() =~= c0.push(x));
                assert(run_of(0, self.cur_value) =~= Seq::<bool>::empty());
                assert(self.vals() =~= old(self).vals());
                assert(counts_pre(self.chunk())) by {
                    assert forall|i: int| 1 <= i < self.chunk().len() implies #[trigger] self.chunk()[i] > 0 by {
                        if i < c0.len() {
                            assert(self.chunk()[i] == c0[i]);
                        }
                    }
                }
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
        if n == 0 {
            assert(old(self).vals() + run_of(0, value) =~= old(self).vals());
            return;
        }
        if value == self.cur_value {
            self.cur_count = self.cur_count + n;
            proof {
                assert(run_of(old(self).cur_count as nat, value) + run_of(n as nat, value)
                    =~= run_of(self.cur_count as nat, value));
                assert(self.vals() =~= old(self).vals() + run_of(n as nat, value));
            }
            return;
        }
        if self.cur_count == 0 && self.segments == 0 && !self.overflowed {
            // the rebuild starts the slab and its first run is true, so
            // the slab needs a zero-count false pad to start on
            let n0 = self.buf.len();
            K::put_count(0, &mut self.buf);
            let n1 = self.buf.len();
            proof {
                K::axiom_enc(0, 0, Seq::empty(), Seq::empty());
                lemma_bytes_push::<K>(Seq::empty(), 0);
                assert(self.cb@ =~= Seq::<u64>::empty());
                assert(Seq::<u64>::empty().push(0) =~= seq![0u64]);
                assert(self.base@ =~= Seq::<u64>::empty());
            }
            self.tail_byte = (n1 - n0) as u8;
            self.segments = self.segments + 1;
            proof {
                self.cb = Ghost(seq![0u64]);
                assert(self.chunk() =~= seq![0u64]);
                assert(counts_bytes::<K>(Seq::<u64>::empty()) =~= Seq::<u8>::empty());
                lemma_vals_push(Seq::empty(), 0);
                assert(vals_at(Seq::<u64>::empty(), 0) =~= Seq::<bool>::empty());
                assert(run_of(0, run_value(0)) =~= Seq::<bool>::empty());
            }
        } else {
            self.flush_run::<K>();
        }
        self.cur_value = value;
        self.cur_count = n;
        proof {
            assert(run_of(0, !value) =~= Seq::<bool>::empty());
            assert(self.vals() =~= old(self).vals() + run_of(n as nat, value));
            lemma_counts_len_le(self.chunk());
            lemma_vals_len(self.chunk(), 0);
            lemma_chunk_len(self.chunk(), n as nat, self.vals().len(), chunks_values(self.done@).len());
            lemma_concat::<K>(if self.overflowed { Seq::empty() } else { self.base@ }, self.cb@);
        }
        if self.standing + self.segments >= self.target {
            self.cut::<K>();
        }
    }
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
#[verifier::rlimit(100)]
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
        self.dec = dec.suspend();
        want
    }

    /// What follows the reader. Reads one count — the run the cursor
    /// stands in; everything past it is bytes.
    #[verifier::spinoff_prover]
    #[verifier::rlimit(150)]
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
        let ghost vs = bool_values::<K>(data@);
        let ghost k = bstate_k::<K>(data@, self.dec);
        let data_len = data.len();
        proof {
            lemma_counts::<K>(cs, data@);
            lemma_values_len(cs);
            lemma_values_split(cs, k);
            lemma_take_bytes_le::<K>(cs, k);
            assert(cs.take(k) =~= cs.take(k));
            assert forall|i: int| k <= i < cs.len() implies #[trigger] cs[i] > 0 by {}
            if k >= 1 {
                assert(cs.take(k).drop_last() =~= cs.take(k - 1));
                lemma_vals_len(cs.take(k), 0);
                lemma_vals_len(cs.take(k - 1), 0);
            }
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
                let rc = cs.skip(k);
                let a = vals_at(cs.take(k - 1), 0);
                let run = run_of(cs[k - 1] as nat, run_value(k - 1));
                assert(vals_at(cs.take(k), 0) == a + run);
                let lt = counts_len(cs.take(k)) as int;
                assert(self.read == lt - partial);
                assert(vs.skip(self.read as int) =~= run_of(partial as nat, t.value) + vals_at(rc, k));
                lemma_vals_len(rc, k);
                assert(cs =~= cs.take(k) + rc);
                lemma_concat::<K>(cs.take(k), rc);
            }
            return (t, Ghost(k));
        }
        if self.dec.byte_pos < data_len {
            proof {
                lemma_next_count::<K>(data@, cs, k, self.dec.byte_pos as int);
                lemma_count_le(cs, k);
            }
            let rest = &data[self.dec.byte_pos..];
            assert(rest@ == data@.skip(self.dec.byte_pos as int));
            let (cb, count) = match K::read_count(rest) {
                Some(v) => v,
                None => (0, 0),
            };
            proof {
                K::axiom_enc(count as u64, cs[k], rest@.skip(cb as int), counts_bytes::<K>(cs.skip(k + 1)));
                assert(rest@ =~= rest@.take(cb as int) + rest@.skip(cb as int));
                lemma_take_bytes_le::<K>(cs, k + 1);
                lemma_bytes_len::<K>(cs);
                assert(cs =~= cs.take(k + 1) + cs.skip(k + 1));
                lemma_concat::<K>(cs.take(k + 1), cs.skip(k + 1));
                assert(cs.take(k + 1) =~= cs.take(k).push(cs[k]));
                lemma_bytes_push::<K>(cs.take(k), cs[k]);
            }
            let t = BTail {
                value: !self.dec.value,
                partial: count,
                start: self.dec.byte_pos + cb,
                items: slab_len - self.read - count,
                segments: slab_segments - self.read_segments - 1,
            };
            proof {
                let rc = cs.skip(k + 1);
                lemma_vals_first(cs.skip(k), k);
                assert(cs.skip(k).skip(1) =~= rc);
                assert(cs.skip(k)[0] == cs[k]);
                assert(vs.skip(self.read as int) =~= vals_at(cs.skip(k), k));
                lemma_vals_len(rc, k + 1);
                assert(cs =~= cs.take(k + 1) + rc);
                lemma_concat::<K>(cs.take(k + 1), rc);
                assert(cs.take(k + 1) =~= cs.take(k).push(cs[k]));
                lemma_bytes_push::<K>(cs.take(k), cs[k]);
            }
            return (t, Ghost(k + 1));
        }
        proof {
            if k < cs.len() {
                lemma_take_bytes_mono::<K>(cs, k, cs.len() as int);
                assert(cs.take(cs.len() as int) =~= cs);
            }
            assert(cs.skip(k) =~= Seq::<u64>::empty());
            assert(cs.take(k) =~= cs);
            assert(vs.skip(self.read as int) =~= Seq::<bool>::empty());
            assert(run_of(0, self.dec.value) + vals_at(cs.skip(k), k) =~= Seq::<bool>::empty());
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
    #[verifier::rlimit(100)]
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
        let ghost cs = bool_counts::<K>(data@);
        let ghost rc = cs.skip(j);
        let ghost f = self.w.chunk();
        let ghost d = self.w.done@;
        let ghost base = self.w.base@;
        let data_len = data.len();
        proof {
            reveal(out_ok);
            lemma_counts::<K>(cs, data@);
            assert(cs =~= cs.take(j) + rc);
            lemma_concat::<K>(cs.take(j), rc);
            lemma_take_bytes_le::<K>(cs, j);
            lemma_vals_at_concat(f, rc, 0);
            if rc.len() > 0 {
                lemma_vals_at_parity(rc, f.len() as int, j);
            } else {
                assert(vals_at(rc, f.len() as int) =~= Seq::<bool>::empty());
                assert(vals_at(rc, j) =~= Seq::<bool>::empty());
            }
            lemma_vals_len(f, 0);
            lemma_vals_len(rc, j);
        }
        // the slab's last count is unchanged when any tail bytes remain
        let tail = if tail_start < data_len { slab_tail } else { self.w.tail_byte };
        let mut bytes = Vec::new();
        core::mem::swap(&mut self.w.buf, &mut bytes);
        let ghost fr = f + rc;
        proof {
            assert(base.len() <= j);
            assert(cs.take(j) =~= base + cs.take(j).skip(base.len() as int));
            lemma_concat::<K>(base, cs.take(j).skip(base.len() as int));
            lemma_concat::<K>(f, rc);
            lemma_concat::<K>(base, self.w.cb@);
            assert(f == base + self.w.cb@);
            let rs = data@.take(self.byte_start as int) + bytes@ + data@.skip(tail_start as int);
            assert(data@.take(self.byte_start as int) =~= counts_bytes::<K>(base)) by {
                assert(data@.take(tail_start as int) == counts_bytes::<K>(cs.take(j)));
            }
            assert(data@.skip(tail_start as int) =~= counts_bytes::<K>(rc));
            assert(rs =~= counts_bytes::<K>(fr));
            assert(counts_ok(fr)) by {
                assert forall|i: int| 1 <= i < fr.len() implies #[trigger] fr[i] > 0 by {
                    if i >= f.len() {
                        assert(fr[i] == rc[i - f.len()]);
                    } else {
                        assert(fr[i] == f[i]);
                    }
                }
                if fr.len() > 0 {
                    if rc.len() > 0 {
                        assert(fr.last() == rc.last());
                    } else {
                        assert(fr =~= f);
                    }
                }
            }
            lemma_counts::<K>(fr, rs);
            lemma_vals_at_zero(fr);
            assert(chunks_values(d) =~= Seq::<bool>::empty());
            if rc.len() > 0 {
                assert(fr.last() == cs.last());
                assert(rc.last() == cs.last());
            } else {
                assert(fr =~= f);
                if f.len() > 0 {
                    assert(f.last() == self.w.cb@.last());
                }
            }
            assert(bslabs_values::<K>(Seq::<BSlab>::empty()) =~= Seq::<bool>::empty());
        }
        proof {
            lemma_pre_len(fr);
            lemma_concat::<K>(base, self.w.cb@);
        }
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
            let rs = data@.take(r.start as int) + r.bytes@ + data@.skip(r.end as int);
            assert(bool_values::<K>(rs) + bslabs_values::<K>(r.overflow@) =~= bool_values::<K>(rs));
            assert(r.start <= r.end <= data@.len());
            assert(bool_valid::<K>(rs));
            let cr = bool_counts::<K>(rs);
            assert(cr == fr);
            assert(r.len == counts_len(cr));
            assert(r.segments == cr.len());
            if tail_start < data_len {
                if rc.len() == 0 {
                    assert(cs.take(j) =~= cs);
                }
                assert(rc.last() == cs.last());
            } else {
                if rc.len() > 0 {
                    lemma_take_bytes_mono::<K>(cs, j, cs.len() as int);
                    assert(cs.take(cs.len() as int) =~= cs);
                }
                assert(fr =~= f);
            }
            assert(r.tail == counts_tail::<K>(cr));
            assert(bool_values::<K>(rs) == chunks_values(d) + vals_at(f, 0) + vals_at(rc, j));
        }
        r
    }

    #[verifier::spinoff_prover]
    #[verifier::rlimit(100)]
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
        let ghost f = self.w.chunk();
        let ghost d = self.w.done@;
        let ghost base = self.w.base@;
        let ghost fr = f + rc;
        let data_len = data.len();
        proof {
            reveal(out_ok);
            lemma_counts::<K>(cs, data@);
            assert(cs =~= cs.take(j) + rc);
            lemma_concat::<K>(cs.take(j), rc);
            lemma_take_bytes_le::<K>(cs, j);
            lemma_vals_at_concat(f, rc, 0);
            if rc.len() > 0 {
                lemma_vals_at_parity(rc, f.len() as int, j);
            } else {
                assert(vals_at(rc, f.len() as int) =~= Seq::<bool>::empty());
                assert(vals_at(rc, j) =~= Seq::<bool>::empty());
            }
            lemma_vals_len(f, 0);
            lemma_vals_len(rc, j);
            assert(f =~= self.w.cb@);
            lemma_concat::<K>(f, rc);
            lemma_chunks_first(d);
            lemma_vals_len(d[0], 0);
            lemma_concat::<K>(base, self.w.out_cb@);
            assert(counts_ok(fr)) by {
                assert forall|i: int| 1 <= i < fr.len() implies #[trigger] fr[i] > 0 by {
                    if i >= f.len() {
                        assert(fr[i] == rc[i - f.len()]);
                    } else {
                        assert(fr[i] == f[i]);
                    }
                }
                if rc.len() > 0 {
                    assert(fr.last() == rc.last());
                } else {
                    assert(fr =~= f);
                }
            }
            lemma_pre_len(fr);
        }
        // the slab's last count is unchanged when any tail bytes remain
        let tail = if tail_start < data_len { slab_tail } else { self.w.tail_byte };
        let mut bytes = Vec::new();
        core::mem::swap(&mut self.w.buf, &mut bytes);
        // the chunk in hand and the carried tail are a new slab
        let rest = &data[tail_start..];
        assert(rest@ == data@.skip(tail_start as int));
        proof {
            assert(rest@ =~= counts_bytes::<K>(rc));
            if tail_start < data_len {
                if rc.len() == 0 {
                    assert(cs.take(j) =~= cs);
                }
                assert(fr.last() == rc.last());
                assert(rc.last() == cs.last());
            } else {
                if rc.len() > 0 {
                    lemma_take_bytes_mono::<K>(cs, j, cs.len() as int);
                    assert(cs.take(cs.len() as int) =~= cs);
                }
                assert(fr =~= f);
                assert(f.last() == self.w.cb@.last());
            }
        }
        let mut last = bytes;
        extend_vec(&mut last, rest);
        let s = new_bslab_vec(last, self.w.emitted + raw_items, self.w.segments + raw_segs, tail);
        let mut overflow = Vec::new();
        core::mem::swap(&mut self.w.overflow, &mut overflow);
        let ghost o0 = overflow@;
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
            let rs = data@.take(r.start as int) + r.bytes@ + data@.skip(r.end as int);
            let d0 = d[0];
            assert(data@.skip(data_len as int) =~= Seq::<u8>::empty());
            assert(cs =~= base + cs.skip(base.len() as int));
            lemma_concat::<K>(base, cs.skip(base.len() as int));
            assert(data@.take(self.byte_start as int) =~= counts_bytes::<K>(base));
            assert(rs =~= counts_bytes::<K>(d0));
            lemma_counts::<K>(d0, rs);
            lemma_vals_at_zero(d0);
            // the slabs: those cut after the first, then the last
            let ds = d.skip(1).push(fr);
            assert forall|i: int| 0 <= i < r.overflow@.len() implies bslab_bytes(#[trigger] r.overflow@[i]) == counts_bytes::<K>(ds[i])
                && counts_ok(ds[i]) && counts_len(ds[i]) <= usize::MAX by {
                if i < o0.len() {
                    assert(r.overflow@[i] == o0[i]);
                    assert(ds[i] == d[i + 1]);
                    lemma_chunk_le(d, i + 1);
                    lemma_vals_len(d[i + 1], 0);
                } else {
                    assert(ds[i] == fr);
                }
            }
            assert forall|i: int| 0 <= i < r.overflow@.len() implies bslab_ok::<K>(#[trigger] r.overflow@[i]) by {
                lemma_counts::<K>(ds[i], bslab_bytes(r.overflow@[i]));
                if i < o0.len() {
                    assert(r.overflow@[i] == o0[i]);
                    assert(ds[i] == d[i + 1]);
                }
            }
            lemma_slabs_chunks::<K>(r.overflow@, ds);
            assert(ds.drop_last() =~= d.skip(1));
            assert(chunks_values(ds) == chunks_values(d.skip(1)) + vals_at(fr, 0));
            lemma_vals_at_zero(fr);
            assert(bool_values::<K>(rs) + bslabs_values::<K>(r.overflow@)
                =~= chunks_values(d) + vals_at(f, 0) + vals_at(rc, j));
        }
        r
    }

    /// Write the rebuild back, carrying the slab's remaining bytes
    /// through untouched (or, if they would overfill the slab, as a slab
    /// of their own).
    #[verifier::spinoff_prover]
    #[verifier::rlimit(100)]
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
    #[verifier::rlimit(100)]
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
        proof {
            reveal(out_ok);
            lemma_counts::<K>(cs, data@);
            lemma_take_bytes_le::<K>(cs, cs.len() as int);
            lemma_take_bytes_le::<K>(cs, self.w.base@.len() as int);
            assert(cs.take(cs.len() as int) =~= cs);
        }
        self.w.flush_run::<K>();
        self.w.cur_count = 0;
        proof {
            assert(cs.skip(cs.len() as int) =~= Seq::<u64>::empty());
            assert(run_of(0, self.w.cur_value) =~= Seq::<bool>::empty());
            assert(self.w.vals() =~= chunks_values(self.w.done@) + vals_at(self.w.chunk(), 0));
            assert(vals_at(Seq::<u64>::empty(), cs.len() as int) =~= Seq::<bool>::empty());
            lemma_vals_len(self.w.chunk(), 0);
            lemma_chunks_len(self.w.done@, self.w.chunk(), 0, self.w.cur_value);
            assert(counts_len(Seq::<u64>::empty()) == 0);
        }
        let r0 = self.seal::<K>(data, data.len(), 0, 0, slab_tail, Ghost(cs.len() as int));
        let spill = into_slab::<K>(t, data, slab_tail, Ghost(read), Ghost(j));
        let mut r = r0;
        match spill {
            Some(s) => {
                let ghost o0 = r.overflow@;
                r.overflow.push(s);
                proof {
                    assert(r.overflow@.drop_last() =~= o0);
                    assert forall|i: int| 0 <= i < r.overflow@.len() implies bslab_ok::<K>(#[trigger] r.overflow@[i]) by {
                        if i < o0.len() {
                            assert(r.overflow@[i] == o0[i]);
                        }
                    }
                }
            },
            None => {},
        }
        proof {
            let rs = data@.take(r.start as int) + r.bytes@ + data@.skip(r.end as int);
            assert(bool_values::<K>(rs) + bslabs_values::<K>(r.overflow@) =~= v0 + vs.skip(read));
        }
        r
    }

    /// The tail fits: the partial run goes through the encoder like any
    /// other, which merges it with the pending run when the parities
    /// agree, and pads or cuts when they do not.
    #[verifier::spinoff_prover]
    #[verifier::rlimit(100)]
    fn close_fit<K: CountOps>(&mut self, data: &[u8], slab_tail: u8, t: &BTail, Ghost(j): Ghost<int>) -> (r: BClosed)
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
        let ghost rc = cs.skip(j);
        let ghost read = self.read as int;
        let ghost v0 = self.vals();
        proof {
            reveal(out_ok);
            lemma_counts::<K>(cs, data@);
            lemma_vals_len(rc, j);
            lemma_take_bytes_le::<K>(cs, self.w.base@.len() as int);
            assert(vs.skip(read).len() == t.partial + counts_len(rc));
        }
        self.w.push::<K>(t.value, t.partial);
        let ghost v1 = self.w.vals();
        let ghost cur1 = self.w.cur_count;
        let ghost val1 = self.w.cur_value;
        let ghost c1 = self.w.chunk();
        let ghost cb1 = self.w.cb@;
        self.w.flush_run::<K>();
        self.w.cur_count = 0;
        proof {
            assert(run_of(0, self.w.cur_value) =~= Seq::<bool>::empty());
            assert(self.w.vals() =~= chunks_values(self.w.done@) + vals_at(self.w.chunk(), 0));
            lemma_vals_len(self.w.chunk(), 0);
            lemma_chunks_len(self.w.done@, self.w.chunk(), 0, self.w.cur_value);
            if t.partial > 0 {
                assert(val1 == t.value);
                assert(self.w.chunk() == c1.push(cur1 as u64));
                assert(run_value(self.w.chunk().len() as int) == !t.value);
            } else {
                assert(rc.len() == 0);
            }
            if cur1 > 0 {
                assert(self.w.cb@ == cb1.push(cur1 as u64));
            }
        }
        let r = self.seal::<K>(data, t.start, t.items, t.segments, slab_tail, Ghost(j));
        proof {
            assert(v1 == v0 + run_of(t.partial as nat, t.value));
            assert(v0 + vs.skip(read) =~= v1 + vals_at(rc, j));
        }
        r
    }

    /// Open a rebuild of the valid slab `data` at item `at`.
    #[verifier::spinoff_prover]
    #[verifier::rlimit(60)]
    #[allow(unused_assignments)] // the scan's results are set where it stops
    pub(crate) fn open<K: CountOps>(data: &[u8], at: usize, max_segments: usize, buf: Vec<u8>) -> (e: Self)
        requires
            bool_valid::<K>(data@), at <= bool_values::<K>(data@).len(), max_segments >= 2,
        ensures
            e.inv::<K>(), e.data@ == data@, e.read == at, e.vals() == bool_values::<K>(data@).take(at as int),
    {
        let ghost cs = bool_counts::<K>(data@);
        let len = data.len();
        proof {
            lemma_counts::<K>(cs, data@);
            lemma_values_len(cs);
            assert(cs.take(0) =~= Seq::<u64>::empty());
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
            proof {
                lemma_take_bytes_le::<K>(cs, i);
            }
            if byte >= len {
                proof {
                    if i < cs.len() {
                        lemma_take_bytes_mono::<K>(cs, i, cs.len() as int);
                        assert(cs.take(cs.len() as int) =~= cs);
                    }
                    assert(cs.take(i) =~= cs);
                    assert(i == cs.len());
                }
                header = byte;
                off = 0;
                cbytes = 0;
                count = 0;
                break;
            }
            proof {
                lemma_next_count::<K>(data@, cs, i, byte as int);
                lemma_count_le(cs, i);
            }
            let rest = &data[byte..];
            assert(rest@ == data@.skip(byte as int));
            let (cb, c) = match K::read_count(rest) {
                Some(v) => v,
                None => (0, 0),
            };
            proof {
                K::axiom_enc(c as u64, cs[i], rest@.skip(cb as int), counts_bytes::<K>(cs.skip(i + 1)));
                assert(rest@ =~= rest@.take(cb as int) + rest@.skip(cb as int));
                assert(cs.take(i + 1).drop_last() =~= cs.take(i));
                lemma_values_take(cs, i + 1);
                assert(cs.take(i + 1) =~= cs.take(i).push(cs[i]));
                lemma_bytes_push::<K>(cs.take(i), cs[i]);
                lemma_take_bytes_le::<K>(cs, i + 1);
                assert(cs =~= cs.take(i + 1) + cs.skip(i + 1));
                lemma_concat::<K>(cs.take(i + 1), cs.skip(i + 1));
                assert(counts_len(cs.take(i + 1)) <= counts_len(cs));
                assert(item + c == counts_len(cs.take(i + 1)));
                lemma_bytes_len::<K>(cs);
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
            assert(counts_pre(base)) by {
                assert forall|j: int| 1 <= j < base.len() implies #[trigger] base[j] > 0 by {
                    assert(base[j] == cs[j]);
                }
            }
            if off > 0 {
                lemma_values_run_take(cs, i, off as int);
            } else if prev is Some {
                assert(cs.take(i).drop_last() =~= cs.take(i - 1));
                lemma_values_run_take(cs, i - 1, cs[i - 1] as int);
                assert(counts_len(cs.take(i - 1)) + cs[i - 1] == counts_len(cs.take(i)));
            } else {
                assert(base =~= Seq::<u64>::empty());
                assert(counts_len(cs.take(i)) == 0) by {
                    if i == 1 {
                        assert(cs.take(1).drop_last() =~= cs.take(0));
                    }
                }
                lemma_values_take(cs, 0);
                assert(counts_bytes::<K>(base) =~= Seq::<u8>::empty());
            }
            lemma_vals_at_zero(base);
            lemma_values_len(base);
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
            let k = if i < cs.len() { i + 1 } else { i };
            if i < cs.len() {
                lemma_bytes_push::<K>(cs.take(i), cs[i]);
                assert(cs.take(i + 1) =~= cs.take(i).push(cs[i]));
                assert(cs.take(i + 1).drop_last() =~= cs.take(i));
            }
            assert(dstate_at::<K>(cs, dec, k));
            lemma_k_unique::<K>(cs, dec, bstate_k::<K>(data@, dec), k);
            if i < cs.len() {
                assert(counts_len(cs.take(i + 1)) == counts_len(cs.take(i)) + cs[i]);
            }
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

    #[allow(unused_variables)]
    fn state_advance(s: &BSlab, st: &mut BoolDecoderState, n: usize, pos: Ghost<nat>) {
        proof {
            lemma_bslab::<C::K>(*s);
        }
        let mut d = BDec::resume::<C::K>(s.data.as_slice(), st);
        if n > 0 {
            d.nth::<C::K>(n - 1);
        }
        *st = d.suspend();
    }
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

} // verus!
