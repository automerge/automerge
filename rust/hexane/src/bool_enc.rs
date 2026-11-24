//! The bool column encoder's state, verified with Verus.
//!
//! [`BEnc`] writes alternating run counts into a caller's buffer: every
//! count it writes is in `BEnc::written`, and the values appended are
//! the values those counts and the pending run stand for. Encoding
//! without a `finish` in the middle ([`BEnc::clean`]) gives a canonical
//! bool slab once finished.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use crate::bool_edit::new_bslab_vec;
// spec and proof functions exist only when Verus compiles the crate
#[cfg(verus_keep_ghost)]
use crate::bool_edit::counts_pre;
#[cfg(verus_keep_ghost)]
use crate::bool_edit::{lemma_pre_len, lemma_vals_len, lemma_vals_push, run_of};
use crate::bool_wire::*;
#[allow(unused_imports)]
use vstd::prelude::*;

use crate::trusted::bool_enc_too_many_values as too_many_values;
verus! {


/// The bool encoder's run bookkeeping; the output buffer is the caller's.
pub(crate) struct BEnc {
    pub(crate) cur_value: bool,
    pub(crate) cur_count: usize,
    /// counts written since the start (or the last restart)
    pub(crate) segments: usize,
    /// values appended since the encoder was made
    pub(crate) len: usize,
    /// the counts written, the values appended since the start (or the
    /// last restart), and the values before that
    #[allow(dead_code)]
    pub(crate) cs: Ghost<Seq<u64>>,
    #[allow(dead_code)]
    pub(crate) vals: Ghost<Seq<bool>>,
    #[allow(dead_code)]
    pub(crate) before: Ghost<nat>,
    /// no `finish` in the middle: only the first count written may be zero
    #[allow(dead_code)]
    pub(crate) clean: Ghost<bool>,
}

impl BEnc {
    pub(crate) open spec fn inv(&self) -> bool {
        let cs = self.cs@;
        &&& self.segments == cs.len()
        &&& self.len == self.before@ + self.vals@.len() && self.len + 3 <= usize::MAX
        &&& self.vals@ == vals_at(cs, 0) + run_of(self.cur_count as nat, self.cur_value)
        &&& self.cur_value == run_value(cs.len() as int)
        &&& self.clean@ ==> counts_pre(cs)
        &&& self.cur_count == 0 && cs.len() > 0 ==> cs.last() > 0
        &&& self.clean@ ==> cs.len() <= self.vals@.len() + 1
    }

    /// The counts written, once finished: a canonical slab of the values
    /// if `clean`.
    pub(crate) open spec fn finished(&self) -> bool {
        &&& self.cur_count == 0
        &&& self.clean@ ==> counts_ok(self.cs@)
        &&& vals_at(self.cs@, 0) == self.vals@
    }

    /// A fresh encoder.
    pub(crate) fn new() -> (e: Self)
        ensures e.inv(), e.cs@.len() == 0, e.vals@.len() == 0, e.before@ == 0, e.clean@,
    {
        let e = BEnc {
            cur_value: false,
            cur_count: 0,
            segments: 0,
            len: 0,
            cs: Ghost(Seq::empty()),
            vals: Ghost(Seq::empty()),
            before: Ghost(0),
            clean: Ghost(true),
        };
        proof {
            assert(vals_at(Seq::<u64>::empty(), 0) + run_of(0, false) =~= Seq::<bool>::empty());
        }
        e
    }

    /// Write the pending run out (even an empty one: every run must be
    /// counted to keep the parity of those after it) and start the next,
    /// of the other value.
    #[inline(always)]
    fn flush_run<K: CountOps>(&mut self, buf: &mut Vec<u8>)
        requires old(self).inv(),
        ensures
            final(self).cs@ == old(self).cs@.push(old(self).cur_count as u64),
            final(buf)@ == old(buf)@ + K::enc(old(self).cur_count as u64),
            final(self).cur_count == 0, final(self).cur_value == !old(self).cur_value,
            final(self).segments == old(self).segments + 1, final(self).len == old(self).len,
            final(self).vals@ == old(self).vals@, final(self).before@ == old(self).before@,
            final(self).clean@ == (old(self).clean@ && (old(self).cur_count > 0 || old(self).segments == 0)),
    {
        if self.segments == usize::MAX {
            too_many_values(Ghost(true));
        }
        let ghost c0 = self.cs@;
        K::put_count(self.cur_count, buf);
        self.segments = self.segments + 1;
        proof {
            self.cs = Ghost(c0.push(self.cur_count as u64));
            self.clean = Ghost(self.clean@ && (self.cur_count > 0 || c0.len() == 0));
        }
        self.cur_count = 0;
        self.cur_value = !self.cur_value;
    }

    /// `append` of `n` copies of the current run's value: the run grows,
    /// and nothing is written.
    #[inline(always)]
    pub(crate) fn extend(&mut self, n: usize)
        requires
            old(self).inv(),
            n <= usize::MAX - 3 - old(self).len,
        ensures
            final(self).inv(), final(self).before@ == old(self).before@,
            final(self).vals@ == old(self).vals@ + run_of(n as nat, old(self).cur_value),
            final(self).cs@ == old(self).cs@, final(self).clean@ == old(self).clean@,
            final(self).segments == old(self).segments, final(self).cur_value == old(self).cur_value,
            final(self).len == old(self).len + n,
            final(self).cur_count > 0 || (n == 0 && old(self).cur_count == 0),
    {
        proof {
            lemma_vals_len(self.cs@, 0);
        }
        self.cur_count = self.cur_count + n;
        self.len = self.len + n;
        proof {
            let v0 = self.vals@;
            self.vals = Ghost(v0 + run_of(n as nat, self.cur_value));
            assert(run_of(old(self).cur_count as nat, self.cur_value) + run_of(n as nat, self.cur_value)
                =~= run_of(self.cur_count as nat, self.cur_value));
            assert(self.vals@ =~= vals_at(self.cs@, 0) + run_of(self.cur_count as nat, self.cur_value));
        }
    }

    /// Append `n` copies of `value`.
    #[inline(always)]
    pub(crate) fn append<K: CountOps>(&mut self, buf: &mut Vec<u8>, value: bool, n: usize)
        requires
            old(self).inv(),
        ensures
            final(self).inv(), final(self).before@ == old(self).before@,
            final(self).vals@ == old(self).vals@ + run_of(n as nat, value),
            old(self).cs@.is_prefix_of(final(self).cs@),
            final(buf)@ == old(buf)@ + counts_bytes::<K>(final(self).cs@.skip(old(self).cs@.len() as int)),
            final(self).clean@ == (old(self).clean@ && !(n > 0 && value != old(self).cur_value
                && old(self).cur_count == 0 && old(self).segments > 0)),
            final(self).segments <= old(self).segments + 1,
            final(self).cur_count > 0 || n == 0,
            n > 0 ==> final(self).cur_value == value,
            n == 0 ==> *final(self) == *old(self),
    {
        let ghost c0 = self.cs@;
        proof {
            assert(c0.skip(c0.len() as int) =~= Seq::<u64>::empty());
            assert(counts_bytes::<K>(Seq::<u64>::empty()) =~= Seq::<u8>::empty());
            assert(old(buf)@ + Seq::<u8>::empty() =~= old(buf)@);
        }
        if n == 0 {
            assert(old(self).vals@ + run_of(0, value) =~= old(self).vals@);
            return;
        }
        if n > usize::MAX - 3 - self.len {
            too_many_values(Ghost(true));
        }
        if value == self.cur_value {
            self.cur_count = self.cur_count + n;
            self.len = self.len + n;
            proof {
                let v0 = self.vals@;
                self.vals = Ghost(v0 + run_of(n as nat, value));
                assert(run_of(old(self).cur_count as nat, value) + run_of(n as nat, value)
                    =~= run_of(self.cur_count as nat, value));
                assert(self.vals@ =~= vals_at(self.cs@, 0) + run_of(self.cur_count as nat, value));
                lemma_vals_len(c0, 0);
            }
            return;
        }
        self.flush_run::<K>(buf);
        self.cur_value = value;
        self.cur_count = n;
        self.len = self.len + n;
        proof {
            let c1 = self.cs@;
            lemma_vals_push(c0, old(self).cur_count as u64);
            self.vals = Ghost(old(self).vals@ + run_of(n as nat, value));
            assert(run_of(0, value) =~= Seq::<bool>::empty());
            assert(self.vals@ =~= vals_at(c1, 0) + run_of(n as nat, value));
            assert(c1.skip(c0.len() as int) =~= seq![old(self).cur_count as u64]);
            lemma_bytes_push::<K>(Seq::empty(), old(self).cur_count as u64);
            assert(Seq::<u64>::empty().push(old(self).cur_count as u64) =~= seq![old(self).cur_count as u64]);
            assert(counts_bytes::<K>(Seq::<u64>::empty()) =~= Seq::<u8>::empty());
            assert(c0.is_prefix_of(c1));
            if self.clean@ {
                assert forall|i: int| 1 <= i < c1.len() implies #[trigger] c1[i] > 0 by {
                    if i < c0.len() {
                        assert(c1[i] == c0[i]);
                    }
                }
                lemma_vals_len(c1, 0);
                lemma_pre_len(c1);
            }
        }
    }

    /// Flush the pending run. Appending afterwards goes on from the run
    /// after it, which has the other value.
    #[inline(always)]
    pub(crate) fn finish<K: CountOps>(&mut self, buf: &mut Vec<u8>)
        requires old(self).inv(),
        ensures
            final(self).inv(), final(self).finished(), final(self).before@ == old(self).before@,
            final(self).vals@ == old(self).vals@, final(self).clean@ == old(self).clean@,
            old(self).cs@.is_prefix_of(final(self).cs@),
            final(buf)@ == old(buf)@ + counts_bytes::<K>(final(self).cs@.skip(old(self).cs@.len() as int)),
            final(self).segments <= old(self).segments + 1,
            old(self).cur_count > 0 ==> final(self).cs@ == old(self).cs@.push(old(self).cur_count as u64),
            old(self).cur_count == 0 ==> *final(self) == *old(self) && final(buf)@ == old(buf)@,
    {
        let ghost c0 = self.cs@;
        proof {
            assert(c0.skip(c0.len() as int) =~= Seq::<u64>::empty());
            assert(counts_bytes::<K>(Seq::<u64>::empty()) =~= Seq::<u8>::empty());
            assert(old(buf)@ + Seq::<u8>::empty() =~= old(buf)@);
        }
        if self.cur_count > 0 {
            let ghost x = self.cur_count as u64;
            self.flush_run::<K>(buf);
            proof {
                let c1 = self.cs@;
                lemma_vals_push(c0, x);
                assert(run_of(0, self.cur_value) =~= Seq::<bool>::empty());
                assert(self.vals@ =~= vals_at(c1, 0) + run_of(0, self.cur_value));
                assert(c1.skip(c0.len() as int) =~= seq![x]);
                lemma_bytes_push::<K>(Seq::empty(), x);
                assert(Seq::<u64>::empty().push(x) =~= seq![x]);
                assert(c0.is_prefix_of(c1));
                if self.clean@ {
                    assert forall|i: int| 1 <= i < c1.len() implies #[trigger] c1[i] > 0 by {
                        if i < c0.len() {
                            assert(c1[i] == c0[i]);
                        }
                    }
                    lemma_pre_len(c1);
                    lemma_vals_len(c1, 0);
                }
            }
        } else {
            proof {
                assert(run_of(0, self.cur_value) =~= Seq::<bool>::empty());
                assert(self.vals@ =~= vals_at(self.cs@, 0));
            }
        }
    }

    /// Start a new slab after a finish: the values so far count as before
    /// it.
    #[inline(always)]
    pub(crate) fn restart(&mut self)
        requires old(self).inv(), old(self).cur_count == 0,
        ensures
            final(self).inv(), final(self).cs@.len() == 0, final(self).vals@.len() == 0, final(self).clean@,
            final(self).before@ == old(self).len, final(self).len == old(self).len,
    {
        self.segments = 0;
        self.cur_value = false;
        proof {
            self.before = Ghost(self.len as nat);
            self.cs = Ghost(Seq::empty());
            self.vals = Ghost(Seq::empty());
            self.clean = Ghost(true);
            assert(vals_at(Seq::<u64>::empty(), 0) + run_of(0, false) =~= Seq::<bool>::empty());
        }
    }

    /// True if every value appended equals `value` (or there are none) —
    /// decided before flushing. Always true then if `clean`; possibly
    /// false after a `finish` in the middle, never wrongly true.
    pub(crate) fn all_equal_pre_finish(&self, value: bool) -> (r: bool)
        requires self.inv(), self.before@ == 0,
        ensures
            r ==> forall|i: int| 0 <= i < self.vals@.len() ==> #[trigger] self.vals@[i] == value,
            self.clean@ && self.cur_count > 0 && (forall|i: int| 0 <= i < self.vals@.len() ==> #[trigger] self.vals@[i] == value) ==> r,
            self.vals@.len() == 0 ==> r,
    {
        let r = self.len == 0 || (self.cur_count == self.len && self.cur_value == value);
        proof {
            let cs = self.cs@;
            lemma_vals_len(cs, 0);
            if r && self.len > 0 {
                assert(vals_at(cs, 0).len() == 0);
                assert(self.vals@ =~= run_of(self.cur_count as nat, self.cur_value));
            }
            if self.clean@ && self.cur_count > 0
                && (forall|i: int| 0 <= i < self.vals@.len() ==> #[trigger] self.vals@[i] == value) {
                lemma_clean_mixed(cs, self.cur_count as nat, self.cur_value, value);
            }
        }
        r
    }
}

/// Clean counts with values, then a pending run of the other value: the
/// values are not all one.
proof fn lemma_clean_mixed(cs: Seq<u64>, n: nat, v: bool, value: bool)
    requires
        counts_pre(cs), n > 0, v == run_value(cs.len() as int),
        forall|i: int| 0 <= i < (vals_at(cs, 0) + run_of(n, v)).len() ==> #[trigger] (vals_at(cs, 0) + run_of(n, v))[i] == value,
    ensures counts_len(cs) == 0, v == value,
{
    let all = vals_at(cs, 0) + run_of(n, v);
    lemma_vals_len(cs, 0);
    let m = vals_at(cs, 0).len() as int;
    assert(all[m] == v);
    if counts_len(cs) > 0 {
        lemma_vals_tail(cs);
        assert(all[m - 1] == vals_at(cs, 0).last());
        assert(run_value(cs.len() - 1) != run_value(cs.len() as int));
    }
}

/// Clean counts with values: their last count has values, and their
/// values end with its parity.
proof fn lemma_vals_tail(cs: Seq<u64>)
    requires counts_pre(cs), counts_len(cs) > 0,
    ensures
        cs.len() > 0, cs.last() > 0,
        vals_at(cs, 0).len() > 0,
        vals_at(cs, 0).last() == run_value(cs.len() - 1),
{
    if cs.len() == 0 {
        assert(counts_len(cs) == 0);
    }
    if cs.len() >= 2 {
        assert(cs[cs.len() - 1] > 0);
    } else {
        assert(cs.drop_last() =~= Seq::<u64>::empty());
        assert(counts_len(cs) == counts_len(cs.drop_last()) + cs.last());
    }
    assert(cs =~= cs.drop_last().push(cs.last()));
    lemma_vals_push(cs.drop_last(), cs.last());
    lemma_vals_len(cs.drop_last(), 0);
    lemma_vals_len(cs, 0);
}

// ── The encoder ─────────────────────────────────────────────────────────────

/// Bool slabs' values, a slab added.
proof fn lemma_bslabs_push<K: CountOps>(s: Seq<BSlab>, x: BSlab)
    ensures bslabs_values::<K>(s.push(x)) == bslabs_values::<K>(s) + bool_values::<K>(bslab_bytes(x)),
{
    assert(s.push(x).drop_last() =~= s);
}

/// A finished clean encoding: a canonical bool slab of its values.
proof fn lemma_finished_slab<K: CountOps>(e: BEnc, data: Seq<u8>)
    requires e.inv(), e.finished(), e.clean@, data == counts_bytes::<K>(e.cs@),
    ensures
        bool_valid::<K>(data), bool_counts::<K>(data) == e.cs@, bool_values::<K>(data) == e.vals@,
        counts_len(e.cs@) == e.vals@.len(),
{
    lemma_vals_len(e.cs@, 0);
    lemma_vals_at_zero(e.cs@);
    lemma_counts::<K>(e.cs@, data);
}

/// A bool column encoder that cuts its output into slabs of a segment
/// budget as it goes.
pub(crate) struct BEncoder {
    pub(crate) data: Vec<u8>,
    pub(crate) state: BEnc,
    /// completed slabs, populated only when `max_segments` is set
    pub(crate) slabs: Vec<BSlab>,
    pub(crate) max_segments: Option<usize>,
    /// items already rolled into `slabs`
    pub(crate) len_flushed: usize,
}

impl BEncoder {
    pub(crate) open spec fn inv<K: CountOps>(&self) -> bool {
        &&& self.state.inv() && self.state.clean@
        &&& self.data@ == counts_bytes::<K>(self.state.cs@)
        &&& self.state.vals@.len() > 0 ==> self.state.cur_count > 0
        &&& self.len_flushed == self.state.before@
        &&& forall|i: int| 0 <= i < self.slabs@.len() ==> bslab_ok::<K>(#[trigger] self.slabs@[i])
        &&& bslabs_values::<K>(self.slabs@).len() == self.state.before@
    }

    /// Every value appended: the slabs', then the state's.
    pub(crate) open spec fn values<K: CountOps>(&self) -> Seq<bool> {
        bslabs_values::<K>(self.slabs@) + self.state.vals@
    }

    pub(crate) fn new<K: CountOps>(max_segments: Option<usize>) -> (e: Self)
        ensures e.inv::<K>(), e.values::<K>().len() == 0, e.slabs@.len() == 0, e.max_segments == max_segments,
    {
        let e = BEncoder { data: Vec::new(), state: BEnc::new(), slabs: Vec::new(), max_segments, len_flushed: 0 };
        proof {
            assert(e.data@ =~= counts_bytes::<K>(Seq::<u64>::empty()));
            assert(bslabs_values::<K>(Seq::<BSlab>::empty()) =~= Seq::<bool>::empty());
        }
        e
    }

    /// Roll the pending buffer into a completed slab once the segment
    /// budget is reached (no-op unless `max_segments` is set). Only cuts
    /// when `incoming` closes the current run, so the pending run always
    /// lands whole in the finished slab.
    #[inline(always)]
    fn maybe_cut<K: CountOps>(&mut self, incoming: bool)
        requires old(self).inv::<K>(),
        ensures
            final(self).inv::<K>(), final(self).values::<K>() == old(self).values::<K>(),
            final(self).state.len == old(self).state.len,
            final(self).state.segments <= old(self).state.segments,
            final(self).max_segments == old(self).max_segments,
            old(self).max_segments is None ==> final(self).slabs@ == old(self).slabs@,
    {
        let max = match self.max_segments {
            Some(m) => m,
            None => return,
        };
        let target = max / 2;
        if target == 0
            || incoming == self.state.cur_value
            || self.state.cur_count == 0
            || self.state.segments + 1 < target
        {
            return;
        }
        self.cut::<K>();
    }

    /// Roll the pending buffer into a completed slab: `maybe_cut`'s rare
    /// path, out of line so that the check every append makes stays
    /// small.
    #[inline(never)]
    fn cut<K: CountOps>(&mut self)
        requires old(self).inv::<K>(),
        ensures
            final(self).inv::<K>(), final(self).values::<K>() == old(self).values::<K>(),
            final(self).state.len == old(self).state.len,
            final(self).state.segments <= old(self).state.segments,
            final(self).max_segments == old(self).max_segments,
    {
        proof {
            lemma_pre_len(self.state.cs@);
            lemma_vals_len(self.state.cs@, 0);
        }
        // the slab's tail record: the byte length of the last count, the
        // pending run's, which `finish` writes
        let ghost cs0 = self.state.cs@;
        let n0 = self.data.len();
        self.state.finish::<K>(&mut self.data);
        proof {
            assert(self.state.cs@ =~= cs0 + self.state.cs@.skip(cs0.len() as int));
            lemma_concat::<K>(cs0, self.state.cs@.skip(cs0.len() as int));
            if old(self).state.cur_count > 0 {
                let x = old(self).state.cur_count as u64;
                K::axiom_enc(x, x, Seq::empty(), Seq::empty());
                lemma_bytes_push::<K>(cs0, x);
            }
        }
        let tail = (self.data.len() - n0) as u8;
        let len = self.state.len - self.len_flushed;
        let mut bytes = Vec::new();
        core::mem::swap(&mut self.data, &mut bytes);
        let ghost e = self.state;
        let ghost s0 = self.slabs@;
        proof {
            let x = old(self).state.cur_count as u64;
            K::axiom_enc(x, x, Seq::empty(), Seq::empty());
            lemma_bytes_push::<K>(old(self).state.cs@, x);
            lemma_finished_slab::<K>(e, bytes@);
        }
        let slab = new_bslab_vec(bytes, len, self.state.segments, tail);
        self.slabs.push(slab);
        self.len_flushed = self.state.len;
        self.state.restart();
        proof {
            lemma_bslabs_push::<K>(s0, slab);
            assert forall|i: int| 0 <= i < self.slabs@.len() implies bslab_ok::<K>(#[trigger] self.slabs@[i]) by {
                if i < s0.len() {
                    assert(self.slabs@[i] == s0[i]);
                }
            }
            assert(self.data@ =~= counts_bytes::<K>(Seq::<u64>::empty()));
            assert(self.values::<K>() =~= old(self).values::<K>());
        }
    }

    /// Append `n` copies of `value`. Most appends extend the current run,
    /// which writes nothing and cuts no slab (`maybe_cut` cuts only where a
    /// run ends): that is taken inline, and the rest out of line.
    #[inline(always)]
    pub(crate) fn append<K: CountOps>(&mut self, value: bool, n: usize)
        requires old(self).inv::<K>(),
        ensures
            final(self).inv::<K>(), final(self).values::<K>() == old(self).values::<K>() + run_of(n as nat, value),
            final(self).state.len == old(self).state.len + n,
            final(self).max_segments == old(self).max_segments,
            old(self).max_segments is None ==> final(self).slabs@ == old(self).slabs@,
    {
        if value == self.state.cur_value && n <= usize::MAX - 3 - self.state.len {
            self.state.extend(n);
            proof {
                assert(self.values::<K>() =~= old(self).values::<K>() + run_of(n as nat, value));
            }
            return;
        }
        self.append_slow::<K>(value, n);
    }

    /// `append`, where the value starts a run (or the count would not
    /// fit, which panics).
    #[inline(never)]
    fn append_slow<K: CountOps>(&mut self, value: bool, n: usize)
        requires old(self).inv::<K>(),
        ensures
            final(self).inv::<K>(), final(self).values::<K>() == old(self).values::<K>() + run_of(n as nat, value),
            final(self).state.len == old(self).state.len + n,
            final(self).max_segments == old(self).max_segments,
            old(self).max_segments is None ==> final(self).slabs@ == old(self).slabs@,
    {
        if n == 0 {
            assert(old(self).values::<K>() + run_of(0, value) =~= old(self).values::<K>());
            return;
        }
        self.maybe_cut::<K>(value);
        proof {
            lemma_pre_len(self.state.cs@);
            lemma_vals_len(self.state.cs@, 0);
        }
        let ghost c0 = self.state.cs@;
        self.state.append::<K>(&mut self.data, value, n);
        proof {
            assert(self.state.cs@ =~= c0 + self.state.cs@.skip(c0.len() as int));
            lemma_concat::<K>(c0, self.state.cs@.skip(c0.len() as int));
            assert(self.values::<K>() =~= old(self).values::<K>() + run_of(n as nat, value));
        }
    }

    /// Flush the pending run and return all slabs (those cut and the last).
    pub(crate) fn take_slabs<K: CountOps>(&mut self) -> (v: Vec<BSlab>)
        requires old(self).inv::<K>(),
        ensures
            forall|i: int| 0 <= i < v@.len() ==> bslab_ok::<K>(#[trigger] v@[i]),
            bslabs_values::<K>(v@) == old(self).values::<K>(),
    {
        proof {
            lemma_pre_len(self.state.cs@);
            lemma_vals_len(self.state.cs@, 0);
        }
        let ghost c0 = self.state.cs@;
        let ghost cs0 = self.state.cs@;
        let n0 = self.data.len();
        self.state.finish::<K>(&mut self.data);
        proof {
            assert(self.state.cs@ =~= cs0 + self.state.cs@.skip(cs0.len() as int));
            lemma_concat::<K>(cs0, self.state.cs@.skip(cs0.len() as int));
            if old(self).state.cur_count > 0 {
                let x = old(self).state.cur_count as u64;
                K::axiom_enc(x, x, Seq::empty(), Seq::empty());
                lemma_bytes_push::<K>(cs0, x);
            }
        }
        let tail = (self.data.len() - n0) as u8;
        let len = self.state.len - self.len_flushed;
        let ghost s0 = self.slabs@;
        if len > 0 {
            let mut bytes = Vec::new();
            core::mem::swap(&mut self.data, &mut bytes);
            proof {
                let x = old(self).state.cur_count as u64;
                K::axiom_enc(x, x, Seq::empty(), Seq::empty());
                lemma_bytes_push::<K>(c0, x);
                lemma_finished_slab::<K>(self.state, bytes@);
            }
            let slab = new_bslab_vec(bytes, len, self.state.segments, tail);
            self.slabs.push(slab);
            self.len_flushed = self.state.len;
            proof {
                lemma_bslabs_push::<K>(s0, slab);
                assert forall|i: int| 0 <= i < self.slabs@.len() implies bslab_ok::<K>(#[trigger] self.slabs@[i]) by {
                    if i < s0.len() {
                        assert(self.slabs@[i] == s0[i]);
                    }
                }
            }
        } else {
            proof {
                assert(self.state.vals@.len() == 0);
                assert(old(self).values::<K>() =~= bslabs_values::<K>(s0));
            }
        }
        let mut v = Vec::new();
        core::mem::swap(&mut self.slabs, &mut v);
        v
    }


    /// Flush, and give the encoding of the values written, of an encoder
    /// that is done with.
    #[inline]
    pub(crate) fn into_bytes<K: CountOps>(self) -> (r: Vec<u8>)
        requires self.inv::<K>(), self.slabs@.len() == 0,
        ensures bool_valid::<K>(r@), bool_values::<K>(r@) == self.values::<K>(),
    {
        let mut e = self;
        proof {
            lemma_pre_len(e.state.cs@);
            lemma_vals_len(e.state.cs@, 0);
        }
        let ghost c0 = e.state.cs@;
        e.state.finish::<K>(&mut e.data);
        proof {
            assert(e.state.cs@ =~= c0 + e.state.cs@.skip(c0.len() as int));
            lemma_concat::<K>(c0, e.state.cs@.skip(c0.len() as int));
            let x = self.state.cur_count as u64;
            K::axiom_enc(x, x, Seq::empty(), Seq::empty());
            lemma_bytes_push::<K>(c0, x);
            lemma_finished_slab::<K>(e.state, e.data@);
            assert(bslabs_values::<K>(Seq::<BSlab>::empty()) =~= Seq::<bool>::empty());
            assert(self.values::<K>() =~= e.state.vals@);
        }
        e.data
    }

    /// Flush, and add the encoding to `out`: what
    /// [`into_bytes`](Self::into_bytes) gives, with no buffer between.
    #[inline]
    pub(crate) fn write_to<K: CountOps>(self, out: &mut Vec<u8>)
        requires self.inv::<K>(), self.slabs@.len() == 0,
        ensures
            final(out)@.len() >= old(out)@.len(),
            final(out)@.take(old(out)@.len() as int) == old(out)@,
            bool_valid::<K>(final(out)@.skip(old(out)@.len() as int)),
            bool_values::<K>(final(out)@.skip(old(out)@.len() as int)) == self.values::<K>(),
    {
        let mut e = self;
        proof {
            lemma_pre_len(e.state.cs@);
            lemma_vals_len(e.state.cs@, 0);
        }
        let ghost c0 = e.state.cs@;
        let ghost o0 = out@;
        e.state.finish::<K>(&mut e.data);
        proof {
            assert(e.state.cs@ =~= c0 + e.state.cs@.skip(c0.len() as int));
            lemma_concat::<K>(c0, e.state.cs@.skip(c0.len() as int));
            let x = self.state.cur_count as u64;
            K::axiom_enc(x, x, Seq::empty(), Seq::empty());
            lemma_bytes_push::<K>(c0, x);
            lemma_finished_slab::<K>(e.state, e.data@);
            assert(bslabs_values::<K>(Seq::<BSlab>::empty()) =~= Seq::<bool>::empty());
            assert(self.values::<K>() =~= e.state.vals@);
        }
        out.extend_from_slice(e.data.as_slice());
        proof {
            assert(out@.skip(o0.len() as int) =~= e.data@);
            assert(out@.take(o0.len() as int) =~= o0);
        }
    }
}

/// A [`BEnc`] that writes into its caller's buffer, and what it has made
/// of that buffer: what was there before, and then the counts written.
pub struct BStream {
    pub(crate) state: BEnc,
    #[allow(dead_code)]
    pub(crate) head: Ghost<Seq<u8>>,
}

impl BStream {
    /// Nothing is written yet, so any buffer will do.
    pub(crate) open spec fn fresh(&self) -> bool {
        self.state.cs@.len() == 0
    }

    /// What was in the buffer before this stream wrote to it.
    pub(crate) open spec fn head_of(&self, buf: Seq<u8>) -> Seq<u8> {
        if self.fresh() {
            buf
        } else {
            self.head@
        }
    }

    /// `buf` is the buffer this stream is writing into.
    pub(crate) open spec fn on<K: CountOps>(&self, buf: Seq<u8>) -> bool {
        &&& self.state.inv() && self.state.clean@ && self.state.before@ == 0
        &&& self.state.cur_count > 0 || self.state.segments == 0
        &&& self.fresh() || buf == self.head@ + counts_bytes::<K>(self.state.cs@)
    }

    pub(crate) fn new<K: CountOps>() -> (s: Self)
        ensures
            s.fresh(),
            s.state.vals@.len() == 0,
            forall|buf: Seq<u8>| s.on::<K>(buf),
    {
        BStream { state: BEnc::new(), head: Ghost(Seq::empty()) }
    }

    #[inline(always)]
    pub(crate) fn append<K: CountOps>(&mut self, buf: &mut Vec<u8>, value: bool, n: usize)
        requires
            old(self).on::<K>(old(buf)@),
        ensures
            final(self).on::<K>(final(buf)@),
            final(self).head_of(final(buf)@) == old(self).head_of(old(buf)@),
            final(self).state.vals@ == old(self).state.vals@ + run_of(n as nat, value),
    {
        let ghost c0 = self.state.cs@;
        let ghost b0 = buf@;
        let ghost h = self.head_of(buf@);
        self.state.append::<K>(buf, value, n);
        proof {
            let c1 = self.state.cs@;
            let more = c1.skip(c0.len() as int);
            assert(c1 =~= c0 + more);
            lemma_concat::<K>(c0, more);
            assert(counts_bytes::<K>(Seq::<u64>::empty()) =~= Seq::<u8>::empty());
            if c0.len() == 0 {
                assert(c0 =~= Seq::<u64>::empty());
                assert(b0 + Seq::<u8>::empty() =~= b0);
            }
            assert(b0 =~= h + counts_bytes::<K>(c0));
            assert(buf@ =~= h + counts_bytes::<K>(c1));
            if c1.len() == 0 {
                assert(c1 =~= Seq::<u64>::empty());
                assert(h + Seq::<u8>::empty() =~= h);
            }
            self.head = Ghost(h);
        }
    }

    /// Whether every value given is `value`.
    #[inline(always)]
    pub(crate) fn all_are(&self, value: bool) -> (r: bool)
        requires
            self.state.inv() && self.state.clean@ && self.state.before@ == 0,
            self.state.cur_count > 0 || self.state.segments == 0,
        ensures
            r == (forall|i: int| 0 <= i < self.state.vals@.len() ==> #[trigger] self.state.vals@[i] == value),
    {
        proof {
            if self.state.cur_count == 0 {
                assert(self.state.cs@ =~= Seq::<u64>::empty());
                lemma_vals_len(self.state.cs@, 0);
            }
        }
        self.state.all_equal_pre_finish(value)
    }

    /// Write what is still held: the buffer is then what it was before
    /// this stream and after that the column of the values given.
    #[inline(always)]
    pub(crate) fn finish<K: CountOps>(self, buf: &mut Vec<u8>)
        requires
            self.on::<K>(old(buf)@),
        ensures
            ({
                let h = self.head_of(old(buf)@);
                let d = final(buf)@.skip(h.len() as int);
                &&& final(buf)@ == h + d
                &&& bool_valid::<K>(d)
                &&& bool_values::<K>(d) == self.state.vals@
            }),
    {
        let mut e = self;
        let ghost c0 = e.state.cs@;
        let ghost b0 = buf@;
        let ghost h = self.head_of(buf@);
        proof {
            lemma_pre_len(c0);
            lemma_vals_len(c0, 0);
        }
        e.state.finish::<K>(buf);
        proof {
            let c1 = e.state.cs@;
            let more = c1.skip(c0.len() as int);
            assert(c1 =~= c0 + more);
            lemma_concat::<K>(c0, more);
            assert(counts_bytes::<K>(Seq::<u64>::empty()) =~= Seq::<u8>::empty());
            if c0.len() == 0 {
                assert(c0 =~= Seq::<u64>::empty());
                assert(b0 + Seq::<u8>::empty() =~= b0);
            }
            assert(b0 =~= h + counts_bytes::<K>(c0));
            let d = counts_bytes::<K>(c1);
            assert(buf@ =~= h + d);
            lemma_finished_slab::<K>(e.state, d);
            assert(buf@.skip(h.len() as int) =~= d);
        }
    }
}

} // verus!
