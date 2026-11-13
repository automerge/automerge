//! Decoding a validated slab's values, verified with Verus.
//!
//! [`Dec`] walks the canonical encoding of a slab (`valid_slab`) and
//! yields its values (`slab_values`) one at a time, a run at a time, or
//! after skipping ahead; `DecState` is a position detached from the
//! slab's bytes.  On a valid slab it never panics, and every value it
//! yields is the model's value at its position.

// Verus proofs read more plainly with `x = x + n`.
#![allow(
    clippy::len_zero,
    clippy::assign_op_pattern,
    clippy::too_many_arguments
)]

// proof functions exist only when Verus compiles the crate
#[cfg(verus_keep_ghost)]
use super::lift::*;
use super::model::*;
use super::postfix::*;
// proof functions exist only when Verus compiles the crate
#[cfg(verus_keep_ghost)]
use super::cut::slab_ok;
#[cfg(verus_keep_ghost)]
use super::read::{lemma_count, lemma_header, lemma_starts, lemma_value};
#[cfg(verus_keep_ghost)]
use super::read::{lit_tail_of, tail_bytes_of};
use super::state::*;
#[cfg(verus_keep_ghost)]
use super::writer::slab_bytes;
use crate::iter_core::{Decode, Decoders};
use crate::rle::{RleTail, Slab};
use std::marker::PhantomData;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// `data` is the canonical encoding of some values, with metadata that
/// fits — what load accepts and what the writer produces.
pub(crate) open spec fn valid_slab<'a, O: RleOps<'a>>(data: Seq<u8>) -> bool {
    exists|gs: Seq<Group<O::V>>| parses::<O>(gs, data)
}

/// The groups of a valid slab.
pub(crate) open spec fn slab_groups<'a, O: RleOps<'a>>(data: Seq<u8>) -> Seq<Group<O::V>> {
    choose|gs: Seq<Group<O::V>>| parses::<O>(gs, data)
}

/// The values of a valid slab.
pub(crate) open spec fn slab_values<'a, O: RleOps<'a>>(data: Seq<u8>) -> Seq<O::V> {
    groups_values(slab_groups::<O>(data), O::null())
}

/// The byte offset of group `i`.
pub(crate) open spec fn gb<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, i: int) -> int {
    groups_bytes::<O>(gs.take(i)).len() as int
}

/// The value offset of group `i`.
pub(crate) open spec fn gl<V>(gs: Seq<Group<V>>, i: int) -> int {
    groups_len(gs.take(i)) as int
}

pub(crate) proof fn lemma_group_values_len<V>(g: Group<V>, null: V)
    ensures group_values(g, null).len() == group_len(g),
{
}

pub(crate) proof fn lemma_groups_values_concat<V>(a: Seq<Group<V>>, b: Seq<Group<V>>, null: V)
    ensures
        groups_values(a + b, null) == groups_values(a, null) + groups_values(b, null),
        groups_values(a, null).len() == groups_len(a),
    decreases a.len() + b.len(),
{
    if b.len() == 0 {
        assert(a + b =~= a);
        if a.len() > 0 {
            lemma_groups_values_concat(a.drop_last(), Seq::empty(), null);
        }
    } else {
        assert((a + b).drop_last() =~= a + b.drop_last());
        lemma_groups_values_concat(a, b.drop_last(), null);
    }
}

/// Across a group boundary the value changes: canonical groups never put
/// two equal values side by side there.
pub(crate) proof fn lemma_boundary<'a, O: RleOps<'a>>(data: Seq<u8>, i: int)
    requires
        valid_slab::<O>(data),
        0 < i < slab_groups::<O>(data).len(),
    ensures ({
        let vs = slab_values::<O>(data);
        let gs = slab_groups::<O>(data);
        &&& 0 < gl(gs, i) < vs.len()
        &&& vs[gl(gs, i) - 1] != vs[gl(gs, i)]
    }),
{
    let gs = slab_groups::<O>(data);
    let vs = slab_values::<O>(data);
    lemma_slab_group::<O>(data, i);
    lemma_slab_group::<O>(data, i - 1);
    lemma_group_at::<O>(gs, i);
    lemma_group_at::<O>(gs, i - 1);
    let a = gs[i - 1];
    let b = gs[i];
    lemma_group_values_len(a, O::null());
    lemma_group_values_len(b, O::null());
    let la = group_values(a, O::null());
    let lb = group_values(b, O::null());
    assert(vs[gl(gs, i)] == lb[0]);
    assert(vs[gl(gs, i) - 1] == la[la.len() - 1]);
    match a {
        Group::Run { count: _, value: x } => {
            match b {
                Group::Null { count: _ } => {
                    O::axiom_null(x);
                },
                _ => {},
            }
        },
        Group::Lit { values } => {
            match b {
                Group::Null { count: _ } => {
                    O::axiom_null(values.last());
                },
                _ => {},
            }
        },
        Group::Null { count: _ } => {
            match b {
                Group::Run { count: _, value: y } => {
                    O::axiom_null(y);
                },
                Group::Lit { values } => {
                    O::axiom_null(values[0]);
                },
                _ => {},
            }
        },
    }
}

/// Where group `i`'s values and bytes sit among all of them.
pub(crate) proof fn lemma_group_at<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, i: int)
    requires 0 <= i < gs.len(),
    ensures
        groups_values(gs, O::null()) == groups_values(gs.take(i), O::null()) + group_values(gs[i], O::null())
            + groups_values(gs.skip(i + 1), O::null()),
        groups_values(gs.take(i), O::null()).len() == gl(gs, i),
        gl(gs, i + 1) == gl(gs, i) + group_len(gs[i]),
        gb::<O>(gs, i + 1) == gb::<O>(gs, i) + group_bytes::<O>(gs[i]).len(),
        groups_bytes::<O>(gs) == groups_bytes::<O>(gs.take(i)) + group_bytes::<O>(gs[i])
            + groups_bytes::<O>(gs.skip(i + 1)),
        groups_values(gs, O::null()).len() == groups_len(gs),
{
    let one = seq![gs[i]];
    assert(gs =~= gs.take(i) + one + gs.skip(i + 1));
    assert(gs.take(i + 1) =~= gs.take(i) + one);
    lemma_groups_values_concat(gs.take(i) + one, gs.skip(i + 1), O::null());
    lemma_groups_values_concat(gs.take(i), one, O::null());
    lemma_groups_values_concat(gs, Seq::empty(), O::null());
    lemma_groups_values_concat(one, Seq::empty(), O::null());
    assert(one.drop_last() =~= Seq::<Group<O::V>>::empty());
    reveal_with_fuel(groups_values, 2);
    assert(groups_values(one, O::null()) =~= group_values(gs[i], O::null()));
    lemma_groups_len_concat(gs.take(i), one);
    reveal_with_fuel(groups_len, 2);
    lemma_groups_bytes_concat::<O>(gs.take(i), one);
    reveal_with_fuel(groups_bytes, 2);
    assert(groups_bytes::<O>(one) =~= group_bytes::<O>(gs[i]));
    lemma_groups_bytes_at::<O>(gs, i);
}

/// The slab's groups parse its bytes, and are canonical.
pub(crate) proof fn lemma_slab<'a, O: RleOps<'a>>(data: Seq<u8>)
    requires valid_slab::<O>(data),
    ensures ({
        let gs = slab_groups::<O>(data);
        &&& canonical::<O>(gs)
        &&& groups_bytes::<O>(gs) == data
        &&& groups_len(gs) <= usize::MAX
        &&& slab_values::<O>(data).len() == groups_len(gs)
    }),
{
    let gs = slab_groups::<O>(data);
    reveal(parses);
    lemma_groups_values_concat(gs, Seq::empty(), O::null());
}


pub(crate) proof fn lemma_skip_skip<T>(s: Seq<T>, a: int, b: int)
    requires 0 <= a, 0 <= b, a + b <= s.len(),
    ensures s.skip(a).skip(b) == s.skip(a + b),
{
    assert(s.skip(a).skip(b) =~= s.skip(a + b));
}

/// Group `i` of a valid slab: where its bytes and values sit.
pub(crate) proof fn lemma_slab_group<'a, O: RleOps<'a>>(data: Seq<u8>, i: int)
    requires valid_slab::<O>(data), 0 <= i < slab_groups::<O>(data).len(),
    ensures ({
        let gs = slab_groups::<O>(data);
        let g = gs[i];
        &&& group_ok::<O>(g)
        &&& i > 0 ==> adjacent_ok(gs[i - 1], g)
        &&& 0 <= gb::<O>(gs, i) < data.len()
        &&& gb::<O>(gs, i + 1) == gb::<O>(gs, i) + group_bytes::<O>(g).len()
        &&& gb::<O>(gs, i + 1) <= data.len()
        &&& group_bytes::<O>(g).len() > 0
        &&& gl(gs, i + 1) == gl(gs, i) + group_len(g)
        &&& group_bytes::<O>(g) == O::enc_signed(header_of(g) as i64) + payload::<O>(g)
        &&& data.skip(gb::<O>(gs, i)) == O::enc_signed(header_of(g) as i64)
            + (payload::<O>(g) + groups_bytes::<O>(gs.skip(i + 1)))
        &&& forall|j: int| 0 <= j < group_len(g)
            ==> slab_values::<O>(data)[gl(gs, i) + j] == #[trigger] group_values(g, O::null())[j]
        &&& gl(gs, i) + group_len(g) <= slab_values::<O>(data).len()
        &&& group_len(g) <= usize::MAX
    }),
{
    let gs = slab_groups::<O>(data);
    let g = gs[i];
    lemma_slab::<O>(data);
    lemma_group_at::<O>(gs, i);
    assert(group_ok::<O>(g));
    if i > 0 {
        assert(adjacent_ok(gs[i - 1], g));
    }
    O::axiom_enc_signed(header_of(g) as i64, 0, Seq::empty(), Seq::empty());
    let rest = groups_bytes::<O>(gs.skip(i + 1));
    assert(data =~= groups_bytes::<O>(gs.take(i)) + (O::enc_signed(header_of(g) as i64) + (payload::<O>(g) + rest)));
    assert(data.skip(gb::<O>(gs, i)) =~= O::enc_signed(header_of(g) as i64) + (payload::<O>(g) + rest));
    assert(data.len() == gb::<O>(gs, i) + group_bytes::<O>(g).len() + rest.len());
    lemma_group_values_len(g, O::null());
    assert(gs =~= gs.take(i + 1) + gs.skip(i + 1));
    lemma_groups_len_concat(gs.take(i + 1), gs.skip(i + 1));
}

/// Literal value `j` of group `i` of a valid slab: where its bytes sit.
pub(crate) proof fn lemma_slab_lit<'a, O: RleOps<'a>>(data: Seq<u8>, i: int, j: int)
    requires
        valid_slab::<O>(data), 0 <= i < slab_groups::<O>(data).len(),
        slab_groups::<O>(data)[i] is Lit,
        0 <= j < slab_groups::<O>(data)[i]->values.len(),
    ensures ({
        let gs = slab_groups::<O>(data);
        let vs = gs[i]->values;
        let b = gb::<O>(gs, i) + lit_head::<O>(vs.len()).len() + encs::<O>(vs.take(j)).len();
        let after = encs::<O>(vs.skip(j + 1)) + groups_bytes::<O>(gs.skip(i + 1));
        &&& !O::is_null(vs[j])
        &&& 0 <= b <= data.len()
        &&& data.skip(b) == O::enc(vs[j]) + after
        &&& encs::<O>(vs.take(j + 1)).len() == encs::<O>(vs.take(j)).len() + O::enc(vs[j]).len()
    }),
{
    let gs = slab_groups::<O>(data);
    let vs = gs[i]->values;
    lemma_slab_group::<O>(data, i);
    lemma_encs_at::<O>(vs, j);
    lemma_encs_concat::<O>(vs.take(j), seq![vs[j]]);
    reveal_with_fuel(encs, 2);
    assert(encs::<O>(seq![vs[j]]) =~= O::enc(vs[j]));
    assert(vs.take(j) + seq![vs[j]] =~= vs.take(j + 1));
    let hd = lit_head::<O>(vs.len());
    let after = encs::<O>(vs.skip(j + 1)) + groups_bytes::<O>(gs.skip(i + 1));
    let s0 = data.skip(gb::<O>(gs, i));
    assert(s0 =~= (hd + encs::<O>(vs.take(j))) + (O::enc(vs[j]) + after));
    lemma_skip_skip(data, gb::<O>(gs, i), (hd.len() + encs::<O>(vs.take(j)).len()) as int);
    assert(s0.skip((hd.len() + encs::<O>(vs.take(j)).len()) as int) =~= O::enc(vs[j]) + after);
}

/// Group `gi` of `data` starts at byte `b`, and its header is `hb` bytes
/// long and has been read.
pub(crate) open spec fn at_header<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int, b: int, hb: int) -> bool {
    let gs = slab_groups::<O>(data);
    &&& 0 <= gi < gs.len()
    &&& b == gb::<O>(gs, gi)
    &&& hb == O::enc_signed(header_of(gs[gi]) as i64).len()
    &&& hb > 0
    &&& b + hb <= data.len()
    &&& data.skip(b + hb) == payload::<O>(gs[gi]) + groups_bytes::<O>(gs.skip(gi + 1))
}

/// The run a decoder is inside.
pub(crate) enum RunState<V> {
    /// A repeat run of this value.
    Repeat(V),
    /// A literal run, its values read one by one from `byte_pos`.
    Literal,
    Null,
    /// Between runs or at the end.
    Idle,
}

/// A forward reader over a valid slab's values.
pub(crate) struct Dec<'a, O: RleOps<'a>> {
    data: &'a [u8],
    pub(crate) byte_pos: usize,
    /// values left in the current run
    pub(crate) remaining: usize,
    state: RunState<O::V>,
    /// Where the current repeat run's value starts, which a suspend
    /// keeps so a resume can read it again.
    ///
    /// A separate field rather than a payload on [`RunState::Repeat`]:
    /// widening that variant costs the decode loop 9-15% on value-heavy
    /// columns (strings, long runs), because every `next` then loads it
    /// alongside the value it actually wants.
    run_at: usize,
    /// the groups begun
    gi: Ghost<int>,
}

impl<'a, O: RleOps<'a>> Dec<'a, O> {
    pub(crate) closed spec fn data(&self) -> Seq<u8> {
        self.data@
    }

    /// A copy.
    // `RunState` is not `Copy`: the match copies its value out
    #[allow(clippy::needless_match)]
    #[inline(always)]
    pub(crate) fn dup(&self) -> (r: Self)
        ensures
            r == *self,
    {
        let state = match self.state {
            RunState::Repeat(v) => RunState::Repeat(v),
            RunState::Literal => RunState::Literal,
            RunState::Null => RunState::Null,
            RunState::Idle => RunState::Idle,
        };
        Dec {
            data: self.data,
            byte_pos: self.byte_pos,
            remaining: self.remaining,
            state,
            run_at: self.run_at,
            gi: self.gi,
        }
    }

    /// The slab's values.
    pub(crate) open spec fn values(&self) -> Seq<O::V> {
        slab_values::<O>(self.data())
    }

    /// The index of the next value.
    pub(crate) closed spec fn pos(&self) -> int {
        gl(slab_groups::<O>(self.data@), self.gi@) - self.remaining
    }

    pub(crate) closed spec fn inv(&self) -> bool {
        let gs = slab_groups::<O>(self.data@);
        let gi = self.gi@;
        &&& valid_slab::<O>(self.data@)
        &&& self.data@.len() <= usize::MAX
        &&& 0 <= gi <= gs.len()
        &&& match self.state {
            RunState::Idle => self.remaining == 0 && self.byte_pos == gb::<O>(gs, gi),
            RunState::Repeat(v) => {
                &&& gi >= 1 && gs[gi - 1] is Run
                &&& run_value(gs[gi - 1]) == v
                &&& self.remaining <= gs[gi - 1]->Run_count
                &&& self.byte_pos == gb::<O>(gs, gi)
                &&& self.run_at == gb::<O>(gs, gi - 1) + O::enc_signed(gs[gi - 1]->Run_count as i64).len()
            },
            RunState::Literal => {
                let vs = gs[gi - 1]->values;
                &&& gi >= 1 && gs[gi - 1] is Lit
                &&& self.remaining <= vs.len()
                &&& self.byte_pos == gb::<O>(gs, gi - 1) + lit_head::<O>(vs.len()).len()
                    + encs::<O>(vs.take(vs.len() - self.remaining)).len()
            },
            RunState::Null => {
                &&& gi >= 1 && gs[gi - 1] is Null
                &&& self.remaining <= gs[gi - 1]->Null_count
                &&& self.byte_pos == gb::<O>(gs, gi)
            },
        }
    }

    /// The segments behind this reader: groups passed, the literal values
    /// read, and a repeat or null run once all of it is read.
    pub(crate) closed spec fn dsegs(&self) -> int {
        let gs = slab_groups::<O>(self.data@);
        let gi = self.gi@;
        match self.state {
            RunState::Idle => gseg(gs, gi),
            RunState::Literal => gseg(gs, gi - 1) + (gs[gi - 1]->values.len() - self.remaining),
            _ => gseg(gs, gi - 1) + (if self.remaining == 0 { 1int } else { 0int }),
        }
    }

    /// Reading a literal.
    pub(crate) closed spec fn lit_now(&self) -> bool {
        self.state is Literal
    }

    /// Values left in the current run.
    pub(crate) closed spec fn left_now(&self) -> int {
        self.remaining as int
    }

    /// A reader at the end of a group is behind all of it.
    proof fn lemma_dsegs_end(&self)
        requires self.inv(), self.remaining == 0,
        ensures self.dsegs() == gseg(slab_groups::<O>(self.data@), self.gi@),
    {
        let gs = slab_groups::<O>(self.data@);
        let gi = self.gi@;
        if !(self.state is Idle) {
            lemma_gseg(gs, gi - 1);
        }
    }

    pub(crate) fn new(data: &'a [u8]) -> (r: Self)
        requires valid_slab::<O>(data@),
        ensures r.inv(), r.data() == data@, r.pos() == 0,
    {
        proof {
            let gs = slab_groups::<O>(data@);
            assert(gs.take(0) =~= Seq::<Group<O::V>>::empty());
        }
        let _len = data.len();
        Dec { data, byte_pos: 0, remaining: 0, state: RunState::Idle, run_at: 0, gi: Ghost(0) }
    }

    /// The facts a position rests on.
    proof fn lemma_pos(&self)
        requires self.inv(),
        ensures
            0 <= self.pos() <= self.values().len(),
            self.pos() + self.remaining <= self.values().len(),
            self.remaining == 0 ==> self.pos() == gl(slab_groups::<O>(self.data@), self.gi@),
    {
        let gs = slab_groups::<O>(self.data@);
        let gi = self.gi@;
        lemma_slab::<O>(self.data@);
        if gi < gs.len() {
            lemma_group_at::<O>(gs, gi);
            assert(gs.skip(gi) =~= seq![gs[gi]] + gs.skip(gi + 1));
        }
        assert(gs =~= gs.take(gi) + gs.skip(gi));
        lemma_groups_len_concat(gs.take(gi), gs.skip(gi));
        if gi >= 1 {
            lemma_group_at::<O>(gs, gi - 1);
        }
    }

    /// What `skip_group` promises.
    pub(crate) closed spec fn skip_post(r0: Self, r1: Self, n: usize, r: Result<O::V, usize>) -> bool {
        &&& r1.inv() && r1.data() == r0.data()
        &&& match r {
            Ok(v) => r0.pos() + n < r0.values().len() && v == r0.values()[r0.pos() + n]
                && r1.pos() == r0.pos() + n + 1,
            Err(m) => {
                &&& m <= n
                &&& r1.state is Idle && r1.remaining == 0
                &&& r1.byte_pos > r0.byte_pos
                &&& r1.byte_pos <= r1.data().len()
                &&& r1.pos() + m == r0.pos() + n
            },
        }
    }

    /// At a run boundary: where it is.
    proof fn lemma_boundary(&self)
        requires self.inv(), self.remaining == 0,
        ensures ({
            let gs = slab_groups::<O>(self.data@);
            let gi = self.gi@;
            &&& self.byte_pos == gb::<O>(gs, gi)
            &&& self.byte_pos <= self.data@.len()
            &&& self.pos() == gl(gs, gi)
            &&& (self.byte_pos >= self.data@.len()) == (gi == gs.len())
            &&& gi == gs.len() ==> self.pos() == self.values().len()
        }),
    {
        let data = self.data@;
        let gs = slab_groups::<O>(data);
        let gi = self.gi@;
        lemma_slab::<O>(data);
        if gi >= 1 {
            lemma_slab_group::<O>(data, gi - 1);
            if self.state is Literal {
                let vs = gs[gi - 1]->values;
                assert(vs.take(vs.len() as int) =~= vs);
            }
        }
        if gi < gs.len() {
            lemma_slab_group::<O>(data, gi);
        } else {
            assert(gs.take(gi) =~= gs);
        }
    }

    /// `advance_run`, out of line: for `next_run_max`, which calls it
    /// once per run. Inlined there (as LLVM does when nothing else calls
    /// it), the run loop runs a fifth slower on aarch64; `next`, a value
    /// at a time, keeps `advance_run` for LLVM to place.
    #[inline(never)]
    fn advance_run_outlined(&mut self)
        requires old(self).inv(), old(self).remaining == 0,
        ensures
            final(self).inv(), final(self).data() == old(self).data(),
            final(self).pos() == old(self).pos(),
            final(self).remaining == 0 ==> final(self).pos() == final(self).values().len(),
            final(self).dsegs() == old(self).dsegs(),
    {
        self.advance_run()
    }

    /// At a run boundary: begin the next run, if there is one.
    fn advance_run(&mut self)
        requires old(self).inv(), old(self).remaining == 0,
        ensures
            final(self).inv(), final(self).data() == old(self).data(),
            final(self).pos() == old(self).pos(),
            final(self).remaining == 0 ==> final(self).pos() == final(self).values().len(),
            final(self).dsegs() == old(self).dsegs(),
    {
        proof {
            old(self).lemma_dsegs_end();
        }
        let ghost data = self.data@;
        let ghost gs = slab_groups::<O>(data);
        let ghost gi = self.gi@;
        proof {
            self.lemma_boundary();
        }
        if self.byte_pos >= self.data.len() {
            self.state = RunState::Idle;
            self.remaining = 0;
            return;
        }
        let ghost g = gs[gi];
        let ghost rest = groups_bytes::<O>(gs.skip(gi + 1));
        let ghost h = header_of(g) as i64;
        let ghost b0 = self.byte_pos as int;
        proof {
            lemma_slab_group::<O>(data, gi);
            lemma_starts(O::enc_signed(h), payload::<O>(g) + rest);
        }
        let (count_bytes, count_raw) = match O::read_signed_opt(&self.data[self.byte_pos..]) {
            Some(x) => x,
            None => {
                // unreachable on a valid slab
                self.state = RunState::Idle;
                self.remaining = 0;
                return;
            },
        };
        let ghost p = b0 + count_bytes;
        proof {
            lemma_header::<O>(data.skip(b0), h, payload::<O>(g) + rest, count_bytes as int, count_raw);
            lemma_skip_skip(data, b0, count_bytes as int);
        }
        if count_raw > 0 {
            let count = count_raw as usize;
            let value_start = self.byte_pos + count_bytes;
            proof {
                lemma_starts(O::enc(run_value(g)), rest);
            }
            let (vlen, value) = O::unpack(&self.data[value_start..]);
            proof {
                lemma_value::<O>(data.skip(p), run_value(g), rest, vlen as int, value);
            }
            self.byte_pos = value_start + vlen;
            self.remaining = count;
            self.run_at = value_start;
            self.state = RunState::Repeat(value);
            self.gi = Ghost(gi + 1);
            proof {
                assert(group_len(g) == count);
                assert(self.byte_pos == gb::<O>(gs, gi + 1));
                assert(self.inv());
            }
        } else if count_raw < 0 {
            let total = (-count_raw) as usize;
            self.byte_pos = self.byte_pos + count_bytes;
            self.remaining = total;
            self.state = RunState::Literal;
            self.gi = Ghost(gi + 1);
            proof {
                let vs = g->values;
                assert(vs.take(vs.len() - self.remaining) =~= Seq::<O::V>::empty());
                assert(group_len(g) == total);
                assert(self.inv());
            }
        } else {
            proof {
                lemma_starts(O::enc_unsigned(g->Null_count as u64), rest);
            }
            let (ncb, null_count) = O::read_unsigned_opt(&self.data[self.byte_pos + count_bytes..]).unwrap();
            proof {
                lemma_count::<O>(data.skip(p), g->Null_count as u64, rest, ncb as int, null_count);
            }
            self.byte_pos = self.byte_pos + count_bytes + ncb;
            self.remaining = null_count as usize;
            self.state = RunState::Null;
            self.gi = Ghost(gi + 1);
            proof {
                assert(group_len(g) == self.remaining);
                assert(self.inv());
            }
        }
    }

    /// Inside a run: its next value, as `next` returns it.
    #[inline(always)]
    fn take_next(&mut self) -> (r: Option<O::V>)
        requires old(self).inv(), old(self).remaining > 0,
        ensures
            final(self).inv(), final(self).data() == old(self).data(),
            old(self).pos() < old(self).values().len(),
            r == Some(old(self).values()[old(self).pos()]),
            final(self).pos() == old(self).pos() + 1,
    {
        let ghost r0 = *self;
        proof {
            self.lemma_at_value();
        }
        self.remaining = self.remaining - 1;
        match &self.state {
            RunState::Repeat(v) => Some(*v),
            RunState::Literal => Some(self.read_lit(Ghost(r0))),
            RunState::Null => Some(O::get_null()),
            RunState::Idle => {
                proof {
                    assert(false);
                }
                None
            },
        }
    }

    /// Inside a literal run, its count already taken down: read the value
    /// at `byte_pos`.
    #[inline(always)]
    fn read_lit(&mut self, Ghost(r0): Ghost<Self>) -> (r: O::V)
        requires
            r0.inv(), r0.remaining > 0, r0.state is Literal,
            old(self).data == r0.data, old(self).byte_pos == r0.byte_pos,
            old(self).remaining == r0.remaining - 1, old(self).state == r0.state,
            old(self).run_at == r0.run_at, old(self).gi == r0.gi,
        ensures
            final(self).inv(), final(self).data() == r0.data(),
            r0.pos() < r0.values().len(),
            r == r0.values()[r0.pos()],
            final(self).pos() == r0.pos() + 1,
            final(self).gi == old(self).gi, final(self).state == old(self).state,
            final(self).remaining == old(self).remaining,
    {
        let ghost data = self.data@;
        let ghost gs = slab_groups::<O>(data);
        let ghost gi = self.gi@;
        let ghost g = gs[gi - 1];
        proof {
            r0.lemma_at_value();
        }
        let ghost vs = g->values;
        let ghost i = vs.len() - (self.remaining + 1);
        let ghost b = self.byte_pos as int;
        let ghost after = encs::<O>(vs.skip(i + 1)) + groups_bytes::<O>(gs.skip(gi));
        proof {
            lemma_slab_lit::<O>(data, gi - 1, i);
            lemma_starts(O::enc(vs[i]), after);
        }
        let rest = &self.data[self.byte_pos..];
        assert(rest@.len() == data.len() - b);
        let (vlen, value) = O::unpack(rest);
        proof {
            lemma_value::<O>(data.skip(b), vs[i], after, vlen as int, value);
            assert(b + vlen <= data.len());
            assert(vs.take(vs.len() - self.remaining) =~= vs.take(i + 1));
        }
        self.byte_pos = self.byte_pos + vlen;
        value
    }

    /// Inside a run: the value at the position.
    proof fn lemma_at_value(&self)
        requires self.inv(), self.remaining > 0,
        ensures ({
            let gs = slab_groups::<O>(self.data@);
            let g = gs[self.gi@ - 1];
            &&& self.gi@ >= 1
            &&& self.pos() < self.values().len()
            &&& self.values()[self.pos()] == group_values(g, O::null())[group_len(g) - self.remaining]
            &&& group_ok::<O>(g)
            &&& group_values(g, O::null())[group_len(g) - self.remaining] == match self.state {
                RunState::Repeat(v) => v,
                RunState::Null => O::null(),
                _ => group_values(g, O::null())[group_len(g) - self.remaining],
            }
        }),
    {
        let data = self.data@;
        let gs = slab_groups::<O>(data);
        let gi = self.gi@;
        lemma_slab_group::<O>(data, gi - 1);
        let k = group_len(gs[gi - 1]) - self.remaining;
        assert(group_values(gs[gi - 1], O::null())[k] == self.values()[gl(gs, gi - 1) + k]);
    }

    /// Inside a literal run: its next value.
    #[inline(always)]
    fn take_lit(&mut self) -> (r: O::V)
        requires old(self).inv(), old(self).remaining > 0, old(self).state is Literal,
        ensures
            final(self).inv(), final(self).data() == old(self).data(),
            old(self).pos() < old(self).values().len(),
            r == old(self).values()[old(self).pos()],
            final(self).pos() == old(self).pos() + 1,
            final(self).gi == old(self).gi, final(self).state is Literal,
            final(self).remaining == old(self).remaining - 1,
    {
        let ghost r0 = *self;
        self.remaining = self.remaining - 1;
        self.read_lit(Ghost(r0))
    }

    /// The next value, if any.
    pub(crate) fn next(&mut self) -> (r: Option<O::V>)
        requires old(self).inv(),
        ensures
            final(self).inv(), final(self).data() == old(self).data(),
            old(self).pos() < old(self).values().len() ==> r == Some(old(self).values()[old(self).pos()])
                && final(self).pos() == old(self).pos() + 1,
            old(self).pos() >= old(self).values().len() ==> r is None && final(self).pos() == old(self).pos(),
    {
        proof {
            self.lemma_pos();
        }
        loop
            invariant
                self.inv(), self.data() == old(self).data(), self.pos() == old(self).pos(),
                old(self).values() == self.values(),
                0 <= self.pos() <= self.values().len(),
            decreases if self.remaining == 0 { 1int } else { 0int },
        {
            if self.remaining > 0 {
                return self.take_next();
            }
            self.advance_run();
            if self.remaining == 0 {
                return None;
            }
        }
    }

    /// The next run of equal values, at most `max` of them, as
    /// `(count, value)`; a literal yields its values one at a time.
    pub(crate) fn next_run_max(&mut self, max: usize) -> (r: Option<crate::Run<O::V>>)
        requires old(self).inv(),
        ensures
            final(self).inv(), final(self).data() == old(self).data(),
            match r {
                Some(run) => {
                    let count = run.count;
                    let value = run.value;
                    &&& 1 <= count <= max
                    &&& final(self).pos() == old(self).pos() + count <= old(self).values().len()
                    &&& forall|k: int| old(self).pos() <= k < final(self).pos() ==> old(self).values()[k] == value
                    &&& count < max && final(self).pos() < old(self).values().len()
                        ==> old(self).values()[final(self).pos()] != value
                    &&& final(self).dsegs() == old(self).dsegs()
                        + (if final(self).lit_now() || final(self).left_now() == 0 { 1int } else { 0int })
                },
                None => final(self).pos() == old(self).pos() && final(self).dsegs() == old(self).dsegs()
                    && (max > 0 ==> old(self).pos() == old(self).values().len()),
            },
    {
        proof {
            self.lemma_pos();
        }
        if max == 0 {
            return None;
        }
        loop
            invariant
                self.inv(), self.data() == old(self).data(), self.pos() == old(self).pos(),
                old(self).values() == self.values(), max > 0, self.dsegs() == old(self).dsegs(),
                0 <= self.pos() <= self.values().len(),
            decreases if self.remaining == 0 { 1int } else { 0int },
        {
            if self.remaining > 0 {
                return self.take_run(max);
            }
            self.advance_run_outlined();
            if self.remaining == 0 {
                return None;
            }
        }
    }

    /// Inside a run: up to `max` of its values, as `next_run_max`
    /// returns them.
    #[inline(always)]
    #[verifier::spinoff_prover]
    /// A repeat or null run taken to its end, short of `max`: the value
    /// after it differs.
    proof fn lemma_run_ends(&self, p0: int, count: usize, max: usize, value: O::V)
        requires
            self.inv(),
            self.pos() == p0 + count,
            p0 + count <= self.values().len(),
            count >= 1,
            forall|k: int| p0 <= k < p0 + count ==> self.values()[k] == value,
            count < max ==> self.remaining == 0,
            self.state is Repeat || self.state is Null,
        ensures
            count < max && self.pos() < self.values().len() ==> self.values()[self.pos()] != value,
    {
        let gs = slab_groups::<O>(self.data@);
        let gi = self.gi@;
        if count < max && self.pos() < self.values().len() {
            lemma_slab::<O>(self.data@);
            assert(gl(gs, gs.len() as int) == groups_len(gs)) by {
                assert(gs.take(gs.len() as int) =~= gs);
            }
            lemma_groups_values_concat(gs, Seq::empty(), O::null());
            assert(gi < gs.len()) by {
                if gi == gs.len() {
                    assert(self.pos() == gl(gs, gs.len() as int));
                }
            }
            lemma_boundary::<O>(self.data@, gi);
            assert(self.values()[self.pos() - 1] == value);
        }
    }

    /// A literal value taken: the value after it differs.
    #[verifier::spinoff_prover]
    proof fn lemma_lit_ends(&self, p0: int, r0: usize, max: usize, value: O::V)
        requires
            self.inv(),
            self.state is Literal,
            self.pos() == p0 + 1,
            self.remaining == r0 - 1,
            r0 >= 1,
            r0 <= group_len(slab_groups::<O>(self.data@)[self.gi@ - 1]),
            p0 < self.values().len(),
            value == self.values()[p0],
        ensures
            1 < max && self.pos() < self.values().len() ==> self.values()[self.pos()] != value,
    {
        let data = self.data@;
        let gs = slab_groups::<O>(data);
        let gi = self.gi@;
        if 1 < max && self.pos() < self.values().len() {
            lemma_slab_group::<O>(data, gi - 1);
            if self.remaining > 0 {
                // both values are the literal's, and a literal's neighbours differ
                lemma_group_at::<O>(gs, gi - 1);
                let g = gs[gi - 1];
                let lv = g->values;
                let j = p0 - gl(gs, gi - 1);
                assert(0 <= j && j + 1 < lv.len());
                assert(self.values()[p0] == lv[j]);
                assert(self.values()[p0 + 1] == lv[j + 1]);
            } else {
                lemma_slab::<O>(data);
                assert(gl(gs, gs.len() as int) == groups_len(gs)) by {
                    assert(gs.take(gs.len() as int) =~= gs);
                }
                lemma_groups_values_concat(gs, Seq::empty(), O::null());
                assert(gi < gs.len()) by {
                    if gi == gs.len() {
                        assert(self.pos() == gl(gs, gs.len() as int));
                    }
                }
                lemma_boundary::<O>(data, gi);
            }
        }
    }

    #[verifier::spinoff_prover]
    fn take_run(&mut self, max: usize) -> (r: Option<crate::Run<O::V>>)
        requires old(self).inv(), old(self).remaining > 0, max > 0,
        ensures
            final(self).inv(), final(self).data() == old(self).data(),
            match r {
                Some(run) => {
                    let count = run.count;
                    let value = run.value;
                    &&& 1 <= count <= max
                    &&& final(self).pos() == old(self).pos() + count <= old(self).values().len()
                    &&& forall|k: int| old(self).pos() <= k < final(self).pos() ==> old(self).values()[k] == value
                    &&& count < max && final(self).pos() < old(self).values().len()
                        ==> old(self).values()[final(self).pos()] != value
                    &&& final(self).dsegs() == old(self).dsegs()
                        + (if final(self).lit_now() || final(self).left_now() == 0 { 1int } else { 0int })
                },
                None => false,
            },
    {
        let ghost data = self.data@;
        let ghost gs = slab_groups::<O>(data);
        let ghost gi = self.gi@;
        let ghost g = gs[gi - 1];
        let ghost p0 = self.pos();
        proof {
            lemma_slab_group::<O>(data, gi - 1);
            self.lemma_at_value();
        }
        match &self.state {
            RunState::Repeat(v) => {
                let value = *v;
                let count = if self.remaining < max { self.remaining } else { max };
                self.remaining = self.remaining - count;
                proof {
                    assert forall|k: int| p0 <= k < p0 + count implies self.values()[k] == value by {
                        let j = k - gl(gs, gi - 1);
                        assert(group_values(g, O::null())[j] == value);
                    }
                    self.lemma_pos();
                    self.lemma_run_ends(p0, count, max, value);
                }
                Some(crate::Run { count, value })
            },
            RunState::Literal => {
                let ghost r0 = self.remaining;
                proof {
                    assert(r0 <= group_len(gs[gi - 1]));
                }
                let value = self.take_lit();
                proof {
                    self.lemma_lit_ends(p0, r0, max, value);
                }
                Some(crate::Run { count: 1, value })
            },
            RunState::Null => {
                let value = O::get_null();
                let count = if self.remaining < max { self.remaining } else { max };
                self.remaining = self.remaining - count;
                proof {
                    assert forall|k: int| p0 <= k < p0 + count implies self.values()[k] == value by {
                        let j = k - gl(gs, gi - 1);
                        assert(group_values(g, O::null())[j] == value);
                    }
                    self.lemma_pos();
                    self.lemma_run_ends(p0, count, max, value);
                }
                Some(crate::Run { count, value })
            },
            RunState::Idle => {
                proof {
                    assert(false);
                }
                None
            },
        }
    }

    /// Inside a literal run: skip `n` of its values, decoding none.
    #[inline]
    fn skip_literals(&mut self, n: usize)
        requires old(self).inv(), old(self).state is Literal, n <= old(self).remaining,
        ensures
            final(self).inv(), final(self).data() == old(self).data(),
            final(self).state is Literal, final(self).remaining == old(self).remaining - n,
            final(self).gi == old(self).gi, final(self).pos() == old(self).pos() + n,
    {
        let ghost data = self.data@;
        let ghost gs = slab_groups::<O>(data);
        let ghost gi = self.gi@;
        let ghost vs = gs[gi - 1]->values;
        let mut k: usize = 0;
        while k < n
            invariant
                self.inv(), self.data@ == data, self.gi@ == gi, self.state is Literal,
                k <= n, n <= old(self).remaining, gs == slab_groups::<O>(data), vs == gs[gi - 1]->values,
                self.remaining == old(self).remaining - k,
            decreases n - k,
        {
            let ghost i = vs.len() - self.remaining;
            let ghost b = self.byte_pos as int;
            let ghost after = encs::<O>(vs.skip(i + 1)) + groups_bytes::<O>(gs.skip(gi));
            proof {
                lemma_slab_lit::<O>(data, gi - 1, i);
                lemma_starts(O::enc(vs[i]), after);
            }
            let rest = &self.data[self.byte_pos..];
            assert(rest@ == data.skip(b));
            let vlen = O::value_len(rest).unwrap();
            proof {
                assert(vs.take(i + 1).len() == i + 1);
            }
            self.byte_pos = self.byte_pos + vlen;
            self.remaining = self.remaining - 1;
            k += 1;
        }
    }

    /// Skip `n` values and return the next, if any; past the end, stop
    /// there.
    pub(crate) fn nth(&mut self, n: usize) -> (r: Option<O::V>)
        requires old(self).inv(),
        ensures
            final(self).inv(), final(self).data() == old(self).data(),
            old(self).pos() + n < old(self).values().len() ==> r == Some(old(self).values()[old(self).pos() + n])
                && final(self).pos() == old(self).pos() + n + 1,
            old(self).pos() + n >= old(self).values().len() ==> r is None
                && final(self).pos() == old(self).values().len(),
    {
        proof {
            self.lemma_pos();
        }
        if self.remaining > 0 {
            if n < self.remaining {
                self.skip_in_run(n);
                proof {
                    self.lemma_pos();
                }
                return self.next();
            }
            let r0 = self.remaining;
            self.skip_in_run(r0);
            return self.nth_at_boundary(n - r0);
        }
        self.nth_at_boundary(n)
    }

    /// Skip `n` values of the current run.
    fn skip_in_run(&mut self, n: usize)
        requires old(self).inv(), n <= old(self).remaining,
        ensures
            final(self).inv(), final(self).data() == old(self).data(),
            final(self).pos() == old(self).pos() + n,
            final(self).remaining == old(self).remaining - n,
    {
        if let RunState::Literal = self.state {
            self.skip_literals(n);
        } else {
            self.remaining = self.remaining - n;
        }
    }

    /// `nth`, from a run boundary.
    fn nth_at_boundary(&mut self, n: usize) -> (r: Option<O::V>)
        requires old(self).inv(), old(self).remaining == 0,
        ensures
            final(self).inv(), final(self).data() == old(self).data(),
            old(self).pos() + n < old(self).values().len() ==> r == Some(old(self).values()[old(self).pos() + n])
                && final(self).pos() == old(self).pos() + n + 1,
            old(self).pos() + n >= old(self).values().len() ==> r is None
                && final(self).pos() == old(self).values().len(),
    {
        let ghost data = self.data@;
        let ghost target = self.pos() + n;
        let mut left = n;
        proof {
            self.lemma_boundary();
        }
        // skipped runs leave no state behind
        self.state = RunState::Idle;
        loop
            invariant
                self.inv(), self.data@ == data, data == old(self).data(),
                self.state is Idle, self.remaining == 0,
                self.pos() + left == target, target == old(self).pos() + n,
                self.byte_pos <= data.len(),
                old(self).values() == slab_values::<O>(data),
            decreases data.len() - self.byte_pos,
        {
            proof {
                self.lemma_boundary();
                self.lemma_pos();
            }
            if self.byte_pos >= self.data.len() {
                proof {
                    assert(self.pos() == self.values().len());
                    assert(target >= self.values().len());
                }
                return None;
            }
            match self.skip_group(left) {
                Ok(v) => {
                    proof {
                        assert(target < self.values().len());
                        assert(v == self.values()[target]);
                    }
                    return Some(v);
                },
                Err(m) => left = m,
            }
        }
    }

    /// At the start of a literal run: return its value `n` if it has
    /// one, else skip it and return how many values are left to skip.
    fn skip_in_lit(&mut self, n: usize) -> (r: Result<O::V, usize>)
        requires
            old(self).inv(), old(self).state is Literal,
            old(self).remaining == group_len(slab_groups::<O>(old(self).data())[old(self).gi@ - 1]),
        ensures
            final(self).inv(), final(self).data() == old(self).data(),
            match r {
                Ok(v) => old(self).pos() + n < old(self).values().len()
                    && v == old(self).values()[old(self).pos() + n]
                    && final(self).pos() == old(self).pos() + n + 1,
                Err(m) => m <= n,
            },
            r is Err ==> {
                &&& final(self).state is Idle && final(self).remaining == 0
                &&& final(self).byte_pos >= old(self).byte_pos
                &&& final(self).byte_pos <= final(self).data().len()
                &&& final(self).pos() + r->Err_0 == old(self).pos() + n
            },
    {
        if n < self.remaining {
            self.skip_literals(n);
            return Ok(self.take_lit());
        }
        let total = self.remaining;
        self.skip_literals(total);
        proof {
            self.lemma_boundary_lit();
        }
        self.state = RunState::Idle;
        Err(n - total)
    }

    /// At the end of a literal run, its bytes are done.
    proof fn lemma_boundary_lit(&self)
        requires self.inv(), self.state is Literal, self.remaining == 0,
        ensures
            self.byte_pos == gb::<O>(slab_groups::<O>(self.data@), self.gi@),
            self.byte_pos <= self.data@.len(),
    {
        self.lemma_boundary();
    }

    /// At a run boundary before the end: return value `n` if it is in the
    /// next run, else skip the run and return how many values are left to
    /// skip.
    #[verifier::spinoff_prover]
    #[inline(always)]
    fn skip_group(&mut self, n: usize) -> (r: Result<O::V, usize>)
        requires
            old(self).inv(), old(self).remaining == 0, old(self).state is Idle,
            old(self).byte_pos < old(self).data().len(),
        ensures Self::skip_post(*old(self), *final(self), n, r),
    {
        let ghost data = self.data@;
        let ghost gs = slab_groups::<O>(data);
        let ghost gi = self.gi@;
        proof {
            self.lemma_boundary();
        }
        let ghost g = gs[gi];
        let ghost rest = groups_bytes::<O>(gs.skip(gi + 1));
        let ghost h = header_of(g) as i64;
        let ghost b0 = self.byte_pos as int;
        proof {
            lemma_slab_group::<O>(data, gi);
            lemma_starts(O::enc_signed(h), payload::<O>(g) + rest);
        }
        let (count_bytes, count_raw) = O::read_signed_opt(&self.data[self.byte_pos..]).unwrap();
        proof {
            lemma_header::<O>(data.skip(b0), h, payload::<O>(g) + rest, count_bytes as int, count_raw);
            lemma_skip_skip(data, b0, count_bytes as int);
            O::axiom_enc_signed(h, h, Seq::empty(), Seq::empty());
        }
        if count_raw > 0 {
            self.skip_run_group(n, count_bytes, count_raw as usize)
        } else if count_raw < 0 {
            let total = (-count_raw) as usize;
            self.byte_pos = self.byte_pos + count_bytes;
            self.gi = Ghost(gi + 1);
            self.remaining = total;
            self.state = RunState::Literal;
            proof {
                let vs = g->values;
                assert(vs.take(vs.len() - total) =~= Seq::<O::V>::empty());
                assert(group_len(g) == total);
            }
            self.skip_in_lit(n)
        } else {
            self.skip_null_group(n, count_bytes)
        }
    }

    /// `skip_group` for a repeat run, whose header has been read.
    fn skip_run_group(&mut self, n: usize, count_bytes: usize, count: usize) -> (r: Result<O::V, usize>)
        requires
            old(self).inv(), old(self).remaining == 0, old(self).state is Idle,
            at_header::<O>(old(self).data(), old(self).gi@, old(self).byte_pos as int, count_bytes as int),
            slab_groups::<O>(old(self).data())[old(self).gi@] is Run,
            count == group_len(slab_groups::<O>(old(self).data())[old(self).gi@]),
        ensures Self::skip_post(*old(self), *final(self), n, r),
    {
        let ghost data = self.data@;
        let ghost gs = slab_groups::<O>(data);
        let ghost gi = self.gi@;
        let ghost g = gs[gi];
        let ghost rest = groups_bytes::<O>(gs.skip(gi + 1));
        let ghost p = self.byte_pos + count_bytes;
        proof {
            self.lemma_boundary();
            lemma_slab_group::<O>(data, gi);
            lemma_starts(O::enc(run_value(g)), rest);
        }
        let value_start = self.byte_pos + count_bytes;
        if n < count {
            let (vlen, value) = O::unpack(&self.data[value_start..]);
            proof {
                lemma_value::<O>(data.skip(p), run_value(g), rest, vlen as int, value);
                assert(group_values(g, O::null())[n as int] == value);
            }
            self.byte_pos = value_start + vlen;
            self.remaining = count - n - 1;
            self.run_at = value_start;
            self.state = RunState::Repeat(value);
            self.gi = Ghost(gi + 1);
            return Ok(value);
        }
        let vlen = O::value_len(&self.data[value_start..]).unwrap();
        self.byte_pos = value_start + vlen;
        self.gi = Ghost(gi + 1);
        proof {
            assert(self.byte_pos == gb::<O>(gs, gi + 1));
            assert(self.pos() == gl(gs, gi + 1));
        }
        Err(n - count)
    }

    /// `skip_group` for a null run, whose header has been read.
    fn skip_null_group(&mut self, n: usize, count_bytes: usize) -> (r: Result<O::V, usize>)
        requires
            old(self).inv(), old(self).remaining == 0, old(self).state is Idle,
            at_header::<O>(old(self).data(), old(self).gi@, old(self).byte_pos as int, count_bytes as int),
            slab_groups::<O>(old(self).data())[old(self).gi@] is Null,
        ensures Self::skip_post(*old(self), *final(self), n, r),
    {
        let ghost data = self.data@;
        let ghost gs = slab_groups::<O>(data);
        let ghost gi = self.gi@;
        let ghost g = gs[gi];
        let ghost rest = groups_bytes::<O>(gs.skip(gi + 1));
        let ghost p = self.byte_pos + count_bytes;
        proof {
            self.lemma_boundary();
            lemma_slab_group::<O>(data, gi);
            lemma_starts(O::enc_unsigned(g->Null_count as u64), rest);
        }
        let (ncb, null_count) = O::read_unsigned_opt(&self.data[self.byte_pos + count_bytes..]).unwrap();
        proof {
            lemma_count::<O>(data.skip(p), g->Null_count as u64, rest, ncb as int, null_count);
        }
        let total = null_count as usize;
        self.byte_pos = self.byte_pos + count_bytes + ncb;
        self.gi = Ghost(gi + 1);
        if n < total {
            self.remaining = total - n - 1;
            self.state = RunState::Null;
            proof {
                assert(group_values(g, O::null())[n as int] == O::null());
            }
            return Ok(O::get_null());
        }
        proof {
            assert(self.byte_pos == gb::<O>(gs, gi + 1));
            assert(self.pos() == gl(gs, gi + 1));
        }
        Err(n - total)
    }

    /// Skip `n` values, stopping at the end.
    pub(crate) fn advance_by(&mut self, n: usize)
        requires old(self).inv(),
        ensures
            final(self).inv(), final(self).data() == old(self).data(),
            final(self).pos() == if old(self).pos() + n < old(self).values().len() {
                old(self).pos() + n
            } else {
                old(self).values().len() as int
            },
    {
        proof {
            self.lemma_pos();
        }
        if n > 0 {
            self.nth(n - 1);
        }
    }

    /// Inside a literal run.
    /// Values left in the current run.
    #[inline(always)]
    pub(crate) fn left(&self) -> (r: usize)
        ensures r == self.left_now(),
    {
        self.remaining
    }

    pub(crate) fn is_literal(&self) -> (r: bool)
        ensures r == self.lit_now(),
    {
        matches!(self.state, RunState::Literal)
    }
}

// ── Suspending and resuming ─────────────────────────────────────────────────

/// The run a suspended reader is inside: [`RunState`] with the value's
/// position in place of the value, which is what drops the lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum SuspendedRun {
    /// A repeat run, and where its value's bytes start — what a resume
    /// reads to hand the value back out borrowed from the slab.
    Repeat { at: usize },
    Literal,
    Null,
    #[default]
    Idle,
}

/// A decoder's position, detached from the slab it was reading.
///
/// Holds no borrow, so it can be kept beside a `&mut` to the column the
/// slab belongs to — which is what lets an edit cursor carry its reader
/// across writes. [`resume`](Dec::resume) restores a decoder from one in
/// O(1).
///
/// Public only because [`ColumnEncoding::State`](crate::ColumnEncoding::State)
/// names it; it is not exported, so nothing outside the crate can name
/// one.
///
/// Position only, never a value: a repeat run records where its value is
/// rather than what it is, so the state is `Copy` and costs a `String`
/// column nothing to take. Holding the value instead would mean either
/// owning it — an allocation per suspend, and a cursor suspends on every
/// move — or holding `Get<'static>`, which for a borrowed type can only
/// come from transmuting away the slab's lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RleDecoderState {
    byte_pos: usize,
    remaining: usize,
    run: SuspendedRun,
}

impl RleDecoderState {
    /// The state of a decoder standing at the start of a slab.
    /// The start of a slab.
    pub(crate) closed spec fn is_start(&self) -> bool {
        self.byte_pos == 0 && self.remaining == 0 && self.run is Idle
    }

    pub(crate) fn start() -> (r: Self)
        ensures r.is_start(),
    {
        RleDecoderState { byte_pos: 0, remaining: 0, run: SuspendedRun::Idle }
    }
}

/// `st` is a position in group `gi` of `data`'s groups.
spec fn state_at<'a, O: RleOps<'a>>(data: Seq<u8>, st: RleDecoderState, gi: int) -> bool {
    let gs = slab_groups::<O>(data);
    &&& 0 <= gi <= gs.len()
    &&& match st.run {
        SuspendedRun::Idle => st.remaining == 0 && st.byte_pos == gb::<O>(gs, gi),
        SuspendedRun::Repeat { at } => {
            &&& gi >= 1 && gs[gi - 1] is Run
            &&& st.remaining <= gs[gi - 1]->Run_count
            &&& st.byte_pos == gb::<O>(gs, gi)
            &&& at == gb::<O>(gs, gi - 1) + O::enc_signed(gs[gi - 1]->Run_count as i64).len()
        },
        SuspendedRun::Literal => {
            let vs = gs[gi - 1]->values;
            &&& gi >= 1 && gs[gi - 1] is Lit
            &&& st.remaining <= vs.len()
            &&& st.byte_pos == gb::<O>(gs, gi - 1) + lit_head::<O>(vs.len()).len()
                + encs::<O>(vs.take(vs.len() - st.remaining)).len()
        },
        SuspendedRun::Null => {
            &&& gi >= 1 && gs[gi - 1] is Null
            &&& st.remaining <= gs[gi - 1]->Null_count
            &&& st.byte_pos == gb::<O>(gs, gi)
        },
    }
}

/// `st` is a position in the valid slab `data`.
pub(crate) closed spec fn state_ok<'a, O: RleOps<'a>>(data: Seq<u8>, st: RleDecoderState) -> bool {
    &&& valid_slab::<O>(data)
    &&& data.len() <= usize::MAX
    &&& exists|gi: int| state_at::<O>(data, st, gi)
}

/// The segments behind `st`, standing in group `gi`.
spec fn rsegs_at<'a, O: RleOps<'a>>(data: Seq<u8>, st: RleDecoderState, gi: int) -> int {
    let gs = slab_groups::<O>(data);
    match st.run {
        SuspendedRun::Idle => gseg(gs, gi),
        SuspendedRun::Literal => gseg(gs, gi - 1) + (gs[gi - 1]->values.len() - st.remaining),
        _ => gseg(gs, gi - 1) + (if st.remaining == 0 { 1int } else { 0int }),
    }
}

/// The segments wholly behind `st` in `data`: the groups passed, the
/// literal values read, and a repeat or null run once all of it is read.
pub(crate) closed spec fn rsegs<'a, O: RleOps<'a>>(data: Seq<u8>, st: RleDecoderState) -> int {
    rsegs_at::<O>(data, st, choose|gi: int| state_at::<O>(data, st, gi))
}

/// The segments behind a position known to stand in group `gi`.
proof fn lemma_rsegs_is<'a, O: RleOps<'a>>(data: Seq<u8>, st: RleDecoderState, gi: int)
    requires valid_slab::<O>(data), state_at::<O>(data, st, gi),
    ensures rsegs::<O>(data, st) == rsegs_at::<O>(data, st, gi),
{
    let g = choose|g: int| state_at::<O>(data, st, g);
    lemma_state_unique::<O>(data, st, g, gi);
}

/// The segments behind a position are among the slab's, which a
/// `usize` counts.
pub(crate) proof fn lemma_rsegs_le<'a, O: RleOps<'a>>(data: Seq<u8>, st: RleDecoderState)
    requires state_ok::<O>(data, st),
    ensures 0 <= rsegs::<O>(data, st) <= groups_segments(slab_groups::<O>(data)) <= usize::MAX,
{
    let gs = slab_groups::<O>(data);
    let gi = choose|g: int| state_at::<O>(data, st, g);
    assert(state_at::<O>(data, st, gi));
    lemma_slab::<O>(data);
    super::read::lemma_segments_le::<O>(gs);
    lemma_gseg_split::<O>(data, gi);
    if gi >= 1 {
        lemma_gseg_split::<O>(data, gi - 1);
        lemma_gseg(gs, gi - 1);
        lemma_slab_group::<O>(data, gi - 1);
    }
}

/// A position at the end of its group is behind all of it.
proof fn lemma_rsegs_end<'a, O: RleOps<'a>>(data: Seq<u8>, st: RleDecoderState, gi: int)
    requires valid_slab::<O>(data), state_at::<O>(data, st, gi), st.remaining == 0,
    ensures rsegs_at::<O>(data, st, gi) == gseg(slab_groups::<O>(data), gi),
{
    let gs = slab_groups::<O>(data);
    if !(st.run is Idle) {
        lemma_gseg(gs, gi - 1);
        lemma_slab_group::<O>(data, gi - 1);
    }
}

/// The byte position of `st`.
pub(crate) closed spec fn sbyte(st: RleDecoderState) -> int {
    st.byte_pos as int
}

/// The index of the value `st` stands before in `data`.
pub(crate) closed spec fn state_pos<'a, O: RleOps<'a>>(data: Seq<u8>, st: RleDecoderState) -> int {
    gl(slab_groups::<O>(data), choose|gi: int| state_at::<O>(data, st, gi)) - st.remaining
}

/// Group offsets increase.
pub(crate) proof fn lemma_gb_lt<'a, O: RleOps<'a>>(data: Seq<u8>, i: int, j: int)
    requires valid_slab::<O>(data), 0 <= i < j <= slab_groups::<O>(data).len(),
    ensures gb::<O>(slab_groups::<O>(data), i) < gb::<O>(slab_groups::<O>(data), j),
    decreases j - i,
{
    lemma_slab_group::<O>(data, i);
    if i + 1 < j {
        lemma_gb_lt::<O>(data, i + 1, j);
    }
}

/// A suspended position names one group.
proof fn lemma_state_unique<'a, O: RleOps<'a>>(data: Seq<u8>, st: RleDecoderState, g1: int, g2: int)
    requires valid_slab::<O>(data), state_at::<O>(data, st, g1), state_at::<O>(data, st, g2),
    ensures g1 == g2,
{
    let gs = slab_groups::<O>(data);
    if g1 != g2 {
        let (a, b) = if g1 < g2 { (g1, g2) } else { (g2, g1) };
        lemma_gb_lt::<O>(data, a, b);
        if st.run is Literal {
            // a literal position lies after its group's start, at or before its end
            assert forall|g: int| #[trigger] state_at::<O>(data, st, g) implies
                gb::<O>(gs, g - 1) < st.byte_pos <= gb::<O>(gs, g) by {
                let vs = gs[g - 1]->values;
                lemma_slab_group::<O>(data, g - 1);
                O::axiom_enc_signed(-(vs.len() as int) as i64, 0, Seq::empty(), Seq::empty());
                let k = vs.len() - st.remaining;
                assert(vs =~= vs.take(k) + vs.skip(k));
                lemma_encs_concat::<O>(vs.take(k), vs.skip(k));
            }
            if a < b - 1 {
                lemma_gb_lt::<O>(data, a, b - 1);
            }
        }
    }
}

impl<'a, O: RleOps<'a>> Dec<'a, O> {
    /// Capture the position so it can be restored against the same bytes
    /// later, without holding a borrow of them. Field copies only.
    /// The position a suspend records.
    pub(crate) closed spec fn dstate(&self) -> RleDecoderState {
        RleDecoderState {
            byte_pos: self.byte_pos,
            remaining: self.remaining,
            run: match self.state {
                RunState::Repeat(_) => SuspendedRun::Repeat { at: self.run_at },
                RunState::Literal => SuspendedRun::Literal,
                RunState::Null => SuspendedRun::Null,
                RunState::Idle => SuspendedRun::Idle,
            },
        }
    }

    /// The position a suspend would record is this reader's.
    pub(crate) proof fn lemma_dstate(&self)
        requires self.inv(),
        ensures
            state_ok::<O>(self.data(), self.dstate()), state_pos::<O>(self.data(), self.dstate()) == self.pos(),
            rsegs::<O>(self.data(), self.dstate()) == self.dsegs(),
    {
        let data = self.data@;
        let r = self.dstate();
        assert(state_at::<O>(data, r, self.gi@));
        let g = choose|gi: int| state_at::<O>(data, r, gi);
        lemma_state_unique::<O>(data, r, g, self.gi@);
    }

    pub(crate) fn suspend(&self) -> (r: RleDecoderState)
        requires self.inv(),
        ensures
            state_ok::<O>(self.data(), r), state_pos::<O>(self.data(), r) == self.pos(),
            r == self.dstate(),
    {
        let r = RleDecoderState {
            byte_pos: self.byte_pos,
            remaining: self.remaining,
            run: match &self.state {
                RunState::Repeat(_) => SuspendedRun::Repeat { at: self.run_at },
                RunState::Literal => SuspendedRun::Literal,
                RunState::Null => SuspendedRun::Null,
                RunState::Idle => SuspendedRun::Idle,
            },
        };
        proof {
            let data = self.data@;
            assert(state_at::<O>(data, r, self.gi@));
            let g = choose|gi: int| state_at::<O>(data, r, gi);
            lemma_state_unique::<O>(data, r, g, self.gi@);
        }
        r
    }

    /// Restore a suspended position against `data`.
    ///
    /// O(1): only a repeat run reads anything, and only its own value.
    pub(crate) fn resume(data: &'a [u8], st: &RleDecoderState) -> (r: Self)
        requires state_ok::<O>(data@, *st),
        ensures
            r.inv(), r.data() == data@, r.pos() == state_pos::<O>(data@, *st), sbyte(r.dstate()) == sbyte(*st),
            r.dstate() == *st,
    {
        let ghost gi = choose|gi: int| state_at::<O>(data@, *st, gi);
        let ghost gs = slab_groups::<O>(data@);
        let (state, run_at) = match &st.run {
            SuspendedRun::Repeat { at } => {
                let ghost g = gs[gi - 1];
                let ghost rest = groups_bytes::<O>(gs.skip(gi));
                proof {
                    assert(state_at::<O>(data@, *st, gi));
                    lemma_slab_group::<O>(data@, gi - 1);
                    let hd = O::enc_signed(g->Run_count as i64);
                    lemma_skip_skip(data@, gb::<O>(gs, gi - 1), hd.len() as int);
                    assert(header_of(g) as i64 == g->Run_count as i64);
                    assert((hd + (O::enc(run_value(g)) + rest)).skip(hd.len() as int) =~= O::enc(run_value(g)) + rest);
                    lemma_starts(O::enc(run_value(g)), rest);
                }
                let (_vlen, value) = O::unpack(&data[*at..]);
                proof {
                    lemma_value::<O>(data@.skip(*at as int), run_value(g), rest, _vlen as int, value);
                }
                (RunState::Repeat(value), *at)
            },
            SuspendedRun::Literal => (RunState::Literal, 0),
            SuspendedRun::Null => (RunState::Null, 0),
            SuspendedRun::Idle => (RunState::Idle, 0),
        };
        Dec { data, byte_pos: st.byte_pos, remaining: st.remaining, state, run_at, gi: Ghost(gi) }
    }
}

// ── Opening a rebuild at an item ────────────────────────────────────────────

/// Where an edit opening at an item finds the slab: the writer's state
/// for the part of the run in hand before the cut (`state`, borrowing its
/// value), the byte offset where its region starts, and a reader standing
/// at the cut.
pub(crate) struct PartitionAt<'a, W: WriteOps<'a>> {
    pub(crate) state: State<'a, W>,
    /// segments of the slab before `byte_start`
    pub(crate) segments: usize,
    pub(crate) byte_start: usize,
    /// Complete runs behind this position, counted for a *reader*: a
    /// position at a run's end is past it, where `segments` — the
    /// writer's count, of runs before the region it opens — is not.
    pub(crate) read_segments: usize,
    /// A reader standing at this position, built from the same walk.
    pub(crate) read_state: RleDecoderState,
}

/// The groups before `i` stand for the values before their end.
proof fn lemma_take_groups<'a, O: RleOps<'a>>(data: Seq<u8>, i: int)
    requires valid_slab::<O>(data), 0 <= i <= slab_groups::<O>(data).len(),
    ensures ({
        let gs = slab_groups::<O>(data);
        &&& canonical::<O>(gs.take(i))
        &&& groups_values(gs.take(i), O::null()) == slab_values::<O>(data).take(gl(gs, i))
        &&& data.take(gb::<O>(gs, i)) == groups_bytes::<O>(gs.take(i))
        &&& 0 <= gb::<O>(gs, i) <= data.len()
        &&& 0 <= gl(gs, i) <= slab_values::<O>(data).len()
    }),
{
    let gs = slab_groups::<O>(data);
    lemma_slab::<O>(data);
    assert forall|j: int| 0 <= j < gs.take(i).len() implies group_ok::<O>(#[trigger] gs.take(i)[j]) by {
        assert(gs.take(i)[j] == gs[j]);
    }
    assert forall|j: int| 0 < j < gs.take(i).len() implies adjacent_ok(gs.take(i)[j - 1], #[trigger] gs.take(i)[j]) by {
        assert(gs.take(i)[j] == gs[j] && gs.take(i)[j - 1] == gs[j - 1]);
    }
    assert(gs =~= gs.take(i) + gs.skip(i));
    lemma_groups_values_concat(gs.take(i), gs.skip(i), O::null());
    lemma_groups_values_concat(gs.skip(i), Seq::empty(), O::null());
    lemma_groups_bytes_concat::<O>(gs.take(i), gs.skip(i));
    lemma_groups_len_concat(gs.take(i), gs.skip(i));
    assert(slab_values::<O>(data).take(gl(gs, i)) =~= groups_values(gs.take(i), O::null()));
    assert(data.take(gb::<O>(gs, i)) =~= groups_bytes::<O>(gs.take(i)));
}

/// Values' encodings are at least a byte each.
proof fn lemma_encs_len<'a, O: RleOps<'a>>(vs: Seq<O::V>)
    ensures encs::<O>(vs).len() >= vs.len(),
    decreases vs.len(),
{
    if vs.len() > 0 {
        O::axiom_enc(vs.last(), vs.last(), Seq::empty(), Seq::empty());
        lemma_encs_len::<O>(vs.drop_last());
    }
}

/// The writer state `s`, with the slab's bytes before `bs` as its
/// prefix, stands for the slab's first `at` values.
pub(crate) open spec fn landed<'a, W: WriteOps<'a>>(data: Seq<u8>, at: int, s: State<'a, W>, bs: int, segs: int) -> bool {
    exists|gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(s, data.take(bs), Seq::empty(), 0, gs, ws, h0)
        && values_of(s, gs, ws) == slab_values::<W::R>(data).take(at) && acc(s, gs, ws) == segs
}

/// The segments of groups `gs`'s first `i`.
pub(crate) open spec fn gseg<V>(gs: Seq<Group<V>>, i: int) -> int {
    groups_segments(gs.take(i)) as int
}

/// Group `i`'s segments follow the first `i`'s.
pub(crate) proof fn lemma_gseg<V>(gs: Seq<Group<V>>, i: int)
    requires 0 <= i < gs.len(),
    ensures gseg(gs, i + 1) == gseg(gs, i) + group_segments(gs[i]),
{
    assert(gs.take(i + 1).drop_last() =~= gs.take(i));
}

/// The literal before group `gi`, if there is one: its header position
/// and length.
pub(crate) open spec fn prev_lit_of<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int) -> Option<(usize, usize)> {
    let gs = slab_groups::<O>(data);
    if gi > 0 && gs[gi - 1] is Lit {
        Some((gb::<O>(gs, gi - 1) as usize, gs[gi - 1]->values.len() as usize))
    } else {
        None
    }
}

/// Values `X.take(a)` then `k` more from a run of `v` there.
proof fn lemma_take_run<V>(x: Seq<V>, a: int, k: int, c: int, v: V)
    requires 0 <= a, 1 <= k <= c, a + c <= x.len(), forall|j: int| a <= j < a + c ==> x[j] == v,
    ensures x.take(a + k) == x.take(a) + copies(k as nat, v),
{
    assert(x.take(a + k) =~= x.take(a) + copies(k as nat, v));
}

/// Landing in a repeat run just after its first value, after a literal:
/// the value joins the literal.
#[verifier::spinoff_prover]
#[verifier::rlimit(40)]
proof fn lemma_land_run_lit<'a, W: WriteOps<'a>>(data: Seq<u8>, gi: int, s: State<'a, W>)
    requires
        valid_slab::<W::R>(data), data.len() < i64::MAX, data.len() <= usize::MAX,
        0 < gi < slab_groups::<W::R>(data).len(), slab_groups::<W::R>(data)[gi] is Run,
        slab_groups::<W::R>(data)[gi - 1] is Lit,
        s matches State::Lit { count, local, header_pos, current, .. }
            && count == slab_groups::<W::R>(data)[gi - 1]->values.len() && local == 0
            && header_pos == gb::<W::R>(slab_groups::<W::R>(data), gi - 1)
            && W::val(current) == run_value(slab_groups::<W::R>(data)[gi]),
    ensures landed::<W>(data, gl(slab_groups::<W::R>(data), gi) + 1, s, gb::<W::R>(slab_groups::<W::R>(data), gi),
        gseg(slab_groups::<W::R>(data), gi)),
{
    let gs = slab_groups::<W::R>(data);
    let g = gs[gi];
    let v = run_value(g);
    let x = slab_values::<W::R>(data);
    let a = gl(gs, gi);
    let gp = gs[gi - 1];
    let vp = gp->values;
    let gs2 = gs.take(gi - 1);
    let h0 = -(vp.len() as int) as i64;
    let pre = data.take(gb::<W::R>(gs, gi));
    lemma_slab_group::<W::R>(data, gi);
    lemma_slab_group::<W::R>(data, gi - 1);
    lemma_take_groups::<W::R>(data, gi);
    lemma_take_groups::<W::R>(data, gi - 1);
    assert(gs.take(gi) =~= gs2.push(gp));
    lemma_push::<W::R>(gs2, gp);
    lemma_encs_len::<W::R>(vp);
    assert(group_ok::<W::R>(gp));
    assert(vp.last() != v);
    assert(group_ok::<W::R>(g));
    lemma_lit_grow::<W::R>(gs2, vp, v);
    assert(vp.take(vp.len() as int) =~= vp);
    assert(vp.skip(vp.len() as int) =~= Seq::<VOf<'a, W>>::empty());
    assert(Seq::<u8>::empty().skip(0) =~= encs::<W::R>(vp.skip(vp.len() as int)));
    assert(group_bytes::<W::R>(gp) == <W::R as RleOps<'a>>::enc_signed(h0) + encs::<W::R>(vp));
    assert(pre =~= groups_bytes::<W::R>(gs2) + <W::R as RleOps<'a>>::enc_signed(h0) + encs::<W::R>(vp.take(vp.len() as int)));
    assert(vp.len() < usize::MAX) by {
        lemma_group_bytes_nonempty::<W::R>(gp);
        assert(gb::<W::R>(gs, gi) <= data.len());
    }
    assert(rel(s, pre, Seq::empty(), 0, gs2, vp, h0));
    lemma_gseg(gs, gi - 1);
    lemma_push::<W::R>(gs2, Group::Lit { values: vp.push(v) });
    lemma_vals_lit_run::<W::R>(data, gi);
    assert(values_of(s, gs2, vp) == x.take(a + 1));
}

/// The values up to a repeat run's first, after a literal.
proof fn lemma_vals_lit_run<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int)
    requires
        valid_slab::<O>(data), 0 < gi < slab_groups::<O>(data).len(),
        slab_groups::<O>(data)[gi] is Run, slab_groups::<O>(data)[gi - 1] is Lit,
    ensures ({
        let gs = slab_groups::<O>(data);
        slab_values::<O>(data).take(gl(gs, gi) + 1)
            == slab_values::<O>(data).take(gl(gs, gi - 1)) + gs[gi - 1]->values.push(run_value(gs[gi]))
    }),
{
    let gs = slab_groups::<O>(data);
    let x = slab_values::<O>(data);
    let g = gs[gi];
    let gp = gs[gi - 1];
    let vp = gp->values;
    let a0 = gl(gs, gi - 1);
    let a = gl(gs, gi);
    lemma_slab_group::<O>(data, gi);
    lemma_slab_group::<O>(data, gi - 1);
    assert forall|j: int| 0 <= j < vp.len() implies x[a0 + j] == vp[j] by {
        assert(group_values(gp, O::null())[j] == vp[j]);
    }
    assert(x[a] == run_value(g)) by {
        assert(group_values(g, O::null())[0] == run_value(g));
    }
    assert(x.take(a + 1) =~= x.take(a0) + vp.push(run_value(g)));
}

/// Landing in a repeat run otherwise: the part before the cut is a run
/// (or a lone value) of its own.
#[verifier::spinoff_prover]
proof fn lemma_land_run<'a, W: WriteOps<'a>>(data: Seq<u8>, gi: int, k: int, s: State<'a, W>)
    requires
        valid_slab::<W::R>(data), data.len() < i64::MAX,
        0 <= gi < slab_groups::<W::R>(data).len(), slab_groups::<W::R>(data)[gi] is Run,
        1 <= k <= slab_groups::<W::R>(data)[gi]->Run_count,
        k == 1 ==> !(gi > 0 && slab_groups::<W::R>(data)[gi - 1] is Lit),
        match s {
            State::Lone(c) => k == 1 && W::val(c) == run_value(slab_groups::<W::R>(data)[gi]),
            State::Run(n, c) => k >= 2 && n == k && W::val(c) == run_value(slab_groups::<W::R>(data)[gi]),
            _ => false,
        },
    ensures landed::<W>(data, gl(slab_groups::<W::R>(data), gi) + k, s, gb::<W::R>(slab_groups::<W::R>(data), gi),
        gseg(slab_groups::<W::R>(data), gi)),
{
    let gs = slab_groups::<W::R>(data);
    let g = gs[gi];
    let v = run_value(g);
    let x = slab_values::<W::R>(data);
    let a = gl(gs, gi);
    lemma_slab_group::<W::R>(data, gi);
    lemma_take_groups::<W::R>(data, gi);
    assert forall|j: int| a <= j < a + group_len(g) implies x[j] == v by {
        assert(group_values(g, <W::R as RleOps<'a>>::null())[j - a] == v);
    }
    lemma_take_run(x, a, k, group_len(g) as int, v);
    let pre = data.take(gb::<W::R>(gs, gi));
    let g1 = pend_group(s, Seq::empty())->Some_0;
    let gs2 = gs.take(gi);
    assert(group_ok::<W::R>(g));
    if gi > 0 {
        assert(gs2.last() == gs[gi - 1]);
        assert(adjacent_ok(gs[gi - 1], g));
    }
    lemma_canonical_join::<W::R>(gs2, g1);
    lemma_push::<W::R>(gs2, g1);
    assert(pre + Seq::<u8>::empty().skip(0) =~= groups_bytes::<W::R>(gs2));
    assert(rel(s, pre, Seq::empty(), 0, gs2, Seq::empty(), 0));
    lemma_run_group::<W::R>(k as nat, v);
    assert(values_of(s, gs2, Seq::empty()) == x.take(a + k));
}

/// Landing in a null run.
#[verifier::spinoff_prover]
proof fn lemma_land_null<'a, W: WriteOps<'a>>(data: Seq<u8>, gi: int, k: int, s: State<'a, W>)
    requires
        valid_slab::<W::R>(data),
        0 <= gi < slab_groups::<W::R>(data).len(), slab_groups::<W::R>(data)[gi] is Null,
        1 <= k <= slab_groups::<W::R>(data)[gi]->Null_count, k <= usize::MAX,
        s == State::<'a, W>::Null(k as usize),
    ensures landed::<W>(data, gl(slab_groups::<W::R>(data), gi) + k, s, gb::<W::R>(slab_groups::<W::R>(data), gi),
        gseg(slab_groups::<W::R>(data), gi)),
{
    let gs = slab_groups::<W::R>(data);
    let g = gs[gi];
    let nl = <W::R as RleOps<'a>>::null();
    let x = slab_values::<W::R>(data);
    let a = gl(gs, gi);
    lemma_slab_group::<W::R>(data, gi);
    lemma_take_groups::<W::R>(data, gi);
    assert forall|j: int| a <= j < a + group_len(g) implies x[j] == nl by {
        assert(group_values(g, nl)[j - a] == nl);
    }
    lemma_take_run(x, a, k, group_len(g) as int, nl);
    let pre = data.take(gb::<W::R>(gs, gi));
    let g1 = Group::<VOf<'a, W>>::Null { count: k as nat };
    let gs2 = gs.take(gi);
    assert(group_ok::<W::R>(g));
    if gi > 0 {
        assert(gs2.last() == gs[gi - 1]);
        assert(adjacent_ok(gs[gi - 1], g));
    }
    lemma_canonical_join::<W::R>(gs2, g1);
    lemma_push::<W::R>(gs2, g1);
    assert(pre + Seq::<u8>::empty().skip(0) =~= groups_bytes::<W::R>(gs2));
    assert(rel(s, pre, Seq::empty(), 0, gs2, Seq::empty(), 0));
    assert(values_of(s, gs2, Seq::empty()) == x.take(a + k));
}

/// Landing in a literal at value `jj`: the values before it and it.
#[verifier::spinoff_prover]
#[verifier::rlimit(80)]
proof fn lemma_land_lit<'a, W: WriteOps<'a>>(data: Seq<u8>, gi: int, jj: int, s: State<'a, W>, bs: int)
    requires
        valid_slab::<W::R>(data), data.len() <= usize::MAX,
        0 <= gi < slab_groups::<W::R>(data).len(), slab_groups::<W::R>(data)[gi] is Lit,
        0 <= jj < slab_groups::<W::R>(data)[gi]->values.len(),
        jj == 0 ==> bs == gb::<W::R>(slab_groups::<W::R>(data), gi),
        jj == 0 ==> (s matches State::Lone(c) && W::val(c) == slab_groups::<W::R>(data)[gi]->values[0]),
        jj > 0 ==> bs == gb::<W::R>(slab_groups::<W::R>(data), gi)
                + lit_head::<W::R>(slab_groups::<W::R>(data)[gi]->values.len()).len()
                + encs::<W::R>(slab_groups::<W::R>(data)[gi]->values.take(jj)).len(),
        jj > 0 ==> (s matches State::Lit { count, local, header_pos, current, .. }
            && count == jj && local == 0 && header_pos == gb::<W::R>(slab_groups::<W::R>(data), gi)
            && W::val(current) == slab_groups::<W::R>(data)[gi]->values[jj]),
    ensures landed::<W>(data, gl(slab_groups::<W::R>(data), gi) + jj + 1, s, bs, gseg(slab_groups::<W::R>(data), gi) + jj),
{
    hide(canonical);
    hide(gseg);
    let gs = slab_groups::<W::R>(data);
    let g = gs[gi];
    let vs = g->values;
    let x = slab_values::<W::R>(data);
    let a = gl(gs, gi);
    let gs2 = gs.take(gi);
    lemma_slab_group::<W::R>(data, gi);
    lemma_take_groups::<W::R>(data, gi);
    assert(group_ok::<W::R>(g));
    let w = vs.take(jj + 1);
    assert forall|j: int| 0 <= j < vs.len() implies x[a + j] == vs[j] by {
        assert(group_values(g, <W::R as RleOps<'a>>::null())[j] == vs[j]);
    }
    assert(x.take(a + jj + 1) =~= x.take(a) + w);
    // the literal's first values are a literal that may follow the same
    assert forall|i: int| 0 <= i < w.len() implies !<W::R as RleOps<'a>>::is_null(#[trigger] w[i]) by {
        assert(w[i] == vs[i]);
    }
    assert forall|i: int| 0 < i < w.len() implies w[i - 1] != #[trigger] w[i] by {
        assert(w[i] == vs[i] && w[i - 1] == vs[i - 1]);
    }
    let gw = Group::Lit { values: w };
    assert(group_ok::<W::R>(gw));
    if gi > 0 {
        assert(gs2.last() == gs[gi - 1]);
        assert(adjacent_ok(gs[gi - 1], g));
        assert(w[0] == vs[0]);
    }
    lemma_canonical_join::<W::R>(gs2, gw);
    lemma_push::<W::R>(gs2, gw);
    let pre = data.take(bs);
    if jj == 0 {
        assert(w =~= seq![vs[0]]);
        assert(pre + Seq::<u8>::empty().skip(0) =~= groups_bytes::<W::R>(gs2));
        assert(rel(s, pre, Seq::empty(), 0, gs2, Seq::empty(), 0));
        assert(values_of(s, gs2, Seq::empty()) == x.take(a + jj + 1));
        assert(acc(s, gs2, Seq::empty()) == gseg(gs, gi) + jj) by {
            reveal(gseg);
        }
    } else {
        let ws = vs.take(jj);
        let h0 = -(vs.len() as int) as i64;
        let hd = <W::R as RleOps<'a>>::enc_signed(h0);
        let rest = groups_bytes::<W::R>(gs.skip(gi + 1));
        assert(vs =~= ws + vs.skip(jj));
        lemma_encs_concat::<W::R>(ws, vs.skip(jj));
        assert(data.skip(gb::<W::R>(gs, gi)) == hd + (encs::<W::R>(vs) + rest));
        assert(data =~= data.take(gb::<W::R>(gs, gi)) + data.skip(gb::<W::R>(gs, gi)));
        assert(pre =~= groups_bytes::<W::R>(gs2) + hd + encs::<W::R>(ws));
        assert(ws.take(jj) =~= ws);
        assert(ws.skip(jj) =~= Seq::<VOf<'a, W>>::empty());
        assert(Seq::<u8>::empty().skip(0) =~= encs::<W::R>(ws.skip(jj)));
        assert(ws.push(vs[jj]) =~= w);
        lemma_encs_len::<W::R>(vs);
        assert(rel(s, pre, Seq::empty(), 0, gs2, ws, h0));
        assert(values_of(s, gs2, ws) == x.take(a + jj + 1));
        assert(acc(s, gs2, ws) == gseg(gs, gi) + jj) by {
            reveal(gseg);
        }
    }
}

/// Walking a slab to an item: landed, or the next group's position.
pub(crate) enum Walk<'a, W: WriteOps<'a>> {
    Landed(PartitionAt<'a, W>),
    Next { pos: usize, item: usize, segments: usize, prev_lit: Option<(usize, usize)> },
}

/// Where a walk to `at` stands: at group `gi`, which `at` lies past the
/// start of.
pub(crate) open spec fn walk_at<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, gi: int, pos: int, item: int, segments: int) -> bool {
    let gs = slab_groups::<O>(data);
    &&& valid_slab::<O>(data) && data.len() < i64::MAX && data.len() <= usize::MAX
    &&& 0 <= gi < gs.len() && pos == gb::<O>(gs, gi) && item == gl(gs, gi) && item < at
    &&& 0 <= segments <= item && segments == gseg(gs, gi)
    &&& at <= slab_values::<O>(data).len() && groups_len(gs) <= usize::MAX
}

/// What a walk step promises: a landing reads `at`, or the next group is
/// reached.
pub(crate) open spec fn walk_post<'a, W: WriteOps<'a>>(data: Seq<u8>, at: int, gi: int, r: Walk<'a, W>) -> bool {
    match r {
        Walk::Landed(p) => p.byte_start <= data.len() && p.byte_start <= sbyte(p.read_state)
            && p.read_segments == rsegs::<W::R>(data, p.read_state)
            && state_ok::<W::R>(data, p.read_state) && state_pos::<W::R>(data, p.read_state) == at
            && landed::<W>(data, at, p.state, p.byte_start as int, p.segments as int),
        Walk::Next { pos, item, segments, prev_lit } => {
            &&& walk_at::<W::R>(data, at, gi + 1, pos as int, item as int, segments as int)
            &&& prev_lit == prev_lit_of::<W::R>(data, gi + 1)
        },
    }
}

/// The header of group `gi` at `pos`: its value and length.
proof fn lemma_walk_header<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int, pos: int, hb: int, hv: i64)
    requires
        valid_slab::<O>(data), 0 <= gi < slab_groups::<O>(data).len(), pos == gb::<O>(slab_groups::<O>(data), gi),
        0 <= hb <= data.len() - pos, data.skip(pos).take(hb) == O::enc_signed(hv),
    ensures ({
        let gs = slab_groups::<O>(data);
        let g = gs[gi];
        &&& hv as int == header_of(g) && hb == O::enc_signed(header_of(g) as i64).len()
        &&& data.skip(pos + hb) == payload::<O>(g) + groups_bytes::<O>(gs.skip(gi + 1))
    }),
{
    let gs = slab_groups::<O>(data);
    let g = gs[gi];
    let rest = groups_bytes::<O>(gs.skip(gi + 1));
    lemma_slab_group::<O>(data, gi);
    lemma_header::<O>(data.skip(pos), header_of(g) as i64, payload::<O>(g) + rest, hb, hv);
    lemma_skip_skip(data, pos, hb);
}

/// A repeat run: land in it, or step past it.
#[verifier::spinoff_prover]
fn walk_run<'a, W: WriteOps<'a>>(data: &'a [u8], at: usize, Ghost(gi): Ghost<int>, pos: usize, item: usize, segments: usize, prev_lit: Option<(usize, usize)>, hb: usize, count: usize) -> (r: Walk<'a, W>)
    requires
        walk_at::<W::R>(data@, at as int, gi, pos as int, item as int, segments as int),
        prev_lit == prev_lit_of::<W::R>(data@, gi),
        slab_groups::<W::R>(data@)[gi] is Run, count == group_len(slab_groups::<W::R>(data@)[gi]),
        hb == <W::R as RleOps<'a>>::enc_signed(count as i64).len(),
        pos + hb <= data@.len(),
        data@.skip(pos + hb) == payload::<W::R>(slab_groups::<W::R>(data@)[gi])
            + groups_bytes::<W::R>(slab_groups::<W::R>(data@).skip(gi + 1)),
    ensures walk_post::<W>(data@, at as int, gi, r),
{
    let ghost gs = slab_groups::<W::R>(data@);
    let ghost g = gs[gi];
    let ghost rest = groups_bytes::<W::R>(gs.skip(gi + 1));
    let ghost p = pos + hb;
    proof {
        lemma_slab_group::<W::R>(data@, gi);
        lemma_starts(<W::R as RleOps<'a>>::enc(run_value(g)), rest);
    }
    let value_start = pos + hb;
    let rest1 = &data[value_start..];
    assert(rest1@ == data@.skip(p));
    if at - item <= count {
        let k = at - item;
        let (vlen, value) = <W::R as RleOps<'a>>::unpack(rest1);
        proof {
            lemma_value::<W::R>(data@.skip(p), run_value(g), rest, vlen as int, value);
        }
        let state = match prev_lit {
            Some((hp, n)) => {
                if k == 1 {
                    State::lit(n, W::cow_ref(value), hp)
                } else {
                    State::make_run(k, W::cow_ref(value))
                }
            },
            None => State::make_run(k, W::cow_ref(value)),
        };
        let rs = RleDecoderState { byte_pos: value_start + vlen, remaining: count - k, run: SuspendedRun::Repeat { at: value_start } };
        proof {
            if prev_lit is Some && k == 1 {
                lemma_slab_group::<W::R>(data@, gi - 1);
                lemma_encs_len::<W::R>(gs[gi - 1]->values);
                assert(gs[gi - 1]->values.len() <= usize::MAX);
                lemma_land_run_lit::<W>(data@, gi, state);
            } else {
                lemma_land_run::<W>(data@, gi, k as int, state);
            }
            assert(state_at::<W::R>(data@, rs, gi + 1));
            let g2 = choose|g3: int| state_at::<W::R>(data@, rs, g3);
            lemma_state_unique::<W::R>(data@, rs, g2, gi + 1);
        }
        let read_segments = if k == count { segments + 1 } else { segments };
        return Walk::Landed(PartitionAt { state, segments, byte_start: pos, read_segments, read_state: rs });
    }
    let vlen = <W::R as RleOps<'a>>::value_len(rest1).unwrap();
    proof {
        lemma_gseg(gs, gi);
        assert(group_ok::<W::R>(g));
        assert(vlen == <W::R as RleOps<'a>>::enc(run_value(g)).len());
        assert(group_bytes::<W::R>(g) == <W::R as RleOps<'a>>::enc_signed(count as i64) + <W::R as RleOps<'a>>::enc(run_value(g)));
        assert(value_start + vlen == gb::<W::R>(gs, gi + 1));
        assert(item + count == gl(gs, gi + 1));
        lemma_slab::<W::R>(data@);
        if gi + 1 == gs.len() {
            assert(gs.take(gi + 1) =~= gs);
        }
    }
    Walk::Next { pos: value_start + vlen, item: item + count, segments: segments + 1, prev_lit: None }
}

/// A null run: land in it, or step past it.
#[verifier::spinoff_prover]
#[verifier::rlimit(40)]
fn walk_null<'a, W: WriteOps<'a>>(data: &'a [u8], at: usize, Ghost(gi): Ghost<int>, pos: usize, item: usize, segments: usize, hb: usize) -> (r: Walk<'a, W>)
    requires
        walk_at::<W::R>(data@, at as int, gi, pos as int, item as int, segments as int),
        slab_groups::<W::R>(data@)[gi] is Null,
        hb == <W::R as RleOps<'a>>::enc_signed(0).len(),
        pos + hb <= data@.len(),
        data@.skip(pos + hb) == payload::<W::R>(slab_groups::<W::R>(data@)[gi])
            + groups_bytes::<W::R>(slab_groups::<W::R>(data@).skip(gi + 1)),
    ensures walk_post::<W>(data@, at as int, gi, r),
{
    let ghost gs = slab_groups::<W::R>(data@);
    let ghost g = gs[gi];
    let ghost rest = groups_bytes::<W::R>(gs.skip(gi + 1));
    let ghost p = pos + hb;
    proof {
        lemma_slab_group::<W::R>(data@, gi);
        lemma_starts(<W::R as RleOps<'a>>::enc_unsigned(g->Null_count as u64), rest);
    }
    let rest1 = &data[pos + hb..];
    assert(rest1@ == data@.skip(p));
    let (cb, c) = <W::R as RleOps<'a>>::read_unsigned_opt(rest1).unwrap();
    proof {
        lemma_count::<W::R>(data@.skip(p), g->Null_count as u64, rest, cb as int, c);
    }
    let count = c as usize;
    if at - item <= count {
        let k = at - item;
        let rs = RleDecoderState { byte_pos: pos + hb + cb, remaining: count - k, run: SuspendedRun::Null };
        proof {
            lemma_land_null::<W>(data@, gi, k as int, State::Null(k));
            assert(state_at::<W::R>(data@, rs, gi + 1));
            let g2 = choose|g3: int| state_at::<W::R>(data@, rs, g3);
            lemma_state_unique::<W::R>(data@, rs, g2, gi + 1);
        }
        let read_segments = if k == count { segments + 1 } else { segments };
        return Walk::Landed(PartitionAt { state: State::Null(k), segments, byte_start: pos, read_segments, read_state: rs });
    }
    proof {
        lemma_gseg(gs, gi);
        assert(group_ok::<W::R>(g));
        assert(group_bytes::<W::R>(g) == <W::R as RleOps<'a>>::enc_signed(0) + <W::R as RleOps<'a>>::enc_unsigned(c));
        assert(pos + hb + cb == gb::<W::R>(gs, gi + 1));
        assert(item + count == gl(gs, gi + 1));
        lemma_slab::<W::R>(data@);
        if gi + 1 == gs.len() {
            assert(gs.take(gi + 1) =~= gs);
        }
    }
    Walk::Next { pos: pos + hb + cb, item: item + count, segments: segments + 1, prev_lit: None }
}

/// A literal run: land in it, or step past it.
#[verifier::spinoff_prover]
#[verifier::rlimit(150)]
fn walk_lit<'a, W: WriteOps<'a>>(data: &'a [u8], at: usize, Ghost(gi): Ghost<int>, pos: usize, item: usize, segments: usize, hb: usize, total: usize) -> (r: Walk<'a, W>)
    requires
        walk_at::<W::R>(data@, at as int, gi, pos as int, item as int, segments as int),
        slab_groups::<W::R>(data@)[gi] is Lit, total == group_len(slab_groups::<W::R>(data@)[gi]),
        hb == lit_head::<W::R>(total as nat).len(),
        pos + hb <= data@.len(),
    ensures walk_post::<W>(data@, at as int, gi, r),
{
    hide(canonical);
    hide(gseg);
    hide(group_ok);
    let ghost gs = slab_groups::<W::R>(data@);
    let ghost g = gs[gi];
    let ghost vs = g->values;
    proof {
        lemma_slab_group::<W::R>(data@, gi);
        assert(vs.take(0) =~= Seq::<VOf<'a, W>>::empty());
    }
    let start = pos;
    let mut q = pos + hb;
    let mut j: usize = 0;
    let stop = if at - item <= total { at - item - 1 } else { total };
    while j < stop
        invariant
            valid_slab::<W::R>(data@), gs == slab_groups::<W::R>(data@), data@.len() <= usize::MAX,
            0 <= gi < gs.len(), g == gs[gi], g is Lit, vs == g->values, vs.len() == total,
            j <= stop, stop <= total, start == gb::<W::R>(gs, gi),
            q == gb::<W::R>(gs, gi) + lit_head::<W::R>(vs.len()).len() + encs::<W::R>(vs.take(j as int)).len(),
        decreases stop - j,
    {
        let ghost after = encs::<W::R>(vs.skip(j + 1)) + groups_bytes::<W::R>(gs.skip(gi + 1));
        proof {
            lemma_slab_lit::<W::R>(data@, gi, j as int);
            lemma_starts(<W::R as RleOps<'a>>::enc(vs[j as int]), after);
        }
        let rest2 = &data[q..];
        assert(rest2@ == data@.skip(q as int));
        let vl = <W::R as RleOps<'a>>::value_len(rest2).unwrap();
        q = q + vl;
        j = j + 1;
    }
    if at - item <= total {
        let jj = stop;
        let ghost after = encs::<W::R>(vs.skip(jj + 1)) + groups_bytes::<W::R>(gs.skip(gi + 1));
        proof {
            lemma_slab_lit::<W::R>(data@, gi, jj as int);
            lemma_starts(<W::R as RleOps<'a>>::enc(vs[jj as int]), after);
        }
        let rest3 = &data[q..];
        assert(rest3@ == data@.skip(q as int));
        let (vlen, value) = <W::R as RleOps<'a>>::unpack(rest3);
        proof {
            lemma_value::<W::R>(data@.skip(q as int), vs[jj as int], after, vlen as int, value);
        }
        let state = State::lit(jj, W::cow_ref(value), start);
        let byte_start = if jj == 0 { start } else { q };
        let rs = RleDecoderState { byte_pos: q + vlen, remaining: total - jj - 1, run: SuspendedRun::Literal };
        proof {
            assert(state_at::<W::R>(data@, rs, gi + 1)) by {
                reveal(gseg);
                assert(vs.take(vs.len() - rs.remaining) =~= vs.take(jj + 1));
            }
            lemma_rsegs_is::<W::R>(data@, rs, gi + 1);
            assert(rsegs_at::<W::R>(data@, rs, gi + 1) == segments + jj + 1) by {
                reveal(gseg);
            }
            lemma_land_lit::<W>(data@, gi, jj as int, state, byte_start as int);
            assert(vs.take(vs.len() - rs.remaining) =~= vs.take(jj + 1));
            assert(state_at::<W::R>(data@, rs, gi + 1));
            let g2 = choose|g3: int| state_at::<W::R>(data@, rs, g3);
            lemma_state_unique::<W::R>(data@, rs, g2, gi + 1);
        }
        return Walk::Landed(PartitionAt { state, segments: segments + jj, byte_start, read_segments: segments + jj + 1, read_state: rs });
    }
    proof {
        lemma_gseg(gs, gi);
        assert(vs.take(total as int) =~= vs);
        assert(group_bytes::<W::R>(g) == lit_head::<W::R>(total as nat) + encs::<W::R>(vs));
        assert(q == gb::<W::R>(gs, gi + 1));
        assert(item + total == gl(gs, gi + 1));
        lemma_slab::<W::R>(data@);
        if gi + 1 == gs.len() {
            assert(gs.take(gi + 1) =~= gs);
        }
    }
    Walk::Next { pos: q, item: item + total, segments: segments + total, prev_lit: Some((start, total)) }
}

/// Open a rebuild of the valid slab `data` at item `at`.
pub(crate) fn partition_at<'a, W: WriteOps<'a>>(data: &'a [u8], at: usize) -> (p: PartitionAt<'a, W>)
    requires
        valid_slab::<W::R>(data@), data@.len() < i64::MAX,
        at <= slab_values::<W::R>(data@).len(),
    ensures
        p.byte_start <= data@.len(), p.byte_start <= sbyte(p.read_state),
        p.read_segments == rsegs::<W::R>(data@, p.read_state),
        state_ok::<W::R>(data@, p.read_state), state_pos::<W::R>(data@, p.read_state) == at,
        landed::<W>(data@, at as int, p.state, p.byte_start as int, p.segments as int),
{
    let ghost gs = slab_groups::<W::R>(data@);
    proof {
        lemma_slab::<W::R>(data@);
    }
    let _len = data.len();
    if at == 0 {
        let rs = RleDecoderState::start();
        proof {
            assert(gs.take(0) =~= Seq::<Group<VOf<'a, W>>>::empty());
            assert(gb::<W::R>(gs, 0) == 0);
            assert(state_at::<W::R>(data@, rs, 0));
            let g = choose|gi: int| state_at::<W::R>(data@, rs, gi);
            lemma_state_unique::<W::R>(data@, rs, g, 0);
            let e = Seq::<Group<VOf<'a, W>>>::empty();
            assert(data@.take(0) =~= Seq::<u8>::empty());
            assert(rel(State::<'a, W>::Empty, data@.take(0), Seq::empty(), 0, e, Seq::empty(), 0));
            assert(slab_values::<W::R>(data@).take(0) =~= values_of(State::<'a, W>::Empty, e, Seq::empty()));
        }
        return PartitionAt { state: State::Empty, segments: 0, byte_start: 0, read_segments: 0, read_state: rs };
    }
    let mut pos: usize = 0;
    let mut item: usize = 0;
    let mut segments: usize = 0;
    let mut prev_lit: Option<(usize, usize)> = None;
    let ghost mut gi: int = 0;
    proof {
        assert(gs.take(0) =~= Seq::<Group<VOf<'a, W>>>::empty());
        if gs.len() == 0 {
            assert(gs.take(0) =~= gs);
        }
    }
    loop
        invariant
            walk_at::<W::R>(data@, at as int, gi, pos as int, item as int, segments as int), gs == slab_groups::<W::R>(data@),
            prev_lit == prev_lit_of::<W::R>(data@, gi),
        decreases gs.len() - gi,
    {
        proof {
            lemma_slab_group::<W::R>(data@, gi);
        }
        let rest0 = &data[pos..];
        assert(rest0@ == data@.skip(pos as int));
        proof {
            let g = gs[gi];
            lemma_starts(<W::R as RleOps<'a>>::enc_signed(header_of(g) as i64), payload::<W::R>(g) + groups_bytes::<W::R>(gs.skip(gi + 1)));
        }
        let (hb, hv) = <W::R as RleOps<'a>>::read_signed_opt(rest0).unwrap();
        proof {
            lemma_walk_header::<W::R>(data@, gi, pos as int, hb as int, hv);
        }
        let w = if hv > 0 {
            walk_run::<W>(data, at, Ghost(gi), pos, item, segments, prev_lit, hb, hv as usize)
        } else if hv < 0 {
            walk_lit::<W>(data, at, Ghost(gi), pos, item, segments, hb, (-hv) as usize)
        } else {
            walk_null::<W>(data, at, Ghost(gi), pos, item, segments, hb)
        };
        match w {
            Walk::Landed(p) => return p,
            Walk::Next { pos: p2, item: i2, segments: s2, prev_lit: pl } => {
                pos = p2;
                item = i2;
                segments = s2;
                prev_lit = pl;
                proof {
                    gi = gi + 1;
                }
            },
        }
    }
}

// ── The end of a slab ───────────────────────────────────────────────────────

/// Group `gi` of `data` is its last, at `pos`, and its header of `hb`
/// bytes is read.
pub(crate) open spec fn last_group_at<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int, pos: int, hb: int) -> bool {
    let gs = slab_groups::<O>(data);
    &&& valid_slab::<O>(data) && data.len() < i64::MAX && data.len() <= usize::MAX
    &&& gs.len() > 0 && gi == gs.len() - 1 && pos == gb::<O>(gs, gi)
    &&& hb == O::enc_signed(header_of(gs[gi]) as i64).len() && pos + hb <= data.len()
    &&& data.skip(pos + hb) == payload::<O>(gs[gi])
    &&& gseg(gs, gi) + group_segments(gs[gi]) == groups_segments(gs)
    &&& gb::<O>(gs, gi) + group_bytes::<O>(gs[gi]).len() == data.len()
}

/// The end of a slab whose last group is a repeat run.
#[verifier::spinoff_prover]
fn tail_run_state<'a, W: WriteOps<'a>>(data: &'a [u8], pos: usize, hb: usize, count: usize, Ghost(gi): Ghost<int>) -> (r: State<'a, W>)
    requires
        last_group_at::<W::R>(data@, gi, pos as int, hb as int),
        slab_groups::<W::R>(data@)[gi] is Run, count as int == header_of(slab_groups::<W::R>(data@)[gi]),
    ensures
        landed::<W>(data@, slab_values::<W::R>(data@).len() as int, r, pos as int,
            groups_segments(slab_groups::<W::R>(data@)) - 1),
{
    hide(canonical);
    let ghost gs = slab_groups::<W::R>(data@);
    let ghost g = gs[gi];
    proof {
        lemma_slab::<W::R>(data@);
        lemma_slab_group::<W::R>(data@, gi);
        lemma_starts(<W::R as RleOps<'a>>::enc(run_value(g)), Seq::empty());
        assert(<W::R as RleOps<'a>>::enc(run_value(g)) + Seq::<u8>::empty() =~= payload::<W::R>(g));
    }
    let vrest = &data[pos + hb..];
    assert(vrest@ == data@.skip(pos + hb));
    let (_vlen, value) = <W::R as RleOps<'a>>::unpack(vrest);
    proof {
        lemma_value::<W::R>(data@.skip(pos + hb), run_value(g), Seq::empty(), _vlen as int, value);
    }
    let s = State::make_run(count, W::cow_ref(value));
    proof {
        lemma_land_run::<W>(data@, gi, count as int, s);
        assert(gl(gs, gi) + count == slab_values::<W::R>(data@).len());
    }
    s
}

/// The end of a slab whose last group is a null run.
#[verifier::spinoff_prover]
fn tail_null_state<'a, W: WriteOps<'a>>(data: &'a [u8], pos: usize, hb: usize, Ghost(gi): Ghost<int>) -> (r: State<'a, W>)
    requires
        last_group_at::<W::R>(data@, gi, pos as int, hb as int),
        slab_groups::<W::R>(data@)[gi] is Null,
    ensures
        landed::<W>(data@, slab_values::<W::R>(data@).len() as int, r, pos as int,
            groups_segments(slab_groups::<W::R>(data@)) - 1),
{
    hide(canonical);
    let ghost gs = slab_groups::<W::R>(data@);
    let ghost g = gs[gi];
    let ghost cb = <W::R as RleOps<'a>>::enc_unsigned(g->Null_count as u64);
    proof {
        lemma_slab::<W::R>(data@);
        lemma_slab_group::<W::R>(data@, gi);
        lemma_starts(cb, Seq::empty());
        assert(cb + Seq::<u8>::empty() =~= payload::<W::R>(g));
    }
    let crest = &data[pos + hb..];
    assert(crest@ == data@.skip(pos + hb));
    let (_n, c) = <W::R as RleOps<'a>>::read_unsigned_opt(crest).unwrap();
    proof {
        lemma_count::<W::R>(data@.skip(pos + hb), g->Null_count as u64, Seq::empty(), _n as int, c);
    }
    let s = State::Null(c as usize);
    proof {
        lemma_land_null::<W>(data@, gi, c as int, s);
        assert(gl(gs, gi) + c == slab_values::<W::R>(data@).len());
    }
    s
}

/// The end of a slab whose last group is a literal of `n` values, the
/// last `tl` bytes wide: the state, and where its bytes start.
#[verifier::spinoff_prover]
#[verifier::rlimit(100)]
fn tail_lit_state<'a, W: WriteOps<'a>>(data: &'a [u8], pos: usize, Ghost(hb): Ghost<int>, n: usize, tl: usize, Ghost(gi): Ghost<int>) -> (r: (State<'a, W>, usize))
    requires
        last_group_at::<W::R>(data@, gi, pos as int, hb),
        slab_groups::<W::R>(data@)[gi] is Lit, n as int == -header_of(slab_groups::<W::R>(data@)[gi]),
        tl == lit_tail_of::<W::R>(slab_groups::<W::R>(data@)),
    ensures
        r.1 <= data@.len(),
        landed::<W>(data@, slab_values::<W::R>(data@).len() as int, r.0, r.1 as int,
            groups_segments(slab_groups::<W::R>(data@)) - 1),
{
    hide(canonical);
    hide(gseg);
    let ghost gs = slab_groups::<W::R>(data@);
    let ghost g = gs[gi];
    let ghost vs = g->values;
    proof {
        lemma_slab::<W::R>(data@);
        lemma_slab_group::<W::R>(data@, gi);
        lemma_slab_lit::<W::R>(data@, gi, n - 1);
        assert(gs.last() == g);
        assert(vs.take(n as int) =~= vs);
        assert(vs.skip(n as int) =~= Seq::<VOf<'a, W>>::empty());
        assert(gs.skip(gi + 1) =~= Seq::<Group<VOf<'a, W>>>::empty());
        assert(groups_bytes::<W::R>(gs.skip(gi + 1)) =~= Seq::<u8>::empty());
        assert(encs::<W::R>(vs.skip(n as int)) =~= Seq::<u8>::empty());
        lemma_starts(<W::R as RleOps<'a>>::enc(vs[n - 1]), Seq::empty());
    }
    let value_pos = data.len() - tl;
    let vrest = &data[value_pos..];
    assert(vrest@ == data@.skip(value_pos as int));
    let (_vlen, value) = <W::R as RleOps<'a>>::unpack(vrest);
    proof {
        assert(encs::<W::R>(vs.skip(n as int)) + groups_bytes::<W::R>(gs.skip(gi + 1)) =~= Seq::<u8>::empty());
        lemma_value::<W::R>(data@.skip(value_pos as int), vs[n - 1], Seq::empty(), _vlen as int, value);
    }
    let s = State::lit(n - 1, W::cow_ref(value), pos);
    let bs = if n == 1 { pos } else { value_pos };
    proof {
        lemma_land_lit::<W>(data@, gi, n - 1, s, bs as int);
        assert(gl(gs, gi) + n == slab_values::<W::R>(data@).len());
        assert(gseg(gs, gi) + n - 1 == groups_segments(gs) - 1);
    }
    (s, bs)
}

/// The writer state at the end of the valid slab `data`, read from its
/// tail record (`tb`, `tl`) rather than by walking: the state, where its
/// bytes start, and the segments of its last group it stands for (1, or
/// 0 for an empty slab).
#[verifier::spinoff_prover]
pub(crate) fn tail_state<'a, W: WriteOps<'a>>(data: &'a [u8], tb: usize, tl: usize) -> (r: (State<'a, W>, usize, usize))
    requires
        valid_slab::<W::R>(data@), data@.len() < i64::MAX,
        tb == tail_bytes_of::<W::R>(slab_groups::<W::R>(data@)),
        tl == lit_tail_of::<W::R>(slab_groups::<W::R>(data@)),
    ensures
        r.1 <= data@.len(), r.2 <= groups_segments(slab_groups::<W::R>(data@)),
        landed::<W>(data@, slab_values::<W::R>(data@).len() as int, r.0, r.1 as int,
            groups_segments(slab_groups::<W::R>(data@)) - r.2),
{
    hide(canonical);
    let ghost gs = slab_groups::<W::R>(data@);
    proof {
        lemma_slab::<W::R>(data@);
    }
    let len = data.len();
    if len == 0 {
        proof {
            if gs.len() > 0 {
                lemma_slab_group::<W::R>(data@, 0);
            }
            let e = Seq::<Group<VOf<'a, W>>>::empty();
            assert(gs =~= e);
            assert(data@.take(0) =~= Seq::<u8>::empty());
            reveal(canonical);
            assert(rel(State::<'a, W>::Empty, data@.take(0), Seq::empty(), 0, e, Seq::empty(), 0));
            assert(slab_values::<W::R>(data@).take(0) =~= values_of(State::<'a, W>::Empty, e, Seq::empty()));
        }
        return (State::Empty, 0, 0);
    }
    let ghost gi = gs.len() - 1;
    proof {
        if gs.len() == 0 {
            assert(groups_bytes::<W::R>(gs).len() == 0);
        }
        lemma_slab_group::<W::R>(data@, gi);
        assert(gs.take(gi + 1) =~= gs);
        assert(gs.last() == gs[gi]);
        lemma_gseg(gs, gi);
        assert(gs.skip(gi + 1) =~= Seq::<Group<VOf<'a, W>>>::empty());
        assert(groups_bytes::<W::R>(gs.skip(gi + 1)) =~= Seq::<u8>::empty());
        super::read::lemma_segments_le::<W::R>(gs);
        lemma_group_segments_pos::<W::R>(gs[gi]);
    }
    let pos = len - tb;
    let (hb, hv) = group_header::<W::R>(data, pos, Ghost(gi));
    proof {
        assert(payload::<W::R>(gs[gi]) + Seq::<u8>::empty() =~= payload::<W::R>(gs[gi]));
    }
    if hv > 0 {
        let s = tail_run_state::<W>(data, pos, hb, hv as usize, Ghost(gi));
        (s, pos, 1)
    } else if hv == 0 {
        let s = tail_null_state::<W>(data, pos, hb, Ghost(gi));
        (s, pos, 1)
    } else {
        let (s, bs) = tail_lit_state::<W>(data, pos, Ghost(hb as int), (-hv) as usize, tl, Ghost(gi));
        (s, bs, 1)
    }
}

/// A group has a segment at least.
proof fn lemma_group_segments_pos<'a, O: RleOps<'a>>(g: Group<O::V>)
    requires group_ok::<O>(g),
    ensures group_segments(g) >= 1,
{
}

// ── What follows a reader ───────────────────────────────────────────────────

/// The header of group `gi`, which starts at `pos`.
fn group_header<'a, O: RleOps<'a>>(data: &'a [u8], pos: usize, Ghost(gi): Ghost<int>) -> (r: (usize, i64))
    requires
        valid_slab::<O>(data@), 0 <= gi < slab_groups::<O>(data@).len(),
        pos == gb::<O>(slab_groups::<O>(data@), gi),
    ensures ({
        let gs = slab_groups::<O>(data@);
        let g = gs[gi];
        &&& r.1 as int == header_of(g) && r.0 == O::enc_signed(header_of(g) as i64).len()
        &&& pos + r.0 <= data@.len()
        &&& data@.skip(pos + r.0) == payload::<O>(g) + groups_bytes::<O>(gs.skip(gi + 1))
    }),
{
    proof {
        let gs = slab_groups::<O>(data@);
        let g = gs[gi];
        lemma_slab_group::<O>(data@, gi);
        lemma_starts(O::enc_signed(header_of(g) as i64), payload::<O>(g) + groups_bytes::<O>(gs.skip(gi + 1)));
    }
    let rest = &data[pos..];
    assert(rest@ == data@.skip(pos as int));
    let (hb, hv) = O::read_signed_opt(rest).unwrap();
    proof {
        lemma_walk_header::<O>(data@, gi, pos as int, hb as int, hv);
    }
    (hb, hv)
}

/// Value `j` of literal group `gi`, which starts at `pos`: the value,
/// how many follow it, and where they start.
fn lit_value_at<'a, O: RleOps<'a>>(data: &'a [u8], pos: usize, Ghost(gi): Ghost<int>, Ghost(j): Ghost<int>, left: usize) -> (r: (O::V, usize, usize))
    requires
        valid_slab::<O>(data@), data@.len() <= usize::MAX, 0 <= gi < slab_groups::<O>(data@).len(),
        slab_groups::<O>(data@)[gi] is Lit,
        0 <= j < slab_groups::<O>(data@)[gi]->values.len(),
        left + j == slab_groups::<O>(data@)[gi]->values.len(),
        pos == gb::<O>(slab_groups::<O>(data@), gi) + lit_head::<O>(slab_groups::<O>(data@)[gi]->values.len()).len()
            + encs::<O>(slab_groups::<O>(data@)[gi]->values.take(j)).len(),
    ensures
        pos <= r.2 <= data@.len(),
        tail_lit::<O>(data@, gl(slab_groups::<O>(data@), gi) + j, r.0, r.1 as nat, r.2 as int, gi, j),
{
    let ghost gs = slab_groups::<O>(data@);
    let ghost vs = gs[gi]->values;
    let ghost after = encs::<O>(vs.skip(j + 1)) + groups_bytes::<O>(gs.skip(gi + 1));
    proof {
        lemma_slab_lit::<O>(data@, gi, j);
        lemma_starts(O::enc(vs[j]), after);
    }
    let rest = &data[pos..];
    assert(rest@ == data@.skip(pos as int));
    let (vlen, value) = O::unpack(rest);
    proof {
        lemma_value::<O>(data@.skip(pos as int), vs[j], after, vlen as int, value);
    }
    (value, left - 1, pos + vlen)
}

/// The first value of literal group `gi`, whose `hb`-byte header (of
/// `hv`) starts at `pos`.
fn first_lit<'a, O: RleOps<'a>>(data: &'a [u8], pos: usize, hb: usize, hv: i64, Ghost(gi): Ghost<int>) -> (r: (O::V, usize, usize))
    requires
        valid_slab::<O>(data@), data@.len() <= usize::MAX, 0 <= gi < slab_groups::<O>(data@).len(),
        pos == gb::<O>(slab_groups::<O>(data@), gi),
        slab_groups::<O>(data@)[gi] is Lit, hv as int == header_of(slab_groups::<O>(data@)[gi]),
        hb == O::enc_signed(hv).len(), pos + hb <= data@.len(),
    ensures
        pos <= r.2 <= data@.len(),
        tail_lit::<O>(data@, gl(slab_groups::<O>(data@), gi), r.0, r.1 as nat, r.2 as int, gi, 0),
{
    let ghost gs = slab_groups::<O>(data@);
    let ghost vs = gs[gi]->values;
    proof {
        lemma_slab_group::<O>(data@, gi);
        assert(vs.take(0) =~= Seq::<O::V>::empty());
        assert(encs::<O>(vs.take(0)).len() == 0);
    }
    let total = (-hv) as usize;
    lit_value_at::<O>(data, pos + hb, Ghost(gi), Ghost(0), total)
}

/// What follows group boundary `gi`, at byte `pos`: inlined, as
/// merging slabs reads a slab's head with it.
#[inline(always)]
fn postfix_at_group<'a, O: RleOps<'a>>(data: &'a [u8], pos: usize, Ghost(gi): Ghost<int>) -> (r: (Option<Postfix<O::V>>, usize))
    requires
        valid_slab::<O>(data@), data@.len() <= usize::MAX, 0 <= gi <= slab_groups::<O>(data@).len(),
        pos == gb::<O>(slab_groups::<O>(data@), gi),
    ensures
        r.1 <= data@.len(), pos <= r.1, postfix_ok::<O>(data@, gl(slab_groups::<O>(data@), gi), r.0, r.1 as int),
        gseg(slab_groups::<O>(data@), gi) + ppend(r.0) + rest_segs::<O>(data@, gl(slab_groups::<O>(data@), gi), r.0, r.1 as int)
            == groups_segments(slab_groups::<O>(data@)),
{
    let ghost gs = slab_groups::<O>(data@);
    proof {
        lemma_slab::<O>(data@);
        if gi < gs.len() {
            lemma_slab_group::<O>(data@, gi);
        } else {
            assert(gs.take(gi) =~= gs);
        }
    }
    if pos == data.len() {
        return (None, pos);
    }
    let ghost g = gs[gi];
    let ghost rest = groups_bytes::<O>(gs.skip(gi + 1));
    let (hb, hv) = group_header::<O>(data, pos, Ghost(gi));
    if hv < 0 {
        let (value, lit, end) = first_lit::<O>(data, pos, hb, hv, Ghost(gi));
        proof {
            lemma_postfix_lit::<O>(data@, gl(gs, gi), value, lit, end as int, gi, 0);
            lemma_rest_lit::<O>(data@, gl(gs, gi), value, lit, end as int, gi, 0);
            lemma_gseg(gs, gi);
            lemma_gseg_split::<O>(data@, gi + 1);
        }
        return (Some(Postfix::Lit { value, lit }), end);
    }
    if hv > 0 {
        proof {
            lemma_starts(O::enc(run_value(g)), rest);
        }
        let vrest = &data[pos + hb..];
        assert(vrest@ == data@.skip(pos + hb));
        let (vlen, value) = O::unpack(vrest);
        proof {
            lemma_value::<O>(data@.skip(pos + hb), run_value(g), rest, vlen as int, value);
        }
        let count = hv as usize;
        let end = pos + hb + vlen;
        let p = Some(Postfix::Run { count, value });
        proof {
            assert(count as nat == group_len(g));
            assert(tail_run::<O>(data@, gl(gs, gi), count as nat, value, end as int, gi));
            lemma_postfix_run::<O>(data@, gl(gs, gi), count, value, end as int, gi);
            lemma_rest_run::<O>(data@, gl(gs, gi), count, value, end as int, gi);
            lemma_gseg(gs, gi);
            lemma_gseg_split::<O>(data@, gi + 1);
        }
        return (p, end);
    }
    proof {
        lemma_starts(O::enc_unsigned(g->Null_count as u64), rest);
    }
    let crest = &data[pos + hb..];
    assert(crest@ == data@.skip(pos + hb));
    let (cb, c) = O::read_unsigned_opt(crest).unwrap();
    proof {
        lemma_count::<O>(data@.skip(pos + hb), g->Null_count as u64, rest, cb as int, c);
    }
    let value = O::get_null();
    let count = c as usize;
    let end = pos + hb + cb;
    proof {
        assert(count as nat == group_len(g));
        assert(tail_run::<O>(data@, gl(gs, gi), count as nat, value, end as int, gi));
        lemma_postfix_run::<O>(data@, gl(gs, gi), count, value, end as int, gi);
        lemma_rest_run::<O>(data@, gl(gs, gi), count, value, end as int, gi);
        lemma_gseg(gs, gi);
        lemma_gseg_split::<O>(data@, gi + 1);
    }
    (Some(Postfix::Run { count, value }), end)
}

/// Inside repeat run `gi - 1`, `r` values left, its value at `vat`, the
/// next group at `pos`.
fn postfix_in_run<'a, O: RleOps<'a>>(data: &'a [u8], pos: usize, r: usize, vat: usize, Ghost(gi): Ghost<int>) -> (p: (Option<Postfix<O::V>>, usize))
    requires
        valid_slab::<O>(data@), data@.len() <= usize::MAX, 1 <= gi <= slab_groups::<O>(data@).len(),
        slab_groups::<O>(data@)[gi - 1] is Run,
        1 <= r <= slab_groups::<O>(data@)[gi - 1]->Run_count,
        pos == gb::<O>(slab_groups::<O>(data@), gi),
        vat == gb::<O>(slab_groups::<O>(data@), gi - 1)
            + O::enc_signed(slab_groups::<O>(data@)[gi - 1]->Run_count as i64).len(),
    ensures
        p.1 <= data@.len(), pos <= p.1, postfix_ok::<O>(data@, gl(slab_groups::<O>(data@), gi) - r, p.0, p.1 as int),
        gseg(slab_groups::<O>(data@), gi - 1) + ppend(p.0) + rest_segs::<O>(data@, gl(slab_groups::<O>(data@), gi) - r, p.0, p.1 as int)
            == groups_segments(slab_groups::<O>(data@)),
{
    let ghost gs = slab_groups::<O>(data@);
    let ghost g = gs[gi - 1];
    let ghost c = g->Run_count;
    let ghost rest = groups_bytes::<O>(gs.skip(gi));
    let ghost at = gl(gs, gi) - r;
    proof {
        lemma_slab::<O>(data@);
        lemma_slab_group::<O>(data@, gi - 1);
        let h = O::enc_signed(c as i64);
        lemma_skip_skip(data@, gb::<O>(gs, gi - 1), h.len() as int);
        assert((h + (O::enc(run_value(g)) + rest)).skip(h.len() as int) =~= O::enc(run_value(g)) + rest);
        lemma_starts(O::enc(run_value(g)), rest);
        if gi < gs.len() {
            lemma_slab_group::<O>(data@, gi);
        } else {
            assert(gs.take(gi) =~= gs);
        }
    }
    let vrest = &data[vat..];
    assert(vrest@ == data@.skip(vat as int));
    let (_vlen, value) = O::unpack(vrest);
    proof {
        lemma_value::<O>(data@.skip(vat as int), run_value(g), rest, _vlen as int, value);
    }
    if r == 1 && pos < data.len() {
        // one value left of the run: a literal after it takes it in
        let (hb, hv) = group_header::<O>(data, pos, Ghost(gi));
        if hv < 0 {
            let (v2, lit, end) = first_lit::<O>(data, pos, hb, hv, Ghost(gi));
            let p = Some(Postfix::LonePlusLit { lone: value, value: v2, lit });
            proof {
                assert(tail_lone::<O>(data@, at, value, v2, lit as nat, end as int, gi - 1));
                lemma_postfix_lone::<O>(data@, at, value, v2, lit, end as int, gi - 1);
                lemma_rest_lone::<O>(data@, at, value, v2, lit, end as int, gi - 1);
                lemma_gseg(gs, gi - 1);
                lemma_gseg(gs, gi);
                lemma_gseg_split::<O>(data@, gi + 1);
                lemma_slab_group::<O>(data@, gi);
            }
            return (p, end);
        }
    }
    proof {
        assert(tail_run::<O>(data@, at, r as nat, value, pos as int, gi - 1));
        lemma_postfix_run::<O>(data@, at, r, value, pos as int, gi - 1);
        lemma_rest_run::<O>(data@, at, r, value, pos as int, gi - 1);
        lemma_gseg(gs, gi - 1);
        lemma_gseg_split::<O>(data@, gi);
    }
    (Some(Postfix::Run { count: r, value }), pos)
}

/// The first run of the valid slab `data` (or its first literal value),
/// and where its bytes after that start.
pub(crate) fn head_postfix<'a, O: RleOps<'a>>(data: &'a [u8]) -> (r: (Option<Postfix<O::V>>, usize))
    requires valid_slab::<O>(data@),
    ensures
        r.1 <= data@.len(), postfix_ok::<O>(data@, 0, r.0, r.1 as int),
        ppend(r.0) + rest_segs::<O>(data@, 0, r.0, r.1 as int) == groups_segments(slab_groups::<O>(data@)),
{
    let _len = data.len();
    proof {
        let gs = slab_groups::<O>(data@);
        assert(gs.take(0) =~= Seq::<Group<O::V>>::empty());
        assert(gb::<O>(gs, 0) == 0 && gl(gs, 0) == 0 && gseg(gs, 0) == 0);
    }
    postfix_at_group::<O>(data, 0, Ghost(0))
}

/// What follows reader position `st` in `data`: its first run (or
/// literal value) as values, and where the slab's bytes after it start.
pub(crate) fn postfix_at<'a, O: RleOps<'a>>(data: &'a [u8], st: &RleDecoderState) -> (r: (Option<Postfix<O::V>>, usize))
    requires state_ok::<O>(data@, *st),
    ensures
        r.1 <= data@.len(), sbyte(*st) <= r.1,
        postfix_ok::<O>(data@, state_pos::<O>(data@, *st), r.0, r.1 as int),
        rsegs::<O>(data@, *st) + ppend(r.0) + rest_segs::<O>(data@, state_pos::<O>(data@, *st), r.0, r.1 as int)
            == groups_segments(slab_groups::<O>(data@)),
{
    let ghost gs = slab_groups::<O>(data@);
    let ghost gi = choose|g: int| state_at::<O>(data@, *st, g);
    let ghost at = state_pos::<O>(data@, *st);
    let r = st.remaining;
    if r == 0 {
        proof {
            if st.run is Literal {
                let vs = gs[gi - 1]->values;
                lemma_slab_group::<O>(data@, gi - 1);
                assert(vs.take(vs.len() as int) =~= vs);
            }
            lemma_rsegs_end::<O>(data@, *st, gi);
        }
        return postfix_at_group::<O>(data, st.byte_pos, Ghost(gi));
    }
    match st.run {
        SuspendedRun::Repeat { at: vat } => postfix_in_run::<O>(data, st.byte_pos, r, vat, Ghost(gi)),
        SuspendedRun::Null => {
            proof {
                lemma_slab_group::<O>(data@, gi - 1);
            }
            let value = O::get_null();
            proof {
                assert(tail_run::<O>(data@, at, r as nat, value, st.byte_pos as int, gi - 1));
                lemma_postfix_run::<O>(data@, at, r, value, st.byte_pos as int, gi - 1);
                lemma_rest_run::<O>(data@, at, r, value, st.byte_pos as int, gi - 1);
                lemma_gseg(gs, gi - 1);
                lemma_gseg_split::<O>(data@, gi);
            }
            (Some(Postfix::Run { count: r, value }), st.byte_pos)
        },
        SuspendedRun::Literal => {
            let ghost vs = gs[gi - 1]->values;
            proof {
                lemma_slab_group::<O>(data@, gi - 1);
            }
            let (value, lit, end) = lit_value_at::<O>(data, st.byte_pos, Ghost(gi - 1), Ghost(vs.len() - r), r);
            proof {
                assert(tail_lit::<O>(data@, at, value, lit as nat, end as int, gi - 1, vs.len() - r));
                lemma_postfix_lit::<O>(data@, at, value, lit, end as int, gi - 1, vs.len() - r);
                lemma_rest_lit::<O>(data@, at, value, lit, end as int, gi - 1, vs.len() - r);
                lemma_gseg(gs, gi - 1);
                lemma_gseg_split::<O>(data@, gi);
            }
            (Some(Postfix::Lit { value, lit }), end)
        },
        SuspendedRun::Idle => {
            proof {
                assert(false);
            }
            (None, 0)
        },
    }
}

// ── Reader positions, read with owned values ───────────────────────────────

/// Group `gi` of a slab is where `st` stands, read either way.
proof fn lemma_lift_state_at<'s, L: Lift<'s>>(data: Seq<u8>, st: RleDecoderState, gi: int)
    requires valid_slab::<L::R>(data),
    ensures state_at::<L::R>(data, st, gi) == state_at::<WR<'s, L>>(data, st, gi),
{
    let gs = slab_groups::<L::R>(data);
    lemma_lift_slab::<L>(data);
    if 0 <= gi <= gs.len() {
        lemma_lift_at::<L>(gs, gi);
        if gi >= 1 {
            lemma_lift_at::<L>(gs, gi - 1);
            let g = gs[gi - 1];
            match g {
                Group::Run { count, .. } => {
                    L::axiom_lift_codec(count as i64, 0);
                },
                Group::Lit { values } => {
                    L::axiom_lift_codec(-(values.len() as int) as i64, 0);
                    let k = values.len() - st.remaining;
                    if 0 <= k <= values.len() {
                        assert(lift_seq::<L>(values).take(k) =~= lift_seq::<L>(values.take(k)));
                        lemma_lift_encs::<L>(values.take(k));
                    }
                },
                _ => {},
            }
        }
    }
}

/// A reader position, read with owned values: the same position.
pub(crate) proof fn lemma_lift_state<'s, L: Lift<'s>>(data: Seq<u8>, st: RleDecoderState)
    requires valid_slab::<L::R>(data),
    ensures
        state_ok::<L::R>(data, st) == state_ok::<WR<'s, L>>(data, st),
        state_ok::<L::R>(data, st) ==> state_pos::<L::R>(data, st) == state_pos::<WR<'s, L>>(data, st),
        state_ok::<L::R>(data, st) ==> rsegs::<L::R>(data, st) == rsegs::<WR<'s, L>>(data, st),
{
    lemma_lift_slab::<L>(data);
    assert forall|gi: int| state_at::<L::R>(data, st, gi) == #[trigger] state_at::<WR<'s, L>>(data, st, gi) by {
        lemma_lift_state_at::<L>(data, st, gi);
    }
    if state_ok::<L::R>(data, st) {
        let g1 = choose|gi: int| state_at::<L::R>(data, st, gi);
        let g2 = choose|gi: int| state_at::<WR<'s, L>>(data, st, gi);
        assert(state_at::<L::R>(data, st, g1));
        assert(state_at::<WR<'s, L>>(data, st, g1));
        lemma_state_unique::<WR<'s, L>>(data, st, g1, g2);
        lemma_lift_at::<L>(slab_groups::<L::R>(data), g1);
        if g1 >= 1 {
            lemma_lift_at::<L>(slab_groups::<L::R>(data), g1 - 1);
            lemma_lift_group::<L>(slab_groups::<L::R>(data)[g1 - 1]);
        }
    }
}

/// Where a reader standing in group `gi` sits, in bytes and values.
proof fn lemma_state_bounds<'a, O: RleOps<'a>>(data: Seq<u8>, st: RleDecoderState, gi: int)
    requires valid_slab::<O>(data), state_at::<O>(data, st, gi),
    ensures ({
        let gs = slab_groups::<O>(data);
        let pos = gl(gs, gi) - st.remaining;
        &&& st.run is Idle ==> sbyte(st) == gb::<O>(gs, gi) && pos == gl(gs, gi)
        &&& !(st.run is Idle) ==> 1 <= gi && gb::<O>(gs, gi - 1) <= sbyte(st) <= gb::<O>(gs, gi)
            && gl(gs, gi - 1) <= pos <= gl(gs, gi)
        &&& (st.run is Repeat || st.run is Null) ==> sbyte(st) == gb::<O>(gs, gi)
        &&& st.run is Repeat ==> gs[gi - 1] is Run
        &&& st.run is Null ==> gs[gi - 1] is Null
        &&& st.run is Literal ==> gs[gi - 1] is Lit
    }),
{
    let gs = slab_groups::<O>(data);
    if !(st.run is Idle) {
        lemma_slab_group::<O>(data, gi - 1);
        lemma_group_values_len(gs[gi - 1], O::null());
        if st.run is Literal {
            let vs = gs[gi - 1]->values;
            lemma_encs_prefix_len::<O>(vs, vs.len() - st.remaining);
        }
    }
}

/// Reading on never moves back in the bytes.
#[verifier::spinoff_prover]
pub(crate) proof fn lemma_sbyte_mono<'a, O: RleOps<'a>>(data: Seq<u8>, s1: RleDecoderState, s2: RleDecoderState)
    requires
        state_ok::<O>(data, s1), state_ok::<O>(data, s2),
        state_pos::<O>(data, s1) < state_pos::<O>(data, s2),
    ensures sbyte(s1) <= sbyte(s2),
{
    let gs = slab_groups::<O>(data);
    let g1 = choose|gi: int| state_at::<O>(data, s1, gi);
    let g2 = choose|gi: int| state_at::<O>(data, s2, gi);
    assert(state_at::<O>(data, s1, g1));
    assert(state_at::<O>(data, s2, g2));
    lemma_state_bounds::<O>(data, s1, g1);
    lemma_state_bounds::<O>(data, s2, g2);
    assert(state_pos::<O>(data, s1) == gl(gs, g1) - s1.remaining);
    assert(state_pos::<O>(data, s2) == gl(gs, g2) - s2.remaining);
    if g1 < g2 {
        lemma_gb_lt::<O>(data, g1, g2);
        if g1 < g2 - 1 {
            lemma_gb_lt::<O>(data, g1, g2 - 1);
        }
        assert(sbyte(s1) <= gb::<O>(gs, g1));
        assert(sbyte(s1) <= sbyte(s2));
    } else if g1 == g2 {
        if s1.run is Literal && s2.run is Literal {
            let vs = gs[g1 - 1]->values;
            lemma_encs_prefix_le::<O>(vs, vs.len() - s1.remaining, vs.len() - s2.remaining);
            assert(sbyte(s1) <= sbyte(s2));
        } else {
            assert(sbyte(s1) <= sbyte(s2));
        }
    } else {
        lemma_gl_le::<O>(data, g2, g1 - 1);
        lemma_gl_le::<O>(data, g1 - 1, g1);
        assert(false);
    }
}

/// The bytes of a literal's first `j` values, and no more than all.
proof fn lemma_encs_prefix_len<'a, O: RleOps<'a>>(vs: Seq<O::V>, j: int)
    requires 0 <= j <= vs.len(),
    ensures encs::<O>(vs.take(j)).len() <= encs::<O>(vs).len(),
{
    assert(vs =~= vs.take(j) + vs.skip(j));
    lemma_encs_concat::<O>(vs.take(j), vs.skip(j));
}

proof fn lemma_encs_prefix_le<'a, O: RleOps<'a>>(vs: Seq<O::V>, i: int, j: int)
    requires 0 <= i <= j <= vs.len(),
    ensures encs::<O>(vs.take(i)).len() <= encs::<O>(vs.take(j)).len(),
{
    assert(vs.take(j) =~= vs.take(i) + vs.take(j).skip(i));
    lemma_encs_concat::<O>(vs.take(i), vs.take(j).skip(i));
}

/// Group value offsets do not decrease.
pub(crate) proof fn lemma_gl_le<'a, O: RleOps<'a>>(data: Seq<u8>, i: int, j: int)
    requires valid_slab::<O>(data), 0 <= i <= j <= slab_groups::<O>(data).len(),
    ensures gl(slab_groups::<O>(data), i) <= gl(slab_groups::<O>(data), j),
    decreases j - i,
{
    if i < j {
        lemma_slab_group::<O>(data, i);
        lemma_gl_le::<O>(data, i + 1, j);
    }
}

/// A reader that has read every value stands at the end of the bytes.
#[verifier::spinoff_prover]
pub(crate) proof fn lemma_sbyte_end<'a, O: RleOps<'a>>(data: Seq<u8>, st: RleDecoderState)
    requires state_ok::<O>(data, st), state_pos::<O>(data, st) == slab_values::<O>(data).len(),
    ensures sbyte(st) == data.len(),
{
    let gs = slab_groups::<O>(data);
    let g = choose|gi: int| state_at::<O>(data, st, gi);
    assert(state_at::<O>(data, st, g));
    lemma_state_bounds::<O>(data, st, g);
    lemma_slab::<O>(data);
    assert(gs.take(gs.len() as int) =~= gs);
    if g < gs.len() {
        // a group after it would hold values not read
        lemma_slab_group::<O>(data, g);
        lemma_gl_le::<O>(data, g + 1, gs.len() as int);
        if !(st.run is Idle) {
            lemma_gl_le::<O>(data, g, g + 1);
        }
        assert(false);
    }
    if st.run is Literal {
        let vs = gs[g - 1]->values;
        lemma_slab_group::<O>(data, g - 1);
        assert(vs.take(vs.len() as int) =~= vs);
    }
}


// ── The column iterator's decoder model ────────────────────────────────────

/// An RLE slab decoder: RLE columns' `ColumnEncoding::Decoder`
/// (`RleDecoder`), verified as the column iterator's `Decode`.
pub struct RDecoder<'a, O: RleOps<'a>> {
    pub(crate) inner: Dec<'a, O>,
}

/// RLE slabs' decoders as the column iterator sees them (`Decoders`),
/// verified against the RLE format: a slab is well-formed when
/// `slab_ok`, and holds the values its bytes encode. `O` is the column's
/// value ops: one type for every lifetime a slab is borrowed for.
///
/// Public only because RLE columns' `ColumnEncoding::ReadModel` names it;
/// this module is not.
pub struct RleReads<O>(PhantomData<O>);

impl<'a, O: RleOps<'a>> Decode for RDecoder<'a, O> {
    type Item = O::V;

    closed spec fn inv(&self) -> bool {
        self.inner.inv()
    }

    closed spec fn rest(&self) -> Seq<O::V> {
        self.inner.values().skip(self.inner.pos())
    }

    #[inline(always)]
    fn d_next(&mut self) -> (r: Option<O::V>) {
        let ghost v = self.inner.values();
        let ghost p = self.inner.pos();
        proof {
            self.inner.lemma_pos();
        }
        let r = self.inner.next();
        proof {
            if r is Some {
                assert(v.skip(p).skip(1) =~= v.skip(self.inner.pos()));
            }
        }
        r
    }

    #[inline(always)]
    fn d_nth(&mut self, n: usize) -> (r: Option<O::V>) {
        let ghost v = self.inner.values();
        let ghost p = self.inner.pos();
        proof {
            self.inner.lemma_pos();
        }
        let r = self.inner.nth(n);
        proof {
            if n < v.skip(p).len() {
                assert(v.skip(p).skip(n + 1) =~= v.skip(self.inner.pos()));
            }
        }
        r
    }

    #[inline(always)]
    fn d_run_max(&mut self, max: usize) -> (r: Option<crate::Run<O::V>>) {
        let ghost v = self.inner.values();
        let ghost p = self.inner.pos();
        proof {
            self.inner.lemma_pos();
        }
        let r = self.inner.next_run_max(max);
        proof {
            if let Some(run) = r {
                assert(v.skip(p).take(run.count as int) =~= Seq::new(run.count as nat, |_i: int| run.value)) by {
                    assert forall|i: int| 0 <= i < run.count implies #[trigger] v.skip(p).take(run.count as int)[i]
                        == run.value by {
                        assert(v[p + i] == run.value);
                    }
                }
                assert(v.skip(p).skip(run.count as int) =~= v.skip(self.inner.pos()));
            }
        }
        r
    }

    #[inline(always)]
    fn d_eq(a: &O::V, b: &O::V) -> (r: bool) {
        O::same(a, b)
    }

    #[inline(always)]
    fn dup(&self) -> (r: Self) {
        RDecoder { inner: self.inner.dup() }
    }
}

/// The empty slab is valid, with no values.
proof fn lemma_empty_slab<'a, O: RleOps<'a>>()
    ensures
        valid_slab::<O>(Seq::<u8>::empty()),
        slab_values::<O>(Seq::<u8>::empty()) == Seq::<O::V>::empty(),
{
    let e = Seq::<Group<O::V>>::empty();
    assert(groups_bytes::<O>(e) =~= Seq::<u8>::empty());
    reveal(parses);
    assert(parses::<O>(e, Seq::<u8>::empty()));
    let gs = slab_groups::<O>(Seq::<u8>::empty());
    lemma_groups_unique::<O>(e, gs);
    assert(groups_values(e, O::null()) =~= Seq::<O::V>::empty());
}

/// What the RLE read model says of a slab: valid as `slab_ok`, its
/// values its bytes' (`slab_values`).
pub(crate) proof fn lemma_reads_are<O: for<'b> RleOps<'b>>(s: Slab)
    ensures
        <RleReads<O> as Decoders<'static>>::ok(s) == slab_ok::<O>(s),
        <RleReads<O> as Decoders<'static>>::vals(s) == slab_values::<O>(slab_bytes(s)),
{
}

impl<'a, O> Decoders<'a> for RleReads<O> where O: for<'b> RleOps<'b> {
    type Tail = RleTail;
    type D = RDecoder<'a, O>;

    closed spec fn ok(s: Slab) -> bool {
        slab_ok::<O>(s)
    }

    closed spec fn vals(s: Slab) -> Seq<<O as RleOps<'a>>::V> {
        slab_values::<O>(slab_bytes(s))
    }

    #[inline(always)]
    fn open(s: &'a Slab) -> (d: RDecoder<'a, O>) {
        let d = RDecoder { inner: Dec::new(s.data.as_slice()) };
        proof {
            assert(d.inner.values().skip(0) =~= d.inner.values());
        }
        d
    }

    #[inline(always)]
    fn none() -> (d: RDecoder<'a, O>) {
        let empty: &'a [u8] = &[];
        proof {
            lemma_empty_slab::<O>();
            assert(empty@ =~= Seq::<u8>::empty());
        }
        RDecoder { inner: Dec::new(empty) }
    }
}

} // verus!

impl<V: Copy> Clone for RunState<V> {
    fn clone(&self) -> Self {
        match self {
            Self::Repeat(v) => Self::Repeat(*v),
            Self::Literal => Self::Literal,
            Self::Null => Self::Null,
            Self::Idle => Self::Idle,
        }
    }
}

impl<'a, O: RleOps<'a>> Clone for Dec<'a, O> {
    fn clone(&self) -> Self {
        Dec {
            data: self.data,
            byte_pos: self.byte_pos,
            remaining: self.remaining,
            state: self.state.clone(),
            run_at: self.run_at,
            gi: self.gi,
        }
    }
}
