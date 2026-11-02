//! Decoding a validated slab's values, verified with Verus.
//!
//! [`Dec`] walks the canonical encoding of a slab ([`valid_slab`]) and
//! yields its values ([`slab_values`]) one at a time, a run at a time, or
//! after skipping ahead; [`DecState`] is a position detached from the
//! slab's bytes.  On a valid slab it never panics, and every value it
//! yields is the model's value at its position.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use super::model::*;
// proof functions exist only when Verus compiles the crate
#[cfg(verus_keep_ghost)]
use super::read::{lemma_count, lemma_header, lemma_starts, lemma_value};
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// `data` is the canonical encoding of some values, with metadata that
/// fits — what load accepts and what the writer produces.
pub open spec fn valid_slab<'a, O: RleOps<'a>>(data: Seq<u8>) -> bool {
    exists|gs: Seq<Group<O::V>>| parses::<O>(gs, data)
}

/// The groups of a valid slab.
pub open spec fn slab_groups<'a, O: RleOps<'a>>(data: Seq<u8>) -> Seq<Group<O::V>> {
    choose|gs: Seq<Group<O::V>>| parses::<O>(gs, data)
}

/// The values of a valid slab.
pub open spec fn slab_values<'a, O: RleOps<'a>>(data: Seq<u8>) -> Seq<O::V> {
    groups_values(slab_groups::<O>(data), O::null())
}

/// The byte offset of group `i`.
pub open spec fn gb<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, i: int) -> int {
    groups_bytes::<O>(gs.take(i)).len() as int
}

/// The value offset of group `i`.
pub open spec fn gl<V>(gs: Seq<Group<V>>, i: int) -> int {
    groups_len(gs.take(i)) as int
}

pub proof fn lemma_group_values_len<V>(g: Group<V>, null: V)
    ensures group_values(g, null).len() == group_len(g),
{
}

pub proof fn lemma_groups_values_concat<V>(a: Seq<Group<V>>, b: Seq<Group<V>>, null: V)
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

/// Where group `i`'s values and bytes sit among all of them.
pub proof fn lemma_group_at<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, i: int)
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
pub proof fn lemma_slab<'a, O: RleOps<'a>>(data: Seq<u8>)
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

/// A string that starts with `x` at offset `p`.
proof fn lemma_prefix_at(s: Seq<u8>, p: int, x: Seq<u8>, rest: Seq<u8>)
    requires 0 <= p <= s.len(), s.skip(p) == x + rest,
    ensures x.is_prefix_of(s.skip(p)), p + x.len() <= s.len(), s.skip(p + x.len()) == rest,
{
    assert((x + rest).take(x.len() as int) =~= x);
    assert(s.skip(p).skip(x.len() as int) =~= s.skip(p + x.len()));
    assert((x + rest).skip(x.len() as int) =~= rest);
}

pub proof fn lemma_skip_skip<T>(s: Seq<T>, a: int, b: int)
    requires 0 <= a, 0 <= b, a + b <= s.len(),
    ensures s.skip(a).skip(b) == s.skip(a + b),
{
    assert(s.skip(a).skip(b) =~= s.skip(a + b));
}

/// Group `i` of a valid slab: where its bytes and values sit.
pub proof fn lemma_slab_group<'a, O: RleOps<'a>>(data: Seq<u8>, i: int)
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
pub proof fn lemma_slab_lit<'a, O: RleOps<'a>>(data: Seq<u8>, i: int, j: int)
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
pub open spec fn at_header<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int, b: int, hb: int) -> bool {
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
    pub closed spec fn data(&self) -> Seq<u8> {
        self.data@
    }

    /// The slab's values.
    pub open spec fn values(&self) -> Seq<O::V> {
        slab_values::<O>(self.data())
    }

    /// The index of the next value.
    pub closed spec fn pos(&self) -> int {
        gl(slab_groups::<O>(self.data@), self.gi@) - self.remaining
    }

    pub closed spec fn inv(&self) -> bool {
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
    pub closed spec fn skip_post(r0: Self, r1: Self, n: usize, r: Result<O::V, usize>) -> bool {
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

    /// At a run boundary: begin the next run, if there is one.
    fn advance_run(&mut self)
        requires old(self).inv(), old(self).remaining == 0,
        ensures
            final(self).inv(), final(self).data() == old(self).data(),
            final(self).pos() == old(self).pos(),
            final(self).remaining == 0 ==> final(self).pos() == final(self).values().len(),
    {
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
    {
        let ghost r0 = *self;
        self.remaining = self.remaining - 1;
        self.read_lit(Ghost(r0))
    }

    /// The next value, if any.
    #[inline]
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
                },
                None => final(self).pos() == old(self).pos()
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
                old(self).values() == self.values(), max > 0,
                0 <= self.pos() <= self.values().len(),
            decreases if self.remaining == 0 { 1int } else { 0int },
        {
            if self.remaining > 0 {
                return self.take_run(max);
            }
            self.advance_run();
            if self.remaining == 0 {
                return None;
            }
        }
    }

    /// Inside a run: up to `max` of its values, as `next_run_max`
    /// returns them.
    #[inline(always)]
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
                }
                Some(crate::Run { count, value })
            },
            RunState::Literal => Some(crate::Run { count: 1, value: self.take_lit() }),
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
    pub(crate) fn is_literal(&self) -> bool {
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
    pub(crate) fn start() -> Self {
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
pub closed spec fn state_ok<'a, O: RleOps<'a>>(data: Seq<u8>, st: RleDecoderState) -> bool {
    &&& valid_slab::<O>(data)
    &&& data.len() <= usize::MAX
    &&& exists|gi: int| state_at::<O>(data, st, gi)
}

/// The index of the value `st` stands before in `data`.
pub closed spec fn state_pos<'a, O: RleOps<'a>>(data: Seq<u8>, st: RleDecoderState) -> int {
    gl(slab_groups::<O>(data), choose|gi: int| state_at::<O>(data, st, gi)) - st.remaining
}

/// Group offsets increase.
proof fn lemma_gb_lt<'a, O: RleOps<'a>>(data: Seq<u8>, i: int, j: int)
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
    pub(crate) fn suspend(&self) -> (r: RleDecoderState)
        requires self.inv(),
        ensures state_ok::<O>(self.data(), r), state_pos::<O>(self.data(), r) == self.pos(),
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
        ensures r.inv(), r.data() == data@, r.pos() == state_pos::<O>(data@, *st),
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
