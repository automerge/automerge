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
use crate::iter_core::{known, no_bytes, push_n, tail, Decode, Decoders};
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

/// A slab of two segments or more holds two values side by side that
/// differ: across its first group boundary, or the first two of the
/// literal that is its one group.
pub(crate) proof fn lemma_two_segments<'a, O: RleOps<'a>>(data: Seq<u8>)
    requires
        valid_slab::<O>(data),
        groups_segments(slab_groups::<O>(data)) >= 2,
    ensures
        exists|i: int|
            0 < i < slab_values::<O>(data).len() && slab_values::<O>(data)[i - 1]
                != #[trigger] slab_values::<O>(data)[i],
{
    let gs = slab_groups::<O>(data);
    let vs = slab_values::<O>(data);
    if gs.len() >= 2 {
        lemma_boundary::<O>(data, 1);
        let i = gl(gs, 1);
        assert(0 < i < vs.len() && vs[i - 1] != vs[i]);
    } else {
        assert(gs.len() == 1);
        let g = gs[0];
        assert(gs.drop_last() =~= Seq::<Group<O::V>>::empty());
        assert(groups_segments(gs.drop_last()) == 0);
        assert(group_segments(g) >= 2);
        lemma_slab_group::<O>(data, 0);
        lemma_slab::<O>(data);
        assert(gs.take(0) =~= Seq::<Group<O::V>>::empty());
        assert(gl(gs, 0) == 0);
        lemma_group_values_len(g, O::null());
        match g {
            Group::Lit { values } => {
                assert(values[0] != values[1]);
                assert(vs[0] == group_values(g, O::null())[0]);
                assert(vs[1] == group_values(g, O::null())[1]);
                assert(0 < 1 < vs.len() && vs[1 - 1] != vs[1]);
            },
            _ => {},
        }
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
                    O::axiom_null();
                },
                _ => {},
            }
        },
        Group::Lit { values } => {
            match b {
                Group::Null { count: _ } => {
                    O::axiom_null();
                },
                _ => {},
            }
        },
        Group::Null { count: _ } => {
            match b {
                Group::Run { count: _, value: y } => {
                    O::axiom_null();
                },
                Group::Lit { values } => {
                    O::axiom_null();
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

/// What follows `a` in `a + (h + p) + r` is `h + (p + r)`.
proof fn lemma_after_head<T>(a: Seq<T>, h: Seq<T>, p: Seq<T>, r: Seq<T>)
    ensures
        (a + (h + p) + r).skip(a.len() as int) == h + (p + r),
        (a + (h + p) + r).len() == a.len() + h.len() + p.len() + r.len(),
{
    assert((a + (h + p) + r).skip(a.len() as int) =~= h + (p + r));
}

/// Item `j` of `b` is item `a.len() + j` of `a + b + c`.
proof fn lemma_mid_item<T>(a: Seq<T>, b: Seq<T>, c: Seq<T>, j: int)
    requires 0 <= j < b.len(),
    ensures
        (a + b + c)[a.len() + j] == b[j],
        (a + b + c).len() == a.len() + b.len() + c.len(),
{
}

/// Group `i` of a valid slab is one a writer emits, after the group
/// before it.
proof fn lemma_slab_group_ok<'a, O: RleOps<'a>>(data: Seq<u8>, i: int)
    requires valid_slab::<O>(data), 0 <= i < slab_groups::<O>(data).len(),
    ensures
        group_ok::<O>(slab_groups::<O>(data)[i]),
        i > 0 ==> adjacent_ok(slab_groups::<O>(data)[i - 1], slab_groups::<O>(data)[i]),
{
    lemma_slab::<O>(data);
}

/// Group `i` of a valid slab: where its bytes sit.
proof fn lemma_slab_group_bytes<'a, O: RleOps<'a>>(data: Seq<u8>, i: int)
    requires valid_slab::<O>(data), 0 <= i < slab_groups::<O>(data).len(),
    ensures ({
        let gs = slab_groups::<O>(data);
        let g = gs[i];
        &&& 0 <= gb::<O>(gs, i) < data.len()
        &&& gb::<O>(gs, i + 1) == gb::<O>(gs, i) + group_bytes::<O>(g).len()
        &&& gb::<O>(gs, i + 1) <= data.len()
        &&& group_bytes::<O>(g).len() > 0
        &&& group_bytes::<O>(g) == O::enc_signed(header_of(g) as i64) + payload::<O>(g)
        &&& data.skip(gb::<O>(gs, i)) == O::enc_signed(header_of(g) as i64)
            + (payload::<O>(g) + groups_bytes::<O>(gs.skip(i + 1)))
    }),
{
    let gs = slab_groups::<O>(data);
    let g = gs[i];
    let head = groups_bytes::<O>(gs.take(i));
    let rest = groups_bytes::<O>(gs.skip(i + 1));
    lemma_slab::<O>(data);
    lemma_groups_bytes_at::<O>(gs, i);
    lemma_group_bytes_nonempty::<O>(g);
    assert(gs.take(i + 1).drop_last() =~= gs.take(i));
    lemma_after_head(head, O::enc_signed(header_of(g) as i64), payload::<O>(g), rest);
}

/// Group `i` of a valid slab: where its values sit.
proof fn lemma_slab_group_values<'a, O: RleOps<'a>>(data: Seq<u8>, i: int)
    requires valid_slab::<O>(data), 0 <= i < slab_groups::<O>(data).len(),
    ensures ({
        let gs = slab_groups::<O>(data);
        let g = gs[i];
        &&& gl(gs, i + 1) == gl(gs, i) + group_len(g)
        &&& forall|j: int| 0 <= j < group_len(g)
            ==> slab_values::<O>(data)[gl(gs, i) + j] == #[trigger] group_values(g, O::null())[j]
        &&& gl(gs, i) + group_len(g) <= slab_values::<O>(data).len()
        &&& group_len(g) <= usize::MAX
    }),
{
    let gs = slab_groups::<O>(data);
    let g = gs[i];
    let a = groups_values(gs.take(i), O::null());
    let b = group_values(g, O::null());
    let c = groups_values(gs.skip(i + 1), O::null());
    lemma_slab::<O>(data);
    lemma_group_at::<O>(gs, i);
    lemma_group_values_len(g, O::null());
    assert((a + b + c).len() == a.len() + b.len() + c.len());
    assert forall|j: int| 0 <= j < group_len(g) implies slab_values::<O>(data)[gl(gs, i) + j] == #[trigger] b[j] by {
        lemma_mid_item(a, b, c, j);
    }
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
    lemma_slab_group_ok::<O>(data, i);
    lemma_slab_group_bytes::<O>(data, i);
    lemma_slab_group_values::<O>(data, i);
}

/// The encodings of the first `j + 1` values are those of the first `j`
/// and the next value's.
proof fn lemma_encs_take_next<'a, O: RleOps<'a>>(vs: Seq<O::V>, j: int)
    requires 0 <= j < vs.len(),
    ensures
        encs::<O>(vs.take(j + 1)) == encs::<O>(vs.take(j)) + O::enc(vs[j]),
        encs::<O>(vs.take(j + 1)).len() == encs::<O>(vs.take(j)).len() + O::enc(vs[j]).len(),
{
    assert(vs.take(j + 1).drop_last() =~= vs.take(j));
}

/// What follows `h + a` in `h + ((a + e + b) + r)`, itself what follows
/// the first `n` items of `s`, is `e + (b + r)`.
proof fn lemma_skip_into<T>(s: Seq<T>, n: int, h: Seq<T>, a: Seq<T>, e: Seq<T>, b: Seq<T>, r: Seq<T>)
    requires 0 <= n <= s.len(), s.skip(n) == h + ((a + e + b) + r),
    ensures
        n + h.len() + a.len() <= s.len(),
        s.skip(n + h.len() + a.len()) == e + (b + r),
{
    let k = (h.len() + a.len()) as int;
    assert(s.skip(n).len() == h.len() + a.len() + e.len() + b.len() + r.len());
    lemma_skip_skip(s, n, k);
    assert(s.skip(n).skip(k) =~= e + (b + r));
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
    lemma_slab_group_ok::<O>(data, i);
    lemma_slab_group_bytes::<O>(data, i);
    lemma_encs_at::<O>(vs, j);
    lemma_encs_take_next::<O>(vs, j);
    lemma_skip_into(data, gb::<O>(gs, i), lit_head::<O>(vs.len()), encs::<O>(vs.take(j)), O::enc(vs[j]),
        encs::<O>(vs.skip(j + 1)), groups_bytes::<O>(gs.skip(i + 1)));
}

/// Every value of repeat or null group `i` of a valid slab is `v`, the
/// group's value.
proof fn lemma_slab_run_values<'a, O: RleOps<'a>>(data: Seq<u8>, i: int, v: O::V)
    requires
        valid_slab::<O>(data), 0 <= i < slab_groups::<O>(data).len(),
        match slab_groups::<O>(data)[i] {
            Group::Run { value, .. } => v == value,
            Group::Lit { .. } => false,
            Group::Null { .. } => v == O::null(),
        },
    ensures ({
        let gs = slab_groups::<O>(data);
        &&& gl(gs, i + 1) == gl(gs, i) + group_len(gs[i])
        &&& gl(gs, i + 1) <= slab_values::<O>(data).len()
        &&& forall|k: int| gl(gs, i) <= k < gl(gs, i + 1) ==> slab_values::<O>(data)[k] == v
    }),
{
    let gs = slab_groups::<O>(data);
    let g = gs[i];
    lemma_slab_group_values::<O>(data, i);
    assert forall|k: int| gl(gs, i) <= k < gl(gs, i + 1) implies slab_values::<O>(data)[k] == v by {
        let j = k - gl(gs, i);
        assert(group_values(g, O::null())[j] == v);
    }
}

/// Values `j` and `j + 1` of literal group `i` of a valid slab differ.
proof fn lemma_slab_lit_values<'a, O: RleOps<'a>>(data: Seq<u8>, i: int, j: int)
    requires
        valid_slab::<O>(data), 0 <= i < slab_groups::<O>(data).len(),
        slab_groups::<O>(data)[i] is Lit,
        0 <= j, j + 1 < slab_groups::<O>(data)[i]->values.len(),
    ensures ({
        let gs = slab_groups::<O>(data);
        let x = slab_values::<O>(data);
        &&& gl(gs, i + 1) == gl(gs, i) + group_len(gs[i])
        &&& gl(gs, i) + j + 1 < x.len()
        &&& x[gl(gs, i) + j] != x[gl(gs, i) + j + 1]
    }),
{
    let gs = slab_groups::<O>(data);
    let g = gs[i];
    let vs = g->values;
    lemma_slab_group_ok::<O>(data, i);
    lemma_slab_group_values::<O>(data, i);
    assert(group_values(g, O::null())[j] == vs[j]);
    assert(group_values(g, O::null())[j + 1] == vs[j + 1]);
    assert(vs[j] != vs[j + 1]);
}

/// Value `k` of group `i` of a valid slab: where it sits among the slab's
/// values, and that it is a repeat run's value or a null run's null.
proof fn lemma_slab_value<'a, O: RleOps<'a>>(data: Seq<u8>, i: int, k: int)
    requires
        valid_slab::<O>(data), 0 <= i < slab_groups::<O>(data).len(),
        0 <= k < group_len(slab_groups::<O>(data)[i]),
    ensures ({
        let gs = slab_groups::<O>(data);
        let g = gs[i];
        let x = group_values(g, O::null())[k];
        &&& group_ok::<O>(g)
        &&& gl(gs, i + 1) == gl(gs, i) + group_len(g)
        &&& 0 <= gl(gs, i) && gl(gs, i) + k < slab_values::<O>(data).len()
        &&& slab_values::<O>(data)[gl(gs, i) + k] == x
        &&& g is Run ==> x == run_value(g)
        &&& g is Null ==> x == O::null()
    }),
{
    lemma_slab_group_ok::<O>(data, i);
    lemma_slab_group_values::<O>(data, i);
}

/// The header of group `gi` of a valid slab starts the bytes from the
/// group's start.
proof fn lemma_header_starts<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int)
    requires valid_slab::<O>(data), 0 <= gi < slab_groups::<O>(data).len(),
    ensures ({
        let gs = slab_groups::<O>(data);
        &&& 0 <= gb::<O>(gs, gi) < data.len()
        &&& O::enc_signed(header_of(gs[gi]) as i64).is_prefix_of(data.skip(gb::<O>(gs, gi)))
    }),
{
    let gs = slab_groups::<O>(data);
    let g = gs[gi];
    lemma_slab_group_bytes::<O>(data, gi);
    lemma_starts(O::enc_signed(header_of(g) as i64), payload::<O>(g) + groups_bytes::<O>(gs.skip(gi + 1)));
}

/// Repeat run `gi`, read whole: its `hb`-byte header `hv` at `pos`, then
/// its value in `vlen` bytes. Its count and value, and where it ends.
proof fn lemma_read_run<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int, pos: int, hb: int, hv: i64, vlen: int, value: O::V)
    requires
        valid_slab::<O>(data), 0 <= gi < slab_groups::<O>(data).len(),
        pos == gb::<O>(slab_groups::<O>(data), gi),
        hv > 0, hv as int == header_of(slab_groups::<O>(data)[gi]),
        hb == O::enc_signed(hv).len(),
        data.skip(pos + hb) == payload::<O>(slab_groups::<O>(data)[gi]) + groups_bytes::<O>(slab_groups::<O>(data).skip(gi + 1)),
        0 <= vlen <= data.skip(pos + hb).len(),
        data.skip(pos + hb).take(vlen) == O::enc(value), !O::is_null(value),
    ensures ({
        let gs = slab_groups::<O>(data);
        let g = gs[gi];
        &&& g is Run && run_value(g) == value
        &&& g->Run_count == hv && hv <= usize::MAX
        &&& hb == O::enc_signed(g->Run_count as i64).len()
        &&& pos + hb + vlen == gb::<O>(gs, gi + 1)
        &&& gl(gs, gi + 1) == gl(gs, gi) + hv
    }),
{
    let gs = slab_groups::<O>(data);
    let g = gs[gi];
    lemma_slab_run_end::<O>(data, gi);
    lemma_slab_group_values::<O>(data, gi);
    lemma_value::<O>(data.skip(pos + hb), run_value(g), groups_bytes::<O>(gs.skip(gi + 1)), vlen, value);
}

/// Literal group `gi`, its `hb`-byte header `hv` read: how many values
/// it has, and that they start after the header.
proof fn lemma_read_lit_head<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int, hb: int, hv: i64)
    requires
        valid_slab::<O>(data), 0 <= gi < slab_groups::<O>(data).len(),
        hv < 0, hv as int == header_of(slab_groups::<O>(data)[gi]),
        hb == O::enc_signed(hv).len(),
    ensures ({
        let gs = slab_groups::<O>(data);
        let vs = gs[gi]->values;
        &&& gs[gi] is Lit
        &&& vs.len() == -hv && 1 <= vs.len() <= i64::MAX && vs.len() <= usize::MAX
        &&& hb == lit_head::<O>(vs.len()).len()
        &&& encs::<O>(vs.take(0)).len() == 0
        &&& gl(gs, gi + 1) == gl(gs, gi) + vs.len()
    }),
{
    let gs = slab_groups::<O>(data);
    lemma_slab_group_ok::<O>(data, gi);
    lemma_slab_group_values::<O>(data, gi);
    lemma_encs_none::<O>(gs[gi]->values);
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
        no_unwind
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
        hide(valid_slab);
        hide(slab_groups);
        hide(slab_values);
        hide(gb);
        hide(gl);
        hide(lit_head);
        hide(encs);
        let gi = self.gi@;
        lemma_take_groups::<O>(self.data@, gi);
        if gi >= 1 {
            lemma_take_groups::<O>(self.data@, gi - 1);
            lemma_slab_group_values::<O>(self.data@, gi - 1);
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

    /// At the end of a run that is not the slab's last: the value that
    /// follows differs from the run's last.
    proof fn lemma_boundary_differs(&self)
        requires self.inv(), self.remaining == 0, self.gi@ >= 1, self.pos() < self.values().len(),
        ensures
            0 < self.pos(),
            self.values()[self.pos() - 1] != self.values()[self.pos()],
    {
        self.lemma_boundary();
        lemma_boundary::<O>(self.data@, self.gi@);
    }

    /// `advance_run`, out of line: for `next_run_max`, which calls it
    /// once per run. Inlined there, the run loop runs a fifth slower on
    /// aarch64.
    #[inline(never)]
    fn advance_run_outlined(&mut self)
        requires old(self).inv(), old(self).remaining == 0,
        ensures
            final(self).inv(), final(self).data() == old(self).data(),
            final(self).pos() == old(self).pos(),
            final(self).remaining == 0 ==> final(self).pos() == final(self).values().len(),
            final(self).dsegs() == old(self).dsegs(),
        no_unwind
    {
        self.advance_run()
    }

    /// At a run boundary: begin the next run, if there is one. In line
    /// in its caller whatever the size, as `next` is in its own.
    #[inline(always)]
    fn advance_run(&mut self)
        requires old(self).inv(), old(self).remaining == 0,
        ensures
            final(self).inv(), final(self).data() == old(self).data(),
            final(self).pos() == old(self).pos(),
            final(self).remaining == 0 ==> final(self).pos() == final(self).values().len(),
            final(self).dsegs() == old(self).dsegs(),
        no_unwind
    {
        hide(valid_slab);
        hide(slab_groups);
        hide(slab_values);
        hide(gb);
        hide(gl);
        hide(gseg);
        hide(groups_bytes);
        hide(payload);
        hide(header_of);
        hide(lit_head);
        hide(encs);
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
        let ghost b0 = self.byte_pos as int;
        proof {
            lemma_header_starts::<O>(data, gi);
        }
        let (count_bytes, count_raw) = match O::read_signed_opt(tail(self.data, self.byte_pos)) {
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
            lemma_walk_header::<O>(data, gi, b0, count_bytes as int, count_raw);
            lemma_header_kind::<O>(data, gi, p, count_raw);
        }
        if count_raw > 0 {
            let count = count_raw as usize;
            let value_start = self.byte_pos + count_bytes;
            let (vlen, value) = O::unpack(tail(self.data, value_start));
            proof {
                lemma_read_run::<O>(data, gi, b0, count_bytes as int, count_raw, vlen as int, value);
            }
            self.byte_pos = value_start + vlen;
            self.remaining = count;
            self.run_at = value_start;
            self.state = RunState::Repeat(value);
            self.gi = Ghost(gi + 1);
            proof {
                assert(self.inv());
            }
        } else if count_raw < 0 {
            proof {
                lemma_read_lit_head::<O>(data, gi, count_bytes as int, count_raw);
            }
            let total = (-count_raw) as usize;
            self.byte_pos = self.byte_pos + count_bytes;
            self.remaining = total;
            self.state = RunState::Literal;
            self.gi = Ghost(gi + 1);
            proof {
                assert(self.inv());
            }
        } else {
            let (ncb, null_count) = known(O::read_unsigned_opt(tail(self.data, self.byte_pos + count_bytes)));
            proof {
                lemma_walk_null_count::<O>(data, gi, b0, count_bytes as int, ncb as int, null_count);
            }
            self.byte_pos = self.byte_pos + count_bytes + ncb;
            self.remaining = null_count as usize;
            self.state = RunState::Null;
            self.gi = Ghost(gi + 1);
            proof {
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
        no_unwind
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

    /// `take_next`, out of line: for `nth`, which from a run boundary
    /// (where `get` calls it) then saves no registers for a read it
    /// does not do.
    #[inline(never)]
    fn take_next_outlined(&mut self) -> (r: Option<O::V>)
        requires old(self).inv(), old(self).remaining > 0,
        ensures
            final(self).inv(), final(self).data() == old(self).data(),
            old(self).pos() < old(self).values().len(),
            r == Some(old(self).values()[old(self).pos()]),
            final(self).pos() == old(self).pos() + 1,
        no_unwind
    {
        self.take_next()
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
        no_unwind
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
        let rest = tail(self.data, self.byte_pos);
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
        hide(valid_slab);
        hide(slab_groups);
        hide(slab_values);
        hide(gb);
        hide(gl);
        hide(group_ok);
        hide(group_values);
        hide(lit_head);
        hide(encs);
        let gs = slab_groups::<O>(self.data@);
        let gi = self.gi@;
        lemma_slab_value::<O>(self.data@, gi - 1, group_len(gs[gi - 1]) - self.remaining);
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
        no_unwind
    {
        let ghost r0 = *self;
        self.remaining = self.remaining - 1;
        self.read_lit(Ghost(r0))
    }

    /// The next value, if any. In line in its caller whatever the size,
    /// and the start of a run with it: a loop that reads value after
    /// value then keeps the decoder in registers. Left to LLVM, which of
    /// the two stays out of line changes from one value type to the next
    /// and with any edit near them, and where one does, the loop reads
    /// and writes the decoder's fields for every value, at two to three
    /// times the instructions.
    #[inline(always)]
    pub(crate) fn next(&mut self) -> (r: Option<O::V>)
        requires old(self).inv(),
        ensures
            final(self).inv(), final(self).data() == old(self).data(),
            old(self).pos() < old(self).values().len() ==> r == Some(old(self).values()[old(self).pos()])
                && final(self).pos() == old(self).pos() + 1,
            old(self).pos() >= old(self).values().len() ==> r is None && final(self).pos() == old(self).pos(),
        no_unwind
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

    /// Append all the values still to come to `out`: a repeat or null
    /// run's value as often as the run goes on, a literal's values one
    /// after another.
    #[inline(always)]
    pub(crate) fn drain_into(&mut self, out: &mut Vec<O::V>)
        requires old(self).inv(),
        ensures
            final(self).inv(), final(self).data() == old(self).data(),
            final(self).pos() == old(self).values().len(),
            0 <= old(self).pos() <= old(self).values().len(),
            final(out)@ == old(out)@ + old(self).values().skip(old(self).pos()),
    {
        let ghost p0 = self.pos();
        let ghost vs = self.values();
        let ghost o0 = out@;
        proof {
            self.lemma_pos();
            assert(out@ =~= o0 + vs.subrange(p0, p0));
        }
        loop
            invariant
                self.inv(), self.data() == old(self).data(), self.values() == vs,
                0 <= p0 <= self.pos() <= vs.len(),
                out@ == o0 + vs.subrange(p0, self.pos()),
            ensures
                self.pos() == vs.len(),
            decreases vs.len() - self.pos(),
        {
            if self.remaining == 0 {
                self.advance_run();
                if self.remaining == 0 {
                    break;
                }
            }
            let ghost p1 = self.pos();
            let ghost o1 = out@;
            self.run_into(out);
            proof {
                assert(out@ =~= o0 + vs.subrange(p0, self.pos())) by {
                    assert(vs.subrange(p0, self.pos()) =~= vs.subrange(p0, p1) + vs.subrange(p1, self.pos()));
                }
            }
        }
        proof {
            assert(vs.subrange(p0, vs.len() as int) =~= vs.skip(p0));
        }
    }

    /// Inside a run: append what is left of it to `out`.
    #[inline(always)]
    fn run_into(&mut self, out: &mut Vec<O::V>)
        requires old(self).inv(), old(self).remaining > 0,
        ensures
            final(self).inv(), final(self).data() == old(self).data(),
            final(self).pos() > old(self).pos(),
            final(self).pos() <= old(self).values().len(),
            final(out)@ == old(out)@ + old(self).values().subrange(old(self).pos(), final(self).pos()),
    {
        let ghost data = self.data@;
        let ghost gi = self.gi@;
        let ghost p0 = self.pos();
        let ghost vs = self.values();
        let ghost o0 = out@;
        proof {
            self.lemma_pos();
            lemma_slab_group_ok::<O>(data, gi - 1);
        }
        match &self.state {
            RunState::Repeat(v) => {
                let value = *v;
                let count = self.remaining;
                self.remaining = 0;
                proof {
                    lemma_slab_run_values::<O>(data, gi - 1, value);
                }
                push_n(out, value, count);
                proof {
                    assert(Seq::new(count as nat, |_i: int| value) =~= vs.subrange(p0, p0 + count));
                }
            },
            RunState::Literal => {
                let ghost r0 = self.remaining;
                proof {
                    assert(out@ =~= o0 + vs.subrange(p0, p0));
                }
                while self.remaining > 0
                    invariant
                        self.inv(), self.data() == old(self).data(), self.values() == vs,
                        self.state is Literal,
                        self.remaining <= r0,
                        self.pos() == p0 + (r0 - self.remaining),
                        0 <= p0, self.pos() <= vs.len(),
                        out@ == o0 + vs.subrange(p0, self.pos()),
                    decreases self.remaining,
                {
                    let ghost p1 = self.pos();
                    let value = self.take_lit();
                    out.push(value);
                    proof {
                        assert(out@ =~= o0 + vs.subrange(p0, p1 + 1));
                    }
                }
            },
            RunState::Null => {
                let value = O::get_null();
                let count = self.remaining;
                self.remaining = 0;
                proof {
                    lemma_slab_run_values::<O>(data, gi - 1, value);
                }
                push_n(out, value, count);
                proof {
                    assert(Seq::new(count as nat, |_i: int| value) =~= vs.subrange(p0, p0 + count));
                }
            },
            RunState::Idle => {
                proof {
                    assert(false);
                }
            },
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
        no_unwind
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
            if self.remaining > 0 {
                // both values are the literal's, and a literal's neighbours differ
                let lv = gs[gi - 1]->values;
                lemma_slab_lit_values::<O>(data, gi - 1, lv.len() - r0);
            } else {
                self.lemma_boundary_differs();
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
        no_unwind
    {
        let ghost data = self.data@;
        let ghost gs = slab_groups::<O>(data);
        let ghost gi = self.gi@;
        let ghost p0 = self.pos();
        proof {
            lemma_slab_group_ok::<O>(data, gi - 1);
        }
        match &self.state {
            RunState::Repeat(v) => {
                let value = *v;
                let count = if self.remaining < max { self.remaining } else { max };
                self.remaining = self.remaining - count;
                proof {
                    lemma_slab_run_values::<O>(data, gi - 1, value);
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
                    lemma_slab_run_values::<O>(data, gi - 1, value);
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
        no_unwind
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
            let rest = tail(self.data, self.byte_pos);
            assert(rest@ == data.skip(b));
            let vlen = known(O::value_len(rest));
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
        no_unwind
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
                return self.take_next_outlined();
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
        no_unwind
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
        no_unwind
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
        no_unwind
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
        no_unwind
    {
        let ghost gi = self.gi@;
        let ghost r0 = *self;
        proof {
            self.lemma_header_ahead();
        }
        let (count_bytes, count_raw) = known(O::read_signed_opt(tail(self.data, self.byte_pos)));
        proof {
            self.lemma_header_read(count_bytes as int, count_raw);
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
                Self::lemma_enter_lit(r0, *self, count_bytes as int);
            }
            self.skip_in_lit(n)
        } else {
            self.skip_null_group(n, count_bytes)
        }
    }

    /// At a run boundary before the end, the bytes ahead begin with the
    /// next group's header.
    proof fn lemma_header_ahead(&self)
        requires self.inv(), self.remaining == 0, self.byte_pos < self.data@.len(),
        ensures
            0 <= self.gi@ < slab_groups::<O>(self.data@).len(),
            O::enc_signed(header_of(slab_groups::<O>(self.data@)[self.gi@]) as i64).is_prefix_of(
                self.data@.skip(self.byte_pos as int),
            ),
    {
        let data = self.data@;
        let gs = slab_groups::<O>(data);
        let gi = self.gi@;
        self.lemma_boundary();
        let g = gs[gi];
        lemma_slab_group::<O>(data, gi);
        lemma_starts(O::enc_signed(header_of(g) as i64), payload::<O>(g) + groups_bytes::<O>(gs.skip(gi + 1)));
    }

    /// The header read at a run boundary is the next group's, and says
    /// which kind of group that is and how long.
    proof fn lemma_header_read(&self, hb: int, hv: i64)
        requires
            self.inv(), self.remaining == 0, self.byte_pos < self.data@.len(),
            0 <= hb <= self.data@.len() - self.byte_pos,
            self.data@.skip(self.byte_pos as int).take(hb) == O::enc_signed(hv),
        ensures ({
            let g = slab_groups::<O>(self.data@)[self.gi@];
            &&& at_header::<O>(self.data@, self.gi@, self.byte_pos as int, hb)
            &&& group_len(g) <= usize::MAX
            &&& hv > 0 ==> g is Run && hv == group_len(g)
            &&& hv < 0 ==> g is Lit && -hv == group_len(g) && group_len(g) <= i64::MAX
            &&& hv == 0 ==> g is Null
        }),
    {
        let data = self.data@;
        let gs = slab_groups::<O>(data);
        let gi = self.gi@;
        self.lemma_boundary();
        lemma_slab_group::<O>(data, gi);
        lemma_walk_header::<O>(data, gi, self.byte_pos as int, hb, hv);
        O::axiom_enc_signed(hv, hv, Seq::empty(), Seq::empty());
    }

    /// A reader moved past the header of the literal it stood before is
    /// at that literal's first value.
    proof fn lemma_enter_lit(r0: Self, r1: Self, hb: int)
        requires
            r0.inv(), r0.remaining == 0, r0.state is Idle,
            at_header::<O>(r0.data@, r0.gi@, r0.byte_pos as int, hb),
            slab_groups::<O>(r0.data@)[r0.gi@] is Lit,
            r1.data == r0.data, r1.gi@ == r0.gi@ + 1, r1.byte_pos == r0.byte_pos + hb,
            r1.remaining == group_len(slab_groups::<O>(r0.data@)[r0.gi@]),
            r1.state is Literal,
        ensures
            r1.inv(), r1.data() == r0.data(), r1.pos() == r0.pos(), r1.byte_pos > r0.byte_pos,
            r1.remaining == group_len(slab_groups::<O>(r1.data())[r1.gi@ - 1]),
    {
        let data = r0.data@;
        let gs = slab_groups::<O>(data);
        let gi = r0.gi@;
        let vs = gs[gi]->values;
        lemma_slab_group::<O>(data, gi);
        assert(vs.take(0) =~= Seq::<O::V>::empty());
    }

    /// `skip_group` for a repeat run, whose header has been read.
    fn skip_run_group(&mut self, n: usize, count_bytes: usize, count: usize) -> (r: Result<O::V, usize>)
        requires
            old(self).inv(), old(self).remaining == 0, old(self).state is Idle,
            at_header::<O>(old(self).data(), old(self).gi@, old(self).byte_pos as int, count_bytes as int),
            slab_groups::<O>(old(self).data())[old(self).gi@] is Run,
            count == group_len(slab_groups::<O>(old(self).data())[old(self).gi@]),
        ensures Self::skip_post(*old(self), *final(self), n, r),
        no_unwind
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
            let (vlen, value) = O::unpack(tail(self.data, value_start));
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
        let vlen = known(O::value_len(tail(self.data, value_start)));
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
        no_unwind
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
        let (ncb, null_count) = known(O::read_unsigned_opt(tail(self.data, self.byte_pos + count_bytes)));
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
/// Not exported: nothing outside the crate can name one.
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

/// The segments behind a position in group `gi` are among those of the
/// first `gi` groups.
proof fn lemma_rsegs_at_le<'a, O: RleOps<'a>>(data: Seq<u8>, st: RleDecoderState, gi: int)
    requires valid_slab::<O>(data), state_at::<O>(data, st, gi),
    ensures
        0 <= gi <= slab_groups::<O>(data).len(),
        0 <= rsegs_at::<O>(data, st, gi) <= gseg(slab_groups::<O>(data), gi),
{
    hide(valid_slab);
    hide(slab_groups);
    hide(gb);
    hide(groups_segments);
    hide(lit_head);
    hide(encs);
    if gi >= 1 {
        lemma_gseg(slab_groups::<O>(data), gi - 1);
    }
}

/// The segments behind a position are among the slab's, which a
/// `usize` counts.
pub(crate) proof fn lemma_rsegs_le<'a, O: RleOps<'a>>(data: Seq<u8>, st: RleDecoderState)
    requires state_ok::<O>(data, st),
    ensures 0 <= rsegs::<O>(data, st) <= groups_segments(slab_groups::<O>(data)) <= usize::MAX,
{
    hide(valid_slab);
    hide(slab_groups);
    hide(state_at);
    hide(rsegs_at);
    hide(gseg);
    hide(canonical);
    hide(groups_segments);
    hide(groups_len);
    let gs = slab_groups::<O>(data);
    let gi = choose|g: int| state_at::<O>(data, st, g);
    lemma_rsegs_at_le::<O>(data, st, gi);
    lemma_slab::<O>(data);
    super::read::lemma_segments_le::<O>(gs);
    lemma_gseg_split::<O>(data, gi);
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

/// The encodings of the first `k` of `vs` are no longer than those of
/// all of them.
proof fn lemma_encs_take_le<'a, O: RleOps<'a>>(vs: Seq<O::V>, k: int)
    requires 0 <= k <= vs.len(),
    ensures encs::<O>(vs.take(k)).len() <= encs::<O>(vs.take(vs.len() as int)).len(),
{
    hide(encs);
    assert(vs.take(vs.len() as int) =~= vs.take(k) + vs.skip(k));
    lemma_encs_concat::<O>(vs.take(k), vs.skip(k));
}

/// A position in the literal before group `g` lies after the literal's
/// start, at or before its end.
proof fn lemma_lit_state_span<'a, O: RleOps<'a>>(data: Seq<u8>, st: RleDecoderState, g: int)
    requires valid_slab::<O>(data), st.run is Literal, state_at::<O>(data, st, g),
    ensures
        gb::<O>(slab_groups::<O>(data), g - 1) < st.byte_pos <= gb::<O>(slab_groups::<O>(data), g),
{
    hide(valid_slab);
    hide(slab_groups);
    hide(gb);
    hide(gl);
    hide(encs);
    let vs = slab_groups::<O>(data)[g - 1]->values;
    lemma_slab_lit_end::<O>(data, g - 1);
    lemma_encs_take_le::<O>(vs, vs.len() - st.remaining);
    O::axiom_enc_signed(-(vs.len() as int) as i64, 0, Seq::empty(), Seq::empty());
}

/// A suspended position names one group.
proof fn lemma_state_unique<'a, O: RleOps<'a>>(data: Seq<u8>, st: RleDecoderState, g1: int, g2: int)
    requires valid_slab::<O>(data), state_at::<O>(data, st, g1), state_at::<O>(data, st, g2),
    ensures g1 == g2,
{
    hide(valid_slab);
    hide(slab_groups);
    hide(gb);
    hide(lit_head);
    hide(encs);
    if g1 != g2 {
        let (a, b) = if g1 < g2 { (g1, g2) } else { (g2, g1) };
        lemma_gb_lt::<O>(data, a, b);
        if st.run is Literal {
            lemma_lit_state_span::<O>(data, st, a);
            lemma_lit_state_span::<O>(data, st, b);
            if a < b - 1 {
                lemma_gb_lt::<O>(data, a, b - 1);
            }
        }
    }
}

/// A position known to stand in group `gi`: it is one of the slab's, and
/// its segments and its index are that group's.
proof fn lemma_state_is<'a, O: RleOps<'a>>(data: Seq<u8>, st: RleDecoderState, gi: int)
    requires valid_slab::<O>(data), data.len() <= usize::MAX, state_at::<O>(data, st, gi),
    ensures
        state_ok::<O>(data, st),
        rsegs::<O>(data, st) == rsegs_at::<O>(data, st, gi),
        state_pos::<O>(data, st) == gl(slab_groups::<O>(data), gi) - st.remaining,
{
    let g = choose|g: int| state_at::<O>(data, st, g);
    lemma_state_unique::<O>(data, st, g, gi);
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

/// The first `i` of canonical groups are canonical.
proof fn lemma_canonical_take<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, i: int)
    requires canonical::<O>(gs), 0 <= i <= gs.len(),
    ensures canonical::<O>(gs.take(i)),
{
    let t = gs.take(i);
    assert forall|j: int| 0 <= j < t.len() implies group_ok::<O>(#[trigger] t[j]) by {
        assert(t[j] == gs[j]);
    }
    assert forall|j: int| 0 < j < t.len() implies adjacent_ok(t[j - 1], #[trigger] t[j]) by {
        assert(t[j] == gs[j] && t[j - 1] == gs[j - 1]);
    }
}

/// The values and the bytes of groups `a` start those of `a + b`.
proof fn lemma_groups_prefix<'a, O: RleOps<'a>>(a: Seq<Group<O::V>>, b: Seq<Group<O::V>>)
    ensures
        groups_len(a) <= groups_len(a + b),
        groups_values(a + b, O::null()).take(groups_len(a) as int) == groups_values(a, O::null()),
        groups_bytes::<O>(a).len() <= groups_bytes::<O>(a + b).len(),
        groups_bytes::<O>(a + b).take(groups_bytes::<O>(a).len() as int) == groups_bytes::<O>(a),
{
    hide(groups_values);
    hide(groups_bytes);
    hide(groups_len);
    let va = groups_values(a, O::null());
    let ba = groups_bytes::<O>(a);
    lemma_groups_values_concat(a, b, O::null());
    lemma_groups_bytes_concat::<O>(a, b);
    lemma_groups_len_concat(a, b);
    assert((va + groups_values(b, O::null())).take(va.len() as int) =~= va);
    assert((ba + groups_bytes::<O>(b)).take(ba.len() as int) =~= ba);
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
    hide(canonical);
    hide(groups_values);
    hide(groups_bytes);
    hide(groups_len);
    let gs = slab_groups::<O>(data);
    lemma_slab::<O>(data);
    lemma_canonical_take::<O>(gs, i);
    assert(gs =~= gs.take(i) + gs.skip(i));
    lemma_groups_prefix::<O>(gs.take(i), gs.skip(i));
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

/// The values up to `k` into group `gi`, a repeat or null run of `v`:
/// those of the groups before it, then `k` copies of `v`.
proof fn lemma_vals_into<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int, k: int, v: O::V)
    requires
        valid_slab::<O>(data), 0 <= gi < slab_groups::<O>(data).len(),
        1 <= k <= group_len(slab_groups::<O>(data)[gi]),
        match slab_groups::<O>(data)[gi] {
            Group::Run { value, .. } => v == value,
            Group::Null { .. } => v == O::null(),
            Group::Lit { .. } => false,
        },
    ensures ({
        let gs = slab_groups::<O>(data);
        slab_values::<O>(data).take(gl(gs, gi) + k) == groups_values(gs.take(gi), O::null()) + copies(k as nat, v)
    }),
{
    let gs = slab_groups::<O>(data);
    let g = gs[gi];
    let x = slab_values::<O>(data);
    let a = gl(gs, gi);
    lemma_slab_group::<O>(data, gi);
    lemma_take_groups::<O>(data, gi);
    assert forall|j: int| a <= j < a + group_len(g) implies x[j] == v by {
        assert(group_values(g, O::null())[j - a] == v);
    }
    lemma_take_run(x, a, k, group_len(g) as int, v);
}

/// A group that may follow group `gi - 1` may stand after the groups
/// before `gi`.
proof fn lemma_head_canonical<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int, g1: Group<O::V>)
    requires
        valid_slab::<O>(data), 0 <= gi < slab_groups::<O>(data).len(),
        group_ok::<O>(g1),
        gi > 0 ==> adjacent_ok(slab_groups::<O>(data)[gi - 1], g1),
    ensures canonical::<O>(slab_groups::<O>(data).take(gi).push(g1)),
{
    let gs = slab_groups::<O>(data);
    lemma_take_groups::<O>(data, gi);
    if gi > 0 {
        assert(gs.take(gi).last() == gs[gi - 1]);
    }
    lemma_canonical_join::<O>(gs.take(gi), g1);
}

/// The first `k` values of repeat run `gi`, as a group of their own, may
/// stand where the run does.
proof fn lemma_run_head_ok<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int, k: int)
    requires
        valid_slab::<O>(data), 0 <= gi < slab_groups::<O>(data).len(),
        slab_groups::<O>(data)[gi] is Run, 1 <= k <= slab_groups::<O>(data)[gi]->Run_count,
        k == 1 ==> !(gi > 0 && slab_groups::<O>(data)[gi - 1] is Lit),
    ensures ({
        let gs = slab_groups::<O>(data);
        canonical::<O>(gs.take(gi).push(run_group(k as nat, run_value(gs[gi]))))
    }),
{
    let gs = slab_groups::<O>(data);
    let g1 = run_group(k as nat, run_value(gs[gi]));
    lemma_slab_group::<O>(data, gi);
    assert(group_ok::<O>(gs[gi]));
    if k == 1 {
        assert(g1->values[0] == run_value(gs[gi]));
    }
    assert(group_ok::<O>(g1));
    lemma_head_canonical::<O>(data, gi, g1);
}

/// The first `k` values of null run `gi`, as a group of their own, may
/// stand where the run does.
proof fn lemma_null_head_ok<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int, k: int)
    requires
        valid_slab::<O>(data), 0 <= gi < slab_groups::<O>(data).len(),
        slab_groups::<O>(data)[gi] is Null, 1 <= k <= slab_groups::<O>(data)[gi]->Null_count,
    ensures canonical::<O>(slab_groups::<O>(data).take(gi).push(Group::Null { count: k as nat })),
{
    let gs = slab_groups::<O>(data);
    let g1 = Group::<O::V>::Null { count: k as nat };
    lemma_slab_group::<O>(data, gi);
    assert(group_ok::<O>(gs[gi]));
    assert(group_ok::<O>(g1));
    lemma_head_canonical::<O>(data, gi, g1);
}

/// A state whose pending group `g1` stands for `k` copies of `v`, with
/// the groups before `gi` behind it, has landed `k` values into group
/// `gi`.
proof fn lemma_land_head<'a, W: WriteOps<'a>>(data: Seq<u8>, gi: int, k: int, s: State<'a, W>, g1: Group<VOf<'a, W>>, v: VOf<'a, W>)
    requires
        valid_slab::<W::R>(data), 0 <= gi < slab_groups::<W::R>(data).len(), 1 <= k,
        !(s is Lit) && !(s is Empty), pend_group(s, Seq::empty()) == Some(g1),
        canonical::<W::R>(slab_groups::<W::R>(data).take(gi).push(g1)),
        group_values(g1, <W::R as RleOps<'a>>::null()) == copies(k as nat, v),
        slab_values::<W::R>(data).take(gl(slab_groups::<W::R>(data), gi) + k)
            == groups_values(slab_groups::<W::R>(data).take(gi), <W::R as RleOps<'a>>::null()) + copies(k as nat, v),
    ensures landed::<W>(data, gl(slab_groups::<W::R>(data), gi) + k, s, gb::<W::R>(slab_groups::<W::R>(data), gi),
        gseg(slab_groups::<W::R>(data), gi)),
{
    hide(canonical);
    hide(valid_slab);
    hide(slab_values);
    let gs = slab_groups::<W::R>(data);
    let pre = data.take(gb::<W::R>(gs, gi));
    let gs2 = gs.take(gi);
    lemma_take_groups::<W::R>(data, gi);
    lemma_push::<W::R>(gs2, g1);
    assert(pre + Seq::<u8>::empty().skip(0) =~= groups_bytes::<W::R>(gs2));
    assert(rel(s, pre, Seq::empty(), 0, gs2, Seq::empty(), 0));
    assert(values_of(s, gs2, Seq::empty()) == slab_values::<W::R>(data).take(gl(gs, gi) + k));
    assert(acc(s, gs2, Seq::empty()) == gseg(gs, gi));
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
    hide(landed);
    hide(canonical);
    hide(valid_slab);
    hide(slab_values);
    let v = run_value(slab_groups::<W::R>(data)[gi]);
    let g1 = run_group(k as nat, v);
    lemma_vals_into::<W::R>(data, gi, k, v);
    lemma_run_head_ok::<W::R>(data, gi, k);
    lemma_run_group::<W::R>(k as nat, v);
    lemma_land_head::<W>(data, gi, k, s, g1, v);
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
    hide(landed);
    hide(canonical);
    hide(valid_slab);
    hide(slab_values);
    let nl = <W::R as RleOps<'a>>::null();
    let g1 = Group::<VOf<'a, W>>::Null { count: k as nat };
    lemma_vals_into::<W::R>(data, gi, k, nl);
    lemma_null_head_ok::<W::R>(data, gi, k);
    assert(group_values(g1, nl) =~= copies(k as nat, nl));
    lemma_land_head::<W>(data, gi, k, s, g1, nl);
}

/// The first items of `a + (h + (p + q)) + r` are `a + h + p`.
proof fn lemma_head_take<T>(a: Seq<T>, h: Seq<T>, p: Seq<T>, q: Seq<T>, r: Seq<T>)
    ensures
        a.len() + h.len() + p.len() <= (a + (h + (p + q)) + r).len(),
        (a + (h + (p + q)) + r).take((a.len() + h.len() + p.len()) as int) == a + h + p,
{
    assert((a + (h + (p + q)) + r).take((a.len() + h.len() + p.len()) as int) =~= a + h + p);
}

/// The first `n` values of literal group `i` of a valid slab are a
/// literal that may follow the groups before it.
proof fn lemma_lit_head_canonical<'a, O: RleOps<'a>>(data: Seq<u8>, i: int, n: int)
    requires
        valid_slab::<O>(data), 0 <= i < slab_groups::<O>(data).len(),
        slab_groups::<O>(data)[i] is Lit,
        1 <= n <= slab_groups::<O>(data)[i]->values.len(),
    ensures ({
        let gs = slab_groups::<O>(data);
        canonical::<O>(gs.take(i).push(Group::Lit { values: gs[i]->values.take(n) }))
    }),
{
    let gs = slab_groups::<O>(data);
    let vs = gs[i]->values;
    let w = vs.take(n);
    let gw = Group::Lit { values: w };
    let gs2 = gs.take(i);
    lemma_slab_group_ok::<O>(data, i);
    lemma_take_groups::<O>(data, i);
    assert forall|k: int| 0 <= k < w.len() implies !O::is_null(#[trigger] w[k]) by {
        assert(w[k] == vs[k]);
    }
    assert forall|k: int| 0 < k < w.len() implies w[k - 1] != #[trigger] w[k] by {
        assert(w[k] == vs[k] && w[k - 1] == vs[k - 1]);
    }
    assert(group_ok::<O>(gw));
    if i > 0 {
        assert(gs2.last() == gs[i - 1]);
        assert(w[0] == vs[0]);
        assert(adjacent_ok(gs[i - 1], gw));
    }
    lemma_canonical_join::<O>(gs2, gw);
}

/// The groups before literal group `i` of a valid slab, then its first
/// `n` values as a literal, stand for the slab's values up to those.
proof fn lemma_lit_head_values<'a, O: RleOps<'a>>(data: Seq<u8>, i: int, n: int)
    requires
        valid_slab::<O>(data), 0 <= i < slab_groups::<O>(data).len(),
        slab_groups::<O>(data)[i] is Lit,
        0 <= n <= slab_groups::<O>(data)[i]->values.len(),
    ensures ({
        let gs = slab_groups::<O>(data);
        let gw = Group::Lit { values: gs[i]->values.take(n) };
        &&& gs[i]->values.len() <= usize::MAX
        &&& gl(gs, i) + n <= slab_values::<O>(data).len()
        &&& groups_values(gs.take(i).push(gw), O::null()) == slab_values::<O>(data).take(gl(gs, i) + n)
    }),
{
    let gs = slab_groups::<O>(data);
    let g = gs[i];
    let vs = g->values;
    let w = vs.take(n);
    let x = slab_values::<O>(data);
    let a = gl(gs, i);
    lemma_slab_group_values::<O>(data, i);
    lemma_take_groups::<O>(data, i);
    lemma_push::<O>(gs.take(i), Group::Lit { values: w });
    assert forall|j: int| 0 <= j < vs.len() implies x[a + j] == vs[j] by {
        assert(group_values(g, O::null())[j] == vs[j]);
    }
    assert(x.take(a + n) =~= x.take(a) + w);
}

/// The bytes of a valid slab before value `j` of literal group `i`: the
/// groups before it, its header, and its first `j` values.
proof fn lemma_lit_head_bytes<'a, O: RleOps<'a>>(data: Seq<u8>, i: int, j: int)
    requires
        valid_slab::<O>(data), 0 <= i < slab_groups::<O>(data).len(),
        slab_groups::<O>(data)[i] is Lit,
        0 <= j <= slab_groups::<O>(data)[i]->values.len(),
    ensures ({
        let gs = slab_groups::<O>(data);
        let vs = gs[i]->values;
        let b = gb::<O>(gs, i) + lit_head::<O>(vs.len()).len() + encs::<O>(vs.take(j)).len();
        &&& b <= data.len()
        &&& data.take(b) == groups_bytes::<O>(gs.take(i)) + lit_head::<O>(vs.len()) + encs::<O>(vs.take(j))
    }),
{
    let gs = slab_groups::<O>(data);
    let vs = gs[i]->values;
    let head = groups_bytes::<O>(gs.take(i));
    let rest = groups_bytes::<O>(gs.skip(i + 1));
    lemma_slab::<O>(data);
    lemma_groups_bytes_at::<O>(gs, i);
    assert(vs =~= vs.take(j) + vs.skip(j));
    lemma_encs_concat::<O>(vs.take(j), vs.skip(j));
    lemma_head_take(head, lit_head::<O>(vs.len()), encs::<O>(vs.take(j)), encs::<O>(vs.skip(j)), rest);
}

/// Landing on a literal's first value: a lone value after the groups
/// before the literal.
#[verifier::spinoff_prover]
proof fn lemma_land_lit_first<'a, W: WriteOps<'a>>(data: Seq<u8>, gi: int, s: State<'a, W>)
    requires
        valid_slab::<W::R>(data),
        0 <= gi < slab_groups::<W::R>(data).len(), slab_groups::<W::R>(data)[gi] is Lit,
        s matches State::Lone(c) && W::val(c) == slab_groups::<W::R>(data)[gi]->values[0],
    ensures landed::<W>(data, gl(slab_groups::<W::R>(data), gi) + 1, s, gb::<W::R>(slab_groups::<W::R>(data), gi),
        gseg(slab_groups::<W::R>(data), gi)),
{
    hide(canonical);
    let gs = slab_groups::<W::R>(data);
    let vs = gs[gi]->values;
    let gs2 = gs.take(gi);
    let pre = data.take(gb::<W::R>(gs, gi));
    let e = Seq::<VOf<'a, W>>::empty();
    lemma_slab_group_ok::<W::R>(data, gi);
    lemma_lit_head_canonical::<W::R>(data, gi, 1);
    lemma_lit_head_values::<W::R>(data, gi, 1);
    lemma_take_groups::<W::R>(data, gi);
    assert(vs.take(1) =~= seq![vs[0]]);
    assert(pre + Seq::<u8>::empty().skip(0) =~= pre);
    assert(rel(s, pre, Seq::empty(), 0, gs2, e, 0));
    assert(values_of(s, gs2, e) == slab_values::<W::R>(data).take(gl(gs, gi) + 1));
    assert(acc(s, gs2, e) == gseg(gs, gi));
}

/// Landing on a later value of a literal: a literal in progress under
/// the slab's header, the values before the one landed on written.
#[verifier::spinoff_prover]
proof fn lemma_land_lit_later<'a, W: WriteOps<'a>>(data: Seq<u8>, gi: int, jj: int, s: State<'a, W>, bs: int)
    requires
        valid_slab::<W::R>(data),
        0 <= gi < slab_groups::<W::R>(data).len(), slab_groups::<W::R>(data)[gi] is Lit,
        0 < jj < slab_groups::<W::R>(data)[gi]->values.len(),
        bs == gb::<W::R>(slab_groups::<W::R>(data), gi)
            + lit_head::<W::R>(slab_groups::<W::R>(data)[gi]->values.len()).len()
            + encs::<W::R>(slab_groups::<W::R>(data)[gi]->values.take(jj)).len(),
        s matches State::Lit { count, local, header_pos, current, .. }
            && count == jj && local == 0 && header_pos == gb::<W::R>(slab_groups::<W::R>(data), gi)
            && W::val(current) == slab_groups::<W::R>(data)[gi]->values[jj],
    ensures landed::<W>(data, gl(slab_groups::<W::R>(data), gi) + jj + 1, s, bs, gseg(slab_groups::<W::R>(data), gi) + jj),
{
    hide(canonical);
    let gs = slab_groups::<W::R>(data);
    let vs = gs[gi]->values;
    let gs2 = gs.take(gi);
    let ws = vs.take(jj);
    let h0 = -(vs.len() as int) as i64;
    let pre = data.take(bs);
    lemma_lit_head_canonical::<W::R>(data, gi, jj + 1);
    lemma_lit_head_values::<W::R>(data, gi, jj + 1);
    lemma_lit_head_bytes::<W::R>(data, gi, jj);
    assert(ws.push(vs[jj]) =~= vs.take(jj + 1));
    assert(ws.take(jj) =~= ws);
    assert(ws.skip(jj) =~= Seq::<VOf<'a, W>>::empty());
    assert(Seq::<u8>::empty().skip(0) =~= encs::<W::R>(ws.skip(jj)));
    assert(rel(s, pre, Seq::empty(), 0, gs2, ws, h0));
    assert(values_of(s, gs2, ws) == slab_values::<W::R>(data).take(gl(gs, gi) + jj + 1));
    assert(acc(s, gs2, ws) == gseg(gs, gi) + jj);
}

/// Landing in a literal at value `jj`: the values before it and it.
#[verifier::spinoff_prover]
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
    if jj == 0 {
        lemma_land_lit_first::<W>(data, gi, s);
    } else {
        lemma_land_lit_later::<W>(data, gi, jj, s, bs);
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

/// A walk to `at` that group `gi` ends before stands at the next group.
proof fn lemma_walk_next<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, gi: int, pos: int, item: int, segments: int)
    requires
        walk_at::<O>(data, at, gi, pos, item, segments),
        gl(slab_groups::<O>(data), gi + 1) < at,
    ensures ({
        let gs = slab_groups::<O>(data);
        walk_at::<O>(data, at, gi + 1, gb::<O>(gs, gi + 1), gl(gs, gi + 1), segments + group_segments(gs[gi]))
    }),
{
    let gs = slab_groups::<O>(data);
    lemma_slab::<O>(data);
    lemma_slab_group_ok::<O>(data, gi);
    lemma_group_at::<O>(gs, gi);
    lemma_gseg(gs, gi);
    if gi + 1 == gs.len() {
        assert(gs.take(gi + 1) =~= gs);
    }
}

/// A walk that lands in repeat or null group `gi`: the reader's position
/// there.
proof fn lemma_walk_run_lands<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, gi: int, pos: int, item: int, segments: int, rs: RleDecoderState)
    requires
        walk_at::<O>(data, at, gi, pos, item, segments),
        match slab_groups::<O>(data)[gi] {
            Group::Run { count, .. } => rs.run matches SuspendedRun::Repeat { at: a }
                && a == pos + O::enc_signed(count as i64).len(),
            Group::Lit { .. } => false,
            Group::Null { .. } => rs.run is Null,
        },
        rs.byte_pos == gb::<O>(slab_groups::<O>(data), gi + 1),
        at + rs.remaining == gl(slab_groups::<O>(data), gi + 1),
    ensures
        state_ok::<O>(data, rs),
        state_pos::<O>(data, rs) == at,
        rsegs::<O>(data, rs) == segments + (if rs.remaining == 0 { 1int } else { 0int }),
{
    let gs = slab_groups::<O>(data);
    lemma_group_at::<O>(gs, gi);
    assert(state_at::<O>(data, rs, gi + 1));
    lemma_state_is::<O>(data, rs, gi + 1);
}

/// A walk that lands in repeat group `gi`, `k` values in: the state a
/// writer has there, which is the literal before the run and the run's
/// value where `k` is 1 and there is one, and `k` of the value otherwise.
proof fn lemma_walk_run_state<'a, W: WriteOps<'a>>(data: Seq<u8>, at: int, gi: int, pos: int, item: int, segments: int, k: int, s: State<'a, W>)
    requires
        walk_at::<W::R>(data, at, gi, pos, item, segments),
        slab_groups::<W::R>(data)[gi] is Run,
        1 <= k <= slab_groups::<W::R>(data)[gi]->Run_count, at == item + k,
        k >= 2 ==> s is Run,
        k == 1 ==> match prev_lit_of::<W::R>(data, gi) {
            Some((_, n)) => n > 0 ==> s is Lit,
            None => s is Lone,
        },
        match s {
            State::Lone(c) => k == 1 && W::val(c) == run_value(slab_groups::<W::R>(data)[gi]),
            State::Run(n, c) => n == k && W::val(c) == run_value(slab_groups::<W::R>(data)[gi]),
            State::Lit { count, local, header_pos, current, .. } => k == 1 && local == 0
                && prev_lit_of::<W::R>(data, gi) == Some((header_pos, count))
                && W::val(current) == run_value(slab_groups::<W::R>(data)[gi]),
            _ => false,
        },
    ensures landed::<W>(data, at, s, pos, segments),
{
    let gs = slab_groups::<W::R>(data);
    if k == 1 && prev_lit_of::<W::R>(data, gi) is Some {
        lemma_slab_group_ok::<W::R>(data, gi - 1);
        lemma_slab_group_bytes::<W::R>(data, gi - 1);
        lemma_slab_lit_end::<W::R>(data, gi - 1);
        lemma_land_run_lit::<W>(data, gi, s);
    } else {
        lemma_land_run::<W>(data, gi, k, s);
    }
}

/// A null run's count, read after its header, and where the run ends.
proof fn lemma_walk_null_count<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int, pos: int, hb: int, cb: int, c: u64)
    requires
        valid_slab::<O>(data), 0 <= gi < slab_groups::<O>(data).len(),
        slab_groups::<O>(data)[gi] is Null,
        pos == gb::<O>(slab_groups::<O>(data), gi),
        hb == O::enc_signed(0).len(),
        pos + hb <= data.len(),
        data.skip(pos + hb) == payload::<O>(slab_groups::<O>(data)[gi])
            + groups_bytes::<O>(slab_groups::<O>(data).skip(gi + 1)),
        0 <= cb <= data.len() - (pos + hb),
        data.skip(pos + hb).take(cb) == O::enc_unsigned(c),
    ensures ({
        let gs = slab_groups::<O>(data);
        &&& c == gs[gi]->Null_count
        &&& 1 <= c <= usize::MAX
        &&& pos + hb + cb == gb::<O>(gs, gi + 1)
        &&& gl(gs, gi + 1) == gl(gs, gi) + c
    }),
{
    let gs = slab_groups::<O>(data);
    let g = gs[gi];
    lemma_slab_group_ok::<O>(data, gi);
    lemma_slab_group_bytes::<O>(data, gi);
    lemma_slab_group_values::<O>(data, gi);
    lemma_count::<O>(data.skip(pos + hb), g->Null_count as u64, groups_bytes::<O>(gs.skip(gi + 1)), cb, c);
}

/// A walk that lands in literal group `gi` after its value `jj`: the
/// reader's position there.
proof fn lemma_walk_lit_lands<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, gi: int, pos: int, item: int, segments: int, jj: int, rs: RleDecoderState)
    requires
        walk_at::<O>(data, at, gi, pos, item, segments),
        slab_groups::<O>(data)[gi] is Lit,
        0 <= jj < slab_groups::<O>(data)[gi]->values.len(),
        at == item + jj + 1,
        rs.run is Literal,
        rs.remaining == slab_groups::<O>(data)[gi]->values.len() - jj - 1,
        rs.byte_pos == pos + lit_head::<O>(slab_groups::<O>(data)[gi]->values.len()).len()
            + encs::<O>(slab_groups::<O>(data)[gi]->values.take(jj + 1)).len(),
    ensures
        state_ok::<O>(data, rs),
        state_pos::<O>(data, rs) == at,
        rsegs::<O>(data, rs) == segments + jj + 1,
{
    let gs = slab_groups::<O>(data);
    let vs = gs[gi]->values;
    lemma_group_at::<O>(gs, gi);
    assert(vs.take(vs.len() - rs.remaining) == vs.take(jj + 1));
    assert(state_at::<O>(data, rs, gi + 1));
    lemma_state_is::<O>(data, rs, gi + 1);
}

/// Repeat group `i` of a valid slab: its count and value, and that it
/// ends after its header and its value.
proof fn lemma_slab_run_end<'a, O: RleOps<'a>>(data: Seq<u8>, i: int)
    requires
        valid_slab::<O>(data), 0 <= i < slab_groups::<O>(data).len(),
        slab_groups::<O>(data)[i] is Run,
    ensures ({
        let gs = slab_groups::<O>(data);
        let g = gs[i];
        &&& 2 <= g->Run_count <= i64::MAX
        &&& !O::is_null(run_value(g))
        &&& payload::<O>(g) == O::enc(run_value(g))
        &&& gb::<O>(gs, i + 1) == gb::<O>(gs, i) + O::enc_signed(g->Run_count as i64).len()
            + O::enc(run_value(g)).len()
        &&& gl(gs, i + 1) == gl(gs, i) + g->Run_count
    }),
{
    let gs = slab_groups::<O>(data);
    lemma_slab_group_ok::<O>(data, i);
    lemma_slab_group_bytes::<O>(data, i);
    lemma_group_at::<O>(gs, i);
}

/// Literal group `i` of a valid slab ends after its header and all its
/// values.
proof fn lemma_slab_lit_end<'a, O: RleOps<'a>>(data: Seq<u8>, i: int)
    requires
        valid_slab::<O>(data), 0 <= i < slab_groups::<O>(data).len(),
        slab_groups::<O>(data)[i] is Lit,
    ensures ({
        let gs = slab_groups::<O>(data);
        let vs = gs[i]->values;
        &&& gb::<O>(gs, i + 1) == gb::<O>(gs, i) + lit_head::<O>(vs.len()).len()
            + encs::<O>(vs.take(vs.len() as int)).len()
        &&& gl(gs, i + 1) == gl(gs, i) + vs.len()
        &&& vs.len() <= usize::MAX
    }),
{
    let gs = slab_groups::<O>(data);
    let vs = gs[i]->values;
    lemma_slab_group_bytes::<O>(data, i);
    lemma_slab_group_values::<O>(data, i);
    assert(vs.take(vs.len() as int) =~= vs);
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
    hide(landed);
    hide(canonical);
    hide(group_ok);
    let ghost gs = slab_groups::<W::R>(data@);
    let ghost g = gs[gi];
    let ghost rest = groups_bytes::<W::R>(gs.skip(gi + 1));
    let ghost p = pos + hb;
    proof {
        lemma_slab_run_end::<W::R>(data@, gi);
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
            lemma_walk_run_state::<W>(data@, at as int, gi, pos as int, item as int, segments as int, k as int, state);
            lemma_walk_run_lands::<W::R>(data@, at as int, gi, pos as int, item as int, segments as int, rs);
        }
        let read_segments = if k == count { segments + 1 } else { segments };
        return Walk::Landed(PartitionAt { state, segments, byte_start: pos, read_segments, read_state: rs });
    }
    let vlen = <W::R as RleOps<'a>>::value_len(rest1).unwrap();
    proof {
        lemma_walk_next::<W::R>(data@, at as int, gi, pos as int, item as int, segments as int);
    }
    Walk::Next { pos: value_start + vlen, item: item + count, segments: segments + 1, prev_lit: None }
}

/// A null run: land in it, or step past it.
#[verifier::spinoff_prover]
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
    hide(landed);
    hide(canonical);
    hide(group_ok);
    let ghost gs = slab_groups::<W::R>(data@);
    let ghost g = gs[gi];
    let ghost rest = groups_bytes::<W::R>(gs.skip(gi + 1));
    let ghost p = pos + hb;
    proof {
        lemma_starts(<W::R as RleOps<'a>>::enc_unsigned(g->Null_count as u64), rest);
    }
    let rest1 = &data[pos + hb..];
    assert(rest1@ == data@.skip(p));
    let (cb, c) = <W::R as RleOps<'a>>::read_unsigned_opt(rest1).unwrap();
    proof {
        lemma_walk_null_count::<W::R>(data@, gi, pos as int, hb as int, cb as int, c);
    }
    let count = c as usize;
    if at - item <= count {
        let k = at - item;
        let rs = RleDecoderState { byte_pos: pos + hb + cb, remaining: count - k, run: SuspendedRun::Null };
        proof {
            lemma_land_null::<W>(data@, gi, k as int, State::Null(k));
            lemma_walk_run_lands::<W::R>(data@, at as int, gi, pos as int, item as int, segments as int, rs);
        }
        let read_segments = if k == count { segments + 1 } else { segments };
        return Walk::Landed(PartitionAt { state: State::Null(k), segments, byte_start: pos, read_segments, read_state: rs });
    }
    proof {
        lemma_walk_next::<W::R>(data@, at as int, gi, pos as int, item as int, segments as int);
    }
    Walk::Next { pos: pos + hb + cb, item: item + count, segments: segments + 1, prev_lit: None }
}

/// Literal value `j` of group `i` of a valid slab starts at byte `q`: its
/// encoding starts the bytes there, and what follows it starts where
/// that ends.
proof fn lemma_lit_step<'a, O: RleOps<'a>>(data: Seq<u8>, i: int, j: int, q: int)
    requires
        valid_slab::<O>(data), 0 <= i < slab_groups::<O>(data).len(),
        slab_groups::<O>(data)[i] is Lit,
        0 <= j < slab_groups::<O>(data)[i]->values.len(),
        q == gb::<O>(slab_groups::<O>(data), i) + lit_head::<O>(slab_groups::<O>(data)[i]->values.len()).len()
            + encs::<O>(slab_groups::<O>(data)[i]->values.take(j)).len(),
    ensures ({
        let gs = slab_groups::<O>(data);
        let vs = gs[i]->values;
        &&& !O::is_null(vs[j])
        &&& 0 <= q && q + O::enc(vs[j]).len() <= data.len()
        &&& O::enc(vs[j]).is_prefix_of(data.skip(q))
        &&& data.skip(q) == O::enc(vs[j]) + (encs::<O>(vs.skip(j + 1)) + groups_bytes::<O>(gs.skip(i + 1)))
        &&& q + O::enc(vs[j]).len() == gb::<O>(gs, i) + lit_head::<O>(vs.len()).len() + encs::<O>(vs.take(j + 1)).len()
    }),
{
    let gs = slab_groups::<O>(data);
    let vs = gs[i]->values;
    lemma_slab_lit::<O>(data, i, j);
    lemma_starts(O::enc(vs[j]), encs::<O>(vs.skip(j + 1)) + groups_bytes::<O>(gs.skip(i + 1)));
}

/// No values take no bytes.
proof fn lemma_encs_none<'a, O: RleOps<'a>>(vs: Seq<O::V>)
    ensures encs::<O>(vs.take(0)).len() == 0,
{
    assert(vs.take(0) =~= Seq::<O::V>::empty());
}

/// A literal run: land in it, or step past it.
#[verifier::spinoff_prover]
fn walk_lit<'a, W: WriteOps<'a>>(data: &'a [u8], at: usize, Ghost(gi): Ghost<int>, pos: usize, item: usize, segments: usize, hb: usize, total: usize) -> (r: Walk<'a, W>)
    requires
        walk_at::<W::R>(data@, at as int, gi, pos as int, item as int, segments as int),
        slab_groups::<W::R>(data@)[gi] is Lit, total == group_len(slab_groups::<W::R>(data@)[gi]),
        hb == lit_head::<W::R>(total as nat).len(),
        pos + hb <= data@.len(),
    ensures walk_post::<W>(data@, at as int, gi, r),
{
    hide(landed);
    hide(canonical);
    hide(gseg);
    hide(group_ok);
    hide(valid_slab);
    hide(slab_groups);
    hide(slab_values);
    hide(gb);
    hide(gl);
    hide(lit_head);
    hide(encs);
    hide(groups_bytes);
    hide(groups_len);
    hide(state_ok);
    hide(state_pos);
    hide(rsegs);
    let ghost gs = slab_groups::<W::R>(data@);
    let ghost g = gs[gi];
    let ghost vs = g->values;
    proof {
        lemma_encs_none::<W::R>(vs);
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
        proof {
            lemma_lit_step::<W::R>(data@, gi, j as int, q as int);
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
            lemma_lit_step::<W::R>(data@, gi, jj as int, q as int);
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
            lemma_land_lit::<W>(data@, gi, jj as int, state, byte_start as int);
            lemma_walk_lit_lands::<W::R>(data@, at as int, gi, pos as int, item as int, segments as int, jj as int, rs);
        }
        return Walk::Landed(PartitionAt { state, segments: segments + jj, byte_start, read_segments: segments + jj + 1, read_state: rs });
    }
    proof {
        lemma_slab_lit_end::<W::R>(data@, gi);
        lemma_walk_next::<W::R>(data@, at as int, gi, pos as int, item as int, segments as int);
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

/// The last value of a slab that ends in a literal of `n` values, the
/// last `tl` bytes wide: where it sits, and what the slab holds with it.
proof fn lemma_tail_lit<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int, pos: int, hb: int, n: int, tl: int)
    requires
        last_group_at::<O>(data, gi, pos, hb),
        slab_groups::<O>(data)[gi] is Lit, n == -header_of(slab_groups::<O>(data)[gi]),
        tl == lit_tail_of::<O>(slab_groups::<O>(data)),
    ensures ({
        let gs = slab_groups::<O>(data);
        let vs = gs[gi]->values;
        let vp = data.len() - tl;
        &&& n == vs.len() && 1 <= n <= usize::MAX
        &&& !O::is_null(vs[n - 1])
        &&& pos <= vp <= data.len()
        &&& vp == pos + lit_head::<O>(vs.len()).len() + encs::<O>(vs.take(n - 1)).len()
        &&& data.skip(vp) == O::enc(vs[n - 1]) + Seq::<u8>::empty()
        &&& gl(gs, gi) + n == slab_values::<O>(data).len()
        &&& gseg(gs, gi) + n == groups_segments(gs)
    }),
{
    let gs = slab_groups::<O>(data);
    let g = gs[gi];
    let vs = g->values;
    lemma_slab::<O>(data);
    lemma_slab_group_ok::<O>(data, gi);
    lemma_slab_lit_end::<O>(data, gi);
    lemma_slab_lit::<O>(data, gi, n - 1);
    assert(gs.last() == g);
    assert(gs.take(gi + 1) =~= gs);
    assert(vs.take(n) =~= vs);
    assert(vs.skip(n) =~= Seq::<O::V>::empty());
    assert(gs.skip(gi + 1) =~= Seq::<Group<O::V>>::empty());
    assert(encs::<O>(vs.skip(n)) + groups_bytes::<O>(gs.skip(gi + 1)) =~= Seq::<u8>::empty());
}

/// The end of a slab whose last group is a literal of `n` values, the
/// last `tl` bytes wide: the state, and where its bytes start.
#[verifier::spinoff_prover]
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
        lemma_tail_lit::<W::R>(data@, gi, pos as int, hb, n as int, tl as int);
        lemma_starts(<W::R as RleOps<'a>>::enc(vs[n - 1]), Seq::empty());
    }
    let value_pos = data.len() - tl;
    let vrest = &data[value_pos..];
    assert(vrest@ == data@.skip(value_pos as int));
    let (_vlen, value) = <W::R as RleOps<'a>>::unpack(vrest);
    proof {
        lemma_value::<W::R>(data@.skip(value_pos as int), vs[n - 1], Seq::empty(), _vlen as int, value);
    }
    let s = State::lit(n - 1, W::cow_ref(value), pos);
    let bs = if n == 1 { pos } else { value_pos };
    proof {
        lemma_land_lit::<W>(data@, gi, n - 1, s, bs as int);
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

/// Group boundary `gi` is the slab's end exactly when no group follows
/// it, and then nothing follows a reader there.
proof fn lemma_postfix_none<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int)
    requires valid_slab::<O>(data), 0 <= gi <= slab_groups::<O>(data).len(),
    ensures ({
        let gs = slab_groups::<O>(data);
        &&& 0 <= gb::<O>(gs, gi) <= data.len()
        &&& gb::<O>(gs, gi) == data.len() <==> gi == gs.len()
        &&& gi == gs.len() ==> postfix_ok::<O>(data, gl(gs, gi), None, data.len() as int)
            && gseg(gs, gi) + rest_segs::<O>(data, gl(gs, gi), None, data.len() as int) == groups_segments(gs)
    }),
{
    let gs = slab_groups::<O>(data);
    lemma_slab::<O>(data);
    if gi < gs.len() {
        lemma_slab_group::<O>(data, gi);
    } else {
        assert(gs.take(gi) =~= gs);
    }
}

/// The header `hv` of group `gi` tells its kind by its sign, and at `at`,
/// where the header ends, comes a repeat run's value or a null run's count.
proof fn lemma_header_kind<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int, at: int, hv: i64)
    requires
        valid_slab::<O>(data), 0 <= gi < slab_groups::<O>(data).len(),
        hv as int == header_of(slab_groups::<O>(data)[gi]),
        data.skip(at) == payload::<O>(slab_groups::<O>(data)[gi]) + groups_bytes::<O>(slab_groups::<O>(data).skip(gi + 1)),
    ensures ({
        let g = slab_groups::<O>(data)[gi];
        &&& hv < 0 ==> g is Lit
        &&& hv > 0 ==> g is Run && !O::is_null(run_value(g)) && O::enc(run_value(g)).is_prefix_of(data.skip(at))
        &&& hv == 0 ==> g is Null && O::nullable() && O::enc_unsigned(g->Null_count as u64).is_prefix_of(data.skip(at))
    }),
{
    let gs = slab_groups::<O>(data);
    let rest = groups_bytes::<O>(gs.skip(gi + 1));
    lemma_slab_group_ok::<O>(data, gi);
    match gs[gi] {
        Group::Run { value, .. } => {
            lemma_starts(O::enc(value), rest);
        },
        Group::Null { count } => {
            lemma_starts(O::enc_unsigned(count as u64), rest);
        },
        Group::Lit { .. } => {},
    }
}

/// A run tail at group `i` as a postfix, and the slab's segments around
/// it.
proof fn lemma_postfix_run_segs<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, count: usize, value: O::V, end: int, i: int)
    requires valid_slab::<O>(data), tail_run::<O>(data, at, count as nat, value, end, i),
    ensures
        postfix_ok::<O>(data, at, Some(Postfix::Run { count, value }), end),
        gseg(slab_groups::<O>(data), i) + 1 + rest_segs::<O>(data, at, Some(Postfix::Run { count, value }), end)
            == groups_segments(slab_groups::<O>(data)),
{
    let gs = slab_groups::<O>(data);
    lemma_postfix_run::<O>(data, at, count, value, end, i);
    lemma_rest_run::<O>(data, at, count, value, end, i);
    lemma_gseg(gs, i);
    lemma_gseg_split::<O>(data, i + 1);
}

/// A literal tail at value `j` of group `i` as a postfix, and the slab's
/// segments around it.
proof fn lemma_postfix_lit_segs<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, value: O::V, lit: usize, end: int, i: int, j: int)
    requires valid_slab::<O>(data), tail_lit::<O>(data, at, value, lit as nat, end, i, j),
    ensures
        postfix_ok::<O>(data, at, Some(Postfix::Lit { value, lit }), end),
        gseg(slab_groups::<O>(data), i) + j + 1 + rest_segs::<O>(data, at, Some(Postfix::Lit { value, lit }), end)
            == groups_segments(slab_groups::<O>(data)),
{
    let gs = slab_groups::<O>(data);
    lemma_postfix_lit::<O>(data, at, value, lit, end, i, j);
    lemma_rest_lit::<O>(data, at, value, lit, end, i, j);
    lemma_gseg(gs, i);
    lemma_gseg_split::<O>(data, i + 1);
}

/// Repeat run `gi`, read whole: its `hb`-byte header `hv` at `pos`, then
/// its value in `vlen` bytes. The postfix it is, and the slab's segments
/// around it.
proof fn lemma_postfix_run_group<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int, pos: int, hb: int, hv: i64, vlen: int, value: O::V)
    requires
        valid_slab::<O>(data), 0 <= gi < slab_groups::<O>(data).len(),
        pos == gb::<O>(slab_groups::<O>(data), gi),
        hv > 0, hv as int == header_of(slab_groups::<O>(data)[gi]),
        hb == O::enc_signed(hv).len(),
        data.skip(pos + hb) == payload::<O>(slab_groups::<O>(data)[gi]) + groups_bytes::<O>(slab_groups::<O>(data).skip(gi + 1)),
        0 <= vlen <= data.skip(pos + hb).len(),
        data.skip(pos + hb).take(vlen) == O::enc(value), !O::is_null(value),
    ensures ({
        let gs = slab_groups::<O>(data);
        let p = Some(Postfix::Run { count: hv as usize, value });
        &&& postfix_ok::<O>(data, gl(gs, gi), p, pos + hb + vlen)
        &&& gseg(gs, gi) + 1 + rest_segs::<O>(data, gl(gs, gi), p, pos + hb + vlen) == groups_segments(gs)
    }),
{
    let gs = slab_groups::<O>(data);
    let g = gs[gi];
    lemma_slab_group::<O>(data, gi);
    lemma_value::<O>(data.skip(pos + hb), run_value(g), groups_bytes::<O>(gs.skip(gi + 1)), vlen, value);
    assert(tail_run::<O>(data, gl(gs, gi), (hv as usize) as nat, value, pos + hb + vlen, gi));
    lemma_postfix_run_segs::<O>(data, gl(gs, gi), hv as usize, value, pos + hb + vlen, gi);
}

/// Null run `gi`, read whole: its `hb`-byte header at `pos`, then its
/// count `c` in `cb` bytes. The postfix it is, and the slab's segments
/// around it.
proof fn lemma_postfix_null_group<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int, pos: int, hb: int, cb: int, c: u64, value: O::V)
    requires
        valid_slab::<O>(data), 0 <= gi < slab_groups::<O>(data).len(),
        slab_groups::<O>(data)[gi] is Null,
        pos == gb::<O>(slab_groups::<O>(data), gi),
        hb == O::enc_signed(0).len(),
        pos + hb <= data.len(),
        data.skip(pos + hb) == payload::<O>(slab_groups::<O>(data)[gi]) + groups_bytes::<O>(slab_groups::<O>(data).skip(gi + 1)),
        0 <= cb <= data.len() - (pos + hb),
        data.skip(pos + hb).take(cb) == O::enc_unsigned(c),
        value == O::null(),
    ensures ({
        let gs = slab_groups::<O>(data);
        let p = Some(Postfix::Run { count: c as usize, value });
        &&& postfix_ok::<O>(data, gl(gs, gi), p, pos + hb + cb)
        &&& gseg(gs, gi) + 1 + rest_segs::<O>(data, gl(gs, gi), p, pos + hb + cb) == groups_segments(gs)
    }),
{
    let gs = slab_groups::<O>(data);
    lemma_walk_null_count::<O>(data, gi, pos, hb, cb, c);
    assert(tail_run::<O>(data, gl(gs, gi), (c as usize) as nat, value, pos + hb + cb, gi));
    lemma_postfix_run_segs::<O>(data, gl(gs, gi), c as usize, value, pos + hb + cb, gi);
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
    hide(valid_slab);
    hide(slab_groups);
    hide(postfix_ok);
    hide(rest_segs);
    hide(tail_run);
    hide(tail_lit);
    hide(gb);
    hide(gl);
    hide(gseg);
    hide(groups_segments);
    hide(groups_bytes);
    hide(payload);
    hide(header_of);
    let ghost gs = slab_groups::<O>(data@);
    proof {
        lemma_postfix_none::<O>(data@, gi);
    }
    if pos == data.len() {
        return (None, pos);
    }
    let (hb, hv) = group_header::<O>(data, pos, Ghost(gi));
    proof {
        lemma_header_kind::<O>(data@, gi, pos + hb, hv);
    }
    if hv < 0 {
        let (value, lit, end) = first_lit::<O>(data, pos, hb, hv, Ghost(gi));
        proof {
            lemma_postfix_lit_segs::<O>(data@, gl(gs, gi), value, lit, end as int, gi, 0);
        }
        return (Some(Postfix::Lit { value, lit }), end);
    }
    if hv > 0 {
        let vrest = &data[pos + hb..];
        assert(vrest@ == data@.skip(pos + hb));
        let (vlen, value) = O::unpack(vrest);
        let count = hv as usize;
        let end = pos + hb + vlen;
        let p = Some(Postfix::Run { count, value });
        proof {
            lemma_postfix_run_group::<O>(data@, gi, pos as int, hb as int, hv, vlen as int, value);
        }
        return (p, end);
    }
    let crest = &data[pos + hb..];
    assert(crest@ == data@.skip(pos + hb));
    let (cb, c) = O::read_unsigned_opt(crest).unwrap();
    let value = O::get_null();
    let count = c as usize;
    let end = pos + hb + cb;
    proof {
        lemma_postfix_null_group::<O>(data@, gi, pos as int, hb as int, cb as int, c, value);
    }
    (Some(Postfix::Run { count, value }), end)
}

/// Repeat run `gi - 1`, whose value starts at `vat`: the value's
/// encoding starts the bytes there, and the run ends at the slab's end
/// exactly when no group follows it.
#[verifier::spinoff_prover]
proof fn lemma_in_run<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int, vat: int)
    requires
        valid_slab::<O>(data), 1 <= gi <= slab_groups::<O>(data).len(),
        slab_groups::<O>(data)[gi - 1] is Run,
        vat == gb::<O>(slab_groups::<O>(data), gi - 1)
            + O::enc_signed(slab_groups::<O>(data)[gi - 1]->Run_count as i64).len(),
    ensures ({
        let gs = slab_groups::<O>(data);
        let v = run_value(gs[gi - 1]);
        &&& 0 <= vat <= data.len()
        &&& data.skip(vat) == O::enc(v) + groups_bytes::<O>(gs.skip(gi))
        &&& !O::is_null(v) && O::enc(v).is_prefix_of(data.skip(vat))
        &&& 0 <= gb::<O>(gs, gi) <= data.len()
        &&& gb::<O>(gs, gi) == data.len() <==> gi == gs.len()
    }),
{
    let gs = slab_groups::<O>(data);
    let g = gs[gi - 1];
    let rest = groups_bytes::<O>(gs.skip(gi));
    let h = O::enc_signed(g->Run_count as i64);
    lemma_slab::<O>(data);
    lemma_slab_group::<O>(data, gi - 1);
    lemma_skip_skip(data, gb::<O>(gs, gi - 1), h.len() as int);
    assert((h + (O::enc(run_value(g)) + rest)).skip(h.len() as int) =~= O::enc(run_value(g)) + rest);
    lemma_starts(O::enc(run_value(g)), rest);
    if gi < gs.len() {
        lemma_slab_group::<O>(data, gi);
    } else {
        assert(gs.take(gi) =~= gs);
    }
}

/// The last `r` values of repeat run `gi - 1`, which no literal takes the
/// last of in: the postfix they are, and the slab's segments around it.
proof fn lemma_postfix_in_run<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int, r: usize, value: O::V)
    requires
        valid_slab::<O>(data), 1 <= gi <= slab_groups::<O>(data).len(),
        slab_groups::<O>(data)[gi - 1] is Run,
        1 <= r <= slab_groups::<O>(data)[gi - 1]->Run_count,
        value == run_value(slab_groups::<O>(data)[gi - 1]),
        r == 1 && gi < slab_groups::<O>(data).len() ==> !(slab_groups::<O>(data)[gi] is Lit),
    ensures ({
        let gs = slab_groups::<O>(data);
        let p = Some(Postfix::Run { count: r, value });
        &&& postfix_ok::<O>(data, gl(gs, gi) - r, p, gb::<O>(gs, gi))
        &&& gseg(gs, gi - 1) + 1 + rest_segs::<O>(data, gl(gs, gi) - r, p, gb::<O>(gs, gi)) == groups_segments(gs)
    }),
{
    let gs = slab_groups::<O>(data);
    assert(tail_run::<O>(data, gl(gs, gi) - r, r as nat, value, gb::<O>(gs, gi), gi - 1));
    lemma_postfix_run_segs::<O>(data, gl(gs, gi) - r, r, value, gb::<O>(gs, gi), gi - 1);
}

/// The last value `lone` of repeat run `gi - 1`, and a literal tail at
/// the first value of group `gi` after it: the postfix they are, and the
/// slab's segments around it.
proof fn lemma_postfix_lone_segs<'a, O: RleOps<'a>>(data: Seq<u8>, gi: int, lone: O::V, value: O::V, lit: usize, end: int)
    requires
        valid_slab::<O>(data), 1 <= gi < slab_groups::<O>(data).len(),
        slab_groups::<O>(data)[gi - 1] is Run,
        lone == run_value(slab_groups::<O>(data)[gi - 1]),
        tail_lit::<O>(data, gl(slab_groups::<O>(data), gi), value, lit as nat, end, gi, 0),
    ensures ({
        let gs = slab_groups::<O>(data);
        let p = Some(Postfix::LonePlusLit { lone, value, lit });
        &&& postfix_ok::<O>(data, gl(gs, gi) - 1, p, end)
        &&& gseg(gs, gi - 1) + 2 + rest_segs::<O>(data, gl(gs, gi) - 1, p, end) == groups_segments(gs)
    }),
{
    let gs = slab_groups::<O>(data);
    let at = gl(gs, gi) - 1;
    assert(tail_lone::<O>(data, at, lone, value, lit as nat, end, gi - 1));
    lemma_postfix_lone::<O>(data, at, lone, value, lit, end, gi - 1);
    lemma_rest_lone::<O>(data, at, lone, value, lit, end, gi - 1);
    lemma_gseg(gs, gi - 1);
    lemma_gseg(gs, gi);
    lemma_gseg_split::<O>(data, gi + 1);
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
    hide(valid_slab);
    hide(slab_groups);
    hide(postfix_ok);
    hide(rest_segs);
    hide(tail_run);
    hide(tail_lit);
    hide(tail_lone);
    hide(gb);
    hide(gl);
    hide(gseg);
    hide(groups_segments);
    hide(groups_bytes);
    hide(payload);
    hide(header_of);
    let ghost gs = slab_groups::<O>(data@);
    let ghost g = gs[gi - 1];
    let ghost rest = groups_bytes::<O>(gs.skip(gi));
    proof {
        lemma_in_run::<O>(data@, gi, vat as int);
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
        proof {
            lemma_header_kind::<O>(data@, gi, pos + hb, hv);
        }
        if hv < 0 {
            let (v2, lit, end) = first_lit::<O>(data, pos, hb, hv, Ghost(gi));
            let p = Some(Postfix::LonePlusLit { lone: value, value: v2, lit });
            proof {
                lemma_postfix_lone_segs::<O>(data@, gi, value, v2, lit, end as int);
            }
            return (p, end);
        }
    }
    proof {
        lemma_postfix_in_run::<O>(data@, gi, r, value);
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

/// A reader standing in group `g` that has read every value: `g` is past
/// the last group, at the end of the bytes, and nothing of its own group
/// is left.
proof fn lemma_end_group<'a, O: RleOps<'a>>(data: Seq<u8>, st: RleDecoderState, g: int)
    requires
        valid_slab::<O>(data),
        state_at::<O>(data, st, g),
        gl(slab_groups::<O>(data), g) - st.remaining == slab_values::<O>(data).len(),
    ensures
        g == slab_groups::<O>(data).len(),
        st.remaining == 0,
        gb::<O>(slab_groups::<O>(data), g) == data.len(),
{
    hide(gl);
    hide(gb);
    hide(canonical);
    hide(group_ok);
    hide(adjacent_ok);
    hide(group_bytes);
    hide(payload);
    hide(header_of);
    hide(group_values);
    hide(slab_values);
    hide(encs);
    let gs = slab_groups::<O>(data);
    let n = gs.len() as int;
    lemma_slab::<O>(data);
    assert(gl(gs, n) == groups_len(gs) && gb::<O>(gs, n) == data.len()) by {
        reveal(gl);
        reveal(gb);
        assert(gs.take(n) =~= gs);
    }
    if g < n {
        // a group after it would hold values not read
        lemma_slab_group::<O>(data, g);
        lemma_gl_le::<O>(data, g + 1, n);
        assert(group_len(gs[g]) >= 1) by {
            reveal(group_ok);
        }
        assert(false);
    }
}

/// A literal read to its end: the reader stands where the next group
/// starts.
proof fn lemma_lit_end<'a, O: RleOps<'a>>(data: Seq<u8>, g: int)
    requires
        valid_slab::<O>(data),
        1 <= g <= slab_groups::<O>(data).len(),
        slab_groups::<O>(data)[g - 1] is Lit,
    ensures ({
        let gs = slab_groups::<O>(data);
        let vs = gs[g - 1]->values;
        gb::<O>(gs, g - 1) + lit_head::<O>(vs.len()).len() + encs::<O>(vs.take(vs.len() as int)).len() == gb::<O>(
            gs,
            g,
        )
    }),
{
    hide(gl);
    hide(gb);
    hide(group_ok);
    hide(adjacent_ok);
    hide(group_bytes);
    hide(group_values);
    hide(slab_values);
    hide(encs);
    let gs = slab_groups::<O>(data);
    let vs = gs[g - 1]->values;
    lemma_slab_group::<O>(data, g - 1);
    assert(vs.take(vs.len() as int) =~= vs);
    assert(group_bytes::<O>(gs[g - 1]).len() == lit_head::<O>(vs.len()).len() + encs::<O>(vs).len());
}

/// A reader that has read every value stands at the end of the bytes.
#[verifier::spinoff_prover]
pub(crate) proof fn lemma_sbyte_end<'a, O: RleOps<'a>>(data: Seq<u8>, st: RleDecoderState)
    requires state_ok::<O>(data, st), state_pos::<O>(data, st) == slab_values::<O>(data).len(),
    ensures sbyte(st) == data.len(),
{
    hide(gl);
    hide(gb);
    hide(encs);
    hide(lit_head);
    hide(slab_values);
    let g = choose|gi: int| state_at::<O>(data, st, gi);
    assert(state_at::<O>(data, st, g));
    lemma_end_group::<O>(data, st, g);
    if st.run is Literal {
        lemma_lit_end::<O>(data, g);
    }
}


// ── The column iterator's decoder model ────────────────────────────────────

/// An RLE slab decoder, verified as the column iterator's `Decode`.
pub struct RDecoder<'a, O: RleOps<'a>> {
    pub(crate) inner: Dec<'a, O>,
}

/// RLE slabs' decoders as the column iterator sees them (`Decoders`),
/// verified against the RLE format: a slab is well-formed when
/// `slab_ok`, and holds the values its bytes encode. `O` is the column's
/// value ops: one type for every lifetime a slab is borrowed for.
///
/// Public only because the verified API's value models name it; this
/// module is not.
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

    #[inline(always)]
    fn d_drain(&mut self, out: &mut Vec<O::V>) {
        self.inner.drain_into(out);
        proof {
            let v = self.inner.values();
            assert(v.skip(self.inner.pos()) =~= Seq::<O::V>::empty());
        }
    }
}

/// The empty slab is valid, with no values.
pub(crate) proof fn lemma_empty_slab<'a, O: RleOps<'a>>()
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
        let empty: &'a [u8] = no_bytes();
        proof {
            lemma_empty_slab::<O>();
            assert(empty@ =~= Seq::<u8>::empty());
        }
        RDecoder { inner: Dec::new(empty) }
    }

    /// One segment is one value, however many times.
    #[inline(always)]
    fn maybe_uniform(s: &'a Slab) -> (r: bool) {
        proof {
            if s.segments >= 2 {
                let vs = slab_values::<O>(slab_bytes(*s));
                lemma_two_segments::<O>(slab_bytes(*s));
                let i = choose|i: int| 0 < i < vs.len() && vs[i - 1] != #[trigger] vs[i];
                assert(Self::vals(*s)[i - 1] != Self::vals(*s)[i]);
            }
        }
        s.segments < 2
    }
}

impl<'a, O: RleOps<'a>> RDecoder<'a, O> {
    /// A decoder on the valid slab `data`: it has all its values to
    /// give.
    #[inline(always)]
    pub(crate) fn on(data: &'a [u8]) -> (d: Self)
        requires
            valid_slab::<O>(data@),
        ensures
            d.inv(),
            d.rest() == slab_values::<O>(data@),
        no_unwind
    {
        let d = RDecoder { inner: Dec::new(data) };
        proof {
            assert(d.inner.values().skip(0) =~= d.inner.values());
        }
        d
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
