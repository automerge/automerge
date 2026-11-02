//! Reading and checking a slab's RLE encoding, verified with Verus.
//!
//! [`Reader`] walks the wire segments of untrusted bytes and rejects
//! anything that is not the canonical encoding of some values
//! ([`model::canonical`](super::model::canonical)) with metadata that
//! fits: at most `usize::MAX` values, and no run over `u32::MAX` bytes.
//! It never panics or overflows.  Its contract is complete in both
//! directions: what it accepts parses as canonical groups, and an error
//! means no canonical parse exists (value and varint encodings are
//! prefix-free, so the parse is unique).

#![allow(clippy::len_zero)]

use super::model::*;
use crate::PackError;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// The error for input that decodes but is not canonical.
#[verifier::external_body]
fn invalid(msg: &'static str) -> PackError {
    PackError::InvalidValue(msg.to_string())
}

/// The last value-bearing segment read, which the next is checked
/// against: a repeat run's value, the last literal value read, a null
/// run, or nothing yet.
#[derive(Clone, Copy)]
#[verifier::allow(autoderive_clone_without_spec)]
pub(crate) enum Prev<V> {
    Start,
    Run(V),
    Lit(V),
    Null,
}

/// One wire segment: a repeat run, a null run, a literal run's header,
/// or one literal value; `bytes` is its encoded length.
#[derive(Clone, Copy)]
#[verifier::allow(autoderive_clone_without_spec)]
pub(crate) enum Seg<V> {
    Run { count: usize, value: V, bytes: usize },
    Null { count: usize, bytes: usize },
    LitHead { count: usize, bytes: usize },
    Lit { value: V, bytes: usize },
}

/// What `prev` should be after groups `gs` and literal values `cur`.
pub(crate) open spec fn prev_of<V>(gs: Seq<Group<V>>, cur: Seq<V>) -> Prev<V> {
    if cur.len() > 0 {
        Prev::Lit(cur.last())
    } else if gs.len() == 0 {
        Prev::Start
    } else {
        match gs.last() {
            Group::Run { value, .. } => Prev::Run(value),
            Group::Lit { values } => Prev::Lit(values.last()),
            Group::Null { .. } => Prev::Null,
        }
    }
}

/// `cur` can begin a literal after `gs`: its values are non-null with no
/// equal neighbours, and it neither follows a literal nor repeats the
/// value of a repeat run before it.
pub(crate) open spec fn lit_prefix_ok<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, cur: Seq<O::V>) -> bool {
    &&& forall|i: int| 0 <= i < cur.len() ==> !O::is_null(#[trigger] cur[i])
    &&& forall|i: int| 0 < i < cur.len() ==> cur[i - 1] != #[trigger] cur[i]
    &&& gs.len() > 0 ==> !(gs.last() is Lit)
    &&& gs.len() > 0 && cur.len() > 0 ==> match gs.last() {
        Group::Run { value, .. } => value != cur[0],
        _ => true,
    }
}

/// `s` starts with `p`.
pub open spec fn starts<T>(s: Seq<T>, p: Seq<T>) -> bool {
    p.len() <= s.len() && s.take(p.len() as int) == p
}

proof fn lemma_starts<T>(p: Seq<T>, q: Seq<T>)
    ensures starts(p + q, p), p.is_prefix_of(p + q), (p + q).skip(p.len() as int) == q,
{
    assert((p + q).take(p.len() as int) =~= p);
    assert((p + q).skip(p.len() as int) =~= q);
}

/// Reading `x` off the front of `s == p + q`: if `s` starts with `x` and
/// `x` has `p`'s length, they agree and the rest is `q`.
proof fn lemma_split<T>(s: Seq<T>, p: Seq<T>, q: Seq<T>, n: int)
    requires s == p + q, n == p.len(),
    ensures s.take(n) == p, s.skip(n) == q,
{
    assert(s.take(n) =~= p);
    assert(s.skip(n) =~= q);
}

/// A signed header read off `p + q` where `p` encodes `h` is `h`.
proof fn lemma_header<'a, O: RleOps<'a>>(s: Seq<u8>, h: i64, q: Seq<u8>, n: int, x: i64)
    requires s == O::enc_signed(h) + q, 0 <= n <= s.len(), s.take(n) == O::enc_signed(x),
    ensures x == h, n == O::enc_signed(h).len(), s.skip(n) == q,
{
    assert(s =~= s.take(n) + s.skip(n));
    O::axiom_enc_signed(x, h, s.skip(n), q);
    lemma_split(s, O::enc_signed(h), q, n);
}

/// The same, for an unsigned count.
proof fn lemma_count<'a, O: RleOps<'a>>(s: Seq<u8>, h: u64, q: Seq<u8>, n: int, x: u64)
    requires s == O::enc_unsigned(h) + q, 0 <= n <= s.len(), s.take(n) == O::enc_unsigned(x),
    ensures x == h, n == O::enc_unsigned(h).len(), s.skip(n) == q,
{
    assert(s =~= s.take(n) + s.skip(n));
    O::axiom_enc_unsigned(x, h, s.skip(n), q);
    lemma_split(s, O::enc_unsigned(h), q, n);
}

/// The same, for a non-null value.
proof fn lemma_value<'a, O: RleOps<'a>>(s: Seq<u8>, h: O::V, q: Seq<u8>, n: int, x: O::V)
    requires
        s == O::enc(h) + q, 0 <= n <= s.len(), s.take(n) == O::enc(x),
        !O::is_null(h), !O::is_null(x),
    ensures x == h, n == O::enc(h).len(), s.skip(n) == q,
{
    assert(s =~= s.take(n) + s.skip(n));
    O::axiom_enc(x, h, s.skip(n), q);
    lemma_split(s, O::enc(h), q, n);
}

/// A literal run's header, for `k` values.
pub open spec fn lit_head<'a, O: RleOps<'a>>(k: nat) -> Seq<u8> {
    O::enc_signed(-(k as int) as i64)
}

/// A group's header value.
pub open spec fn header_of<V>(g: Group<V>) -> int {
    match g {
        Group::Run { count, .. } => count as int,
        Group::Lit { values } => -(values.len() as int),
        Group::Null { .. } => 0,
    }
}

/// A group's bytes after its header.
pub open spec fn payload<'a, O: RleOps<'a>>(g: Group<O::V>) -> Seq<u8> {
    match g {
        Group::Run { value, .. } => O::enc(value),
        Group::Lit { values } => encs::<O>(values),
        Group::Null { count } => O::enc_unsigned(count as u64),
    }
}

/// The bytes of groups `gs`, then, if `k > 0`, the header of a literal
/// of `k` values and its values `cur`.
pub open spec fn read_bytes<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, k: nat, cur: Seq<O::V>) -> Seq<u8> {
    if k > 0 {
        groups_bytes::<O>(gs) + lit_head::<O>(k) + encs::<O>(cur)
    } else {
        groups_bytes::<O>(gs)
    }
}

/// No acceptable parse of `data` exists.
pub open spec fn no_parse<'a, O: RleOps<'a>>(data: Seq<u8>) -> bool {
    forall|g2: Seq<Group<O::V>>| !parses::<O>(g2, data)
}

/// Every acceptable parse of `data` begins with `gs` and, if `k > 0`, a
/// literal of `k` values beginning with `cur`.
#[verifier::opaque]
pub open spec fn complete<'a, O: RleOps<'a>>(data: Seq<u8>, gs: Seq<Group<O::V>>, k: nat, cur: Seq<O::V>) -> bool {
    forall|g2: Seq<Group<O::V>>| #[trigger] parses::<O>(g2, data) ==> {
        &&& g2.len() >= gs.len()
        &&& g2.take(gs.len() as int) == gs
        &&& k > 0 ==> g2.len() > gs.len() && g2[gs.len() as int] is Lit
            && g2[gs.len() as int]->values.len() == k
            && g2[gs.len() as int]->values.take(cur.len() as int) == cur
    }
}

/// Every acceptable parse of `data` continues `gs` with a group whose
/// header is `h` and whose payload, then the rest, starts at byte `p`.
#[verifier::opaque]
pub open spec fn header_at<'a, O: RleOps<'a>>(data: Seq<u8>, gs: Seq<Group<O::V>>, h: int, p: int) -> bool {
    forall|g2: Seq<Group<O::V>>| #[trigger] parses::<O>(g2, data) ==> {
        &&& g2.len() > gs.len()
        &&& g2.take(gs.len() as int) == gs
        &&& header_of(g2[gs.len() as int]) == h
        &&& data.skip(p) == payload::<O>(g2[gs.len() as int]) + groups_bytes::<O>(g2.skip(gs.len() as int + 1))
    }
}

/// Every acceptable parse of `data` continues `gs` with a literal of `k`
/// values beginning with `cur`, whose remaining values, then the rest,
/// start at byte `p`.
#[verifier::opaque]
pub open spec fn lit_at<'a, O: RleOps<'a>>(data: Seq<u8>, gs: Seq<Group<O::V>>, k: nat, cur: Seq<O::V>, p: int) -> bool {
    forall|g2: Seq<Group<O::V>>| #[trigger] parses::<O>(g2, data) ==> {
        &&& g2.len() > gs.len()
        &&& g2.take(gs.len() as int) == gs
        &&& g2[gs.len() as int] is Lit
        &&& g2[gs.len() as int]->values.len() == k
        &&& g2[gs.len() as int]->values.take(cur.len() as int) == cur
        &&& data.skip(p) == encs::<O>(g2[gs.len() as int]->values.skip(cur.len() as int))
            + groups_bytes::<O>(g2.skip(gs.len() as int + 1))
    }
}

proof fn lemma_group_bytes_split<'a, O: RleOps<'a>>(g: Group<O::V>)
    requires group_ok::<O>(g),
    ensures
        group_bytes::<O>(g) == O::enc_signed(header_of(g) as i64) + payload::<O>(g),
        i64::MIN <= header_of(g) <= i64::MAX,
{
}

/// Splitting a parse after its first `n + 1` groups.
proof fn lemma_parse_after<'a, O: RleOps<'a>>(g2: Seq<Group<O::V>>, n: int)
    requires 0 <= n < g2.len(),
    ensures
        groups_len(g2) >= groups_len(g2.take(n)) + group_len(g2[n]),
        g2.take(n + 1) == g2.take(n).push(g2[n]),
        groups_bytes::<O>(g2) == groups_bytes::<O>(g2.take(n)) + group_bytes::<O>(g2[n])
            + groups_bytes::<O>(g2.skip(n + 1)),
{
    assert(g2 =~= g2.take(n + 1) + g2.skip(n + 1));
    lemma_groups_len_concat(g2.take(n + 1), g2.skip(n + 1));
    assert(g2.take(n + 1) =~= g2.take(n).push(g2[n]));
    assert(g2.take(n + 1).drop_last() =~= g2.take(n));
    lemma_groups_bytes_at::<O>(g2, n);
}

/// At a group boundary with bytes left, every acceptable parse has a
/// next group there.
proof fn lemma_next_group<'a, O: RleOps<'a>>(data: Seq<u8>, gs: Seq<Group<O::V>>, pos: int)
    requires
        complete::<O>(data, gs, 0, Seq::empty()),
        0 <= pos < data.len(), data.take(pos) == groups_bytes::<O>(gs),
    ensures
        forall|g2: Seq<Group<O::V>>| #[trigger] parses::<O>(g2, data) ==> {
            &&& g2.len() > gs.len()
            &&& g2.take(gs.len() as int) == gs
            &&& data.skip(pos) == group_bytes::<O>(g2[gs.len() as int]) + groups_bytes::<O>(g2.skip(gs.len() as int + 1))
        },
{
    reveal(parses);
    reveal(complete);
    let n = gs.len() as int;
    assert forall|g2: Seq<Group<O::V>>| #[trigger] parses::<O>(g2, data) implies {
        &&& g2.len() > n
        &&& g2.take(n) == gs
        &&& data.skip(pos) == group_bytes::<O>(g2[n]) + groups_bytes::<O>(g2.skip(n + 1))
    } by {
        if g2.len() == n {
            assert(g2 =~= g2.take(n));
        }
        lemma_parse_after::<O>(g2, n);
        let tail = group_bytes::<O>(g2[n]) + groups_bytes::<O>(g2.skip(n + 1));
        assert(data =~= groups_bytes::<O>(gs) + tail);
        lemma_split(data, groups_bytes::<O>(gs), tail, pos);
    }
}

/// A header read off the next group's bytes.
proof fn lemma_header_ok<'a, O: RleOps<'a>>(data: Seq<u8>, gs: Seq<Group<O::V>>, pos: int, hn: int, h: i64)
    requires
        complete::<O>(data, gs, 0, Seq::empty()),
        0 <= pos < data.len(), data.take(pos) == groups_bytes::<O>(gs),
        0 <= hn <= data.len() - pos, data.skip(pos).take(hn) == O::enc_signed(h),
    ensures
        header_at::<O>(data, gs, h as int, pos + hn),
        data.take(pos + hn) == groups_bytes::<O>(gs) + O::enc_signed(h),
{
    reveal(parses);
    reveal(header_at);
    lemma_next_group::<O>(data, gs, pos);
    let n = gs.len() as int;
    assert forall|g2: Seq<Group<O::V>>| #[trigger] parses::<O>(g2, data) implies {
        &&& header_of(g2[n]) == h
        &&& data.skip(pos + hn) == payload::<O>(g2[n]) + groups_bytes::<O>(g2.skip(n + 1))
    } by {
        let g = g2[n];
        assert(group_ok::<O>(g));
        lemma_group_bytes_split::<O>(g);
        let q = payload::<O>(g) + groups_bytes::<O>(g2.skip(n + 1));
        assert(data.skip(pos) =~= O::enc_signed(header_of(g) as i64) + q);
        lemma_header::<O>(data.skip(pos), header_of(g) as i64, q, hn, h);
        assert(data.skip(pos).skip(hn) =~= data.skip(pos + hn));
    }
    assert(data.take(pos + hn) =~= data.take(pos) + data.skip(pos).take(hn));
}

/// No header can be read off the next group's bytes: no parse.
proof fn lemma_header_err<'a, O: RleOps<'a>>(data: Seq<u8>, gs: Seq<Group<O::V>>, pos: int)
    requires
        complete::<O>(data, gs, 0, Seq::empty()),
        0 <= pos < data.len(), data.take(pos) == groups_bytes::<O>(gs),
        forall|x: i64| !O::enc_signed(x).is_prefix_of(data.skip(pos)),
    ensures no_parse::<O>(data),
{
    reveal(parses);
    lemma_next_group::<O>(data, gs, pos);
    let n = gs.len() as int;
    assert forall|g2: Seq<Group<O::V>>| !parses::<O>(g2, data) by {
        if parses::<O>(g2, data) {
            let g = g2[n];
            assert(group_ok::<O>(g));
            lemma_group_bytes_split::<O>(g);
            let q = payload::<O>(g) + groups_bytes::<O>(g2.skip(n + 1));
            assert(data.skip(pos) =~= O::enc_signed(header_of(g) as i64) + q);
            lemma_starts(O::enc_signed(header_of(g) as i64), q);
        }
    }
}

/// Inside a literal, every acceptable parse has the remaining values next.
proof fn lemma_lit_at<'a, O: RleOps<'a>>(data: Seq<u8>, gs: Seq<Group<O::V>>, k: nat, cur: Seq<O::V>, pos: int)
    requires
        complete::<O>(data, gs, k, cur), k > 0, cur.len() <= k,
        0 <= pos <= data.len(), data.take(pos) == read_bytes::<O>(gs, k, cur),
    ensures lit_at::<O>(data, gs, k, cur, pos),
{
    reveal(complete);
    reveal(lit_at);
    let n = gs.len() as int;
    assert forall|g2: Seq<Group<O::V>>| #[trigger] parses::<O>(g2, data) implies
        data.skip(pos) == encs::<O>(g2[n]->values.skip(cur.len() as int)) + groups_bytes::<O>(g2.skip(n + 1)) by {
        let vs = g2[n]->values;
        let i = cur.len() as int;
        assert(g2.take(n) == gs && g2[n] is Lit && vs.len() == k && vs.take(i) == cur);
        assert(groups_bytes::<O>(g2) == data) by {
            reveal(parses);
        }
        lemma_parse_after::<O>(g2, n);
        assert(vs =~= vs.take(i) + vs.skip(i));
        lemma_encs_concat::<O>(cur, vs.skip(i));
        let tail = encs::<O>(vs.skip(i)) + groups_bytes::<O>(g2.skip(n + 1));
        assert(group_bytes::<O>(g2[n]) == lit_head::<O>(k) + encs::<O>(vs));
        assert(data == read_bytes::<O>(gs, k, cur) + tail) by {
            assert(data == groups_bytes::<O>(gs) + group_bytes::<O>(g2[n]) + groups_bytes::<O>(g2.skip(n + 1)));
            assert(group_bytes::<O>(g2[n]) == lit_head::<O>(k) + (encs::<O>(cur) + encs::<O>(vs.skip(i))));
            assert(data =~= read_bytes::<O>(gs, k, cur) + tail);
        }
        lemma_split(data, read_bytes::<O>(gs, k, cur), tail, pos);
    }
}

/// The parse's value at a literal position, split off.
proof fn lemma_lit_value<'a, O: RleOps<'a>>(data: Seq<u8>, gs: Seq<Group<O::V>>, k: nat, cur: Seq<O::V>, pos: int, g2: Seq<Group<O::V>>)
    requires lit_at::<O>(data, gs, k, cur, pos), parses::<O>(g2, data), cur.len() < k,
    ensures ({
        let n = gs.len() as int;
        let vs = g2[n]->values;
        let i = cur.len() as int;
        &&& g2.len() > n && g2.take(n) == gs && g2[n] is Lit && vs.len() == k && vs.take(i) == cur
        &&& !O::is_null(vs[i])
        &&& data.skip(pos) == O::enc(vs[i]) + (encs::<O>(vs.skip(i + 1)) + groups_bytes::<O>(g2.skip(n + 1)))
        &&& group_ok::<O>(g2[n])
        &&& group_bytes::<O>(g2[n]).len() <= u32::MAX
        &&& n > 0 ==> adjacent_ok(gs.last(), g2[n])
        &&& groups_len(g2) <= usize::MAX
    }),
{
    reveal(parses);
    reveal(lit_at);
    let n = gs.len() as int;
    let vs = g2[n]->values;
    let i = cur.len() as int;
    assert(group_ok::<O>(g2[n]));
    if n > 0 {
        lemma_parse_after::<O>(g2, n);
        assert(adjacent_ok(g2[n - 1], g2[n]));
        assert(g2.take(n)[n - 1] == g2[n - 1]);
    }
    assert(vs.skip(i) =~= seq![vs[i]] + vs.skip(i + 1));
    lemma_encs_concat::<O>(seq![vs[i]], vs.skip(i + 1));
    reveal_with_fuel(encs, 2);
    assert(encs::<O>(seq![vs[i]]) =~= O::enc(vs[i]));
}

/// A repeat run's value.
pub open spec fn run_value<V>(g: Group<V>) -> V {
    match g {
        Group::Run { value, .. } => value,
        _ => arbitrary(),
    }
}

/// A positive header: every acceptable parse has a repeat run of that
/// many next.
proof fn lemma_run_at<'a, O: RleOps<'a>>(data: Seq<u8>, gs: Seq<Group<O::V>>, h: int, p: int, g2: Seq<Group<O::V>>)
    requires header_at::<O>(data, gs, h, p), h > 0, parses::<O>(g2, data),
    ensures ({
        let n = gs.len() as int;
        &&& g2.len() > n && g2.take(n) == gs
        &&& g2[n] == Group::Run { count: h as nat, value: run_value(g2[n]) }
        &&& 2 <= h && !O::is_null(run_value(g2[n]))
        &&& data.skip(p) == O::enc(run_value(g2[n])) + groups_bytes::<O>(g2.skip(n + 1))
        &&& groups_len(g2) >= groups_len(gs) + h
        &&& group_bytes::<O>(g2[n]).len() <= u32::MAX
        &&& n > 0 ==> adjacent_ok(gs.last(), g2[n])
        &&& groups_len(g2) <= usize::MAX
    }),
{
    reveal(parses);
    reveal(header_at);
    let n = gs.len() as int;
    assert(group_ok::<O>(g2[n]));
    lemma_parse_after::<O>(g2, n);
    if n > 0 {
        assert(adjacent_ok(g2[n - 1], g2[n]));
        assert(g2.take(n)[n - 1] == g2[n - 1]);
    }
}

/// A zero header: every acceptable parse has a null run next.
proof fn lemma_null_at<'a, O: RleOps<'a>>(data: Seq<u8>, gs: Seq<Group<O::V>>, p: int, g2: Seq<Group<O::V>>)
    requires header_at::<O>(data, gs, 0, p), parses::<O>(g2, data),
    ensures ({
        let n = gs.len() as int;
        &&& g2.len() > n && g2.take(n) == gs
        &&& g2[n] is Null
        &&& 1 <= g2[n]->Null_count <= u64::MAX && O::nullable()
        &&& data.skip(p) == O::enc_unsigned(g2[n]->Null_count as u64) + groups_bytes::<O>(g2.skip(n + 1))
        &&& groups_len(g2) >= groups_len(gs) + g2[n]->Null_count
        &&& group_bytes::<O>(g2[n]).len() <= u32::MAX
        &&& n > 0 ==> adjacent_ok(gs.last(), g2[n])
        &&& groups_len(g2) <= usize::MAX
    }),
{
    reveal(parses);
    reveal(header_at);
    let n = gs.len() as int;
    assert(group_ok::<O>(g2[n]));
    lemma_parse_after::<O>(g2, n);
    if n > 0 {
        assert(adjacent_ok(g2[n - 1], g2[n]));
        assert(g2.take(n)[n - 1] == g2[n - 1]);
    }
}

/// A negative header: every acceptable parse has a literal of that many
/// values next.
proof fn lemma_lit_head_at<'a, O: RleOps<'a>>(data: Seq<u8>, gs: Seq<Group<O::V>>, h: int, p: int, g2: Seq<Group<O::V>>)
    requires header_at::<O>(data, gs, h, p), h < 0, parses::<O>(g2, data),
    ensures ({
        let n = gs.len() as int;
        &&& g2.len() > n && g2.take(n) == gs
        &&& g2[n] is Lit && g2[n]->values.len() == -h <= i64::MAX
        &&& data.skip(p) == encs::<O>(g2[n]->values) + groups_bytes::<O>(g2.skip(n + 1))
        &&& groups_len(g2) >= groups_len(gs) - h
        &&& n > 0 ==> !(gs.last() is Lit)
        &&& groups_len(g2) <= usize::MAX
    }),
{
    reveal(parses);
    reveal(header_at);
    let n = gs.len() as int;
    assert(group_ok::<O>(g2[n]));
    lemma_parse_after::<O>(g2, n);
    if n > 0 {
        assert(adjacent_ok(g2[n - 1], g2[n]));
        assert(g2.take(n)[n - 1] == g2[n - 1]);
    }
}

/// Adding a group to canonical groups it may follow.
proof fn lemma_canonical_push<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, g: Group<O::V>)
    requires
        canonical::<O>(gs), group_ok::<O>(g),
        gs.len() > 0 ==> adjacent_ok(gs.last(), g),
    ensures canonical::<O>(gs.push(g)),
{
    let gs2 = gs.push(g);
    assert forall|i: int| 0 <= i < gs2.len() implies group_ok::<O>(#[trigger] gs2[i]) by {
        if i < gs.len() {
            assert(gs2[i] == gs[i]);
        }
    }
    assert forall|i: int| 0 < i < gs2.len() implies adjacent_ok(gs2[i - 1], #[trigger] gs2[i]) by {
        assert(gs2[i - 1] == gs[i - 1]);
        if i < gs.len() {
            assert(gs2[i] == gs[i]);
        }
    }
}

/// The bytes, count and metadata bound of groups with one more.
proof fn lemma_groups_push<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, g: Group<O::V>)
    requires forall|i: int| 0 <= i < gs.len() ==> (#[trigger] group_bytes::<O>(gs[i])).len() <= u32::MAX,
        group_bytes::<O>(g).len() <= u32::MAX,
    ensures
        groups_bytes::<O>(gs.push(g)) == groups_bytes::<O>(gs) + group_bytes::<O>(g),
        groups_len(gs.push(g)) == groups_len(gs) + group_len(g),
        forall|i: int| 0 <= i < gs.push(g).len() ==> (#[trigger] group_bytes::<O>(gs.push(g)[i])).len() <= u32::MAX,
{
    assert(gs.push(g).drop_last() =~= gs);
    assert forall|i: int| 0 <= i < gs.push(g).len() implies (#[trigger] group_bytes::<O>(gs.push(g)[i])).len() <= u32::MAX by {
        if i < gs.len() {
            assert(gs.push(g)[i] == gs[i]);
        }
    }
}

/// Every acceptable parse continues with `g`: they begin with `gs + [g]`.
proof fn lemma_complete_push<'a, O: RleOps<'a>>(data: Seq<u8>, gs: Seq<Group<O::V>>, g: Group<O::V>)
    requires
        forall|g2: Seq<Group<O::V>>| #[trigger] parses::<O>(g2, data)
            ==> g2.len() > gs.len() && g2.take(gs.len() as int) == gs && g2[gs.len() as int] == g,
    ensures complete::<O>(data, gs.push(g), 0, Seq::empty()),
{
    reveal(complete);
    assert forall|g2: Seq<Group<O::V>>| #[trigger] parses::<O>(g2, data) implies
        g2.take(gs.len() as int + 1) == gs.push(g) by {
        assert(g2.take(gs.len() as int + 1) =~= g2.take(gs.len() as int).push(g2[gs.len() as int]));
    }
}

/// Every acceptable parse has `value` next in the literal.
pub open spec fn value_next<'a, O: RleOps<'a>>(data: Seq<u8>, gs: Seq<Group<O::V>>, cur: Seq<O::V>, value: O::V) -> bool {
    forall|g2: Seq<Group<O::V>>| #[trigger] parses::<O>(g2, data) ==> g2[gs.len() as int]->values[cur.len() as int] == value
}

/// A literal value read, with more to come.
proof fn lemma_lit_continue<'a, O: RleOps<'a>>(data: Seq<u8>, gs: Seq<Group<O::V>>, k: nat, cur: Seq<O::V>, p: int, value: O::V)
    requires lit_at::<O>(data, gs, k, cur, p), cur.len() < k, value_next::<O>(data, gs, cur, value),
    ensures complete::<O>(data, gs, k, cur.push(value)),
{
    reveal(complete);
    let n = gs.len() as int;
    let i = cur.len() as int;
    assert forall|g2: Seq<Group<O::V>>| #[trigger] parses::<O>(g2, data) implies {
        &&& g2.len() >= gs.len() && g2.take(n) == gs
        &&& g2.len() > n && g2[n] is Lit && g2[n]->values.len() == k
        &&& g2[n]->values.take(i + 1) == cur.push(value)
    } by {
        lemma_lit_value::<O>(data, gs, k, cur, p, g2);
        assert(g2[n]->values.take(i + 1) =~= g2[n]->values.take(i).push(g2[n]->values[i]));
    }
}

/// The last value of a literal read: every acceptable parse has the
/// literal next.
proof fn lemma_lit_close<'a, O: RleOps<'a>>(data: Seq<u8>, gs: Seq<Group<O::V>>, k: nat, cur: Seq<O::V>, p: int, value: O::V)
    requires lit_at::<O>(data, gs, k, cur, p), cur.len() + 1 == k, value_next::<O>(data, gs, cur, value),
    ensures
        forall|g2: Seq<Group<O::V>>| #[trigger] parses::<O>(g2, data) ==> {
            &&& g2.len() > gs.len() && g2.take(gs.len() as int) == gs
            &&& g2[gs.len() as int] == Group::Lit { values: cur.push(value) }
            &&& group_bytes::<O>(g2[gs.len() as int]).len() <= u32::MAX
            &&& gs.len() > 0 ==> adjacent_ok(gs.last(), g2[gs.len() as int])
        },
{
    let n = gs.len() as int;
    let i = cur.len() as int;
    assert forall|g2: Seq<Group<O::V>>| #[trigger] parses::<O>(g2, data) implies {
        &&& g2.len() > n && g2.take(n) == gs
        &&& g2[n] == Group::Lit { values: cur.push(value) }
        &&& group_bytes::<O>(g2[n]).len() <= u32::MAX
        &&& n > 0 ==> adjacent_ok(gs.last(), g2[n])
    } by {
        lemma_lit_value::<O>(data, gs, k, cur, p, g2);
        assert(g2[n]->values =~= g2[n]->values.take(i).push(g2[n]->values[i]));
    }
}

/// The next group, read in full after its header.
pub(crate) open spec fn read_one<'a, O: RleOps<'a>>(r0: Reader<'a, O>, r1: Reader<'a, O>, g: Group<O::V>, bytes: usize) -> bool {
    &&& !r0.in_lit() && !r1.in_lit()
    &&& r1.groups() == r0.groups().push(g)
    &&& bytes == group_bytes::<O>(g).len()
    &&& r1.spec_pos() == r0.spec_pos() + bytes
}

/// What [`Reader::next`] promises.
pub(crate) open spec fn next_post<'a, O: RleOps<'a>>(r0: Reader<'a, O>, r1: Reader<'a, O>, r: Result<Option<Seg<O::V>>, PackError>) -> bool {
    &&& r is Ok ==> r1.inv()
    &&& r1.data() == r0.data()
    &&& match r {
        Err(_) => no_parse::<O>(r0.data()),
        Ok(None) => !r0.in_lit() && parses::<O>(r0.groups(), r0.data())
            && r1.groups() == r0.groups() && !r1.in_lit() && r1.spec_pos() == r0.data().len(),
        Ok(Some(seg)) => r1.spec_pos() > r0.spec_pos() && match seg {
            Seg::Run { count, value, bytes } => read_one(r0, r1, Group::Run { count: count as nat, value }, bytes),
            Seg::Null { count, bytes } => read_one(r0, r1, Group::Null { count: count as nat }, bytes),
            Seg::LitHead { count, bytes } => {
                &&& !r0.in_lit() && r1.in_lit()
                &&& r1.groups() == r0.groups()
                &&& r1.lit_values() == Seq::<O::V>::empty()
                &&& count >= 1
                &&& bytes == lit_head::<O>(count as nat).len()
                &&& r1.spec_pos() == r0.spec_pos() + bytes
            },
            Seg::Lit { value, bytes } => {
                &&& r0.in_lit()
                &&& bytes == O::enc(value).len()
                &&& r1.spec_pos() == r0.spec_pos() + bytes
                &&& r1.in_lit() ==> r1.groups() == r0.groups() && r1.lit_values() == r0.lit_values().push(value)
                &&& !r1.in_lit() ==> r1.groups() == r0.groups().push(Group::Lit { values: r0.lit_values().push(value) })
            },
        },
    }
}

/// A walk over the wire segments of a slab's bytes, checking them.
pub(crate) struct Reader<'a, O: RleOps<'a>> {
    data: &'a [u8],
    pos: usize,
    /// values left in the current literal run
    lit_left: usize,
    /// where the current literal run's header starts
    lit_start: usize,
    prev: Prev<O::V>,
    /// values read
    len: usize,
    /// the complete groups read
    gs: Ghost<Seq<Group<O::V>>>,
    /// inside a literal run: its length, and the values read
    lit_k: Ghost<nat>,
    cur: Ghost<Seq<O::V>>,
}

impl<'a, O: RleOps<'a>> Reader<'a, O> {
    pub closed spec fn data(&self) -> Seq<u8> {
        self.data@
    }

    /// The complete groups read.
    pub closed spec fn groups(&self) -> Seq<Group<O::V>> {
        self.gs@
    }

    pub closed spec fn spec_pos(&self) -> int {
        self.pos as int
    }

    /// Inside a literal run.
    pub closed spec fn in_lit(&self) -> bool {
        self.lit_k@ > 0
    }

    /// Inside a literal run: the values read so far.
    pub closed spec fn lit_values(&self) -> Seq<O::V> {
        self.cur@
    }

    /// What `inv` says of the groups and position.
    pub proof fn lemma_inv(&self)
        requires self.inv(),
        ensures
            canonical::<O>(self.groups()),
            !self.in_lit() ==> self.spec_pos() == groups_bytes::<O>(self.groups()).len(),
            self.in_lit() ==> self.spec_pos() >= groups_bytes::<O>(self.groups()).len(),
            self.spec_pos() <= self.data().len(),
            groups_len(self.groups()) + self.lit_values().len() <= usize::MAX,
            !self.in_lit() ==> self.lit_values().len() == 0,
            forall|i: int| 0 <= i < self.groups().len() ==> (#[trigger] group_bytes::<O>(self.groups()[i])).len() <= u32::MAX,
    {
        assert(self.data@.take(self.pos as int).len() == self.pos);
    }

    pub closed spec fn inv(&self) -> bool {
        &&& self.pos <= self.data.len()
        &&& self.data@.take(self.pos as int) == read_bytes::<O>(self.gs@, self.lit_k@, self.cur@)
        &&& canonical::<O>(self.gs@)
        &&& forall|i: int| 0 <= i < self.gs@.len() ==> (#[trigger] group_bytes::<O>(self.gs@[i])).len() <= u32::MAX
        &&& self.len == groups_len(self.gs@) + self.cur@.len()
        &&& self.prev == prev_of(self.gs@, self.cur@)
        &&& if self.lit_k@ > 0 {
            &&& self.lit_left > 0
            &&& self.lit_left + self.cur@.len() == self.lit_k@ <= i64::MAX
            &&& self.len + self.lit_left <= usize::MAX
            &&& self.lit_start == groups_bytes::<O>(self.gs@).len()
            &&& lit_prefix_ok::<O>(self.gs@, self.cur@)
        } else {
            self.cur@.len() == 0 && self.lit_left == 0
        }
        &&& complete::<O>(self.data@, self.gs@, self.lit_k@, self.cur@)
    }

    pub(crate) fn new(data: &'a [u8]) -> (r: Self)
        ensures
            r.inv(), r.data() == data@, r.groups() == Seq::<Group<O::V>>::empty(),
            !r.in_lit(), r.spec_pos() == 0,
    {
        let r = Reader {
            data,
            pos: 0,
            lit_left: 0,
            lit_start: 0,
            prev: Prev::Start,
            len: 0,
            gs: Ghost(Seq::empty()),
            lit_k: Ghost(0),
            cur: Ghost(Seq::empty()),
        };
        proof {
            reveal(complete);
            assert(data@.take(0) =~= Seq::<u8>::empty());
            assert forall|g2: Seq<Group<O::V>>| #[trigger] parses::<O>(g2, data@) implies
                g2.take(0) == Seq::<Group<O::V>>::empty() by {
                assert(g2.take(0) =~= Seq::<Group<O::V>>::empty());
            }
        }
        r
    }

    /// The number of values read.
    pub(crate) fn values_read(&self) -> (r: usize)
        requires self.inv(),
        ensures r == groups_len(self.groups()) + self.lit_values().len(),
    {
        self.len
    }

    /// The byte position: where the next segment starts.
    pub(crate) fn pos(&self) -> (r: usize)
        ensures r == self.spec_pos(),
    {
        self.pos
    }

    /// The next segment, `None` at the end of the data, or an error if no
    /// acceptable parse of the data exists.
    pub(crate) fn next(&mut self) -> (r: Result<Option<Seg<O::V>>, PackError>)
        requires old(self).inv(),
        ensures next_post(*old(self), *final(self), r),
    {
        if self.lit_left > 0 {
            return self.next_lit_value();
        }
        let ghost data = self.data@;
        let ghost gs = self.gs@;
        proof {
            assert(self.cur@ =~= Seq::<O::V>::empty());
        }
        if self.pos == self.data.len() {
            proof {
                reveal(parses);
                assert(data.take(self.pos as int) =~= data);
            }
            return Ok(None);
        }
        let rest = &self.data[self.pos..];
        let (hn, h) = match O::read_signed(rest) {
            Ok(x) => x,
            Err(e) => {
                proof {
                    assert(rest@ =~= data.skip(self.pos as int));
                    lemma_header_err::<O>(data, gs, self.pos as int);
                }
                return Err(e);
            },
        };
        proof {
            assert(rest@ =~= data.skip(self.pos as int));
            lemma_header_ok::<O>(data, gs, self.pos as int, hn as int, h);
        }
        if h > 0 {
            self.next_run(hn, h)
        } else if h < 0 {
            self.next_lit_head(hn, h)
        } else {
            self.next_null(hn)
        }
    }

    fn next_run(&mut self, hn: usize, h: i64) -> (r: Result<Option<Seg<O::V>>, PackError>)
        requires
            old(self).inv(), !old(self).in_lit(), h > 0,
            header_at::<O>(old(self).data(), old(self).groups(), h as int, old(self).spec_pos() + hn),
            old(self).spec_pos() + hn <= old(self).data().len(),
            old(self).data().take(old(self).spec_pos() + hn) == groups_bytes::<O>(old(self).groups()) + O::enc_signed(h),
            hn == O::enc_signed(h).len(),
        ensures next_post(*old(self), *final(self), r),
    {
        let ghost data = self.data@;
        let ghost gs = self.gs@;
        let ghost n = gs.len() as int;
        let ghost p = self.pos + hn;
        proof {
            assert(self.cur@ =~= Seq::<O::V>::empty());
        }
        if h < 2 {
            proof {
                assert forall|g2: Seq<Group<O::V>>| !parses::<O>(g2, data) by {
                    if parses::<O>(g2, data) {
                        lemma_run_at::<O>(data, gs, h as int, p, g2);
                    }
                }
            }
            return Err(invalid("repeat run with count < 2"));
        }
        if h as u64 > (usize::MAX - self.len) as u64 {
            proof {
                assert forall|g2: Seq<Group<O::V>>| !parses::<O>(g2, data) by {
                    if parses::<O>(g2, data) {
                        lemma_run_at::<O>(data, gs, h as int, p, g2);
                    }
                }
            }
            return Err(invalid("more values than fit a usize"));
        }
        let count = h as usize;
        let rest = &self.data[self.pos + hn..];
        let (vn, value) = match O::try_unpack(rest) {
            Ok(x) => x,
            Err(e) => {
                proof {
                    assert forall|g2: Seq<Group<O::V>>| !parses::<O>(g2, data) by {
                        if parses::<O>(g2, data) {
                            lemma_run_at::<O>(data, gs, h as int, p, g2);
                            lemma_starts(O::enc(run_value(g2[n])), groups_bytes::<O>(g2.skip(n + 1)));
                        }
                    }
                }
                return Err(e);
            },
        };
        proof {
            assert forall|g2: Seq<Group<O::V>>| #[trigger] parses::<O>(g2, data) implies
                g2.len() > n && g2.take(n) == gs && g2[n] == (Group::Run { count: count as nat, value }) by {
                lemma_run_at::<O>(data, gs, h as int, p, g2);
                lemma_value::<O>(rest@, run_value(g2[n]), groups_bytes::<O>(g2.skip(n + 1)), vn as int, value);
            }
        }
        let ghost g = Group::Run { count: count as nat, value };
        match self.prev {
            Prev::Run(x) | Prev::Lit(x) => {
                if O::same(&x, &value) {
                    proof {
                        assert forall|g2: Seq<Group<O::V>>| !parses::<O>(g2, data) by {
                            if parses::<O>(g2, data) {
                                lemma_run_at::<O>(data, gs, h as int, p, g2);
                            }
                        }
                    }
                    return Err(invalid("equal values side by side (should be one repeat run)"));
                }
            },
            _ => {},
        }
        let bytes = hn + vn;
        if bytes > u32::MAX as usize {
            proof {
                assert forall|g2: Seq<Group<O::V>>| !parses::<O>(g2, data) by {
                    if parses::<O>(g2, data) {
                        lemma_run_at::<O>(data, gs, h as int, p, g2);
                    }
                }
            }
            return Err(invalid("run over 4 GiB"));
        }
        proof {
            let rb = groups_bytes::<O>(gs) + O::enc_signed(h);
            assert(data.take(p + vn) =~= data.take(p) + rest@.take(vn as int));
            assert(group_bytes::<O>(g) == O::enc_signed(h) + O::enc(value));
            assert(group_ok::<O>(g));
            if n > 0 {
                assert(adjacent_ok(gs.last(), g));
            }
            lemma_canonical_push::<O>(gs, g);
            lemma_groups_push::<O>(gs, g);
            lemma_complete_push::<O>(data, gs, g);
        }
        self.pos = self.pos + bytes;
        self.len = self.len + count;
        self.prev = Prev::Run(value);
        self.gs = Ghost(gs.push(g));
        proof {
            O::axiom_enc_signed(h, h, Seq::empty(), Seq::empty());
            assert(self.data@.take(self.pos as int) == read_bytes::<O>(self.gs@, self.lit_k@, self.cur@));
            assert(canonical::<O>(self.gs@));
            assert(self.len == groups_len(self.gs@) + self.cur@.len());
            assert(self.prev == prev_of(self.gs@, self.cur@));
            assert(complete::<O>(self.data@, self.gs@, self.lit_k@, self.cur@));
            assert(self.inv());
            assert(self.groups() == gs.push(g));
            assert(bytes == group_bytes::<O>(g).len());
            assert(self.spec_pos() == p + vn);
        }
        Ok(Some(Seg::Run { count, value, bytes }))
    }

    fn next_lit_head(&mut self, hn: usize, h: i64) -> (r: Result<Option<Seg<O::V>>, PackError>)
        requires
            old(self).inv(), !old(self).in_lit(), h < 0,
            header_at::<O>(old(self).data(), old(self).groups(), h as int, old(self).spec_pos() + hn),
            old(self).spec_pos() + hn <= old(self).data().len(),
            old(self).data().take(old(self).spec_pos() + hn) == groups_bytes::<O>(old(self).groups()) + O::enc_signed(h),
            hn == O::enc_signed(h).len(),
        ensures next_post(*old(self), *final(self), r),
    {
        let ghost data = self.data@;
        let ghost gs = self.gs@;
        let ghost n = gs.len() as int;
        let ghost p = self.pos + hn;
        proof {
            assert(self.cur@ =~= Seq::<O::V>::empty());
        }
        let bad = if h == i64::MIN {
            Some("literal run over i64::MAX values")
        } else if matches!(self.prev, Prev::Lit(_)) {
            Some("adjacent literal runs")
        } else if (-h) as u64 > (usize::MAX - self.len) as u64 {
            Some("more values than fit a usize")
        } else {
            None
        };
        if let Some(msg) = bad {
            proof {
                assert forall|g2: Seq<Group<O::V>>| !parses::<O>(g2, data) by {
                    if parses::<O>(g2, data) {
                        lemma_lit_head_at::<O>(data, gs, h as int, p, g2);
                    }
                }
            }
            return Err(invalid(msg));
        }
        let count = (-h) as usize;
        self.lit_left = count;
        self.lit_start = self.pos;
        self.pos = self.pos + hn;
        self.lit_k = Ghost(count as nat);
        proof {
            reveal(complete);
            assert(encs::<O>(Seq::<O::V>::empty()) =~= Seq::<u8>::empty());
            assert(lit_head::<O>(count as nat) == O::enc_signed(h));
            assert(self.data@.take(self.pos as int) =~= read_bytes::<O>(self.gs@, self.lit_k@, self.cur@));
            assert forall|g2: Seq<Group<O::V>>| #[trigger] parses::<O>(g2, data) implies {
                &&& g2.len() > n && g2.take(n) == gs && g2[n] is Lit
                &&& g2[n]->values.len() == count
                &&& g2[n]->values.take(0) == self.cur@
            } by {
                lemma_lit_head_at::<O>(data, gs, h as int, p, g2);
                assert(g2[n]->values.take(0) =~= Seq::<O::V>::empty());
            }
            assert(complete::<O>(self.data@, self.gs@, self.lit_k@, self.cur@));
            O::axiom_enc_signed(h, h, Seq::empty(), Seq::empty());
            assert(self.inv());
        }
        Ok(Some(Seg::LitHead { count, bytes: hn }))
    }

    fn next_null(&mut self, hn: usize) -> (r: Result<Option<Seg<O::V>>, PackError>)
        requires
            old(self).inv(), !old(self).in_lit(),
            header_at::<O>(old(self).data(), old(self).groups(), 0, old(self).spec_pos() + hn),
            old(self).spec_pos() + hn <= old(self).data().len(),
            old(self).data().take(old(self).spec_pos() + hn) == groups_bytes::<O>(old(self).groups()) + O::enc_signed(0),
            hn == O::enc_signed(0).len(),
        ensures next_post(*old(self), *final(self), r),
    {
        let ghost data = self.data@;
        let ghost gs = self.gs@;
        let ghost n = gs.len() as int;
        let ghost p = self.pos + hn;
        proof {
            assert(self.cur@ =~= Seq::<O::V>::empty());
        }
        let rest = &self.data[self.pos + hn..];
        let (cn, c) = match O::read_unsigned(rest) {
            Ok(x) => x,
            Err(e) => {
                proof {
                    assert forall|g2: Seq<Group<O::V>>| !parses::<O>(g2, data) by {
                        if parses::<O>(g2, data) {
                            lemma_null_at::<O>(data, gs, p, g2);
                            lemma_starts(O::enc_unsigned(g2[n]->Null_count as u64), groups_bytes::<O>(g2.skip(n + 1)));
                        }
                    }
                }
                return Err(e);
            },
        };
        proof {
            assert forall|g2: Seq<Group<O::V>>| #[trigger] parses::<O>(g2, data) implies
                g2.len() > n && g2.take(n) == gs && g2[n] == (Group::<O::V>::Null { count: c as nat }) by {
                lemma_null_at::<O>(data, gs, p, g2);
                lemma_count::<O>(rest@, g2[n]->Null_count as u64, groups_bytes::<O>(g2.skip(n + 1)), cn as int, c);
            }
        }
        let bad = if c == 0 {
            Some("null run with count 0")
        } else if !O::nullable_exec() {
            Some("null in non-nullable column")
        } else if matches!(self.prev, Prev::Null) {
            Some("adjacent null runs")
        } else if c > (usize::MAX - self.len) as u64 {
            Some("more values than fit a usize")
        } else if hn + cn > u32::MAX as usize {
            Some("run over 4 GiB")
        } else {
            None
        };
        if let Some(msg) = bad {
            proof {
                assert forall|g2: Seq<Group<O::V>>| !parses::<O>(g2, data) by {
                    if parses::<O>(g2, data) {
                        lemma_null_at::<O>(data, gs, p, g2);
                    }
                }
            }
            return Err(invalid(msg));
        }
        let count = c as usize;
        let bytes = hn + cn;
        let ghost g = Group::<O::V>::Null { count: count as nat };
        proof {
            assert(data.take(p + cn) =~= data.take(p) + rest@.take(cn as int));
            assert(group_bytes::<O>(g) == O::enc_signed(0) + O::enc_unsigned(c));
            assert(group_ok::<O>(g));
            if n > 0 {
                assert(adjacent_ok(gs.last(), g));
            }
            lemma_canonical_push::<O>(gs, g);
            lemma_groups_push::<O>(gs, g);
            lemma_complete_push::<O>(data, gs, g);
        }
        self.pos = self.pos + bytes;
        self.len = self.len + count;
        self.prev = Prev::Null;
        self.gs = Ghost(gs.push(g));
        proof {
            O::axiom_enc_signed(0, 0, Seq::empty(), Seq::empty());
            assert(self.data@.take(self.pos as int) == read_bytes::<O>(self.gs@, self.lit_k@, self.cur@));
            assert(canonical::<O>(self.gs@));
            assert(self.len == groups_len(self.gs@) + self.cur@.len());
            assert(self.prev == prev_of(self.gs@, self.cur@));
            assert(complete::<O>(self.data@, self.gs@, self.lit_k@, self.cur@));
            assert(self.inv());
            assert(self.groups() == gs.push(g));
            assert(bytes == group_bytes::<O>(g).len());
        }
        Ok(Some(Seg::Null { count, bytes }))
    }

    fn next_lit_value(&mut self) -> (r: Result<Option<Seg<O::V>>, PackError>)
        requires old(self).inv(), old(self).in_lit(),
        ensures next_post(*old(self), *final(self), r),
    {
        let ghost data = self.data@;
        let ghost gs = self.gs@;
        let ghost n = gs.len() as int;
        let ghost k = self.lit_k@;
        let ghost cur = self.cur@;
        let ghost i = cur.len() as int;
        let ghost p = self.pos as int;
        proof {
            lemma_lit_at::<O>(data, gs, k, cur, p);
        }
        let rest = &self.data[self.pos..];
        let (bytes, value) = match O::try_unpack(rest) {
            Ok(x) => x,
            Err(e) => {
                proof {
                    assert forall|g2: Seq<Group<O::V>>| !parses::<O>(g2, data) by {
                        if parses::<O>(g2, data) {
                            lemma_lit_value::<O>(data, gs, k, cur, p, g2);
                            let vs = g2[n]->values;
                            lemma_starts(O::enc(vs[i]), encs::<O>(vs.skip(i + 1)) + groups_bytes::<O>(g2.skip(n + 1)));
                        }
                    }
                }
                return Err(e);
            },
        };
        proof {
            assert forall|g2: Seq<Group<O::V>>| #[trigger] parses::<O>(g2, data) implies
                g2[n]->values[i] == value by {
                lemma_lit_value::<O>(data, gs, k, cur, p, g2);
                let vs = g2[n]->values;
                lemma_value::<O>(rest@, vs[i], encs::<O>(vs.skip(i + 1)) + groups_bytes::<O>(g2.skip(n + 1)), bytes as int, value);
            }
            assert(value_next::<O>(data, gs, cur, value));
        }
        match self.prev {
            Prev::Run(x) | Prev::Lit(x) => {
                if O::same(&x, &value) {
                    proof {
                        assert forall|g2: Seq<Group<O::V>>| !parses::<O>(g2, data) by {
                            if parses::<O>(g2, data) {
                                lemma_lit_value::<O>(data, gs, k, cur, p, g2);
                                let vs = g2[n]->values;
                                if i > 0 {
                                    assert(vs[i - 1] == vs.take(i)[i - 1]);
                                }
                            }
                        }
                    }
                    return Err(invalid("equal values side by side (should be one repeat run)"));
                }
            },
            _ => {},
        }
        let ghost cur2 = cur.push(value);
        proof {
            assert(data.take(p + bytes) =~= data.take(p) + rest@.take(bytes as int));
            lemma_encs_concat::<O>(cur, seq![value]);
            reveal_with_fuel(encs, 2);
            assert(encs::<O>(seq![value]) =~= O::enc(value));
            assert(cur + seq![value] =~= cur2);
            assert(lit_prefix_ok::<O>(gs, cur2)) by {
                assert forall|j: int| 0 < j < cur2.len() implies cur2[j - 1] != #[trigger] cur2[j] by {
                    if j < i {
                        assert(cur2[j - 1] == cur[j - 1] && cur2[j] == cur[j]);
                    }
                }
                assert forall|j: int| 0 <= j < cur2.len() implies !O::is_null(#[trigger] cur2[j]) by {
                    if j < i {
                        assert(cur2[j] == cur[j]);
                    }
                }
            }
            O::axiom_enc(value, value, Seq::empty(), Seq::empty());
        }
        self.pos = self.pos + bytes;
        self.lit_left = self.lit_left - 1;
        self.len = self.len + 1;
        self.prev = Prev::Lit(value);
        self.cur = Ghost(cur2);
        if self.lit_left > 0 {
            proof {
                lemma_lit_continue::<O>(data, gs, k, cur, p, value);
                assert(self.data@.take(self.pos as int) == read_bytes::<O>(self.gs@, self.lit_k@, self.cur@));
                assert(self.prev == prev_of(self.gs@, self.cur@));
                assert(self.inv());
            }
            return Ok(Some(Seg::Lit { value, bytes }));
        }
        proof {
            lemma_lit_close::<O>(data, gs, k, cur, p, value);
            assert(self.data@.take(self.pos as int) == read_bytes::<O>(gs, k, self.cur@));
            assert(self.cur@.len() == k);
            assert(bytes > 0);
        }
        self.close_lit(Ghost(*old(self)), value, bytes)
    }

    /// Ending a literal run after its last value was read.
    fn close_lit(&mut self, Ghost(r0): Ghost<Self>, value: O::V, bytes: usize) -> (r: Result<Option<Seg<O::V>>, PackError>)
        requires ({
            let s = old(self);
            let gs = r0.gs@;
            let n = gs.len() as int;
            let g = Group::Lit { values: s.cur@ };
            &&& r0.inv() && r0.in_lit()
            &&& s.data == r0.data && s.gs@ == gs && s.lit_k@ == r0.lit_k@ && s.lit_start == r0.lit_start
            &&& s.cur@ == r0.cur@.push(value) && s.cur@.len() == s.lit_k@
            &&& s.lit_left == 0 && s.len == r0.len + 1 && s.prev == Prev::Lit(value)
            &&& s.pos == r0.pos + bytes && bytes == O::enc(value).len() && bytes > 0
            &&& s.pos <= s.data.len()
            &&& s.lit_start == groups_bytes::<O>(gs).len()
            &&& s.data@.take(s.pos as int) == read_bytes::<O>(gs, s.lit_k@, s.cur@)
            &&& lit_prefix_ok::<O>(gs, s.cur@)
            &&& forall|g2: Seq<Group<O::V>>| #[trigger] parses::<O>(g2, s.data@) ==> {
                &&& g2.len() > n && g2.take(n) == gs
                &&& g2[n] == g
                &&& group_bytes::<O>(g2[n]).len() <= u32::MAX
                &&& n > 0 ==> adjacent_ok(gs.last(), g2[n])
            }
        }),
        ensures next_post(r0, *final(self), r),
    {
        let ghost data = self.data@;
        let ghost gs = self.gs@;
        let ghost n = gs.len() as int;
        let ghost k = self.lit_k@;
        let ghost g = Group::Lit { values: self.cur@ };
        proof {
            assert(group_bytes::<O>(g) == lit_head::<O>(k) + encs::<O>(self.cur@));
            assert(self.data@.take(self.pos as int).len() == self.pos);
        }
        if self.pos - self.lit_start > u32::MAX as usize {
            proof {
                assert forall|g2: Seq<Group<O::V>>| !parses::<O>(g2, data) by {
                    if parses::<O>(g2, data) {
                        assert(g2[n] == g);
                    }
                }
            }
            return Err(invalid("literal run over 4 GiB"));
        }
        proof {
            assert(group_ok::<O>(g));
            lemma_canonical_push::<O>(gs, g);
            lemma_groups_push::<O>(gs, g);
            lemma_complete_push::<O>(data, gs, g);
        }
        self.gs = Ghost(gs.push(g));
        self.cur = Ghost(Seq::empty());
        self.lit_k = Ghost(0);
        proof {
            assert(self.data@.take(self.pos as int) == read_bytes::<O>(self.gs@, self.lit_k@, self.cur@));
            assert(canonical::<O>(self.gs@));
            assert(self.len == groups_len(self.gs@) + self.cur@.len());
            assert(self.prev == prev_of(self.gs@, self.cur@));
            assert(complete::<O>(self.data@, self.gs@, self.lit_k@, self.cur@));
            assert(self.inv());
        }
        Ok(Some(Seg::Lit { value, bytes }))
    }
}
// ── Validating a whole slab ─────────────────────────────────────────────────

/// The byte length of the last group: the slab's tail.
pub open spec fn tail_bytes_of<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>) -> nat {
    if gs.len() == 0 { 0 } else { group_bytes::<O>(gs.last()).len() }
}

/// If the last group is a literal, the byte length of its last value.
pub open spec fn lit_tail_of<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>) -> nat {
    if gs.len() > 0 && gs.last() is Lit { O::enc(gs.last()->values.last()).len() } else { 0 }
}

/// Canonical groups have no more segments than values.
proof fn lemma_segments_le<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>)
    requires canonical::<O>(gs),
    ensures groups_segments(gs) <= groups_len(gs),
    decreases gs.len(),
{
    if gs.len() > 0 {
        let d = gs.drop_last();
        assert(group_ok::<O>(gs[gs.len() - 1]));
        assert forall|i: int| 0 <= i < d.len() implies group_ok::<O>(#[trigger] d[i]) by {
            assert(d[i] == gs[i]);
        }
        assert forall|i: int| 0 < i < d.len() implies adjacent_ok(d[i - 1], #[trigger] d[i]) by {
            assert(d[i] == gs[i] && d[i - 1] == gs[i - 1]);
        }
        lemma_segments_le::<O>(d);
    }
}

/// What load needs of a slab: its value count, its segment count, and
/// its tail.
pub(crate) struct Summary {
    pub(crate) len: usize,
    pub(crate) segments: usize,
    /// byte length of the last group
    pub(crate) tail_bytes: u32,
    /// byte length of the last value, if the last group is a literal;
    /// else 0
    pub(crate) lit_tail: u32,
}

/// Check that `data` is a canonical encoding whose metadata fits, and
/// summarize it.  An error means it is not.
pub(crate) fn validate<'a, O: RleOps<'a>>(data: &'a [u8]) -> (r: Result<Summary, PackError>)
    ensures
        match r {
            Ok(s) => exists|gs: Seq<Group<O::V>>| {
                &&& parses::<O>(gs, data@)
                &&& s.len == groups_len(gs)
                &&& s.segments == groups_segments(gs)
                &&& s.tail_bytes == tail_bytes_of::<O>(gs)
                &&& s.lit_tail == lit_tail_of::<O>(gs)
            },
            Err(_) => no_parse::<O>(data@),
        },
{
    let mut rd = Reader::<O>::new(data);
    let mut segments: usize = 0;
    // where the last group, or the literal being read, starts
    let mut start: usize = 0;
    let mut lit_tail: usize = 0;
    loop
        invariant
            rd.inv(), rd.data() == data@,
            segments == groups_segments(rd.groups()) + rd.lit_values().len(),
            start <= rd.spec_pos(),
            rd.in_lit() ==> start == groups_bytes::<O>(rd.groups()).len(),
            !rd.in_lit() && rd.groups().len() > 0 ==> start == groups_bytes::<O>(rd.groups().drop_last()).len(),
            !rd.in_lit() && rd.groups().len() == 0 ==> start == 0,
            !rd.in_lit() ==> lit_tail == lit_tail_of::<O>(rd.groups()),
        decreases data.len() - rd.spec_pos(),
    {
        let ghost r0 = rd;
        proof {
            rd.lemma_inv();
        }
        let at = rd.pos();
        match rd.next() {
            Err(e) => return Err(e),
            Ok(None) => {
                let len = rd.values_read();
                let ghost gs = rd.groups();
                proof {
                    rd.lemma_inv();
                    if gs.len() > 0 {
                        let last = gs.last();
                        assert(group_bytes::<O>(gs[gs.len() - 1]).len() <= u32::MAX);
                        if last is Lit {
                            let vs = last->values;
                            assert(vs.len() > 0) by {
                                assert(group_ok::<O>(gs[gs.len() - 1]));
                            }
                            assert(encs::<O>(vs) == encs::<O>(vs.drop_last()) + O::enc(vs.last()));
                        }
                    }
                }
                return Ok(Summary {
                    len,
                    segments,
                    tail_bytes: (data.len() - start) as u32,
                    lit_tail: lit_tail as u32,
                });
            },
            Ok(Some(seg)) => {
                proof {
                    rd.lemma_inv();
                    lemma_segments_le::<O>(rd.groups());
                    let gs0 = r0.groups();
                    if !rd.in_lit() && rd.groups().len() > 0 {
                        assert(rd.groups().drop_last() =~= gs0 || rd.groups() == gs0);
                    }
                }
                match seg {
                    Seg::Run { .. } | Seg::Null { .. } => {
                        proof {
                            assert(rd.groups().drop_last() =~= r0.groups());
                        }
                        segments += 1;
                        start = at;
                        lit_tail = 0;
                    },
                    Seg::LitHead { .. } => {
                        start = at;
                    },
                    Seg::Lit { bytes, .. } => {
                        proof {
                            if !rd.in_lit() {
                                assert(rd.groups().drop_last() =~= r0.groups());
                            }
                        }
                        segments += 1;
                        lit_tail = bytes;
                    },
                }
            },
        }
    }
}

} // verus!
