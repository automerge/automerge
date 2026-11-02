//! What follows an edit point in a slab, and joining it back on to what
//! the writer wrote — verified with Verus.
//!
//! An edit rewrites a slab from its start (or wherever it opened) up to
//! some point, and keeps the slab's bytes after it. The first run after
//! the point may have to merge with what was just written, so it is
//! handed to the writer as values — the [`Postfix`] — and only the bytes
//! after it are kept as they are. [`State::flush_postfix`] feeds the
//! postfix in and flushes; the bytes it leaves, followed by the slab's
//! bytes from the postfix's end, are the canonical encoding of the
//! values written then the slab's values from the edit point on
//! ([`joined`]).

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

// spec functions exist only when Verus compiles the crate
#[cfg(verus_keep_ghost)]
use super::dec::*;
use super::model::*;
use super::state::*;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// The first run after an edit point, as values: what the writer has to
/// take in, because it may merge with what was written before it.
#[derive(Debug)]
pub(crate) enum Postfix<V> {
    /// `count` copies of `value` (a repeat run's last values, or nulls),
    /// up to the end of their run.
    Run { count: usize, value: V },
    /// A literal value, with `lit` more of its literal after it — which
    /// a flush must count in the header it writes.
    Lit { value: V, lit: usize },
    /// A repeat run's last value, then the first value of the literal
    /// after it, with `lit` more. One value cannot be a repeat run, and a
    /// literal next to it would have to absorb it, so the two go in
    /// together.
    LonePlusLit { lone: V, value: V, lit: usize },
}

impl<V> Postfix<V> {
    /// The slab segments the postfix's values stand for.
    pub(crate) fn pending(&self) -> (r: usize)
        ensures 1 <= r <= 2,
    {
        match self {
            Postfix::Run { .. } | Postfix::Lit { .. } => 1,
            Postfix::LonePlusLit { .. } => 2,
        }
    }
}

// ── What a postfix stands for ───────────────────────────────────────────────

/// `count` copies of `value` from `at` close group `i` of `data`, whose
/// following groups start at `end`. A single value left of a repeat run
/// is not followed by a literal (that is [`Postfix::LonePlusLit`]).
pub(crate) open spec fn tail_run<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, count: nat, value: O::V, end: int, i: int) -> bool {
    let gs = slab_groups::<O>(data);
    &&& 0 <= i < gs.len()
    &&& match gs[i] {
        Group::Run { value: v, .. } => v == value,
        Group::Null { .. } => O::is_null(value),
        Group::Lit { .. } => false,
    }
    &&& 1 <= count <= group_len(gs[i])
    &&& at + count == gl(gs, i + 1)
    &&& end == gb::<O>(gs, i + 1)
    &&& count == 1 && gs[i] is Run && i + 1 < gs.len() ==> !(gs[i + 1] is Lit)
}

/// The value at `at` is value `j` of literal group `i` of `data`, with
/// `lit` more after it, which start at `end`.
pub(crate) open spec fn tail_lit<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, value: O::V, lit: nat, end: int, i: int, j: int) -> bool {
    let gs = slab_groups::<O>(data);
    &&& 0 <= i < gs.len() && gs[i] is Lit
    &&& 0 <= j < gs[i]->values.len()
    &&& value == gs[i]->values[j] && lit + j + 1 == gs[i]->values.len()
    &&& at == gl(gs, i) + j
    &&& end == gb::<O>(gs, i) + lit_head::<O>(gs[i]->values.len()).len() + encs::<O>(gs[i]->values.take(j + 1)).len()
}

/// The value at `at` is the last of repeat run `i`, the literal after
/// it starts with `value`, and `lit` more of its values start at `end`.
pub(crate) open spec fn tail_lone<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, lone: O::V, value: O::V, lit: nat, end: int, i: int) -> bool {
    let gs = slab_groups::<O>(data);
    &&& 0 <= i && i + 1 < gs.len()
    &&& gs[i] is Run && run_value(gs[i]) == lone
    &&& at + 1 == gl(gs, i + 1)
    &&& tail_lit::<O>(data, at + 1, value, lit, end, i + 1, 0)
}

/// `p` is the first run of the valid slab `data`'s values from `at`, and
/// the slab's bytes from `end` encode the rest — or there is nothing
/// from `at`, and no bytes from `end`.
pub(crate) open spec fn postfix_ok<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, p: Option<Postfix<O::V>>, end: int) -> bool {
    &&& valid_slab::<O>(data)
    &&& match p {
        None => at == slab_values::<O>(data).len() && end == data.len(),
        Some(Postfix::Run { count, value }) => exists|i: int| #[trigger] tail_run::<O>(data, at, count as nat, value, end, i),
        Some(Postfix::Lit { value, lit }) => exists|i: int, j: int| #[trigger] tail_lit::<O>(data, at, value, lit as nat, end, i, j),
        Some(Postfix::LonePlusLit { lone, value, lit }) => exists|i: int| #[trigger] tail_lone::<O>(data, at, lone, value, lit as nat, end, i),
    }
}

/// The writer's view of a postfix.
pub(crate) open spec fn pmap<'a, W: WriteOps<'a>>(p: Option<Postfix<W::Cow>>) -> Option<Postfix<VOf<'a, W>>> {
    match p {
        None => None,
        Some(Postfix::Run { count, value }) => Some(Postfix::Run { count, value: W::val(value) }),
        Some(Postfix::Lit { value, lit }) => Some(Postfix::Lit { value: W::val(value), lit }),
        Some(Postfix::LonePlusLit { lone, value, lit }) => Some(Postfix::LonePlusLit { lone: W::val(lone), value: W::val(value), lit }),
    }
}

/// The values a postfix adds to the pending group, at most.
pub(crate) open spec fn plen_of<V>(p: Option<Postfix<V>>) -> nat {
    match p {
        None => 0,
        Some(Postfix::Run { count, .. }) => count as nat,
        Some(Postfix::Lit { lit, .. }) => (1 + lit) as nat,
        Some(Postfix::LonePlusLit { lit, .. }) => (2 + lit) as nat,
    }
}

/// The postfix holds a null only in a nullable column.
pub(crate) open spec fn nulls_ok<'a, W: WriteOps<'a>>(p: Option<Postfix<W::Cow>>) -> bool {
    match p {
        None => true,
        Some(Postfix::Run { value, .. }) | Some(Postfix::Lit { value, .. }) =>
            <W::R as RleOps<'a>>::is_null(W::val(value)) ==> <W::R as RleOps<'a>>::nullable(),
        Some(Postfix::LonePlusLit { lone, value, .. }) =>
            (<W::R as RleOps<'a>>::is_null(W::val(lone)) || <W::R as RleOps<'a>>::is_null(W::val(value)))
                ==> <W::R as RleOps<'a>>::nullable(),
    }
}

/// `bytes` are the canonical encoding of `vals`.
pub open spec fn encodes<'a, O: RleOps<'a>>(bytes: Seq<u8>, vals: Seq<O::V>) -> bool {
    exists|gs: Seq<Group<O::V>>| #[trigger] canonical::<O>(gs) && groups_bytes::<O>(gs) == bytes
        && groups_values(gs, O::null()) == vals
}

/// Flushing `(s0, b0)` with the postfix `p` wrote `b1` and returned the
/// rewrite `rw`: wherever `p` came from, those bytes then the slab's
/// bytes after the postfix are the canonical encoding of the values
/// written, then the slab's values from the edit point.
pub(crate) open spec fn joined<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, b1: Seq<u8>, rw: Option<RewriteHeader>, p: Option<Postfix<VOf<'a, W>>>) -> bool {
    forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64, data: Seq<u8>, at: int, end: int|
        #[trigger] rel(s0, pre, b0, base, gs, ws, h0) && #[trigger] postfix_ok::<W::R>(data, at, p, end)
        ==> 0 <= base <= b1.len()
            && encodes::<W::R>(apply::<W>(rw, pre, h0) + b1.skip(base) + data.skip(end),
                values_of(s0, gs, ws) + slab_values::<W::R>(data).skip(at))
}

/// Building a [`postfix_ok`] from its witnesses.
pub(crate) proof fn lemma_postfix_run<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, count: usize, value: O::V, end: int, i: int)
    requires valid_slab::<O>(data), tail_run::<O>(data, at, count as nat, value, end, i),
    ensures postfix_ok::<O>(data, at, Some(Postfix::Run { count, value }), end),
{
    let p = Some(Postfix::Run { count, value });
    assert(p->Some_0->Run_count == count && p->Some_0->Run_value == value);
    assert(exists|k: int| #[trigger] tail_run::<O>(data, at, count as nat, value, end, k));
}

pub(crate) proof fn lemma_postfix_lit<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, value: O::V, lit: usize, end: int, i: int, j: int)
    requires valid_slab::<O>(data), tail_lit::<O>(data, at, value, lit as nat, end, i, j),
    ensures postfix_ok::<O>(data, at, Some(Postfix::Lit { value, lit }), end),
{
    let p = Some(Postfix::Lit { value, lit });
    assert(p->Some_0->Lit_lit == lit && p->Some_0->Lit_value == value);
    assert(exists|k: int, l: int| #[trigger] tail_lit::<O>(data, at, value, lit as nat, end, k, l));
}

pub(crate) proof fn lemma_postfix_lone<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, lone: O::V, value: O::V, lit: usize, end: int, i: int)
    requires valid_slab::<O>(data), tail_lone::<O>(data, at, lone, value, lit as nat, end, i),
    ensures postfix_ok::<O>(data, at, Some(Postfix::LonePlusLit { lone, value, lit }), end),
{
    let p = Some(Postfix::LonePlusLit { lone, value, lit });
    assert(p->Some_0->LonePlusLit_lit == lit && p->Some_0->LonePlusLit_value == value && p->Some_0->LonePlusLit_lone == lone);
    assert(exists|k: int| #[trigger] tail_lone::<O>(data, at, lone, value, lit as nat, end, k));
}

// ── Lemmas ──────────────────────────────────────────────────────────────────

/// Canonical groups joined where they may meet stay canonical.
pub proof fn lemma_canonical_concat<'a, O: RleOps<'a>>(a: Seq<Group<O::V>>, b: Seq<Group<O::V>>)
    requires
        canonical::<O>(a), canonical::<O>(b),
        a.len() > 0 && b.len() > 0 ==> adjacent_ok(a.last(), b[0]),
    ensures canonical::<O>(a + b),
{
    let c = a + b;
    assert forall|i: int| 0 <= i < c.len() implies group_ok::<O>(#[trigger] c[i]) by {
        if i < a.len() {
            assert(c[i] == a[i]);
        } else {
            assert(c[i] == b[i - a.len()]);
        }
    }
    assert forall|i: int| 0 < i < c.len() implies adjacent_ok(c[i - 1], #[trigger] c[i]) by {
        if i < a.len() {
            assert(c[i] == a[i] && c[i - 1] == a[i - 1]);
        } else if i == a.len() {
            assert(c[i] == b[0] && c[i - 1] == a.last());
        } else {
            assert(c[i] == b[i - a.len()] && c[i - 1] == b[i - 1 - a.len()]);
        }
    }
}

/// The groups after group `i` of a valid slab: their bytes start at
/// `gb(i + 1)`, and their values follow the first `gl(i + 1)`.
pub proof fn lemma_slab_after<'a, O: RleOps<'a>>(data: Seq<u8>, i: int)
    requires valid_slab::<O>(data), 0 <= i < slab_groups::<O>(data).len(),
    ensures ({
        let gs = slab_groups::<O>(data);
        let rest = gs.skip(i + 1);
        &&& canonical::<O>(rest)
        &&& data.skip(gb::<O>(gs, i + 1)) == groups_bytes::<O>(rest)
        &&& slab_values::<O>(data) == groups_values(gs.take(i), O::null()) + group_values(gs[i], O::null())
            + groups_values(rest, O::null())
        &&& groups_values(gs.take(i), O::null()).len() == gl(gs, i)
        &&& gl(gs, i + 1) == gl(gs, i) + group_len(gs[i])
        &&& group_values(gs[i], O::null()).len() == group_len(gs[i])
        &&& i + 1 < gs.len() ==> rest[0] == gs[i + 1]
    }),
{
    let gs = slab_groups::<O>(data);
    let rest = gs.skip(i + 1);
    lemma_slab::<O>(data);
    lemma_group_at::<O>(gs, i);
    lemma_group_values_len(gs[i], O::null());
    assert forall|k: int| 0 <= k < rest.len() implies group_ok::<O>(#[trigger] rest[k]) by {
        assert(rest[k] == gs[k + i + 1]);
    }
    assert forall|k: int| 0 < k < rest.len() implies adjacent_ok(rest[k - 1], #[trigger] rest[k]) by {
        assert(rest[k] == gs[k + i + 1] && rest[k - 1] == gs[k + i]);
    }
    assert(gs.take(i + 1) =~= gs.take(i).push(gs[i]));
    lemma_push::<O>(gs.take(i), gs[i]);
    assert(data =~= groups_bytes::<O>(gs.take(i + 1)) + groups_bytes::<O>(rest));
    assert(data.skip(gb::<O>(gs, i + 1)) =~= groups_bytes::<O>(rest));
}

/// The last values of a repeat or null run: `count` copies of its value.
proof fn lemma_run_tail_values<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, count: nat, value: O::V, end: int, i: int)
    requires valid_slab::<O>(data), tail_run::<O>(data, at, count, value, end, i),
    ensures ({
        let gs = slab_groups::<O>(data);
        slab_values::<O>(data).skip(at) == copies(count, value) + groups_values(gs.skip(i + 1), O::null())
    }),
{
    let gs = slab_groups::<O>(data);
    let g = gs[i];
    lemma_slab_after::<O>(data, i);
    lemma_slab_group::<O>(data, i);
    let a = groups_values(gs.take(i), O::null());
    let gv = group_values(g, O::null());
    let k = group_len(g) - count;
    if g is Null {
        O::axiom_null(value);
        assert(value == O::null());
    }
    assert(gv.skip(k) =~= copies(count, value));
    assert((a + gv + groups_values(gs.skip(i + 1), O::null())).skip(at)
        =~= gv.skip(k) + groups_values(gs.skip(i + 1), O::null()));
}

/// The pending group a state ending in `v` stands for, as the last of
/// all its groups.
proof fn lemma_last_group<'a, W: WriteOps<'a>>(s: State<'a, W>, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, v: VOf<'a, W>)
    requires last_is(s, v),
    ensures ({
        let a = all_groups(s, gs, ws);
        &&& a.len() > 0 && a == gs.push(pend_group(s, ws)->Some_0)
        &&& match a.last() {
            Group::Run { value, .. } => value == v && !<W::R as RleOps<'a>>::is_null(v),
            Group::Null { .. } => <W::R as RleOps<'a>>::is_null(v),
            Group::Lit { values } => values.len() > 0 && values.last() == v && !<W::R as RleOps<'a>>::is_null(v),
        }
        &&& s is Run ==> a.last() is Run
    }),
{
}

/// Writer groups `a` ending in `v`, then the groups after a run tail:
/// canonical, and their bytes and values join.
proof fn lemma_join_run<'a, O: RleOps<'a>>(a: Seq<Group<O::V>>, data: Seq<u8>, at: int, count: nat, v: O::V, end: int, i: int)
    requires
        valid_slab::<O>(data), tail_run::<O>(data, at, count, v, end, i),
        canonical::<O>(a), a.len() > 0,
        match a.last() {
            Group::Run { value, .. } => value == v && !O::is_null(v),
            Group::Null { .. } => O::is_null(v),
            Group::Lit { values } => values.len() > 0 && values.last() == v && count == 1 && !O::is_null(v),
        },
    ensures
        encodes::<O>(groups_bytes::<O>(a) + data.skip(end),
            groups_values(a, O::null()) + groups_values(slab_groups::<O>(data).skip(i + 1), O::null())),
{
    let gs = slab_groups::<O>(data);
    let rest = gs.skip(i + 1);
    lemma_slab_after::<O>(data, i);
    lemma_slab_group::<O>(data, i);
    if rest.len() > 0 {
        let n = gs[i + 1];
        lemma_slab_group::<O>(data, i + 1);
        assert(adjacent_ok(gs[i], n));
        if O::is_null(v) {
            // a null tail closes a null run
            assert(gs[i] is Null);
        } else {
            assert(gs[i] is Run);
        }
        assert(adjacent_ok(a.last(), rest[0]));
    }
    lemma_canonical_concat::<O>(a, rest);
    lemma_groups_bytes_concat::<O>(a, rest);
    lemma_groups_values_concat(a, rest, O::null());
}

/// Groups `rest` may follow a literal or repeat run ending in `last`.
pub(crate) open spec fn may_follow<V>(last: V, rest: Seq<Group<V>>) -> bool {
    rest.len() > 0 ==> !(rest[0] is Lit) && (rest[0] is Run ==> run_value(rest[0]) != last)
}

/// The literal values after value `j` of a literal group: a literal of
/// their own, unlike the value before them.
proof fn lemma_lit_rest<'a, O: RleOps<'a>>(vs: Seq<O::V>, j: int)
    requires group_ok::<O>(Group::Lit { values: vs }), 0 <= j, j + 1 < vs.len(),
    ensures ({
        let l = vs.skip(j + 1);
        group_ok::<O>(Group::Lit { values: l }) && vs[j] != l[0] && l.last() == vs.last()
    }),
{
    let l = vs.skip(j + 1);
    assert(l[0] == vs[j + 1]);
    assert forall|k: int| 0 <= k < l.len() implies !O::is_null(#[trigger] l[k]) by {
        assert(l[k] == vs[k + j + 1]);
    }
    assert forall|k: int| 0 < k < l.len() implies l[k - 1] != #[trigger] l[k] by {
        assert(l[k] == vs[k + j + 1] && l[k - 1] == vs[k + j]);
    }
}

/// The rest of literal group `i` after value `j`, and the groups after it:
/// their bytes start at `end`, and their values follow `at`.
#[verifier::spinoff_prover]
proof fn lemma_lit_tail_facts<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, v: O::V, lit: nat, end: int, i: int, j: int)
    requires valid_slab::<O>(data), tail_lit::<O>(data, at, v, lit, end, i, j),
    ensures ({
        let gs = slab_groups::<O>(data);
        let vs = gs[i]->values;
        let l = vs.skip(j + 1);
        let rest = gs.skip(i + 1);
        &&& data.skip(end) == encs::<O>(l) + groups_bytes::<O>(rest)
        &&& slab_values::<O>(data).skip(at + 1) == l + groups_values(rest, O::null())
        &&& canonical::<O>(rest)
        &&& may_follow(vs.last(), rest)
        &&& !O::is_null(v) && l.len() == lit
        &&& lit == 0 ==> vs.last() == v
        &&& lit > 0 ==> group_ok::<O>(Group::Lit { values: l }) && v != l[0] && l.last() == vs.last()
    }),
{
    let gs = slab_groups::<O>(data);
    let vs = gs[i]->values;
    let l = vs.skip(j + 1);
    let rest = gs.skip(i + 1);
    lemma_slab_after::<O>(data, i);
    lemma_slab_group::<O>(data, i);
    lemma_slab_lit::<O>(data, i, j);
    let b = gb::<O>(gs, i) + lit_head::<O>(vs.len()).len() + encs::<O>(vs.take(j)).len();
    let e = O::enc(vs[j]);
    let after = encs::<O>(l) + groups_bytes::<O>(rest);
    assert(data.skip(b).len() == data.len() - b);
    assert(end == b + e.len());
    lemma_skip_skip(data, b, e.len() as int);
    assert((e + after).skip(e.len() as int) =~= after);
    let a = groups_values(gs.take(i), O::null());
    assert((a + vs + groups_values(rest, O::null())).skip(at + 1) =~= l + groups_values(rest, O::null()));
    if rest.len() > 0 {
        lemma_slab_group::<O>(data, i + 1);
        assert(adjacent_ok(gs[i], gs[i + 1]));
    }
    if lit > 0 {
        lemma_lit_rest::<O>(vs, j);
    }
}

/// A literal `w` the writer closed, extended by the literal values `l`
/// after it in the slab: still canonical.
proof fn lemma_lit_extend<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, w: Seq<O::V>, l: Seq<O::V>)
    requires
        canonical::<O>(gs.push(Group::Lit { values: w })),
        group_ok::<O>(Group::Lit { values: l }),
        w.len() > 0 && w.last() != l[0], w.len() + l.len() <= i64::MAX,
    ensures canonical::<O>(gs.push(Group::Lit { values: w + l })),
{
    lemma_canonical_split::<O>(gs, Group::Lit { values: w });
    let n = w + l;
    assert forall|k: int| 0 <= k < n.len() implies !O::is_null(#[trigger] n[k]) by {
        if k < w.len() {
            assert(n[k] == w[k]);
        } else {
            assert(n[k] == l[k - w.len()]);
        }
    }
    assert forall|k: int| 0 < k < n.len() implies n[k - 1] != #[trigger] n[k] by {
        if k < w.len() {
            assert(n[k] == w[k] && n[k - 1] == w[k - 1]);
        } else if k == w.len() {
            assert(n[k] == l[0] && n[k - 1] == w.last());
        } else {
            assert(n[k] == l[k - w.len()] && n[k - 1] == l[k - 1 - w.len()]);
        }
    }
    assert(n[0] == w[0]);
    lemma_canonical_join::<O>(gs, Group::Lit { values: n });
}

/// Groups ending in a literal or repeat run of `last`, then groups that
/// may follow it: canonical, with their bytes and values joined.
proof fn lemma_join_rest<'a, O: RleOps<'a>>(a: Seq<Group<O::V>>, rest: Seq<Group<O::V>>, last: O::V)
    requires
        canonical::<O>(a), canonical::<O>(rest), a.len() > 0, may_follow(last, rest),
        match a.last() {
            Group::Run { value, .. } => value == last,
            Group::Lit { values } => values.len() > 0 && values.last() == last,
            Group::Null { .. } => false,
        },
    ensures
        encodes::<O>(groups_bytes::<O>(a) + groups_bytes::<O>(rest),
            groups_values(a, O::null()) + groups_values(rest, O::null())),
{
    if rest.len() > 0 {
        assert(adjacent_ok(a.last(), rest[0]));
    }
    lemma_canonical_concat::<O>(a, rest);
    lemma_groups_bytes_concat::<O>(a, rest);
    lemma_groups_values_concat(a, rest, O::null());
}

/// A state ending in `v`, flushed onto the literal tail at `at`: the
/// bytes written, then the slab's from `end`, are the canonical encoding
/// of the values written, then the slab's after `at`.
#[verifier::spinoff_prover]
#[verifier::rlimit(30)]
proof fn lemma_join_lit<'a, W: WriteOps<'a>>(s1: State<'a, W>, pre1: Seq<u8>, b1: Seq<u8>, base: int, gs1: Seq<Group<VOf<'a, W>>>, ws1: Seq<VOf<'a, W>>, h0: i64, b2: Seq<u8>, rw2: Option<RewriteHeader>, data: Seq<u8>, at: int, v: VOf<'a, W>, lit: nat, end: int, i: int, j: int)
    requires
        rel(s1, pre1, b1, base, gs1, ws1, h0), last_is(s1, v),
        lit == 0 ==> flushed(s1, b1, b2, rw2),
        lit > 0 ==> flushed_lit(s1, b1, b2, rw2, lit),
        s1.plen() + lit <= i64::MAX,
        valid_slab::<W::R>(data), tail_lit::<W::R>(data, at, v, lit, end, i, j),
    ensures
        0 <= base <= b2.len(),
        encodes::<W::R>(apply::<W>(rw2, pre1, h0) + b2.skip(base) + data.skip(end),
            values_of(s1, gs1, ws1) + slab_values::<W::R>(data).skip(at + 1)),
{
    let gs = slab_groups::<W::R>(data);
    let vs = gs[i]->values;
    let l = vs.skip(j + 1);
    let rest = gs.skip(i + 1);
    let nul = <W::R as RleOps<'a>>::null();
    let out = apply::<W>(rw2, pre1, h0) + b2.skip(base);
    lemma_lit_tail_facts::<W::R>(data, at, v, lit, end, i, j);
    lemma_last_group::<W>(s1, gs1, ws1, v);
    let a = all_groups(s1, gs1, ws1);
    if lit == 0 {
        assert(l =~= Seq::<VOf<'a, W>>::empty());
        assert(encs::<W::R>(l) =~= Seq::<u8>::empty());
        assert(out + data.skip(end) =~= groups_bytes::<W::R>(a) + groups_bytes::<W::R>(rest));
        assert(values_of(s1, gs1, ws1) + slab_values::<W::R>(data).skip(at + 1)
            =~= groups_values(a, nul) + groups_values(rest, nul));
        lemma_join_rest::<W::R>(a, rest, vs.last());
    } else if s1 is Run {
        let n = Group::<VOf<'a, W>>::Lit { values: l };
        lemma_canonical_join::<W::R>(a, n);
        lemma_push::<W::R>(a, n);
        assert(out + data.skip(end) =~= groups_bytes::<W::R>(a.push(n)) + groups_bytes::<W::R>(rest));
        assert(values_of(s1, gs1, ws1) + slab_values::<W::R>(data).skip(at + 1)
            =~= groups_values(a.push(n), nul) + groups_values(rest, nul));
        lemma_join_rest::<W::R>(a.push(n), rest, vs.last());
    } else {
        // the pending literal (or lone value) takes in the slab's
        let w = pend_group(s1, ws1)->Some_0->values;
        assert(w.len() > 0 && w.last() == v);
        assert(w.len() == s1.plen());
        let n = Group::<VOf<'a, W>>::Lit { values: w + l };
        lemma_lit_extend::<W::R>(gs1, w, l);
        lemma_push::<W::R>(gs1, n);
        lemma_push::<W::R>(gs1, Group::Lit { values: w });
        lemma_encs_concat::<W::R>(w, l);
        if s1 is Lone {
            reveal_with_fuel(encs, 2);
            assert(encs::<W::R>(w) =~= <W::R as RleOps<'a>>::enc(v));
        }
        assert(out + data.skip(end) =~= groups_bytes::<W::R>(gs1.push(n)) + groups_bytes::<W::R>(rest));
        assert(values_of(s1, gs1, ws1) + slab_values::<W::R>(data).skip(at + 1)
            =~= groups_values(gs1.push(n), nul) + groups_values(rest, nul));
        lemma_join_rest::<W::R>(gs1.push(n), rest, vs.last());
    }
}

// ── Composing the writer's steps ────────────────────────────────────────────

/// The rewrite two steps return together: the second's, else the first's.
pub(crate) open spec fn comb(rw1: Option<RewriteHeader>, rw2: Option<RewriteHeader>) -> Option<RewriteHeader> {
    if rw2 is Some { rw2 } else { rw1 }
}

/// `a += b` on flush results: the rewrites combine as [`comb`].
#[verifier::external_body]
pub(crate) fn add_flush(a: &mut FlushState, b: FlushState)
    ensures final(a).rewrite == comb(old(a).rewrite, b.rewrite),
{
    *a += b;
}

/// Of two rewrites at most one applies, so applying both is applying
/// the one returned.
pub(crate) proof fn lemma_apply_comb<'a, W: WriteOps<'a>>(rw1: Option<RewriteHeader>, rw2: Option<RewriteHeader>, pre: Seq<u8>, h0: i64)
    requires !(rw1 is Some && rw2 is Some),
    ensures apply::<W>(rw2, apply::<W>(rw1, pre, h0), h0) == apply::<W>(comb(rw1, rw2), pre, h0),
{
}

/// No postfix: a plain flush.
proof fn lemma_joined_none<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, b1: Seq<u8>, rw: Option<RewriteHeader>)
    requires flushed(s0, b0, b1, rw),
    ensures joined(s0, b0, b1, rw, None),
{
    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64, data: Seq<u8>, at: int, end: int|
        #[trigger] rel(s0, pre, b0, base, gs, ws, h0) && #[trigger] postfix_ok::<W::R>(data, at, None, end)
        implies 0 <= base <= b1.len()
            && encodes::<W::R>(apply::<W>(rw, pre, h0) + b1.skip(base) + data.skip(end),
                values_of(s0, gs, ws) + slab_values::<W::R>(data).skip(at)) by {
        let a = all_groups(s0, gs, ws);
        assert(apply::<W>(rw, pre, h0) + b1.skip(base) + data.skip(end) =~= groups_bytes::<W::R>(a));
        assert(values_of(s0, gs, ws) + slab_values::<W::R>(data).skip(at) =~= groups_values(a, <W::R as RleOps<'a>>::null()));
    }
}

/// A run postfix: appended, then flushed.
#[verifier::spinoff_prover]
proof fn lemma_joined_run<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, s1: State<'a, W>, b1: Seq<u8>, b2: Seq<u8>, rw1: Option<RewriteHeader>, rw2: Option<RewriteHeader>, count: usize, v: VOf<'a, W>)
    requires
        step(s0, b0, s1, b1, rw1, copies(count as nat, v)), rw_ok(s0, s1, rw1),
        count >= 1 ==> last_is(s1, v),
        count >= 2 && !<W::R as RleOps<'a>>::is_null(v) ==> s1 is Run,
        flushed(s1, b1, b2, rw2), rw2 is Some ==> pre_lit(s1),
    ensures joined(s0, b0, b2, comb(rw1, rw2), Some(Postfix::Run { count, value: v })),
{
    hide(canonical);
    let p = Some(Postfix::Run { count, value: v });
    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64, data: Seq<u8>, at: int, end: int|
        #[trigger] rel(s0, pre, b0, base, gs, ws, h0) && #[trigger] postfix_ok::<W::R>(data, at, p, end)
        implies 0 <= base <= b2.len()
            && encodes::<W::R>(apply::<W>(comb(rw1, rw2), pre, h0) + b2.skip(base) + data.skip(end),
                values_of(s0, gs, ws) + slab_values::<W::R>(data).skip(at)) by {
        let i = choose|i: int| tail_run::<W::R>(data, at, count as nat, v, end, i);
        let pre1 = apply::<W>(rw1, pre, h0);
        let (gs1, ws1) = choose|gs1: Seq<Group<VOf<'a, W>>>, ws1: Seq<VOf<'a, W>>| #[trigger] rel(s1, pre1, b1, base, gs1, ws1, h0)
            && values_of(s1, gs1, ws1) == values_of(s0, gs, ws) + copies(count as nat, v);
        let a = all_groups(s1, gs1, ws1);
        lemma_apply_comb::<W>(rw1, rw2, pre, h0);
        lemma_last_group::<W>(s1, gs1, ws1, v);
        lemma_join_run::<W::R>(a, data, at, count as nat, v, end, i);
        lemma_run_tail_values::<W::R>(data, at, count as nat, v, end, i);
        let rest = groups_values(slab_groups::<W::R>(data).skip(i + 1), <W::R as RleOps<'a>>::null());
        assert(values_of(s0, gs, ws) + slab_values::<W::R>(data).skip(at) =~= groups_values(a, <W::R as RleOps<'a>>::null()) + rest);
    }
}

/// The slab value at a literal tail's position.
proof fn lemma_lit_tail_value<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, v: O::V, lit: nat, end: int, i: int, j: int)
    requires valid_slab::<O>(data), tail_lit::<O>(data, at, v, lit, end, i, j),
    ensures slab_values::<O>(data).skip(at) == seq![v] + slab_values::<O>(data).skip(at + 1),
{
    let gs = slab_groups::<O>(data);
    lemma_slab_group::<O>(data, i);
    assert(group_values(gs[i], O::null())[j] == v);
    assert(slab_values::<O>(data)[at] == v);
    assert(slab_values::<O>(data).skip(at) =~= seq![v] + slab_values::<O>(data).skip(at + 1));
}

/// A literal postfix: its value appended, then flushed onto the rest of
/// its literal.
#[verifier::spinoff_prover]
proof fn lemma_joined_lit<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, s1: State<'a, W>, b1: Seq<u8>, b2: Seq<u8>, rw1: Option<RewriteHeader>, rw2: Option<RewriteHeader>, v: VOf<'a, W>, lit: usize)
    requires
        step(s0, b0, s1, b1, rw1, copies(1, v)), rw_ok(s0, s1, rw1), last_is(s1, v),
        lit == 0 ==> flushed(s1, b1, b2, rw2),
        lit > 0 ==> flushed_lit(s1, b1, b2, rw2, lit as nat),
        rw2 is Some ==> pre_lit(s1), s1.plen() + lit <= i64::MAX,
    ensures joined(s0, b0, b2, comb(rw1, rw2), Some(Postfix::Lit { value: v, lit })),
{
    hide(canonical);
    let p = Some(Postfix::Lit { value: v, lit });
    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64, data: Seq<u8>, at: int, end: int|
        #[trigger] rel(s0, pre, b0, base, gs, ws, h0) && #[trigger] postfix_ok::<W::R>(data, at, p, end)
        implies 0 <= base <= b2.len()
            && encodes::<W::R>(apply::<W>(comb(rw1, rw2), pre, h0) + b2.skip(base) + data.skip(end),
                values_of(s0, gs, ws) + slab_values::<W::R>(data).skip(at)) by {
        let (i, j) = choose|i: int, j: int| tail_lit::<W::R>(data, at, v, lit as nat, end, i, j);
        let pre1 = apply::<W>(rw1, pre, h0);
        let (gs1, ws1) = choose|gs1: Seq<Group<VOf<'a, W>>>, ws1: Seq<VOf<'a, W>>| #[trigger] rel(s1, pre1, b1, base, gs1, ws1, h0)
            && values_of(s1, gs1, ws1) == values_of(s0, gs, ws) + copies(1, v);
        lemma_apply_comb::<W>(rw1, rw2, pre, h0);
        lemma_join_lit::<W>(s1, pre1, b1, base, gs1, ws1, h0, b2, rw2, data, at, v, lit as nat, end, i, j);
        lemma_lit_tail_value::<W::R>(data, at, v, lit as nat, end, i, j);
        assert(copies(1, v) =~= seq![v]);
        assert(values_of(s0, gs, ws) + slab_values::<W::R>(data).skip(at)
            =~= values_of(s1, gs1, ws1) + slab_values::<W::R>(data).skip(at + 1));
    }
}

/// A repeat run's last value then a literal's first: both appended, then
/// flushed onto the rest of the literal.
#[verifier::spinoff_prover]
proof fn lemma_joined_lone<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, s1: State<'a, W>, b1: Seq<u8>, s2: State<'a, W>, b2: Seq<u8>, b3: Seq<u8>, rw1: Option<RewriteHeader>, rw2: Option<RewriteHeader>, rw3: Option<RewriteHeader>, lone: VOf<'a, W>, v: VOf<'a, W>, lit: usize)
    requires
        step(s0, b0, s1, b1, rw1, copies(1, lone)), rw_ok(s0, s1, rw1),
        step(s1, b1, s2, b2, rw2, copies(1, v)), rw_ok(s1, s2, rw2), last_is(s2, v),
        lit == 0 ==> flushed(s2, b2, b3, rw3),
        lit > 0 ==> flushed_lit(s2, b2, b3, rw3, lit as nat),
        rw3 is Some ==> pre_lit(s2), s2.plen() + lit <= i64::MAX,
    ensures joined(s0, b0, b3, comb(comb(rw1, rw2), rw3), Some(Postfix::LonePlusLit { lone, value: v, lit })),
{
    hide(canonical);
    let p = Some(Postfix::LonePlusLit { lone, value: v, lit });
    let nul = <W::R as RleOps<'a>>::null();
    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64, data: Seq<u8>, at: int, end: int|
        #[trigger] rel(s0, pre, b0, base, gs, ws, h0) && #[trigger] postfix_ok::<W::R>(data, at, p, end)
        implies 0 <= base <= b3.len()
            && encodes::<W::R>(apply::<W>(comb(comb(rw1, rw2), rw3), pre, h0) + b3.skip(base) + data.skip(end),
                values_of(s0, gs, ws) + slab_values::<W::R>(data).skip(at)) by {
        let i = choose|i: int| tail_lone::<W::R>(data, at, lone, v, lit as nat, end, i);
        let pre1 = apply::<W>(rw1, pre, h0);
        let (gs1, ws1) = choose|gs1: Seq<Group<VOf<'a, W>>>, ws1: Seq<VOf<'a, W>>| #[trigger] rel(s1, pre1, b1, base, gs1, ws1, h0)
            && values_of(s1, gs1, ws1) == values_of(s0, gs, ws) + copies(1, lone);
        let pre2 = apply::<W>(rw2, pre1, h0);
        let (gs2, ws2) = choose|gs2: Seq<Group<VOf<'a, W>>>, ws2: Seq<VOf<'a, W>>| #[trigger] rel(s2, pre2, b2, base, gs2, ws2, h0)
            && values_of(s2, gs2, ws2) == values_of(s1, gs1, ws1) + copies(1, v);
        lemma_apply_comb::<W>(rw1, rw2, pre, h0);
        lemma_apply_comb::<W>(comb(rw1, rw2), rw3, pre, h0);
        lemma_join_lit::<W>(s2, pre2, b2, base, gs2, ws2, h0, b3, rw3, data, at + 1, v, lit as nat, end, i + 1, 0);
        lemma_lit_tail_value::<W::R>(data, at + 1, v, lit as nat, end, i + 1, 0);
        // the value before the literal is the repeat run's last
        let dg = slab_groups::<W::R>(data);
        lemma_slab_group::<W::R>(data, i);
        assert(group_values(dg[i], nul)[group_len(dg[i]) - 1] == lone);
        assert(slab_values::<W::R>(data)[at] == lone);
        assert(slab_values::<W::R>(data).skip(at) =~= seq![lone] + slab_values::<W::R>(data).skip(at + 1));
        assert(copies(1, v) =~= seq![v]);
        assert(copies(1, lone) =~= seq![lone]);
        assert(values_of(s0, gs, ws) + slab_values::<W::R>(data).skip(at)
            =~= values_of(s2, gs2, ws2) + slab_values::<W::R>(data).skip(at + 2));
    }
}

impl<'a, W: WriteOps<'a>> State<'a, W> {
    /// Feed the postfix in, then flush: the bytes written, then the
    /// slab's after the postfix, encode the values written then the
    /// slab's from the edit point.
    pub(crate) fn flush_postfix(&mut self, buf: &mut Vec<u8>, postfix: Option<Postfix<W::Cow>>) -> (f: FlushState)
        requires
            wf(*old(self), old(buf)@), nulls_ok::<W>(postfix),
            old(self).plen() + plen_of(postfix) <= i64::MAX, old(self).plen() + plen_of(postfix) + 2 < usize::MAX,
        ensures
            (*final(self)) is Empty,
            rw_from(*old(self), f.rewrite),
            joined(*old(self), old(buf)@, final(buf)@, f.rewrite, pmap::<W>(postfix)),
    {
        let ghost s0 = *self;
        let ghost b0 = buf@;
        match postfix {
            None => {
                let f = self.flush(buf);
                proof {
                    lemma_joined_none::<W>(s0, b0, buf@, f.rewrite);
                }
                f
            },
            Some(Postfix::Run { count, value }) => {
                let ghost v = W::val(value);
                let mut f = self.append_cow(buf, value, count);
                let ghost s1 = *self;
                let ghost b1 = buf@;
                let ghost rw1 = f.rewrite;
                let f2 = self.flush(buf);
                add_flush(&mut f, f2);
                proof {
                    lemma_joined_run::<W>(s0, b0, s1, b1, buf@, rw1, f2.rewrite, count, v);
                }
                f
            },
            Some(Postfix::Lit { value, lit }) => {
                let ghost v = W::val(value);
                let mut f = self.append_cow(buf, value, 1);
                let ghost s1 = *self;
                let ghost b1 = buf@;
                let ghost rw1 = f.rewrite;
                let f2 = self.flush_with_lit(buf, lit);
                add_flush(&mut f, f2);
                proof {
                    lemma_joined_lit::<W>(s0, b0, s1, b1, buf@, rw1, f2.rewrite, v, lit);
                }
                f
            },
            Some(Postfix::LonePlusLit { lone, value, lit }) => {
                let ghost l = W::val(lone);
                let ghost v = W::val(value);
                let mut f = self.append_cow(buf, lone, 1);
                let ghost s1 = *self;
                let ghost b1 = buf@;
                let ghost rw1 = f.rewrite;
                let f1 = self.append_cow(buf, value, 1);
                let ghost s2 = *self;
                let ghost b2 = buf@;
                add_flush(&mut f, f1);
                let f2 = self.flush_with_lit(buf, lit);
                add_flush(&mut f, f2);
                proof {
                    lemma_joined_lone::<W>(s0, b0, s1, b1, s2, b2, buf@, rw1, f1.rewrite, f2.rewrite, l, v, lit);
                }
                f
            },
        }
    }
}

} // verus!

impl<V> Postfix<V> {
    /// The same postfix, its values mapped by `f`.
    #[inline]
    pub(crate) fn map<U>(self, mut f: impl FnMut(V) -> U) -> Postfix<U> {
        match self {
            Postfix::Run { count, value } => Postfix::Run {
                count,
                value: f(value),
            },
            Postfix::Lit { value, lit } => Postfix::Lit {
                value: f(value),
                lit,
            },
            Postfix::LonePlusLit { lone, value, lit } => Postfix::LonePlusLit {
                lone: f(lone),
                value: f(value),
                lit,
            },
        }
    }
}
