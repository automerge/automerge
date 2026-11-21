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
//! (`joined`).

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

// spec functions exist only when Verus compiles the crate
#[cfg(verus_keep_ghost)]
use super::dec::*;
use super::model::*;
#[cfg(verus_keep_ghost)]
use super::read::{lit_tail_of, tail_bytes_of};
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

/// The slab segments a postfix's values stand for.
pub(crate) open spec fn ppend<V>(p: Option<Postfix<V>>) -> int {
    match p {
        None => 0,
        Some(Postfix::LonePlusLit { .. }) => 2,
        Some(_) => 1,
    }
}

impl<V> Postfix<V> {
    /// The slab segments the postfix's values stand for.
    pub(crate) fn pending(&self) -> (r: usize)
        ensures 1 <= r <= 2, r == ppend(Some(*self)),
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
        Group::Null { .. } => value == O::null(),
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

/// The values a postfix appends to the writer (the following literal
/// values stay bytes).
pub(crate) open spec fn pcount<V>(p: Option<Postfix<V>>) -> nat {
    match p {
        None => 0,
        Some(Postfix::Run { count, .. }) => count as nat,
        Some(Postfix::Lit { .. }) => 1,
        Some(Postfix::LonePlusLit { .. }) => 2,
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

/// The postfix holds a null only in a nullable column, by its values.
pub(crate) open spec fn vnulls_ok<'a, O: RleOps<'a>>(p: Option<Postfix<O::V>>) -> bool {
    match p {
        None => true,
        Some(Postfix::Run { value, .. }) | Some(Postfix::Lit { value, .. }) => O::is_null(value) ==> O::nullable(),
        Some(Postfix::LonePlusLit { lone, value, .. }) => (O::is_null(lone) || O::is_null(value)) ==> O::nullable(),
    }
}

/// `bytes` are the canonical encoding of `vals`.
pub(crate) open spec fn encodes<'a, O: RleOps<'a>>(bytes: Seq<u8>, vals: Seq<O::V>, segs: int) -> bool {
    exists|gs: Seq<Group<O::V>>| #[trigger] canonical::<O>(gs) && groups_bytes::<O>(gs) == bytes
        && groups_values(gs, O::null()) == vals && groups_segments(gs) == segs
}

/// `bytes` are a canonical encoding whose last group is `tb` bytes long,
/// and `tl` the width of its last value if it is a literal (else 0): a
/// slab's tail record.
pub(crate) open spec fn tail_ok<'a, O: RleOps<'a>>(bytes: Seq<u8>, tb: int, tl: int) -> bool {
    exists|gs: Seq<Group<O::V>>| #[trigger] canonical::<O>(gs) && groups_bytes::<O>(gs) == bytes
        && tb == tail_bytes_of::<O>(gs) && tl == lit_tail_of::<O>(gs)
}

/// The tail record of a slab rebuilt from bytes before the edit (`plen`
/// of them), the bytes written (`mid`, the writer's position `w` naming
/// its last group, the buffer read from `base`) and the slab's bytes
/// after the postfix (`q`), whose old tail record is `(tb, tl)`: the old
/// one if its last run is among those carried bytes, else the writer's
/// last group's, which may run on into them.
pub(crate) open spec fn tail_after(w: WPos, plen: int, base: int, mid: int, q: int, tb: int, tl: int) -> (int, int) {
    let start = wstart(w, plen, base);
    let wl = match w {
        WPos::Lit { bytes, .. } => bytes as int,
        _ => 0int,
    };
    if q == 0 {
        (plen + mid - start, wl)
    } else if tb <= q {
        (tb, tl)
    } else {
        (plen + mid + q - start, tl)
    }
}

/// Flushing `(s0, b0)` with the postfix `p` (the buffer read from its
/// start) left `w` such that merging gives the rebuilt slab's tail.
pub(crate) open spec fn joined_tail<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, b1: Seq<u8>, rw: Option<RewriteHeader>, p: Option<Postfix<VOf<'a, W>>>, w: WPos) -> bool {
    forall|pre: Seq<u8>, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64, data: Seq<u8>, at: int, end: int|
        #[trigger] rel(s0, pre, b0, 0, gs, ws, h0) && #[trigger] postfix_ok::<W::R>(data, at, p, end)
        ==> {
            let t = tail_after(w, apply::<W>(rw, pre, h0).len() as int, 0, b1.len() as int, data.skip(end).len() as int,
                tail_bytes_of::<W::R>(slab_groups::<W::R>(data)) as int, lit_tail_of::<W::R>(slab_groups::<W::R>(data)) as int);
            tail_ok::<W::R>(apply::<W>(rw, pre, h0) + b1.skip(0) + data.skip(end), t.0, t.1)
        }
}

/// The segments of the slab after the postfix `p` read at `at`: the rest
/// of a literal it ends inside, and the groups after.
pub(crate) open spec fn rest_segs<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, p: Option<Postfix<O::V>>, end: int) -> int {
    let gs = slab_groups::<O>(data);
    match p {
        None => 0,
        Some(Postfix::Run { count, value }) => {
            let i = choose|i: int| tail_run::<O>(data, at, count as nat, value, end, i);
            groups_segments(gs.skip(i + 1)) as int
        },
        Some(Postfix::Lit { value, lit }) => {
            let (i, j) = choose|i: int, j: int| tail_lit::<O>(data, at, value, lit as nat, end, i, j);
            lit + groups_segments(gs.skip(i + 1))
        },
        Some(Postfix::LonePlusLit { lone, value, lit }) => {
            let i = choose|i: int| tail_lone::<O>(data, at, lone, value, lit as nat, end, i);
            lit + groups_segments(gs.skip(i + 2))
        },
    }
}

/// Flushing `(s0, b0)` with the postfix `p` wrote `b1` and returned the
/// rewrite `rw`: wherever `p` came from, those bytes then the slab's
/// bytes after the postfix are the canonical encoding of the values
/// written, then the slab's values from the edit point.
pub(crate) open spec fn joined<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, b1: Seq<u8>, rw: Option<RewriteHeader>, p: Option<Postfix<VOf<'a, W>>>, seg: int) -> bool {
    forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64, data: Seq<u8>, at: int, end: int|
        #[trigger] rel(s0, pre, b0, base, gs, ws, h0) && #[trigger] postfix_ok::<W::R>(data, at, p, end)
        ==> 0 <= base <= b1.len()
            && encodes::<W::R>(apply::<W>(rw, pre, h0) + b1.skip(base) + data.skip(end),
                values_of(s0, gs, ws) + slab_values::<W::R>(data).skip(at),
                acc(s0, gs, ws) + seg + rest_segs::<W::R>(data, at, p, end))
}

/// A postfix of values borrowed from the slab, for the writer.
pub(crate) fn cow_postfix<'a, W: WriteOps<'a>>(p: Option<Postfix<VOf<'a, W>>>) -> (r: Option<Postfix<W::Cow>>)
    ensures pmap::<W>(r) == p,
{
    match p {
        None => None,
        Some(Postfix::Run { count, value }) => Some(Postfix::Run { count, value: W::cow_ref(value) }),
        Some(Postfix::Lit { value, lit }) => Some(Postfix::Lit { value: W::cow_ref(value), lit }),
        Some(Postfix::LonePlusLit { lone, value, lit }) =>
            Some(Postfix::LonePlusLit { lone: W::cow_ref(lone), value: W::cow_ref(value), lit }),
    }
}

/// A postfix of a valid slab holds a null only in a nullable column.
pub(crate) proof fn lemma_postfix_nulls<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, p: Option<Postfix<O::V>>, end: int)
    requires postfix_ok::<O>(data, at, p, end),
    ensures vnulls_ok::<O>(p),
{
    match p {
        None => {},
        Some(Postfix::Run { count, value }) => {
            let i = choose|i: int| tail_run::<O>(data, at, count as nat, value, end, i);
            lemma_slab_group::<O>(data, i);
        },
        Some(Postfix::Lit { value, lit }) => {
            let (i, j) = choose|i: int, j: int| tail_lit::<O>(data, at, value, lit as nat, end, i, j);
            lemma_slab_group::<O>(data, i);
        },
        Some(Postfix::LonePlusLit { lone, value, lit }) => {
            let i = choose|i: int| tail_lone::<O>(data, at, lone, value, lit as nat, end, i);
            lemma_slab_group::<O>(data, i);
            lemma_slab_group::<O>(data, i + 1);
        },
    }
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
pub(crate) proof fn lemma_canonical_concat<'a, O: RleOps<'a>>(a: Seq<Group<O::V>>, b: Seq<Group<O::V>>)
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
pub(crate) proof fn lemma_slab_after<'a, O: RleOps<'a>>(data: Seq<u8>, i: int)
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
            groups_values(a, O::null()) + groups_values(slab_groups::<O>(data).skip(i + 1), O::null()),
            (groups_segments(a) + groups_segments(slab_groups::<O>(data).skip(i + 1))) as int),
        tail_ok::<O>(groups_bytes::<O>(a) + data.skip(end),
            tail_bytes_of::<O>(a + slab_groups::<O>(data).skip(i + 1)) as int,
            lit_tail_of::<O>(a + slab_groups::<O>(data).skip(i + 1)) as int),
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
            // the null value of a null run is a null
            O::axiom_null();
            assert(gs[i] is Run);
        }
        assert(adjacent_ok(a.last(), rest[0]));
    }
    lemma_canonical_concat::<O>(a, rest);
    lemma_groups_bytes_concat::<O>(a, rest);
    lemma_groups_values_concat(a, rest, O::null());
    lemma_groups_len_concat(a, rest);
    assert(canonical::<O>(a + rest));
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
    // group_ok's quantifiers can loop; nothing here needs its body
    hide(group_ok);
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
            groups_values(a, O::null()) + groups_values(rest, O::null()),
            (groups_segments(a) + groups_segments(rest)) as int),
        tail_ok::<O>(groups_bytes::<O>(a) + groups_bytes::<O>(rest),
            tail_bytes_of::<O>(a + rest) as int, lit_tail_of::<O>(a + rest) as int),
{
    if rest.len() > 0 {
        assert(adjacent_ok(a.last(), rest[0]));
    }
    lemma_canonical_concat::<O>(a, rest);
    lemma_groups_bytes_concat::<O>(a, rest);
    lemma_groups_values_concat(a, rest, O::null());
    lemma_groups_len_concat(a, rest);
    assert(canonical::<O>(a + rest));
}

/// `a`, then `b` and `c`, are `a` and `b`, then `c`.
proof fn lemma_cat3<T>(a: Seq<T>, b: Seq<T>, c: Seq<T>)
    ensures a + (b + c) == a + b + c,
{
    assert(a + (b + c) =~= a + b + c);
}

/// `a` and `b`, then `c` and `d`, are `a`, then `b` and `c`, then `d`.
proof fn lemma_cat4<T>(a: Seq<T>, b: Seq<T>, c: Seq<T>, d: Seq<T>)
    ensures (a + b) + (c + d) == a + (b + c) + d,
{
    assert((a + b) + (c + d) =~= a + (b + c) + d);
}

/// What flushing a state ending in a non-null `v` wrote, onto `lit`
/// literal values that follow: its groups' bytes, and for `lit > 0` the
/// header of the literal those values join, a new one after a repeat run
/// and else the pending one, of the values `w`.
proof fn lemma_flush_onto<'a, W: WriteOps<'a>>(s1: State<'a, W>, pre1: Seq<u8>, b1: Seq<u8>, base: int, gs1: Seq<Group<VOf<'a, W>>>, ws1: Seq<VOf<'a, W>>, h0: i64, b2: Seq<u8>, rw2: Option<RewriteHeader>, v: VOf<'a, W>, lit: nat, seg: int)
    requires
        rel(s1, pre1, b1, base, gs1, ws1, h0), last_is(s1, v),
        !<W::R as RleOps<'a>>::is_null(v),
        lit == 0 ==> flushed(s1, b1, b2, rw2, seg),
        lit > 0 ==> flushed_lit(s1, b1, b2, rw2, lit, seg),
        lit > 0 ==> lit_fits(s1, lit),
    ensures ({
        let a = all_groups(s1, gs1, ws1);
        let out = apply::<W>(rw2, pre1, h0) + b2.skip(base);
        let w = pend_group(s1, ws1)->Some_0->values;
        &&& 0 <= base <= b2.len()
        &&& canonical::<W::R>(a)
        &&& lit == 0 || s1 is Run ==> groups_segments(a) == acc(s1, gs1, ws1) + seg
        &&& lit == 0 ==> out == groups_bytes::<W::R>(a)
        &&& lit > 0 && s1 is Run ==> out == groups_bytes::<W::R>(a) + lit_head::<W::R>(lit)
        &&& lit > 0 && !(s1 is Run) ==> {
            &&& s1 is Lit || s1 is Lone
            &&& a == gs1.push(Group::Lit { values: w })
            &&& w.len() > 0 && w.last() == v && w.len() + lit <= i64::MAX
            &&& out == groups_bytes::<W::R>(gs1) + lit_head::<W::R>(w.len() + lit) + encs::<W::R>(w)
            &&& groups_segments(gs1) + w.len() == acc(s1, gs1, ws1) + seg
        }
    }),
{
    hide(canonical);
    if let State::Lone(c) = s1 {
        lemma_lone_bytes::<W::R>(W::val(c));
    }
}

/// Groups `p` and a literal of `vals` ending in `last`, then groups that
/// may follow it: canonical, with their bytes and values joined, and the
/// literal the last group where none follow.
proof fn lemma_join_pushed<'a, O: RleOps<'a>>(p: Seq<Group<O::V>>, vals: Seq<O::V>, rest: Seq<Group<O::V>>, last: O::V)
    requires
        canonical::<O>(p.push(Group::Lit { values: vals })), canonical::<O>(rest),
        vals.len() > 0, vals.last() == last, may_follow(last, rest),
    ensures ({
        let bytes = groups_bytes::<O>(p) + (lit_head::<O>(vals.len()) + encs::<O>(vals)) + groups_bytes::<O>(rest);
        &&& encodes::<O>(bytes, groups_values(p, O::null()) + vals + groups_values(rest, O::null()),
            (groups_segments(p) + vals.len() + groups_segments(rest)) as int)
        &&& tail_ok::<O>(bytes,
            if rest.len() > 0 { tail_bytes_of::<O>(rest) as int } else { bytes.len() - groups_bytes::<O>(p).len() },
            if rest.len() > 0 { lit_tail_of::<O>(rest) as int } else { O::enc(last).len() as int })
    }),
{
    hide(canonical);
    let n = Group::<O::V>::Lit { values: vals };
    lemma_push::<O>(p, n);
    lemma_join_rest::<O>(p.push(n), rest, last);
    lemma_tail_concat::<O>(p.push(n), rest);
}

/// Groups `a` ending in `last`, then the groups `rest` that may follow
/// it, with no literal values `l` between them: canonical, with their
/// bytes and values joined.
proof fn lemma_join_none<'a, O: RleOps<'a>>(a: Seq<Group<O::V>>, rest: Seq<Group<O::V>>, last: O::V, l: Seq<O::V>, out: Seq<u8>, tb: Seq<u8>, tv: Seq<O::V>)
    requires
        canonical::<O>(a), canonical::<O>(rest), a.len() > 0, may_follow(last, rest),
        match a.last() {
            Group::Run { value, .. } => value == last,
            Group::Lit { values } => values.len() > 0 && values.last() == last,
            Group::Null { .. } => false,
        },
        l.len() == 0,
        out == groups_bytes::<O>(a),
        tb == encs::<O>(l) + groups_bytes::<O>(rest),
        tv == l + groups_values(rest, O::null()),
    ensures
        encodes::<O>(out + tb, groups_values(a, O::null()) + tv,
            (groups_segments(a) + groups_segments(rest)) as int),
        tail_ok::<O>(out + tb,
            if rest.len() > 0 { tail_bytes_of::<O>(rest) as int } else { tail_bytes_of::<O>(a) as int },
            if rest.len() > 0 { lit_tail_of::<O>(rest) as int } else { lit_tail_of::<O>(a) as int }),
{
    hide(canonical);
    assert(tb =~= groups_bytes::<O>(rest));
    assert(tv =~= groups_values(rest, O::null()));
    lemma_join_rest::<O>(a, rest, last);
    lemma_tail_concat::<O>(a, rest);
}

/// Groups `a`, then a new literal of the values `l` ending in `last`,
/// whose header the bytes `out` end in, then the groups `rest` that may
/// follow it: canonical, with their bytes and values joined.
proof fn lemma_join_new_lit<'a, O: RleOps<'a>>(a: Seq<Group<O::V>>, rest: Seq<Group<O::V>>, last: O::V, l: Seq<O::V>, out: Seq<u8>, tb: Seq<u8>, tv: Seq<O::V>)
    requires
        canonical::<O>(a), canonical::<O>(rest), may_follow(last, rest),
        group_ok::<O>(Group::Lit { values: l }), l.len() > 0, l.last() == last,
        a.len() > 0 ==> adjacent_ok(a.last(), Group::Lit { values: l }),
        out == groups_bytes::<O>(a) + lit_head::<O>(l.len()),
        tb == encs::<O>(l) + groups_bytes::<O>(rest),
        tv == l + groups_values(rest, O::null()),
    ensures
        encodes::<O>(out + tb, groups_values(a, O::null()) + tv,
            (groups_segments(a) + l.len() + groups_segments(rest)) as int),
        tail_ok::<O>(out + tb,
            if rest.len() > 0 { tail_bytes_of::<O>(rest) as int } else { (out + tb).len() - groups_bytes::<O>(a).len() },
            if rest.len() > 0 { lit_tail_of::<O>(rest) as int } else { O::enc(last).len() as int }),
{
    hide(canonical);
    lemma_canonical_join::<O>(a, Group::Lit { values: l });
    lemma_join_pushed::<O>(a, l, rest, last);
    lemma_cat4(groups_bytes::<O>(a), lit_head::<O>(l.len()), encs::<O>(l), groups_bytes::<O>(rest));
    lemma_cat3(groups_values(a, O::null()), l, groups_values(rest, O::null()));
}

/// Groups `g`, then a literal of `w` that takes in the values `l` ending
/// in `last`, whose header and `w` the bytes `out` end in, then the
/// groups `rest` that may follow it: canonical, with their bytes and
/// values joined.
proof fn lemma_join_open_lit<'a, O: RleOps<'a>>(g: Seq<Group<O::V>>, w: Seq<O::V>, rest: Seq<Group<O::V>>, last: O::V, l: Seq<O::V>, out: Seq<u8>, tb: Seq<u8>, tv: Seq<O::V>)
    requires
        canonical::<O>(g.push(Group::Lit { values: w })), canonical::<O>(rest), may_follow(last, rest),
        group_ok::<O>(Group::Lit { values: l }), l.len() > 0, l.last() == last,
        w.len() > 0 && w.last() != l[0], w.len() + l.len() <= i64::MAX,
        out == groups_bytes::<O>(g) + lit_head::<O>(w.len() + l.len()) + encs::<O>(w),
        tb == encs::<O>(l) + groups_bytes::<O>(rest),
        tv == l + groups_values(rest, O::null()),
    ensures
        encodes::<O>(out + tb, groups_values(g.push(Group::Lit { values: w }), O::null()) + tv,
            (groups_segments(g) + w.len() + l.len() + groups_segments(rest)) as int),
        tail_ok::<O>(out + tb,
            if rest.len() > 0 { tail_bytes_of::<O>(rest) as int } else { (out + tb).len() - groups_bytes::<O>(g).len() },
            if rest.len() > 0 { lit_tail_of::<O>(rest) as int } else { O::enc(last).len() as int }),
{
    hide(canonical);
    let n = w + l;
    let pb = groups_bytes::<O>(g);
    let h = lit_head::<O>(n.len());
    let vg = groups_values(g, O::null());
    let vr = groups_values(rest, O::null());
    lemma_lit_extend::<O>(g, w, l);
    assert(n.last() == l.last());
    lemma_join_pushed::<O>(g, n, rest, last);
    lemma_push::<O>(g, Group::Lit { values: w });
    lemma_encs_concat::<O>(w, l);
    lemma_cat4(pb + h, encs::<O>(w), encs::<O>(l), groups_bytes::<O>(rest));
    lemma_cat3(pb, h, encs::<O>(w) + encs::<O>(l));
    lemma_cat4(vg, w, l, vr);
}

/// A state ending in `v`, flushed onto the literal tail at `at`: the
/// bytes written, then the slab's from `end`, are the canonical encoding
/// of the values written, then the slab's after `at`.
#[verifier::spinoff_prover]
proof fn lemma_join_lit<'a, W: WriteOps<'a>>(s1: State<'a, W>, pre1: Seq<u8>, b1: Seq<u8>, base: int, gs1: Seq<Group<VOf<'a, W>>>, ws1: Seq<VOf<'a, W>>, h0: i64, b2: Seq<u8>, rw2: Option<RewriteHeader>, data: Seq<u8>, at: int, v: VOf<'a, W>, lit: nat, end: int, i: int, j: int, seg: int)
    requires
        rel(s1, pre1, b1, base, gs1, ws1, h0), last_is(s1, v),
        lit == 0 ==> flushed(s1, b1, b2, rw2, seg),
        lit > 0 ==> flushed_lit(s1, b1, b2, rw2, lit, seg),
        lit > 0 ==> lit_fits(s1, lit),
        valid_slab::<W::R>(data), tail_lit::<W::R>(data, at, v, lit, end, i, j),
    ensures
        0 <= base <= b2.len(),
        encodes::<W::R>(apply::<W>(rw2, pre1, h0) + b2.skip(base) + data.skip(end),
            values_of(s1, gs1, ws1) + slab_values::<W::R>(data).skip(at + 1),
            acc(s1, gs1, ws1) + seg + lit + groups_segments(slab_groups::<W::R>(data).skip(i + 1))),
        ({
            let rest = slab_groups::<W::R>(data).skip(i + 1);
            let a = all_groups(s1, gs1, ws1);
            let bytes = apply::<W>(rw2, pre1, h0) + b2.skip(base) + data.skip(end);
            let vs = slab_groups::<W::R>(data)[i]->values;
            let lstart = if s1 is Lit || s1 is Lone { groups_bytes::<W::R>(gs1).len() } else { groups_bytes::<W::R>(a).len() };
            tail_ok::<W::R>(bytes,
                if rest.len() > 0 { tail_bytes_of::<W::R>(rest) as int }
                    else if lit == 0 { tail_bytes_of::<W::R>(a) as int } else { bytes.len() - lstart },
                if rest.len() > 0 { lit_tail_of::<W::R>(rest) as int }
                    else if lit == 0 { lit_tail_of::<W::R>(a) as int } else { <W::R as RleOps<'a>>::enc(vs.last()).len() as int })
        }),
{
    // the case lemmas read these; nothing here needs their bodies
    hide(canonical);
    hide(rel);
    hide(flushed);
    hide(flushed_lit);
    hide(tail_lit);
    hide(encodes);
    hide(tail_ok);
    let gs = slab_groups::<W::R>(data);
    let vs = gs[i]->values;
    let l = vs.skip(j + 1);
    let rest = gs.skip(i + 1);
    let a = all_groups(s1, gs1, ws1);
    let out = apply::<W>(rw2, pre1, h0) + b2.skip(base);
    let tb = data.skip(end);
    let tv = slab_values::<W::R>(data).skip(at + 1);
    lemma_lit_tail_facts::<W::R>(data, at, v, lit, end, i, j);
    lemma_last_group::<W>(s1, gs1, ws1, v);
    lemma_flush_onto::<W>(s1, pre1, b1, base, gs1, ws1, h0, b2, rw2, v, lit, seg);
    if lit == 0 {
        lemma_join_none::<W::R>(a, rest, vs.last(), l, out, tb, tv);
    } else if s1 is Run {
        lemma_join_new_lit::<W::R>(a, rest, vs.last(), l, out, tb, tv);
    } else {
        // the pending literal (or lone value) takes in the slab's
        let w = pend_group(s1, ws1)->Some_0->values;
        lemma_join_open_lit::<W::R>(gs1, w, rest, vs.last(), l, out, tb, tv);
    }
}

/// The last group of groups joined.
pub(crate) proof fn lemma_tail_concat<'a, O: RleOps<'a>>(a: Seq<Group<O::V>>, b: Seq<Group<O::V>>)
    ensures
        tail_bytes_of::<O>(a + b) == if b.len() > 0 { tail_bytes_of::<O>(b) } else { tail_bytes_of::<O>(a) },
        lit_tail_of::<O>(a + b) == if b.len() > 0 { lit_tail_of::<O>(b) } else { lit_tail_of::<O>(a) },
{
    if b.len() > 0 {
        assert((a + b).last() == b.last());
    } else {
        assert(a + b =~= a);
    }
}

// ── Composing the writer's steps ────────────────────────────────────────────

/// The rewrite two steps return together: the second's, else the first's.
pub(crate) open spec fn comb(rw1: Option<RewriteHeader>, rw2: Option<RewriteHeader>) -> Option<RewriteHeader> {
    if rw2 is Some { rw2 } else { rw1 }
}

/// `a += b` on flush results: the segments add up, the rewrites combine
/// as `comb`, and the position is the later one's.
#[inline(always)]
pub(crate) fn add_flush(a: &mut FlushState, b: FlushState)
    requires old(a).segments + b.segments <= usize::MAX,
    ensures
        final(a).rewrite == comb(old(a).rewrite, b.rewrite),
        final(a).segments == old(a).segments + b.segments,
        final(a).wpos == b.wpos,
{
    a.segments = a.segments + b.segments;
    a.wpos = b.wpos;
    if b.rewrite.is_some() {
        a.rewrite = b.rewrite;
    }
}

/// Of two rewrites at most one applies, so applying both is applying
/// the one returned.
pub(crate) proof fn lemma_apply_comb<'a, W: WriteOps<'a>>(rw1: Option<RewriteHeader>, rw2: Option<RewriteHeader>, pre: Seq<u8>, h0: i64)
    requires !(rw1 is Some && rw2 is Some),
    ensures apply::<W>(rw2, apply::<W>(rw1, pre, h0), h0) == apply::<W>(comb(rw1, rw2), pre, h0),
{
}

/// No postfix: a plain flush.
proof fn lemma_joined_none<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, b1: Seq<u8>, rw: Option<RewriteHeader>, seg: int)
    requires flushed(s0, b0, b1, rw, seg),
    ensures joined(s0, b0, b1, rw, None, seg),
{
    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64, data: Seq<u8>, at: int, end: int|
        #[trigger] rel(s0, pre, b0, base, gs, ws, h0) && #[trigger] postfix_ok::<W::R>(data, at, None, end)
        implies 0 <= base <= b1.len()
            && encodes::<W::R>(apply::<W>(rw, pre, h0) + b1.skip(base) + data.skip(end),
                values_of(s0, gs, ws) + slab_values::<W::R>(data).skip(at),
                acc(s0, gs, ws) + seg + rest_segs::<W::R>(data, at, None, end)) by {
        let a = all_groups(s0, gs, ws);
        assert(apply::<W>(rw, pre, h0) + b1.skip(base) + data.skip(end) =~= groups_bytes::<W::R>(a));
        assert(values_of(s0, gs, ws) + slab_values::<W::R>(data).skip(at) =~= groups_values(a, <W::R as RleOps<'a>>::null()));
    }
}

/// A run postfix: appended, then flushed.
#[verifier::spinoff_prover]
proof fn lemma_joined_run<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, s1: State<'a, W>, b1: Seq<u8>, b2: Seq<u8>, rw1: Option<RewriteHeader>, rw2: Option<RewriteHeader>, count: usize, v: VOf<'a, W>, seg1: int, seg2: int)
    requires
        step(s0, b0, s1, b1, rw1, copies(count as nat, v), seg1), rw_ok(s0, s1, rw1),
        count >= 1 ==> last_is(s1, v),
        count >= 2 && !<W::R as RleOps<'a>>::is_null(v) ==> s1 is Run,
        flushed(s1, b1, b2, rw2, seg2), rw2 is Some ==> pre_lit(s1),
    ensures joined(s0, b0, b2, comb(rw1, rw2), Some(Postfix::Run { count, value: v }), seg1 + seg2),
{
    hide(canonical);
    let p = Some(Postfix::Run { count, value: v });
    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64, data: Seq<u8>, at: int, end: int|
        #[trigger] rel(s0, pre, b0, base, gs, ws, h0) && #[trigger] postfix_ok::<W::R>(data, at, p, end)
        implies 0 <= base <= b2.len()
            && encodes::<W::R>(apply::<W>(comb(rw1, rw2), pre, h0) + b2.skip(base) + data.skip(end),
                values_of(s0, gs, ws) + slab_values::<W::R>(data).skip(at),
                acc(s0, gs, ws) + (seg1 + seg2) + rest_segs::<W::R>(data, at, p, end)) by {
        let i = choose|i: int| tail_run::<W::R>(data, at, count as nat, v, end, i);
        let pre1 = apply::<W>(rw1, pre, h0);
        let (gs1, ws1) = choose|gs1: Seq<Group<VOf<'a, W>>>, ws1: Seq<VOf<'a, W>>| #[trigger] rel(s1, pre1, b1, base, gs1, ws1, h0)
            && values_of(s1, gs1, ws1) == values_of(s0, gs, ws) + copies(count as nat, v)
            && acc(s1, gs1, ws1) == acc(s0, gs, ws) + seg1;
        let a = all_groups(s1, gs1, ws1);
        lemma_apply_comb::<W>(rw1, rw2, pre, h0);
        lemma_last_group::<W>(s1, gs1, ws1, v);
        lemma_join_run::<W::R>(a, data, at, count as nat, v, end, i);
        lemma_run_tail_values::<W::R>(data, at, count as nat, v, end, i);
        let rest = groups_values(slab_groups::<W::R>(data).skip(i + 1), <W::R as RleOps<'a>>::null());
        assert(values_of(s0, gs, ws) + slab_values::<W::R>(data).skip(at) =~= groups_values(a, <W::R as RleOps<'a>>::null()) + rest);
    }
}

/// A sequence from one of its items on: that item, then the rest.
proof fn lemma_skip_first<T>(s: Seq<T>, at: int)
    requires 0 <= at < s.len(),
    ensures s.skip(at) == seq![s[at]] + s.skip(at + 1),
{
    assert(s.skip(at) =~= seq![s[at]] + s.skip(at + 1));
}

/// A literal tail's value is the slab's value at its position.
proof fn lemma_lit_tail_at<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, v: O::V, lit: nat, end: int, i: int, j: int)
    requires valid_slab::<O>(data), tail_lit::<O>(data, at, v, lit, end, i, j),
    ensures 0 <= at < slab_values::<O>(data).len(), slab_values::<O>(data)[at] == v,
{
    hide(valid_slab);
    hide(slab_values);
    let gs = slab_groups::<O>(data);
    lemma_slab_group::<O>(data, i);
    assert(group_values(gs[i], O::null())[j] == v);
}

/// The slab value at a literal tail's position.
proof fn lemma_lit_tail_value<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, v: O::V, lit: nat, end: int, i: int, j: int)
    requires valid_slab::<O>(data), tail_lit::<O>(data, at, v, lit, end, i, j),
    ensures slab_values::<O>(data).skip(at) == seq![v] + slab_values::<O>(data).skip(at + 1),
{
    hide(valid_slab);
    hide(tail_lit);
    hide(slab_values);
    lemma_lit_tail_at::<O>(data, at, v, lit, end, i, j);
    lemma_skip_first(slab_values::<O>(data), at);
}

/// A literal postfix: its value appended, then flushed onto the rest of
/// its literal.
#[verifier::spinoff_prover]
proof fn lemma_joined_lit<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, s1: State<'a, W>, b1: Seq<u8>, b2: Seq<u8>, rw1: Option<RewriteHeader>, rw2: Option<RewriteHeader>, v: VOf<'a, W>, lit: usize, seg1: int, seg2: int)
    requires
        step(s0, b0, s1, b1, rw1, copies(1, v), seg1), rw_ok(s0, s1, rw1), last_is(s1, v),
        lit == 0 ==> flushed(s1, b1, b2, rw2, seg2),
        lit > 0 ==> flushed_lit(s1, b1, b2, rw2, lit as nat, seg2),
        rw2 is Some ==> pre_lit(s1), lit > 0 ==> lit_fits(s1, lit as nat),
    ensures joined(s0, b0, b2, comb(rw1, rw2), Some(Postfix::Lit { value: v, lit }), seg1 + seg2),
{
    hide(canonical);
    let p = Some(Postfix::Lit { value: v, lit });
    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64, data: Seq<u8>, at: int, end: int|
        #[trigger] rel(s0, pre, b0, base, gs, ws, h0) && #[trigger] postfix_ok::<W::R>(data, at, p, end)
        implies 0 <= base <= b2.len()
            && encodes::<W::R>(apply::<W>(comb(rw1, rw2), pre, h0) + b2.skip(base) + data.skip(end),
                values_of(s0, gs, ws) + slab_values::<W::R>(data).skip(at),
                acc(s0, gs, ws) + (seg1 + seg2) + rest_segs::<W::R>(data, at, p, end)) by {
        let (i, j) = choose|i: int, j: int| tail_lit::<W::R>(data, at, v, lit as nat, end, i, j);
        let pre1 = apply::<W>(rw1, pre, h0);
        let (gs1, ws1) = choose|gs1: Seq<Group<VOf<'a, W>>>, ws1: Seq<VOf<'a, W>>| #[trigger] rel(s1, pre1, b1, base, gs1, ws1, h0)
            && values_of(s1, gs1, ws1) == values_of(s0, gs, ws) + copies(1, v)
            && acc(s1, gs1, ws1) == acc(s0, gs, ws) + seg1;
        lemma_apply_comb::<W>(rw1, rw2, pre, h0);
        lemma_join_lit::<W>(s1, pre1, b1, base, gs1, ws1, h0, b2, rw2, data, at, v, lit as nat, end, i, j, seg2);
        lemma_lit_tail_value::<W::R>(data, at, v, lit as nat, end, i, j);
        assert(copies(1, v) =~= seq![v]);
        assert(values_of(s0, gs, ws) + slab_values::<W::R>(data).skip(at)
            =~= values_of(s1, gs1, ws1) + slab_values::<W::R>(data).skip(at + 1));
    }
}

/// The slab's values from a repeat run's last value before a literal:
/// that value, the literal's first, and the values after it.
proof fn lemma_lone_tail_values<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, lone: O::V, v: O::V, lit: nat, end: int, i: int)
    requires
        valid_slab::<O>(data),
        tail_lone::<O>(data, at, lone, v, lit, end, i),
    ensures
        slab_values::<O>(data).skip(at) == seq![lone] + seq![v] + slab_values::<O>(data).skip(at + 2),
{
    hide(slab_values);
    let gs = slab_groups::<O>(data);
    let x = slab_values::<O>(data);
    lemma_slab_group::<O>(data, i);
    assert(group_values(gs[i], O::null())[group_len(gs[i]) - 1] == lone);
    assert(x[at] == lone);
    lemma_skip_first(x, at);
    lemma_lit_tail_value::<O>(data, at + 1, v, lit, end, i + 1, 0);
    lemma_cat3(seq![lone], seq![v], x.skip(at + 2));
}

/// [`lemma_joined_lone`] for one view of the state it started from, and
/// one slab the postfix is the first run of, its lone value closing
/// group `i`.
proof fn lemma_joined_lone_at<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, s1: State<'a, W>, b1: Seq<u8>, s2: State<'a, W>, b2: Seq<u8>, b3: Seq<u8>, rw1: Option<RewriteHeader>, rw2: Option<RewriteHeader>, rw3: Option<RewriteHeader>, lone: VOf<'a, W>, v: VOf<'a, W>, lit: usize, seg1: int, seg2: int, seg3: int, pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64, data: Seq<u8>, at: int, end: int, i: int)
    requires
        step(s0, b0, s1, b1, rw1, copies(1, lone), seg1),
        rw_ok(s0, s1, rw1),
        step(s1, b1, s2, b2, rw2, copies(1, v), seg2),
        rw_ok(s1, s2, rw2),
        last_is(s2, v),
        lit == 0 ==> flushed(s2, b2, b3, rw3, seg3),
        lit > 0 ==> flushed_lit(s2, b2, b3, rw3, lit as nat, seg3),
        rw3 is Some ==> pre_lit(s2),
        lit > 0 ==> lit_fits(s2, lit as nat),
        rel(s0, pre, b0, base, gs, ws, h0),
        valid_slab::<W::R>(data),
        tail_lone::<W::R>(data, at, lone, v, lit as nat, end, i),
    ensures
        0 <= base <= b3.len(),
        encodes::<W::R>(apply::<W>(comb(comb(rw1, rw2), rw3), pre, h0) + b3.skip(base) + data.skip(end),
            values_of(s0, gs, ws) + slab_values::<W::R>(data).skip(at),
            acc(s0, gs, ws) + (seg1 + seg2 + seg3) + lit + groups_segments(slab_groups::<W::R>(data).skip(i + 2))),
{
    // the lemmas called read these; nothing here needs their bodies
    hide(canonical);
    hide(rel);
    hide(flushed);
    hide(flushed_lit);
    hide(tail_lit);
    hide(encodes);
    hide(valid_slab);
    hide(slab_values);
    hide(values_of);
    hide(acc);
    hide(groups_segments);
    let pre1 = apply::<W>(rw1, pre, h0);
    let (gs1, ws1) = choose|gs1: Seq<Group<VOf<'a, W>>>, ws1: Seq<VOf<'a, W>>| #[trigger] rel(s1, pre1, b1, base, gs1, ws1, h0)
        && values_of(s1, gs1, ws1) == values_of(s0, gs, ws) + copies(1, lone)
        && acc(s1, gs1, ws1) == acc(s0, gs, ws) + seg1;
    let pre2 = apply::<W>(rw2, pre1, h0);
    let (gs2, ws2) = choose|gs2: Seq<Group<VOf<'a, W>>>, ws2: Seq<VOf<'a, W>>| #[trigger] rel(s2, pre2, b2, base, gs2, ws2, h0)
        && values_of(s2, gs2, ws2) == values_of(s1, gs1, ws1) + copies(1, v)
        && acc(s2, gs2, ws2) == acc(s1, gs1, ws1) + seg2;
    lemma_apply_comb::<W>(rw1, rw2, pre, h0);
    lemma_apply_comb::<W>(comb(rw1, rw2), rw3, pre, h0);
    lemma_join_lit::<W>(s2, pre2, b2, base, gs2, ws2, h0, b3, rw3, data, at + 1, v, lit as nat, end, i + 1, 0, seg3);
    lemma_lone_tail_values::<W::R>(data, at, lone, v, lit as nat, end, i);
    let x = values_of(s0, gs, ws);
    let y = slab_values::<W::R>(data).skip(at + 2);
    assert(copies(1, v) =~= seq![v]);
    assert(copies(1, lone) =~= seq![lone]);
    assert(x + (seq![lone] + seq![v] + y) =~= x + seq![lone] + seq![v] + y);
}

/// A repeat run's last value then a literal's first: both appended, then
/// flushed onto the rest of the literal.
#[verifier::spinoff_prover]
proof fn lemma_joined_lone<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, s1: State<'a, W>, b1: Seq<u8>, s2: State<'a, W>, b2: Seq<u8>, b3: Seq<u8>, rw1: Option<RewriteHeader>, rw2: Option<RewriteHeader>, rw3: Option<RewriteHeader>, lone: VOf<'a, W>, v: VOf<'a, W>, lit: usize, seg1: int, seg2: int, seg3: int)
    requires
        step(s0, b0, s1, b1, rw1, copies(1, lone), seg1), rw_ok(s0, s1, rw1),
        step(s1, b1, s2, b2, rw2, copies(1, v), seg2), rw_ok(s1, s2, rw2), last_is(s2, v),
        lit == 0 ==> flushed(s2, b2, b3, rw3, seg3),
        lit > 0 ==> flushed_lit(s2, b2, b3, rw3, lit as nat, seg3),
        rw3 is Some ==> pre_lit(s2), lit > 0 ==> lit_fits(s2, lit as nat),
    ensures joined(s0, b0, b3, comb(comb(rw1, rw2), rw3), Some(Postfix::LonePlusLit { lone, value: v, lit }), seg1 + seg2 + seg3),
{
    hide(canonical);
    hide(rel);
    hide(step);
    hide(flushed);
    hide(flushed_lit);
    hide(tail_lone);
    hide(encodes);
    hide(rest_segs);
    let p = Some(Postfix::LonePlusLit { lone, value: v, lit });
    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64, data: Seq<u8>, at: int, end: int|
        #[trigger] rel(s0, pre, b0, base, gs, ws, h0) && #[trigger] postfix_ok::<W::R>(data, at, p, end)
        implies 0 <= base <= b3.len()
            && encodes::<W::R>(apply::<W>(comb(comb(rw1, rw2), rw3), pre, h0) + b3.skip(base) + data.skip(end),
                values_of(s0, gs, ws) + slab_values::<W::R>(data).skip(at),
                acc(s0, gs, ws) + (seg1 + seg2 + seg3) + rest_segs::<W::R>(data, at, p, end)) by {
        let i = choose|i: int| tail_lone::<W::R>(data, at, lone, v, lit as nat, end, i);
        lemma_rest_lone::<W::R>(data, at, lone, v, lit, end, i);
        lemma_joined_lone_at::<W>(s0, b0, s1, b1, s2, b2, b3, rw1, rw2, rw3, lone, v, lit, seg1, seg2, seg3, pre, base, gs, ws, h0, data, at, end, i);
    }
}

impl<'a, W: WriteOps<'a>> State<'a, W> {
    /// Feed the postfix in, then flush: the bytes written, then the
    /// slab's after the postfix, encode the values written then the
    /// slab's from the edit point.
    pub(crate) fn flush_postfix(&mut self, buf: &mut Vec<u8>, postfix: Option<Postfix<W::Cow>>) -> (f: FlushState)
        requires
            wf(*old(self), old(buf)@), nulls_ok::<W>(postfix),
            old(self).plen() + plen_of(postfix) + 2 < usize::MAX,
        ensures
            (*final(self)) is Empty,
            rw_from(*old(self), f.rewrite),
            joined(*old(self), old(buf)@, final(buf)@, f.rewrite, pmap::<W>(postfix), f.segments as int),
            f.segments <= old(self).pseg() + pcount(postfix),
            joined_tail(*old(self), old(buf)@, final(buf)@, f.rewrite, pmap::<W>(postfix), f.wpos),
    {
        let ghost s0 = *self;
        let ghost b0 = buf@;
        match postfix {
            None => {
                let f = self.flush(buf);
                proof {
                    lemma_joined_none::<W>(s0, b0, buf@, f.rewrite, f.segments as int);
                    lemma_joined_none_tail::<W>(s0, b0, buf@, f.rewrite, f.segments as int, f.wpos);
                }
                f
            },
            Some(Postfix::Run { count, value }) => {
                let ghost v = W::val(value);
                let mut f = self.append_cow(buf, value, count);
                let ghost s1 = *self;
                let ghost b1 = buf@;
                let ghost rw1 = f.rewrite;
                let ghost seg1 = f.segments as int;
                let f2 = self.flush(buf);
                add_flush(&mut f, f2);
                proof {
                    lemma_joined_run::<W>(s0, b0, s1, b1, buf@, rw1, f2.rewrite, count, v, seg1, f2.segments as int);
                    lemma_joined_run_tail::<W>(s0, b0, s1, b1, buf@, rw1, f2.rewrite, count, v, seg1, f2.segments as int, f2.wpos);
                }
                f
            },
            Some(Postfix::Lit { value, lit }) => {
                let ghost v = W::val(value);
                let mut f = self.append_cow(buf, value, 1);
                let ghost s1 = *self;
                let ghost b1 = buf@;
                let ghost rw1 = f.rewrite;
                let ghost seg1 = f.segments as int;
                let f2 = self.flush_with_lit(buf, lit);
                add_flush(&mut f, f2);
                proof {
                    lemma_joined_lit::<W>(s0, b0, s1, b1, buf@, rw1, f2.rewrite, v, lit, seg1, f2.segments as int);
                    lemma_joined_lit_tail::<W>(s0, b0, s1, b1, buf@, rw1, f2.rewrite, v, lit, seg1, f2.segments as int, f2.wpos);
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
                let ghost seg1 = f.segments as int;
                let f1 = self.append_cow(buf, value, 1);
                let ghost s2 = *self;
                let ghost b2 = buf@;
                add_flush(&mut f, f1);
                let f2 = self.flush_with_lit(buf, lit);
                add_flush(&mut f, f2);
                proof {
                    lemma_joined_lone::<W>(s0, b0, s1, b1, s2, b2, buf@, rw1, f1.rewrite, f2.rewrite, l, v, lit, seg1, f1.segments as int, f2.segments as int);
                    lemma_joined_lone_tail::<W>(s0, b0, s1, b1, s2, b2, buf@, rw1, f1.rewrite, f2.rewrite, l, v, lit, seg1, f1.segments as int, f2.segments as int, f2.wpos);
                }
                f
            },
        }
    }
}

/// A postfix holds no more values than the slab has from its position.
pub(crate) proof fn lemma_plen_bound<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, p: Option<Postfix<O::V>>, end: int)
    requires postfix_ok::<O>(data, at, p, end),
    ensures at + plen_of(p) <= slab_values::<O>(data).len(), 0 <= at,
{
    lemma_slab::<O>(data);
    match p {
        None => {},
        Some(Postfix::Run { count, value }) => {
            let i = choose|i: int| tail_run::<O>(data, at, count as nat, value, end, i);
            lemma_slab_after::<O>(data, i);
            lemma_group_values_len(slab_groups::<O>(data)[i], O::null());
        },
        Some(Postfix::Lit { value, lit }) => {
            let (i, j) = choose|i: int, j: int| tail_lit::<O>(data, at, value, lit as nat, end, i, j);
            lemma_slab_after::<O>(data, i);
        },
        Some(Postfix::LonePlusLit { lone, value, lit }) => {
            let i = choose|i: int| tail_lone::<O>(data, at, lone, value, lit as nat, end, i);
            lemma_slab_after::<O>(data, i);
            lemma_slab_after::<O>(data, i + 1);
            lemma_group_values_len(slab_groups::<O>(data)[i], O::null());
        },
    }
}

/// A null among canonical groups' values means the column is nullable.
proof fn lemma_groups_null<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, k: int)
    requires
        forall|i: int| 0 <= i < gs.len() ==> group_ok::<O>(#[trigger] gs[i]),
        0 <= k < groups_values(gs, O::null()).len(), O::is_null(groups_values(gs, O::null())[k]),
    ensures O::nullable(),
    decreases gs.len(),
{
    let a = groups_values(gs.drop_last(), O::null());
    let g = gs.last();
    lemma_groups_values_concat(gs.drop_last(), Seq::empty(), O::null());
    assert(gs.drop_last() + Seq::<Group<O::V>>::empty() =~= gs.drop_last());
    assert(group_ok::<O>(gs[gs.len() - 1]));
    if k < a.len() {
        assert forall|i: int| 0 <= i < gs.drop_last().len() implies group_ok::<O>(#[trigger] gs.drop_last()[i]) by {
            assert(gs.drop_last()[i] == gs[i]);
        }
        lemma_groups_null::<O>(gs.drop_last(), k);
    } else {
        let v = groups_values(gs, O::null())[k];
        assert(v == group_values(g, O::null())[k - a.len()]);
    }
}

/// A null among a valid slab's values means the column is nullable.
pub(crate) proof fn lemma_slab_null<'a, O: RleOps<'a>>(data: Seq<u8>, k: int)
    requires valid_slab::<O>(data), 0 <= k < slab_values::<O>(data).len(), O::is_null(slab_values::<O>(data)[k]),
    ensures O::nullable(),
{
    lemma_slab::<O>(data);
    lemma_groups_null::<O>(slab_groups::<O>(data), k);
}

/// A writer's postfix, by its values.
pub(crate) proof fn lemma_pmap<'a, W: WriteOps<'a>>(p: Option<Postfix<W::Cow>>)
    ensures
        nulls_ok::<W>(p) == vnulls_ok::<W::R>(pmap::<W>(p)), plen_of(pmap::<W>(p)) == plen_of(p),
        pcount(pmap::<W>(p)) == pcount(p), pcount(p) <= plen_of(p),
{
}

// ── The rest of the slab after a postfix ───────────────────────────────────

/// A run tail names one group.
proof fn lemma_tail_run_unique<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, count: nat, v: O::V, end: int, i1: int, i2: int)
    requires valid_slab::<O>(data), tail_run::<O>(data, at, count, v, end, i1), tail_run::<O>(data, at, count, v, end, i2),
    ensures i1 == i2,
{
    if i1 < i2 {
        lemma_gb_lt::<O>(data, i1 + 1, i2 + 1);
    } else if i2 < i1 {
        lemma_gb_lt::<O>(data, i2 + 1, i1 + 1);
    }
}

/// The group whose values `at` lies among.
proof fn lemma_in_group_unique<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, i1: int, i2: int)
    requires
        valid_slab::<O>(data),
        0 <= i1 < slab_groups::<O>(data).len(), 0 <= i2 < slab_groups::<O>(data).len(),
        gl(slab_groups::<O>(data), i1) <= at < gl(slab_groups::<O>(data), i1 + 1),
        gl(slab_groups::<O>(data), i2) <= at < gl(slab_groups::<O>(data), i2 + 1),
    ensures i1 == i2,
{
    if i1 < i2 {
        lemma_gl_le::<O>(data, i1 + 1, i2);
    } else if i2 < i1 {
        lemma_gl_le::<O>(data, i2 + 1, i1);
    }
}

/// A literal tail names one group and value.
proof fn lemma_tail_lit_unique<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, v: O::V, lit: nat, end: int, i1: int, j1: int, i2: int, j2: int)
    requires valid_slab::<O>(data), tail_lit::<O>(data, at, v, lit, end, i1, j1), tail_lit::<O>(data, at, v, lit, end, i2, j2),
    ensures i1 == i2 && j1 == j2,
{
    lemma_slab_group::<O>(data, i1);
    lemma_slab_group::<O>(data, i2);
    lemma_in_group_unique::<O>(data, at, i1, i2);
}

/// A lone value before a literal names one group.
proof fn lemma_tail_lone_unique<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, lone: O::V, v: O::V, lit: nat, end: int, i1: int, i2: int)
    requires valid_slab::<O>(data), tail_lone::<O>(data, at, lone, v, lit, end, i1), tail_lone::<O>(data, at, lone, v, lit, end, i2),
    ensures i1 == i2,
{
    lemma_slab_group::<O>(data, i1);
    lemma_slab_group::<O>(data, i2);
    lemma_slab_group::<O>(data, i1 + 1);
    lemma_slab_group::<O>(data, i2 + 1);
    lemma_in_group_unique::<O>(data, at + 1, i1 + 1, i2 + 1);
}

/// The slab's segments after a run tail at group `i`.
pub(crate) proof fn lemma_rest_run<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, count: usize, v: O::V, end: int, i: int)
    requires valid_slab::<O>(data), tail_run::<O>(data, at, count as nat, v, end, i),
    ensures rest_segs::<O>(data, at, Some(Postfix::Run { count, value: v }), end) == groups_segments(slab_groups::<O>(data).skip(i + 1)),
{
    let k = choose|k: int| tail_run::<O>(data, at, count as nat, v, end, k);
    lemma_tail_run_unique::<O>(data, at, count as nat, v, end, i, k);
}

/// The slab's segments after a literal tail at value `j` of group `i`.
pub(crate) proof fn lemma_rest_lit<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, v: O::V, lit: usize, end: int, i: int, j: int)
    requires valid_slab::<O>(data), tail_lit::<O>(data, at, v, lit as nat, end, i, j),
    ensures rest_segs::<O>(data, at, Some(Postfix::Lit { value: v, lit }), end) == lit + groups_segments(slab_groups::<O>(data).skip(i + 1)),
{
    let (k, l) = choose|k: int, l: int| tail_lit::<O>(data, at, v, lit as nat, end, k, l);
    lemma_tail_lit_unique::<O>(data, at, v, lit as nat, end, i, j, k, l);
}

/// The slab's segments after a lone value and a literal's first.
pub(crate) proof fn lemma_rest_lone<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, lone: O::V, v: O::V, lit: usize, end: int, i: int)
    requires valid_slab::<O>(data), tail_lone::<O>(data, at, lone, v, lit as nat, end, i),
    ensures rest_segs::<O>(data, at, Some(Postfix::LonePlusLit { lone, value: v, lit }), end)
        == lit + groups_segments(slab_groups::<O>(data).skip(i + 2)),
{
    let k = choose|k: int| tail_lone::<O>(data, at, lone, v, lit as nat, end, k);
    lemma_tail_lone_unique::<O>(data, at, lone, v, lit as nat, end, i, k);
}

/// A slab's segments: those of its first `k` groups, and the rest's.
pub(crate) proof fn lemma_gseg_split<'a, O: RleOps<'a>>(data: Seq<u8>, k: int)
    requires valid_slab::<O>(data), 0 <= k <= slab_groups::<O>(data).len(),
    ensures
        gseg(slab_groups::<O>(data), k) + groups_segments(slab_groups::<O>(data).skip(k))
            == groups_segments(slab_groups::<O>(data)),
        groups_segments(slab_groups::<O>(data).skip(k)) <= groups_len(slab_groups::<O>(data).skip(k)),
        groups_len(slab_groups::<O>(data).skip(k)) == groups_len(slab_groups::<O>(data)) - gl(slab_groups::<O>(data), k),
{
    let gs = slab_groups::<O>(data);
    assert(gs =~= gs.take(k) + gs.skip(k));
    lemma_groups_len_concat(gs.take(k), gs.skip(k));
    lemma_slab::<O>(data);
    let rest = gs.skip(k);
    assert forall|x: int| 0 <= x < rest.len() implies group_ok::<O>(#[trigger] rest[x]) by {
        assert(rest[x] == gs[x + k]);
    }
    assert forall|x: int| 0 < x < rest.len() implies adjacent_ok(rest[x - 1], #[trigger] rest[x]) by {
        assert(rest[x] == gs[x + k] && rest[x - 1] == gs[x - 1 + k]);
    }
    super::read::lemma_segments_le::<O>(rest);
}

/// A postfix's values and the segments after it fit in the values from
/// its position.
#[verifier::spinoff_prover]
pub(crate) proof fn lemma_rest_bound<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, p: Option<Postfix<O::V>>, end: int)
    requires postfix_ok::<O>(data, at, p, end),
    ensures pcount(p) + rest_segs::<O>(data, at, p, end) <= slab_values::<O>(data).len() - at, 0 <= rest_segs::<O>(data, at, p, end),
{
    hide(canonical);
    let gs = slab_groups::<O>(data);
    lemma_slab::<O>(data);
    match p {
        None => {},
        Some(Postfix::Run { count, value }) => {
            let i = choose|i: int| tail_run::<O>(data, at, count as nat, value, end, i);
            lemma_rest_run::<O>(data, at, count, value, end, i);
            lemma_gseg_split::<O>(data, i + 1);
        },
        Some(Postfix::Lit { value, lit }) => {
            let (i, j) = choose|i: int, j: int| tail_lit::<O>(data, at, value, lit as nat, end, i, j);
            lemma_rest_lit::<O>(data, at, value, lit, end, i, j);
            lemma_gseg_split::<O>(data, i + 1);
            lemma_slab_group::<O>(data, i);
        },
        Some(Postfix::LonePlusLit { lone, value, lit }) => {
            let i = choose|i: int| tail_lone::<O>(data, at, lone, value, lit as nat, end, i);
            lemma_rest_lone::<O>(data, at, lone, value, lit, end, i);
            lemma_gseg_split::<O>(data, i + 2);
            lemma_slab_group::<O>(data, i + 1);
        },
    }
}

/// The slab's last group, when groups follow position `k`: theirs.
proof fn lemma_tail_rest<'a, O: RleOps<'a>>(data: Seq<u8>, k: int)
    requires valid_slab::<O>(data), 0 <= k <= slab_groups::<O>(data).len(),
    ensures ({
        let gs = slab_groups::<O>(data);
        let rest = gs.skip(k);
        &&& rest.len() > 0 ==> tail_bytes_of::<O>(gs) == tail_bytes_of::<O>(rest)
            && lit_tail_of::<O>(gs) == lit_tail_of::<O>(rest)
            && tail_bytes_of::<O>(rest) <= groups_bytes::<O>(rest).len()
        &&& rest.len() == 0 ==> k == gs.len()
    }),
{
    let gs = slab_groups::<O>(data);
    let rest = gs.skip(k);
    if rest.len() > 0 {
        assert(rest.last() == gs.last());
        assert(groups_bytes::<O>(rest) == groups_bytes::<O>(rest.drop_last()) + group_bytes::<O>(rest.last()));
    }
}

/// No postfix: the slab's tail is the writer's last group's.
proof fn lemma_joined_none_tail<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, b1: Seq<u8>, rw: Option<RewriteHeader>, seg: int, w: WPos)
    requires
        flushed(s0, b0, b1, rw, seg), flushed_tail(s0, b0, b1, rw, w),
        s0 is Empty ==> b1 == b0 && w == (WPos::Run { pos: 0, local: true }),
    ensures joined_tail(s0, b0, b1, rw, None, w),
{
    hide(canonical);
    assert forall|pre: Seq<u8>, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64, data: Seq<u8>, at: int, end: int|
        #[trigger] rel(s0, pre, b0, 0, gs, ws, h0) && #[trigger] postfix_ok::<W::R>(data, at, None, end)
        implies ({
            let t = tail_after(w, apply::<W>(rw, pre, h0).len() as int, 0, b1.len() as int, data.skip(end).len() as int,
                tail_bytes_of::<W::R>(slab_groups::<W::R>(data)) as int, lit_tail_of::<W::R>(slab_groups::<W::R>(data)) as int);
            tail_ok::<W::R>(apply::<W>(rw, pre, h0) + b1.skip(0) + data.skip(end), t.0, t.1)
        }) by {
        let a = all_groups(s0, gs, ws);
        let out = apply::<W>(rw, pre, h0) + b1.skip(0) + data.skip(end);
        assert(data.skip(end) =~= Seq::<u8>::empty());
        assert(out =~= groups_bytes::<W::R>(a));
        assert(b1.skip(0) =~= b1);
        if !(s0 is Empty) {
            lemma_push::<W::R>(gs, pend_group(s0, ws)->Some_0);
        } else {
            assert(gs.len() == 0);
            assert(groups_bytes::<W::R>(gs) =~= Seq::<u8>::empty());
        }
        assert(canonical::<W::R>(a));
    }
}

/// A run postfix: the remainder's last group, else the writer's.
#[verifier::spinoff_prover]
proof fn lemma_joined_run_tail<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, s1: State<'a, W>, b1: Seq<u8>, b2: Seq<u8>, rw1: Option<RewriteHeader>, rw2: Option<RewriteHeader>, count: usize, v: VOf<'a, W>, seg1: int, seg2: int, w: WPos)
    requires
        step(s0, b0, s1, b1, rw1, copies(count as nat, v), seg1), rw_ok(s0, s1, rw1),
        count >= 1 ==> last_is(s1, v),
        count >= 2 && !<W::R as RleOps<'a>>::is_null(v) ==> s1 is Run,
        flushed(s1, b1, b2, rw2, seg2), rw2 is Some ==> pre_lit(s1), flushed_tail(s1, b1, b2, rw2, w),
    ensures joined_tail(s0, b0, b2, comb(rw1, rw2), Some(Postfix::Run { count, value: v }), w),
{
    hide(canonical);
    let p = Some(Postfix::Run { count, value: v });
    assert forall|pre: Seq<u8>, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64, data: Seq<u8>, at: int, end: int|
        #[trigger] rel(s0, pre, b0, 0, gs, ws, h0) && #[trigger] postfix_ok::<W::R>(data, at, p, end)
        implies ({
            let t = tail_after(w, apply::<W>(comb(rw1, rw2), pre, h0).len() as int, 0, b2.len() as int, data.skip(end).len() as int,
                tail_bytes_of::<W::R>(slab_groups::<W::R>(data)) as int, lit_tail_of::<W::R>(slab_groups::<W::R>(data)) as int);
            tail_ok::<W::R>(apply::<W>(comb(rw1, rw2), pre, h0) + b2.skip(0) + data.skip(end), t.0, t.1)
        }) by {
        let i = choose|i: int| tail_run::<W::R>(data, at, count as nat, v, end, i);
        let pre1 = apply::<W>(rw1, pre, h0);
        let (gs1, ws1) = choose|gs1: Seq<Group<VOf<'a, W>>>, ws1: Seq<VOf<'a, W>>| #[trigger] rel(s1, pre1, b1, 0, gs1, ws1, h0)
            && values_of(s1, gs1, ws1) == values_of(s0, gs, ws) + copies(count as nat, v)
            && acc(s1, gs1, ws1) == acc(s0, gs, ws) + seg1;
        let a = all_groups(s1, gs1, ws1);
        let dg = slab_groups::<W::R>(data);
        lemma_apply_comb::<W>(rw1, rw2, pre, h0);
        lemma_last_group::<W>(s1, gs1, ws1, v);
        lemma_join_run::<W::R>(a, data, at, count as nat, v, end, i);
        lemma_slab_after::<W::R>(data, i);
        lemma_tail_rest::<W::R>(data, i + 1);
        lemma_tail_concat::<W::R>(a, dg.skip(i + 1));
        lemma_push::<W::R>(gs1, pend_group(s1, ws1)->Some_0);
        assert(b2.skip(0) =~= b2);
        assert(apply::<W>(rw2, pre1, h0) + b2.skip(0) == groups_bytes::<W::R>(a));
    }
}

/// A literal's values after value `j`, where there are some: their bytes
/// are some of the literal's, and not all of them.
proof fn lemma_lit_rest_bytes<'a, O: RleOps<'a>>(vs: Seq<O::V>, j: int)
    requires
        0 <= j,
        j + 1 < vs.len(),
    ensures
        0 < encs::<O>(vs.skip(j + 1)).len() < group_bytes::<O>(Group::Lit { values: vs }).len(),
{
    let l = vs.skip(j + 1);
    assert(vs =~= vs.take(j + 1) + l);
    lemma_encs_concat::<O>(vs.take(j + 1), l);
    O::axiom_enc_signed(-(vs.len() as int) as i64, 0, Seq::empty(), Seq::empty());
    O::axiom_enc(l.last(), l.last(), Seq::empty(), Seq::empty());
    assert(encs::<O>(l) == encs::<O>(l.drop_last()) + O::enc(l.last()));
}

/// A slab's tail record, against its bytes after a literal tail: where
/// groups follow the literal, those bytes hold the last group whole;
/// where none do, the literal is the last group, with no bytes after the
/// tail if it ends there, and else some, but fewer than the literal's.
proof fn lemma_lit_tail_old<'a, O: RleOps<'a>>(data: Seq<u8>, at: int, v: O::V, lit: nat, end: int, i: int, j: int)
    requires
        valid_slab::<O>(data),
        tail_lit::<O>(data, at, v, lit, end, i, j),
    ensures ({
        let gs = slab_groups::<O>(data);
        let rest = gs.skip(i + 1);
        let q = data.skip(end).len();
        let tb = tail_bytes_of::<O>(gs);
        let tl = lit_tail_of::<O>(gs);
        &&& rest.len() > 0 ==> 0 < q && tb <= q && tb == tail_bytes_of::<O>(rest) && tl == lit_tail_of::<O>(rest)
        &&& rest.len() == 0 && lit == 0 ==> q == 0
        &&& rest.len() == 0 && lit > 0 ==> 0 < q < tb && tl == O::enc(gs[i]->values.last()).len()
    }),
{
    hide(canonical);
    hide(group_ok);
    let gs = slab_groups::<O>(data);
    let vs = gs[i]->values;
    let l = vs.skip(j + 1);
    let rest = gs.skip(i + 1);
    lemma_lit_tail_facts::<O>(data, at, v, lit, end, i, j);
    lemma_tail_rest::<O>(data, i + 1);
    if rest.len() > 0 {
        lemma_group_bytes_nonempty::<O>(rest.last());
    } else {
        assert(groups_bytes::<O>(rest) =~= Seq::<u8>::empty());
        if lit == 0 {
            assert(l =~= Seq::<O::V>::empty());
            assert(encs::<O>(l) =~= Seq::<u8>::empty());
        } else {
            // the slab's last group is the literal the postfix ends inside
            assert(gs.last() == gs[i]);
            assert(gs[i] == Group::Lit { values: vs });
            lemma_lit_rest_bytes::<O>(vs, j);
        }
    }
}

/// What flushing a state ending in `v` left `w` naming, as a tail
/// record: the last of the groups written.
proof fn lemma_flushed_last<'a, W: WriteOps<'a>>(s1: State<'a, W>, pre1: Seq<u8>, b1: Seq<u8>, gs1: Seq<Group<VOf<'a, W>>>, ws1: Seq<VOf<'a, W>>, h0: i64, b2: Seq<u8>, rw2: Option<RewriteHeader>, v: VOf<'a, W>, seg: int, w: WPos)
    requires
        rel(s1, pre1, b1, 0, gs1, ws1, h0),
        last_is(s1, v),
        flushed(s1, b1, b2, rw2, seg),
        flushed_tail(s1, b1, b2, rw2, w),
    ensures ({
        let a = all_groups(s1, gs1, ws1);
        let plen = apply::<W>(rw2, pre1, h0).len() as int;
        &&& plen + b2.len() - wstart(w, plen, 0) == tail_bytes_of::<W::R>(a)
        &&& lit_tail_of::<W::R>(a) == match w {
            WPos::Lit { bytes, .. } => bytes as int,
            _ => 0int,
        }
    }),
{
    hide(canonical);
    hide(rel);
    hide(groups_bytes);
    let a = all_groups(s1, gs1, ws1);
    let out = apply::<W>(rw2, pre1, h0) + b2.skip(0);
    lemma_last_group::<W>(s1, gs1, ws1, v);
    lemma_push::<W::R>(gs1, pend_group(s1, ws1)->Some_0);
    assert(out.len() == groups_bytes::<W::R>(gs1).len() + group_bytes::<W::R>(a.last()).len());
}

/// What a literal-tail flush leaves for the merge to read: the
/// remainder's last group, else the writer's, else the literal joined.
#[verifier::spinoff_prover]
proof fn lemma_lit_tail_after<'a, W: WriteOps<'a>>(s1: State<'a, W>, pre1: Seq<u8>, b1: Seq<u8>, gs1: Seq<Group<VOf<'a, W>>>, ws1: Seq<VOf<'a, W>>, h0: i64, b2: Seq<u8>, rw2: Option<RewriteHeader>, data: Seq<u8>, at: int, v: VOf<'a, W>, lit: nat, end: int, i: int, j: int, seg: int, w: WPos)
    requires
        rel(s1, pre1, b1, 0, gs1, ws1, h0), last_is(s1, v),
        lit == 0 ==> flushed(s1, b1, b2, rw2, seg) && flushed_tail(s1, b1, b2, rw2, w),
        lit > 0 ==> flushed_lit(s1, b1, b2, rw2, lit, seg) && flushed_lit_tail(s1, b1, b2, rw2, w),
        lit > 0 ==> lit_fits(s1, lit),
        valid_slab::<W::R>(data), tail_lit::<W::R>(data, at, v, lit, end, i, j),
    ensures ({
        let t = tail_after(w, apply::<W>(rw2, pre1, h0).len() as int, 0, b2.len() as int, data.skip(end).len() as int,
            tail_bytes_of::<W::R>(slab_groups::<W::R>(data)) as int, lit_tail_of::<W::R>(slab_groups::<W::R>(data)) as int);
        tail_ok::<W::R>(apply::<W>(rw2, pre1, h0) + b2.skip(0) + data.skip(end), t.0, t.1)
    }),
{
    hide(canonical);
    hide(rel);
    hide(flushed);
    hide(flushed_lit);
    hide(flushed_tail);
    hide(tail_lit);
    hide(encodes);
    hide(tail_ok);
    let rest = slab_groups::<W::R>(data).skip(i + 1);
    lemma_join_lit::<W>(s1, pre1, b1, 0, gs1, ws1, h0, b2, rw2, data, at, v, lit, end, i, j, seg);
    lemma_lit_tail_old::<W::R>(data, at, v, lit, end, i, j);
    if rest.len() == 0 && lit == 0 {
        lemma_flushed_last::<W>(s1, pre1, b1, gs1, ws1, h0, b2, rw2, v, seg, w);
    }
}

/// A literal postfix: the tail the merge reads.
#[verifier::spinoff_prover]
proof fn lemma_joined_lit_tail<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, s1: State<'a, W>, b1: Seq<u8>, b2: Seq<u8>, rw1: Option<RewriteHeader>, rw2: Option<RewriteHeader>, v: VOf<'a, W>, lit: usize, seg1: int, seg2: int, w: WPos)
    requires
        step(s0, b0, s1, b1, rw1, copies(1, v), seg1), rw_ok(s0, s1, rw1), last_is(s1, v),
        lit == 0 ==> flushed(s1, b1, b2, rw2, seg2) && flushed_tail(s1, b1, b2, rw2, w),
        lit > 0 ==> flushed_lit(s1, b1, b2, rw2, lit as nat, seg2) && flushed_lit_tail(s1, b1, b2, rw2, w),
        rw2 is Some ==> pre_lit(s1), lit > 0 ==> lit_fits(s1, lit as nat),
    ensures joined_tail(s0, b0, b2, comb(rw1, rw2), Some(Postfix::Lit { value: v, lit }), w),
{
    hide(canonical);
    let p = Some(Postfix::Lit { value: v, lit });
    assert forall|pre: Seq<u8>, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64, data: Seq<u8>, at: int, end: int|
        #[trigger] rel(s0, pre, b0, 0, gs, ws, h0) && #[trigger] postfix_ok::<W::R>(data, at, p, end)
        implies ({
            let t = tail_after(w, apply::<W>(comb(rw1, rw2), pre, h0).len() as int, 0, b2.len() as int, data.skip(end).len() as int,
                tail_bytes_of::<W::R>(slab_groups::<W::R>(data)) as int, lit_tail_of::<W::R>(slab_groups::<W::R>(data)) as int);
            tail_ok::<W::R>(apply::<W>(comb(rw1, rw2), pre, h0) + b2.skip(0) + data.skip(end), t.0, t.1)
        }) by {
        let (i, j) = choose|i: int, j: int| tail_lit::<W::R>(data, at, v, lit as nat, end, i, j);
        let pre1 = apply::<W>(rw1, pre, h0);
        let (gs1, ws1) = choose|gs1: Seq<Group<VOf<'a, W>>>, ws1: Seq<VOf<'a, W>>| #[trigger] rel(s1, pre1, b1, 0, gs1, ws1, h0)
            && values_of(s1, gs1, ws1) == values_of(s0, gs, ws) + copies(1, v)
            && acc(s1, gs1, ws1) == acc(s0, gs, ws) + seg1;
        lemma_apply_comb::<W>(rw1, rw2, pre, h0);
        lemma_lit_tail_after::<W>(s1, pre1, b1, gs1, ws1, h0, b2, rw2, data, at, v, lit as nat, end, i, j, seg2, w);
    }
}

/// A lone value then a literal: the tail the merge reads.
#[verifier::spinoff_prover]
proof fn lemma_joined_lone_tail<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, s1: State<'a, W>, b1: Seq<u8>, s2: State<'a, W>, b2: Seq<u8>, b3: Seq<u8>, rw1: Option<RewriteHeader>, rw2: Option<RewriteHeader>, rw3: Option<RewriteHeader>, lone: VOf<'a, W>, v: VOf<'a, W>, lit: usize, seg1: int, seg2: int, seg3: int, w: WPos)
    requires
        step(s0, b0, s1, b1, rw1, copies(1, lone), seg1), rw_ok(s0, s1, rw1),
        step(s1, b1, s2, b2, rw2, copies(1, v), seg2), rw_ok(s1, s2, rw2), last_is(s2, v),
        lit == 0 ==> flushed(s2, b2, b3, rw3, seg3) && flushed_tail(s2, b2, b3, rw3, w),
        lit > 0 ==> flushed_lit(s2, b2, b3, rw3, lit as nat, seg3) && flushed_lit_tail(s2, b2, b3, rw3, w),
        rw3 is Some ==> pre_lit(s2), lit > 0 ==> lit_fits(s2, lit as nat),
    ensures joined_tail(s0, b0, b3, comb(comb(rw1, rw2), rw3), Some(Postfix::LonePlusLit { lone, value: v, lit }), w),
{
    hide(canonical);
    let p = Some(Postfix::LonePlusLit { lone, value: v, lit });
    assert forall|pre: Seq<u8>, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64, data: Seq<u8>, at: int, end: int|
        #[trigger] rel(s0, pre, b0, 0, gs, ws, h0) && #[trigger] postfix_ok::<W::R>(data, at, p, end)
        implies ({
            let t = tail_after(w, apply::<W>(comb(comb(rw1, rw2), rw3), pre, h0).len() as int, 0, b3.len() as int, data.skip(end).len() as int,
                tail_bytes_of::<W::R>(slab_groups::<W::R>(data)) as int, lit_tail_of::<W::R>(slab_groups::<W::R>(data)) as int);
            tail_ok::<W::R>(apply::<W>(comb(comb(rw1, rw2), rw3), pre, h0) + b3.skip(0) + data.skip(end), t.0, t.1)
        }) by {
        let i = choose|i: int| tail_lone::<W::R>(data, at, lone, v, lit as nat, end, i);
        let pre1 = apply::<W>(rw1, pre, h0);
        let (gs1, ws1) = choose|gs1: Seq<Group<VOf<'a, W>>>, ws1: Seq<VOf<'a, W>>| #[trigger] rel(s1, pre1, b1, 0, gs1, ws1, h0)
            && values_of(s1, gs1, ws1) == values_of(s0, gs, ws) + copies(1, lone)
            && acc(s1, gs1, ws1) == acc(s0, gs, ws) + seg1;
        let pre2 = apply::<W>(rw2, pre1, h0);
        let (gs2, ws2) = choose|gs2: Seq<Group<VOf<'a, W>>>, ws2: Seq<VOf<'a, W>>| #[trigger] rel(s2, pre2, b2, 0, gs2, ws2, h0)
            && values_of(s2, gs2, ws2) == values_of(s1, gs1, ws1) + copies(1, v)
            && acc(s2, gs2, ws2) == acc(s1, gs1, ws1) + seg2;
        lemma_apply_comb::<W>(rw1, rw2, pre, h0);
        lemma_apply_comb::<W>(comb(rw1, rw2), rw3, pre, h0);
        lemma_lit_tail_after::<W>(s2, pre2, b2, gs2, ws2, h0, b3, rw3, data, at + 1, v, lit as nat, end, i + 1, 0, seg3, w);
    }
}

/// A tail record names part of the slab: its last value inside its last
/// run, inside the bytes.
pub(crate) proof fn lemma_tail_ok_bounds<'a, O: RleOps<'a>>(bytes: Seq<u8>, tb: int, tl: int)
    requires tail_ok::<O>(bytes, tb, tl),
    ensures 0 <= tl <= tb <= bytes.len(),
{
    let gs = choose|gs: Seq<Group<O::V>>| #[trigger] canonical::<O>(gs) && groups_bytes::<O>(gs) == bytes
        && tb == tail_bytes_of::<O>(gs) && tl == lit_tail_of::<O>(gs);
    if gs.len() > 0 {
        assert(groups_bytes::<O>(gs) == groups_bytes::<O>(gs.drop_last()) + group_bytes::<O>(gs.last()));
        if gs.last() is Lit {
            let vs = gs.last()->values;
            assert(group_ok::<O>(gs[gs.len() - 1]));
            assert(encs::<O>(vs) == encs::<O>(vs.drop_last()) + O::enc(vs.last()));
        }
    }
}

} // verus!
