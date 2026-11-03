//! Owning values borrowed from a slab, verified with Verus.
//!
//! A reader hands out values borrowed from the slab (`T::Get<'s>`); an
//! edit cursor keeps its writer across calls, so it holds owned ones.
//! The two are separate value models to Verus. [`Lift`] is the bridge:
//! owning a value keeps its encoding and its nullness (trusted, as the
//! value types are). From that, everything about a slab carries over:
//! its parse, its values, a writer state over it, and a postfix.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use super::model::*;
// spec functions exist only when Verus compiles the crate
#[cfg(verus_keep_ghost)]
use super::dec::*;
use super::postfix::*;
// spec functions exist only when Verus compiles the crate
#[cfg(verus_keep_ghost)]
use super::read::{lit_tail_of, tail_bytes_of};
use super::state::*;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// The owned value model of a writer over borrowed values.
pub trait Lift<'s>: WriteOps<'s> {
    /// The writer over owned values.
    type W: WriteOps<'static>;

    /// The owned copy of a value.
    spec fn lift(v: VOf<'s, Self>) -> VOf<'static, Self::W>;

    /// Owning a value keeps its encoding and its nullness.
    proof fn axiom_lift(v: VOf<'s, Self>)
        ensures
            <<Self::W as WriteOps<'static>>::R as RleOps<'static>>::enc(Self::lift(v)) == <Self::R as RleOps<'s>>::enc(v),
            <<Self::W as WriteOps<'static>>::R as RleOps<'static>>::is_null(Self::lift(v)) == <Self::R as RleOps<'s>>::is_null(v);

    /// The codec and the null are the same.
    proof fn axiom_lift_codec(n: i64, m: u64)
        ensures
            <<Self::W as WriteOps<'static>>::R as RleOps<'static>>::enc_signed(n) == <Self::R as RleOps<'s>>::enc_signed(n),
            <<Self::W as WriteOps<'static>>::R as RleOps<'static>>::enc_unsigned(m) == <Self::R as RleOps<'s>>::enc_unsigned(m),
            <<Self::W as WriteOps<'static>>::R as RleOps<'static>>::nullable() == <Self::R as RleOps<'s>>::nullable(),
            <<Self::W as WriteOps<'static>>::R as RleOps<'static>>::null() == Self::lift(<Self::R as RleOps<'s>>::null());

    /// Every owned value is the owned copy of one borrowed (it lends
    /// out its contents as such).
    proof fn axiom_unlift(w: VOf<'static, Self::W>)
        ensures exists|v: VOf<'s, Self>| Self::lift(v) == w;

    /// Own a value.
    fn own(c: Self::Cow) -> (r: <Self::W as WriteOps<'static>>::Cow)
        ensures <Self::W as WriteOps<'static>>::val(r) == Self::lift(Self::val(c));
}

/// The owned writer's value model.
#[allow(dead_code)]
pub type WR<'s, L> = <<L as Lift<'s>>::W as WriteOps<'static>>::R;

/// Owned copies of values.
pub(crate) open spec fn lift_seq<'s, L: Lift<'s>>(vs: Seq<VOf<'s, L>>) -> Seq<VOf<'static, L::W>> {
    vs.map_values(|v: VOf<'s, L>| L::lift(v))
}

/// A group of owned copies.
pub(crate) open spec fn lift_group<'s, L: Lift<'s>>(g: Group<VOf<'s, L>>) -> Group<VOf<'static, L::W>> {
    match g {
        Group::Run { count, value } => Group::Run { count, value: L::lift(value) },
        Group::Lit { values } => Group::Lit { values: lift_seq::<L>(values) },
        Group::Null { count } => Group::Null { count },
    }
}

/// Groups of owned copies.
pub(crate) open spec fn lift_groups<'s, L: Lift<'s>>(gs: Seq<Group<VOf<'s, L>>>) -> Seq<Group<VOf<'static, L::W>>> {
    gs.map_values(|g: Group<VOf<'s, L>>| lift_group::<L>(g))
}

/// Owning keeps distinct non-null values distinct.
pub(crate) proof fn lemma_lift_eq<'s, L: Lift<'s>>(a: VOf<'s, L>, b: VOf<'s, L>)
    requires !<L::R as RleOps<'s>>::is_null(a), !<L::R as RleOps<'s>>::is_null(b),
    ensures (L::lift(a) == L::lift(b)) == (a == b),
{
    L::axiom_lift(a);
    L::axiom_lift(b);
    if L::lift(a) == L::lift(b) {
        <L::R as RleOps<'s>>::axiom_enc(a, b, Seq::empty(), Seq::empty());
        assert(<L::R as RleOps<'s>>::enc(a) + Seq::<u8>::empty() =~= <L::R as RleOps<'s>>::enc(b) + Seq::<u8>::empty());
    }
}

pub(crate) proof fn lemma_lift_encs<'s, L: Lift<'s>>(vs: Seq<VOf<'s, L>>)
    ensures encs::<WR<'s, L>>(lift_seq::<L>(vs)) == encs::<L::R>(vs),
    decreases vs.len(),
{
    if vs.len() > 0 {
        assert(lift_seq::<L>(vs).drop_last() =~= lift_seq::<L>(vs.drop_last()));
        lemma_lift_encs::<L>(vs.drop_last());
        L::axiom_lift(vs.last());
    }
}

pub(crate) proof fn lemma_lift_group<'s, L: Lift<'s>>(g: Group<VOf<'s, L>>)
    ensures
        group_bytes::<WR<'s, L>>(lift_group::<L>(g)) == group_bytes::<L::R>(g),
        group_len(lift_group::<L>(g)) == group_len(g),
        group_segments(lift_group::<L>(g)) == group_segments(g),
        group_values(lift_group::<L>(g), <WR<'s, L> as RleOps<'static>>::null())
            == lift_seq::<L>(group_values(g, <L::R as RleOps<'s>>::null())),
        group_ok::<L::R>(g) ==> group_ok::<WR<'s, L>>(lift_group::<L>(g)),
{
    L::axiom_lift_codec(header_of(g) as i64, 0);
    match g {
        Group::Run { count, value } => {
            L::axiom_lift(value);
        },
        Group::Lit { values } => {
            lemma_lift_encs::<L>(values);
            if group_ok::<L::R>(g) {
                let w = lift_seq::<L>(values);
                assert forall|i: int| 0 <= i < w.len() implies !<WR<'s, L> as RleOps<'static>>::is_null(#[trigger] w[i]) by {
                    L::axiom_lift(values[i]);
                }
                assert forall|i: int| 0 < i < w.len() implies w[i - 1] != #[trigger] w[i] by {
                    lemma_lift_eq::<L>(values[i - 1], values[i]);
                }
            }
        },
        Group::Null { count } => {
            L::axiom_lift_codec(0, count as u64);
        },
    }
    L::axiom_lift_codec(0, 0);
    let n = <L::R as RleOps<'s>>::null();
    assert(group_values(lift_group::<L>(g), <WR<'s, L> as RleOps<'static>>::null())
        =~= lift_seq::<L>(group_values(g, n)));
}

/// Groups `b` may follow `a` after owning, as before.
proof fn lemma_lift_adjacent<'s, L: Lift<'s>>(a: Group<VOf<'s, L>>, b: Group<VOf<'s, L>>)
    requires group_ok::<L::R>(a), group_ok::<L::R>(b), adjacent_ok(a, b),
    ensures adjacent_ok(lift_group::<L>(a), lift_group::<L>(b)),
{
    match (a, b) {
        (Group::Run { value: x, .. }, Group::Run { value: y, .. }) => lemma_lift_eq::<L>(x, y),
        (Group::Run { value: x, .. }, Group::Lit { values }) => {
            assert(!<L::R as RleOps<'s>>::is_null(values[0]));
            lemma_lift_eq::<L>(x, values[0]);
        },
        (Group::Lit { values }, Group::Run { value: y, .. }) => {
            assert(!<L::R as RleOps<'s>>::is_null(values[values.len() - 1]));
            lemma_lift_eq::<L>(values.last(), y);
        },
        _ => {},
    }
}

pub(crate) proof fn lemma_lift_groups<'s, L: Lift<'s>>(gs: Seq<Group<VOf<'s, L>>>)
    ensures
        groups_bytes::<WR<'s, L>>(lift_groups::<L>(gs)) == groups_bytes::<L::R>(gs),
        groups_len(lift_groups::<L>(gs)) == groups_len(gs),
        groups_segments(lift_groups::<L>(gs)) == groups_segments(gs),
        groups_values(lift_groups::<L>(gs), <WR<'s, L> as RleOps<'static>>::null())
            == lift_seq::<L>(groups_values(gs, <L::R as RleOps<'s>>::null())),
        canonical::<L::R>(gs) ==> canonical::<WR<'s, L>>(lift_groups::<L>(gs)),
    decreases gs.len(),
{
    let lg = lift_groups::<L>(gs);
    if gs.len() > 0 {
        assert(lg.drop_last() =~= lift_groups::<L>(gs.drop_last()));
        lemma_lift_groups::<L>(gs.drop_last());
        lemma_lift_group::<L>(gs.last());
        let n = <L::R as RleOps<'s>>::null();
        assert(lift_seq::<L>(groups_values(gs.drop_last(), n) + group_values(gs.last(), n))
            =~= lift_seq::<L>(groups_values(gs.drop_last(), n)) + lift_seq::<L>(group_values(gs.last(), n)));
    } else {
        assert(lift_seq::<L>(Seq::<VOf<'s, L>>::empty()) =~= Seq::<VOf<'static, L::W>>::empty());
    }
    if canonical::<L::R>(gs) {
        assert forall|i: int| 0 <= i < lg.len() implies group_ok::<WR<'s, L>>(#[trigger] lg[i]) by {
            lemma_lift_group::<L>(gs[i]);
        }
        assert forall|i: int| 0 < i < lg.len() implies adjacent_ok(lg[i - 1], #[trigger] lg[i]) by {
            lemma_lift_adjacent::<L>(gs[i - 1], gs[i]);
        }
    }
}

/// A valid slab, read with owned values: the same parse, owned.
pub(crate) proof fn lemma_lift_slab<'s, L: Lift<'s>>(data: Seq<u8>)
    requires valid_slab::<L::R>(data),
    ensures
        valid_slab::<WR<'s, L>>(data),
        slab_groups::<WR<'s, L>>(data) == lift_groups::<L>(slab_groups::<L::R>(data)),
        slab_values::<WR<'s, L>>(data) == lift_seq::<L>(slab_values::<L::R>(data)),
{
    let gs = slab_groups::<L::R>(data);
    let lg = lift_groups::<L>(gs);
    reveal(parses);
    lemma_lift_groups::<L>(gs);
    assert(parses::<WR<'s, L>>(lg, data));
    let g2 = slab_groups::<WR<'s, L>>(data);
    assert(parses::<WR<'s, L>>(g2, data));
    assert(all_ok::<WR<'s, L>>(g2) && all_ok::<WR<'s, L>>(lg));
    lemma_groups_unique::<WR<'s, L>>(g2, lg);
}

proof fn lemma_lift_push<'s, L: Lift<'s>>(gs: Seq<Group<VOf<'s, L>>>, g: Group<VOf<'s, L>>)
    ensures
        lift_groups::<L>(gs.push(g)) == lift_groups::<L>(gs).push(lift_group::<L>(g)),
        lift_groups::<L>(gs).len() == gs.len(),
{
    assert(lift_groups::<L>(gs.push(g)) =~= lift_groups::<L>(gs).push(lift_group::<L>(g)));
}

/// `r` is `s` with its values owned.
pub(crate) open spec fn lifted<'s, L: Lift<'s>>(s: State<'s, L>, r: State<'static, L::W>) -> bool {
    match (s, r) {
        (State::Empty, State::Empty) => true,
        (State::Lone(c), State::Lone(d)) => <L::W as WriteOps<'static>>::val(d) == L::lift(L::val(c)),
        (State::Run(n, c), State::Run(m, d)) => n == m && <L::W as WriteOps<'static>>::val(d) == L::lift(L::val(c)),
        (State::Null(n), State::Null(m)) => n == m,
        (State::Lit { count, local, header_pos, bytes, current },
         State::Lit { count: c2, local: l2, header_pos: h2, bytes: b2, current: d }) =>
            count == c2 && local == l2 && header_pos == h2 && bytes == b2
            && <L::W as WriteOps<'static>>::val(d) == L::lift(L::val(current)),
        _ => false,
    }
}

/// Own the values a writer state holds.
pub(crate) fn own_state<'s, L: Lift<'s>>(s: State<'s, L>) -> (r: State<'static, L::W>)
    ensures lifted::<L>(s, r),
{
    match s {
        State::Empty => State::Empty,
        State::Lone(v) => State::Lone(L::own(v)),
        State::Run(n, v) => State::Run(n, L::own(v)),
        State::Lit { count, local, header_pos, bytes, current } => State::Lit {
            count,
            local,
            header_pos,
            bytes,
            current: L::own(current),
        },
        State::Null(n) => State::Null(n),
    }
}

/// A writer state, owned, stands for the same bytes and the values owned.
#[verifier::spinoff_prover]
pub(crate) proof fn lemma_lift_rel<'s, L: Lift<'s>>(s: State<'s, L>, r: State<'static, L::W>, pre: Seq<u8>, buf: Seq<u8>, base: int, gs: Seq<Group<VOf<'s, L>>>, ws: Seq<VOf<'s, L>>, h0: i64)
    requires lifted::<L>(s, r), rel::<L>(s, pre, buf, base, gs, ws, h0),
    ensures
        rel::<L::W>(r, pre, buf, base, lift_groups::<L>(gs), lift_seq::<L>(ws), h0),
        values_of::<L::W>(r, lift_groups::<L>(gs), lift_seq::<L>(ws)) == lift_seq::<L>(values_of::<L>(s, gs, ws)),
        acc::<L::W>(r, lift_groups::<L>(gs), lift_seq::<L>(ws)) == acc::<L>(s, gs, ws),
{
    hide(canonical);
    hide(lift_groups);
    let lg = lift_groups::<L>(gs);
    let lw = lift_seq::<L>(ws);
    let a = all_groups::<L>(s, gs, ws);
    lemma_lift_groups::<L>(gs);
    lemma_lift_groups::<L>(a);
    lemma_lift_encs::<L>(ws);
    L::axiom_lift_codec(-1i64, 0);
    L::axiom_lift_codec(h0, 0);
    match s {
        State::Empty => {
            lemma_lift_push::<L>(gs, Group::Null { count: 0 });
        },
        State::Lone(c) => {
            assert(seq![<L::W as WriteOps<'static>>::val(r->Lone_0)] =~= lift_seq::<L>(seq![L::val(c)]));
            lemma_lift_push::<L>(gs, pend_group::<L>(s, ws)->Some_0);
        },
        State::Lit { count, local, current, .. } => {
            assert(lw.push(<L::W as WriteOps<'static>>::val(r->Lit_current)) =~= lift_seq::<L>(ws.push(L::val(current))));
            lemma_lift_push::<L>(gs, pend_group::<L>(s, ws)->Some_0);
            let k = count - local;
            lemma_lift_encs::<L>(ws.take(k));
            lemma_lift_encs::<L>(ws.skip(k));
            assert(lw.take(k) =~= lift_seq::<L>(ws.take(k)));
            assert(lw.skip(k) =~= lift_seq::<L>(ws.skip(k)));
        },
        _ => {
            lemma_lift_push::<L>(gs, pend_group::<L>(s, ws)->Some_0);
            L::axiom_lift_codec(0, 0);
        },
    }
}

/// Where group `k` sits, read with owned values: the same.
pub(crate) proof fn lemma_lift_at<'s, L: Lift<'s>>(gs: Seq<Group<VOf<'s, L>>>, k: int)
    requires 0 <= k <= gs.len(),
    ensures
        gl(lift_groups::<L>(gs), k) == gl(gs, k),
        gb::<WR<'s, L>>(lift_groups::<L>(gs), k) == gb::<L::R>(gs, k),
        gseg(lift_groups::<L>(gs), k) == gseg(gs, k),
        groups_segments(lift_groups::<L>(gs).skip(k)) == groups_segments(gs.skip(k)),
        k < gs.len() ==> lift_groups::<L>(gs)[k] == lift_group::<L>(gs[k]),
{
    assert(lift_groups::<L>(gs).take(k) =~= lift_groups::<L>(gs.take(k)));
    lemma_lift_groups::<L>(gs.take(k));
    assert(lift_groups::<L>(gs).skip(k) =~= lift_groups::<L>(gs.skip(k)));
    lemma_lift_groups::<L>(gs.skip(k));
}

/// A postfix's values, owned.
pub(crate) open spec fn plift<'s, L: Lift<'s>>(p: Option<Postfix<VOf<'s, L>>>) -> Option<Postfix<VOf<'static, L::W>>> {
    match p {
        None => None,
        Some(Postfix::Run { count, value }) => Some(Postfix::Run { count, value: L::lift(value) }),
        Some(Postfix::Lit { value, lit }) => Some(Postfix::Lit { value: L::lift(value), lit }),
        Some(Postfix::LonePlusLit { lone, value, lit }) => Some(Postfix::LonePlusLit { lone: L::lift(lone), value: L::lift(value), lit }),
    }
}

/// Own the values of a postfix read from the slab.
pub(crate) fn own_postfix<'s, L: Lift<'s>>(p: Option<Postfix<VOf<'s, L>>>) -> (r: Option<Postfix<<L::W as WriteOps<'static>>::Cow>>)
    ensures pmap::<L::W>(r) == plift::<L>(p),
{
    match p {
        None => None,
        Some(Postfix::Run { count, value }) => Some(Postfix::Run { count, value: L::own(L::cow_ref(value)) }),
        Some(Postfix::Lit { value, lit }) => Some(Postfix::Lit { value: L::own(L::cow_ref(value)), lit }),
        Some(Postfix::LonePlusLit { lone, value, lit }) =>
            Some(Postfix::LonePlusLit { lone: L::own(L::cow_ref(lone)), value: L::own(L::cow_ref(value)), lit }),
    }
}

/// A literal tail, read with owned values.
proof fn lemma_lift_tail_lit<'s, L: Lift<'s>>(data: Seq<u8>, at: int, v: VOf<'s, L>, lit: nat, end: int, i: int, j: int)
    requires valid_slab::<L::R>(data), tail_lit::<L::R>(data, at, v, lit, end, i, j),
    ensures tail_lit::<WR<'s, L>>(data, at, L::lift(v), lit, end, i, j),
{
    let gs = slab_groups::<L::R>(data);
    let vs = gs[i]->values;
    lemma_lift_slab::<L>(data);
    lemma_lift_at::<L>(gs, i);
    L::axiom_lift_codec(-(vs.len() as int) as i64, 0);
    assert(lift_seq::<L>(vs).take(j + 1) =~= lift_seq::<L>(vs.take(j + 1)));
    lemma_lift_encs::<L>(vs.take(j + 1));
}

/// A postfix of the slab, read with owned values.
pub(crate) proof fn lemma_lift_postfix<'s, L: Lift<'s>>(data: Seq<u8>, at: int, p: Option<Postfix<VOf<'s, L>>>, end: int)
    requires postfix_ok::<L::R>(data, at, p, end),
    ensures
        postfix_ok::<WR<'s, L>>(data, at, plift::<L>(p), end), vnulls_ok::<WR<'s, L>>(plift::<L>(p)),
        rest_segs::<WR<'s, L>>(data, at, plift::<L>(p), end) == rest_segs::<L::R>(data, at, p, end),
{
    let gs = slab_groups::<L::R>(data);
    lemma_lift_slab::<L>(data);
    match p {
        None => {},
        Some(Postfix::Run { count, value }) => {
            let i = choose|i: int| tail_run::<L::R>(data, at, count as nat, value, end, i);
            lemma_lift_at::<L>(gs, i);
            lemma_lift_at::<L>(gs, i + 1);
            if i + 1 < gs.len() {
                assert(lift_groups::<L>(gs)[i + 1] == lift_group::<L>(gs[i + 1]));
            }
            L::axiom_lift(value);
            lemma_lift_group::<L>(gs[i]);
            lemma_slab_group::<L::R>(data, i);
            assert(tail_run::<WR<'s, L>>(data, at, count as nat, L::lift(value), end, i));
            lemma_postfix_run::<WR<'s, L>>(data, at, count, L::lift(value), end, i);
            lemma_rest_run::<WR<'s, L>>(data, at, count, L::lift(value), end, i);
            lemma_rest_run::<L::R>(data, at, count, value, end, i);
            lemma_lift_at::<L>(gs, i + 1);
        },
        Some(Postfix::Lit { value, lit }) => {
            let (i, j) = choose|i: int, j: int| tail_lit::<L::R>(data, at, value, lit as nat, end, i, j);
            lemma_lift_tail_lit::<L>(data, at, value, lit as nat, end, i, j);
            lemma_postfix_lit::<WR<'s, L>>(data, at, L::lift(value), lit, end, i, j);
            lemma_rest_lit::<WR<'s, L>>(data, at, L::lift(value), lit, end, i, j);
            lemma_rest_lit::<L::R>(data, at, value, lit, end, i, j);
            lemma_lift_at::<L>(gs, i + 1);
            lemma_slab_lit::<L::R>(data, i, j);
            L::axiom_lift(value);
        },
        Some(Postfix::LonePlusLit { lone, value, lit }) => {
            let i = choose|i: int| tail_lone::<L::R>(data, at, lone, value, lit as nat, end, i);
            lemma_lift_tail_lit::<L>(data, at + 1, value, lit as nat, end, i + 1, 0);
            lemma_lift_at::<L>(gs, i);
            lemma_lift_at::<L>(gs, i + 1);
            assert(tail_lone::<WR<'s, L>>(data, at, L::lift(lone), L::lift(value), lit as nat, end, i));
            lemma_postfix_lone::<WR<'s, L>>(data, at, L::lift(lone), L::lift(value), lit, end, i);
            lemma_rest_lone::<WR<'s, L>>(data, at, L::lift(lone), L::lift(value), lit, end, i);
            lemma_rest_lone::<L::R>(data, at, lone, value, lit, end, i);
            lemma_lift_at::<L>(gs, i + 2);
            lemma_slab_group::<L::R>(data, i);
            lemma_slab_lit::<L::R>(data, i + 1, 0);
            L::axiom_lift(lone);
            L::axiom_lift(value);
        },
    }
}

/// A borrowed value whose owned copy is `w`.
pub(crate) open spec fn unlift<'s, L: Lift<'s>>(w: VOf<'static, L::W>) -> VOf<'s, L> {
    choose|v: VOf<'s, L>| L::lift(v) == w
}

/// Groups of borrowed values whose owned copies are `gs`.
pub(crate) open spec fn unlift_groups<'s, L: Lift<'s>>(gs: Seq<Group<VOf<'static, L::W>>>) -> Seq<Group<VOf<'s, L>>> {
    gs.map_values(|g: Group<VOf<'static, L::W>>| match g {
        Group::Run { count, value } => Group::Run { count, value: unlift::<L>(value) },
        Group::Lit { values } => Group::Lit { values: values.map_values(|w: VOf<'static, L::W>| unlift::<L>(w)) },
        Group::Null { count } => Group::Null { count },
    })
}

/// Groups that are canonical once owned were canonical borrowed.
proof fn lemma_unlift_canonical<'s, L: Lift<'s>>(gs: Seq<Group<VOf<'s, L>>>)
    requires canonical::<WR<'s, L>>(lift_groups::<L>(gs)),
    ensures canonical::<L::R>(gs),
{
    let lg = lift_groups::<L>(gs);
    L::axiom_lift_codec(0, 0);
    assert forall|i: int| 0 <= i < gs.len() implies group_ok::<L::R>(#[trigger] gs[i]) by {
        assert(group_ok::<WR<'s, L>>(lg[i]));
        match gs[i] {
            Group::Run { value, .. } => L::axiom_lift(value),
            Group::Lit { values } => {
                let w = lift_seq::<L>(values);
                assert forall|k: int| 0 <= k < values.len() implies !<L::R as RleOps<'s>>::is_null(#[trigger] values[k]) by {
                    L::axiom_lift(values[k]);
                    assert(!<WR<'s, L> as RleOps<'static>>::is_null(w[k]));
                }
                assert forall|k: int| 0 < k < values.len() implies values[k - 1] != #[trigger] values[k] by {
                    assert(w[k - 1] != w[k]);
                }
            },
            Group::Null { .. } => {},
        }
    }
    assert forall|i: int| 0 < i < gs.len() implies adjacent_ok(gs[i - 1], #[trigger] gs[i]) by {
        assert(adjacent_ok(lg[i - 1], lg[i]));
        match (gs[i - 1], gs[i]) {
            (Group::Lit { values }, Group::Run { .. }) => {
                assert(lift_seq::<L>(values).last() == L::lift(values.last()));
            },
            _ => {},
        }
    }
}

/// A slab valid read with owned values is valid read with borrowed ones.
pub(crate) proof fn lemma_unlift_slab<'s, L: Lift<'s>>(data: Seq<u8>)
    requires valid_slab::<WR<'s, L>>(data),
    ensures valid_slab::<L::R>(data),
{
    let gw = slab_groups::<WR<'s, L>>(data);
    let gl = unlift_groups::<L>(gw);
    assert forall|w: VOf<'static, L::W>| L::lift(#[trigger] unlift::<L>(w)) == w by {
        L::axiom_unlift(w);
    }
    assert(lift_groups::<L>(gl) =~= gw) by {
        assert forall|i: int| 0 <= i < gw.len() implies lift_groups::<L>(gl)[i] == gw[i] by {
            match gw[i] {
                Group::Lit { values } => {
                    assert(lift_seq::<L>(values.map_values(|w: VOf<'static, L::W>| unlift::<L>(w))) =~= values);
                },
                _ => {},
            }
        }
    }
    reveal(parses);
    lemma_lift_groups::<L>(gl);
    lemma_unlift_canonical::<L>(gl);
    assert(parses::<L::R>(gl, data));
}

/// A slab's last group, read with owned values: the same tail record.
pub(crate) proof fn lemma_lift_last<'s, L: Lift<'s>>(gs: Seq<Group<VOf<'s, L>>>)
    requires canonical::<L::R>(gs),
    ensures
        tail_bytes_of::<WR<'s, L>>(lift_groups::<L>(gs)) == tail_bytes_of::<L::R>(gs),
        lit_tail_of::<WR<'s, L>>(lift_groups::<L>(gs)) == lit_tail_of::<L::R>(gs),
{
    if gs.len() > 0 {
        let k = gs.len() - 1;
        lemma_lift_at::<L>(gs, k);
        lemma_lift_group::<L>(gs[k]);
        assert(gs.last() == gs[k]);
        if gs[k] is Lit {
            let vs = gs[k]->values;
            assert(group_ok::<L::R>(gs[k]));
            L::axiom_lift(vs.last());
            assert(lift_seq::<L>(vs).last() == L::lift(vs.last()));
        }
    }
}

/// An encoding, read with owned values.
pub(crate) proof fn lemma_lift_encodes<'s, L: Lift<'s>>(x: Seq<u8>, vals: Seq<VOf<'s, L>>, segs: int)
    requires encodes::<L::R>(x, vals, segs),
    ensures encodes::<WR<'s, L>>(x, lift_seq::<L>(vals), segs),
{
    let gs = choose|gs: Seq<Group<VOf<'s, L>>>| #[trigger] canonical::<L::R>(gs) && groups_bytes::<L::R>(gs) == x
        && groups_values(gs, <L::R as RleOps<'s>>::null()) == vals && groups_segments(gs) == segs;
    lemma_lift_groups::<L>(gs);
    let lg = lift_groups::<L>(gs);
    assert(canonical::<WR<'s, L>>(lg));
}

/// A tail record, read with owned values.
pub(crate) proof fn lemma_lift_tail_ok<'s, L: Lift<'s>>(x: Seq<u8>, tb: int, tl: int)
    requires tail_ok::<L::R>(x, tb, tl),
    ensures tail_ok::<WR<'s, L>>(x, tb, tl),
{
    let gs = choose|gs: Seq<Group<VOf<'s, L>>>| #[trigger] canonical::<L::R>(gs) && groups_bytes::<L::R>(gs) == x
        && tb == tail_bytes_of::<L::R>(gs) && tl == lit_tail_of::<L::R>(gs);
    lemma_lift_groups::<L>(gs);
    lemma_lift_last::<L>(gs);
    let lg = lift_groups::<L>(gs);
    assert(canonical::<WR<'s, L>>(lg));
}

/// A valid slab read with owned values: valid read with borrowed ones,
/// with the same metadata.
pub(crate) proof fn lemma_unlift_meta<'s, L: Lift<'s>>(data: Seq<u8>)
    requires valid_slab::<WR<'s, L>>(data),
    ensures
        valid_slab::<L::R>(data),
        slab_values::<WR<'s, L>>(data) == lift_seq::<L>(slab_values::<L::R>(data)),
        groups_segments(slab_groups::<WR<'s, L>>(data)) == groups_segments(slab_groups::<L::R>(data)),
        tail_bytes_of::<WR<'s, L>>(slab_groups::<WR<'s, L>>(data)) == tail_bytes_of::<L::R>(slab_groups::<L::R>(data)),
        lit_tail_of::<WR<'s, L>>(slab_groups::<WR<'s, L>>(data)) == lit_tail_of::<L::R>(slab_groups::<L::R>(data)),
{
    lemma_unlift_slab::<L>(data);
    lemma_lift_slab::<L>(data);
    lemma_slab::<L::R>(data);
    lemma_lift_groups::<L>(slab_groups::<L::R>(data));
    lemma_lift_last::<L>(slab_groups::<L::R>(data));
}

} // verus!

// Outside `verus!`, so Verus takes it on trust; its spec items exist only
// when Verus compiles the crate.
impl<'s, T: crate::RleValue, V: crate::AsColumnRef<T>, C: crate::Codec> Lift<'s>
    for WOps<'s, T, V, C>
{
    type W = WOps<'static, T, T, C>;

    #[cfg(verus_keep_ghost)]
    fn lift(_v: VOf<'s, Self>) -> VOf<'static, Self::W> {
        unimplemented!()
    }

    #[cfg(verus_keep_ghost)]
    fn axiom_lift(_v: VOf<'s, Self>) {}

    #[cfg(verus_keep_ghost)]
    fn axiom_lift_codec(_n: i64, _m: u64) {}

    #[cfg(verus_keep_ghost)]
    fn axiom_unlift(_w: VOf<'static, Self::W>) {}

    #[inline(always)]
    fn own(c: Self::Cow) -> <Self::W as WriteOps<'static>>::Cow {
        RleCow::Owned(match c {
            RleCow::Owned(v) => T::to_owned(v.as_column_ref()),
            RleCow::Ref(g) => T::to_owned(g),
        })
    }
}
