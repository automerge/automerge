//! Cutting loaded bytes into slabs, verified with Verus.
//!
//! A load reads the saved bytes with the verified [`Reader`], one wire
//! segment at a time, and cuts them into slabs of a target number of
//! segments. A slab's bytes are a range of the input copied verbatim,
//! except where a cut splits a literal run: the piece after the cut
//! gets a header of its own, and the piece before it has its header
//! rewritten to its own count.
//!
//! [`Cut`] is that bookkeeping. Its invariant (`Cut::ok`) describes
//! the slab being cut as groups (`sgroups`) of what the reader has
//! read since the last cut: the rest of a literal the cut split
//! (`piece`), the groups read whole (`mid`), and a literal still
//! being read. Each cut slab is then the canonical encoding of the
//! values between two cuts, with its length, segment count and tail
//! record.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

// spec functions exist only when Verus compiles the crate
#[cfg(verus_keep_ghost)]
use super::dec::*;
#[cfg(verus_keep_ghost)]
use super::merge::slab_meta;
use super::model::*;
#[cfg(verus_keep_ghost)]
use super::postfix::lemma_canonical_concat;
#[cfg(verus_keep_ghost)]
use super::read::{lit_prefix_ok, lit_tail_of, next_post, read_bytes, tail_bytes_of};
use super::read::{Reader, Seg};
#[cfg(verus_keep_ghost)]
use super::state::lemma_push;
use super::state::VOf;
use super::writer::*;
use super::Slab;
use crate::PackError;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// The error for a slab whose last run its tail record can't hold.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn over_4gib() -> PackError {
    PackError::InvalidValue("a run must stay under 4 GiB".to_string())
}

// ── The slab being cut, as groups ───────────────────────────────────────────

/// The literal the last cut split: closed as group `g0`, or still being
/// read.
pub(crate) open spec fn lit0<V>(gs: Seq<Group<V>>, cur: Seq<V>, g0: int) -> Seq<V> {
    if gs.len() > g0 { gs[g0]->Lit_values } else { cur }
}

/// The values of the literal the last cut split that came after it (or
/// none, if the cut fell between groups: `c0 == 0`).
pub(crate) open spec fn piece<V>(gs: Seq<Group<V>>, cur: Seq<V>, g0: int, c0: int) -> Seq<V> {
    if c0 > 0 { lit0(gs, cur, g0).skip(c0) } else { Seq::empty() }
}

/// The groups read whole since the last cut, after the piece.
pub(crate) open spec fn mid<V>(gs: Seq<Group<V>>, g0: int, c0: int) -> Seq<Group<V>> {
    if c0 > 0 {
        if gs.len() > g0 { gs.skip(g0 + 1) } else { Seq::empty() }
    } else {
        gs.skip(g0)
    }
}

/// The reader is inside a literal whose header is in this slab.
pub(crate) open spec fn open_in<V>(gs: Seq<Group<V>>, il: bool, g0: int, c0: int) -> bool {
    il && !(c0 > 0 && gs.len() == g0)
}

/// A literal of `vs`, if there are any.
pub(crate) open spec fn opt_lit<V>(vs: Seq<V>) -> Seq<Group<V>> {
    if vs.len() > 0 { seq![Group::Lit { values: vs }] } else { Seq::empty() }
}

/// The slab being cut, as groups.
pub(crate) open spec fn sgroups<V>(gs: Seq<Group<V>>, il: bool, cur: Seq<V>, g0: int, c0: int) -> Seq<Group<V>> {
    opt_lit(piece(gs, cur, g0, c0)) + mid(gs, g0, c0)
        + (if open_in(gs, il, g0, c0) { opt_lit(cur) } else { Seq::empty() })
}

/// The input bytes since the last cut: the piece has no header there,
/// and an open literal has the header of its full count `k`.
pub(crate) open spec fn raw<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, il: bool, k: nat, cur: Seq<O::V>, g0: int, c0: int) -> Seq<u8> {
    encs::<O>(piece(gs, cur, g0, c0)) + groups_bytes::<O>(mid(gs, g0, c0))
        + (if open_in(gs, il, g0, c0) { lit_head::<O>(k) + encs::<O>(cur) } else { Seq::empty() })
}

/// The input bytes up to the last cut.
pub(crate) open spec fn cut_bytes<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, cur: Seq<O::V>, g0: int, c0: int, k0: nat) -> Seq<u8> {
    groups_bytes::<O>(gs.take(g0))
        + (if c0 > 0 { lit_head::<O>(k0) + encs::<O>(lit0(gs, cur, g0).take(c0)) } else { Seq::empty() })
}

/// The values up to the last cut.
pub(crate) open spec fn done<V>(gs: Seq<Group<V>>, cur: Seq<V>, g0: int, c0: int, null: V) -> Seq<V> {
    groups_values(gs.take(g0), null) + (if c0 > 0 { lit0(gs, cur, g0).take(c0) } else { Seq::empty() })
}

/// The length of the last literal among `gs` (0 if none).
pub(crate) open spec fn last_lit_len<V>(gs: Seq<Group<V>>) -> nat
    decreases gs.len(),
{
    if gs.len() == 0 {
        0
    } else if gs.last() is Lit {
        gs.last()->Lit_values.len()
    } else {
        last_lit_len(gs.drop_last())
    }
}

/// The count of the last literal header in this slab's input bytes (0
/// if none).
pub(crate) open spec fn head_k<V>(gs: Seq<Group<V>>, il: bool, k: nat, g0: int, c0: int) -> nat {
    if open_in(gs, il, g0, c0) { k } else { last_lit_len(mid(gs, g0, c0)) }
}

/// The input bytes of the slab's last group: the piece has no header,
/// an open literal has its full count's.
pub(crate) open spec fn raw_tail<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, il: bool, k: nat, cur: Seq<O::V>, g0: int, c0: int) -> int {
    let m = mid(gs, g0, c0);
    if open_in(gs, il, g0, c0) {
        (lit_head::<O>(k).len() + encs::<O>(cur).len()) as int
    } else if m.len() > 0 {
        group_bytes::<O>(m.last()).len() as int
    } else {
        encs::<O>(piece(gs, cur, g0, c0)).len() as int
    }
}

/// Where the reader stands relative to the last cut, at group `g0` and
/// `c0` values into it (a literal of `k0`), or between groups (`c0 == 0`).
pub(crate) open spec fn shape<V>(gs: Seq<Group<V>>, il: bool, k: nat, cur: Seq<V>, g0: int, c0: int, k0: nat) -> bool {
    &&& 0 <= g0 <= gs.len() && 0 <= c0
    &&& c0 > 0 ==> {
        &&& c0 < k0
        &&& gs.len() > g0 ==> gs[g0] is Lit && gs[g0]->Lit_values.len() == k0
        &&& gs.len() == g0 ==> il && k == k0 && c0 <= cur.len()
    }
}

/// A finished slab: valid, with its metadata.
pub(crate) open spec fn slab_ok<'a, O: RleOps<'a>>(s: Slab) -> bool {
    &&& slab_meta::<O>(slab_bytes(s), slab_segments(s) as int, slab_tail_of(s))
    &&& slab_len(s) == slab_values::<O>(slab_bytes(s)).len()
}

/// The values of slabs, in order.
pub(crate) open spec fn slabs_values<'a, O: RleOps<'a>>(s: Seq<Slab>) -> Seq<O::V>
    decreases s.len(),
{
    if s.len() == 0 { Seq::empty() } else { slabs_values::<O>(s.drop_last()) + slab_values::<O>(slab_bytes(s.last())) }
}

// ── Lemmas ──────────────────────────────────────────────────────────────────

/// The bytes read are those up to the cut, then this slab's.
#[verifier::spinoff_prover]
proof fn lemma_raw<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, il: bool, k: nat, cur: Seq<O::V>, g0: int, c0: int, k0: nat)
    requires
        shape(gs, il, k, cur, g0, c0, k0), il == (k > 0), !il ==> cur.len() == 0,
    ensures
        read_bytes::<O>(gs, k, cur) == cut_bytes::<O>(gs, cur, g0, c0, k0) + raw::<O>(gs, il, k, cur, g0, c0),
{
    let e = Seq::<u8>::empty();
    lemma_groups_bytes_concat::<O>(gs.take(g0), gs.skip(g0));
    assert(gs =~= gs.take(g0) + gs.skip(g0));
    if c0 > 0 && gs.len() > g0 {
        let vs = gs[g0]->Lit_values;
        let r = gs.skip(g0);
        assert(r =~= seq![gs[g0]] + gs.skip(g0 + 1));
        lemma_groups_bytes_first::<O>(r);
        assert(r.skip(1) =~= gs.skip(g0 + 1));
        assert(vs =~= vs.take(c0) + vs.skip(c0));
        lemma_encs_concat::<O>(vs.take(c0), vs.skip(c0));
    } else if c0 > 0 {
        assert(gs.skip(g0) =~= Seq::<Group<O::V>>::empty());
        assert(gs.take(g0) =~= gs);
        assert(cur =~= cur.take(c0) + cur.skip(c0));
        lemma_encs_concat::<O>(cur.take(c0), cur.skip(c0));
    }
    assert(encs::<O>(Seq::<O::V>::empty()) =~= e);
    assert(groups_bytes::<O>(Seq::<Group<O::V>>::empty()) =~= e);
    assert(read_bytes::<O>(gs, k, cur) =~= cut_bytes::<O>(gs, cur, g0, c0, k0) + raw::<O>(gs, il, k, cur, g0, c0));
}

/// A literal of `vs`, as values.
proof fn lemma_opt_lit<V>(vs: Seq<V>, null: V)
    ensures groups_values(opt_lit(vs), null) == vs, groups_len(opt_lit(vs)) == vs.len(),
        groups_segments(opt_lit(vs)) == vs.len(),
{
    if vs.len() > 0 {
        let g = opt_lit(vs);
        assert(g.drop_last() =~= Seq::<Group<V>>::empty());
        reveal_with_fuel(groups_values, 2);
        reveal_with_fuel(groups_len, 2);
        reveal_with_fuel(groups_segments, 2);
        assert(groups_values(g, null) =~= vs);
    } else {
        assert(groups_values(opt_lit(vs), null) =~= vs);
    }
}

/// The values read are those up to the cut, then this slab's.
#[verifier::spinoff_prover]
proof fn lemma_vals<V>(gs: Seq<Group<V>>, il: bool, k: nat, cur: Seq<V>, g0: int, c0: int, k0: nat, null: V)
    requires shape(gs, il, k, cur, g0, c0, k0), !il ==> cur.len() == 0,
    ensures
        groups_values(gs, null) + cur == done(gs, cur, g0, c0, null) + groups_values(sgroups(gs, il, cur, g0, c0), null),
        groups_len(sgroups(gs, il, cur, g0, c0)) == groups_values(sgroups(gs, il, cur, g0, c0), null).len(),
{
    let p = opt_lit(piece(gs, cur, g0, c0));
    let m = mid(gs, g0, c0);
    let q = if open_in(gs, il, g0, c0) { opt_lit(cur) } else { Seq::<Group<V>>::empty() };
    let e = Seq::<Group<V>>::empty();
    lemma_opt_lit(piece(gs, cur, g0, c0), null);
    lemma_opt_lit(cur, null);
    lemma_groups_values_concat(p, m, null);
    lemma_groups_values_concat(p + m, q, null);
    lemma_groups_values_concat(gs.take(g0), gs.skip(g0), null);
    lemma_groups_values_concat(sgroups(gs, il, cur, g0, c0), e, null);
    assert(sgroups(gs, il, cur, g0, c0) + e =~= sgroups(gs, il, cur, g0, c0));
    assert(gs =~= gs.take(g0) + gs.skip(g0));
    assert(groups_values(e, null) =~= Seq::<V>::empty());
    if c0 > 0 && gs.len() > g0 {
        let vs = gs[g0]->Lit_values;
        let r = gs.skip(g0);
        assert(r =~= seq![gs[g0]] + gs.skip(g0 + 1));
        lemma_groups_values_concat(seq![gs[g0]], gs.skip(g0 + 1), null);
        lemma_opt_lit(vs, null);
        assert(seq![gs[g0]] == opt_lit(vs));
        assert(vs =~= vs.take(c0) + vs.skip(c0));
        if il {
            assert(groups_values(gs, null) + cur =~= done(gs, cur, g0, c0, null) + groups_values(sgroups(gs, il, cur, g0, c0), null));
        } else {
            assert(groups_values(gs, null) + cur =~= done(gs, cur, g0, c0, null) + groups_values(sgroups(gs, il, cur, g0, c0), null));
        }
    } else if c0 > 0 {
        assert(gs.skip(g0) =~= e);
        assert(gs.take(g0) =~= gs);
        assert(cur =~= cur.take(c0) + cur.skip(c0));
        assert(groups_values(gs, null) + cur =~= done(gs, cur, g0, c0, null) + groups_values(sgroups(gs, il, cur, g0, c0), null));
    } else {
        assert(groups_values(gs, null) + cur =~= done(gs, cur, g0, c0, null) + groups_values(sgroups(gs, il, cur, g0, c0), null));
    }
}

/// A literal of `vs`: canonical, if its values may make one.
proof fn lemma_lit_ok<'a, O: RleOps<'a>>(vs: Seq<O::V>)
    requires
        vs.len() <= i64::MAX,
        forall|i: int| 0 <= i < vs.len() ==> !O::is_null(#[trigger] vs[i]),
        forall|i: int| 0 < i < vs.len() ==> vs[i - 1] != #[trigger] vs[i],
    ensures canonical::<O>(opt_lit(vs)),
{
    if vs.len() > 0 {
        assert(opt_lit(vs)[0] == Group::Lit { values: vs });
    }
}

/// The values after `c` of a literal's: a literal's too.
proof fn lemma_skip_lit<'a, O: RleOps<'a>>(vs: Seq<O::V>, c: int)
    requires
        0 <= c <= vs.len(), vs.len() <= i64::MAX,
        forall|i: int| 0 <= i < vs.len() ==> !O::is_null(#[trigger] vs[i]),
        forall|i: int| 0 < i < vs.len() ==> vs[i - 1] != #[trigger] vs[i],
    ensures canonical::<O>(opt_lit(vs.skip(c))),
{
    let w = vs.skip(c);
    assert forall|i: int| 0 <= i < w.len() implies !O::is_null(#[trigger] w[i]) by {
        assert(w[i] == vs[c + i]);
    }
    assert forall|i: int| 0 < i < w.len() implies w[i - 1] != #[trigger] w[i] by {
        assert(w[i] == vs[c + i] && w[i - 1] == vs[c + i - 1]);
    }
    lemma_lit_ok::<O>(w);
}

/// Groups `gs` from `i`: canonical.
proof fn lemma_canonical_skip<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, i: int)
    requires canonical::<O>(gs), 0 <= i <= gs.len(),
    ensures canonical::<O>(gs.skip(i)),
{
    let r = gs.skip(i);
    assert forall|j: int| 0 <= j < r.len() implies group_ok::<O>(#[trigger] r[j]) by {
        assert(r[j] == gs[i + j]);
    }
    assert forall|j: int| 0 < j < r.len() implies adjacent_ok(r[j - 1], #[trigger] r[j]) by {
        assert(r[j] == gs[i + j] && r[j - 1] == gs[i + j - 1]);
    }
}

/// The slab being cut is canonical.
#[verifier::spinoff_prover]
proof fn lemma_canon<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, il: bool, k: nat, cur: Seq<O::V>, g0: int, c0: int, k0: nat)
    requires
        shape(gs, il, k, cur, g0, c0, k0), canonical::<O>(gs), k <= i64::MAX,
        il ==> cur.len() < k && lit_prefix_ok::<O>(gs, cur),
        !il ==> cur.len() == 0,
    ensures canonical::<O>(sgroups(gs, il, cur, g0, c0)),
{
    let pv = piece(gs, cur, g0, c0);
    let p = opt_lit(pv);
    let m = mid(gs, g0, c0);
    let q = if open_in(gs, il, g0, c0) { opt_lit(cur) } else { Seq::<Group<O::V>>::empty() };
    // the piece
    if c0 > 0 && gs.len() > g0 {
        let vs = gs[g0]->Lit_values;
        assert(group_ok::<O>(gs[g0]));
        lemma_skip_lit::<O>(vs, c0);
    } else if c0 > 0 {
        lemma_skip_lit::<O>(cur, c0);
    } else {
        assert(p =~= Seq::<Group<O::V>>::empty());
    }
    // the groups read whole
    if c0 > 0 && gs.len() > g0 {
        lemma_canonical_skip::<O>(gs, g0 + 1);
    } else if c0 > 0 {
        assert(m =~= Seq::<Group<O::V>>::empty());
    } else {
        lemma_canonical_skip::<O>(gs, g0);
    }
    // the literal being read
    if open_in(gs, il, g0, c0) {
        lemma_lit_ok::<O>(cur);
    }
    // where they meet
    if p.len() > 0 && m.len() > 0 {
        assert(adjacent_ok(gs[g0], gs[g0 + 1]));
        assert(m[0] == gs[g0 + 1]);
        assert(p.last() == Group::Lit { values: pv });
        assert(pv.last() == gs[g0]->Lit_values.last());
    }
    lemma_canonical_concat::<O>(p, m);
    if (p + m).len() > 0 && q.len() > 0 {
        if m.len() > 0 {
            assert((p + m).last() == m.last());
            assert(m.last() == gs.last());
        } else {
            assert(c0 > 0 && gs.len() > g0);
            assert(gs.skip(g0 + 1).len() == 0);
            assert(gs.last() == gs[g0]);
        }
        assert(q[0] == Group::Lit { values: cur });
    }
    lemma_canonical_concat::<O>(p + m, q);
}

/// A slab's last group's input bytes are among its input bytes.
proof fn lemma_raw_tail<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, il: bool, k: nat, cur: Seq<O::V>, g0: int, c0: int)
    ensures 0 <= raw_tail::<O>(gs, il, k, cur, g0, c0) <= raw::<O>(gs, il, k, cur, g0, c0).len(),
{
    let m = mid(gs, g0, c0);
    if !open_in(gs, il, g0, c0) && m.len() > 0 {
        assert(groups_bytes::<O>(m) == groups_bytes::<O>(m.drop_last()) + group_bytes::<O>(m.last()));
    }
}

/// The last literal among groups with one pushed.
proof fn lemma_last_lit_push<V>(gs: Seq<Group<V>>, g: Group<V>)
    ensures last_lit_len(gs.push(g)) == if g is Lit { g->Lit_values.len() } else { last_lit_len(gs) },
{
    assert(gs.push(g).drop_last() =~= gs);
}

/// Sizes stay in range: the slab's values and input bytes are among the
/// reader's.
#[verifier::spinoff_prover]
proof fn lemma_bounds<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, il: bool, k: nat, cur: Seq<O::V>, g0: int, c0: int, k0: nat)
    requires
        shape(gs, il, k, cur, g0, c0, k0), il == (k > 0), !il ==> cur.len() == 0,
        read_bytes::<O>(gs, k, cur).len() <= usize::MAX, groups_len(gs) + cur.len() <= usize::MAX,
    ensures
        groups_len(sgroups(gs, il, cur, g0, c0)) <= usize::MAX,
        raw_tail::<O>(gs, il, k, cur, g0, c0) <= usize::MAX,
        cut_bytes::<O>(gs, cur, g0, c0, k0).len() + raw::<O>(gs, il, k, cur, g0, c0).len() == read_bytes::<O>(gs, k, cur).len(),
{
    lemma_raw::<O>(gs, il, k, cur, g0, c0, k0);
    lemma_raw_tail::<O>(gs, il, k, cur, g0, c0);
    lemma_vals(gs, il, k, cur, g0, c0, k0, O::null());
    lemma_groups_values_concat(gs, Seq::empty(), O::null());
    let sg = sgroups(gs, il, cur, g0, c0);
    assert((groups_values(gs, O::null()) + cur).len()
        == (done(gs, cur, g0, c0, O::null()) + groups_values(sg, O::null())).len());
}

/// A literal header read: the slab's groups are as they were, with an
/// empty literal open.
proof fn lemma_step_head<V>(gs: Seq<Group<V>>, g0: int, c0: int, k0: nat, kn: nat)
    requires shape(gs, false, 0, Seq::<V>::empty(), g0, c0, k0), kn >= 1,
    ensures
        shape(gs, true, kn, Seq::<V>::empty(), g0, c0, k0),
        open_in(gs, true, g0, c0), !open_in(gs, false, g0, c0),
        sgroups(gs, true, Seq::<V>::empty(), g0, c0) == sgroups(gs, false, Seq::<V>::empty(), g0, c0),
        piece(gs, Seq::<V>::empty(), g0, c0) == piece(gs, Seq::<V>::empty(), g0, c0),
        head_k(gs, true, kn, g0, c0) == kn,
        head_k(gs, false, 0, g0, c0) == last_lit_len(mid(gs, g0, c0)),
{
    let e = Seq::<V>::empty();
    assert(opt_lit(e) =~= Seq::<Group<V>>::empty());
    assert(sgroups(gs, true, e, g0, c0) =~= sgroups(gs, false, e, g0, c0));
}

/// A group read whole after the last cut, outside a literal.
#[verifier::spinoff_prover]
proof fn lemma_step_group<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, g: Group<O::V>, g0: int, c0: int, k0: nat)
    requires shape(gs, false, 0, Seq::<O::V>::empty(), g0, c0, k0), !(g is Lit),
    ensures ({
        let e = Seq::<O::V>::empty();
        let g1 = gs.push(g);
        &&& shape(g1, false, 0, e, g0, c0, k0)
        &&& cut_bytes::<O>(g1, e, g0, c0, k0) == cut_bytes::<O>(gs, e, g0, c0, k0)
        &&& done(g1, e, g0, c0, O::null()) == done(gs, e, g0, c0, O::null())
        &&& sgroups(g1, false, e, g0, c0) == sgroups(gs, false, e, g0, c0).push(g)
        &&& piece(g1, e, g0, c0) == piece(gs, e, g0, c0)
        &&& raw_tail::<O>(g1, false, 0, e, g0, c0) == group_bytes::<O>(g).len()
        &&& head_k(g1, false, 0, g0, c0) == head_k(gs, false, 0, g0, c0)
        &&& !open_in(g1, false, g0, c0)
    }),
{
    let e = Seq::<O::V>::empty();
    let g1 = gs.push(g);
    let m0 = mid(gs, g0, c0);
    let m1 = mid(g1, g0, c0);
    assert(g1.take(g0) =~= gs.take(g0));
    if c0 > 0 {
        assert(g1[g0] == gs[g0]);
        assert(m1 =~= m0.push(g));
    } else {
        assert(m1 =~= m0.push(g));
    }
    lemma_last_lit_push(m0, g);
    let p = opt_lit(piece(gs, e, g0, c0));
    assert(sgroups(g1, false, e, g0, c0) =~= sgroups(gs, false, e, g0, c0).push(g));
}

/// A literal value read that does not close the literal.
#[verifier::spinoff_prover]
proof fn lemma_step_more<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, k: nat, cur: Seq<O::V>, v: O::V, g0: int, c0: int, k0: nat)
    requires shape(gs, true, k, cur, g0, c0, k0), cur.len() + 1 < k,
    ensures ({
        let c1 = cur.push(v);
        let sg0 = sgroups(gs, true, cur, g0, c0);
        let sg1 = sgroups(gs, true, c1, g0, c0);
        &&& shape(gs, true, k, c1, g0, c0, k0)
        &&& cut_bytes::<O>(gs, c1, g0, c0, k0) == cut_bytes::<O>(gs, cur, g0, c0, k0)
        &&& done(gs, c1, g0, c0, O::null()) == done(gs, cur, g0, c0, O::null())
        &&& groups_len(sg1) == groups_len(sg0) + 1
        &&& groups_segments(sg1) == groups_segments(sg0) + 1
        &&& lit_tail_of::<O>(sg1) == O::enc(v).len()
        &&& raw_tail::<O>(gs, true, k, c1, g0, c0) == raw_tail::<O>(gs, true, k, cur, g0, c0) + O::enc(v).len()
        &&& head_k(gs, true, k, g0, c0) == head_k(gs, true, k, g0, c0)
        &&& open_in(gs, true, g0, c0) ==> piece(gs, c1, g0, c0) == piece(gs, cur, g0, c0)
        &&& !open_in(gs, true, g0, c0) ==> piece(gs, c1, g0, c0).len() == piece(gs, cur, g0, c0).len() + 1
            && head_k(gs, true, k, g0, c0) == 0
        &&& open_in(gs, true, g0, c0) ==> head_k(gs, true, k, g0, c0) == k && k >= 1
    }),
{
    let c1 = cur.push(v);
    let x = opt_lit(piece(gs, cur, g0, c0)) + mid(gs, g0, c0);
    assert(c1.drop_last() =~= cur);
    assert(encs::<O>(c1) == encs::<O>(cur) + O::enc(v));
    if open_in(gs, true, g0, c0) {
        assert(piece(gs, c1, g0, c0) =~= piece(gs, cur, g0, c0));
        assert(sgroups(gs, true, cur, g0, c0) == x + opt_lit(cur));
        assert(sgroups(gs, true, c1, g0, c0) =~= x + opt_lit(c1));
        lemma_groups_len_concat(x, opt_lit(cur));
        lemma_groups_len_concat(x, opt_lit(c1));
        lemma_opt_lit(cur, O::null());
        lemma_opt_lit(c1, O::null());
        assert((x + opt_lit(c1)).last() == Group::Lit { values: c1 });
    } else {
        let p0 = cur.skip(c0);
        let p1 = c1.skip(c0);
        assert(p1 =~= p0.push(v));
        assert(p1.drop_last() =~= p0);
        assert(mid(gs, g0, c0) =~= Seq::<Group<O::V>>::empty());
        assert(sgroups(gs, true, cur, g0, c0) =~= opt_lit(p0));
        assert(sgroups(gs, true, c1, g0, c0) =~= opt_lit(p1));
        lemma_opt_lit(p0, O::null());
        lemma_opt_lit(p1, O::null());
        assert(c1.take(c0) =~= cur.take(c0));
    }
}

/// A literal value read that closes the literal.
#[verifier::spinoff_prover]
proof fn lemma_step_close<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, k: nat, cur: Seq<O::V>, v: O::V, g0: int, c0: int, k0: nat)
    requires shape(gs, true, k, cur, g0, c0, k0), cur.len() + 1 == k,
    ensures ({
        let e = Seq::<O::V>::empty();
        let c1 = cur.push(v);
        let g1 = gs.push(Group::Lit { values: c1 });
        let sg0 = sgroups(gs, true, cur, g0, c0);
        let sg1 = sgroups(g1, false, e, g0, c0);
        &&& shape(g1, false, 0, e, g0, c0, k0)
        &&& cut_bytes::<O>(g1, e, g0, c0, k0) == cut_bytes::<O>(gs, cur, g0, c0, k0)
        &&& done(g1, e, g0, c0, O::null()) == done(gs, cur, g0, c0, O::null())
        &&& groups_len(sg1) == groups_len(sg0) + 1
        &&& groups_segments(sg1) == groups_segments(sg0) + 1
        &&& lit_tail_of::<O>(sg1) == O::enc(v).len()
        &&& raw_tail::<O>(g1, false, 0, e, g0, c0) == raw_tail::<O>(gs, true, k, cur, g0, c0) + O::enc(v).len()
        &&& !open_in(g1, false, g0, c0)
        &&& open_in(gs, true, g0, c0) ==> piece(g1, e, g0, c0) == piece(gs, cur, g0, c0)
            && head_k(gs, true, k, g0, c0) == k && head_k(g1, false, 0, g0, c0) == k && k >= 1
        &&& !open_in(gs, true, g0, c0) ==> piece(g1, e, g0, c0).len() == piece(gs, cur, g0, c0).len() + 1
            && head_k(gs, true, k, g0, c0) == 0 && head_k(g1, false, 0, g0, c0) == 0
    }),
{
    let e = Seq::<O::V>::empty();
    let c1 = cur.push(v);
    let lg = Group::Lit { values: c1 };
    let g1 = gs.push(lg);
    assert(c1.drop_last() =~= cur);
    assert(encs::<O>(c1) == encs::<O>(cur) + O::enc(v));
    assert(g1.take(g0) =~= gs.take(g0));
    assert(opt_lit(e) =~= Seq::<Group<O::V>>::empty());
    if open_in(gs, true, g0, c0) {
        let m0 = mid(gs, g0, c0);
        let x = opt_lit(piece(gs, cur, g0, c0)) + m0;
        if c0 > 0 {
            assert(g1[g0] == gs[g0]);
        }
        assert(mid(g1, g0, c0) =~= m0.push(lg));
        assert(piece(g1, e, g0, c0) =~= piece(gs, cur, g0, c0));
        assert(sgroups(gs, true, cur, g0, c0) == x + opt_lit(cur));
        assert(sgroups(g1, false, e, g0, c0) =~= x + opt_lit(c1));
        lemma_groups_len_concat(x, opt_lit(cur));
        lemma_groups_len_concat(x, opt_lit(c1));
        lemma_opt_lit(cur, O::null());
        lemma_opt_lit(c1, O::null());
        assert((x + opt_lit(c1)).last() == lg);
        lemma_last_lit_push(m0, lg);
        assert(mid(g1, g0, c0).last() == lg);
    } else {
        let p0 = cur.skip(c0);
        let p1 = c1.skip(c0);
        assert(g1[g0] == lg);
        assert(p1 =~= p0.push(v));
        assert(p1.drop_last() =~= p0);
        assert(mid(gs, g0, c0) =~= Seq::<Group<O::V>>::empty());
        assert(mid(g1, g0, c0) =~= Seq::<Group<O::V>>::empty());
        assert(sgroups(gs, true, cur, g0, c0) =~= opt_lit(p0));
        assert(sgroups(g1, false, e, g0, c0) =~= opt_lit(p1));
        lemma_opt_lit(p0, O::null());
        lemma_opt_lit(p1, O::null());
        assert(c1.take(c0) =~= cur.take(c0));
        assert(gs.take(g0) =~= gs);
    }
}

/// A literal of `vs`, as bytes.
proof fn lemma_opt_lit_bytes<'a, O: RleOps<'a>>(vs: Seq<O::V>)
    ensures groups_bytes::<O>(opt_lit(vs)) == if vs.len() > 0 { lit_head::<O>(vs.len()) + encs::<O>(vs) } else { Seq::empty() },
{
    if vs.len() > 0 {
        assert(opt_lit(vs).drop_last() =~= Seq::<Group<O::V>>::empty());
        reveal_with_fuel(groups_bytes, 2);
        assert(groups_bytes::<O>(opt_lit(vs)) =~= lit_head::<O>(vs.len()) + encs::<O>(vs));
    } else {
        assert(groups_bytes::<O>(opt_lit(vs)) =~= Seq::<u8>::empty());
    }
}

/// No literal among groups whose last literal has no values.
proof fn lemma_no_last_lit<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>)
    requires canonical::<O>(gs), last_lit_len(gs) == 0, gs.len() > 0,
    ensures !(gs.last() is Lit),
{
    assert(group_ok::<O>(gs[gs.len() - 1]));
}

/// The slab being cut, as bytes: its input bytes with a header before
/// the piece, and the open literal's header for the values read.
#[verifier::spinoff_prover]
proof fn lemma_sg_bytes<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, il: bool, k: nat, cur: Seq<O::V>, g0: int, c0: int)
    ensures ({
        let pc = piece(gs, cur, g0, c0);
        let m = mid(gs, g0, c0);
        let sg = sgroups(gs, il, cur, g0, c0);
        let hp = if pc.len() > 0 { lit_head::<O>(pc.len()) } else { Seq::<u8>::empty() };
        let op = open_in(gs, il, g0, c0);
        &&& groups_bytes::<O>(sg) == hp + encs::<O>(pc) + groups_bytes::<O>(m)
            + (if op && cur.len() > 0 { lit_head::<O>(cur.len()) + encs::<O>(cur) } else { Seq::empty() })
        &&& sg.len() > 0 && op && cur.len() > 0 ==> sg.last() == Group::Lit { values: cur }
        &&& sg.len() > 0 && !(op && cur.len() > 0) && m.len() > 0 ==> sg.last() == m.last()
        &&& sg.len() > 0 && !(op && cur.len() > 0) && m.len() == 0 ==> sg.last() == Group::Lit { values: pc } && pc.len() > 0
    }),
{
    let pc = piece(gs, cur, g0, c0);
    let m = mid(gs, g0, c0);
    let p = opt_lit(pc);
    let q = if open_in(gs, il, g0, c0) { opt_lit(cur) } else { Seq::<Group<O::V>>::empty() };
    lemma_opt_lit_bytes::<O>(pc);
    lemma_opt_lit_bytes::<O>(cur);
    lemma_groups_bytes_concat::<O>(p, m);
    lemma_groups_bytes_concat::<O>(p + m, q);
    assert(encs::<O>(Seq::<O::V>::empty()) =~= Seq::<u8>::empty());
    assert(groups_bytes::<O>(Seq::<Group<O::V>>::empty()) =~= Seq::<u8>::empty());
    let sg = sgroups(gs, il, cur, g0, c0);
    assert(sg == p + m + q);
    if pc.len() > 0 {
        assert(groups_bytes::<O>(sg) =~= lit_head::<O>(pc.len()) + encs::<O>(pc) + groups_bytes::<O>(m)
            + (if open_in(gs, il, g0, c0) && cur.len() > 0 { lit_head::<O>(cur.len()) + encs::<O>(cur) } else { Seq::empty() }));
    } else {
        assert(groups_bytes::<O>(sg) =~= Seq::<u8>::empty() + encs::<O>(pc) + groups_bytes::<O>(m)
            + (if open_in(gs, il, g0, c0) && cur.len() > 0 { lit_head::<O>(cur.len()) + encs::<O>(cur) } else { Seq::empty() }));
    }
}

/// Groups that parse bytes are the slab's.
proof fn lemma_slab_is<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, data: Seq<u8>)
    requires canonical::<O>(gs), groups_len(gs) <= usize::MAX, groups_bytes::<O>(gs) == data,
    ensures valid_slab::<O>(data), slab_groups::<O>(data) == gs,
{
    reveal(parses);
    assert(parses::<O>(gs, data));
    let g2 = slab_groups::<O>(data);
    assert(parses::<O>(g2, data));
    assert(all_ok::<O>(g2) && all_ok::<O>(gs));
    lemma_groups_unique::<O>(g2, gs);
}

/// Just after a cut at the reader: no slab yet, and everything read is
/// done.
#[verifier::spinoff_prover]
proof fn lemma_cut_reset<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, il: bool, k: nat, cur: Seq<O::V>)
    requires il == (k > 0), !il ==> cur.len() == 0, il ==> 0 < cur.len() < k,
    ensures ({
        let g0 = gs.len() as int;
        let c0 = if il { cur.len() as int } else { 0 };
        let e = Seq::<Group<O::V>>::empty();
        &&& shape(gs, il, k, cur, g0, c0, k)
        &&& sgroups(gs, il, cur, g0, c0) == e
        &&& piece(gs, cur, g0, c0).len() == 0
        &&& head_k(gs, il, k, g0, c0) == 0
        &&& raw_tail::<O>(gs, il, k, cur, g0, c0) == 0
        &&& cut_bytes::<O>(gs, cur, g0, c0, k) == read_bytes::<O>(gs, k, cur)
        &&& done(gs, cur, g0, c0, O::null()) == groups_values(gs, O::null()) + cur
        &&& groups_len(e) == 0 && groups_segments(e) == 0 && lit_tail_of::<O>(e) == 0
    }),
{
    let g0 = gs.len() as int;
    let c0 = if il { cur.len() as int } else { 0 };
    let e = Seq::<Group<O::V>>::empty();
    assert(gs.take(g0) =~= gs);
    assert(gs.skip(g0) =~= e);
    assert(cur.skip(c0) =~= Seq::<O::V>::empty());
    assert(cur.take(c0) =~= cur);
    assert(opt_lit(Seq::<O::V>::empty()) =~= e);
    assert(sgroups(gs, il, cur, g0, c0) =~= e);
    assert(encs::<O>(Seq::<O::V>::empty()) =~= Seq::<u8>::empty());
    if !il {
        assert(cur =~= Seq::<O::V>::empty());
    }
    assert(cut_bytes::<O>(gs, cur, g0, c0, k) =~= read_bytes::<O>(gs, k, cur));
    assert(done(gs, cur, g0, c0, O::null()) =~= groups_values(gs, O::null()) + cur);
}

/// A last literal value's bytes are among its group's.
pub(crate) proof fn lemma_lit_tail_le<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>)
    requires canonical::<O>(gs),
    ensures lit_tail_of::<O>(gs) <= tail_bytes_of::<O>(gs),
{
    if gs.len() > 0 && gs.last() is Lit {
        let vs = gs.last()->Lit_values;
        assert(group_ok::<O>(gs[gs.len() - 1]));
        assert(vs =~= vs.drop_last().push(vs.last()));
        assert(encs::<O>(vs) == encs::<O>(vs.drop_last()) + O::enc(vs.last()));
    }
}

// ── Cutting ─────────────────────────────────────────────────────────────────

/// The slab-cutting state of a load.
pub(crate) struct Cut {
    /// the slabs cut so far
    slabs: Vec<Slab>,
    /// the slab being cut: its values and segments
    len: usize,
    segments: usize,
    /// the byte width of its last literal value, if its last group is a
    /// literal (else 0)
    lit_tail: usize,
    /// where its bytes start in the input
    start: usize,
    /// the count of the last literal header in its bytes (0 if none),
    /// and the values read of that literal (or, if none, of the piece
    /// of a literal the last cut split)
    last_lit_count: usize,
    lit_count: usize,
    /// the piece's length, once a literal header follows it
    pending_header: usize,
    /// the input bytes of its last group so far
    tail_len: usize,
    /// where the last cut fell: `c0` values into group `g0` (a literal
    /// of `k0`), or before it (`c0 == 0`)
    g0: Ghost<int>,
    c0: Ghost<int>,
    k0: Ghost<nat>,
}

/// What building a slab's bytes needs of the cut state.
closed spec fn build_ok<'a, O: RleOps<'a>>(c: Cut, gs: Seq<Group<O::V>>, il: bool, k: nat, cur: Seq<O::V>) -> bool {
    let g0 = c.g0@;
    let c0 = c.c0@;
    let sg = sgroups(gs, il, cur, g0, c0);
    let pc = piece(gs, cur, g0, c0);
    let hk = head_k(gs, il, k, g0, c0);
    &&& shape(gs, il, k, cur, g0, c0, c.k0@)
    &&& il == (k > 0) && k <= i64::MAX && (il ==> 0 < cur.len() < k) && (!il ==> cur.len() == 0)
    &&& canonical::<O>(sg) && groups_segments(sg) > 0
    &&& c.tail_len == raw_tail::<O>(gs, il, k, cur, g0, c0)
    &&& c.lit_tail == lit_tail_of::<O>(sg)
    &&& c.last_lit_count == hk
    &&& c.lit_count == (if hk == 0 { pc.len() } else if open_in(gs, il, g0, c0) { cur.len() } else { hk })
    &&& c.pending_header == (if hk == 0 { 0 } else { pc.len() })
}

/// What each branch of building a slab's bytes stands for.
#[verifier::spinoff_prover]
#[verifier::rlimit(60)]
proof fn lemma_build_cases<'a, O: RleOps<'a>>(c: Cut, gs: Seq<Group<O::V>>, il: bool, k: nat, cur: Seq<O::V>)
    requires build_ok::<O>(c, gs, il, k, cur),
    ensures ({
        let g0 = c.g0@;
        let c0 = c.c0@;
        let pc = piece(gs, cur, g0, c0);
        let m = mid(gs, g0, c0);
        let sg = sgroups(gs, il, cur, g0, c0);
        let rw = raw::<O>(gs, il, k, cur, g0, c0);
        let op = open_in(gs, il, g0, c0);
        let hp = if pc.len() > 0 { lit_head::<O>(pc.len()) } else { Seq::<u8>::empty() };
        let ow = lit_head::<O>(k) + encs::<O>(cur);
        &&& c.pending_header > 0 ==> c.pending_header == pc.len()
        &&& c.pending_header == 0 && c.lit_count > c.last_lit_count && c.last_lit_count == 0 ==> c.lit_count == pc.len() > 0
        &&& c.pending_header == 0 && !(c.lit_count > c.last_lit_count && c.last_lit_count == 0) ==> pc.len() == 0
        &&& c.pending_header > 0 ==> !(!op && m.len() == 0)
        &&& c.pending_header == 0 && c.lit_count > c.last_lit_count && c.last_lit_count == 0
            ==> (c.lit_tail > 0) == (!op && m.len() == 0)
        &&& (c.lit_count < c.last_lit_count) == op
        &&& pc.len() <= i64::MAX
        &&& c.tail_len <= rw.len()
        &&& op ==> {
            &&& c.tail_len == ow.len() && rw.skip(rw.len() - ow.len()) == ow
            &&& 1 <= c.lit_count == cur.len()
            &&& groups_bytes::<O>(sg) == hp + rw.take(rw.len() - ow.len()) + lit_head::<O>(cur.len()) + encs::<O>(cur)
            &&& tail_bytes_of::<O>(sg) == lit_head::<O>(cur.len()).len() + encs::<O>(cur).len()
        }
        &&& !op ==> {
            &&& groups_bytes::<O>(sg) == hp + rw
            &&& tail_bytes_of::<O>(sg) == c.tail_len + (if m.len() == 0 { hp.len() } else { 0 })
        }
    }),
{
    let g0 = c.g0@;
    let c0 = c.c0@;
    lemma_build_counts::<O>(c, gs, il, k, cur);
    lemma_build_bytes::<O>(gs, il, k, cur, g0, c0);
    lemma_raw_tail::<O>(gs, il, k, cur, g0, c0);
}

/// The counters' branches in building a slab's bytes.
#[verifier::spinoff_prover]
proof fn lemma_build_counts<'a, O: RleOps<'a>>(c: Cut, gs: Seq<Group<O::V>>, il: bool, k: nat, cur: Seq<O::V>)
    requires build_ok::<O>(c, gs, il, k, cur),
    ensures ({
        let g0 = c.g0@;
        let c0 = c.c0@;
        let pc = piece(gs, cur, g0, c0);
        let m = mid(gs, g0, c0);
        let op = open_in(gs, il, g0, c0);
        &&& c.pending_header > 0 ==> c.pending_header == pc.len()
        &&& c.pending_header == 0 && c.lit_count > c.last_lit_count && c.last_lit_count == 0 ==> c.lit_count == pc.len() > 0
        &&& c.pending_header == 0 && !(c.lit_count > c.last_lit_count && c.last_lit_count == 0) ==> pc.len() == 0
        &&& c.pending_header > 0 ==> !(!op && m.len() == 0)
        &&& c.pending_header == 0 && c.lit_count > c.last_lit_count && c.last_lit_count == 0
            ==> (c.lit_tail > 0) == (!op && m.len() == 0)
        &&& (c.lit_count < c.last_lit_count) == op
        &&& pc.len() <= i64::MAX
        &&& op ==> 1 <= c.lit_count == cur.len()
    }),
{
    hide(canonical);
    let g0 = c.g0@;
    let c0 = c.c0@;
    let pc = piece(gs, cur, g0, c0);
    let m = mid(gs, g0, c0);
    let sg = sgroups(gs, il, cur, g0, c0);
    let op = open_in(gs, il, g0, c0);
    let e = Seq::<Group<O::V>>::empty();
    if !op {
        assert(sg == opt_lit(pc) + m + e);
        assert(opt_lit(pc) + m + e =~= opt_lit(pc) + m);
        if m.len() > 0 {
            assert(sg.last() == m.last());
            reveal(canonical);
            assert(group_ok::<O>(sg[sg.len() - 1]));
        } else if pc.len() > 0 {
            assert(sg =~= opt_lit(pc));
            O::axiom_enc(pc.last(), pc.last(), Seq::empty(), Seq::empty());
        }
    }
    if pc.len() > 0 {
        assert(sg[0] == Group::Lit { values: pc });
        reveal(canonical);
        assert(group_ok::<O>(sg[0]));
    }
}

/// The bytes in building a slab's: its input bytes, with the piece's
/// header before them and the open literal's rewritten.
#[verifier::spinoff_prover]
proof fn lemma_build_bytes<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, il: bool, k: nat, cur: Seq<O::V>, g0: int, c0: int)
    requires il ==> cur.len() > 0,
    ensures ({
        let pc = piece(gs, cur, g0, c0);
        let m = mid(gs, g0, c0);
        let sg = sgroups(gs, il, cur, g0, c0);
        let rw = raw::<O>(gs, il, k, cur, g0, c0);
        let op = open_in(gs, il, g0, c0);
        let hp = if pc.len() > 0 { lit_head::<O>(pc.len()) } else { Seq::<u8>::empty() };
        let ow = lit_head::<O>(k) + encs::<O>(cur);
        &&& op ==> {
            &&& raw_tail::<O>(gs, il, k, cur, g0, c0) == ow.len() && rw.skip(rw.len() - ow.len()) == ow
            &&& groups_bytes::<O>(sg) == hp + rw.take(rw.len() - ow.len()) + lit_head::<O>(cur.len()) + encs::<O>(cur)
            &&& sg.len() > 0 ==> tail_bytes_of::<O>(sg) == lit_head::<O>(cur.len()).len() + encs::<O>(cur).len()
        }
        &&& !op ==> {
            &&& groups_bytes::<O>(sg) == hp + rw
            &&& sg.len() > 0 ==> tail_bytes_of::<O>(sg) == raw_tail::<O>(gs, il, k, cur, g0, c0) + (if m.len() == 0 { hp.len() } else { 0 })
        }
    }),
{
    let pc = piece(gs, cur, g0, c0);
    let m = mid(gs, g0, c0);
    let rw = raw::<O>(gs, il, k, cur, g0, c0);
    let ow = lit_head::<O>(k) + encs::<O>(cur);
    lemma_sg_bytes::<O>(gs, il, k, cur, g0, c0);
    if open_in(gs, il, g0, c0) {
        assert(rw.skip(rw.len() - ow.len()) =~= ow);
        assert(rw.take(rw.len() - ow.len()) =~= encs::<O>(pc) + groups_bytes::<O>(m));
    } else {
        assert(rw =~= encs::<O>(pc) + groups_bytes::<O>(m));
    }
}

impl Cut {
    /// What holds between segments, but for the segment target.
    pub(crate) closed spec fn ok_at<'a, O: RleOps<'a>>(&self, r: Reader<'a, O>) -> bool {
        let gs = r.groups();
        let il = r.in_lit();
        let k = r.lit_k();
        let cur = r.lit_values();
        let g0 = self.g0@;
        let c0 = self.c0@;
        let sg = sgroups(gs, il, cur, g0, c0);
        let pc = piece(gs, cur, g0, c0);
        let hk = head_k(gs, il, k, g0, c0);
        &&& r.inv()
        &&& shape(gs, il, k, cur, g0, c0, self.k0@)
        &&& self.start == cut_bytes::<O>(gs, cur, g0, c0, self.k0@).len()
        &&& forall|i: int| 0 <= i < self.slabs@.len() ==> slab_ok::<O>(#[trigger] self.slabs@[i])
        &&& slabs_values::<O>(self.slabs@) == done(gs, cur, g0, c0, O::null())
        &&& self.len == groups_len(sg) && self.segments == groups_segments(sg)
        &&& self.lit_tail == lit_tail_of::<O>(sg)
        &&& self.tail_len == raw_tail::<O>(gs, il, k, cur, g0, c0)
        &&& self.last_lit_count == hk
        &&& self.lit_count == (if hk == 0 { pc.len() } else if open_in(gs, il, g0, c0) { cur.len() } else { hk })
        &&& self.pending_header == (if hk == 0 { 0 } else { pc.len() })
    }

    /// What holds between segments: the slab is under the target.
    pub(crate) open spec fn ok<'a, O: RleOps<'a>>(&self, r: Reader<'a, O>, target: usize) -> bool {
        self.ok_at(r) && self.seg_count() < target
    }

    pub(crate) closed spec fn seg_count(&self) -> nat {
        self.segments as nat
    }

    /// The segments of the slab being cut.
    #[inline(always)]
    pub(crate) fn seg(&self) -> (n: usize)
        ensures n == self.seg_count(),
    {
        self.segments
    }

    /// The slabs cut.
    pub(crate) fn into_slabs(self) -> (v: Vec<Slab>)
        ensures v@ == self.done_slabs(),
    {
        self.slabs
    }

    /// With nothing left to cut, the slabs hold every value read.
    pub(crate) proof fn lemma_finished<'a, O: RleOps<'a>>(&self, r: Reader<'a, O>)
        requires self.ok_at(r), !r.in_lit(), self.seg_count() == 0,
        ensures
            forall|i: int| 0 <= i < self.done_slabs().len() ==> slab_ok::<O>(#[trigger] self.done_slabs()[i]),
            slabs_values::<O>(self.done_slabs()) == groups_values(r.groups(), O::null()),
    {
        let gs = r.groups();
        let cur = r.lit_values();
        let sg = sgroups(gs, false, cur, self.g0@, self.c0@);
        r.lemma_inv();
        lemma_vals(gs, false, r.lit_k(), cur, self.g0@, self.c0@, self.k0@, O::null());
        lemma_canon::<O>(gs, false, r.lit_k(), cur, self.g0@, self.c0@, self.k0@);
        if sg.len() > 0 {
            assert(group_ok::<O>(sg[sg.len() - 1]));
            assert(sg =~= sg.drop_last().push(sg.last()));
            lemma_push::<O>(sg.drop_last(), sg.last());
        }
        assert(sg =~= Seq::<Group<O::V>>::empty());
        assert(groups_values(sg, O::null()) =~= Seq::<O::V>::empty());
        assert(cur =~= Seq::<O::V>::empty());
        assert(groups_values(gs, O::null()) + cur =~= groups_values(gs, O::null()));
        assert(done(gs, cur, self.g0@, self.c0@, O::null()) + Seq::<O::V>::empty() =~= done(gs, cur, self.g0@, self.c0@, O::null()));
    }

    /// The slabs cut so far.
    pub(crate) closed spec fn done_slabs(&self) -> Seq<Slab> {
        self.slabs@
    }

    pub(crate) fn slabs_completed(&self) -> (n: usize)
        ensures n == self.done_slabs().len(),
    {
        self.slabs.len()
    }

    /// The length of slab `i` cut so far.
    pub(crate) fn completed_slab_len(&self, i: usize) -> (n: usize)
        requires i < self.done_slabs().len(),
        ensures n == slab_len(self.done_slabs()[i as int]),
    {
        slab_len_of(&self.slabs[i])
    }

    /// Count the segment `seg` the reader read (from `r0` to `r1`) into
    /// the slab; yield its run, if it has values.
    #[inline(always)]
    pub(crate) fn track<'a, O: RleOps<'a>>(&mut self, seg: Seg<O::V>, Ghost(r0): Ghost<Reader<'a, O>>, Ghost(r1): Ghost<Reader<'a, O>>) -> (out: Option<crate::Run<O::V>>)
        requires old(self).ok_at(r0), next_post(r0, r1, Ok(Some(seg))), old(self).seg_count() < usize::MAX,
        ensures
            final(self).ok_at(r1), final(self).done_slabs() == old(self).done_slabs(),
            final(self).seg_count() <= old(self).seg_count() + 1,
            final(self).seg_count() == old(self).seg_count() || !(r1.in_lit() && r1.lit_values().len() == 0),
    {
        let ghost gs = r0.groups();
        let ghost k = r0.lit_k();
        let ghost cur = r0.lit_values();
        let ghost g0 = self.g0@;
        let ghost c0 = self.c0@;
        let ghost k0 = self.k0@;
        let ghost e = Seq::<O::V>::empty();
        proof {
            r0.lemma_inv();
            r1.lemma_inv();
        }
        match seg {
            Seg::LitHead { count, bytes } => {
                self.track_head::<O>(count, bytes, Ghost(r0), Ghost(r1));
                None
            },
            Seg::Lit { value, bytes } => {
                self.track_lit::<O>(Ghost(value), bytes, Ghost(r0), Ghost(r1));
                Some(crate::Run { count: 1, value })
            },
            Seg::Run { count, value, bytes } => {
                self.track_group::<O>(count, bytes, Ghost(r0), Ghost(r1), Ghost(Group::Run { count: count as nat, value }));
                if count > 0 { Some(crate::Run { count, value }) } else { None }
            },
            Seg::Null { count, bytes } => {
                self.track_group::<O>(count, bytes, Ghost(r0), Ghost(r1), Ghost(Group::Null { count: count as nat }));
                proof {
                    assert(group_ok::<O>(r1.groups()[r1.groups().len() - 1]));
                }
                if count > 0 { Some(crate::Run { count, value: O::get_null() }) } else { None }
            },
        }
    }

    /// No slab yet, before the reader `r` has read anything.
    pub(crate) fn new<'a, O: RleOps<'a>>(Ghost(r): Ghost<Reader<'a, O>>) -> (c: Self)
        requires r.inv(), r.groups() == Seq::<Group<O::V>>::empty(), !r.in_lit(),
        ensures c.ok_at(r), c.seg_count() == 0, c.done_slabs().len() == 0,
    {
        let c = Cut {
            slabs: Vec::new(),
            len: 0,
            segments: 0,
            lit_tail: 0,
            start: 0,
            last_lit_count: 0,
            lit_count: 0,
            pending_header: 0,
            tail_len: 0,
            g0: Ghost(0),
            c0: Ghost(0),
            k0: Ghost(0),
        };
        proof {
            let e = Seq::<Group<O::V>>::empty();
            r.lemma_inv();
            assert(r.lit_values() =~= Seq::<O::V>::empty());
            assert(e.skip(0) =~= e);
            assert(e.take(0) =~= e);
            assert(sgroups(e, false, r.lit_values(), 0, 0) =~= e);
            assert(groups_values(e, O::null()) =~= Seq::<O::V>::empty());
            assert(done(e, r.lit_values(), 0, 0, O::null()) =~= Seq::<O::V>::empty());
            assert(slabs_values::<O>(c.slabs@) =~= Seq::<O::V>::empty());
            assert(encs::<O>(Seq::<O::V>::empty()) =~= Seq::<u8>::empty());
        }
        c
    }

    /// The slab's bytes, from its input bytes `raw`: a literal split by
    /// the last cut gets a header of its own, and one split by this cut
    /// its header for the values before the cut. Returns the bytes and
    /// the length of their last group.
    #[verifier::spinoff_prover]
    #[verifier::rlimit(60)]
    fn build<'a, W: WriteOps<'a>>(&self, raw_bytes: &[u8], Ghost(gs): Ghost<Seq<Group<VOf<'a, W>>>>, Ghost(il): Ghost<bool>, Ghost(k): Ghost<nat>, Ghost(cur): Ghost<Seq<VOf<'a, W>>>) -> (res: (Vec<u8>, usize))
        requires
            build_ok::<W::R>(*self, gs, il, k, cur),
            raw_bytes@ == raw::<W::R>(gs, il, k, cur, self.g0@, self.c0@),
        ensures ({
            let sg = sgroups(gs, il, cur, self.g0@, self.c0@);
            &&& res.0@ == groups_bytes::<W::R>(sg)
            &&& res.1 == tail_bytes_of::<W::R>(sg)
        }),
    {
        hide(canonical);
        let ghost g0 = self.g0@;
        let ghost c0 = self.c0@;
        let ghost pc = piece(gs, cur, g0, c0);
        let ghost sg = sgroups(gs, il, cur, g0, c0);
        let ghost rw = raw::<W::R>(gs, il, k, cur, g0, c0);
        let ghost ow = lit_head::<W::R>(k) + encs::<W::R>(cur);
        proof {
            lemma_build_cases::<W::R>(*self, gs, il, k, cur);
        }
        let mut data: Vec<u8> = Vec::new();
        let mut tb = self.tail_len;
        let mut add_head = false;
        if self.pending_header > 0 {
            W::put_signed(-(self.pending_header as i64), &mut data);
        } else if self.lit_count > self.last_lit_count && self.last_lit_count == 0 {
            W::put_signed(-(self.lit_count as i64), &mut data);
            add_head = self.lit_tail > 0;
        }
        let hl = data.len();
        let ghost hp = data@;
        proof {
            if pc.len() == 0 {
                assert(hp =~= Seq::<u8>::empty());
            }
        }
        data.extend_from_slice(raw_bytes);
        proof {
            assert(data@ =~= hp + rw) by {
                assert forall|i: int| 0 <= i < data@.len() implies #[trigger] data@[i] == (hp + rw)[i] by {
                    if i >= hp.len() {
                        assert(cloned::<u8>(raw_bytes@[i - hp.len()], data@[i]));
                    }
                }
            }
        }
        let _total = data.len();
        if add_head {
            tb = tb + hl;
        }
        if self.lit_count < self.last_lit_count {
            let header_pos = data.len() - self.tail_len;
            let ghost d1 = data@;
            proof {
                assert(d1.skip(header_pos as int) =~= rw.skip(rw.len() - ow.len()));
                assert(lit_head::<W::R>(k).is_prefix_of(d1.skip(header_pos as int)));
            }
            let _ = W::rewrite_lit_header(&mut data, header_pos, self.lit_count);
            proof {
                assert(d1.take(header_pos as int) =~= hp + rw.take(rw.len() - ow.len()));
                assert(d1.skip(header_pos + lit_head::<W::R>(k).len()) =~= encs::<W::R>(cur));
                assert(data@ =~= groups_bytes::<W::R>(sg));
            }
            tb = data.len() - header_pos;
        }
        (data, tb)
    }

    /// Cut the slab at the reader `r`'s position `pos` in `input`.
    ///
    /// # Errors
    ///
    /// If the slab's last run is 4 GiB or more: its tail record can't
    /// hold it.
    #[verifier::spinoff_prover]
    #[verifier::rlimit(60)]
    pub(crate) fn cut_slab<'a, W: WriteOps<'a>>(&mut self, input: &[u8], pos: usize, Ghost(r): Ghost<Reader<'a, W::R>>) -> (res: Result<(), PackError>)
        requires
            old(self).ok_at(r), input@ == r.data(), pos == r.spec_pos(), old(self).seg_count() > 0,
            !(r.in_lit() && r.lit_values().len() == 0),
        ensures
            res is Ok ==> final(self).ok_at(r) && final(self).seg_count() == 0
                && final(self).done_slabs().len() == old(self).done_slabs().len() + 1,
    {
        hide(canonical);
        let ghost gs = r.groups();
        let ghost il = r.in_lit();
        let ghost k = r.lit_k();
        let ghost cur = r.lit_values();
        let ghost g0 = self.g0@;
        let ghost c0 = self.c0@;
        let ghost k0 = self.k0@;
        let ghost sg = sgroups(gs, il, cur, g0, c0);
        let ghost rw = raw::<W::R>(gs, il, k, cur, g0, c0);
        let ghost nl = <W::R as RleOps<'a>>::null();
        proof {
            r.lemma_inv();
            lemma_raw::<W::R>(gs, il, k, cur, g0, c0, k0);
            lemma_bounds::<W::R>(gs, il, k, cur, g0, c0, k0);
            lemma_canon::<W::R>(gs, il, k, cur, g0, c0, k0);
            lemma_vals(gs, il, k, cur, g0, c0, k0, nl);
            let cb = cut_bytes::<W::R>(gs, cur, g0, c0, k0);
            assert(input@.take(pos as int) == cb + rw);
            assert(input@.subrange(self.start as int, pos as int) =~= input@.take(pos as int).skip(self.start as int));
            assert((cb + rw).skip(cb.len() as int) =~= rw);
        }
        let raw_bytes = &input[self.start..pos];
        let (data, tb) = self.build::<W>(raw_bytes, Ghost(gs), Ghost(il), Ghost(k), Ghost(cur));
        if tb > 0xffff_ffff {
            return Err(over_4gib());
        }
        proof {
            lemma_slab_is::<W::R>(sg, data@);
            lemma_lit_tail_le::<W::R>(sg);
            lemma_groups_values_concat(sg, Seq::empty(), nl);
        }
        let tail = tail_rec(tb, self.lit_tail);
        let slab = new_slab(data, self.len, self.segments, tail);
        let ghost s0 = self.slabs@;
        self.slabs.push(slab);
        self.start = pos;
        self.len = 0;
        self.segments = 0;
        self.lit_tail = 0;
        self.tail_len = 0;
        self.pending_header = 0;
        self.last_lit_count = 0;
        self.lit_count = 0;
        self.g0 = Ghost(gs.len() as int);
        self.c0 = Ghost(if il { cur.len() as int } else { 0 });
        self.k0 = Ghost(k);
        proof {
            assert(self.slabs@.drop_last() =~= s0);
            assert(self.slabs@.last() == slab);
            assert forall|i: int| 0 <= i < self.slabs@.len() implies slab_ok::<W::R>(#[trigger] self.slabs@[i]) by {
                if i < s0.len() {
                    assert(self.slabs@[i] == s0[i]);
                }
            }
            lemma_cut_reset::<W::R>(gs, il, k, cur);
        }
        Ok(())
    }

    #[inline(always)]
    #[verifier::spinoff_prover]
    fn track_head<'a, O: RleOps<'a>>(&mut self, count: usize, bytes: usize, Ghost(r0): Ghost<Reader<'a, O>>, Ghost(r1): Ghost<Reader<'a, O>>)
        requires
            old(self).ok_at(r0), r0.inv(), r1.inv(), !r0.in_lit(), r1.in_lit(), r1.groups() == r0.groups(),
            r1.lit_values() == Seq::<O::V>::empty(), count >= 1, r1.lit_k() == count,
            bytes == lit_head::<O>(count as nat).len(),
        ensures
            final(self).ok_at(r1), final(self).done_slabs() == old(self).done_slabs(),
            final(self).seg_count() == old(self).seg_count(),
    {
        let ghost e = Seq::<O::V>::empty();
        proof {
            r0.lemma_inv();
            assert(r0.lit_values() =~= e);
            lemma_step_head(r0.groups(), self.g0@, self.c0@, self.k0@, count as nat);
            assert(encs::<O>(e) =~= Seq::<u8>::empty());
        }
        if self.last_lit_count < self.lit_count {
            self.pending_header = self.lit_count;
        }
        self.tail_len = bytes;
        self.last_lit_count = count;
        self.lit_count = 0;
    }

    #[inline(always)]
    #[verifier::spinoff_prover]
    fn track_lit<'a, O: RleOps<'a>>(&mut self, Ghost(value): Ghost<O::V>, bytes: usize, Ghost(r0): Ghost<Reader<'a, O>>, Ghost(r1): Ghost<Reader<'a, O>>)
        requires
            old(self).ok_at(r0), r0.inv(), r1.inv(), r0.in_lit(), bytes == O::enc(value).len(),
            old(self).seg_count() < usize::MAX,
            r1.in_lit() ==> r1.groups() == r0.groups() && r1.lit_values() == r0.lit_values().push(value)
                && r1.lit_k() == r0.lit_k(),
            !r1.in_lit() ==> r1.groups() == r0.groups().push(Group::Lit { values: r0.lit_values().push(value) })
                && r0.lit_values().len() + 1 == r0.lit_k(),
        ensures
            final(self).ok_at(r1), final(self).done_slabs() == old(self).done_slabs(),
            final(self).seg_count() == old(self).seg_count() + 1, r1.lit_values().len() > 0 || !r1.in_lit(),
    {
        let ghost gs = r0.groups();
        let ghost k = r0.lit_k();
        let ghost cur = r0.lit_values();
        let ghost g0 = self.g0@;
        let ghost c0 = self.c0@;
        let ghost k0 = self.k0@;
        proof {
            r0.lemma_inv();
            r1.lemma_inv();
            if r1.in_lit() {
                lemma_step_more::<O>(gs, k, cur, value, g0, c0, k0);
            } else {
                lemma_step_close::<O>(gs, k, cur, value, g0, c0, k0);
            }
            lemma_bounds::<O>(r1.groups(), r1.in_lit(), r1.lit_k(), r1.lit_values(), g0, c0, k0);
        }
        self.len = self.len + 1;
        self.segments = self.segments + 1;
        self.lit_tail = bytes;
        self.tail_len = self.tail_len + bytes;
        self.lit_count = self.lit_count + 1;
    }

    #[inline(always)]
    #[verifier::spinoff_prover]
    fn track_group<'a, O: RleOps<'a>>(&mut self, count: usize, bytes: usize, Ghost(r0): Ghost<Reader<'a, O>>, Ghost(r1): Ghost<Reader<'a, O>>, Ghost(g): Ghost<Group<O::V>>)
        requires
            old(self).ok_at(r0), r1.inv(), !r0.in_lit(), !r1.in_lit(), r1.groups() == r0.groups().push(g), !(g is Lit),
            count == group_len(g), bytes == group_bytes::<O>(g).len(), old(self).seg_count() < usize::MAX,
        ensures
            final(self).ok_at(r1), final(self).done_slabs() == old(self).done_slabs(),
            final(self).seg_count() == old(self).seg_count() + 1,
    {
        proof {
            self.lemma_group(r0, r1, g);
            r1.lemma_inv();
            assert(r1.lit_values() =~= Seq::<O::V>::empty());
        }
        self.len = self.len + count;
        self.segments = self.segments + 1;
        self.lit_tail = 0;
        self.tail_len = bytes;
    }

    /// A group read whole: the slab's sizes after it stay in range.
    proof fn lemma_group<'a, O: RleOps<'a>>(&self, r0: Reader<'a, O>, r1: Reader<'a, O>, g: Group<O::V>)
        requires
            self.ok_at(r0), !r0.in_lit(), !r1.in_lit(), r1.inv(), r1.groups() == r0.groups().push(g), !(g is Lit),
        ensures ({
            let e = Seq::<O::V>::empty();
            let sg0 = sgroups(r0.groups(), false, e, self.g0@, self.c0@);
            let g1 = r1.groups();
            &&& self.len + group_len(g) <= usize::MAX
            &&& shape(g1, false, 0, e, self.g0@, self.c0@, self.k0@)
            &&& cut_bytes::<O>(g1, e, self.g0@, self.c0@, self.k0@) == cut_bytes::<O>(r0.groups(), e, self.g0@, self.c0@, self.k0@)
            &&& done(g1, e, self.g0@, self.c0@, O::null()) == done(r0.groups(), e, self.g0@, self.c0@, O::null())
            &&& groups_len(sgroups(g1, false, e, self.g0@, self.c0@)) == self.len + group_len(g)
            &&& groups_segments(sgroups(g1, false, e, self.g0@, self.c0@)) == self.segments + 1
            &&& lit_tail_of::<O>(sgroups(g1, false, e, self.g0@, self.c0@)) == 0
            &&& raw_tail::<O>(g1, false, 0, e, self.g0@, self.c0@) == group_bytes::<O>(g).len()
            &&& head_k(g1, false, 0, self.g0@, self.c0@) == head_k(r0.groups(), false, 0, self.g0@, self.c0@)
            &&& piece(g1, e, self.g0@, self.c0@) == piece(r0.groups(), e, self.g0@, self.c0@)
            &&& !open_in(g1, false, self.g0@, self.c0@)
        }),
    {
        let e = Seq::<O::V>::empty();
        r0.lemma_inv();
        r1.lemma_inv();
        assert(r0.lit_values() =~= e);
        assert(r1.lit_values() =~= e);
        lemma_step_group::<O>(r0.groups(), g, self.g0@, self.c0@, self.k0@);
        lemma_bounds::<O>(r1.groups(), false, 0, e, self.g0@, self.c0@, self.k0@);
        let sg0 = sgroups(r0.groups(), false, e, self.g0@, self.c0@);
        assert(sg0.push(g).drop_last() =~= sg0);
    }
}

// ── Loading ─────────────────────────────────────────────────────────────────

/// A load in progress: the reader over the input, and the cutting of
/// what it has read into slabs of `target` segments.
pub(crate) struct Load<'a, W: WriteOps<'a>> {
    reader: Reader<'a, W::R>,
    input: &'a [u8],
    cut: Cut,
    target: usize,
}

impl<'a, W: WriteOps<'a>> Load<'a, W> {
    pub(crate) closed spec fn inv(&self) -> bool {
        &&& self.cut.ok(self.reader, self.target)
        &&& self.input@ == self.reader.data()
    }

    /// The input.
    pub(crate) closed spec fn data(&self) -> Seq<u8> {
        self.input@
    }

    /// The slabs cut so far.
    pub(crate) closed spec fn done_slabs(&self) -> Seq<Slab> {
        self.cut.done_slabs()
    }

    /// Start loading `data`, into slabs of `max_segments / 2` segments.
    pub(crate) fn new(data: &'a [u8], max_segments: usize) -> (l: Self)
        requires max_segments >= 2,
        ensures l.inv(), l.data() == data@,
    {
        let reader = Reader::new(data);
        let cut = Cut::new::<W::R>(Ghost(reader));
        Load { reader, input: data, cut, target: max_segments / 2 }
    }

    pub(crate) fn slabs_completed(&self) -> (n: usize)
        ensures n == self.done_slabs().len(),
    {
        self.cut.slabs_completed()
    }

    pub(crate) fn completed_slab_len(&self, i: usize) -> (n: usize)
        requires i < self.done_slabs().len(),
        ensures n == slab_len(self.done_slabs()[i as int]),
    {
        self.cut.completed_slab_len(i)
    }

    /// The next run, or `None` at the end of the input.
    ///
    /// # Errors
    ///
    /// If the input is not a canonical encoding, or a slab's last run is
    /// 4 GiB or more.
    #[inline]
    pub(crate) fn try_next_run(&mut self) -> (r: Result<Option<crate::Run<VOf<'a, W>>>, PackError>)
        requires old(self).inv(),
        ensures r is Ok ==> final(self).inv() && final(self).data() == old(self).data(),
    {
        loop
            invariant self.inv(), self.data() == old(self).data(),
            decreases self.reader.data().len() - self.reader.spec_pos(),
        {
            let ghost r0 = self.reader;
            proof {
                r0.lemma_inv();
            }
            let segment = match self.reader.next() {
                Ok(Some(s)) => s,
                Ok(None) => return Ok(None),
                Err(e) => return Err(e),
            };
            proof {
                self.reader.lemma_inv();
            }
            let out = self.cut.track::<W::R>(segment, Ghost(r0), Ghost(self.reader));
            if self.cut.seg() == self.target {
                let pos = self.reader.pos();
                match self.cut.cut_slab::<W>(self.input, pos, Ghost(self.reader)) {
                    Ok(()) => {},
                    Err(e) => return Err(e),
                }
            }
            if let Some(run) = out {
                return Ok(Some(run));
            }
        }
    }

    /// Read and check the rest of the input, and cut the last slab.
    ///
    /// # Errors
    ///
    /// If the input is not a canonical encoding, or a slab's last run is
    /// 4 GiB or more.
    pub(crate) fn finalize(self) -> (r: Result<Vec<Slab>, PackError>)
        requires self.inv(),
        ensures
            r matches Ok(slabs) ==> {
                &&& valid_slab::<W::R>(self.data())
                &&& forall|i: int| 0 <= i < slabs@.len() ==> slab_ok::<W::R>(#[trigger] slabs@[i])
                &&& slabs_values::<W::R>(slabs@) == slab_values::<W::R>(self.data())
            },
    {
        let mut cut = self.cut;
        let target = self.target;
        let input = self.input;
        let mut reader = self.reader;
        let ghost data = input@;
        loop
            invariant cut.ok(reader, target), input@ == reader.data(), data == input@,
            ensures
                cut.ok(reader, target), input@ == reader.data(), data == input@,
                parses::<W::R>(reader.groups(), data), !reader.in_lit(),
            decreases reader.data().len() - reader.spec_pos(),
        {
            let ghost r0 = reader;
            proof {
                r0.lemma_inv();
            }
            let segment = match reader.next() {
                Ok(Some(s)) => s,
                Ok(None) => {
                    proof {
                        reader.lemma_inv();
                        r0.lemma_inv();
                    }
                    break;
                },
                Err(e) => return Err(e),
            };
            proof {
                reader.lemma_inv();
            }
            let _ = cut.track::<W::R>(segment, Ghost(r0), Ghost(reader));
            if cut.seg() == target {
                let pos = reader.pos();
                match cut.cut_slab::<W>(input, pos, Ghost(reader)) {
                    Ok(()) => {},
                    Err(e) => return Err(e),
                }
            }
        }
        if cut.seg() > 0 {
            let pos = reader.pos();
            match cut.cut_slab::<W>(input, pos, Ghost(reader)) {
                Ok(()) => {},
                Err(e) => return Err(e),
            }
        }
        proof {
            cut.lemma_finished(reader);
            reveal(parses);
            lemma_slab::<W::R>(data);
            lemma_slab_is_parse::<W::R>(reader.groups(), data);
        }
        Ok(cut.into_slabs())
    }
}

/// A parse of a slab's bytes is the slab's.
proof fn lemma_slab_is_parse<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, data: Seq<u8>)
    requires parses::<O>(gs, data),
    ensures valid_slab::<O>(data), slab_values::<O>(data) == groups_values(gs, O::null()),
{
    reveal(parses);
    lemma_slab_is::<O>(gs, data);
}

} // verus!
