//! Canonical RLE encodings are unique, verified with Verus.
//!
//! `model::lemma_groups_unique` shows that a slab's bytes determine its
//! groups. This module shows the other half: the values determine the
//! canonical groups (`lemma_canonical_unique`). Null runs are maximal (two
//! never meet); a repeat run covers a whole stretch of equal values (no
//! neighbour may carry its value on); and a literal ends exactly where the
//! values reach a null, two equal values, or their end. So two valid
//! slabs with the same values are the same bytes (`lemma_slab_unique`): a
//! column that loads saves back to the bytes it was loaded from, since
//! save writes a valid slab of the column's values.
//!
//! Trusted: what `RleOps` assumes of the value type (`axiom_null`: in a
//! nullable column, null is one value).

#[cfg(verus_keep_ghost)]
use crate::rle::dec::{
    lemma_group_values_len, lemma_groups_values_concat, slab_groups, slab_values, valid_slab,
};
#[cfg(verus_keep_ghost)]
use crate::rle::model::{
    adjacent_ok, canonical, group_len, group_ok, group_values, groups_bytes, groups_values, parses,
    Group, RleOps,
};
use vstd::prelude::*;

verus! {

/// A group's last value.
pub(crate) open spec fn last_val<V>(g: Group<V>, null: V) -> V {
    group_values(g, null).last()
}

/// Groups' values, split after the first group.
proof fn lemma_values_first<V>(gs: Seq<Group<V>>, null: V)
    requires
        gs.len() > 0,
    ensures
        groups_values(gs, null) == group_values(gs[0], null) + groups_values(gs.skip(1), null),
{
    assert(gs =~= seq![gs[0]] + gs.skip(1));
    lemma_groups_values_concat(seq![gs[0]], gs.skip(1), null);
    assert(seq![gs[0]].drop_last() =~= Seq::<Group<V>>::empty());
    reveal_with_fuel(groups_values, 2);
    assert(groups_values(seq![gs[0]], null) =~= group_values(gs[0], null));
}

/// A canonical sequence's first group, and what follows it: the end of
/// the values, or the next group's first value (twice, for a run).
proof fn lemma_after_first<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>)
    requires
        canonical::<O>(gs),
        gs.len() > 0,
    ensures
        ({
            let vs = groups_values(gs, O::null());
            let g = group_values(gs[0], O::null());
            let k = group_len(gs[0]) as int;
            &&& 1 <= k <= vs.len()
            &&& g.len() == k
            &&& forall|i: int| 0 <= i < k ==> vs[i] == #[trigger] g[i]
            &&& gs.len() == 1 ==> vs.len() == k
            &&& gs.len() > 1 ==> k < vs.len() && vs[k] == group_values(gs[1], O::null())[0]
            &&& gs.len() > 1 && gs[1] is Run ==> k + 1 < vs.len() && vs[k + 1] == vs[k]
            &&& groups_values(gs.skip(1), O::null()) == vs.skip(k)
            &&& canonical::<O>(gs.skip(1))
        }),
{
    let null = O::null();
    let vs = groups_values(gs, null);
    lemma_values_first(gs, null);
    lemma_group_values_len(gs[0], null);
    assert(group_ok::<O>(gs[0]));
    let rest = gs.skip(1);
    assert(vs.skip(group_len(gs[0]) as int) =~= groups_values(rest, null));
    assert forall|i: int| 0 <= i < rest.len() implies group_ok::<O>(#[trigger] rest[i]) by {
        assert(rest[i] == gs[i + 1]);
    }
    assert forall|i: int| 0 < i < rest.len() implies adjacent_ok(rest[i - 1], #[trigger] rest[i]) by {
        assert(rest[i] == gs[i + 1]);
        assert(rest[i - 1] == gs[i]);
    }
    if gs.len() == 1 {
        assert(rest =~= Seq::<Group<O::V>>::empty());
        assert(groups_values(rest, null) =~= Seq::<O::V>::empty());
    } else {
        lemma_values_first(rest, null);
        assert(rest[0] == gs[1]);
        assert(group_ok::<O>(gs[1]));
        lemma_group_values_len(gs[1], null);
    }
}

/// Across the first group boundary of a canonical sequence the value
/// changes: the next group's first value is not the first group's last.
proof fn lemma_boundary_differs<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>)
    requires
        canonical::<O>(gs),
        gs.len() > 1,
    ensures
        group_values(gs[1], O::null())[0] != last_val(gs[0], O::null()),
        gs[0] is Null ==> !(gs[1] is Null) && !O::is_null(group_values(gs[1], O::null())[0]),
{
    let null = O::null();
    assert(adjacent_ok(gs[0], gs[1]));
    assert(group_ok::<O>(gs[0]) && group_ok::<O>(gs[1]));
    lemma_group_values_len(gs[0], null);
    lemma_group_values_len(gs[1], null);
    let x = last_val(gs[0], null);
    let y = group_values(gs[1], null)[0];
    // a null meets a value: they differ, as null is one value
    O::axiom_null(x);
    O::axiom_null(y);
    O::axiom_null(null);
    match gs[0] {
        Group::Run { count, value } => {
            assert(x == value);
        },
        Group::Lit { values } => {
            assert(x == values.last());
            assert(!O::is_null(values[values.len() - 1]));
        },
        Group::Null { count } => {
            assert(x == null);
        },
    }
    match gs[1] {
        Group::Run { count, value } => {
            assert(y == value);
        },
        Group::Lit { values } => {
            assert(y == values[0]);
        },
        Group::Null { count } => {
            assert(y == null);
        },
    }
}

/// Every value of a group that isn't a null run is non-null.
proof fn lemma_non_null<'a, O: RleOps<'a>>(g: Group<O::V>, i: int)
    requires
        group_ok::<O>(g),
        !(g is Null),
        0 <= i < group_len(g),
    ensures
        !O::is_null(group_values(g, O::null())[i]),
{
}

/// Two canonical sequences with the same values cannot start with a null
/// run of `c` and one of `d > c` nulls.
proof fn lemma_null_runs<'a, O: RleOps<'a>>(a: Seq<Group<O::V>>, b: Seq<Group<O::V>>, c: nat, d: nat)
    requires
        canonical::<O>(a),
        canonical::<O>(b),
        a.len() > 0 && b.len() > 0,
        groups_values(a, O::null()) == groups_values(b, O::null()),
        a[0] == (Group::<O::V>::Null { count: c }),
        b[0] == (Group::<O::V>::Null { count: d }),
        c < d,
    ensures
        false,
{
    let null = O::null();
    lemma_after_first::<O>(a);
    lemma_after_first::<O>(b);
    let vs = groups_values(a, null);
    // in `b` the value at `c` is a null; in `a` it starts the next group,
    // which is not a null run
    assert(vs[c as int] == group_values(b[0], null)[c as int]);
    lemma_boundary_differs::<O>(a);
    assert(group_ok::<O>(b[0]));
    O::axiom_null(null);
}

/// A run of `c` copies of `x` and a run of `d > c` copies of it cannot
/// both start canonical sequences with the same values.
proof fn lemma_runs<'a, O: RleOps<'a>>(a: Seq<Group<O::V>>, b: Seq<Group<O::V>>, c: nat, d: nat, x: O::V)
    requires
        canonical::<O>(a),
        canonical::<O>(b),
        a.len() > 0 && b.len() > 0,
        groups_values(a, O::null()) == groups_values(b, O::null()),
        a[0] == (Group::<O::V>::Run { count: c, value: x }),
        b[0] == (Group::<O::V>::Run { count: d, value: x }),
        c < d,
    ensures
        false,
{
    let null = O::null();
    lemma_after_first::<O>(a);
    lemma_after_first::<O>(b);
    let vs = groups_values(a, null);
    assert(vs[c as int] == group_values(b[0], null)[c as int]);
    lemma_boundary_differs::<O>(a);
    assert(last_val(a[0], null) == x);
}

/// A run cannot start one canonical sequence and a literal another with
/// the same values: the run's second value repeats its first, the
/// literal's doesn't.
proof fn lemma_run_lit<'a, O: RleOps<'a>>(a: Seq<Group<O::V>>, b: Seq<Group<O::V>>)
    requires
        canonical::<O>(a),
        canonical::<O>(b),
        a.len() > 0 && b.len() > 0,
        groups_values(a, O::null()) == groups_values(b, O::null()),
        a[0] is Run,
        b[0] is Lit,
    ensures
        false,
{
    let null = O::null();
    lemma_after_first::<O>(a);
    lemma_after_first::<O>(b);
    let vs = groups_values(a, null);
    assert(group_ok::<O>(a[0]) && group_ok::<O>(b[0]));
    // the run's first two values are equal
    if let Group::Run { count, value } = a[0] {
        let g = group_values(a[0], null);
        assert(vs[0] == g[0] && vs[1] == g[1]);
        assert(g[0] == value && g[1] == value);
    }
    assert(vs[1] == vs[0]);
    if let Group::Lit { values } = b[0] {
        if values.len() >= 2 {
            assert(vs[1] == values[1] && vs[0] == values[0]);
        } else {
            lemma_boundary_differs::<O>(b);
            assert(last_val(b[0], null) == values[0]);
        }
    }
}

/// A literal of `l` values and a literal of `m > l` values cannot both
/// start canonical sequences with the same values: where the shorter one
/// ends, the values reach a null or two equal values, which the longer one
/// does not hold there.
proof fn lemma_lits<'a, O: RleOps<'a>>(a: Seq<Group<O::V>>, b: Seq<Group<O::V>>, va: Seq<O::V>, vb: Seq<O::V>)
    requires
        canonical::<O>(a),
        canonical::<O>(b),
        a.len() > 0 && b.len() > 0,
        groups_values(a, O::null()) == groups_values(b, O::null()),
        a[0] == (Group::<O::V>::Lit { values: va }),
        b[0] == (Group::<O::V>::Lit { values: vb }),
        va.len() < vb.len(),
    ensures
        false,
{
    let null = O::null();
    lemma_after_first::<O>(a);
    lemma_after_first::<O>(b);
    let vs = groups_values(a, null);
    let l = va.len() as int;
    assert(group_ok::<O>(a[0]) && group_ok::<O>(b[0]));
    assert(vs[l] == vb[l]);
    lemma_boundary_differs::<O>(a);
    assert(adjacent_ok(a[0], a[1]));
    O::axiom_null(null);
    O::axiom_null(vb[l]);
    match a[1] {
        Group::Null { count } => {
            // `a` has a null at `l`, `b` a value
            assert(group_ok::<O>(a[1]));
        },
        Group::Run { count, value } => {
            // `a` has two equal values at `l`; `b` doesn't
            assert(vs[l + 1] == vs[l]);
            if l + 1 < vb.len() {
                assert(vs[l + 1] == vb[l + 1]);
            } else {
                lemma_boundary_differs::<O>(b);
                assert(last_val(b[0], null) == vb[l]);
            }
        },
        Group::Lit { .. } => {},
    }
}

/// Two canonical sequences with the same values start with the same group.
proof fn lemma_first_group<'a, O: RleOps<'a>>(a: Seq<Group<O::V>>, b: Seq<Group<O::V>>)
    requires
        canonical::<O>(a),
        canonical::<O>(b),
        a.len() > 0 && b.len() > 0,
        groups_values(a, O::null()) == groups_values(b, O::null()),
    ensures
        a[0] == b[0],
{
    let null = O::null();
    lemma_after_first::<O>(a);
    lemma_after_first::<O>(b);
    let vs = groups_values(a, null);
    assert(group_ok::<O>(a[0]) && group_ok::<O>(b[0]));
    assert(vs[0] == group_values(a[0], null)[0]);
    assert(vs[0] == group_values(b[0], null)[0]);
    O::axiom_null(null);
    O::axiom_null(vs[0]);
    match (a[0], b[0]) {
        (Group::Null { count: c }, Group::Null { count: d }) => {
            if c < d {
                lemma_null_runs::<O>(a, b, c, d);
            } else if d < c {
                lemma_null_runs::<O>(b, a, d, c);
            }
        },
        (Group::Run { count: c, value: x }, Group::Run { count: d, value: y }) => {
            assert(x == y);
            if c < d {
                lemma_runs::<O>(a, b, c, d, x);
            } else if d < c {
                lemma_runs::<O>(b, a, d, c, x);
            }
        },
        (Group::Lit { values: va }, Group::Lit { values: vb }) => {
            if va.len() < vb.len() {
                lemma_lits::<O>(a, b, va, vb);
            } else if vb.len() < va.len() {
                lemma_lits::<O>(b, a, vb, va);
            }
            assert forall|i: int| 0 <= i < va.len() implies va[i] == vb[i] by {
                assert(vs[i] == va[i] && vs[i] == vb[i]);
            }
            assert(va =~= vb);
        },
        (Group::Run { .. }, Group::Lit { .. }) => {
            lemma_run_lit::<O>(a, b);
        },
        (Group::Lit { .. }, Group::Run { .. }) => {
            lemma_run_lit::<O>(b, a);
        },
        (Group::Null { .. }, _) => {
            lemma_non_null::<O>(b[0], 0);
        },
        (_, Group::Null { .. }) => {
            lemma_non_null::<O>(a[0], 0);
        },
    }
}

/// The values determine the canonical groups: two canonical sequences of
/// groups with the same values are the same.
pub(crate) proof fn lemma_canonical_unique<'a, O: RleOps<'a>>(a: Seq<Group<O::V>>, b: Seq<Group<O::V>>)
    requires
        canonical::<O>(a),
        canonical::<O>(b),
        groups_values(a, O::null()) == groups_values(b, O::null()),
    ensures
        a == b,
    decreases a.len(),
{
    let null = O::null();
    if a.len() == 0 || b.len() == 0 {
        // a canonical group holds a value, so no values means no groups
        if a.len() > 0 {
            lemma_after_first::<O>(a);
        }
        if b.len() > 0 {
            lemma_after_first::<O>(b);
        }
        assert(a =~= b);
    } else {
        lemma_first_group::<O>(a, b);
        lemma_after_first::<O>(a);
        lemma_after_first::<O>(b);
        lemma_canonical_unique::<O>(a.skip(1), b.skip(1));
        assert(a =~= seq![a[0]] + a.skip(1));
        assert(b =~= seq![b[0]] + b.skip(1));
    }
}

/// Two valid RLE slabs with the same values are the same bytes. So a
/// column that loads (whose bytes are a valid slab of its values) saves
/// back to those bytes (save writes a valid slab of the column's values).
pub(crate) proof fn lemma_slab_unique<'a, O: RleOps<'a>>(x: Seq<u8>, y: Seq<u8>)
    requires
        valid_slab::<O>(x),
        valid_slab::<O>(y),
        slab_values::<O>(x) == slab_values::<O>(y),
    ensures
        x == y,
{
    let gx = slab_groups::<O>(x);
    let gy = slab_groups::<O>(y);
    reveal(parses);
    lemma_canonical_unique::<O>(gx, gy);
}

} // verus!
