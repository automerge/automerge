//! Canonical bool encodings are unique, verified with Verus.
//!
//! A bool slab is a list of run counts, alternating `false` and `true`
//! from `false`, canonical when only the first count may be zero (and not
//! as the last). `bool_wire::lemma_unique` shows that the bytes determine
//! the counts; this module shows that the values do
//! (`lemma_counts_unique`): two canonical lists with the same values end
//! in runs of the same length, or the shorter one's run before would put
//! the other value inside the longer one's. So two valid bool slabs with
//! the same values are the same bytes (`lemma_bool_slab_unique`), and a
//! bool column that loads saves back to the bytes it was loaded from.

#[cfg(verus_keep_ghost)]
use crate::bool_wire::{
    bool_counts, bool_parses, bool_valid, bool_values, counts_len, counts_ok, counts_values,
    lemma_run_values, lemma_values_len, run_value, CountOps,
};
use vstd::prelude::*;

verus! {

/// Every count after the first is positive (canonical, but for the last).
pub(crate) open spec fn later_positive(cs: Seq<u64>) -> bool {
    forall|i: int| 1 <= i < cs.len() ==> #[trigger] cs[i] > 0
}

/// Counts' values: those of all but the last count, then the last run.
proof fn lemma_values_last(cs: Seq<u64>)
    requires
        cs.len() > 0,
    ensures
        counts_values(cs) == counts_values(cs.drop_last()) + Seq::new(
            cs.last() as nat,
            |_j: int| run_value(cs.len() - 1),
        ),
        counts_values(cs).len() == counts_values(cs.drop_last()).len() + cs.last(),
{
}

/// Where the last run is nonempty, the last value is its value.
proof fn lemma_last_value(cs: Seq<u64>)
    requires
        cs.len() > 0,
        cs.last() > 0,
    ensures
        counts_values(cs).len() > 0,
        counts_values(cs).last() == run_value(cs.len() - 1),
{
    lemma_values_last(cs);
}

/// One count against three or more, with the same parity: the longer list
/// holds a `true`, the single count only `false`s.
proof fn lemma_one_against_more(a: Seq<u64>, b: Seq<u64>)
    requires
        a.len() == 1,
        b.len() >= 3,
        later_positive(b),
        counts_values(a) == counts_values(b),
    ensures
        false,
{
    lemma_run_values(b, 1, 0);
    let q = (counts_len(b.take(1)) + 0) as int;
    assert(b.take(1).drop_last() =~= Seq::<u64>::empty());
    lemma_values_last(a);
    assert(a.drop_last() =~= Seq::<u64>::empty());
    assert(counts_values(a)[q] == run_value(0));
}

/// Two lists whose last runs end the same values cannot have a shorter
/// last run in the first: the value just before it would lie in the second
/// list's last run, and differ from it.
proof fn lemma_shorter_last(a: Seq<u64>, b: Seq<u64>)
    requires
        a.len() >= 2,
        b.len() >= 2,
        a.len() % 2 == b.len() % 2,
        later_positive(a),
        later_positive(b),
        counts_values(a) == counts_values(b),
        a.last() < b.last(),
    ensures
        false,
{
    lemma_values_last(a);
    lemma_values_last(b);
    let da = a.drop_last();
    let ma = counts_values(da).len();
    let n = counts_values(a).len();
    let p = ma - 1;
    assert(ma >= 1);
    // the run before `a`'s last is nonempty: it ends at `p`
    lemma_values_len(da);
    assert(da.last() > 0) by {
        if a.len() == 2 {
            assert(da.drop_last() =~= Seq::<u64>::empty());
            assert(counts_len(da) == counts_len(da.drop_last()) + da.last());
            assert(counts_len(da.drop_last()) == 0);
        } else {
            assert(da.last() == a[a.len() - 2]);
            assert(a[a.len() - 2] > 0);
        }
    }
    lemma_last_value(da);
    assert(counts_values(a)[p] == counts_values(da)[p]);
    assert(counts_values(a)[p] == run_value(a.len() - 2));
    // in `b`, `p` lies in the last run
    let mb = counts_values(b.drop_last()).len();
    assert(mb <= p < n);
    assert(counts_values(b)[p] == run_value(b.len() - 1));
}

/// Lists of counts, positive after the first, of the same parity and with
/// the same values, are the same.
proof fn lemma_counts_unique_from(a: Seq<u64>, b: Seq<u64>)
    requires
        a.len() > 0,
        b.len() > 0,
        a.len() % 2 == b.len() % 2,
        later_positive(a),
        later_positive(b),
        counts_values(a) == counts_values(b),
    ensures
        a == b,
    decreases a.len() + b.len(),
{
    if a.len() == 1 && b.len() == 1 {
        lemma_values_last(a);
        lemma_values_last(b);
        assert(a.drop_last() =~= Seq::<u64>::empty());
        assert(b.drop_last() =~= Seq::<u64>::empty());
        assert(a =~= b);
    } else if a.len() == 1 {
        lemma_one_against_more(a, b);
    } else if b.len() == 1 {
        lemma_one_against_more(b, a);
    } else {
        if a.last() < b.last() {
            lemma_shorter_last(a, b);
        } else if b.last() < a.last() {
            lemma_shorter_last(b, a);
        }
        lemma_values_last(a);
        lemma_values_last(b);
        let (da, db) = (a.drop_last(), b.drop_last());
        let m = counts_values(da).len();
        assert(counts_values(da) =~= counts_values(a).take(m as int));
        assert(counts_values(db) =~= counts_values(b).take(m as int));
        assert forall|i: int| 1 <= i < da.len() implies #[trigger] da[i] > 0 by {
            assert(da[i] == a[i]);
        }
        assert forall|i: int| 1 <= i < db.len() implies #[trigger] db[i] > 0 by {
            assert(db[i] == b[i]);
        }
        lemma_counts_unique_from(da, db);
        assert(a =~= da.push(a.last()));
        assert(b =~= db.push(b.last()));
    }
}

/// The values determine canonical counts: two canonical lists of counts
/// with the same values are the same.
pub(crate) proof fn lemma_counts_unique(a: Seq<u64>, b: Seq<u64>)
    requires
        counts_ok(a),
        counts_ok(b),
        counts_values(a) == counts_values(b),
    ensures
        a == b,
{
    if a.len() == 0 || b.len() == 0 {
        // a canonical nonempty list ends in a nonempty run
        if a.len() > 0 {
            lemma_last_value(a);
        }
        if b.len() > 0 {
            lemma_last_value(b);
        }
        assert(a =~= b);
    } else {
        lemma_last_value(a);
        lemma_last_value(b);
        // the last value tells the parity of the number of runs
        assert(run_value(a.len() - 1) == run_value(b.len() - 1));
        lemma_counts_unique_from(a, b);
    }
}

/// Two valid bool slabs with the same values are the same bytes.
pub(crate) proof fn lemma_bool_slab_unique<K: CountOps>(x: Seq<u8>, y: Seq<u8>)
    requires
        bool_valid::<K>(x),
        bool_valid::<K>(y),
        bool_values::<K>(x) == bool_values::<K>(y),
    ensures
        x == y,
{
    let (cx, cy) = (bool_counts::<K>(x), bool_counts::<K>(y));
    assert(bool_parses::<K>(cx, x) && bool_parses::<K>(cy, y));
    lemma_counts_unique(cx, cy);
}

} // verus!
