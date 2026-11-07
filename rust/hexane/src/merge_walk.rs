//! The merge walk shared by the column cursor and [`RawColumn`]: after an
//! edit rebuilds slabs `[start, end)`, merge undersized neighbours at the
//! boundaries around them.  Verified with Verus.
//!
//! The walk only decides *which* adjacent pairs to try; a [`Merger`]
//! decides whether a pair merges, and merging keeps the slabs' contents
//! (`content`).  Proved: the contents are unchanged, and the slabs
//! outside the returned range are the old ones outside the old range —
//! which is what lets the caller update its index over just that range.
//!
//! [`RawColumn`]: crate::RawColumn

#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// How to merge two adjacent slabs, and what merging keeps.
pub(crate) trait Merger<T> {
    /// What a slab holds.
    type X;

    /// The slabs' contents, one after another.
    spec fn content(s: Seq<T>) -> Seq<Self::X>;

    /// What the slabs keep between merges.
    spec fn inv(&self, s: Seq<T>) -> bool;

    /// Merge slab `a + 1` into slab `a` if they should be; returns whether
    /// they were.
    fn try_pair(&self, v: &mut Vec<T>, a: usize) -> (r: bool)
        requires
            self.inv(old(v)@),
        ensures
            self.inv(final(v)@),
            Self::content(final(v)@) == Self::content(old(v)@),
            r ==> {
                &&& a + 1 < old(v)@.len()
                &&& final(v)@.len() == old(v)@.len() - 1
                &&& final(v)@.take(a as int) == old(v)@.take(a as int)
                &&& final(v)@.skip(a + 1) == old(v)@.skip(a + 2)
            },
            !r ==> final(v)@ == old(v)@,
    ;
}

/// The walk's bookkeeping: the slabs before `start` and from `end` on are
/// the old ones, shifted left by the `drop` merges so far.
pub(crate) open spec fn framed<T>(v: Seq<T>, v0: Seq<T>, start: int, end: int, end0: int) -> bool {
    let drop = v0.len() - v.len();
    &&& 0 <= start < end <= v.len() <= v0.len()
    &&& v.take(start) == v0.take(start)
    &&& v.skip(end) == v0.skip(end + drop)
    &&& end + drop >= end0
}

/// A merge of `a` and `a + 1` inside `[start, end)` shortens the range.
proof fn lemma_inner<T>(v: Seq<T>, w: Seq<T>, v0: Seq<T>, start: int, end: int, end0: int, a: int)
    requires
        framed(v, v0, start, end, end0),
        start <= a,
        a + 1 < end,
        a + 1 < v.len(),
        w.len() == v.len() - 1,
        w.take(a) == v.take(a),
        w.skip(a + 1) == v.skip(a + 2),
    ensures
        framed(w, v0, start, end - 1, end0),
{
    assert(w.take(start) =~= w.take(a).take(start));
    assert(v.take(start) =~= v.take(a).take(start));
    assert(w.skip(end - 1) =~= w.skip(a + 1).skip(end - 1 - (a + 1)));
    assert(v.skip(end) =~= v.skip(a + 2).skip(end - (a + 2)));
}

/// A merge of the range's last slab with the one after it.
proof fn lemma_right<T>(v: Seq<T>, w: Seq<T>, v0: Seq<T>, start: int, end: int, end0: int)
    requires
        framed(v, v0, start, end, end0),
        end < v.len(),
        w.len() == v.len() - 1,
        w.take(end - 1) == v.take(end - 1),
        w.skip(end) == v.skip(end + 1),
    ensures
        framed(w, v0, start, end, end0),
{
    assert(w.take(start) =~= w.take(end - 1).take(start));
    assert(v.take(start) =~= v.take(end - 1).take(start));
    let drop = v0.len() - v.len();
    assert(v.skip(end + 1) =~= v.skip(end).skip(1));
    assert(v0.skip(end + drop + 1) =~= v0.skip(end + drop).skip(1));
}

/// A merge of the slab before the range with the range's first.
proof fn lemma_left<T>(v: Seq<T>, w: Seq<T>, v0: Seq<T>, start: int, end: int, end0: int)
    requires
        framed(v, v0, start, end, end0),
        start > 0,
        w.len() == v.len() - 1,
        w.take(start - 1) == v.take(start - 1),
        w.skip(start) == v.skip(start + 1),
    ensures
        framed(w, v0, start - 1, end - 1, end0),
{
    assert(v.take(start - 1) =~= v.take(start).take(start - 1));
    assert(v0.take(start - 1) =~= v0.take(start).take(start - 1));
    assert(w.skip(end - 1) =~= w.skip(start).skip(end - 1 - start));
    assert(v.skip(end) =~= v.skip(start + 1).skip(end - (start + 1)));
}

/// Walk the four slab boundaries around the rebuilt range `[start, end)`
/// and merge undersized neighbours.  Returns the range after merging,
/// and whether anything merged.
///
/// Only three slabs of a rebuilt range can be undersized, so only the
/// pairs around them are worth testing — a splice that spills a thousand
/// slabs should not walk them.
///
///   [ edited | spill | spill | … | spill | partial ]
///
/// The spills in the middle were cut at the segment budget, so they are
/// full by construction. What can be short is the *first* slab (a delete
/// shrank the one being edited), the *last spill* (it holds whatever was
/// left over), and the *last* slab (one a cascading delete only partly
/// consumed).
///
/// Right end first: merging there shortens the range, and the left end's
/// indices do not move.
pub(crate) fn merge_around<T, M: Merger<T>>(m: &M, v: &mut Vec<T>, start: usize, end: usize) -> (r: (
    usize,
    usize,
    bool,
))
    requires
        start < end <= old(v)@.len(),
        m.inv(old(v)@),
    ensures
        m.inv(final(v)@),
        M::content(final(v)@) == M::content(old(v)@),
        r.0 <= start,
        framed(final(v)@, old(v)@, r.0 as int, r.1 as int, end as int),
        !r.2 ==> final(v)@ == old(v)@ && r.0 == start && r.1 == end,
{
    let ghost v0 = v@;
    let ghost c0 = M::content(v@);
    let ghost end0 = end as int;
    let ghost start0 = start;
    let mut start = start;
    let mut end = end;
    let mut merged = false;
    proof {
        assert(v@.take(start as int) =~= v0.take(start as int));
        assert(v@.skip(end as int) =~= v0.skip(end as int));
    }
    if end - start >= 3 {
        let ghost w0 = v@;
        if m.try_pair(v, end - 3) {
            proof {
                lemma_inner(w0, v@, v0, start as int, end as int, end0, end - 3);
            }
            end -= 1;
            merged = true;
        }
    }
    if end - start >= 2 {
        let ghost w0 = v@;
        if m.try_pair(v, end - 2) {
            proof {
                lemma_inner(w0, v@, v0, start as int, end as int, end0, end - 2);
            }
            end -= 1;
            merged = true;
        }
    }
    // external right — keep merging outward
    loop
        invariant
            framed(v@, v0, start as int, end as int, end0),
            start == start0,
            m.inv(v@),
            M::content(v@) == c0,
            !merged ==> v@ == v0 && end == end0,
        decreases v@.len(),
    {
        let ghost w0 = v@;
        if !m.try_pair(v, end - 1) {
            break;
        }
        proof {
            lemma_right(w0, v@, v0, start as int, end as int, end0);
        }
        merged = true;
    }
    // the edited slab against the first slab that follows it
    if end - start >= 2 {
        let ghost w0 = v@;
        if m.try_pair(v, start) {
            proof {
                lemma_inner(w0, v@, v0, start as int, end as int, end0, start as int);
            }
            end -= 1;
            merged = true;
        }
    }
    // external left — keep merging outward
    while start > 0
        invariant
            framed(v@, v0, start as int, end as int, end0),
            start <= start0,
            m.inv(v@),
            M::content(v@) == c0,
            !merged ==> v@ == v0 && start == start0 && end == end0,
        decreases start,
    {
        let ghost w0 = v@;
        if !m.try_pair(v, start - 1) {
            break;
        }
        proof {
            lemma_left(w0, v@, v0, start as int, end as int, end0);
        }
        start -= 1;
        end -= 1;
        merged = true;
    }
    (start, end, merged)
}

} // verus!

#[cfg(test)]
mod tests {
    use super::*;

    /// Segment counts, merged by `Column`'s rule: either side under `min`
    /// and the two fit `max`. (Outside `verus!`, so its spec items are
    /// placeholders; the tests check the walk's choices, not its proofs.)
    struct Segs {
        min: usize,
        max: usize,
    }

    impl Merger<usize> for Segs {
        type X = ();

        fn try_pair(&self, v: &mut Vec<usize>, a: usize) -> bool {
            let b = a + 1;
            if b >= v.len() {
                return false;
            }
            if (v[a] < self.min || v[b] < self.min) && v[a] + v[b] <= self.max {
                v[a] += v[b];
                v.remove(b);
                return true;
            }
            false
        }
    }

    fn walk(segs: &mut Vec<usize>, range: std::ops::Range<usize>, min: usize, max: usize) {
        merge_around(&Segs { min, max }, segs, range.start, range.end);
    }

    /// The undersized slab is in the middle of the range, which is where
    /// a rebuild that spills and then cascades a delete puts it.
    #[test]
    fn merges_an_interior_pair() {
        let mut segs = vec![8, 4, 1, 8];
        walk(&mut segs, 0..4, 2, 8);
        assert_eq!(segs, vec![8, 5, 8]);
    }

    /// The last spill and the partly-consumed slab both short: the merge
    /// walks from the right end, so the second pair sees the first's
    /// result.
    #[test]
    fn merges_both_short_slabs_at_the_right_end() {
        let mut segs = vec![8, 4, 1, 1];
        walk(&mut segs, 0..4, 2, 8);
        assert_eq!(segs, vec![8, 6]);
    }

    /// Merging outward, past the right end of the range.
    #[test]
    fn merges_past_the_right_end() {
        let mut segs = vec![3, 1, 3];
        walk(&mut segs, 1..2, 2, 8);
        assert_eq!(segs, vec![3, 4]);
    }

    /// And past the left end.
    #[test]
    fn merges_past_the_left_end() {
        let mut segs = vec![1, 3, 5];
        walk(&mut segs, 1..2, 2, 8);
        assert_eq!(segs, vec![4, 5]);
    }

    /// Neighbours too big to absorb an undersized slab are left alone —
    /// merging is bounded by the budget, not obligatory.
    #[test]
    fn leaves_a_pair_that_would_overflow() {
        let mut segs = vec![8, 1, 8];
        walk(&mut segs, 1..2, 2, 8);
        assert_eq!(segs, vec![8, 1, 8]);
    }
}
