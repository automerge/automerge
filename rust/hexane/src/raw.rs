//! Raw-byte arena column.
//!
//! A [`RawColumn`] is conceptually a giant `Vec<u8>` that supports O(log S)
//! random splices at any byte offset — where S is the number of slabs, kept
//! bounded by [`RawColumn::with_max_segments`].  Unlike [`Column`](crate::Column)
//! it stores no per-item encoding: the bytes you splice in are the bytes that
//! come out.
//!
//! The column does **not** know where logical values begin and end.  That
//! responsibility belongs to the caller (typically a paired `ValueMeta`
//! column whose prefix sum gives byte offsets into the raw arena).  What the
//! column *does* guarantee is that **every splice point becomes a potential
//! slab boundary, and splits only happen at splice points**.  As long as
//! callers splice at value boundaries, no value ever crosses a slab — which
//! is what makes the zero-copy [`RawColumn::get`] API safe.
//!
//! Verification (Verus): the column stands for its slabs' bytes one after
//! another (its `View`), and `wf` ties the rest to them: `bit` is their
//! Fenwick tree (`crate::fenwick`), `total_len` their count, and no slab
//! is empty.  [`try_splice_slice`](RawColumn::try_splice_slice) is proved to
//! be exactly a splice of that byte sequence, and the reads (`try_get`,
//! `iter_at`, [`RawColumnIter`]'s `take` and `skip`, `save_to`) and `load`
//! to return exactly its bytes.  [`get`](RawColumn::get) asks of a verified
//! caller the range that `try_get` takes, where it panics for another;
//! a clone is a column of the same bytes.  The promise above is proved
//! as well: `cuts` are the places where slabs meet, a splice leaves a
//! cut only where one was or at an end of the bytes it puts in
//! (`spliced_cut`), and bytes lie within one slab exactly when no cut is
//! strictly between them (`lemma_in_one_slab`).  Unverified are only the
//! `IntoIterator` plumbing of `try_splice` and `copy_ranges` (which
//! splices through the verified path).

// Verus has no specs for `Vec::is_empty`, `usize::abs_diff` or `vec![x]`.
#![allow(clippy::len_zero, clippy::manual_abs_diff, clippy::vec_init_then_push)]

use crate::column::normalize_splices;
use crate::fenwick;
#[cfg(verus_keep_ghost)]
use crate::fenwick::{bit_ok, lemma_sum_le, lemma_sum_one, lemma_sum_split, lens, sum, total};
#[cfg(verus_keep_ghost)]
use crate::merge_walk::framed;
use crate::merge_walk::{merge_around, Merger};
#[cfg(verus_keep_ghost)]
use crate::rle::writer::concat;
use crate::rle::writer::splice_bytes;
#[cfg(verus_keep_ghost)]
use crate::splices::{applied, applied_from, lemma_back_ends, lemma_back_step, normal};
use crate::splices::{wrong_columns, Normalized};
use crate::PackError;
use std::ops::Range;
#[allow(unused_imports)]
use vstd::prelude::*;
use vstd::slice::{slice_subrange, slice_to_vec};

verus! {

/// Default per-slab byte budget.  Chosen empirically via the `raw_compare`
/// bench (`benches/raw_compare.rs`): on a random-insert workload, 4 KiB sits
/// at the bottom of a shallow plateau that runs from ~3 KiB to ~5 KiB — ~5×
/// faster than 64 KiB and ~2× faster than 1 KiB.  Tune per-column via
/// [`RawColumn::with_max_segments`].
const DEFAULT_MAX_SEGMENTS: usize = 4_096;

#[derive(Debug)]
struct RawSlab {
    data: Vec<u8>,
}

impl View for RawSlab {
    type V = Seq<u8>;

    closed spec fn view(&self) -> Seq<u8> {
        self.data@
    }
}

impl Clone for RawSlab {
    fn clone(&self) -> (r: Self)
        ensures
            r@ == self@,
    {
        let r = RawSlab { data: self.data.clone() };
        assert(r.data@ =~= self.data@);
        r
    }
}

/// A byte arena with O(log S) random splices.
///
/// See the module docs for the invariant around splice points and slab
/// boundaries.
#[derive(Debug)]
pub struct RawColumn {
    slabs: Vec<RawSlab>,
    /// Fenwick tree of slab byte lengths, 1-indexed.  `bit[0]` is unused,
    /// `bit[1..=slabs.len()]` holds the tree.
    bit: Vec<usize>,
    total_len: usize,
    /// Target per-slab byte budget (not a hard cap — single blobs larger
    /// than this live in their own oversize slab).
    max_segments: usize,
}

impl Clone for RawColumn {
    /// A column of its own, with the same bytes.
    fn clone(&self) -> (r: Self)
        ensures
            r@ == self@,
            self.wf() ==> r.wf(),
            r.cuts() == self.cuts(),
    {
        let r = RawColumn {
            slabs: self.slabs.clone(),
            bit: self.bit.clone(),
            total_len: self.total_len,
            max_segments: self.max_segments,
        };
        proof {
            assert(datas(r.slabs@) =~= datas(self.slabs@));
            assert(sizes(r.slabs@) =~= sizes(self.slabs@));
            assert(r.bit@ =~= self.bit@);
            lemma_edges_same(r.slabs@, self.slabs@);
        }
        r
    }
}

// ── Model ───────────────────────────────────────────────────────────────────

/// Each slab's bytes.
spec fn datas(slabs: Seq<RawSlab>) -> Seq<Seq<u8>> {
    slabs.map_values(|s: RawSlab| s.data@)
}

/// The slabs' bytes, one after another.
spec fn flat(slabs: Seq<RawSlab>) -> Seq<u8> {
    concat(datas(slabs))
}

/// Each slab's byte count.
spec fn sizes(slabs: Seq<RawSlab>) -> Seq<nat> {
    slabs.map_values(|s: RawSlab| s.data@.len())
}

/// No slab is empty.
spec fn full(slabs: Seq<RawSlab>) -> bool {
    forall|i: int| 0 <= i < slabs.len() ==> #[trigger] slabs[i].data@.len() > 0
}

/// No slab outside `[start, end)` is empty.
spec fn full_outside(slabs: Seq<RawSlab>, start: int, end: int) -> bool {
    forall|i: int| 0 <= i < slabs.len() && !(start <= i < end) ==> #[trigger] slabs[i].data@.len() > 0
}

/// The weight function [`fenwick::build`] is given.
spec fn size_of(s: RawSlab) -> nat {
    s.data@.len()
}

/// Where slab `i` begins: how many bytes the slabs before it hold. For
/// `i` the number of slabs, where the last one ends.
spec fn edge(s: Seq<RawSlab>, i: int) -> int {
    flat(s.take(i)).len() as int
}

/// The places where slabs meet, and the two ends of their bytes.
spec fn edges(s: Seq<RawSlab>) -> ISet<int> {
    ISet::new(|x: int| exists|i: int| 0 <= i <= s.len() && x == #[trigger] edge(s, i))
}

/// Whether `x` may be a cut of a column in which `del` bytes at `index`
/// were replaced by `n`, and whose cuts were `cuts`: it is a cut that
/// was there before the splice, or one that was after it, moved as the
/// bytes after it are, or an end of the bytes put in. See
/// [`RawColumn::try_splice_slice`].
pub open spec fn spliced_cut(cuts: ISet<int>, index: int, del: int, n: int, x: int) -> bool {
    ||| x <= index && cuts.contains(x)
    ||| x >= index + n && cuts.contains(x - n + del)
    ||| x == index
    ||| x == index + n
}

proof fn lemma_concat_append<V>(a: Seq<Seq<V>>, b: Seq<Seq<V>>)
    ensures
        concat(a + b) == concat(a) + concat(b),
    decreases b.len(),
{
    if b.len() == 0 {
        assert(a + b =~= a);
        assert(concat(a) + concat(b) =~= concat(a));
    } else {
        lemma_concat_append(a, b.drop_last());
        assert((a + b).drop_last() =~= a + b.drop_last());
        assert(concat(a + b) =~= concat(a) + concat(b));
    }
}

/// `flat` splits anywhere.
proof fn lemma_flat_split(s: Seq<RawSlab>, k: int)
    requires
        0 <= k <= s.len(),
    ensures
        flat(s) == flat(s.take(k)) + flat(s.skip(k)),
{
    assert(datas(s) =~= datas(s.take(k)) + datas(s.skip(k)));
    lemma_concat_append(datas(s.take(k)), datas(s.skip(k)));
}

proof fn lemma_flat_cat(a: Seq<RawSlab>, b: Seq<RawSlab>)
    ensures
        flat(a + b) == flat(a) + flat(b),
{
    assert(datas(a + b) =~= datas(a) + datas(b));
    lemma_concat_append(datas(a), datas(b));
}

proof fn lemma_flat_unit(s: RawSlab)
    ensures
        flat(seq![s]) == s.data@,
        flat(Seq::<RawSlab>::empty()) == Seq::<u8>::empty(),
{
    let e = Seq::<Seq<u8>>::empty();
    assert(datas(seq![s]) =~= e.push(s.data@));
    assert(e.push(s.data@).drop_last() =~= e);
    assert(datas(Seq::<RawSlab>::empty()) =~= e);
    assert(concat(e) == Seq::<u8>::empty());
    assert(concat(e.push(s.data@)) == concat(e) + s.data@);
    assert(Seq::<u8>::empty() + s.data@ =~= s.data@);
}

/// Slab `k` sits between the slabs before and after it.
proof fn lemma_flat_at(s: Seq<RawSlab>, k: int)
    requires
        0 <= k < s.len(),
    ensures
        flat(s) == flat(s.take(k)) + s[k].data@ + flat(s.skip(k + 1)),
{
    lemma_flat_split(s, k);
    lemma_flat_split(s.skip(k), 1);
    assert(s.skip(k).take(1) =~= seq![s[k]]);
    assert(s.skip(k).skip(1) =~= s.skip(k + 1));
    lemma_flat_unit(s[k]);
    assert(flat(s) =~= flat(s.take(k)) + s[k].data@ + flat(s.skip(k + 1)));
}

/// The bytes before slab `k` number the sizes before it.
proof fn lemma_flat_len(s: Seq<RawSlab>, k: int)
    requires
        0 <= k <= s.len(),
    ensures
        flat(s.take(k)).len() == sum(sizes(s), 0, k),
    decreases k,
{
    if k == 0 {
        assert(s.take(0) =~= Seq::<RawSlab>::empty());
        lemma_flat_unit(s[0]);
    } else {
        lemma_flat_len(s, k - 1);
        assert(s.take(k) =~= s.take(k - 1) + seq![s[k - 1]]);
        lemma_flat_cat(s.take(k - 1), seq![s[k - 1]]);
        lemma_flat_unit(s[k - 1]);
    }
}

proof fn lemma_flat_total(s: Seq<RawSlab>)
    ensures
        flat(s).len() == total(sizes(s)),
{
    lemma_flat_len(s, s.len() as int);
    assert(s.take(s.len() as int) =~= s);
}

// ── Where slabs meet ────────────────────────────────────────────────────────

/// Each slab ends where the next begins.
proof fn lemma_edge_step(s: Seq<RawSlab>, i: int)
    requires
        0 <= i < s.len(),
    ensures
        edge(s, i + 1) == edge(s, i) + s[i].data@.len(),
{
    assert(s.take(i + 1) =~= s.take(i) + seq![s[i]]);
    lemma_flat_cat(s.take(i), seq![s[i]]);
    lemma_flat_unit(s[i]);
}

/// The first edge is where the bytes begin, the last where they end.
proof fn lemma_edge_ends(s: Seq<RawSlab>)
    ensures
        edge(s, 0) == 0,
        edge(s, s.len() as int) == flat(s).len(),
{
    lemma_flat_none(s.take(0));
    assert(s.take(s.len() as int) =~= s);
}

/// Edges come in order.
proof fn lemma_edge_le(s: Seq<RawSlab>, i: int, j: int)
    requires
        0 <= i <= j <= s.len(),
    ensures
        edge(s, i) <= edge(s, j),
    decreases j - i,
{
    if i < j {
        lemma_edge_le(s, i, j - 1);
        lemma_edge_step(s, j - 1);
    }
}

/// Slabs that are alike before `k` have the same edges up to it.
proof fn lemma_edge_prefix(s: Seq<RawSlab>, t: Seq<RawSlab>, k: int, i: int)
    requires
        0 <= i <= k,
        k <= s.len(),
        k <= t.len(),
        s.take(k) == t.take(k),
    ensures
        edge(s, i) == edge(t, i),
{
    assert(s.take(i) =~= s.take(k).take(i));
    assert(t.take(i) =~= t.take(k).take(i));
}

/// Slabs that are alike from `a` of the ones and from `b` of the others
/// on have their edges as far beyond those two.
proof fn lemma_edge_suffix(s: Seq<RawSlab>, t: Seq<RawSlab>, a: int, b: int, m: int)
    requires
        0 <= a <= s.len(),
        0 <= b <= t.len(),
        s.skip(a) == t.skip(b),
        0 <= m <= s.len() - a,
    ensures
        edge(s, a + m) - edge(s, a) == edge(t, b + m) - edge(t, b),
{
    assert(s.skip(a).len() == s.len() - a);
    assert(t.skip(b).len() == t.len() - b);
    assert(b + m <= t.len());
    lemma_flat_split(s.take(a + m), a);
    lemma_flat_split(t.take(b + m), b);
    assert(s.take(a + m).take(a) =~= s.take(a));
    assert(t.take(b + m).take(b) =~= t.take(b));
    assert(s.take(a + m).skip(a) =~= s.skip(a).take(m));
    assert(t.take(b + m).skip(b) =~= t.skip(b).take(m));
}

/// Edges are of the slabs' bytes alone.
proof fn lemma_edges_same(s: Seq<RawSlab>, t: Seq<RawSlab>)
    requires
        datas(s) == datas(t),
    ensures
        edges(s) == edges(t),
{
    assert(datas(s).len() == s.len() && datas(t).len() == t.len());
    assert forall|i: int|
        #![trigger edge(s, i)]
        #![trigger edge(t, i)]
        0 <= i <= s.len() implies edge(s, i) == edge(t, i) by {
        assert(datas(s.take(i)) =~= datas(s).take(i));
        assert(datas(t.take(i)) =~= datas(t).take(i));
    }
    assert(edges(s) =~= edges(t));
}

/// The edges of no slabs, or of one: the ends of its bytes.
proof fn lemma_edges_few(s: Seq<RawSlab>)
    requires
        s.len() <= 1,
    ensures
        forall|x: int| #[trigger] edges(s).contains(x) ==> x == 0 || x == flat(s).len(),
{
    lemma_edge_ends(s);
    assert forall|x: int| #[trigger] edges(s).contains(x) implies x == 0 || x == flat(s).len() by {
        let j = choose|j: int| 0 <= j <= s.len() && x == #[trigger] edge(s, j);
        assert(j == 0 || j == s.len());
    }
}

/// The slab that holds byte `x`, looked for from slab `k` on.
proof fn lemma_edge_find(s: Seq<RawSlab>, x: int, k: int) -> (i: int)
    requires
        0 <= k < s.len(),
        edge(s, k) <= x < edge(s, s.len() as int),
    ensures
        k <= i < s.len(),
        edge(s, i) <= x < edge(s, i + 1),
    decreases s.len() - k,
{
    if x < edge(s, k + 1) {
        k
    } else {
        lemma_edge_find(s, x, k + 1)
    }
}

/// Whether bytes `[start, end)` of the slabs lie within slab `i`.
spec fn held_by(s: Seq<RawSlab>, i: int, start: int, end: int) -> bool {
    0 <= i < s.len() && edge(s, i) <= start && end <= edge(s, i + 1)
}

/// Bytes `[start, end)` of the slabs lie within one of them exactly when
/// no edge is strictly between `start` and `end`.
proof fn lemma_edges_between(s: Seq<RawSlab>, start: int, end: int)
    requires
        0 <= start < end <= flat(s).len(),
    ensures
        (exists|i: int| #[trigger] held_by(s, i, start, end)) <==> (forall|x: int|
            start < x < end ==> !#[trigger] edges(s).contains(x)),
{
    lemma_edge_ends(s);
    if exists|i: int| #[trigger] held_by(s, i, start, end) {
        let i = choose|i: int| #[trigger] held_by(s, i, start, end);
        assert forall|x: int| start < x < end implies !#[trigger] edges(s).contains(x) by {
            if edges(s).contains(x) {
                let j = choose|j: int| 0 <= j <= s.len() && x == #[trigger] edge(s, j);
                if j <= i {
                    lemma_edge_le(s, j, i);
                } else {
                    lemma_edge_le(s, i + 1, j);
                }
            }
        }
    }
    if forall|x: int| start < x < end ==> !#[trigger] edges(s).contains(x) {
        let i = lemma_edge_find(s, start, 0);
        assert(edges(s).contains(edge(s, i + 1)));
        assert(held_by(s, i, start, end));
    }
}

/// The edges after a splice at `offset` in slab `si` of `s0` that leaves
/// slabs `si..end` of `s1` in its place: those before the slab, the ends
/// of the bytes put in (where the slab was split, it was at one of
/// them), and those after the slab, moved as the bytes after it are.
proof fn lemma_edges_at(
    s0: Seq<RawSlab>,
    s1: Seq<RawSlab>,
    si: int,
    end: int,
    offset: int,
    ld: int,
    bytes: Seq<u8>,
)
    requires
        0 <= si < s0.len(),
        0 <= offset,
        0 <= ld,
        offset + ld <= s0[si].data@.len(),
        end == si + 1 || end == si + 2,
        end <= s1.len(),
        s1.take(si) == s0.take(si),
        s1.skip(end) == s0.skip(si + 1),
        flat(s1.subrange(si, end)) == s0[si].data@.take(offset) + bytes + s0[si].data@.skip(offset + ld),
        end == si + 2 ==> s1[si].data@.len() == offset || s1[si].data@.len() == offset + bytes.len(),
    ensures
        edge(s0, si + 1) == edge(s0, si) + s0[si].data@.len(),
        edge(s1, end) == edge(s0, si + 1) - ld + bytes.len(),
        forall|x: int| #[trigger]
            edges(s1).contains(x) ==> {
                ||| x <= edge(s0, si) && edges(s0).contains(x)
                ||| x == edge(s0, si) + offset
                ||| x == edge(s0, si) + offset + bytes.len()
                ||| x >= edge(s1, end) && edges(s0).contains(x + ld - bytes.len())
            },
{
    let p = edge(s0, si);
    let n = bytes.len() as int;
    lemma_edge_step(s0, si);
    lemma_edge_prefix(s1, s0, si, si);
    // the slabs left in the slab's place hold its bytes, spliced
    lemma_flat_split(s1.take(end), si);
    assert(s1.take(end).take(si) =~= s1.take(si));
    assert(s1.take(end).skip(si) =~= s1.subrange(si, end));
    assert(edge(s1, end) == p + (offset + n + (s0[si].data@.len() - offset - ld)));
    assert(s1.skip(end).len() == s1.len() - end);
    assert(s0.skip(si + 1).len() == s0.len() - (si + 1));
    assert forall|x: int| #[trigger] edges(s1).contains(x) implies {
        ||| x <= p && edges(s0).contains(x)
        ||| x == p + offset
        ||| x == p + offset + n
        ||| x >= edge(s1, end) && edges(s0).contains(x + ld - n)
    } by {
        let j = choose|j: int| 0 <= j <= s1.len() && x == #[trigger] edge(s1, j);
        if j <= si {
            lemma_edge_prefix(s1, s0, si, j);
            lemma_edge_le(s0, j, si);
            assert(x == edge(s0, j));
            assert(edges(s0).contains(x));
        } else if j < end {
            // the slab was split, and this is where
            lemma_edge_step(s1, si);
            assert(x == p + offset || x == p + offset + n);
        } else {
            lemma_edge_suffix(s1, s0, end, si + 1, j - end);
            lemma_edge_le(s1, end, j);
            assert(x + ld - n == edge(s0, si + 1 + (j - end)));
            assert(edges(s0).contains(x + ld - n));
        }
    }
}

/// The edges after the first `del` bytes of the slabs from `start` on
/// are deleted, slabs `start..end` whole and then, if `partial`, the
/// start of slab `end`: those up to the deletion, and those after it,
/// `del` earlier.
proof fn lemma_edges_drained(
    s0: Seq<RawSlab>,
    s3: Seq<RawSlab>,
    sf: Seq<RawSlab>,
    start: int,
    end: int,
    del: int,
    partial: bool,
)
    requires
        0 <= start <= end <= s0.len(),
        s3.len() == s0.len(),
        !partial ==> s3 == s0 && edge(s0, end) - edge(s0, start) == del,
        partial ==> {
            &&& end < s0.len()
            &&& s3 == s0.update(end, s3[end])
            &&& edge(s0, end) - edge(s0, start) + (s0[end].data@.len() - s3[end].data@.len()) == del
        },
        sf.len() == s3.len() - (end - start),
        sf.take(start) == s3.take(start),
        sf.skip(start) == s3.skip(end),
    ensures
        forall|x: int| #[trigger]
            edges(sf).contains(x) ==> {
                ||| x <= edge(s0, start) && edges(s0).contains(x)
                ||| x >= edge(s0, start) && edges(s0).contains(x + del)
            },
{
    let b = edge(s0, start);
    assert(s3.take(end) =~= s0.take(end));
    assert(s3.take(start) =~= s0.take(start));
    assert forall|x: int| #[trigger] edges(sf).contains(x) implies {
        ||| x <= b && edges(s0).contains(x)
        ||| x >= b && edges(s0).contains(x + del)
    } by {
        let j = choose|j: int| 0 <= j <= sf.len() && x == #[trigger] edge(sf, j);
        if j <= start {
            lemma_edge_prefix(sf, s0, start, j);
            lemma_edge_le(s0, j, start);
            assert(x == edge(s0, j));
        } else {
            // the slab `j` was before the whole ones went
            let k = j + (end - start);
            lemma_edge_prefix(sf, s0, start, start);
            lemma_edge_suffix(sf, s3, start, end, j - start);
            lemma_edge_prefix(s3, s0, end, end);
            lemma_edge_le(s3, end, k);
            if partial {
                lemma_edge_step(s3, end);
                lemma_edge_step(s0, end);
                assert(s3.skip(end + 1) =~= s0.skip(end + 1));
                lemma_edge_suffix(s3, s0, end + 1, end + 1, k - (end + 1));
            }
            assert(x + del == edge(s0, k));
        }
    }
}

/// A splice at `offset` in the slab of `len` bytes at `p`, which leaves
/// the edges `e1` and the slabs it touched ending at `b`, and then the
/// rest of the deletion after them, which leaves `e2`: every edge left
/// is one that a splice of `del` bytes at `p + offset` for `n` may leave.
proof fn lemma_edges_spliced(
    e0: ISet<int>,
    e1: ISet<int>,
    e2: ISet<int>,
    p: int,
    len: int,
    offset: int,
    del: int,
    n: int,
    b: int,
)
    requires
        0 <= offset <= len,
        0 <= del,
        0 <= n,
        b == p + len - local_del(len, offset, del) + n,
        forall|x: int| #[trigger]
            e1.contains(x) ==> {
                ||| x <= p && e0.contains(x)
                ||| x == p + offset
                ||| x == p + offset + n
                ||| x >= b && e0.contains(x + local_del(len, offset, del) - n)
            },
        forall|x: int| #[trigger]
            e2.contains(x) ==> {
                ||| x <= b && e1.contains(x)
                ||| x >= b && e1.contains(x + (del - local_del(len, offset, del)))
            },
    ensures
        forall|x: int| #[trigger] e2.contains(x) ==> spliced_cut(e0, p + offset, del, n, x),
{
    let ld = local_del(len, offset, del);
    assert forall|x: int| #[trigger] e2.contains(x) implies spliced_cut(e0, p + offset, del, n, x) by {
        if x <= b && e1.contains(x) {
            if x >= b && e0.contains(x + ld - n) && ld == del {
                assert(x - n + del == x + ld - n);
            }
        } else {
            let y = x + (del - ld);
            assert(e1.contains(y));
            if y >= b && e0.contains(y + ld - n) {
                assert(x - n + del == y + ld - n);
            }
        }
    }
}

/// Slab `a + 1` merged into slab `a` leaves no edge that was not one.
proof fn lemma_edges_merged(s0: Seq<RawSlab>, s1: Seq<RawSlab>, a: int)
    requires
        0 <= a,
        a + 1 < s0.len(),
        s1.len() == s0.len() - 1,
        s1.take(a) == s0.take(a),
        s1.skip(a + 1) == s0.skip(a + 2),
        s1[a].data@ == s0[a].data@ + s0[a + 1].data@,
    ensures
        forall|x: int| #[trigger] edges(s1).contains(x) ==> edges(s0).contains(x),
{
    assert forall|x: int| #[trigger] edges(s1).contains(x) implies edges(s0).contains(x) by {
        let j = choose|j: int| 0 <= j <= s1.len() && x == #[trigger] edge(s1, j);
        if j <= a {
            lemma_edge_prefix(s1, s0, a, j);
            assert(x == edge(s0, j));
        } else {
            lemma_edge_prefix(s1, s0, a, a);
            lemma_edge_step(s1, a);
            lemma_edge_step(s0, a);
            lemma_edge_step(s0, a + 1);
            lemma_edge_suffix(s1, s0, a + 1, a + 2, j - (a + 1));
            assert(x == edge(s0, j + 1));
        }
    }
}

/// An empty slab dropped leaves no edge that was not one.
proof fn lemma_edges_dropped(s: Seq<RawSlab>, k: int)
    requires
        0 <= k < s.len(),
        s[k].data@.len() == 0,
    ensures
        forall|x: int| #[trigger] edges(s.remove(k)).contains(x) ==> edges(s).contains(x),
{
    let t = s.remove(k);
    assert(t.take(k) =~= s.take(k));
    assert(t.skip(k) =~= s.skip(k + 1));
    assert forall|x: int| #[trigger] edges(t).contains(x) implies edges(s).contains(x) by {
        let j = choose|j: int| 0 <= j <= t.len() && x == #[trigger] edge(t, j);
        if j <= k {
            lemma_edge_prefix(t, s, k, j);
            assert(x == edge(s, j));
        } else {
            lemma_edge_prefix(t, s, k, k);
            lemma_edge_step(s, k);
            lemma_edge_suffix(t, s, k, k + 1, j - k);
            assert(x == edge(s, j + 1));
        }
    }
}

/// Bytes `off..off + take` of slab `i` are the column's bytes from the
/// slab's start plus `off`; and a column with bytes after slab `i` has a
/// slab after it, which is not empty.
proof fn lemma_chunk(c: RawColumn, i: int, off: int, take: int)
    requires
        c.wf(),
        0 <= i < c.slabs@.len(),
        0 <= off,
        0 <= take,
        off + take <= c.slabs@[i].data@.len(),
    ensures
        ({
            let w = sizes(c.slabs@);
            let pos = sum(w, 0, i) + off;
            &&& pos + take <= c@.len()
            &&& c@.subrange(pos, pos + take) == c.slabs@[i].data@.subrange(off, off + take)
            &&& sum(w, 0, i + 1) == sum(w, 0, i) + c.slabs@[i].data@.len()
            &&& sum(w, 0, i + 1) <= c@.len()
            &&& sum(w, 0, i + 1) < c@.len() ==> i + 1 < c.slabs@.len() && c.slabs@[i + 1].data@.len() > 0
        }),
{
    let ss = c.slabs@;
    let w = sizes(ss);
    let pos = sum(w, 0, i) + off;
    lemma_flat_at(ss, i);
    lemma_flat_len(ss, i);
    lemma_flat_total(ss);
    lemma_sum_split(w, 0, i, i + 1);
    lemma_sum_one(w, i);
    lemma_sum_split(w, 0, i + 1, ss.len() as int);
    assert(c@.subrange(pos, pos + take) =~= ss[i].data@.subrange(off, off + take));
}

/// A range of `s` goes into `v0` a chunk at a time, for the `del` bytes
/// at `at`: with the bytes `start..pos` in, the chunk from `pos` goes
/// after them, and the first chunk takes the place of the `del` bytes.
proof fn lemma_splice_chunk(
    v0: Seq<u8>,
    v1: Seq<u8>,
    s: Seq<u8>,
    at: int,
    del: int,
    start: int,
    pos: int,
    take: int,
)
    requires
        0 <= at,
        0 <= del,
        at + del <= v0.len(),
        0 <= start <= pos,
        0 <= take,
        pos + take <= s.len(),
        pos == start ==> v1 == v0,
        pos > start ==> v1 == v0.take(at) + s.subrange(start, pos) + v0.skip(at + del),
    ensures
        ({
            let here = at + (pos - start);
            let gone = if pos == start {
                del
            } else {
                0
            };
            let v2 = v0.take(at) + s.subrange(start, pos + take) + v0.skip(at + del);
            &&& v1.take(here) + s.subrange(pos, pos + take) + v1.skip(here + gone) == v2
            &&& here + take <= v2.len()
        }),
{
    let here = at + (pos - start);
    let gone = if pos == start {
        del
    } else {
        0
    };
    let v2 = v0.take(at) + s.subrange(start, pos + take) + v0.skip(at + del);
    assert(s.subrange(start, pos + take) =~= s.subrange(start, pos) + s.subrange(pos, pos + take));
    if pos > start {
        assert(v1.take(here) =~= v0.take(at) + s.subrange(start, pos));
        assert(v1.skip(here) =~= v0.skip(at + del));
    } else {
        assert(s.subrange(start, pos) =~= Seq::<u8>::empty());
    }
    assert(v1.take(here) + s.subrange(pos, pos + take) + v1.skip(here + gone) =~= v2);
}

/// The column with slab `k` gone, when it held nothing.
proof fn lemma_flat_drop_empty(s: Seq<RawSlab>, k: int)
    requires
        0 <= k < s.len(),
        s[k].data@.len() == 0,
    ensures
        flat(s.remove(k)) == flat(s),
{
    lemma_flat_at(s, k);
    assert(s.remove(k) =~= s.take(k) + s.skip(k + 1));
    lemma_flat_cat(s.take(k), s.skip(k + 1));
    assert(flat(s) =~= flat(s.take(k)) + flat(s.skip(k + 1)));
}

// ── Failures ────────────────────────────────────────────────────────────────

/// Panics with `msg`.  Trusted only in that a panic does not return.
/// Every caller proves `why`: the condition its documentation gives for
/// the panic, so none panics otherwise.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn fail(msg: &str, Ghost(why): Ghost<bool>)
    requires
        why,
    ensures
        false,
{
    panic!("{msg}")
}

#[verifier::external_body]
#[cold]
#[inline(never)]
fn fail_take(n: usize, available: usize, Ghost(why): Ghost<bool>)
    requires
        why,
    ensures
        false,
{
    panic!("RawColumnIter::take would cross slab boundary: want {n}, have {available}")
}

#[verifier::external_body]
#[cold]
#[inline(never)]
fn fail_seek(pos: usize, target: usize, Ghost(why): Ghost<bool>)
    requires
        why,
    ensures
        false,
{
    panic!("RawColumnIter::seek_to is forward-only (at {pos} want {target})")
}

#[verifier::external_body]
#[cold]
#[inline(never)]
fn splice_oob(index: usize, del: usize, len: usize) -> PackError {
    PackError::InvalidValue(format!("raw splice out of bounds: index={index} del={del} len={len}"))
}

#[verifier::external_body]
#[cold]
#[inline(never)]
fn splice_too_long(len: usize, del: usize, insert: usize) -> PackError {
    PackError::InvalidValue(format!(
        "raw splice too long: len={len} del={del} insert={insert} overflows usize"
    ))
}

/// Panics: a splice out of bounds, or one that would leave the column
/// more than `usize::MAX` bytes long.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn splice_failed(e: PackError)
    ensures
        false,
{
    panic!("splice range out of bounds: {e:?}")
}

#[verifier::external_body]
#[cold]
#[inline(never)]
fn get_oob(start: usize, end: usize, len: usize) -> PackError {
    PackError::InvalidValue(format!(
        "raw get out of bounds: range={:?} len={}",
        start..end, len
    ))
}

#[verifier::external_body]
#[cold]
#[inline(never)]
fn get_cross(start: usize, end: usize, slab: usize, offset: usize, slab_len: usize) -> PackError {
    PackError::InvalidValue(format!(
        "raw get spans slab boundary: range={:?} slab={} offset={} slab_len={}",
        start..end, slab, offset, slab_len,
    ))
}

/// `v.drain(start..end)`.
#[verifier::external_body]
#[inline(always)]
fn drain_slabs(v: &mut Vec<RawSlab>, start: usize, end: usize)
    requires
        start <= end <= old(v)@.len(),
    ensures
        final(v)@ == old(v)@.take(start as int) + old(v)@.skip(end as int),
{
    v.drain(start..end);
}

// ── RawColumn ───────────────────────────────────────────────────────────────

impl View for RawColumn {
    type V = Seq<u8>;

    closed spec fn view(&self) -> Seq<u8> {
        flat(self.slabs@)
    }
}

impl RawColumn {
    /// `bit` is the Fenwick tree of the slab sizes, `total_len` their sum,
    /// and no slab is empty.
    pub closed spec fn wf(&self) -> bool {
        &&& bit_ok(lens(self.bit@), sizes(self.slabs@))
        &&& self.total_len == total(sizes(self.slabs@))
        &&& full(self.slabs@)
    }

    /// Bytes `[start, end)` lie within one slab.
    pub closed spec fn in_one_slab(&self, start: int, end: int) -> bool {
        exists|i: int|
            0 <= i < self.slabs@.len() && #[trigger] sum(sizes(self.slabs@), 0, i) <= start && end <= sum(
                sizes(self.slabs@),
                0,
                i + 1,
            )
    }

    /// Where the column's slabs meet, and its two ends. Bytes lie within
    /// one slab, which is where [`get`](Self::get) can give them, exactly
    /// when no cut is strictly between them (`lemma_in_one_slab`); and a
    /// splice leaves a cut only where one was, or at an end of the bytes
    /// it puts in (`spliced_cut`). So bytes spliced in as one, and never
    /// spliced into, stay within one slab.
    pub closed spec fn cuts(&self) -> ISet<int> {
        edges(self.slabs@)
    }

    /// Bytes `[start, end)` of the column lie within one slab exactly when
    /// no cut is strictly between `start` and `end`.
    pub proof fn lemma_in_one_slab(&self, start: int, end: int)
        requires
            0 <= start < end <= self@.len(),
        ensures
            self.in_one_slab(start, end) <==> forall|x: int|
                start < x < end ==> !#[trigger] self.cuts().contains(x),
    {
        // what the edges are, and what lying in one slab is, are the
        // lemmas' to know
        hide(edges);
        hide(RawColumn::in_one_slab);
        let s = self.slabs@;
        assert(self.cuts() == edges(s));
        lemma_edges_between(s, start, end);
        if self.in_one_slab(start, end) {
            self.lemma_one_slab_held(start, end);
        }
        if exists|i: int| #[trigger] held_by(s, i, start, end) {
            let i = choose|i: int| #[trigger] held_by(s, i, start, end);
            self.lemma_held_one_slab(i, start, end);
        }
    }

    /// Bytes that lie within one slab are held by one.
    proof fn lemma_one_slab_held(&self, start: int, end: int)
        requires
            self.in_one_slab(start, end),
        ensures
            exists|i: int| #[trigger] held_by(self.slabs@, i, start, end),
    {
        let s = self.slabs@;
        let i = choose|i: int|
            0 <= i < s.len() && #[trigger] sum(sizes(s), 0, i) <= start && end <= sum(sizes(s), 0, i + 1);
        lemma_flat_len(s, i);
        lemma_flat_len(s, i + 1);
        assert(held_by(s, i, start, end));
    }

    /// Bytes that a slab holds lie within one slab.
    proof fn lemma_held_one_slab(&self, i: int, start: int, end: int)
        requires
            held_by(self.slabs@, i, start, end),
        ensures
            self.in_one_slab(start, end),
    {
        let s = self.slabs@;
        lemma_flat_len(s, i);
        lemma_flat_len(s, i + 1);
        assert(sum(sizes(s), 0, i) <= start && end <= sum(sizes(s), 0, i + 1));
    }

    /// A column's length is its slabs' sizes together, and a column that
    /// holds a byte has a slab.
    proof fn lemma_len(&self)
        requires
            self.wf(),
        ensures
            self@.len() == self.total_len,
            self@.len() == total(sizes(self.slabs@)),
            self@.len() > 0 ==> self.slabs@.len() > 0,
    {
        lemma_flat_total(self.slabs@);
        if self.slabs@.len() == 0 {
            assert(sizes(self.slabs@) =~= Seq::<nat>::empty());
        }
    }

    pub fn new() -> (r: Self)
        ensures
            r.wf(),
            r@ == Seq::<u8>::empty(),
            forall|x: int| #[trigger] r.cuts().contains(x) ==> x == 0,
    {
        Self::with_max_segments(DEFAULT_MAX_SEGMENTS)
    }

    pub fn with_max_segments(max_segments: usize) -> (r: Self)
        ensures
            r.wf(),
            r@ == Seq::<u8>::empty(),
            forall|x: int| #[trigger] r.cuts().contains(x) ==> x == 0,
    {
        if max_segments == 0 {
            fail("max_segments must be non-zero", Ghost(max_segments == 0));
        }
        let r = Self { slabs: Vec::new(), bit: vec![0usize; 1], total_len: 0, max_segments };
        proof {
            assert(r.slabs@ =~= Seq::<RawSlab>::empty());
            assert(datas(r.slabs@) =~= Seq::<Seq<u8>>::empty());
            assert(sizes(r.slabs@) =~= Seq::<nat>::empty());
            lemma_edges_few(r.slabs@);
        }
        r
    }

    #[inline]
    pub fn len(&self) -> (n: usize)
        requires
            self.wf(),
        ensures
            n == self@.len(),
    {
        proof {
            lemma_flat_total(self.slabs@);
        }
        self.total_len
    }

    #[inline]
    pub fn is_empty(&self) -> (b: bool)
        requires
            self.wf(),
        ensures
            b == (self@.len() == 0),
    {
        proof {
            lemma_flat_total(self.slabs@);
        }
        self.total_len == 0
    }

    // ── Splice ──────────────────────────────────────────────────────────────

    /// Non-panicking variant of [`splice_slice`](Self::splice_slice).
    ///
    /// Flow mirrors `Column`'s `Encoding::splice`: locate the target slab,
    /// delegate to `splice_slab` which handles one slab plus any overflow,
    /// then clean up cross-slab deletes, merge undersized neighbours, and
    /// update the BIT.
    pub fn try_splice_slice(&mut self, index: usize, del: usize, bytes: &[u8]) -> (r: Result<
        (),
        PackError,
    >)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            r is Ok <==> index + del <= old(self)@.len() && old(self)@.len() - del + bytes@.len()
                <= usize::MAX,
            r is Ok ==> final(self)@ == old(self)@.take(index as int) + bytes@ + old(self)@.skip(
                index + del,
            ),
            r is Err ==> final(self)@ == old(self)@,
            r is Ok ==> forall|x: int| #[trigger]
                final(self).cuts().contains(x) ==> spliced_cut(
                    old(self).cuts(),
                    index as int,
                    del as int,
                    bytes@.len() as int,
                    x,
                ),
            r is Err ==> final(self).cuts() == old(self).cuts(),
    {
        proof {
            lemma_flat_total(self.slabs@);
        }
        if index > self.total_len || del > self.total_len - index {
            return Err(splice_oob(index, del, self.total_len));
        }
        if bytes.len() > usize::MAX - (self.total_len - del) {
            return Err(splice_too_long(self.total_len, del, bytes.len()));
        }
        let ghost old_bytes = self@;
        let ghost target = old_bytes.take(index as int) + bytes@ + old_bytes.skip(index + del);
        if del == 0 && bytes.len() == 0 {
            assert(target =~= old_bytes);
            return Ok(());
        }
        if self.slabs.len() == 0 {
            // An empty column: the bytes are its one slab.
            let slab = RawSlab { data: slice_to_vec(bytes) };
            self.slabs.push(slab);
            self.bit.push(bytes.len());
            self.total_len = bytes.len();
            proof {
                lemma_flat_unit(slab);
                assert(self.slabs@ =~= seq![slab]);
                assert(target =~= bytes@);
                assert(1usize & !((1usize - 1) as usize) == 1) by (bit_vector);
                assert(sum(sizes(self.slabs@), 0, 0) == 0);
                assert(sizes(self.slabs@) =~= seq![bytes@.len()]);
                lemma_sum_one(sizes(self.slabs@), 0);
                lemma_edges_few(self.slabs@);
            }
            return Ok(());
        }
        let (si, offset) = self.find_slab(index);
        let ghost old_slabs = self.slabs@;
        let old_weight = self.slabs[si].data.len();
        proof {
            lemma_flat_len(old_slabs, si as int);
        }
        let (end_spliced, mut changed) = splice_in(&mut self.slabs, self.max_segments, si, offset, del, bytes);
        proof {
            lemma_flat_total(self.slabs@);
        }
        self.total_len = self.total_len - del + bytes.len();
        let ghost s1 = self.slabs@;
        let (start, end, merged) = merge_around(
            &RawMerge { max_segments: self.max_segments, base: Ghost(edges(s1)) },
            &mut self.slabs,
            si,
            end_spliced,
        );
        proof {
            lemma_merged_full(self.slabs@, s1, start as int, end as int, si as int, end_spliced as int);
        }
        changed = changed || merged;
        let removed = drop_empty(&mut self.slabs, start, end);
        changed = changed || removed;
        proof {
            lemma_flat_total(self.slabs@);
        }
        // Update BIT: point update if exactly one slab's weight changed,
        // full rebuild otherwise.
        if !changed {
            proof {
                assert(sizes(self.slabs@) =~= sizes(old_slabs).update(
                    si as int,
                    self.slabs@[si as int].data@.len(),
                ));
            }
            let new_weight = self.slabs[si].data.len();
            fenwick::update(&mut self.bit, si, &old_weight, &new_weight, Ghost(sizes(old_slabs)));
        } else {
            self.rebuild_bit();
        }
        Ok(())
    }

    /// Recompute the Fenwick tree from the slabs.
    fn rebuild_bit(&mut self)
        requires
            total(sizes(old(self).slabs@)) <= usize::MAX,
        ensures
            bit_ok(lens(final(self).bit@), sizes(final(self).slabs@)),
            final(self).slabs == old(self).slabs,
            final(self).total_len == old(self).total_len,
            final(self).max_segments == old(self).max_segments,
    {
        proof {
            assert(self.slabs@.map_values(|s: RawSlab| size_of(s)) =~= sizes(self.slabs@));
        }
        self.bit = fenwick::build(
            self.slabs.as_slice(),
            |s: &RawSlab| -> (n: usize)
                ensures
                    n == size_of(*s),
                { s.data.len() },
            Ghost(|s: RawSlab| size_of(s)),
        );
    }

    /// Insert / delete / replace bytes at `index` with a single byte slice.
    /// Panics if `index + del` exceeds the column length.
    #[inline(never)]
    pub fn splice_slice(&mut self, index: usize, del: usize, bytes: &[u8])
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self)@ == old(self)@.take(index as int) + bytes@ + old(self)@.skip(index + del),
            forall|x: int| #[trigger]
                final(self).cuts().contains(x) ==> spliced_cut(
                    old(self).cuts(),
                    index as int,
                    del as int,
                    bytes@.len() as int,
                    x,
                ),
    {
        if let Err(e) = self.try_splice_slice(index, del, bytes) {
            splice_failed(e);
        }
    }

    /// Replace the `del` bytes at `at` with `src`'s bytes `start..end`,
    /// spliced a chunk per slab of `src` they lie in (those slabs are cut at
    /// value boundaries and within the budget): panics if the column would
    /// hold more than `usize::MAX` bytes.
    #[allow(unused_variables)] // `nslabs` is for the spec
    fn splice_range(&mut self, at: usize, del: usize, src: &RawColumn, start: usize, end: usize)
        requires
            old(self).wf(),
            src.wf(),
            at + del <= old(self)@.len(),
            start <= end <= src@.len(),
        ensures
            final(self).wf(),
            final(self)@ == old(self)@.take(at as int) + src@.subrange(start as int, end as int) + old(self)@.skip(
                at + del,
            ),
    {
        hide(flat);
        hide(RawColumn::wf);
        let ghost (v0, s) = (self@, src@);
        let ghost w = sizes(src.slabs@);
        proof {
            src.lemma_len();
        }
        if start < end {
            let (mut i, mut off) = src.find_slab(start);
            let nslabs = src.slabs.len();
            let mut pos = start;
            let mut here = at;
            let mut gone = del;
            while pos < end
                invariant
                    self.wf(),
                    src.wf(),
                    s == src@,
                    w == sizes(src.slabs@),
                    nslabs == src.slabs@.len(),
                    s.len() == total(w),
                    start <= pos <= end <= s.len(),
                    pos < end ==> i < src.slabs@.len() && sum(w, 0, i as int) + off == pos && off < src.slabs@[i as int].data@.len(),
                    pos == start ==> self@ == v0 && here == at && gone == del,
                    pos > start ==> gone == 0 && here == at + (pos - start) && self@ == v0.take(at as int)
                        + s.subrange(start as int, pos as int) + v0.skip(at + del),
                    at + del <= v0.len(),
                decreases end - pos,
            {
                let data = &src.slabs[i].data;
                let take = if data.len() - off < end - pos { data.len() - off } else { end - pos };
                proof {
                    // the chunk; and if the range goes on, it does from the
                    // start of the next slab
                    lemma_chunk(*src, i as int, off as int, take as int);
                }
                let chunk = slice_subrange(data.as_slice(), off, off + take);
                let ghost v1 = self@;
                self.splice_slice(here, gone, chunk);
                proof {
                    lemma_splice_chunk(v0, v1, s, at as int, del as int, start as int, pos as int, take as int);
                    self.lemma_len();
                }
                pos += take;
                here += take;
                gone = 0;
                i += 1;
                off = 0;
            }
            proof {
                assert(s.subrange(start as int, end as int) == s.subrange(start as int, pos as int));
            }
        } else if del > 0 {
            // a pure delete, or one whose range copies nothing
            self.splice_slice(at, del, &[]);
            proof {
                assert(src@.subrange(start as int, end as int) =~= Seq::<u8>::empty());
            }
        } else {
            proof {
                assert(src@.subrange(start as int, end as int) =~= Seq::<u8>::empty());
                assert(v0 =~= v0.take(at as int) + Seq::<u8>::empty() + v0.skip(at + del));
            }
        }
    }

    /// `copy_ranges`: the normalized splices `list`, applied back to
    /// front, so that each lands above those still to come and their
    /// positions hold, each range's bytes from `src` (`splice_range`). The
    /// column becomes what the splices `list` was made from give one by
    /// one.
    pub(crate) fn copy_from(&mut self, src: &RawColumn, list: &Normalized)
        requires
            old(self).wf(),
            src.wf(),
        ensures
            final(self).wf(),
            final(self)@ == applied(old(self)@, src@, list.seen()),
    {
        hide(flat);
        hide(RawColumn::wf);
        hide(Normalized::wf);
        hide(normal);
        hide(applied);
        hide(applied_from);
        proof {
            self.lemma_len();
            src.lemma_len();
        }
        if list.src_len() != src.total_len || list.dst_len() != self.total_len {
            wrong_columns();
        }
        let sps = list.splices();
        let ghost (dst, s) = (self@, src@);
        proof {
            list.lemma_applied(dst, s);
            lemma_back_ends(dst, s, sps@);
        }
        let mut k = sps.len();
        while k > 0
            invariant
                self.wf(),
                src.wf(),
                s == src@,
                k <= sps@.len(),
                normal(sps@, s.len() as int, dst.len() as int),
                self@ == applied_from(dst, s, sps@, k as int),
            decreases k,
        {
            k -= 1;
            let sp = &sps[k];
            proof {
                lemma_back_step(dst, s, sps@, k as int);
            }
            self.splice_range(sp.pos, sp.delete, src, sp.range.start, sp.range.end);
        }
    }

    /// The slab holding byte `index` and the byte's offset in it; at the
    /// end, the last slab and its length.
    fn find_slab(&self, index: usize) -> (r: (usize, usize))
        requires
            self.wf(),
            self.slabs@.len() > 0,
            index <= self.total_len,
        ensures
            r.0 < self.slabs@.len(),
            sum(sizes(self.slabs@), 0, r.0 as int) + r.1 == index,
            r.1 <= self.slabs@[r.0 as int].data@.len(),
            index < self.total_len ==> r.1 < self.slabs@[r.0 as int].data@.len(),
    {
        let ghost w = sizes(self.slabs@);
        if index == self.total_len {
            // Appending at the very end: land in the last slab.
            let last = self.slabs.len() - 1;
            proof {
                lemma_sum_split(w, 0, last as int, last + 1);
                lemma_sum_one(w, last as int);
            }
            (last, self.slabs[last].data.len())
        } else {
            let r = fenwick::find(self.bit.as_slice(), index, self.slabs.len(), Ghost(w));
            proof {
                if r.0 == self.slabs@.len() {
                    assert(false);
                }
            }
            r
        }
    }

    // ── Range read ──────────────────────────────────────────────────────────

    /// The bytes `[start, end)`, if they lie within one slab.
    fn get_span(&self, start: usize, end: usize) -> (r: Result<&[u8], PackError>)
        requires
            self.wf(),
        ensures
            r is Ok <==> start <= end <= self@.len() && (start == end || self.in_one_slab(
                start as int,
                end as int,
            )),
            r is Ok ==> r->Ok_0@ == self@.subrange(start as int, end as int),
    {
        proof {
            lemma_flat_total(self.slabs@);
        }
        if end > self.total_len || start > end {
            return Err(get_oob(start, end, self.total_len));
        }
        if start == end {
            let empty: &[u8] = &[];
            assert(empty@ =~= self@.subrange(start as int, end as int));
            return Ok(empty);
        }
        let ghost w = sizes(self.slabs@);
        let (slab_idx, offset) = self.find_slab(start);
        let span = end - start;
        let slab = &self.slabs[slab_idx];
        let ghost p = sum(w, 0, slab_idx as int);
        proof {
            lemma_sum_split(w, 0, slab_idx as int, slab_idx + 1);
            lemma_sum_one(w, slab_idx as int);
        }
        if span > slab.data.len() - offset {
            proof {
                assert forall|i: int|
                    0 <= i < self.slabs@.len() && #[trigger] sum(w, 0, i) <= start implies !(end <= sum(
                        w,
                        0,
                        i + 1,
                    )) by {
                    if i < slab_idx {
                        lemma_sum_le(w, 0, i + 1, 0, slab_idx as int);
                    } else if i > slab_idx {
                        lemma_sum_le(w, 0, slab_idx + 1, 0, i);
                    }
                }
            }
            return Err(get_cross(start, end, slab_idx, offset, slab.data.len()));
        }
        proof {
            lemma_flat_at(self.slabs@, slab_idx as int);
            lemma_flat_len(self.slabs@, slab_idx as int);
            assert(self@.subrange(start as int, end as int) =~= slab.data@.subrange(
                offset as int,
                offset + span,
            ));
        }
        Ok(slice_subrange(slab.data.as_slice(), offset, offset + span))
    }

    // ── Iter ────────────────────────────────────────────────────────────────

    /// Sequential reader starting at byte 0.
    pub fn iter(&self) -> (it: RawColumnIter<'_>)
        requires
            self.wf(),
        ensures
            it.wf(),
            it.bytes() == self@,
            it.spec_pos() == 0,
    {
        proof {
            lemma_flat_total(self.slabs@);
        }
        RawColumnIter { slabs: self.slabs.as_slice(), slab_idx: 0, offset_in_slab: 0, pos: 0 }
    }

    /// Sequential reader positioned at byte `pos`.  O(log S) seek.
    /// Panics if `pos > len()`.
    pub fn iter_at(&self, pos: usize) -> (it: RawColumnIter<'_>)
        requires
            self.wf(),
        ensures
            pos <= self@.len(),
            it.wf(),
            it.bytes() == self@,
            it.spec_pos() == pos,
    {
        proof {
            lemma_flat_total(self.slabs@);
        }
        if pos > self.total_len {
            fail("iter_at out of bounds", Ghost(pos > self@.len()));
        }
        if pos == 0 || self.slabs.len() == 0 {
            return self.iter();
        }
        if pos == self.total_len {
            // Positioned one past the end of the last slab.
            return RawColumnIter {
                slabs: self.slabs.as_slice(),
                slab_idx: self.slabs.len(),
                offset_in_slab: 0,
                pos,
            };
        }
        let (slab_idx, offset) = self.find_slab(pos);
        RawColumnIter { slabs: self.slabs.as_slice(), slab_idx, offset_in_slab: offset, pos }
    }

    // ── Wire format ─────────────────────────────────────────────────────────

    pub fn save(&self) -> (out: Vec<u8>)
        requires
            self.wf(),
        ensures
            out@ == self@,
    {
        let mut out = Vec::with_capacity(self.total_len);
        self.append_to(&mut out);
        out
    }

    /// Append the column's bytes to `out`.
    fn append_to(&self, out: &mut Vec<u8>)
        ensures
            final(out)@ == old(out)@ + self@,
    {
        let ghost s = self.slabs@;
        let mut i = 0;
        while i < self.slabs.len()
            invariant
                s == self.slabs@,
                i <= s.len(),
                out@ == old(out)@ + flat(s.take(i as int)),
            decreases s.len() - i,
        {
            proof {
                assert(s.take(i + 1) =~= s.take(i as int) + seq![s[i as int]]);
                lemma_flat_cat(s.take(i as int), seq![s[i as int]]);
                lemma_flat_unit(s[i as int]);
            }
            out.extend_from_slice(self.slabs[i].data.as_slice());
            i += 1;
        }
        proof {
            assert(s.take(s.len() as int) =~= s);
        }
    }

    /// Deserialize with a custom `max_segments` budget.
    pub fn load_with_max_segments(data: &[u8], max_segments: usize) -> (r: Result<Self, PackError>)
        ensures
            r is Ok,
            r->Ok_0.wf(),
            r->Ok_0@ == data@,
            forall|x: int| #[trigger] r->Ok_0.cuts().contains(x) ==> x == 0 || x == data@.len(),
    {
        if max_segments == 0 {
            fail("max_segments must be non-zero", Ghost(max_segments == 0));
        }
        if data.len() == 0 {
            let r = Self::with_max_segments(max_segments);
            assert(data@ =~= Seq::<u8>::empty());
            return Ok(r);
        }
        let slab = RawSlab { data: slice_to_vec(data) };
        let mut slabs = Vec::with_capacity(1);
        slabs.push(slab);
        let mut r = Self { slabs, bit: Vec::new(), total_len: data.len(), max_segments };
        proof {
            lemma_flat_unit(slab);
            assert(r.slabs@ =~= seq![slab]);
            assert(sizes(r.slabs@) =~= seq![data@.len()]);
            lemma_sum_one(sizes(r.slabs@), 0);
            assert(sum(sizes(r.slabs@), 0, 0) == 0);
            lemma_edges_few(r.slabs@);
        }
        r.rebuild_bit();
        Ok(r)
    }
}

// ── Splice phases ───────────────────────────────────────────────────────────

/// How many of `del` bytes a splice at `offset` deletes from a slab of
/// `len` bytes; the rest carry on into the slabs after it.
spec fn local_del(len: int, offset: int, del: int) -> int {
    if del <= len - offset {
        del
    } else {
        len - offset
    }
}

/// Bytes `a + d + rest` with `del` of them at `offset` of `d` replaced
/// by `bytes`: `d` keeps what is around the bytes it loses, and `rest`
/// loses the bytes the deletion reaches into it.
proof fn lemma_splice_bytes(a: Seq<u8>, d: Seq<u8>, rest: Seq<u8>, offset: int, del: int, bytes: Seq<u8>)
    requires
        0 <= offset <= d.len(),
        0 <= del,
        offset + del <= d.len() + rest.len(),
    ensures
        ({
            let ld = local_del(d.len() as int, offset, del);
            let b = a + d + rest;
            let index = a.len() + offset;
            a + (d.take(offset) + bytes + d.skip(offset + ld)) + rest.skip(del - ld) == b.take(index) + bytes
                + b.skip(index + del)
        }),
{
    let ld = local_del(d.len() as int, offset, del);
    let b = a + d + rest;
    let index = a.len() + offset;
    assert(b.take(index) =~= a + d.take(offset));
    assert(b.skip(index + del) =~= d.skip(offset + ld) + rest.skip(del - ld));
    assert(a + (d.take(offset) + bytes + d.skip(offset + ld)) + rest.skip(del - ld) =~= b.take(index) + bytes
        + b.skip(index + del));
}

/// A splice in bounds at `offset` in slab `si`: what the slab keeps is
/// no more than the column keeps, and the deletion reaches no further
/// than the slabs after it.
proof fn lemma_splice_within(s: Seq<RawSlab>, si: int, offset: int, del: int)
    requires
        0 <= si < s.len(),
        0 <= offset <= s[si].data@.len(),
        0 <= del,
        flat(s.take(si)).len() + offset + del <= flat(s).len(),
    ensures
        ({
            let ld = local_del(s[si].data@.len() as int, offset, del);
            &&& 0 <= ld <= del
            &&& s[si].data@.len() - ld <= flat(s).len() - del
            &&& del - ld <= flat(s.skip(si + 1)).len()
        }),
{
    lemma_flat_at(s, si);
}

/// Slabs `s1` that from `end` on are the slabs after `si` of `s0`, none
/// of which is empty: none of them is empty.
proof fn lemma_full_after(s0: Seq<RawSlab>, s1: Seq<RawSlab>, si: int, end: int)
    requires
        full(s0),
        0 <= si < s0.len(),
        0 <= end <= s1.len(),
        s1.skip(end) == s0.skip(si + 1),
    ensures
        forall|i: int| end <= i < s1.len() ==> #[trigger] s1[i].data@.len() > 0,
{
    assert forall|i: int| end <= i < s1.len() implies #[trigger] s1[i].data@.len() > 0 by {
        assert(s1.skip(end).len() == s1.len() - end);
        assert(s0.skip(si + 1).len() == s0.len() - si - 1);
        assert(s1[i] == s1.skip(end)[i - end]);
        assert(s0.skip(si + 1)[i - end] == s0[i - end + si + 1]);
    }
}

/// Slabs `s2` that before `si` are those of `s0`, none of which is
/// empty, and from `end` on are not empty: none outside `si..hi` is
/// empty, for any `hi` from `end` on.
proof fn lemma_full_around(s0: Seq<RawSlab>, s1: Seq<RawSlab>, s2: Seq<RawSlab>, si: int, end: int, hi: int)
    requires
        full(s0),
        0 <= si <= end <= hi,
        si <= s0.len(),
        end <= s1.len(),
        end <= s2.len(),
        s1.take(si) == s0.take(si),
        s2.take(end) == s1.take(end),
        forall|i: int| end <= i < s2.len() ==> #[trigger] s2[i].data@.len() > 0,
    ensures
        full_outside(s2, si, hi),
{
    assert forall|i: int| 0 <= i < s2.len() && !(si <= i < hi) implies #[trigger] s2[i].data@.len() > 0 by {
        if i < si {
            assert(s2[i] == s2.take(end)[i]);
            assert(s1.take(end)[i] == s1.take(si)[i]);
            assert(s0.take(si)[i] == s0[i]);
        }
    }
}

/// A splice at `offset` in slab `si` of `s0`, which leaves slabs
/// `si..end` of `s1` in its place, and then the rest of the deletion
/// from the slabs after them, which leaves `s2`: together they splice
/// the column's bytes.
proof fn lemma_spliced_in(
    s0: Seq<RawSlab>,
    s1: Seq<RawSlab>,
    s2: Seq<RawSlab>,
    si: int,
    end: int,
    offset: int,
    del: int,
    bytes: Seq<u8>,
)
    requires
        0 <= si < s0.len(),
        0 <= offset <= s0[si].data@.len(),
        0 <= del,
        flat(s0.take(si)).len() + offset + del <= flat(s0).len(),
        si < end <= s1.len(),
        end <= s2.len(),
        s1.take(si) == s0.take(si),
        s1.skip(end) == s0.skip(si + 1),
        flat(s1.subrange(si, end)) == s0[si].data@.take(offset) + bytes + s0[si].data@.skip(
            offset + local_del(s0[si].data@.len() as int, offset, del),
        ),
        s2.take(end) == s1.take(end),
        flat(s2.skip(end)) == flat(s1.skip(end)).skip(del - local_del(s0[si].data@.len() as int, offset, del)),
    ensures
        ({
            let index = flat(s0.take(si)).len() + offset;
            flat(s2) == flat(s0).take(index) + bytes + flat(s0).skip(index + del)
        }),
{
    lemma_flat_at(s0, si);
    lemma_flat_split(s2, end);
    lemma_flat_split(s1.take(end), si);
    assert(s1.take(end).take(si) =~= s1.take(si));
    assert(s1.take(end).skip(si) =~= s1.subrange(si, end));
    lemma_splice_bytes(flat(s0.take(si)), s0[si].data@, flat(s0.skip(si + 1)), offset, del, bytes);
}

/// Splice `del` bytes at `offset` in slab `si` for `bytes`, carrying the
/// deletion on into the slabs after it.  Returns the end of the slabs it
/// touched, and whether it changed any but slab `si`.
#[verifier::spinoff_prover]
fn splice_in(
    slabs: &mut Vec<RawSlab>,
    max_segments: usize,
    si: usize,
    offset: usize,
    del: usize,
    bytes: &[u8],
) -> (r: (usize, bool))
    requires
        si < old(slabs)@.len(),
        offset <= old(slabs)@[si as int].data@.len(),
        flat(old(slabs)@.take(si as int)).len() + offset + del <= flat(old(slabs)@).len(),
        flat(old(slabs)@).len() - del + bytes@.len() <= usize::MAX,
        full(old(slabs)@),
    ensures
        ({
            let index = flat(old(slabs)@.take(si as int)).len() + offset;
            flat(final(slabs)@) == flat(old(slabs)@).take(index) + bytes@ + flat(old(slabs)@).skip(
                index + del,
            )
        }),
        si < r.0 <= final(slabs)@.len(),
        full_outside(final(slabs)@, si as int, r.0 as int),
        !r.1 ==> r.0 == si + 1 && final(slabs)@ == old(slabs)@.update(
            si as int,
            final(slabs)@[si as int],
        ),
        forall|x: int| #[trigger]
            edges(final(slabs)@).contains(x) ==> spliced_cut(
                edges(old(slabs)@),
                edge(old(slabs)@, si as int) + offset,
                del as int,
                bytes@.len() as int,
                x,
            ),
{
    hide(flat);
    let ghost s0 = slabs@;
    proof {
        lemma_splice_within(s0, si as int, offset as int, del as int);
    }
    let (end, overflow_del) = splice_at(slabs, max_segments, si, offset, del, bytes);
    let ghost s1 = slabs@;
    proof {
        lemma_full_after(s0, s1, si as int, end as int);
    }
    let (partial, drained) = drain_after(slabs, end, overflow_del);
    let _ = slabs.len();
    proof {
        lemma_spliced_in(s0, s1, slabs@, si as int, end as int, offset as int, del as int, bytes@);
        lemma_full_around(s0, s1, slabs@, si as int, end as int, end + if partial { 1int } else { 0 });
        let len0 = s0[si as int].data@.len() as int;
        lemma_edges_at(s0, s1, si as int, end as int, offset as int, local_del(len0, offset as int, del as int), bytes@);
        lemma_edges_spliced(
            edges(s0),
            edges(s1),
            edges(slabs@),
            edge(s0, si as int),
            len0,
            offset as int,
            del as int,
            bytes@.len() as int,
            edge(s1, end as int),
        );
    }
    let changed = end > si + 1 || partial || drained;
    let end = if partial { end + 1 } else { end };
    (end, changed)
}

/// Splice slab `si` and put any overflow slab after it.  Returns the end
/// of the slabs it now spans and the bytes left to delete after them.
fn splice_at(
    slabs: &mut Vec<RawSlab>,
    max_segments: usize,
    si: usize,
    offset: usize,
    del: usize,
    bytes: &[u8],
) -> (r: (usize, usize))
    requires
        si < old(slabs)@.len(),
        offset <= old(slabs)@[si as int].data@.len(),
        old(slabs)@[si as int].data@.len() + bytes@.len() - local_del(
            old(slabs)@[si as int].data@.len() as int,
            offset as int,
            del as int,
        ) <= usize::MAX,
    ensures
        ({
            let d0 = old(slabs)@[si as int].data@;
            let ld = local_del(d0.len() as int, offset as int, del as int);
            &&& r.1 == del - ld
            &&& flat(final(slabs)@.subrange(si as int, r.0 as int)) == d0.take(offset as int)
                + bytes@ + d0.skip(offset + ld)
        }),
        r.0 == si + 1 || r.0 == si + 2,
        r.0 <= final(slabs)@.len(),
        final(slabs)@.take(si as int) == old(slabs)@.take(si as int),
        final(slabs)@.skip(r.0 as int) == old(slabs)@.skip(si + 1),
        r.0 == si + 1 ==> final(slabs)@ == old(slabs)@.update(si as int, final(slabs)@[si as int]),
        r.0 == si + 2 ==> final(slabs)@[si as int].data@.len() == offset || final(slabs)@[si as int].data@.len()
            == offset + bytes@.len(),
{
    let ghost s0 = slabs@;
    let _ = slabs.len();
    let (overflow, overflow_del) = splice_slab(&mut slabs[si], offset, del, bytes, max_segments);
    let ghost s1 = slabs@;
    let ghost e = s1[si as int];
    let mut end = si + 1;
    proof {
        assert(s1 =~= s0.update(si as int, e));
    }
    // Insert the overflow slab.
    if let Some(o) = overflow {
        slabs.insert(end, o);
        let _ = slabs.len();
        end += 1;
        proof {
            assert(slabs@.subrange(si as int, end as int) =~= seq![e] + seq![o]);
            lemma_flat_cat(seq![e], seq![o]);
            lemma_flat_unit(e);
            lemma_flat_unit(o);
        }
    } else {
        proof {
            assert(slabs@.subrange(si as int, end as int) =~= seq![e]);
            lemma_flat_unit(e);
            assert(e.data@ + Seq::<u8>::empty() =~= e.data@);
        }
    }
    proof {
        assert(slabs@.take(si as int) =~= s0.take(si as int));
        assert(slabs@.skip(end as int) =~= s0.skip(si + 1));
    }
    (end, overflow_del)
}

/// No slabs hold no bytes.
proof fn lemma_flat_none(s: Seq<RawSlab>)
    requires
        s.len() == 0,
    ensures
        flat(s) == Seq::<u8>::empty(),
{
    assert(datas(s) =~= Seq::<Seq<u8>>::empty());
}

/// A deletion of the first `remaining` bytes of the slabs from `end` on
/// that takes all of slab `end`: the rest of it is from the slabs after.
proof fn lemma_drain_whole(s: Seq<RawSlab>, end: int, remaining: int)
    requires
        0 <= end < s.len(),
        s[end].data@.len() <= remaining <= flat(s.skip(end)).len(),
    ensures
        flat(s.skip(end)).len() == s[end].data@.len() + flat(s.skip(end + 1)).len(),
        flat(s.skip(end)).skip(remaining) == flat(s.skip(end + 1)).skip(remaining - s[end].data@.len()),
{
    hide(flat);
    lemma_flat_first(s.skip(end));
    assert(s.skip(end).skip(1) =~= s.skip(end + 1));
    assert(flat(s.skip(end)).skip(remaining) =~= flat(s.skip(end + 1)).skip(remaining - s[end].data@.len()));
}

/// A deletion of the first `remaining` bytes of the slabs from `end` on
/// that ends inside slab `end`: the slab loses them, and that is all.
proof fn lemma_drain_part(s1: Seq<RawSlab>, s2: Seq<RawSlab>, end: int, remaining: int)
    requires
        0 <= end < s1.len(),
        s2.len() == s1.len(),
        0 <= remaining <= s1[end].data@.len(),
        s2[end].data@ == s1[end].data@.skip(remaining),
        s2.skip(end + 1) == s1.skip(end + 1),
    ensures
        flat(s2.skip(end)) == flat(s1.skip(end)).skip(remaining),
{
    hide(flat);
    lemma_flat_first(s1.skip(end));
    lemma_flat_first(s2.skip(end));
    assert(s1.skip(end).skip(1) =~= s1.skip(end + 1));
    assert(s2.skip(end).skip(1) =~= s2.skip(end + 1));
    assert(flat(s2.skip(end)) =~= flat(s1.skip(end)).skip(remaining));
}

/// Delete the first `del` bytes of the slabs from `start` on: drop the
/// slabs that go whole, and trim the one that goes in part.  Returns
/// whether one went in part (it stays at `start`), and whether any went
/// whole.
fn drain_after(slabs: &mut Vec<RawSlab>, start: usize, del: usize) -> (r: (bool, bool))
    requires
        start <= old(slabs)@.len(),
        del <= flat(old(slabs)@.skip(start as int)).len(),
        forall|i: int| start <= i < old(slabs)@.len() ==> #[trigger] old(slabs)@[i].data@.len() > 0,
    ensures
        start <= final(slabs)@.len(),
        final(slabs)@.take(start as int) == old(slabs)@.take(start as int),
        flat(final(slabs)@.skip(start as int)) == flat(old(slabs)@.skip(start as int)).skip(del as int),
        forall|i: int| start <= i < final(slabs)@.len() ==> #[trigger] final(slabs)@[i].data@.len() > 0,
        r.0 ==> start < final(slabs)@.len(),
        !r.0 && !r.1 ==> final(slabs)@ == old(slabs)@,
        forall|x: int| #[trigger]
            edges(final(slabs)@).contains(x) ==> {
                ||| x <= edge(old(slabs)@, start as int) && edges(old(slabs)@).contains(x)
                ||| x >= edge(old(slabs)@, start as int) && edges(old(slabs)@).contains(x + del)
            },
{
    hide(flat);
    let ghost s0 = slabs@;
    let mut remaining = del;
    let mut end = start;
    let mut partial = false;
    while remaining > 0 && end < slabs.len()
        invariant
            start <= end <= slabs@.len(),
            slabs@.len() == s0.len(),
            slabs@.take(end as int) == s0.take(end as int),
            forall|i: int| end <= i < slabs@.len() ==> #[trigger] slabs@[i].data@.len() > 0,
            partial ==> remaining == 0 && end < slabs@.len(),
            !partial ==> slabs@ == s0,
            !partial ==> edge(s0, end as int) - edge(s0, start as int) + remaining == del,
            partial ==> {
                &&& slabs@ == s0.update(end as int, slabs@[end as int])
                &&& edge(s0, end as int) - edge(s0, start as int) + (s0[end as int].data@.len()
                    - slabs@[end as int].data@.len()) == del
            },
            flat(s0.skip(start as int)).skip(del as int) == flat(slabs@.skip(end as int)).skip(
                remaining as int,
            ),
            remaining <= flat(slabs@.skip(end as int)).len(),
        decreases slabs@.len() - end, remaining,
    {
        let ghost s1 = slabs@;
        let slab_len = slabs[end].data.len();
        if remaining >= slab_len {
            proof {
                lemma_drain_whole(s1, end as int, remaining as int);
                lemma_edge_step(s0, end as int);
            }
            remaining -= slab_len;
            end += 1;
            proof {
                assert(slabs@.take(end as int) =~= s0.take(end as int));
            }
        } else {
            splice_bytes(&mut slabs[end].data, 0, remaining, &[]);
            proof {
                let s2 = slabs@;
                assert(s2.skip(end + 1) =~= s1.skip(end + 1));
                assert(s2[end as int].data@ =~= s1[end as int].data@.skip(remaining as int));
                lemma_drain_part(s1, s2, end as int, remaining as int);
                assert(flat(s2.skip(end as int)).skip(0) =~= flat(s2.skip(end as int)));
                assert(s2.take(end as int) =~= s0.take(end as int));
                assert(s2 =~= s0.update(end as int, s2[end as int]));
            }
            remaining = 0;
            partial = true;
        }
    }
    proof {
        if remaining > 0 {
            // out of slabs, but the deletion is in bounds
            lemma_flat_none(slabs@.skip(end as int));
        }
        assert(flat(slabs@.skip(end as int)).skip(0) =~= flat(slabs@.skip(end as int)));
    }
    let ghost s3 = slabs@;
    // Bulk-remove fully consumed slabs in one shift.
    let drained = start < end;
    if drained {
        drain_slabs(slabs, start, end);
    }
    proof {
        if drained {
            assert(slabs@.take(start as int) =~= s3.take(start as int));
            assert(slabs@.skip(start as int) =~= s3.skip(end as int));
            assert(s3.take(start as int) =~= s0.take(start as int));
            assert forall|i: int| start <= i < slabs@.len() implies #[trigger] slabs@[i].data@.len() > 0 by {
                assert(slabs@[i] == s3[i + (end - start)]);
            }
        }
        lemma_edges_drained(s0, s3, slabs@, start as int, end as int, del as int, partial);
    }
    (partial, drained)
}

/// Splice a single slab: delete up to `del` bytes at `index`, insert
/// `values`.  Mirrors the shape of [`ColumnEncoding::splice_slab`] used by
/// `Column` — `del` may exceed the slab's length, in which case the
/// excess is returned as `overflow_del` for the caller to apply to the
/// following slabs.
///
/// If the insert pushes the slab past `max_segments`, we split at the
/// splice point (before or after the inserted bytes, whichever is more
/// balanced) and return the right half as an overflow slab.  At most one
/// overflow slab is ever produced — the column's invariant only permits
/// splits at splice points, so there's nowhere else to cut.
///
/// Returns `(overflow_slab, overflow_del)`.
///
/// [`ColumnEncoding::splice_slab`]: crate::encoding::ColumnEncoding::splice_slab
fn splice_slab(
    slab: &mut RawSlab,
    index: usize,
    del: usize,
    values: &[u8],
    max_segments: usize,
) -> (r: (Option<RawSlab>, usize))
    requires
        index <= old(slab).data@.len(),
        old(slab).data@.len() + values@.len() - local_del(
            old(slab).data@.len() as int,
            index as int,
            del as int,
        ) <= usize::MAX,
    ensures
        ({
            let ld = local_del(old(slab).data@.len() as int, index as int, del as int);
            &&& r.1 == del - ld
            &&& final(slab).data@ + (if r.0 is Some {
                r.0->Some_0.data@
            } else {
                Seq::empty()
            }) == old(slab).data@.take(index as int) + values@ + old(slab).data@.skip(index + ld)
        }),
        r.0 is Some ==> final(slab).data@.len() == index || final(slab).data@.len() == index + values@.len(),
{
    let slab_len = slab.data.len();
    let local_del = if del <= slab_len - index {
        del
    } else {
        slab_len - index
    };
    let overflow_del = del - local_del;

    // Predict post-splice size without committing the work yet — if the slab
    // will overflow we want to split *before* splicing so we don't shift the
    // suffix in place only to immediately move half of it into a new slab.
    let new_len = slab_len - local_del + values.len();
    if new_len <= max_segments {
        // Fast path: in-place splice, no overflow.
        splice_bytes(&mut slab.data, index, local_del, values);
        assert(slab.data@ + Seq::<u8>::empty() =~= slab.data@);
        return (None, overflow_del);
    }
    let split_at = pick_split(index, values.len(), new_len);
    if split_at == 0 || split_at == new_len {
        // Edge split wouldn't actually reduce size — happens when a single
        // inserted blob is itself larger than `max_segments`.  Leave the
        // slab oversize (invariant still holds: the blob lives as one unit).
        splice_bytes(&mut slab.data, index, local_del, values);
        assert(slab.data@ + Seq::<u8>::empty() =~= slab.data@);
        return (None, overflow_del);
    }
    // Split first: peel the suffix (everything past the deleted region) off
    // as its own Vec, truncate the original down to just the prefix, then
    // distribute `values` to whichever side the balanced split picked.  This
    // saves the in-place suffix memmove that a post-splice split would do.
    let ghost d0 = slab.data@;
    let suffix = slab.data.split_off(index + local_del);
    slab.data.truncate(index);  // drop the deleted bytes
    let right_data = if split_at == index {
        // Split *before* the inserted bytes: left = prefix, right = values + suffix.
        let mut right = Vec::with_capacity(values.len() + suffix.len());
        right.extend_from_slice(values);
        right.extend_from_slice(suffix.as_slice());
        right
    } else {
        // Split *after* the inserted bytes: left = prefix + values, right = suffix.
        slab.data.extend_from_slice(values);
        suffix
    };
    assert(slab.data@ + right_data@ =~= d0.take(index as int) + values@ + d0.skip(index + local_del));
    (Some(RawSlab { data: right_data }), overflow_del)
}

/// `|a - b|`.
#[inline(always)]
fn abs_diff(a: usize, b: usize) -> usize {
    if a >= b {
        a - b
    } else {
        b - a
    }
}

/// Pick the byte offset at which to split a slab that has just overflowed.
///
/// Two candidate splits are considered — both at splice-point boundaries,
/// which preserves the "values never cross slabs" invariant:
///
/// * **before the inserted bytes**: `split = offset_before_insert`
///   (left = prefix, right = inserted + suffix)
/// * **after the inserted bytes**: `split = offset_before_insert + inserted_len`
///   (left = prefix + inserted, right = suffix)
///
/// Returns whichever produces the more balanced result.  Ties prefer the
/// "before" split.
fn pick_split(offset_before_insert: usize, inserted_len: usize, total_len: usize) -> (r: usize)
    requires
        offset_before_insert + inserted_len <= total_len,
    ensures
        r == offset_before_insert || r == offset_before_insert + inserted_len,
{
    let split_a = offset_before_insert;
    let imbalance_a = abs_diff(split_a, total_len - split_a);
    let split_b = offset_before_insert + inserted_len;
    let imbalance_b = abs_diff(split_b, total_len - split_b);
    if imbalance_a <= imbalance_b {
        split_a
    } else {
        split_b
    }
}

// ── Merging ─────────────────────────────────────────────────────────────────

/// The first slab's bytes come first.
proof fn lemma_flat_first(s: Seq<RawSlab>)
    requires
        s.len() > 0,
    ensures
        flat(s) == s[0].data@ + flat(s.skip(1)),
{
    lemma_flat_at(s, 0);
    assert(s.take(0) =~= Seq::<RawSlab>::empty());
    lemma_flat_unit(s[0]);
    assert(Seq::<u8>::empty() + s[0].data@ =~= s[0].data@);
}

/// Slabs `a` and `a + 1` hold no more bytes than the column.
proof fn lemma_pair_sizes(s: Seq<RawSlab>, a: int)
    requires
        0 <= a,
        a + 1 < s.len(),
    ensures
        s[a].data@.len() + s[a + 1].data@.len() <= flat(s).len(),
{
    lemma_flat_at(s, a);
    lemma_flat_at(s.skip(a + 1), 0);
}

/// Slab `a + 1` merged into slab `a` leaves the column's bytes as they
/// were.
proof fn lemma_pair_merged(s0: Seq<RawSlab>, s1: Seq<RawSlab>, a: int)
    requires
        0 <= a,
        a + 1 < s0.len(),
        s1.len() == s0.len() - 1,
        s1.take(a) == s0.take(a),
        s1.skip(a + 1) == s0.skip(a + 2),
        s1[a].data@ == s0[a].data@ + s0[a + 1].data@,
    ensures
        flat(s1) == flat(s0),
{
    hide(flat);
    lemma_flat_at(s0, a);
    lemma_flat_first(s0.skip(a + 1));
    assert(s0.skip(a + 1).skip(1) =~= s0.skip(a + 2));
    lemma_flat_at(s1, a);
    assert(flat(s1) =~= flat(s0));
}

/// Merges two adjacent slabs when either is under a quarter of the budget
/// and together they fit it; the walk around a splice is the shared
/// [`merge_around`].
struct RawMerge {
    max_segments: usize,
    /// Every edge of the slabs is one of these: a merge makes no new one.
    #[allow(dead_code)]
    base: Ghost<ISet<int>>,
}

impl Merger<RawSlab> for RawMerge {
    type X = u8;

    closed spec fn content(s: Seq<RawSlab>) -> Seq<u8> {
        flat(s)
    }

    closed spec fn inv(&self, s: Seq<RawSlab>) -> bool {
        &&& flat(s).len() <= usize::MAX
        &&& forall|x: int| #[trigger] edges(s).contains(x) ==> self.base@.contains(x)
    }

    #[verifier::spinoff_prover]
    fn try_pair(&self, slabs: &mut Vec<RawSlab>, a: usize) -> bool {
        hide(flat);
        if a >= slabs.len() || slabs.len() - a < 2 {
            return false;
        }
        let ghost s0 = slabs@;
        let max = self.max_segments;
        let min = self.max_segments / 4;
        let a_len = slabs[a].data.len();
        let b_len = slabs[a + 1].data.len();
        proof {
            lemma_pair_sizes(s0, a as int);
        }
        if (a_len < min || b_len < min) && a_len + b_len <= max {
            let slab_b = slabs.remove(a + 1);
            slabs[a].data.extend_from_slice(slab_b.data.as_slice());
            proof {
                let s1 = slabs@;
                assert(s1.take(a as int) =~= s0.take(a as int));
                assert(s1.skip(a + 1) =~= s0.skip(a + 2));
                assert(s1[a as int].data@ =~= s0[a as int].data@ + s0[a + 1].data@);
                lemma_pair_merged(s0, s1, a as int);
                lemma_edges_merged(s0, s1, a as int);
            }
            true
        } else {
            false
        }
    }
}

/// The slabs outside the merged range are old ones outside the old range.
proof fn lemma_merged_full(v: Seq<RawSlab>, v0: Seq<RawSlab>, start: int, end: int, s0: int, e0: int)
    requires
        framed(v, v0, start, end, e0),
        start <= s0,
        full_outside(v0, s0, e0),
    ensures
        full_outside(v, start, end),
{
    let drop = v0.len() - v.len();
    assert forall|i: int| 0 <= i < v.len() && !(start <= i < end) implies #[trigger] v[i].data@.len() > 0 by {
        if i < start {
            assert(v.take(start)[i] == v0.take(start)[i]);
        } else {
            assert(v.skip(end)[i - end] == v0.skip(end + drop)[i - end]);
        }
    }
}

/// Drop the empty slabs in `[start, end)`, the only place they can be.
/// Returns whether it dropped any.
fn drop_empty(slabs: &mut Vec<RawSlab>, start: usize, end: usize) -> (removed: bool)
    requires
        start <= end <= old(slabs)@.len(),
        full_outside(old(slabs)@, start as int, end as int),
    ensures
        flat(final(slabs)@) == flat(old(slabs)@),
        full(final(slabs)@),
        !removed ==> final(slabs)@ == old(slabs)@,
        forall|x: int| #[trigger] edges(final(slabs)@).contains(x) ==> edges(old(slabs)@).contains(x),
{
    // A delete can leave an empty slab behind, for example when a tiny
    // value is inserted at the front of a loaded oversize slab and then
    // removed again. Empty slabs have zero weight in the BIT but are not
    // transparent to sequential readers, so drop them before rebuilding
    // the index.
    let ghost s0 = slabs@;
    let mut k = start;
    let mut end = end;
    let mut removed = false;
    while k < end
        invariant
            start <= k <= end <= slabs@.len(),
            flat(slabs@) == flat(s0),
            forall|i: int| 0 <= i < slabs@.len() && !(k <= i < end) ==> #[trigger] slabs@[i].data@.len() > 0,
            !removed ==> slabs@ == s0,
            forall|x: int| #[trigger] edges(slabs@).contains(x) ==> edges(s0).contains(x),
        decreases end - k,
    {
        if slabs[k].data.len() == 0 {
            proof {
                lemma_flat_drop_empty(slabs@, k as int);
                lemma_edges_dropped(slabs@, k as int);
            }
            slabs.remove(k);
            end -= 1;
            removed = true;
        } else {
            k += 1;
        }
    }
    removed
}

// ── Iter ───────────────────────────────────────────────────────────────────

/// The next `n > 0` bytes of `it` fit within one slab exactly when they fit
/// within its current one.
proof fn lemma_fits_here(it: RawColumnIter, n: nat)
    requires
        it.wf(),
        n > 0,
    ensures
        it.fits(n) <==> it.slab_idx < it.slabs@.len() && it.offset_in_slab + n <= it.slabs@[it.slab_idx as int].data@.len(),
{
    let w = sizes(it.slabs@);
    let k = it.slab_idx as int;
    if k < w.len() {
        lemma_sum_split(w, 0, k, k + 1);
        lemma_sum_one(w, k);
    }
    if it.fits(n) {
        let i = choose|i: int| 0 <= i < w.len() && #[trigger] sum(w, 0, i) <= it.pos && it.pos + n <= sum(w, 0, i + 1);
        // slabs are non-empty, so `i` is the slab holding `pos`
        if i < k {
            lemma_sum_le(w, 0, i + 1, 0, k);
        } else if i > k {
            lemma_sum_le(w, 0, k + 1, 0, i);
            lemma_sum_split(w, 0, k, k + 1);
            lemma_sum_one(w, k);
            if k == w.len() {
                lemma_sum_le(w, 0, i, 0, w.len() as int);
            }
        }
    } else if k < w.len() && it.offset_in_slab + n <= w[k] {
        assert(sum(w, 0, k) <= it.pos && it.pos + n <= sum(w, 0, k + 1));
    }
}

/// Sequential byte reader over a [`RawColumn`].
///
/// Not a `std::iter::Iterator` — the natural unit of reading here is a
/// variable-sized blob, so instead of `next()` this exposes [`take`] and
/// [`skip`].  State is maintained across calls so consecutive reads don't
/// re-lookup the slab.
///
/// [`take`]: RawColumnIter::take
/// [`skip`]: RawColumnIter::skip
#[derive(Debug)]
pub struct RawColumnIter<'a> {
    slabs: &'a [RawSlab],
    slab_idx: usize,
    offset_in_slab: usize,
    /// Absolute byte position — `slab_idx` and `offset_in_slab` are the
    /// physical derivation; this mirrors them for `pos()` and `seek_to`.
    pos: usize,
}

impl<'a> Clone for RawColumnIter<'a> {
    /// An iterator at the same place of the same column.
    fn clone(&self) -> (r: Self)
        ensures
            r == *self,
    {
        RawColumnIter {
            slabs: self.slabs,
            slab_idx: self.slab_idx,
            offset_in_slab: self.offset_in_slab,
            pos: self.pos,
        }
    }
}

impl<'a> Default for RawColumnIter<'a> {
    /// The iterator of a column with no bytes.
    fn default() -> (r: Self)
        ensures
            r.wf(),
            r.bytes() == Seq::<u8>::empty(),
            r.spec_pos() == 0,
    {
        let slabs: &'a [RawSlab] = &[];
        let r = RawColumnIter { slabs, slab_idx: 0, offset_in_slab: 0, pos: 0 };
        proof {
            assert(r.slabs@ =~= Seq::<RawSlab>::empty());
            assert(datas(r.slabs@) =~= Seq::<Seq<u8>>::empty());
            assert(sizes(r.slabs@) =~= Seq::<nat>::empty());
            lemma_flat_total(r.slabs@);
        }
        r
    }
}

impl<'a> RawColumnIter<'a> {
    /// `(slab_idx, offset_in_slab)` is byte `pos` of the slabs, none of
    /// which is empty; only in the last slab can it be the slab's end.
    pub closed spec fn wf(&self) -> bool {
        let w = sizes(self.slabs@);
        &&& total(w) <= usize::MAX
        &&& full(self.slabs@)
        &&& self.slab_idx <= w.len()
        &&& self.slab_idx < w.len() ==> self.offset_in_slab <= w[self.slab_idx as int]
        &&& self.slab_idx + 1 < w.len() ==> self.offset_in_slab < w[self.slab_idx as int]
        &&& self.slab_idx == w.len() ==> self.offset_in_slab == 0
        &&& sum(w, 0, self.slab_idx as int) + self.offset_in_slab == self.pos
    }

    /// The next `n` bytes lie within one slab.
    pub closed spec fn fits(&self, n: nat) -> bool {
        let w = sizes(self.slabs@);
        n == 0 || exists|i: int|
            0 <= i < w.len() && #[trigger] sum(w, 0, i) <= self.pos && self.pos + n <= sum(
                w,
                0,
                i + 1,
            )
    }

    /// The column's bytes.
    pub closed spec fn bytes(&self) -> Seq<u8> {
        flat(self.slabs@)
    }

    pub closed spec fn spec_pos(&self) -> nat {
        self.pos as nat
    }

    /// Current absolute byte position.  Useful for suspending / later
    /// resuming via [`RawColumn::iter_at`].
    #[inline]
    pub fn pos(&self) -> (p: usize)
        ensures
            p == self.spec_pos(),
    {
        self.pos
    }

    /// Read the next `n` bytes and advance.  Panics if the read would cross
    /// a slab boundary — by the column's invariant this only happens if the
    /// caller passed a length that doesn't correspond to a single value
    /// boundary.  (Proved: it panics only if the bytes don't all lie within
    /// one slab, `fits`.)
    pub fn take(&mut self, n: usize) -> (out: &'a [u8])
        requires
            old(self).wf(),
        ensures
            old(self).fits(n as nat),
            final(self).wf(),
            final(self).bytes() == old(self).bytes(),
            old(self).spec_pos() + n <= old(self).bytes().len(),
            final(self).spec_pos() == old(self).spec_pos() + n,
            out@ == old(self).bytes().subrange(
                old(self).spec_pos() as int,
                old(self).spec_pos() + n,
            ),
    {
        let ghost w = sizes(self.slabs@);
        proof {
            lemma_flat_total(self.slabs@);
            lemma_sum_le(w, 0, self.slab_idx as int, 0, w.len() as int);
            if self.slab_idx < w.len() {
                lemma_sum_split(w, 0, self.slab_idx as int, self.slab_idx + 1);
                lemma_sum_one(w, self.slab_idx as int);
                lemma_sum_le(w, 0, self.slab_idx + 1, 0, w.len() as int);
            }
        }
        if n == 0 {
            let empty: &'a [u8] = &[];
            assert(empty@ =~= self.bytes().subrange(self.pos as int, self.pos as int));
            return empty;
        }
        proof {
            lemma_fits_here(*self, n as nat);
        }
        if self.slab_idx >= self.slabs.len() {
            fail("RawColumnIter::take past end", Ghost(!self.fits(n as nat)));
        }
        let slabs: &'a [RawSlab] = self.slabs;
        let slab: &'a RawSlab = &slabs[self.slab_idx];
        let available = slab.data.len() - self.offset_in_slab;
        if n > available {
            fail_take(n, available, Ghost(!self.fits(n as nat)));
        }
        let out = slice_subrange(
            slab.data.as_slice(),
            self.offset_in_slab,
            self.offset_in_slab + n,
        );
        proof {
            lemma_flat_at(self.slabs@, self.slab_idx as int);
            lemma_flat_len(self.slabs@, self.slab_idx as int);
            assert(out@ =~= self.bytes().subrange(self.pos as int, self.pos + n));
        }
        self.offset_in_slab += n;
        self.pos += n;
        if self.offset_in_slab >= slab.data.len() && self.slab_idx + 1 < self.slabs.len() {
            self.slab_idx += 1;
            self.offset_in_slab = 0;
        }
        out
    }

    /// Advance the cursor by `n` bytes, crossing slab boundaries as needed.
    /// Panics if `n` walks past the end of the column.
    pub fn skip(&mut self, n: usize)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self).bytes() == old(self).bytes(),
            old(self).spec_pos() + n <= old(self).bytes().len(),
            final(self).spec_pos() == old(self).spec_pos() + n,
    {
        let ghost w = sizes(self.slabs@);
        let ghost p0 = self.pos;
        let mut remaining = n;
        while remaining > 0
            invariant
                w == sizes(self.slabs@),
                total(w) <= usize::MAX,
                full(self.slabs@),
                self.slab_idx <= w.len(),
                self.slab_idx < w.len() ==> self.offset_in_slab <= w[self.slab_idx as int],
                self.slab_idx + 1 < w.len() ==> self.offset_in_slab < w[self.slab_idx as int],
                self.slab_idx == w.len() ==> self.offset_in_slab == 0,
                sum(w, 0, self.slab_idx as int) + self.offset_in_slab + remaining == p0 + n,
                self.pos == p0,
                self.slabs == old(self).slabs,
            decreases w.len() - self.slab_idx, remaining,
        {
            if self.slab_idx >= self.slabs.len() {
                proof {
                    lemma_flat_total(self.slabs@);
                }
                fail("RawColumnIter::skip past end", Ghost(p0 + n > self.bytes().len()));
            }
            let available = self.slabs[self.slab_idx].data.len() - self.offset_in_slab;
            proof {
                lemma_sum_split(w, 0, self.slab_idx as int, self.slab_idx + 1);
                lemma_sum_one(w, self.slab_idx as int);
            }
            if remaining < available {
                self.offset_in_slab += remaining;
                remaining = 0;
            } else {
                remaining -= available;
                self.slab_idx += 1;
                self.offset_in_slab = 0;
            }
        }
        proof {
            lemma_flat_total(self.slabs@);
            lemma_sum_le(w, 0, self.slab_idx as int, 0, w.len() as int);
            if self.slab_idx < w.len() {
                lemma_sum_split(w, 0, self.slab_idx as int, self.slab_idx + 1);
                lemma_sum_one(w, self.slab_idx as int);
                lemma_sum_le(w, 0, self.slab_idx + 1, 0, w.len() as int);
            }
        }
        self.pos += n;
    }

    /// Forward-only absolute seek.  `target` must be `>= pos()`; use
    /// [`RawColumn::iter_at`] for arbitrary seeks.  Equivalent to
    /// `self.skip(target - self.pos())` with a bounds check.
    pub fn seek_to(&mut self, target: usize)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self).bytes() == old(self).bytes(),
            old(self).spec_pos() <= target <= old(self).bytes().len(),
            final(self).spec_pos() == target,
    {
        if target < self.pos {
            fail_seek(self.pos, target, Ghost(target < self.spec_pos()));
        }
        self.skip(target - self.pos);
    }
}

impl Default for RawColumn {
    /// A column with no bytes.
    fn default() -> (r: Self)
        ensures
            r.wf(),
            r@ == Seq::<u8>::empty(),
            forall|x: int| #[trigger] r.cuts().contains(x) ==> x == 0,
    {
        Self::new()
    }
}

impl RawColumn {
    // ── Range read ──────────────────────────────────────────────────────────

    /// Return a contiguous slice for `range`.
    /// Panics if `range` is out of bounds or crosses a slab boundary.
    ///
    /// A verified caller shows that it is neither: the `requires` is what
    /// [`try_get`](Self::try_get) gives `Ok` for.
    pub fn get(&self, range: Range<usize>) -> (out: &[u8])
        requires
            self.wf(),
            range.start <= range.end <= self@.len(),
            range.start == range.end || self.in_one_slab(range.start as int, range.end as int),
        ensures
            out@ == self@.subrange(range.start as int, range.end as int),
    {
        self.try_get(range).expect("raw get: OOB or cross-slab range")
    }

    /// Non-panicking variant of [`get`](Self::get).
    pub fn try_get(&self, range: Range<usize>) -> (r: Result<&[u8], PackError>)
        requires
            self.wf(),
        ensures
            r is Ok <==> range.start <= range.end <= self@.len() && (range.start == range.end
                || self.in_one_slab(range.start as int, range.end as int)),
            r is Ok ==> r->Ok_0@ == self@.subrange(range.start as int, range.end as int),
    {
        self.get_span(range.start, range.end)
    }

    // ── Wire format ─────────────────────────────────────────────────────────

    /// Append the column's bytes to `out`; returns where they are.
    pub fn save_to(&self, out: &mut Vec<u8>) -> (r: Range<usize>)
        ensures
            final(out)@ == old(out)@ + self@,
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        let start = out.len();
        self.append_to(out);
        start..out.len()
    }

    /// Deserialize from `data`.  Everything lands in a single slab — the
    /// caller knows where value boundaries are and can splice further if
    /// they want splits.
    pub fn load(data: &[u8]) -> (r: Result<Self, PackError>)
        ensures
            r is Ok,
            r->Ok_0.wf(),
            r->Ok_0@ == data@,
            forall|x: int| #[trigger] r->Ok_0.cuts().contains(x) ==> x == 0 || x == data@.len(),
    {
        Self::load_with_max_segments(data, DEFAULT_MAX_SEGMENTS)
    }
}

} // verus!

impl RawColumn {
    /// Insert / delete / replace bytes at `index`, taking a stream of byte
    /// slices for the inserted content.  Panics if `index + del` exceeds
    /// the column length.
    ///
    /// Use this when the caller already has bytes broken into per-item
    /// chunks (e.g. one `raw_value()` per op).  The chunks are concatenated
    /// internally before handing off to [`splice_slice`](Self::splice_slice).
    /// Zero- and single-chunk inputs skip the concatenation buffer.
    pub fn splice<I, B>(&mut self, index: usize, del: usize, chunks: I)
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        self.try_splice(index, del, chunks)
            .expect("splice range out of bounds")
    }

    /// Non-panicking variant of [`splice`](Self::splice).
    pub fn try_splice<I, B>(&mut self, index: usize, del: usize, chunks: I) -> Result<(), PackError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        let mut iter = chunks.into_iter();
        let first = iter.next();
        match first {
            None => self.try_splice_slice(index, del, &[]),
            Some(first) => match iter.next() {
                None => {
                    // Single chunk — forward without allocating a concat buffer.
                    self.try_splice_slice(index, del, first.as_ref())
                }
                Some(second) => {
                    let first = first.as_ref();
                    let second = second.as_ref();
                    let mut buf = Vec::with_capacity(first.len() + second.len());
                    buf.extend_from_slice(first);
                    buf.extend_from_slice(second);
                    for chunk in iter {
                        buf.extend_from_slice(chunk.as_ref());
                    }
                    self.try_splice_slice(index, del, &buf)
                }
            },
        }
    }

    // ── Multi-point copy ────────────────────────────────────────────────────

    /// Splice byte ranges of `src` into this column at several points in
    /// one pass.
    ///
    /// `pos` / `delete` are in this column's *pre-merge* byte coordinates
    /// (as for [`Column::copy_ranges`](crate::Column::copy_ranges)) and
    /// must ascend without overlapping; `range` is a byte range of `src`,
    /// likewise ascending.
    ///
    /// This is [`splice_slice`](Self::splice_slice) in a loop, applied back
    /// to front so that each edit lands *above* every splice still to be
    /// applied and their positions stay valid without any shift
    /// bookkeeping. A typed [`Column`](crate::Column) needs a bulk path
    /// because splicing decodes and re-encodes runs; raw bytes have no
    /// such cost, so a bulk path here would buy nothing but a way to
    /// diverge from the single-point policy — which is what the previous
    /// implementation did, cutting the destination at every splice point
    /// and never coalescing, so a chain of small merges fragmented the
    /// arena into one slab per insertion point.
    ///
    /// # Boundaries
    ///
    /// The column does not know where values begin and end, so the caller
    /// must give every `pos` and every `range` endpoint on a value
    /// boundary — exactly the existing splice contract. Splits only ever
    /// happen at a splice point or at the end of an inserted payload
    /// (see `pick_split`), so no value ends up spanning a slab.
    pub fn copy_ranges<I>(&mut self, src: Self, splices: I)
    where
        I: IntoIterator<Item = crate::Splice>,
    {
        let splices = normalize_splices(splices, src.len(), self.total_len);
        // verified (`RawColumn::copy_from`): applied back to front, each
        // range spliced a chunk per source slab, the column becomes the
        // splices applied one by one
        self.copy_from(&src, &splices);
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Splice;

    fn drive(col: &RawColumn, expected: &[u8]) {
        assert_eq!(col.len(), expected.len(), "len mismatch");
        assert_eq!(col.save(), expected, "save bytes mismatch");
    }

    #[test]
    fn empty_column() {
        let col = RawColumn::new();
        assert!(col.is_empty());
        assert_eq!(col.len(), 0);
        assert_eq!(col.save(), Vec::<u8>::new());
    }

    #[test]
    fn single_slab_append() {
        let mut col = RawColumn::with_max_segments(32);
        col.splice_slice(0, 0, b"hello");
        col.splice_slice(5, 0, b", world");
        drive(&col, b"hello, world");
    }

    #[test]
    fn single_slab_insert_middle() {
        let mut col = RawColumn::with_max_segments(32);
        col.splice_slice(0, 0, b"helloworld");
        col.splice_slice(5, 0, b", ");
        drive(&col, b"hello, world");
    }

    #[test]
    fn single_slab_delete() {
        let mut col = RawColumn::with_max_segments(32);
        col.splice_slice(0, 0, b"hello, world");
        col.splice_slice(5, 2, b""); // delete ", "
        drive(&col, b"helloworld");
    }

    #[test]
    fn single_slab_replace() {
        let mut col = RawColumn::with_max_segments(32);
        col.splice_slice(0, 0, b"hello, world");
        col.splice_slice(7, 5, b"hexane");
        drive(&col, b"hello, hexane");
    }

    #[test]
    fn splice_overflow_splits() {
        let mut col = RawColumn::with_max_segments(8);
        col.splice_slice(0, 0, b"abcdef"); // 6 bytes — fits
        col.splice_slice(3, 0, b"XYZ"); // becomes "abcXYZdef" = 9 bytes — overflow
        drive(&col, b"abcXYZdef");
        // Should have produced 2 slabs.
        assert_eq!(col.slabs.len(), 2, "expected split into 2 slabs");
    }

    #[test]
    fn balanced_split_picks_better_side() {
        // After insert the slab is [PPPP | X | SSSSSSSS] (prefix 4, insert 1, suffix 8).
        // Option A: split before insert → (4 | 9). Imbalance 5.
        // Option B: split after insert → (5 | 8). Imbalance 3.
        // Should pick B.
        let mut col = RawColumn::with_max_segments(10);
        col.splice_slice(0, 0, b"PPPPSSSSSSSS"); // 12 bytes, forces split at construction
                                                 // Clear and rebuild cleanly with no pre-split.
        let mut col = RawColumn::with_max_segments(15);
        col.splice_slice(0, 0, b"PPPPSSSSSSSS"); // 12 bytes, fits
        col.splice_slice(4, 0, b"X"); // 13 bytes — still fits (< 15), no overflow
        drive(&col, b"PPPPXSSSSSSSS");

        // Now force the overflow.
        let mut col = RawColumn::with_max_segments(12);
        col.splice_slice(0, 0, b"PPPPSSSSSSSS"); // exactly 12 bytes
        col.splice_slice(4, 0, b"X"); // 13 bytes — overflow by 1
        drive(&col, b"PPPPXSSSSSSSS");
        assert_eq!(col.slabs.len(), 2);
        // Better split is "after insert": left=5 bytes, right=8.
        assert_eq!(col.slabs[0].data, b"PPPPX");
        assert_eq!(col.slabs[1].data, b"SSSSSSSS");
    }

    #[test]
    fn balanced_split_picks_before_when_ties_or_better() {
        // Insert near the end: PPPPPPPPP | X | S (9 + 1 + 1 = 11).
        // Option A: before insert → (9 | 2), imbalance 7.
        // Option B: after insert  → (10 | 1), imbalance 9.
        // Should pick A.
        let mut col = RawColumn::with_max_segments(10);
        col.splice_slice(0, 0, b"PPPPPPPPPS");
        col.splice_slice(9, 0, b"X");
        drive(&col, b"PPPPPPPPPXS");
        assert_eq!(col.slabs.len(), 2);
        assert_eq!(col.slabs[0].data, b"PPPPPPPPP");
        assert_eq!(col.slabs[1].data, b"XS");
    }

    #[test]
    fn oversize_blob_gets_own_slab() {
        // Blob larger than max_segments — cannot split (would cross value
        // boundaries), so it lives in one oversize slab.
        let mut col = RawColumn::with_max_segments(4);
        col.splice_slice(0, 0, b"xxxxxxxxxxxxxxxx"); // 16 bytes > max 4
        drive(&col, b"xxxxxxxxxxxxxxxx");
        // Either one oversize slab or (if split at edge skipped) something,
        // but `get(0..16)` must return the whole thing intact.
        assert_eq!(col.get(0..16), b"xxxxxxxxxxxxxxxx");
    }

    #[test]
    fn get_within_slab() {
        let mut col = RawColumn::with_max_segments(32);
        col.splice_slice(0, 0, b"hello, world");
        assert_eq!(col.get(0..5), b"hello");
        assert_eq!(col.get(7..12), b"world");
        assert_eq!(col.get(0..12), b"hello, world");
        assert_eq!(col.get(5..5), b"");
    }

    #[test]
    #[should_panic(expected = "cross-slab")]
    fn get_cross_slab_panics() {
        let mut col = RawColumn::with_max_segments(4);
        col.splice_slice(0, 0, b"abcd"); // slab 0 full
        col.splice_slice(4, 0, b"efgh"); // slab 1
                                         // col is now split into two slabs; get a range spanning both.
        let _ = col.get(2..6);
    }

    #[test]
    fn try_get_cross_slab_errors() {
        let mut col = RawColumn::with_max_segments(4);
        col.splice_slice(0, 0, b"abcd");
        col.splice_slice(4, 0, b"efgh");
        assert!(col.try_get(2..6).is_err(), "expected cross-slab error");
    }

    #[test]
    fn try_get_oob_errors() {
        let mut col = RawColumn::new();
        col.splice_slice(0, 0, b"abc");
        assert!(col.try_get(0..10).is_err());
        assert!(col.try_get(5..6).is_err());
    }

    #[test]
    fn try_splice_oob_errors() {
        let mut col = RawColumn::new();
        col.splice_slice(0, 0, b"abc");
        assert!(col.try_splice_slice(5, 0, b"x").is_err());
        assert!(col.try_splice_slice(0, 10, b"").is_err());
    }

    #[test]
    fn multi_slab_splice_across_boundary() {
        let mut col = RawColumn::with_max_segments(4);
        // Build up three slabs of content.
        col.splice_slice(0, 0, b"aaaa"); // slab 0 (4 bytes)
        col.splice_slice(4, 0, b"bbbb"); // pushes into slab 1 via split
        col.splice_slice(8, 0, b"cccc"); // pushes into slab 2
        drive(&col, b"aaaabbbbcccc");

        // Delete 6 bytes starting at position 3 (crosses into slab 2).
        col.splice_slice(3, 6, b"");
        drive(&col, b"aaaccc");
    }

    #[test]
    fn merge_after_tiny_splices() {
        // Drive many tiny splices into a column with small max_segments;
        // then delete enough to trigger merges.  Sanity-check that slab
        // count stays bounded.
        let mut col = RawColumn::with_max_segments(16);
        for i in 0..100 {
            let byte = b'a' + (i % 26) as u8;
            col.splice_slice(col.len(), 0, &[byte]);
        }
        let len_before = col.len();
        let slabs_before = col.slabs.len();
        assert!(slabs_before > 1, "should have multiple slabs");

        // Delete most bytes — merges should happen.
        col.splice_slice(0, len_before - 10, b"");
        assert_eq!(col.len(), 10);
        // Slab count should have shrunk (not necessarily to 1, but <= ceil(10/16) ish).
        assert!(
            col.slabs.len() <= slabs_before,
            "slabs didn't shrink: before={} after={}",
            slabs_before,
            col.slabs.len(),
        );
    }

    #[test]
    fn iter_take_and_skip() {
        let mut col = RawColumn::with_max_segments(32);
        col.splice_slice(0, 0, b"hello, world");
        let mut it = col.iter();
        assert_eq!(it.take(5), b"hello");
        it.skip(2);
        assert_eq!(it.take(5), b"world");
    }

    #[test]
    fn iter_across_slabs_with_skip() {
        let mut col = RawColumn::with_max_segments(4);
        col.splice_slice(0, 0, b"abcd");
        col.splice_slice(4, 0, b"efgh");
        col.splice_slice(8, 0, b"ijkl");
        let mut it = col.iter();
        // Take bytes from within each slab; skip across boundaries.
        assert_eq!(it.take(4), b"abcd");
        it.skip(1); // skip 'e'
        assert_eq!(it.take(3), b"fgh");
        assert_eq!(it.take(4), b"ijkl");
    }

    #[test]
    fn iter_at_seeks() {
        let mut col = RawColumn::with_max_segments(4);
        col.splice_slice(0, 0, b"abcd");
        col.splice_slice(4, 0, b"efgh");
        let mut it = col.iter_at(4);
        assert_eq!(it.take(4), b"efgh");
    }

    #[test]
    #[should_panic(expected = "cross slab boundary")]
    fn iter_take_cross_slab_panics() {
        let mut col = RawColumn::with_max_segments(4);
        col.splice_slice(0, 0, b"abcd");
        col.splice_slice(4, 0, b"efgh");
        let mut it = col.iter();
        // take(8) would cross — must panic.
        let _ = it.take(8);
    }

    #[test]
    fn delete_front_insert_from_loaded_oversize_slab() {
        let mut col = RawColumn::load_with_max_segments(b"abcdef", 4).unwrap();
        assert_eq!(col.slabs.len(), 1);

        col.splice_slice(0, 0, b"X");
        assert_eq!(col.slabs[0].data, b"X");
        col.splice_slice(0, 1, b"");

        drive(&col, b"abcdef");
        let mut it = col.iter();
        assert_eq!(it.take(1), b"a");
    }

    #[test]
    fn load_save_roundtrip() {
        let data = b"The quick brown fox jumps over the lazy dog".to_vec();
        let col = RawColumn::load(&data).unwrap();
        assert_eq!(col.len(), data.len());
        assert_eq!(col.save(), data);
    }

    #[test]
    fn load_empty() {
        let col = RawColumn::load(&[]).unwrap();
        assert!(col.is_empty());
        assert_eq!(col.save(), Vec::<u8>::new());
    }

    #[test]
    fn save_to_range() {
        let mut col = RawColumn::with_max_segments(4);
        col.splice_slice(0, 0, b"abcd");
        col.splice_slice(4, 0, b"efgh");
        let mut out = b"prefix:".to_vec();
        let range = col.save_to(&mut out);
        assert_eq!(&out[range.clone()], b"abcdefgh");
        assert_eq!(&out[..range.start], b"prefix:");
    }

    // ── copy_ranges ─────────────────────────────────────────────────────────

    /// Reference model: apply the splices to a plain `Vec<u8>`.
    fn reference_copy(dst: &[u8], src: &[u8], splices: &[Splice]) -> Vec<u8> {
        let mut out = dst.to_vec();
        let mut shift = 0isize;
        for sp in splices {
            let at = (sp.pos as isize + shift) as usize;
            let bytes = &src[sp.range.clone()];
            out.splice(at..at + sp.delete, bytes.iter().copied());
            shift += sp.range.len() as isize - sp.delete as isize;
        }
        out
    }

    fn assert_copy(dst_bytes: &[u8], src_bytes: &[u8], max_seg: usize, splices: Vec<Splice>) {
        let mut dst = RawColumn::with_max_segments(max_seg);
        dst.splice_slice(0, 0, dst_bytes);
        let mut src = RawColumn::with_max_segments(max_seg);
        src.splice_slice(0, 0, src_bytes);

        let expected = reference_copy(dst_bytes, src_bytes, &splices);
        dst.copy_ranges(src, splices);

        assert_eq!(dst.save(), expected, "merged bytes");
        assert_eq!(dst.len(), expected.len(), "merged len");
        // the BIT and the slab vector must agree, and no slab may be empty
        assert!(dst.slabs.iter().all(|s| !s.data.is_empty()), "empty slab");
        assert_eq!(
            dst.bit,
            fenwick::build(&dst.slabs, |s: &RawSlab| s.data.len(), Ghost::assume_new()),
            "BIT out of sync"
        );
        // every byte must still be reachable through the sequential reader
        let mut it = dst.iter();
        let mut seen = Vec::new();
        while it.pos() < dst.len() {
            seen.extend_from_slice(it.take(1));
        }
        assert_eq!(seen, expected, "sequential read");
    }

    #[test]
    fn copy_ranges_single_append() {
        assert_copy(
            b"abcd",
            b"WXYZ",
            32,
            vec![Splice {
                pos: 4,
                delete: 0,
                range: 0..4,
            }],
        );
    }

    /// A chain of small copies into a growing arena must not fragment it:
    /// the previous implementation cut the destination at every splice
    /// point and never coalesced, so the slab count tracked the number of
    /// insertions rather than the byte count.
    #[test]
    fn copy_ranges_keeps_the_arena_compact() {
        let budget = 256;
        let mut dst = RawColumn::with_max_segments(budget);
        dst.splice_slice(0, 0, &vec![b'x'; 8 * budget]);
        let mut expected = vec![b'x'; 8 * budget];
        // 400 scattered 4-byte copies, the shape a fragment chain has
        for i in 0..400 {
            let mut src = RawColumn::with_max_segments(budget);
            src.splice_slice(0, 0, b"abcd");
            let at = ((i * 37) % (dst.len() - 4)) & !3;
            dst.copy_ranges(
                src,
                [Splice {
                    pos: at,
                    delete: 0,
                    range: 0..4,
                }],
            );
            expected.splice(at..at, *b"abcd");
        }
        assert_eq!(dst.save(), expected, "bytes");
        // bounded by bytes, not by the 400 insertion points
        let slabs = dst.slabs.len();
        assert!(
            slabs <= 2 * dst.len().div_ceil(budget),
            "arena fragmented: {slabs} slabs for {} bytes (budget {budget})",
            dst.len()
        );
    }

    #[test]
    fn copy_ranges_multi_point() {
        assert_copy(
            b"aaaabbbbcccc",
            b"XXYYZZ",
            32,
            vec![
                Splice {
                    pos: 0,
                    delete: 0,
                    range: 0..2,
                },
                Splice {
                    pos: 4,
                    delete: 0,
                    range: 2..4,
                },
                Splice {
                    pos: 12,
                    delete: 0,
                    range: 4..6,
                },
            ],
        );
    }

    #[test]
    fn copy_ranges_with_deletes() {
        assert_copy(
            b"aaaabbbbcccc",
            b"XXXXYYYY",
            32,
            vec![
                Splice {
                    pos: 0,
                    delete: 4,
                    range: 0..4,
                },
                Splice {
                    pos: 8,
                    delete: 4,
                    range: 4..8,
                },
            ],
        );
    }

    #[test]
    fn copy_ranges_skips_gaps_in_src() {
        // the middle of src is never referenced — those slabs are dropped
        assert_copy(
            b"..",
            b"AAAABBBBCCCC",
            4,
            vec![
                Splice {
                    pos: 1,
                    delete: 0,
                    range: 0..4,
                },
                Splice {
                    pos: 2,
                    delete: 0,
                    range: 8..12,
                },
            ],
        );
    }

    #[test]
    fn copy_ranges_empty_dst() {
        assert_copy(
            b"",
            b"hello",
            32,
            vec![Splice {
                pos: 0,
                delete: 0,
                range: 0..5,
            }],
        );
    }

    #[test]
    fn copy_ranges_noop_when_empty() {
        let mut dst = RawColumn::with_max_segments(8);
        dst.splice_slice(0, 0, b"abcd");
        let src = RawColumn::load(b"xyz").unwrap();
        dst.copy_ranges(src, std::iter::empty());
        drive(&dst, b"abcd");
    }

    /// The point of the exercise: a big source merged into a small
    /// destination must carry the source's slabs across whole, so its
    /// value boundaries survive and `get` never reports a cross-slab read.
    #[test]
    fn copy_ranges_preserves_src_slab_boundaries() {
        // 12 four-byte "values", one per splice, so every value boundary
        // is a slab boundary in src
        let mut src = RawColumn::with_max_segments(4);
        for i in 0..12u8 {
            let v = [b'a' + i; 4];
            src.splice_slice(src.len(), 0, &v);
        }
        assert_eq!(src.slabs.len(), 12, "one slab per value");

        let mut dst = RawColumn::with_max_segments(4);
        dst.splice_slice(0, 0, b"0000");

        dst.copy_ranges(
            src,
            vec![
                Splice {
                    pos: 0,
                    delete: 0,
                    range: 0..24,
                },
                Splice {
                    pos: 4,
                    delete: 0,
                    range: 24..48,
                },
            ],
        );

        assert_eq!(dst.len(), 52);
        // no byte copying: the 12 source slabs plus the untouched dst slab
        assert_eq!(dst.slabs.len(), 13, "src slabs carried across whole");
        // every value is still readable as one contiguous slice
        for i in 0..13 {
            assert_eq!(dst.get(i * 4..i * 4 + 4).len(), 4);
        }
    }

    #[test]
    fn copy_ranges_cuts_src_mid_slab() {
        // ranges that do not line up with src's slab boundaries still work;
        // only the straddling slabs pay a split
        assert_copy(
            b"__",
            b"abcdefghijkl",
            32, // src is one slab
            vec![
                Splice {
                    pos: 1,
                    delete: 0,
                    range: 2..5,
                },
                Splice {
                    pos: 2,
                    delete: 0,
                    range: 7..11,
                },
            ],
        );
    }

    #[test]
    fn fuzz_copy_ranges_vs_vec() {
        use rand::{RngExt, SeedableRng};

        let mut rng = rand::rngs::SmallRng::seed_from_u64(0xc0de_1234);
        for round in 0..500 {
            let max_seg = [4usize, 7, 16, 4096][round % 4];
            let dst_len = rng.random_range(0..40);
            let src_len = rng.random_range(0..60);
            let dst_bytes: Vec<u8> = (0..dst_len).map(|_| rng.random::<u8>()).collect();
            let src_bytes: Vec<u8> = (0..src_len).map(|_| rng.random::<u8>()).collect();

            // ascending, non-overlapping on both sides
            let mut splices = Vec::new();
            let mut dst_pos = 0usize;
            let mut src_pos = 0usize;
            while dst_pos <= dst_len && src_pos < src_len {
                let pos = dst_pos + rng.random_range(0..=(dst_len - dst_pos));
                let delete = rng.random_range(0..=(dst_len - pos).min(3));
                let start = src_pos + rng.random_range(0..=(src_len - src_pos).min(5));
                let end = start + rng.random_range(0..=(src_len - start).min(8));
                splices.push(Splice {
                    pos,
                    delete,
                    range: start..end,
                });
                dst_pos = pos + delete;
                src_pos = end;
                if rng.random_range(0..3) == 0 {
                    break;
                }
            }
            assert_copy(&dst_bytes, &src_bytes, max_seg, splices);
        }
    }

    // ── Randomised fuzz ─────────────────────────────────────────────────────

    #[test]
    fn fuzz_splice_vs_vec() {
        use rand::{RngExt, SeedableRng};

        let mut col = RawColumn::with_max_segments(16);
        let mut reference: Vec<u8> = Vec::new();
        let mut rng = rand::rngs::SmallRng::seed_from_u64(0x1234_5678);
        for _ in 0..5_000 {
            let op = rng.random_range(0..3);
            let len = reference.len();
            match op {
                // insert
                0 => {
                    let at = rng.random_range(0..=len);
                    let n = rng.random_range(1..=8);
                    let bytes: Vec<u8> = (0..n).map(|_| rng.random::<u8>()).collect();
                    col.splice_slice(at, 0, &bytes);
                    reference.splice(at..at, bytes);
                }
                // delete
                1 if len > 0 => {
                    let at = rng.random_range(0..len);
                    let del = rng.random_range(1..=(len - at).min(6));
                    col.splice_slice(at, del, b"");
                    reference.drain(at..at + del);
                }
                // replace
                _ if len > 0 => {
                    let at = rng.random_range(0..len);
                    let del = rng.random_range(1..=(len - at).min(4));
                    let n = rng.random_range(1..=6);
                    let bytes: Vec<u8> = (0..n).map(|_| rng.random::<u8>()).collect();
                    col.splice_slice(at, del, &bytes);
                    reference.splice(at..at + del, bytes);
                }
                _ => {}
            }
            assert_eq!(col.len(), reference.len());
            assert_eq!(col.save(), reference);
        }
    }
}
