//! The splices [`copy_ranges`](crate::Column::copy_ranges) takes, verified
//! with Verus.
//!
//! A splice deletes `delete` items of the destination at `pos` and puts the
//! source's items in `range` there; a list of them, ascending in both
//! columns, edits the destination in one pass. `copy_ranges` first
//! normalizes the list: clamps each range to the source (a reversed range
//! is empty, as a `Range` is; an empty range sits where the last range
//! kept ended), drops splices that do nothing, and merges a splice into
//! the one before where they touch in both columns.
//!
//! The model: `applied` is the destination after the splices, as a
//! sequence. Proved: [`Normalize`] checks the splices it takes (it panics
//! on ones out of order or out of bounds), and the list it gives back is
//! normalized (`normal`), and applied to any destination and source of
//! the given lengths gives what the splices it took give.
//!
//! `copy_ranges` then carries the list out one of two ways. The direct
//! way applies it to the destination with the edit cursor. The inverted
//! way keeps the source instead: it cuts out the gaps between the ranges,
//! back to front, and puts the destination's gaps back between them.
//! Proved for that: the cuts [`Cuts`] gives, made in turn, leave exactly
//! the ranges' items (`kept`), and the gaps [`Gaps`] gives, as splices
//! inserting them, are a normalized list that, applied to those with the
//! destination as its source, gives what the splices give. So both ways
//! come down to the edit cursor applying a normalized list, which
//! `crate::copy` proves of both. `Normalize` works on the columns of
//! positions (`ids`, `negs`), and what splices in order make of a
//! destination and source depends only on their lengths
//! (`lemma_any_columns`), so its result holds for any columns.
//! [`normalize`] feeds it the caller's splices and hands the copies a
//! [`Normalized`] list, whose type invariant carries this, so each copy
//! is proved to give what the caller's splices give one by one.

use crate::Splice;
use std::ops::Range;
use vstd::prelude::*;

verus! {

#[verifier::external_type_specification]
#[allow(dead_code)]
pub struct ExSplice(Splice);

/// Panics: the splices are out of order or out of bounds.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn bad_splices(why: &str)
    ensures
        false,
{
    panic!("{}", why)
}

/// Panics: a normalized list used for columns of other lengths than it
/// was made for.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn wrong_columns()
    ensures
        false,
{
    panic!("splices normalized for columns of other lengths")
}

pub open spec fn min_i(a: int, b: int) -> int {
    if a <= b { a } else { b }
}

/// The source items a splice's range copies: clamped to the source, and
/// none for a reversed range.
pub open spec fn copied<V>(src: Seq<V>, sp: Splice) -> Seq<V> {
    let s = min_i(sp.range.start as int, src.len() as int);
    let e = min_i(sp.range.end as int, src.len() as int);
    if s < e { src.subrange(s, e) } else { Seq::empty() }
}

/// Where the last of `sps` ends in the destination (0 for none).
pub open spec fn end_of(sps: Seq<Splice>) -> int {
    if sps.len() == 0 { 0 } else { sps.last().pos + sps.last().delete }
}

/// The destination's items up to where the last splice ends, edited.
pub open spec fn produced<V>(dst: Seq<V>, src: Seq<V>, sps: Seq<Splice>) -> Seq<V>
    decreases sps.len(),
{
    if sps.len() == 0 {
        Seq::empty()
    } else {
        let p = sps.drop_last();
        produced(dst, src, p) + dst.subrange(end_of(p), sps.last().pos as int) + copied(src, sps.last())
    }
}

/// The destination with splices `sps` applied, one after another.
pub open spec fn applied<V>(dst: Seq<V>, src: Seq<V>, sps: Seq<Splice>) -> Seq<V> {
    produced(dst, src, sps) + dst.subrange(end_of(sps), dst.len() as int)
}

/// Splices ascending in the destination and inside it.
pub open spec fn in_order(sps: Seq<Splice>, dst_len: int) -> bool {
    &&& forall|i: int| 0 <= i < sps.len() ==> #[trigger] sps[i].pos + sps[i].delete <= dst_len
    &&& forall|i: int| 0 < i < sps.len() ==> sps[i - 1].pos + sps[i - 1].delete <= #[trigger] sps[i].pos
}

/// A normalized list: in order in both columns, ranges inside the
/// source and not reversed, and none doing nothing.
pub(crate) open spec fn normal(sps: Seq<Splice>, src_len: int, dst_len: int) -> bool {
    &&& in_order(sps, dst_len)
    &&& forall|i: int| 0 <= i < sps.len() ==> #[trigger] sps[i].range.start <= sps[i].range.end <= src_len
    &&& forall|i: int| 0 <= i < sps.len() ==> #[trigger] sps[i].range.start < sps[i].range.end
        || sps[i].delete > 0
    &&& forall|i: int| 0 < i < sps.len() ==> sps[i - 1].range.end <= #[trigger] sps[i].range.start
}

/// The ranges of the splices `sps` ascend in the source: each ends
/// where the ones after it start, or before.
pub open spec fn ranges_ascend(sps: Seq<Splice>) -> bool {
    forall|i: int, j: int| #![trigger sps[i], sps[j]] 0 <= i < j < sps.len() ==> sps[i].range.end <= sps[j].range.start
}

/// What the splice `sp` does, with a source of `src_len` items: it
/// deletes something, or copies something.
pub open spec fn splice_does(sp: Splice, src_len: int) -> bool {
    sp.delete > 0 || min_i(sp.range.start as int, src_len) < min_i(sp.range.end as int, src_len)
}

/// One of the splices `sps` does something (`splice_does`).
pub open spec fn splices_do(sps: Seq<Splice>, src_len: int) -> bool {
    exists|i: int| 0 <= i < sps.len() && splice_does(#[trigger] sps[i], src_len)
}

/// Whether one of the splices `sps` does something, with a source of
/// `src_len` items.
pub(crate) fn does_anything(sps: &[Splice], src_len: usize) -> (r: bool)
    ensures
        r == splices_do(sps@, src_len as int),
{
    let mut i: usize = 0;
    while i < sps.len()
        invariant
            i <= sps.len(),
            forall|j: int| 0 <= j < i ==> !splice_does(#[trigger] sps@[j], src_len as int),
        decreases sps.len() - i,
    {
        let s = if sps[i].range.start < src_len { sps[i].range.start } else { src_len };
        let e = if sps[i].range.end < src_len { sps[i].range.end } else { src_len };
        if sps[i].delete > 0 || s < e {
            proof {
                assert(splice_does(sps@[i as int], src_len as int));
            }
            return true;
        }
        i += 1;
    }
    false
}

/// The columns of positions: `ids(n)` numbers a destination's items
/// `0..n`, `negs(m)` a source's `-1, -2, ..`, so no item of one is an
/// item of the other.
pub(crate) open spec fn ids(n: nat) -> Seq<int> {
    Seq::new(n, |i: int| i)
}

pub(crate) open spec fn negs(n: nat) -> Seq<int> {
    Seq::new(n, |j: int| -j - 1)
}

/// A splice copies the mapped items of the mapped source.
proof fn lemma_copied_map<V, W>(s: Seq<V>, sp: Splice, f: spec_fn(V) -> W)
    ensures
        copied(s.map_values(f), sp) == copied(s, sp).map_values(f),
{
    let st = min_i(sp.range.start as int, s.len() as int);
    let e = min_i(sp.range.end as int, s.len() as int);
    if st < e {
        assert(s.map_values(f).subrange(st, e) =~= s.subrange(st, e).map_values(f));
    } else {
        assert(Seq::<V>::empty().map_values(f) =~= Seq::<W>::empty());
    }
}

/// Splices in order make of mapped columns the mapped result.
proof fn lemma_produced_map<V, W>(d: Seq<V>, s: Seq<V>, sps: Seq<Splice>, f: spec_fn(V) -> W)
    requires
        in_order(sps, d.len() as int),
    ensures
        produced(d.map_values(f), s.map_values(f), sps) == produced(d, s, sps).map_values(f),
    decreases sps.len(),
{
    hide(in_order);
    if sps.len() == 0 {
        assert(Seq::<V>::empty().map_values(f) =~= Seq::<W>::empty());
    } else {
        let p = sps.drop_last();
        let l = sps.last();
        lemma_in_order_front(sps, d.len() as int);
        lemma_produced_map(d, s, p, f);
        lemma_copied_map(s, l, f);
        let a = end_of(p);
        let b = l.pos as int;
        assert(0 <= a <= b <= d.len());
        assert(d.map_values(f).subrange(a, b) =~= d.subrange(a, b).map_values(f));
        let x = produced(d, s, p);
        let y = d.subrange(a, b);
        let z = copied(s, l);
        assert((x + y + z).map_values(f) =~= x.map_values(f) + y.map_values(f) + z.map_values(f));
    }
}

/// Splices in order make of mapped columns the mapped result.
pub(crate) proof fn lemma_applied_map<V, W>(d: Seq<V>, s: Seq<V>, sps: Seq<Splice>, f: spec_fn(V) -> W)
    requires
        in_order(sps, d.len() as int),
    ensures
        applied(d.map_values(f), s.map_values(f), sps) == applied(d, s, sps).map_values(f),
{
    lemma_produced_map(d, s, sps, f);
    let e = end_of(sps);
    if sps.len() > 0 {
        assert(sps[sps.len() - 1] == sps.last());
    }
    assert(0 <= e <= d.len());
    assert(d.map_values(f).subrange(e, d.len() as int) =~= d.subrange(e, d.len() as int).map_values(f));
    let x = produced(d, s, sps);
    let y = d.subrange(e, d.len() as int);
    assert((x + y).map_values(f) =~= x.map_values(f) + y.map_values(f));
}

/// Splices in order give only items that have a property every item of
/// the destination and of the source has.
pub(crate) proof fn lemma_applied_all<V>(d: Seq<V>, s: Seq<V>, sps: Seq<Splice>, p: spec_fn(V) -> bool)
    requires
        in_order(sps, d.len() as int),
        forall|i: int| 0 <= i < d.len() ==> p(#[trigger] d[i]),
        forall|i: int| 0 <= i < s.len() ==> p(#[trigger] s[i]),
    ensures
        forall|i: int| 0 <= i < applied(d, s, sps).len() ==> p(#[trigger] applied(d, s, sps)[i]),
{
    lemma_produced_all(d, s, sps, p);
    let e = end_of(sps);
    if sps.len() > 0 {
        lemma_in_order_front(sps, d.len() as int);
    }
    assert(0 <= e <= d.len());
    let x = produced(d, s, sps);
    let y = d.subrange(e, d.len() as int);
    assert forall|i: int| 0 <= i < (x + y).len() implies p(#[trigger] (x + y)[i]) by {
        if i < x.len() {
            assert((x + y)[i] == x[i]);
        } else {
            assert((x + y)[i] == y[i - x.len()]);
            assert(y[i - x.len()] == d[e + i - x.len()]);
        }
    }
    assert(applied(d, s, sps) == x + y);
}

/// The same, of the items up to where the last splice ends.
proof fn lemma_produced_all<V>(d: Seq<V>, s: Seq<V>, sps: Seq<Splice>, p: spec_fn(V) -> bool)
    requires
        in_order(sps, d.len() as int),
        forall|i: int| 0 <= i < d.len() ==> p(#[trigger] d[i]),
        forall|i: int| 0 <= i < s.len() ==> p(#[trigger] s[i]),
    ensures
        forall|i: int| 0 <= i < produced(d, s, sps).len() ==> p(#[trigger] produced(d, s, sps)[i]),
    decreases sps.len(),
{
    if sps.len() > 0 {
        let q = sps.drop_last();
        let l = sps.last();
        lemma_in_order_front(sps, d.len() as int);
        lemma_produced_all(d, s, q, p);
        let x = produced(d, s, q);
        let a = end_of(q);
        let y = d.subrange(a, l.pos as int);
        let z = copied(s, l);
        let st = min_i(l.range.start as int, s.len() as int);
        assert forall|i: int| 0 <= i < z.len() implies p(#[trigger] z[i]) by {
            assert(z[i] == s[st + i]);
        }
        assert forall|i: int| 0 <= i < (x + y + z).len() implies p(#[trigger] (x + y + z)[i]) by {
            if i < x.len() {
                assert((x + y + z)[i] == x[i]);
            } else if i < x.len() + y.len() {
                assert((x + y + z)[i] == y[i - x.len()]);
                assert(y[i - x.len()] == d[a + i - x.len()]);
            } else {
                assert((x + y + z)[i] == z[i - x.len() - y.len()]);
            }
        }
        assert(produced(d, s, sps) == x + y + z);
    }
}

/// What splices in order make of a destination and a source depends only
/// on the columns' lengths: two lists that agree on the columns of
/// positions (`ids`, `negs`) agree on any columns of those lengths.
pub(crate) proof fn lemma_any_columns<V>(dst: Seq<V>, src: Seq<V>, a: Seq<Splice>, b: Seq<Splice>)
    requires
        in_order(a, dst.len() as int),
        in_order(b, dst.len() as int),
        applied(ids(dst.len()), negs(src.len()), a) == applied(ids(dst.len()), negs(src.len()), b),
    ensures
        applied(dst, src, a) == applied(dst, src, b),
{
    let (di, si) = (ids(dst.len()), negs(src.len()));
    let f = |x: int| if x >= 0 { dst[x] } else { src[-x - 1] };
    assert(di.map_values(f) =~= dst);
    assert(si.map_values(f) =~= src);
    lemma_applied_map(di, si, a, f);
    lemma_applied_map(di, si, b, f);
}

/// Normalizing a list of splices, one at a time (`normalize_splices`).
pub(crate) struct Normalize {
    pub(crate) out: Vec<Splice>,
    /// where the splices so far end, in the source (the furthest) and the
    /// destination
    pub(crate) src_pos: usize,
    pub(crate) dst_pos: usize,
    pub(crate) src_len: usize,
    pub(crate) dst_len: usize,
    /// the splices taken so far, and the columns of positions of a
    /// destination and source of the lengths given (`ids`, `negs`)
    #[allow(dead_code)]
    pub(crate) seen: Ghost<Seq<Splice>>,
    #[allow(dead_code)]
    pub(crate) dst: Ghost<Seq<int>>,
    #[allow(dead_code)]
    pub(crate) src: Ghost<Seq<int>>,
}


/// A list in order without its last, and where its last lies.
proof fn lemma_in_order_front(sps: Seq<Splice>, dst_len: int)
    requires
        in_order(sps, dst_len),
        sps.len() > 0,
    ensures
        in_order(sps.drop_last(), dst_len),
        0 <= end_of(sps.drop_last()) <= sps.last().pos,
        sps.last().pos + sps.last().delete <= dst_len,
{
    let p = sps.drop_last();
    let n = p.len() as int;
    assert(sps[n] == sps.last());
    assert(sps[n].pos + sps[n].delete <= dst_len);
    assert forall|i: int| 0 <= i < p.len() implies #[trigger] p[i].pos + p[i].delete <= dst_len by {
        assert(p[i] == sps[i]);
    }
    assert forall|i: int| 0 < i < p.len() implies p[i - 1].pos + p[i - 1].delete <= #[trigger] p[i].pos by {
        assert(p[i] == sps[i] && p[i - 1] == sps[i - 1]);
    }
    if n > 0 {
        assert(p.last() == sps[n - 1]);
        assert(sps[n - 1].pos + sps[n - 1].delete <= sps[n].pos);
    }
}

proof fn lemma_in_order_push(seen: Seq<Splice>, sp: Splice, dst_len: int)
    requires
        in_order(seen, dst_len),
        end_of(seen) <= sp.pos,
        sp.pos + sp.delete <= dst_len,
    ensures
        in_order(seen.push(sp), dst_len),
{
    let s1 = seen.push(sp);
    assert forall|i: int| 0 < i < s1.len() implies s1[i - 1].pos + s1[i - 1].delete <= #[trigger] s1[i].pos by {
        if i == s1.len() - 1 {
            assert(s1[i - 1] == seen.last());
        }
    }
}

/// A splice that does nothing: the destination runs on to its position.
proof fn lemma_skip<V>(dst: Seq<V>, src: Seq<V>, out: Seq<Splice>, seen: Seq<Splice>, sp: Splice)
    requires
        produced(dst, src, seen) == produced(dst, src, out) + dst.subrange(end_of(out), end_of(seen)),
        0 <= end_of(out) <= end_of(seen) <= sp.pos <= dst.len(),
        copied(src, sp).len() == 0,
        sp.delete == 0,
    ensures
        produced(dst, src, seen.push(sp)) == produced(dst, src, out) + dst.subrange(end_of(out), end_of(seen.push(sp))),
{
    let s1 = seen.push(sp);
    assert(s1.drop_last() =~= seen);
    assert(copied(src, sp) =~= Seq::<V>::empty());
    assert(dst.subrange(end_of(out), end_of(seen)) + dst.subrange(end_of(seen), sp.pos as int)
        =~= dst.subrange(end_of(out), sp.pos as int));
    assert(produced(dst, src, s1) =~= produced(dst, src, out) + dst.subrange(end_of(out), end_of(s1)));
}

/// Adding a splice, clamped.
proof fn lemma_add<V>(dst: Seq<V>, src: Seq<V>, out: Seq<Splice>, seen: Seq<Splice>, sp: Splice, c: Splice, src_len: int, dst_len: int, src_pos: int)
    requires
        produced(dst, src, seen) == produced(dst, src, out) + dst.subrange(end_of(out), end_of(seen)),
        0 <= end_of(out) <= end_of(seen) <= sp.pos,
        sp.pos + sp.delete <= dst_len == dst.len(),
        src.len() == src_len,
        c.pos == sp.pos && c.delete == sp.delete,
        copied(src, c) == copied(src, sp),
        c.range.start <= c.range.end <= min_i(src_pos, src_len),
        c.range.start < c.range.end || c.delete > 0,
        normal(out, src_len, dst_len),
        out.len() > 0 ==> out.last().range.end <= c.range.start,
    ensures
        produced(dst, src, seen.push(sp)) == produced(dst, src, out.push(c)) + dst.subrange(end_of(out.push(c)), end_of(seen.push(sp))),
        end_of(out.push(c)) == end_of(seen.push(sp)),
        normal(out.push(c), src_len, dst_len),
        out.push(c).last().range.end <= min_i(src_pos, src_len),
{
    lemma_add_produced(dst, src, out, seen, sp, c);
    lemma_add_normal(out, c, src_len, dst_len);
}

proof fn lemma_add_produced<V>(dst: Seq<V>, src: Seq<V>, out: Seq<Splice>, seen: Seq<Splice>, sp: Splice, c: Splice)
    requires
        produced(dst, src, seen) == produced(dst, src, out) + dst.subrange(end_of(out), end_of(seen)),
        0 <= end_of(out) <= end_of(seen) <= sp.pos,
        sp.pos + sp.delete <= dst.len(),
        c.pos == sp.pos && c.delete == sp.delete,
        copied(src, c) == copied(src, sp),
    ensures
        produced(dst, src, seen.push(sp)) == produced(dst, src, out.push(c)) + dst.subrange(end_of(out.push(c)), end_of(seen.push(sp))),
        end_of(out.push(c)) == end_of(seen.push(sp)),
{
    let s1 = seen.push(sp);
    let o1 = out.push(c);
    assert(s1.drop_last() =~= seen);
    assert(o1.drop_last() =~= out);
    assert(dst.subrange(end_of(out), end_of(seen)) + dst.subrange(end_of(seen), sp.pos as int)
        =~= dst.subrange(end_of(out), sp.pos as int));
    assert(dst.subrange(end_of(o1), end_of(s1)) =~= Seq::<V>::empty());
    assert(produced(dst, src, s1) =~= produced(dst, src, o1) + dst.subrange(end_of(o1), end_of(s1)));
}

proof fn lemma_add_normal(out: Seq<Splice>, c: Splice, src_len: int, dst_len: int)
    requires
        normal(out, src_len, dst_len),
        end_of(out) <= c.pos,
        c.pos + c.delete <= dst_len,
        c.range.start <= c.range.end <= src_len,
        c.range.start < c.range.end || c.delete > 0,
        out.len() > 0 ==> out.last().range.end <= c.range.start,
    ensures
        normal(out.push(c), src_len, dst_len),
{
    hide(in_order);
    lemma_in_order_push(out, c, dst_len);
    let o1 = out.push(c);
    let n = out.len() as int;
    assert forall|i: int| 0 <= i < o1.len() implies #[trigger] o1[i].range.start <= o1[i].range.end <= src_len by {
        if i < n {
            assert(out[i].range.start <= out[i].range.end <= src_len);
        }
    }
    assert forall|i: int| 0 <= i < o1.len() implies #[trigger] o1[i].range.start < o1[i].range.end
        || o1[i].delete > 0 by {
        if i < n {
            assert(out[i].range.start < out[i].range.end || out[i].delete > 0);
        }
    }
    assert forall|i: int| 0 < i < o1.len() implies o1[i - 1].range.end <= #[trigger] o1[i].range.start by {
        if i == n {
            assert(o1[i - 1] == out.last());
        } else {
            assert(out[i - 1].range.end <= out[i].range.start);
        }
    }
}

/// The last of a normalized list is inside both columns.
proof fn lemma_normal_last(out: Seq<Splice>, src_len: int, dst_len: int)
    requires
        normal(out, src_len, dst_len),
        out.len() > 0,
    ensures
        out.last().pos + out.last().delete <= dst_len,
        out.last().range.start <= out.last().range.end <= src_len,
{
    let l = out[out.len() - 1];
    assert(l.pos + l.delete <= dst_len);
    assert(l.range.start <= l.range.end <= src_len);
}

/// A normalized list without its last, and what holds of its last.
proof fn lemma_normal_front(o: Seq<Splice>, last: Splice, src_len: int, dst_len: int)
    requires
        normal(o.push(last), src_len, dst_len),
    ensures
        normal(o, src_len, dst_len),
        0 <= end_of(o) <= last.pos,
        last.pos + last.delete <= dst_len,
        last.range.start <= last.range.end <= src_len,
        last.range.start < last.range.end || last.delete > 0,
        o.len() > 0 ==> o.last().range.end <= last.range.start,
{
    hide(in_order);
    let o1 = o.push(last);
    let n = o.len() as int;
    assert(o1.drop_last() =~= o);
    assert(o1.last() == last);
    lemma_in_order_front(o1, dst_len);
    assert(o1[n] == last);
    assert(o1[n].range.start <= o1[n].range.end <= src_len);
    assert(o1[n].range.start < o1[n].range.end || o1[n].delete > 0);
    assert forall|i: int| 0 <= i < o.len() implies #[trigger] o[i].range.start <= o[i].range.end <= src_len by {
        assert(o[i] == o1[i]);
        assert(o1[i].range.start <= o1[i].range.end <= src_len);
    }
    assert forall|i: int| 0 <= i < o.len() implies #[trigger] o[i].range.start < o[i].range.end
        || o[i].delete > 0 by {
        assert(o[i] == o1[i]);
        assert(o1[i].range.start < o1[i].range.end || o1[i].delete > 0);
    }
    assert forall|i: int| 0 < i < o.len() implies o[i - 1].range.end <= #[trigger] o[i].range.start by {
        assert(o[i] == o1[i] && o[i - 1] == o1[i - 1]);
        assert(o1[i - 1].range.end <= o1[i].range.start);
    }
    if n > 0 {
        assert(o.last() == o1[n - 1]);
        assert(o1[n - 1].range.end <= o1[n].range.start);
    }
}

/// A splice whose range, clamped to the source, is empty or reversed
/// copies nothing.
proof fn lemma_copied_none<V>(src: Seq<V>, sp: Splice)
    requires
        min_i(sp.range.start as int, src.len() as int) >= min_i(sp.range.end as int, src.len() as int),
    ensures
        copied(src, sp).len() == 0,
{
}

/// A splice's range clamped to the source, or (where that leaves nothing)
/// any empty range inside it, copies what the splice copies.
proof fn lemma_copied_clamped<V>(src: Seq<V>, sp: Splice, c: Splice)
    requires
        ({
            let s0 = min_i(sp.range.start as int, src.len() as int);
            let e0 = min_i(sp.range.end as int, src.len() as int);
            if s0 < e0 {
                c.range.start == s0 && c.range.end == e0
            } else {
                c.range.start == c.range.end
            }
        }),
    ensures
        copied(src, c) == copied(src, sp),
{
}

/// What a merged splice produces: what the last one and the next produce
/// in turn.
proof fn lemma_merge_produced<V>(dst: Seq<V>, src: Seq<V>, o: Seq<Splice>, last: Splice, seen: Seq<Splice>, sp: Splice, c: Splice, m: Splice)
    requires
        produced(dst, src, seen) == produced(dst, src, o.push(last)) + dst.subrange(end_of(o.push(last)), end_of(seen)),
        end_of(o.push(last)) <= end_of(seen) <= sp.pos,
        last.pos + last.delete == sp.pos,
        sp.pos + sp.delete <= dst.len(),
        copied(src, c) == copied(src, sp),
        last.range.end == c.range.start,
        last.range.start <= last.range.end,
        c.range.start <= c.range.end <= src.len(),
        m.pos == last.pos && m.delete == last.delete + sp.delete && m.range.start == last.range.start,
        m.range.end == c.range.end,
    ensures
        produced(dst, src, seen.push(sp)) == produced(dst, src, o.push(m)) + dst.subrange(end_of(o.push(m)), end_of(seen.push(sp))),
        end_of(o.push(m)) == end_of(seen.push(sp)),
{
    let s1 = seen.push(sp);
    let o1 = o.push(last);
    let om = o.push(m);
    assert(s1.drop_last() =~= seen);
    assert(o1.drop_last() =~= o);
    assert(om.drop_last() =~= o);
    assert(o1.last() == last);
    // the merged range copies the two in turn
    assert(copied(src, m) =~= copied(src, last) + copied(src, sp)) by {
        if last.range.start < last.range.end {
            assert(copied(src, last) == src.subrange(last.range.start as int, last.range.end as int));
        } else {
            assert(copied(src, last) =~= Seq::<V>::empty());
        }
        if last.range.end < m.range.end {
            assert(copied(src, c) == src.subrange(last.range.end as int, m.range.end as int));
        } else {
            assert(copied(src, c) =~= Seq::<V>::empty());
        }
    }
    assert(end_of(seen) == sp.pos);
    assert(dst.subrange(sp.pos as int, sp.pos as int) =~= Seq::<V>::empty());
    assert(dst.subrange(end_of(om), end_of(s1)) =~= Seq::<V>::empty());
    assert(produced(dst, src, s1) =~= produced(dst, src, om) + dst.subrange(end_of(om), end_of(s1)));
}

/// Merging a splice into the last one, which it touches in both columns.
#[verifier::spinoff_prover]
proof fn lemma_merge<V>(dst: Seq<V>, src: Seq<V>, o: Seq<Splice>, last: Splice, seen: Seq<Splice>, sp: Splice, c: Splice, m: Splice, src_len: int, dst_len: int)
    requires
        produced(dst, src, seen) == produced(dst, src, o.push(last)) + dst.subrange(end_of(o.push(last)), end_of(seen)),
        end_of(o.push(last)) <= end_of(seen) <= sp.pos,
        last.pos + last.delete == sp.pos,
        sp.pos + sp.delete <= dst_len == dst.len(),
        src.len() == src_len,
        copied(src, c) == copied(src, sp),
        last.range.end == c.range.start,
        c.range.start <= c.range.end <= src_len,
        m.pos == last.pos && m.delete == last.delete + sp.delete && m.range.start == last.range.start,
        m.range.end == c.range.end,
        normal(o.push(last), src_len, dst_len),
    ensures
        produced(dst, src, seen.push(sp)) == produced(dst, src, o.push(m)) + dst.subrange(end_of(o.push(m)), end_of(seen.push(sp))),
        end_of(o.push(m)) == end_of(seen.push(sp)),
        normal(o.push(m), src_len, dst_len),
        o.push(m).last().range.end == m.range.end,
{
    hide(normal);
    lemma_normal_front(o, last, src_len, dst_len);
    lemma_merge_produced(dst, src, o, last, seen, sp, c, m);
    lemma_add_normal(o, m, src_len, dst_len);
    assert(o.push(m).last() == m);
}

impl Normalize {
    pub(crate) open spec fn inv(&self) -> bool {
        let out = self.out@;
        let seen = self.seen@;
        &&& self.dst@ == ids(self.dst_len as nat) && self.src@ == negs(self.src_len as nat)
        &&& in_order(seen, self.dst_len as int)
        &&& self.dst_pos == end_of(seen)
        &&& normal(out, self.src_len as int, self.dst_len as int)
        &&& end_of(out) <= end_of(seen)
        &&& out.len() > 0 ==> out.last().range.end <= min_i(self.src_pos as int, self.src_len as int)
        &&& produced(self.dst@, self.src@, seen) == produced(self.dst@, self.src@, out)
            + self.dst@.subrange(end_of(out), end_of(seen))
    }

    /// Start normalizing splices from a source of `src_len` items into a
    /// destination of `dst_len`.
    #[inline(always)]
    pub(crate) fn new(src_len: usize, dst_len: usize) -> (n: Self)
        ensures
            n.inv(),
            n.seen@.len() == 0,
            n.src_pos == 0,
            n.src_len == src_len,
            n.dst_len == dst_len,
            n.dst@ == ids(dst_len as nat),
            n.src@ == negs(src_len as nat),
    {
        let ghost dst = ids(dst_len as nat);
        let ghost src = negs(src_len as nat);
        proof {
            assert(dst.subrange(0, 0) =~= Seq::<int>::empty());
            assert(Seq::<int>::empty() + Seq::<int>::empty() =~= Seq::<int>::empty());
        }
        Normalize {
            out: Vec::new(),
            src_pos: 0,
            dst_pos: 0,
            src_len,
            dst_len,
            seen: Ghost(Seq::empty()),
            dst: Ghost(dst),
            src: Ghost(src),
        }
    }

    /// Take the next splice: check it, then drop it (if it does nothing),
    /// merge it into the last (if they touch), or add it, clamped.
    #[verifier::spinoff_prover]
    #[inline(always)]
    pub(crate) fn push(&mut self, sp: Splice)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).seen@ == old(self).seen@.push(sp),
            final(self).src_pos == if sp.range.end > old(self).src_pos {
                sp.range.end
            } else {
                old(self).src_pos
            },
            final(self).src_len == old(self).src_len,
            final(self).dst_len == old(self).dst_len,
            final(self).dst == old(self).dst,
            final(self).src == old(self).src,
    {
        hide(produced);
        hide(copied);
        hide(normal);
        hide(in_order);
        if sp.pos < self.dst_pos {
            bad_splices("destination regions must be ascending and non-overlapping");
        }
        // `pos + delete` could overflow
        if !(sp.pos <= self.dst_len && sp.delete <= self.dst_len - sp.pos) {
            bad_splices("splice out of bounds");
        }
        if sp.range.start < self.src_pos {
            bad_splices("src ranges must be ascending and non-overlapping");
        }
        let ghost seen = self.seen@;
        let ghost out = self.out@;
        let ghost dst = self.dst@;
        let ghost src = self.src@;
        proof {
            lemma_in_order_push(seen, sp, self.dst_len as int);
        }
        self.dst_pos = sp.pos + sp.delete;
        if sp.range.end > self.src_pos {
            self.src_pos = sp.range.end;
        }
        let s0 = if sp.range.start < self.src_len { sp.range.start } else { self.src_len };
        let e0 = if sp.range.end < self.src_len { sp.range.end } else { self.src_len };
        self.seen = Ghost(seen.push(sp));
        let n = self.out.len();
        // an empty range (a reversed one too, as a `Range`) copies
        // nothing, and stays where the copying is: at the end of the last
        // range kept, so the ranges kept still ascend
        let (start, end) = if s0 < e0 {
            (s0, e0)
        } else if sp.delete == 0 {
            // nothing to copy or delete: the destination runs on to `pos`
            proof {
                lemma_copied_none(src, sp);
                lemma_skip(dst, src, out, seen, sp);
            }
            return;
        } else if n > 0 {
            (self.out[n - 1].range.end, self.out[n - 1].range.end)
        } else {
            (0, 0)
        };
        proof {
            lemma_copied_clamped(src, sp, Splice { pos: sp.pos, delete: sp.delete, range: start..end });
            if n > 0 {
                assert(out[n - 1] == out.last());
                lemma_normal_last(out, self.src_len as int, self.dst_len as int);
            }
        }
        if n > 0 && self.out[n - 1].pos + self.out[n - 1].delete == sp.pos && self.out[n - 1].range.end == start {
            // touches the last in both columns: one splice
            let last = Splice { pos: self.out[n - 1].pos, delete: self.out[n - 1].delete, range: self.out[n - 1].range.start..self.out[n - 1].range.end };
            let merged = Splice { pos: last.pos, delete: last.delete + sp.delete, range: last.range.start..end };
            self.out.set(n - 1, merged);
            proof {
                assert(last == out.last());
                assert(out =~= out.drop_last().push(last));
                let c = Splice { pos: sp.pos, delete: sp.delete, range: start..end };
                lemma_merge(dst, src, out.drop_last(), last, seen, sp, c, merged, self.src_len as int, self.dst_len as int);
                assert(self.out@ =~= out.drop_last().push(merged));
            }
        } else {
            let clamped = Splice { pos: sp.pos, delete: sp.delete, range: start..end };
            self.out.push(clamped);
            proof {
                lemma_add(dst, src, out, seen, sp, clamped, self.src_len as int, self.dst_len as int, self.src_pos as int);
                assert(self.out@ =~= out.push(clamped));
            }
        }
    }

    /// The normalized list: applied, it gives what the splices taken give.
    #[inline(always)]
    pub(crate) fn finish(self) -> (out: Vec<Splice>)
        requires
            self.inv(),
        ensures
            normal(out@, self.src_len as int, self.dst_len as int),
            in_order(self.seen@, self.dst_len as int),
            self.dst@ == ids(self.dst_len as nat),
            self.src@ == negs(self.src_len as nat),
            applied(self.dst@, self.src@, out@) == applied(self.dst@, self.src@, self.seen@),
    {
        proof {
            let (dst, src, out, seen) = (self.dst@, self.src@, self.out@, self.seen@);
            assert(end_of(seen) <= self.dst_len) by {
                if seen.len() > 0 {
                    assert(seen[seen.len() - 1] == seen.last());
                }
            }
            assert(dst.subrange(end_of(out), end_of(seen)) + dst.subrange(end_of(seen), dst.len() as int)
                =~= dst.subrange(end_of(out), dst.len() as int));
            assert(applied(dst, src, out) =~= applied(dst, src, seen)) by {
                assert(applied(dst, src, seen) == produced(dst, src, out) + dst.subrange(end_of(out), end_of(seen))
                    + dst.subrange(end_of(seen), dst.len() as int));
            }
        }
        self.out
    }
}

/// The source items the splices copy, one after another: what is left of
/// the source once the gaps between its ranges are dropped.
pub(crate) open spec fn kept<V>(src: Seq<V>, sps: Seq<Splice>) -> Seq<V>
    decreases sps.len(),
{
    if sps.len() == 0 {
        Seq::empty()
    } else {
        kept(src, sps.drop_last()) + copied(src, sps.last())
    }
}

/// `s` without the `n` items from `at`.
pub(crate) open spec fn cut<V>(s: Seq<V>, at: int, n: int) -> Seq<V> {
    s.subrange(0, at) + s.subrange(at + n, s.len() as int)
}

/// `s` with `cuts` (each `(at, n)`) removed, one after another.
pub(crate) open spec fn removed<V>(s: Seq<V>, cuts: Seq<(usize, usize)>) -> Seq<V>
    decreases cuts.len(),
{
    if cuts.len() == 0 {
        s
    } else {
        cut(removed(s, cuts.drop_last()), cuts.last().0 as int, cuts.last().1 as int)
    }
}

/// The splices' ranges lie inside the source, none reversed, ascending.
pub(crate) open spec fn ranges_in(sps: Seq<Splice>, src_len: int) -> bool {
    &&& forall|i: int| 0 <= i < sps.len() ==> #[trigger] sps[i].range.start <= sps[i].range.end <= src_len
    &&& forall|i: int| 0 < i < sps.len() ==> sps[i - 1].range.end <= #[trigger] sps[i].range.start
}

/// What splices keep, split anywhere.
proof fn lemma_kept_split<V>(src: Seq<V>, sps: Seq<Splice>, i: int)
    requires
        0 <= i <= sps.len(),
    ensures
        kept(src, sps) == kept(src, sps.take(i)) + kept(src, sps.skip(i)),
    decreases sps.len(),
{
    if i == sps.len() {
        assert(sps.take(i) =~= sps);
        assert(sps.skip(i) =~= Seq::<Splice>::empty());
        assert(kept(src, sps) + Seq::<V>::empty() =~= kept(src, sps));
    } else {
        let d = sps.drop_last();
        lemma_kept_split(src, d, i);
        assert(d.take(i) =~= sps.take(i));
        assert(sps.skip(i).drop_last() =~= d.skip(i));
        assert(sps.skip(i).last() == sps.last());
        assert(kept(src, sps.skip(i)) == kept(src, d.skip(i)) + copied(src, sps.last()));
        assert(kept(src, sps) =~= kept(src, sps.take(i)) + kept(src, sps.skip(i)));
    }
}

/// Splice `i` keeps its range, after what those before it keep.
proof fn lemma_kept_at<V>(src: Seq<V>, sps: Seq<Splice>, i: int)
    requires
        0 <= i < sps.len(),
        ranges_in(sps, src.len() as int),
    ensures
        kept(src, sps.take(i + 1)) == kept(src, sps.take(i)) + src.subrange(sps[i].range.start as int, sps[i].range.end as int),
        kept(src, sps.skip(i)) == src.subrange(sps[i].range.start as int, sps[i].range.end as int) + kept(src, sps.skip(i + 1)),
        kept(src, sps.take(i)).len() <= (if i > 0 { sps[i - 1].range.end as int } else { 0 }),
    decreases i,
{
    let r = src.subrange(sps[i].range.start as int, sps[i].range.end as int);
    assert(copied(src, sps[i]) == r) by {
        if sps[i].range.start == sps[i].range.end {
            assert(r =~= Seq::<V>::empty());
        }
    }
    assert(sps.take(i + 1).drop_last() =~= sps.take(i));
    assert(sps.take(i + 1).last() == sps[i]);
    let t = sps.skip(i);
    lemma_kept_split(src, t, 1);
    assert(t.skip(1) =~= sps.skip(i + 1));
    assert(t.take(1).drop_last() =~= Seq::<Splice>::empty());
    assert(t.take(1).last() == sps[i]);
    assert(kept(src, Seq::<Splice>::empty()) == Seq::<V>::empty());
    assert(kept(src, t.take(1)) == kept(src, t.take(1).drop_last()) + copied(src, t.take(1).last()));
    assert(kept(src, t.take(1)) =~= r);
    if i > 0 {
        lemma_kept_at(src, sps, i - 1);
        assert(sps[i - 1].range.end <= sps[i].range.start);
        if i > 1 {
            assert(sps[i - 2].range.end <= sps[i - 1].range.start);
        }
    }
}

/// The source's first `end` items and then `k`, split at `start`.
proof fn lemma_front_split<V>(src: Seq<V>, start: int, end: int, k: Seq<V>)
    requires
        0 <= start <= end <= src.len(),
    ensures
        src.subrange(0, end) + k == src.subrange(0, start) + (src.subrange(start, end) + k),
{
    assert(src.subrange(0, end) + k =~= src.subrange(0, start) + (src.subrange(start, end) + k));
}

/// Where the cuts made leave the source's first `to` items and then `k`,
/// one more, `c`, up to `to`, leaves the source's items before it and then
/// `k`.
proof fn lemma_cut_push<V>(src: Seq<V>, cuts: Seq<(usize, usize)>, c: (usize, usize), to: int, k: Seq<V>)
    requires
        c.0 <= to <= src.len(),
        c.1 == to - c.0,
        removed(src, cuts) == src.subrange(0, to) + k,
    ensures
        c.0 + c.1 <= removed(src, cuts).len(),
        removed(src, cuts.push(c)) == src.subrange(0, c.0 as int) + k,
{
    let cur = removed(src, cuts);
    let cs = cuts.push(c);
    assert(cs.drop_last() =~= cuts);
    assert(cs.last() == c);
    assert(cur.subrange(0, c.0 as int) =~= src.subrange(0, c.0 as int));
    assert(cur.subrange(to, cur.len() as int) =~= k);
}

/// The splices a caller gives `copy_ranges`, from outside: nothing is
/// assumed of them.
pub(crate) trait SpliceList {
    fn next_splice(&mut self) -> Option<Splice>;
}

/// A normalized list of splices (`Normalize`), for a source of
/// `src_len` items and a destination of `dst_len`. Its type invariant:
/// the list is `normal`, and on the columns of positions it gives what
/// the splices it was made from (`seen`) give one by one, so it does on
/// any columns of those lengths (`Normalized::lemma_applied`).
pub(crate) struct Normalized {
    sps: Vec<Splice>,
    src_len: usize,
    dst_len: usize,
    #[allow(dead_code)]
    seen: Ghost<Seq<Splice>>,
}

impl Normalized {
    #[verifier::type_invariant]
    spec fn inv(&self) -> bool {
        let (d, s) = (ids(self.dst_len as nat), negs(self.src_len as nat));
        &&& normal(self.sps@, self.src_len as int, self.dst_len as int)
        &&& in_order(self.seen@, self.dst_len as int)
        &&& applied(d, s, self.sps@) == applied(d, s, self.seen@)
    }

    /// The normalized list.
    pub(crate) closed spec fn view(&self) -> Seq<Splice> {
        self.sps@
    }

    /// The splices it was made from.
    pub(crate) closed spec fn seen(&self) -> Seq<Splice> {
        self.seen@
    }

    /// The source's and the destination's lengths it was made for.
    pub(crate) closed spec fn src_count(&self) -> nat {
        self.src_len as nat
    }

    pub(crate) closed spec fn dst_count(&self) -> nat {
        self.dst_len as nat
    }

    /// The type invariant, in its views.
    pub(crate) open spec fn wf(&self) -> bool {
        let (d, s) = (ids(self.dst_count()), negs(self.src_count()));
        &&& normal(self@, self.src_count() as int, self.dst_count() as int)
        &&& in_order(self.seen(), self.dst_count() as int)
        &&& applied(d, s, self@) == applied(d, s, self.seen())
    }

    #[inline(always)]
    pub(crate) fn splices(&self) -> (r: &[Splice])
        ensures
            r@ == self@,
            self.wf(),
    {
        proof {
            use_type_invariant(self);
        }
        self.sps.as_slice()
    }

    #[inline(always)]
    pub(crate) fn src_len(&self) -> (r: usize)
        ensures
            r == self.src_count(),
    {
        self.src_len
    }

    #[inline(always)]
    pub(crate) fn dst_len(&self) -> (r: usize)
        ensures
            r == self.dst_count(),
    {
        self.dst_len
    }

    /// Applied to any destination and source of its lengths, the list
    /// gives what the splices it was made from give one by one.
    pub(crate) proof fn lemma_applied<V>(&self, dst: Seq<V>, src: Seq<V>)
        requires
            self.wf(),
            dst.len() == self.dst_count(),
            src.len() == self.src_count(),
        ensures
            normal(self@, src.len() as int, dst.len() as int),
            applied(dst, src, self@) == applied(dst, src, self.seen()),
    {
        lemma_any_columns(dst, src, self@, self.seen());
    }
}

/// Normalize the caller's `splices` for a source of `src_len` items and
/// a destination of `dst_len`, checking them (`Normalize`; it panics on
/// splices out of order or out of bounds). Ends when they do.
#[verifier::exec_allows_no_decreases_clause]
// a Verus loop with an invariant: `loop` + `match`
#[allow(clippy::while_let_loop)]
#[inline(always)]
pub(crate) fn normalize<S: SpliceList>(splices: &mut S, src_len: usize, dst_len: usize) -> (r: Normalized)
    ensures
        r.src_count() == src_len,
        r.dst_count() == dst_len,
{
    let mut n = Normalize::new(src_len, dst_len);
    // the splices the caller gave, as it gave them
    let ghost mut taken = Seq::<Splice>::empty();
    proof {
        assert(n.seen@ =~= taken);
    }
    loop
        invariant
            n.inv(),
            n.src_len == src_len,
            n.dst_len == dst_len,
            n.dst@ == ids(dst_len as nat),
            n.src@ == negs(src_len as nat),
            n.seen@ == taken,
    {
        match splices.next_splice() {
            Some(sp) => {
                proof {
                    taken = taken.push(sp);
                }
                n.push(sp);
            },
            None => break,
        }
    }
    let sps = n.finish();
    Normalized { sps, src_len, dst_len, seen: Ghost(taken) }
}

/// Before the splice at `i` of splices in order: the ones before it end
/// in the destination where it starts or before, it ends within the
/// destination, and the ranges after it start where its range ends or
/// later.
proof fn lemma_slice_at(sps: Seq<Splice>, i: int, dst_len: int)
    requires
        in_order(sps, dst_len),
        ranges_ascend(sps),
        0 <= i < sps.len(),
    ensures
        end_of(sps.take(i)) <= sps[i].pos,
        sps[i].pos + sps[i].delete <= dst_len,
        forall|j: int| i < j < sps.len() ==> sps[i].range.end <= #[trigger] sps[j].range.start,
{
    if i > 0 {
        assert(sps.take(i).last() == sps[i - 1]);
    }
    assert forall|j: int| i < j < sps.len() implies sps[i].range.end <= #[trigger] sps[j].range.start by {
        assert(sps[i].range.end <= sps[j].range.start);
    }
}

/// Normalize the splices `sps`, which are in order in both columns, for
/// a source of `src_len` items and a destination of `dst_len`: none of
/// `Normalize`'s checks refuses one (shown before each is taken).
pub(crate) fn normalize_slice(sps: &[Splice], src_len: usize, dst_len: usize) -> (r: Normalized)
    requires
        in_order(sps@, dst_len as int),
        ranges_ascend(sps@),
    ensures
        r.src_count() == src_len,
        r.dst_count() == dst_len,
        r.seen() == sps@,
{
    hide(produced);
    hide(copied);
    hide(normal);
    hide(applied);
    hide(in_order);
    hide(ranges_ascend);
    hide(end_of);
    let mut n = Normalize::new(src_len, dst_len);
    let mut i: usize = 0;
    proof {
        assert(n.seen@ =~= sps@.take(0));
    }
    while i < sps.len()
        invariant
            i <= sps.len(),
            n.inv(),
            n.src_len == src_len,
            n.dst_len == dst_len,
            n.dst@ == ids(dst_len as nat),
            n.src@ == negs(src_len as nat),
            n.seen@ == sps@.take(i as int),
            forall|j: int| i <= j < sps.len() ==> n.src_pos <= #[trigger] sps@[j].range.start,
            in_order(sps@, dst_len as int),
            ranges_ascend(sps@),
        decreases sps.len() - i,
    {
        let sp = Splice { pos: sps[i].pos, delete: sps[i].delete, range: sps[i].range.start..sps[i].range.end };
        proof {
            assert(sp == sps@[i as int]);
            lemma_slice_at(sps@, i as int, dst_len as int);
            // what `Normalize::push` checks
            assert(n.dst_pos <= sp.pos);
            assert(sp.pos + sp.delete <= dst_len);
            assert(n.src_pos <= sp.range.start);
        }
        n.push(sp);
        proof {
            assert(sps@.take(i + 1) =~= sps@.take(i as int).push(sp));
        }
        i += 1;
    }
    proof {
        assert(sps@.take(sps@.len() as int) =~= sps@);
    }
    let out = n.finish();
    Normalized { sps: out, src_len, dst_len, seen: Ghost(sps@) }
}

/// Cutting the gaps out of the source, back to front (`copy::cut_gaps`,
/// `dcopy::cut_gaps`), leaving what the splices keep.
pub(crate) struct Cuts<V> {
    /// the ranges not yet passed (those before `i`), and where the
    /// source runs uncut to
    pub(crate) i: usize,
    pub(crate) end: usize,
    /// the cuts made so far, the splices, and a source of their length
    #[allow(dead_code)]
    pub(crate) cuts: Ghost<Seq<(usize, usize)>>,
    #[allow(dead_code)]
    pub(crate) sps: Ghost<Seq<Splice>>,
    #[allow(dead_code)]
    pub(crate) src: Ghost<Seq<V>>,
}

impl<V> Cuts<V> {
    /// Cuts can still come: each one given lowers this.
    pub(crate) open spec fn left(&self) -> nat {
        2 * self.i as nat + if self.end > 0 { 1nat } else { 0nat }
    }

    pub(crate) open spec fn inv(&self) -> bool {
        let (sps, src) = (self.sps@, self.src@);
        &&& ranges_in(sps, src.len() as int)
        &&& self.i <= sps.len()
        &&& self.end <= src.len()
        &&& self.i > 0 ==> sps[self.i - 1].range.end <= self.end
        &&& removed(src, self.cuts@) == src.subrange(0, self.end as int) + kept(src, sps.skip(self.i as int))
    }

    /// Start cutting a source of `src_len` items (any such `src`) down to
    /// what `sps` keep.
    #[inline(always)]
    pub(crate) fn new(sps: &[Splice], src_len: usize, Ghost(src): Ghost<Seq<V>>) -> (c: Self)
        requires
            ranges_in(sps@, src_len as int),
            src.len() == src_len,
        ensures
            c.inv(),
            c.cuts@.len() == 0,
            c.sps@ == sps@,
            c.src@ == src,
    {
        proof {
            assert(sps@.skip(sps@.len() as int) =~= Seq::<Splice>::empty());
            assert(src.subrange(0, src_len as int) + Seq::<V>::empty() =~= src);
            if sps@.len() > 0 {
                assert(sps@[sps@.len() - 1].range.start <= sps@[sps@.len() - 1].range.end <= src_len);
            }
        }
        Cuts { i: sps.len(), end: src_len, cuts: Ghost(Seq::empty()), sps: Ghost(sps@), src: Ghost(src) }
    }

    /// The next cut to make, `(at, n)`: remove `n` items from `at`. None
    /// once the cuts made leave what the splices keep.
    #[inline(always)]
    pub(crate) fn next(&mut self, sps: &[Splice]) -> (r: Option<(usize, usize)>)
        requires
            old(self).inv(),
            sps@ == old(self).sps@,
        ensures
            final(self).inv(),
            final(self).sps == old(self).sps,
            final(self).src == old(self).src,
            r matches Some(c) ==> final(self).cuts@ == old(self).cuts@.push(c)
                && c.0 + c.1 <= removed(old(self).src@, old(self).cuts@).len()
                && final(self).left() < old(self).left(),
            r is None ==> final(self).cuts@ == old(self).cuts@
                && removed(final(self).src@, final(self).cuts@) == kept(final(self).src@, final(self).sps@),
    {
        hide(kept);
        hide(copied);
        hide(removed);
        let ghost src = self.src@;
        let ghost cuts0 = self.cuts@;
        let ghost left0 = self.left();
        while self.i > 0
            invariant
                self.inv(),
                sps@ == self.sps@,
                self.sps == old(self).sps,
                self.src == old(self).src,
                self.cuts@ == cuts0,
                cuts0 == old(self).cuts@,
                src == self.src@,
                left0 == old(self).left(),
                self.left() <= left0,
            decreases self.i,
        {
            let i = self.i - 1;
            let start = sps[i].range.start;
            let end = sps[i].range.end;
            let to = self.end;
            proof {
                lemma_kept_at(src, sps@, i as int);
                lemma_front_split(src, start as int, end as int, kept(src, sps@.skip(i + 1)));
                if i > 0 {
                    assert(sps@[i - 1].range.end <= sps@[i as int].range.start);
                }
            }
            self.i = i;
            self.end = start;
            if end < to {
                proof {
                    let c = (end, (to - end) as usize);
                    lemma_cut_push(src, cuts0, c, to as int, kept(src, sps@.skip(i + 1)));
                    self.cuts = Ghost(cuts0.push(c));
                }
                return Some((end, to - end));
            }
        }
        if self.end > 0 {
            let to = self.end;
            self.end = 0;
            proof {
                let c = (0usize, to);
                lemma_cut_push(src, cuts0, c, to as int, kept(src, sps@.skip(0)));
                self.cuts = Ghost(cuts0.push(c));
            }
            return Some((0, to));
        }
        proof {
            assert(sps@.skip(0) =~= sps@);
            assert(src.subrange(0, 0) + kept(src, sps@) =~= kept(src, sps@));
        }
        None
    }
}

#[cfg(test)]
impl Cuts<()> {
    /// [`new`](Self::new), for the test helpers' glue, which counts rows
    /// rather than holding values: the cuts' items are units.
    #[inline(always)]
    pub(crate) fn of_len(sps: &[Splice], src_len: usize) -> (c: Self)
        requires
            ranges_in(sps@, src_len as int),
        ensures
            c.inv(),
            c.cuts@.len() == 0,
            c.sps@ == sps@,
            c.src@.len() == src_len,
    {
        Self::new(sps, src_len, Ghost(Seq::new(src_len as nat, |_i: int| ())))
    }
}

/// The gaps between the splices, in the destination, put back between
/// the ranges kept (`copy::put_gaps`, `dcopy::put_gaps`): each, as a
/// splice inserting it, is
/// at the length of the ranges before it.
pub(crate) struct Gaps<V> {
    /// the splices passed, where the last ends in the destination, and the
    /// length of their ranges
    pub(crate) i: usize,
    pub(crate) end: usize,
    pub(crate) at: usize,
    pub(crate) dst_len: usize,
    /// the gaps given so far, as splices into what the splices keep; the
    /// last given (past the last splice); the splices; and a destination
    /// and source of the lengths given
    #[allow(dead_code)]
    pub(crate) gaps: Ghost<Seq<Splice>>,
    #[allow(dead_code)]
    pub(crate) last: Ghost<bool>,
    #[allow(dead_code)]
    pub(crate) sps: Ghost<Seq<Splice>>,
    #[allow(dead_code)]
    pub(crate) dst: Ghost<Seq<V>>,
    #[allow(dead_code)]
    pub(crate) src: Ghost<Seq<V>>,
}

impl<V> Gaps<V> {
    /// Gaps can still come: each one given lowers this.
    pub(crate) open spec fn left(&self) -> nat {
        2 * (self.sps@.len() - self.i) as nat + if self.last@ { 0nat } else { 1nat }
    }

    pub(crate) open spec fn inv(&self) -> bool {
        let (sps, dst, src, gaps) = (self.sps@, self.dst@, self.src@, self.gaps@);
        let k = kept(src, sps);
        &&& normal(sps, src.len() as int, dst.len() as int)
        &&& dst.len() == self.dst_len
        &&& self.i <= sps.len()
        &&& self.end <= self.dst_len
        &&& self.at == kept(src, sps.take(self.i as int)).len()
        &&& self.at <= k.len()
        &&& normal(gaps, dst.len() as int, k.len() as int)
        &&& gaps.len() > 0 ==> gaps.last().range.end <= self.end && end_of(gaps) <= self.at
        &&& !self.last@ ==> {
            &&& self.end == end_of(sps.take(self.i as int))
            &&& produced(k, dst, gaps) + k.subrange(end_of(gaps), self.at as int) == produced(dst, src, sps.take(self.i as int))
        }
        &&& self.last@ ==> {
            &&& self.i == sps.len() && self.end == self.dst_len
            &&& applied(k, dst, gaps) == applied(dst, src, sps)
        }
    }

    /// Start on the gaps between normalized splices `sps`, from a source
    /// and into a destination (any such) of the given lengths.
    #[allow(unused_variables)]
    #[inline(always)]
    pub(crate) fn new(sps: &[Splice], dst_len: usize, Ghost(dst): Ghost<Seq<V>>, Ghost(src): Ghost<Seq<V>>) -> (g: Self)
        requires
            normal(sps@, src.len() as int, dst_len as int),
            dst.len() == dst_len,
        ensures
            g.inv(),
            g.gaps@.len() == 0,
            g.sps@ == sps@,
            g.dst@ == dst,
            g.src@ == src,
    {
        proof {
            let k = kept(src, sps@);
            assert(sps@.take(0) =~= Seq::<Splice>::empty());
            assert(k.subrange(0, 0) =~= Seq::<V>::empty());
            assert(Seq::<V>::empty() + Seq::<V>::empty() =~= Seq::<V>::empty());
            lemma_kept_split(src, sps@, 0);
        }
        Gaps {
            i: 0,
            end: 0,
            at: 0,
            dst_len,
            gaps: Ghost(Seq::empty()),
            last: Ghost(false),
            sps: Ghost(sps@),
            dst: Ghost(dst),
            src: Ghost(src),
        }
    }

    /// The next gap, `(at, gap)`: put the destination's items in `gap` at
    /// `at`. None once the gaps given, put back between the ranges kept,
    /// give what the splices give.
    #[verifier::spinoff_prover]
    #[inline(always)]
    pub(crate) fn next(&mut self, sps: &[Splice]) -> (r: Option<(usize, Range<usize>)>)
        requires
            old(self).inv(),
            sps@ == old(self).sps@,
        ensures
            final(self).inv(),
            final(self).sps == old(self).sps,
            final(self).dst == old(self).dst,
            final(self).src == old(self).src,
            r matches Some(g) ==> final(self).gaps@ == old(self).gaps@.push(
                Splice { pos: g.0, delete: 0, range: g.1 },
            ) && final(self).left() < old(self).left()
                && normal(final(self).gaps@, final(self).dst@.len() as int, kept(final(self).src@, final(self).sps@).len() as int),
            r is None ==> final(self).gaps@ == old(self).gaps@
                && applied(kept(final(self).src@, final(self).sps@), final(self).dst@, final(self).gaps@)
                == applied(final(self).dst@, final(self).src@, final(self).sps@),
    {
        hide(in_order);
        hide(produced);
        hide(kept);
        hide(copied);
        let ghost (dst, src, gaps0) = (self.dst@, self.src@, self.gaps@);
        let ghost k = kept(src, sps@);
        let n = sps.len();
        while self.i < n
            invariant
                self.inv(),
                sps@ == self.sps@,
                n == sps@.len(),
                self.sps == old(self).sps,
                self.dst == old(self).dst,
                self.src == old(self).src,
                self.gaps@ == gaps0,
                gaps0 == old(self).gaps@,
                dst == self.dst@,
                src == self.src@,
                k == kept(src, sps@),
                self.left() <= old(self).left(),
            decreases n - self.i,
        {
            let i = self.i;
            let pos = sps[i].pos;
            let delete = sps[i].delete;
            let start = sps[i].range.start;
            let end = sps[i].range.end;
            let at = self.at;
            let from = self.end;
            proof {
                assert(!self.last@);
                assert(sps@[i as int].pos + sps@[i as int].delete <= dst.len()) by {
                    reveal(in_order);
                }
                lemma_kept_at(src, sps@, i as int);
                lemma_kept_split(src, sps@, i + 1);
                lemma_dst_step(dst, src, sps@, i as int);
            }
            self.i = i + 1;
            self.end = pos + delete;
            self.at = at + (end - start);
            if pos > from {
                let ghost g = Splice { pos: at, delete: 0, range: from..pos };
                proof {
                    lemma_gap(k, dst, src, gaps0, g, sps@, i as int);
                    self.gaps = Ghost(gaps0.push(g));
                }
                return Some((at, from..pos));
            }
            proof {
                lemma_no_gap(k, dst, src, gaps0, sps@, i as int, at as int);
            }
        }
        if self.end < self.dst_len {
            let from = self.end;
            let at = self.at;
            let ghost g = Splice { pos: at, delete: 0, range: from..self.dst_len };
            self.end = self.dst_len;
            proof {
                assert(!self.last@);
                assert(sps@.take(n as int) =~= sps@);
                lemma_last_gap(k, dst, src, gaps0, g, sps@);
                self.gaps = Ghost(gaps0.push(g));
                self.last = Ghost(true);
            }
            return Some((at, from..self.dst_len));
        }
        proof {
            if !self.last@ {
                assert(sps@.take(n as int) =~= sps@);
                assert(k.subrange(end_of(gaps0), k.len() as int) == k.subrange(end_of(gaps0), self.at as int));
                assert(dst.subrange(end_of(sps@), dst.len() as int) =~= Seq::<V>::empty());
                assert(applied(dst, src, sps@) =~= produced(dst, src, sps@));
            }
        }
        None
    }
}

/// Splice `i`, in the destination: where it ends, after those before it,
/// and what it adds to what they produce.
proof fn lemma_dst_step<V>(dst: Seq<V>, src: Seq<V>, sps: Seq<Splice>, i: int)
    requires
        0 <= i < sps.len(),
        normal(sps, src.len() as int, dst.len() as int),
    ensures
        end_of(sps.take(i)) <= sps[i].pos,
        end_of(sps.take(i + 1)) == sps[i].pos + sps[i].delete,
        produced(dst, src, sps.take(i + 1)) == produced(dst, src, sps.take(i)) + dst.subrange(
            end_of(sps.take(i)),
            sps[i].pos as int,
        ) + src.subrange(sps[i].range.start as int, sps[i].range.end as int),
{
    let t = sps.take(i + 1);
    assert(t.drop_last() =~= sps.take(i));
    assert(t.last() == sps[i]);
    if i > 0 {
        assert(sps.take(i).last() == sps[i - 1]);
    }
    assert(copied(src, sps[i]) =~= src.subrange(sps[i].range.start as int, sps[i].range.end as int));
}

/// What the splices produce up to splice `j` begins what they all
/// produce.
proof fn lemma_produced_prefix<V>(dst: Seq<V>, src: Seq<V>, sps: Seq<Splice>, j: int)
    requires
        0 <= j <= sps.len(),
        normal(sps, src.len() as int, dst.len() as int),
    ensures
        produced(dst, src, sps.take(j)).len() <= produced(dst, src, sps).len(),
        produced(dst, src, sps).take(produced(dst, src, sps.take(j)).len() as int) == produced(dst, src, sps.take(j)),
    decreases sps.len() - j,
{
    if j == sps.len() {
        assert(sps.take(j) =~= sps);
    } else {
        lemma_produced_prefix(dst, src, sps, j + 1);
        lemma_dst_step(dst, src, sps, j);
        let (p, q) = (produced(dst, src, sps.take(j)), produced(dst, src, sps.take(j + 1)));
        assert(q.take(p.len() as int) =~= p);
        assert(produced(dst, src, sps).take(q.len() as int).take(p.len() as int) =~= produced(dst, src, sps).take(
            p.len() as int,
        ));
    }
}

/// The destination with the splices from `k` on applied, back to front
/// (as `RawColumn::copy_ranges` applies them, so each lands above those
/// still to come and their positions hold): the destination's items up to
/// where splice `k - 1` ends, then what all the splices give from there.
pub(crate) open spec fn applied_from<V>(dst: Seq<V>, src: Seq<V>, sps: Seq<Splice>, k: int) -> Seq<V> {
    dst.subrange(0, end_of(sps.take(k))) + applied(dst, src, sps).skip(produced(dst, src, sps.take(k)).len() as int)
}

/// Back to front: before any splice, the destination; after them all,
/// what they give.
pub(crate) proof fn lemma_back_ends<V>(dst: Seq<V>, src: Seq<V>, sps: Seq<Splice>)
    requires
        normal(sps, src.len() as int, dst.len() as int),
    ensures
        applied_from(dst, src, sps, sps.len() as int) == dst,
        applied_from(dst, src, sps, 0) == applied(dst, src, sps),
{
    assert(sps.take(sps.len() as int) =~= sps);
    assert(sps.take(0) =~= Seq::<Splice>::empty());
    if sps.len() > 0 {
        lemma_dst_step(dst, src, sps, sps.len() - 1);
        assert(sps.take(sps.len() as int) =~= sps);
    }
    let p = produced(dst, src, sps);
    assert(applied(dst, src, sps).skip(p.len() as int) =~= dst.subrange(end_of(sps), dst.len() as int));
    assert(dst.subrange(0, end_of(sps)) + dst.subrange(end_of(sps), dst.len() as int) =~= dst);
    assert(dst.subrange(0, 0) + applied(dst, src, sps).skip(0) =~= applied(dst, src, sps));
}

/// The first `n` items of `x` and then `y` are the first `n` of `x`.
proof fn lemma_take_front<V>(x: Seq<V>, y: Seq<V>, n: int)
    requires
        0 <= n <= x.len(),
    ensures
        (x + y).take(n) == x.take(n),
{
    assert((x + y).take(n) =~= x.take(n));
}

/// A column that begins with `p` and then `m`: past `p`, it is `m` and
/// then the column past both.
proof fn lemma_skip_mid<V>(a: Seq<V>, p: Seq<V>, m: Seq<V>)
    requires
        p.len() + m.len() <= a.len(),
        a.take((p.len() + m.len()) as int) == p + m,
    ensures
        a.skip(p.len() as int) == m + a.skip((p.len() + m.len()) as int),
{
    let n = (p.len() + m.len()) as int;
    assert forall|i: int| 0 <= i < m.len() implies a[p.len() + i] == m[i] by {
        assert(a.take(n)[p.len() + i] == (p + m)[p.len() + i]);
    }
    assert(a.skip(p.len() as int) =~= m + a.skip(n));
}

/// A column `a` that begins with `q`, which is `p`, then the
/// destination's items `e..pos`, then `c`: the destination up to `e` and
/// `a` past `p` is the destination up to `e1` and `a` past `q`, with `c`
/// in place of the items `pos..e1`.
proof fn lemma_back_seqs<V>(dst: Seq<V>, a: Seq<V>, p: Seq<V>, q: Seq<V>, c: Seq<V>, e: int, pos: int, e1: int)
    requires
        0 <= e <= pos <= e1 <= dst.len(),
        q == p + dst.subrange(e, pos) + c,
        q.len() <= a.len(),
        a.take(q.len() as int) == q,
    ensures
        ({
            let f1 = dst.subrange(0, e1) + a.skip(q.len() as int);
            &&& e1 <= f1.len()
            &&& dst.subrange(0, e) + a.skip(p.len() as int) == f1.take(pos) + c + f1.skip(e1)
        }),
{
    let rest = a.skip(q.len() as int);
    let f1 = dst.subrange(0, e1) + rest;
    let m = dst.subrange(e, pos) + c;
    assert(f1.take(pos) =~= dst.subrange(0, pos));
    assert(f1.skip(e1) =~= rest);
    assert(p + dst.subrange(e, pos) + c =~= p + m);
    lemma_skip_mid(a, p, m);
    assert(dst.subrange(0, e) + (m + rest) =~= dst.subrange(0, pos) + c + rest);
}

/// Back to front, one more: splice `k` applied to what the splices
/// after it give.
#[verifier::spinoff_prover]
pub(crate) proof fn lemma_back_step<V>(dst: Seq<V>, src: Seq<V>, sps: Seq<Splice>, k: int)
    requires
        0 <= k < sps.len(),
        normal(sps, src.len() as int, dst.len() as int),
    ensures
        sps[k].pos + sps[k].delete <= applied_from(dst, src, sps, k + 1).len(),
        sps[k].range.start <= sps[k].range.end <= src.len(),
        applied_from(dst, src, sps, k) == applied_from(dst, src, sps, k + 1).take(sps[k].pos as int) + src.subrange(
            sps[k].range.start as int,
            sps[k].range.end as int,
        ) + applied_from(dst, src, sps, k + 1).skip(sps[k].pos + sps[k].delete),
{
    let a = applied(dst, src, sps);
    let (p, q) = (produced(dst, src, sps.take(k)), produced(dst, src, sps.take(k + 1)));
    let (e, e1) = (end_of(sps.take(k)), end_of(sps.take(k + 1)));
    let sp = sps[k];
    let c = src.subrange(sp.range.start as int, sp.range.end as int);
    lemma_dst_step(dst, src, sps, k);
    lemma_produced_prefix(dst, src, sps, k + 1);
    assert(sp.range.start <= sp.range.end <= src.len());
    assert(sp.pos + sp.delete <= dst.len());
    // what all the splices give begins with what they produce up to
    // splice `k`: the destination's items before it, then its range
    lemma_take_front(produced(dst, src, sps), dst.subrange(end_of(sps), dst.len() as int), q.len() as int);
    lemma_back_seqs(dst, a, p, q, c, e, sp.pos as int, e1);
}

/// A gap before splice `i`, given as a splice into what the splices keep.
#[verifier::spinoff_prover]
proof fn lemma_gap<V>(k: Seq<V>, dst: Seq<V>, src: Seq<V>, gaps: Seq<Splice>, g: Splice, sps: Seq<Splice>, i: int)
    requires
        0 <= i < sps.len(),
        normal(sps, src.len() as int, dst.len() as int),
        k == kept(src, sps),
        k == kept(src, sps.take(i + 1)) + kept(src, sps.skip(i + 1)),
        kept(src, sps.take(i + 1)) == kept(src, sps.take(i)) + src.subrange(sps[i].range.start as int, sps[i].range.end as int),
        g.pos == kept(src, sps.take(i)).len(),
        g.delete == 0,
        g.range.start == end_of(sps.take(i)),
        g.range.end == sps[i].pos,
        g.range.start < g.range.end,
        end_of(sps.take(i)) <= sps[i].pos,
        normal(gaps, dst.len() as int, k.len() as int),
        gaps.len() > 0 ==> gaps.last().range.end <= g.range.start && end_of(gaps) <= g.pos,
        produced(k, dst, gaps) + k.subrange(end_of(gaps), g.pos as int) == produced(dst, src, sps.take(i)),
        produced(dst, src, sps.take(i + 1)) == produced(dst, src, sps.take(i)) + dst.subrange(
            end_of(sps.take(i)),
            sps[i].pos as int,
        ) + src.subrange(sps[i].range.start as int, sps[i].range.end as int),
    ensures
        normal(gaps.push(g), dst.len() as int, k.len() as int),
        produced(k, dst, gaps.push(g)) + k.subrange(end_of(gaps.push(g)), g.pos + (sps[i].range.end - sps[i].range.start))
            == produced(dst, src, sps.take(i + 1)),
{
    hide(normal);
    let gs = gaps.push(g);
    let r = src.subrange(sps[i].range.start as int, sps[i].range.end as int);
    let a = g.pos as int;
    let b = a + r.len();
    // splice `i` is inside both columns, so the gap is, and the range
    // it keeps is inside what is kept
    assert(sps[i].pos + sps[i].delete <= dst.len() && sps[i].range.start <= sps[i].range.end <= src.len()) by {
        reveal(normal);
    }
    assert(b <= k.len());
    lemma_add_normal(gaps, g, dst.len() as int, k.len() as int);
    assert(gs.drop_last() =~= gaps);
    assert(gs.last() == g);
    assert(copied(dst, g) =~= dst.subrange(g.range.start as int, g.range.end as int));
    assert(k.subrange(a, b) =~= r);
    assert(produced(k, dst, gs) == produced(k, dst, gaps) + k.subrange(end_of(gaps), a) + dst.subrange(
        g.range.start as int,
        g.range.end as int,
    ));
    assert(end_of(gs) == a);
    assert(produced(k, dst, gs) + k.subrange(a, b) =~= produced(dst, src, sps.take(i + 1)));
}

/// No gap before splice `i`: its range follows on in what is kept.
proof fn lemma_no_gap<V>(k: Seq<V>, dst: Seq<V>, src: Seq<V>, gaps: Seq<Splice>, sps: Seq<Splice>, i: int, at: int)
    requires
        0 <= i < sps.len(),
        sps[i].pos <= dst.len(),
        sps[i].range.start <= sps[i].range.end <= src.len(),
        k == kept(src, sps.take(i + 1)) + kept(src, sps.skip(i + 1)),
        kept(src, sps.take(i + 1)) == kept(src, sps.take(i)) + src.subrange(sps[i].range.start as int, sps[i].range.end as int),
        at == kept(src, sps.take(i)).len(),
        end_of(sps.take(i)) == sps[i].pos,
        0 <= end_of(gaps) <= at,
        produced(k, dst, gaps) + k.subrange(end_of(gaps), at) == produced(dst, src, sps.take(i)),
        produced(dst, src, sps.take(i + 1)) == produced(dst, src, sps.take(i)) + dst.subrange(
            end_of(sps.take(i)),
            sps[i].pos as int,
        ) + src.subrange(sps[i].range.start as int, sps[i].range.end as int),
    ensures
        produced(k, dst, gaps) + k.subrange(end_of(gaps), at + (sps[i].range.end - sps[i].range.start))
            == produced(dst, src, sps.take(i + 1)),
{
    let r = src.subrange(sps[i].range.start as int, sps[i].range.end as int);
    let b = at + r.len();
    assert(k.subrange(at, b) =~= r);
    assert(k.subrange(end_of(gaps), b) =~= k.subrange(end_of(gaps), at) + k.subrange(at, b));
    assert(dst.subrange(sps[i].pos as int, sps[i].pos as int) =~= Seq::<V>::empty());
    assert(produced(k, dst, gaps) + k.subrange(end_of(gaps), b) =~= produced(dst, src, sps.take(i + 1)));
}

/// The gap after the last splice.
proof fn lemma_last_gap<V>(k: Seq<V>, dst: Seq<V>, src: Seq<V>, gaps: Seq<Splice>, g: Splice, sps: Seq<Splice>)
    requires
        k == kept(src, sps),
        g.pos == k.len(),
        g.delete == 0,
        g.range.start == end_of(sps),
        g.range.end == dst.len(),
        g.range.start < g.range.end,
        normal(gaps, dst.len() as int, k.len() as int),
        gaps.len() > 0 ==> gaps.last().range.end <= g.range.start && end_of(gaps) <= g.pos,
        produced(k, dst, gaps) + k.subrange(end_of(gaps), g.pos as int) == produced(dst, src, sps),
    ensures
        normal(gaps.push(g), dst.len() as int, k.len() as int),
        applied(k, dst, gaps.push(g)) == applied(dst, src, sps),
{
    hide(normal);
    let gs = gaps.push(g);
    lemma_add_normal(gaps, g, dst.len() as int, k.len() as int);
    assert(gs.drop_last() =~= gaps);
    assert(gs.last() == g);
    assert(copied(dst, g) =~= dst.subrange(g.range.start as int, g.range.end as int));
    assert(k.subrange(k.len() as int, k.len() as int) =~= Seq::<V>::empty());
    assert(applied(k, dst, gs) =~= applied(dst, src, sps));
}

} // verus!
