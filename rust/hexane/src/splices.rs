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

pub(crate) open spec fn min_i(a: int, b: int) -> int {
    if a <= b { a } else { b }
}

/// The source items a splice's range copies: clamped to the source, and
/// none for a reversed range.
pub(crate) open spec fn copied<V>(src: Seq<V>, sp: Splice) -> Seq<V> {
    let s = min_i(sp.range.start as int, src.len() as int);
    let e = min_i(sp.range.end as int, src.len() as int);
    if s < e { src.subrange(s, e) } else { Seq::empty() }
}

/// Where the last of `sps` ends in the destination (0 for none).
pub(crate) open spec fn end_of(sps: Seq<Splice>) -> int {
    if sps.len() == 0 { 0 } else { sps.last().pos + sps.last().delete }
}

/// The destination's items up to where the last splice ends, edited.
pub(crate) open spec fn produced<V>(dst: Seq<V>, src: Seq<V>, sps: Seq<Splice>) -> Seq<V>
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
pub(crate) open spec fn applied<V>(dst: Seq<V>, src: Seq<V>, sps: Seq<Splice>) -> Seq<V> {
    produced(dst, src, sps) + dst.subrange(end_of(sps), dst.len() as int)
}

/// Splices ascending in the destination and inside it.
pub(crate) open spec fn in_order(sps: Seq<Splice>, dst_len: int) -> bool {
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
    if sps.len() == 0 {
        assert(Seq::<V>::empty().map_values(f) =~= Seq::<W>::empty());
    } else {
        let p = sps.drop_last();
        let l = sps.last();
        assert(in_order(p, d.len() as int)) by {
            assert forall|i: int| 0 <= i < p.len() implies #[trigger] p[i].pos + p[i].delete <= d.len() by {
                assert(p[i] == sps[i]);
            }
            assert forall|i: int| 0 < i < p.len() implies p[i - 1].pos + p[i - 1].delete <= #[trigger] p[i].pos by {
                assert(p[i] == sps[i] && p[i - 1] == sps[i - 1]);
            }
        }
        lemma_produced_map(d, s, p, f);
        lemma_copied_map(s, l, f);
        let a = end_of(p);
        let b = l.pos as int;
        assert(sps[sps.len() - 1] == l);
        assert(l.pos + l.delete <= d.len());
        if p.len() > 0 {
            assert(p.last() == sps[sps.len() - 2]);
            assert(sps[sps.len() - 2].pos + sps[sps.len() - 2].delete <= sps[sps.len() - 1].pos);
        }
        assert(0 <= a <= b <= d.len());
        assert(d.map_values(f).subrange(a, b) =~= d.subrange(a, b).map_values(f));
        let x = produced(d, s, p);
        let y = d.subrange(a, b);
        let z = copied(s, l);
        assert((x + y + z).map_values(f) =~= x.map_values(f) + y.map_values(f) + z.map_values(f));
    }
}

/// Splices in order make of mapped columns the mapped result.
proof fn lemma_applied_map<V, W>(d: Seq<V>, s: Seq<V>, sps: Seq<Splice>, f: spec_fn(V) -> W)
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
    let o1 = out.push(c);
    assert forall|i: int| 0 < i < o1.len() implies o1[i - 1].range.end <= #[trigger] o1[i].range.start by {
        if i == o1.len() - 1 {
            assert(o1[i - 1] == out.last());
        }
    }
    assert forall|i: int| 0 < i < o1.len() implies o1[i - 1].pos + o1[i - 1].delete <= #[trigger] o1[i].pos by {
        if i == o1.len() - 1 {
            assert(o1[i - 1] == out.last());
        }
    }
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
    let s1 = seen.push(sp);
    let o1 = o.push(last);
    let om = o.push(m);
    assert(s1.drop_last() =~= seen);
    assert(o1.drop_last() =~= o);
    assert(om.drop_last() =~= o);
    assert(o1.last() == last);
    assert(o1[o1.len() - 1] == last);
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
    assert forall|i: int| 0 < i < om.len() implies om[i - 1].range.end <= #[trigger] om[i].range.start by {
        if i == om.len() - 1 {
            assert(om[i - 1] == o1[i - 1]);
        }
    }
    assert forall|i: int| 0 < i < om.len() implies om[i - 1].pos + om[i - 1].delete <= #[trigger] om[i].pos by {
        if i == om.len() - 1 {
            assert(om[i - 1] == o1[i - 1]);
        }
    }
    assert forall|i: int| 0 <= i < om.len() implies #[trigger] om[i].range.start <= om[i].range.end <= src_len by {
        if i < om.len() - 1 {
            assert(om[i] == o1[i]);
        }
    }
    assert forall|i: int| 0 <= i < om.len() implies #[trigger] om[i].range.start < om[i].range.end || om[i].delete > 0 by {
        if i < om.len() - 1 {
            assert(om[i] == o1[i]);
        }
    }
    assert forall|i: int| 0 <= i < om.len() implies #[trigger] om[i].pos + om[i].delete <= dst_len by {
        if i < om.len() - 1 {
            assert(om[i] == o1[i]);
        }
    }
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
            final(self).src_len == old(self).src_len,
            final(self).dst_len == old(self).dst_len,
            final(self).dst == old(self).dst,
            final(self).src == old(self).src,
    {
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
                lemma_skip(dst, src, out, seen, sp);
            }
            return;
        } else if n > 0 {
            (self.out[n - 1].range.end, self.out[n - 1].range.end)
        } else {
            (0, 0)
        };
        proof {
            assert(copied(src, Splice { pos: sp.pos, delete: sp.delete, range: start..end }) == copied(src, sp)) by {
                if s0 >= e0 {
                    assert(copied(src, sp) =~= Seq::<int>::empty());
                }
            }
            if n > 0 {
                assert(out[n - 1] == out.last());
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
                let k = kept(src, sps@.skip(i + 1));
                let r = src.subrange(start as int, end as int);
                assert(src.subrange(0, end as int) =~= src.subrange(0, start as int) + r);
                if i > 0 {
                    assert(sps@[i - 1].range.end <= sps@[i as int].range.start);
                }
            }
            self.i = i;
            self.end = start;
            if end < to {
                proof {
                    let cur = removed(src, cuts0);
                    let k = kept(src, sps@.skip(i + 1));
                    assert(cur == src.subrange(0, to as int) + k);
                    assert(cur.len() >= to);
                    assert(cur.subrange(0, end as int) =~= src.subrange(0, end as int));
                    assert(cur.subrange(to as int, cur.len() as int) =~= k);
                    let c = (end, (to - end) as usize);
                    self.cuts = Ghost(cuts0.push(c));
                    assert(self.cuts@.drop_last() =~= cuts0);
                    assert(removed(src, self.cuts@) =~= src.subrange(0, start as int) + kept(src, sps@.skip(i as int)));
                }
                return Some((end, to - end));
            }
            proof {
                assert(src.subrange(0, to as int) + kept(src, sps@.skip(i + 1)) =~= src.subrange(0, start as int)
                    + kept(src, sps@.skip(i as int)));
            }
        }
        if self.end > 0 {
            let to = self.end;
            self.end = 0;
            proof {
                let cur = removed(src, cuts0);
                let k = kept(src, sps@.skip(0));
                assert(sps@.skip(0) =~= sps@.skip(self.i as int));
                assert(cur.len() >= to);
                assert(cur.subrange(0, 0) + cur.subrange(to as int, cur.len() as int) =~= k);
                self.cuts = Ghost(cuts0.push((0usize, to)));
                assert(self.cuts@.drop_last() =~= cuts0);
                assert(removed(src, self.cuts@) =~= src.subrange(0, 0) + k);
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
                assert(sps@[i as int].pos + sps@[i as int].delete <= dst.len());
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

/// A gap before splice `i`, given as a splice into what the splices keep.
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
    let gs = gaps.push(g);
    let r = src.subrange(sps[i].range.start as int, sps[i].range.end as int);
    let a = g.pos as int;
    let b = a + r.len();
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
    assert(b <= k.len());
    assert forall|j: int| 0 < j < gs.len() implies gs[j - 1].pos + gs[j - 1].delete <= #[trigger] gs[j].pos by {
        if j == gs.len() - 1 {
            assert(gs[j - 1] == gaps.last());
        }
    }
    assert forall|j: int| 0 < j < gs.len() implies gs[j - 1].range.end <= #[trigger] gs[j].range.start by {
        if j == gs.len() - 1 {
            assert(gs[j - 1] == gaps.last());
        }
    }
    assert(g.range.end <= dst.len()) by {
        assert(sps[i].pos + sps[i].delete <= dst.len());
    }
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
    let gs = gaps.push(g);
    assert(gs.drop_last() =~= gaps);
    assert(gs.last() == g);
    assert(copied(dst, g) =~= dst.subrange(g.range.start as int, g.range.end as int));
    assert(k.subrange(k.len() as int, k.len() as int) =~= Seq::<V>::empty());
    assert(applied(k, dst, gs) =~= applied(dst, src, sps));
    assert forall|j: int| 0 < j < gs.len() implies gs[j - 1].pos + gs[j - 1].delete <= #[trigger] gs[j].pos by {
        if j == gs.len() - 1 {
            assert(gs[j - 1] == gaps.last());
        }
    }
    assert forall|j: int| 0 < j < gs.len() implies gs[j - 1].range.end <= #[trigger] gs[j].range.start by {
        if j == gs.len() - 1 {
            assert(gs[j - 1] == gaps.last());
        }
    }
}

} // verus!
