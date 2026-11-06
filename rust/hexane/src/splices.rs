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
//! the given lengths gives what the splices it took give. Trusted: the
//! glue that feeds it, and that the two ways `copy_ranges` carries the
//! list out do what `applied` says.

use crate::Splice;
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

/// Normalizing a list of splices, one at a time (`normalize_splices`).
pub(crate) struct Normalize {
    pub(crate) out: Vec<Splice>,
    /// where the splices so far end, in the source (the furthest) and the
    /// destination
    pub(crate) src_pos: usize,
    pub(crate) dst_pos: usize,
    pub(crate) src_len: usize,
    pub(crate) dst_len: usize,
    /// the splices taken so far, and a destination and source of the
    /// lengths given (any: nothing depends on their items)
    #[allow(dead_code)]
    pub(crate) seen: Ghost<Seq<Splice>>,
    #[allow(dead_code)]
    pub(crate) dst: Ghost<Seq<int>>,
    #[allow(dead_code)]
    pub(crate) src: Ghost<Seq<int>>,
}

/// What a splice copies, clamped: the same as the splice's own range.
proof fn lemma_clamp<V>(src: Seq<V>, sp: Splice, c: Splice)
    requires
        c.range.start == min_i(sp.range.start as int, src.len() as int),
        c.range.end == if min_i(sp.range.end as int, src.len() as int) > c.range.start {
            min_i(sp.range.end as int, src.len() as int)
        } else {
            c.range.start as int
        },
    ensures
        copied(src, c) == copied(src, sp),
        c.range.start <= c.range.end <= src.len(),
{
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
proof fn lemma_skip(dst: Seq<int>, src: Seq<int>, out: Seq<Splice>, seen: Seq<Splice>, sp: Splice)
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
    assert(copied(src, sp) =~= Seq::<int>::empty());
    assert(dst.subrange(end_of(out), end_of(seen)) + dst.subrange(end_of(seen), sp.pos as int)
        =~= dst.subrange(end_of(out), sp.pos as int));
    assert(produced(dst, src, s1) =~= produced(dst, src, out) + dst.subrange(end_of(out), end_of(s1)));
}

/// Adding a splice, clamped.
proof fn lemma_add(dst: Seq<int>, src: Seq<int>, out: Seq<Splice>, seen: Seq<Splice>, sp: Splice, c: Splice, src_len: int, dst_len: int, src_pos: int)
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

proof fn lemma_add_produced(dst: Seq<int>, src: Seq<int>, out: Seq<Splice>, seen: Seq<Splice>, sp: Splice, c: Splice)
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
    assert(dst.subrange(end_of(o1), end_of(s1)) =~= Seq::<int>::empty());
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
proof fn lemma_merge(dst: Seq<int>, src: Seq<int>, o: Seq<Splice>, last: Splice, seen: Seq<Splice>, sp: Splice, c: Splice, m: Splice, src_len: int, dst_len: int)
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
            assert(copied(src, last) =~= Seq::<int>::empty());
        }
        if last.range.end < m.range.end {
            assert(copied(src, c) == src.subrange(last.range.end as int, m.range.end as int));
        } else {
            assert(copied(src, c) =~= Seq::<int>::empty());
        }
    }
    assert(end_of(seen) == sp.pos);
    assert(dst.subrange(sp.pos as int, sp.pos as int) =~= Seq::<int>::empty());
    assert(dst.subrange(end_of(om), end_of(s1)) =~= Seq::<int>::empty());
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
        &&& self.dst@.len() == self.dst_len && self.src@.len() == self.src_len
        &&& in_order(seen, self.dst_len as int)
        &&& self.dst_pos == end_of(seen)
        &&& normal(out, self.src_len as int, self.dst_len as int)
        &&& end_of(out) <= end_of(seen)
        &&& out.len() > 0 ==> out.last().range.end <= min_i(self.src_pos as int, self.src_len as int)
        &&& produced(self.dst@, self.src@, seen) == produced(self.dst@, self.src@, out)
            + self.dst@.subrange(end_of(out), end_of(seen))
    }

    /// Start normalizing splices from a source of `src_len` items into a
    /// destination of `dst_len`; `dst` and `src` are any such columns.
    #[inline(always)]
    pub(crate) fn new(src_len: usize, dst_len: usize, Ghost(dst): Ghost<Seq<int>>, Ghost(src): Ghost<Seq<int>>) -> (n: Self)
        requires
            dst.len() == dst_len,
            src.len() == src_len,
        ensures
            n.inv(),
            n.seen@.len() == 0,
            n.src_len == src_len,
            n.dst_len == dst_len,
            n.dst@ == dst,
            n.src@ == src,
    {
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

} // verus!
