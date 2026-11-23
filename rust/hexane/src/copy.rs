//! `copy_ranges`' two strategies, verified with Verus.
//!
//! [`copy_ranges`](crate::Column::copy_ranges) normalizes its splices
//! (`splices::Normalize`) and then carries them out one of two ways, each
//! a chain of the edit cursor's steps. The source's items come from a
//! [`RangeFeed`]: shifted to a range, it gives the source's items in it,
//! in runs. Proved, given the column invariant on entry:
//!
//! - [`copy_direct`] applies the list to the destination with one
//!   cursor, seeking to each splice, deleting and inserting the range the
//!   feed gives: the column becomes `applied(dst, src, sps)`.
//! - [`copy_invert`] starts from the source's slabs: it cuts the gaps
//!   between the ranges out of them, back to front (`splices::Cuts`, each
//!   cut a [`splice_feed`](crate::cursor::splice_feed)), then puts the
//!   destination's items back between the ranges kept with one cursor
//!   (`splices::Gaps`, the feed now over the destination). The column
//!   becomes the same `applied(dst, src, sps)`.
//!
//! Both keep the column invariant, and both end: every loop has a
//! measure, the feed's included.
//!
//! The source's feed is verified too ([`SrcFeed`]): a reader over the
//! source column's slabs, moved to each range with its index, and each
//! run's value written as its owned form (`ReadsAgree::ins`, verified for
//! both encodings, through `rle::lift::Lift` for RLE). Trusted: what the
//! cursor trusts.

#![allow(clippy::too_many_arguments)]

use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::{all_ok, col_ok, lemma_vals_own, lemma_vals_total, slab_lens, vals_of};
use crate::cursor::{splice_feed_bounded, BoundedFeed, Cursor, Insertable, ReadsAgree, RunFeed};
use crate::cursor::{Slabs, Visitor, Weigh};
#[cfg(verus_keep_ghost)]
use crate::fenwick::sum;
#[cfg(verus_keep_ghost)]
use crate::index::wlens;
use crate::index::IndexOps;
#[cfg(verus_keep_ghost)]
use crate::iter_core::Decode;
#[cfg(verus_keep_ghost)]
use crate::iter_core::{find_ok, vals_of as iter_vals_of};
use crate::iter_core::{Decoders, ICore};
#[cfg(verus_keep_ghost)]
use crate::splices::{applied, copied, cut, end_of, kept, normal, produced, ranges_in, removed};
use crate::splices::{Cuts, Gaps};
use crate::Splice;
use std::marker::PhantomData;
use std::ops::Range;
use vstd::prelude::*;

verus! {

/// A source's items, a range at a time: shifted to a range, it gives the
/// source's items in it, in runs, then `None`. It moves only forwards.
pub(crate) trait RangeFeed<E: Slabs> {
    type X: Insertable<E>;

    /// The feed's own invariant, which it keeps.
    spec fn inv(&self) -> bool;

    /// The source's items.
    spec fn src(&self) -> Seq<E::V>;

    /// Where the next item comes from, and where the range ends.
    spec fn at(&self) -> int;

    spec fn end(&self) -> int;

    fn shift(&mut self, range: Range<usize>)
        requires
            old(self).inv(),
            old(self).at() <= range.start <= range.end <= old(self).src().len(),
        ensures
            final(self).inv(),
            final(self).src() == old(self).src(),
            final(self).at() == range.start,
            final(self).end() == range.end,
    ;

    fn next_run(&mut self) -> (r: Option<(Self::X, usize)>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).src() == old(self).src(),
            final(self).end() == old(self).end(),
            match r {
                Some((x, n)) => {
                    &&& n >= 1
                    &&& old(self).at() + n <= old(self).end()
                    &&& final(self).at() == old(self).at() + n
                    &&& old(self).src().subrange(old(self).at(), old(self).at() + n) == Seq::new(
                        n as nat,
                        |_i: int| x.val(),
                    )
                },
                None => final(self).at() == old(self).at() && old(self).at() >= old(self).end(),
            },
    ;
}

/// No runs at all: a splice that only deletes.
pub(crate) struct NoRuns<X> {
    #[allow(dead_code)]
    pub(crate) none: Option<X>,
}

impl<E: Slabs, X: Insertable<E>> RunFeed<E> for NoRuns<X> {
    type X = X;

    type K = ();

    open spec fn inv(&self) -> bool {
        true
    }

    open spec fn key(&self) -> () {
        ()
    }

    open spec fn given(&self) -> Seq<E::V> {
        Seq::empty()
    }

    open spec fn ended(&self) -> bool {
        true
    }

    #[inline(always)]
    fn next_run(&mut self) -> (r: Option<(X, usize)>) {
        None
    }
}

impl<E: Slabs, X: Insertable<E>> BoundedFeed<E> for NoRuns<X> {
    open spec fn bound(&self) -> nat {
        0
    }

    #[inline(always)]
    fn next_bounded(&mut self) -> (r: Option<(X, usize)>) {
        None
    }
}

/// Insert at the cursor every item the feed has left in its range.
#[inline(never)]
fn insert_range<E: Slabs, R: RangeFeed<E>>(
    cur: &mut Cursor<E>,
    slabs: &Vec<Slab<E::Tail>>,
    max_segments: usize,
    feed: &mut R,
)
    requires
        old(cur).wf(slabs@),
        crate::cursor::all_ok::<E>(slabs@),
        max_segments >= 2,
        old(feed).inv(),
        0 <= old(feed).at() <= old(feed).end() <= old(feed).src().len(),
    ensures
        final(feed).inv(),
        final(cur).wf(slabs@),
        final(cur).o == old(cur).o,
        final(cur).orig == old(cur).orig,
        final(cur).remaining == old(cur).remaining,
        final(cur).slab == old(cur).slab,
        final(cur).in_slab == old(cur).in_slab,
        final(cur).p@ == old(cur).p@ + old(feed).src().subrange(old(feed).at(), old(feed).end()),
        final(feed).src() == old(feed).src(),
        final(feed).at() == old(feed).end(),
        final(feed).end() == old(feed).end(),
{
    let ghost (src, a0, e0, p0) = (feed.src(), feed.at(), feed.end(), cur.p@);
    proof {
        assert(src.subrange(a0, a0) =~= Seq::<E::V>::empty());
        assert(p0 + src.subrange(a0, a0) =~= p0);
    }
    loop
        invariant
            cur.wf(slabs@),
            crate::cursor::all_ok::<E>(slabs@),
            max_segments >= 2,
            cur.o == old(cur).o,
            cur.orig == old(cur).orig,
            cur.remaining == old(cur).remaining,
            cur.slab == old(cur).slab,
            cur.in_slab == old(cur).in_slab,
            feed.inv(),
            feed.src() == src,
            feed.end() == e0,
            0 <= a0 <= feed.at() <= e0 <= src.len(),
            cur.p@ == p0 + src.subrange(a0, feed.at()),
        ensures
            feed.inv(),
            cur.wf(slabs@),
            cur.o == old(cur).o,
            cur.orig == old(cur).orig,
            cur.remaining == old(cur).remaining,
            cur.slab == old(cur).slab,
            cur.in_slab == old(cur).in_slab,
            feed.src() == src,
            feed.end() == e0,
            feed.at() == e0,
            cur.p@ == p0 + src.subrange(a0, e0),
        decreases feed.end() - feed.at(),
    {
        let ghost a = feed.at();
        match feed.next_run() {
            None => break,
            Some((x, n)) => {
                let ghost xv = x.val();
                cur.insert(slabs, max_segments, x, n);
                proof {
                    let run = Seq::new(n as nat, |_i: int| xv);
                    assert(src.subrange(a0, a + n) =~= src.subrange(a0, a) + src.subrange(a, a + n));
                    assert(cur.p@ =~= p0 + src.subrange(a0, feed.at()));
                }
            },
        }
    }
}

/// Splice `k` of a normalized list lies inside both columns, and starts
/// in each no earlier than the splices before it end.
pub(crate) proof fn lemma_splice_at(sps: Seq<Splice>, src_len: int, dst_len: int, k: int)
    requires
        normal(sps, src_len, dst_len),
        0 <= k < sps.len(),
    ensures
        0 <= end_of(sps.take(k)) <= sps[k].pos,
        sps[k].pos + sps[k].delete <= dst_len,
        end_of(sps.take(k + 1)) == sps[k].pos + sps[k].delete,
        sps[k].range.start <= sps[k].range.end <= src_len,
        k > 0 ==> sps[k - 1].range.end <= sps[k].range.start,
{
    let sp = sps[k];
    assert(sps.take(k + 1).last() == sp);
    assert(sp.range.start <= sp.range.end <= src_len);
    if k > 0 {
        assert(sps.take(k).last() == sps[k - 1]);
        assert(sps[k - 1].range.end <= sp.range.start);
    }
}

/// What the first `k + 1` splices produce: what the first `k` do, then
/// the destination's items up to splice `k`, then the range it copies.
pub(crate) proof fn lemma_produced_next<V>(dst: Seq<V>, src: Seq<V>, sps: Seq<Splice>, k: int)
    requires
        0 <= k < sps.len(),
        sps[k].range.start <= sps[k].range.end <= src.len(),
    ensures
        produced(dst, src, sps.take(k + 1)) == produced(dst, src, sps.take(k)) + dst.subrange(
            end_of(sps.take(k)),
            sps[k].pos as int,
        ) + src.subrange(sps[k].range.start as int, sps[k].range.end as int),
{
    let t = sps.take(k + 1);
    assert(t.drop_last() =~= sps.take(k));
    assert(t.last() == sps[k]);
    assert(copied(src, sps[k]) =~= src.subrange(sps[k].range.start as int, sps[k].range.end as int));
}

/// A column's item count is the length of its values.
proof fn lemma_col_len<E: Slabs, W, I: IndexOps<W>, G: Weigh<E, W>>(
    s: Seq<Slab<E::Tail>>,
    ix: I,
    total_len: usize,
    max_segments: usize,
)
    requires
        col_ok::<E, W, I, G>(s, ix, total_len, max_segments),
    ensures
        vals_of::<E>(s).len() == total_len,
{
    lemma_vals_total::<E>(s);
}

/// What an edit needs of a cursor in a column: the cursor stands in
/// well-formed slabs, and the original items from it on are the ones it
/// has left.
proof fn lemma_cur_left<E: Slabs, W, I: IndexOps<W>, G: Weigh<E, W>>(
    cur: Cursor<E>,
    s: Seq<Slab<E::Tail>>,
    ix: I,
    total_len: usize,
    max_segments: usize,
)
    requires
        cur.ok::<W, I, G>(s, ix, total_len, max_segments),
    ensures
        cur.wf(s),
        all_ok::<E>(s),
        max_segments >= 2,
        cur.orig + cur.remaining == cur.o@.len(),
{
}

/// A column whose cursor has no rebuild open holds what the cursor
/// produced, then the original items from the cursor on; and it has
/// room for three more items.
proof fn lemma_cur_closed<E: Slabs, W, I: IndexOps<W>, G: Weigh<E, W>>(
    cur: Cursor<E>,
    s: Seq<Slab<E::Tail>>,
    ix: I,
    total_len: usize,
    max_segments: usize,
)
    requires
        cur.ok::<W, I, G>(s, ix, total_len, max_segments),
        !cur.dirty,
    ensures
        vals_of::<E>(s) == cur.p@ + cur.o@.subrange(cur.orig as int, cur.o@.len() as int),
        total_len + 3 <= usize::MAX,
{
    cur.lemma_clean(s);
    lemma_vals_total::<E>(s);
}

/// `copy_ranges`, directly: one cursor over the destination applies the
/// normalized splices `sps` in turn, each range's items from `feed`. The
/// column becomes `applied(dst, src, sps)`.
#[verifier::spinoff_prover]
#[inline(always)]
pub(crate) fn copy_direct<E: Slabs, W, I: IndexOps<W>, G: Weigh<E, W>, F: Visitor<E>, R: RangeFeed<E>>(
    slabs: &mut Vec<Slab<E::Tail>>,
    index: &mut I,
    total_len: &mut usize,
    max_segments: usize,
    counter: &mut usize,
    sps: &[Splice],
    f: &mut F,
    feed: &mut R,
)
    requires
        col_ok::<E, W, I, G>(old(slabs)@, *old(index), *old(total_len), max_segments),
        *old(total_len) + 3 <= usize::MAX,
        normal(sps@, old(feed).src().len() as int, *old(total_len) as int),
        old(feed).inv(),
        old(feed).at() == 0,
    ensures
        col_ok::<E, W, I, G>(final(slabs)@, *final(index), *final(total_len), max_segments),
        *final(total_len) + 3 <= usize::MAX,
        vals_of::<E>(final(slabs)@) == applied(vals_of::<E>(old(slabs)@), old(feed).src(), sps@),
{
    hide(col_ok);
    hide(Cursor::wf);
    hide(vals_of);
    hide(all_ok);
    hide(normal);
    hide(produced);
    let ghost dst = vals_of::<E>(slabs@);
    let ghost src = feed.src();
    proof {
        lemma_col_len::<E, W, I, G>(slabs@, *index, *total_len, max_segments);
        assert(sps@.take(0) =~= Seq::<Splice>::empty());
        assert(dst.take(0) =~= Seq::<E::V>::empty());
        assert(produced(dst, src, Seq::<Splice>::empty()) =~= Seq::<E::V>::empty()) by {
            reveal(produced);
        }
    }
    let mut cur = Cursor::<E>::new::<W, I, G>(slabs, index, *total_len, Ghost(max_segments), 0);
    let mut k: usize = 0;
    while k < sps.len()
        invariant
            cur.ok::<W, I, G>(slabs@, *index, *total_len, max_segments),
            cur.on_value(slabs@),
            cur.o@ == dst,
            k <= sps@.len(),
            normal(sps@, src.len() as int, dst.len() as int),
            cur.orig == end_of(sps@.take(k as int)),
            cur.p@ == produced(dst, src, sps@.take(k as int)),
            feed.inv(),
            feed.src() == src,
            k == 0 ==> feed.at() == 0,
            k > 0 ==> feed.at() == sps@[k - 1].range.end,
        decreases sps@.len() - k,
    {
        let pos = sps[k].pos;
        let del = sps[k].delete;
        let start = sps[k].range.start;
        let end = sps[k].range.end;
        proof {
            lemma_splice_at(sps@, src.len() as int, dst.len() as int, k as int);
            lemma_produced_next(dst, src, sps@, k as int);
            lemma_cur_left::<E, W, I, G>(cur, slabs@, *index, *total_len, max_segments);
        }
        let to = pos - cur.orig;
        cur.advance_with::<W, I, G, F>(slabs, index, total_len, max_segments, counter, to, f);
        proof {
            lemma_cur_left::<E, W, I, G>(cur, slabs@, *index, *total_len, max_segments);
        }
        cur.delete_with::<W, I, G, F>(slabs, index, total_len, max_segments, counter, del, f);
        feed.shift(start..end);
        proof {
            lemma_cur_left::<E, W, I, G>(cur, slabs@, *index, *total_len, max_segments);
        }
        insert_range(&mut cur, slabs, max_segments, feed);
        k += 1;
    }
    cur.close::<W, I, G>(slabs, index, total_len, max_segments, counter);
    proof {
        lemma_cur_closed::<E, W, I, G>(cur, slabs@, *index, *total_len, max_segments);
        assert(sps@.take(sps@.len() as int) =~= sps@);
    }
}

/// Cut the gaps between `sps`' ranges out of the column, back to front:
/// it keeps exactly the ranges' items.
#[inline(always)]
fn cut_gaps<E: Slabs, W, I: IndexOps<W>, G: Weigh<E, W>, F: Visitor<E>, X: Insertable<E>>(
    slabs: &mut Vec<Slab<E::Tail>>,
    index: &mut I,
    total_len: &mut usize,
    max_segments: usize,
    counter: &mut usize,
    sps: &[Splice],
    f: &mut F,
    Ghost(none): Ghost<Option<X>>,
)
    requires
        col_ok::<E, W, I, G>(old(slabs)@, *old(index), *old(total_len), max_segments),
        *old(total_len) + 3 <= usize::MAX,
        ranges_in(sps@, *old(total_len) as int),
    ensures
        col_ok::<E, W, I, G>(final(slabs)@, *final(index), *final(total_len), max_segments),
        *final(total_len) <= *old(total_len),
        vals_of::<E>(final(slabs)@) == kept(vals_of::<E>(old(slabs)@), sps@),
{
    let ghost src = vals_of::<E>(slabs@);
    proof {
        lemma_vals_total::<E>(slabs@);
    }
    let mut cuts = Cuts::<E::V>::new(sps, *total_len, Ghost(src));
    loop
        invariant
            col_ok::<E, W, I, G>(slabs@, *index, *total_len, max_segments),
            *total_len <= src.len(),
            src.len() + 3 <= usize::MAX,
            src.len() == *old(total_len),
            cuts.inv(),
            cuts.sps@ == sps@,
            cuts.src@ == src,
            vals_of::<E>(slabs@) == removed(src, cuts.cuts@),
        ensures
            col_ok::<E, W, I, G>(slabs@, *index, *total_len, max_segments),
            *total_len <= src.len(),
            vals_of::<E>(slabs@) == kept(src, sps@),
        decreases cuts.left(),
    {
        let ghost cuts0 = cuts.cuts@;
        match cuts.next(sps) {
            None => break,
            Some((at, n)) => {
                proof {
                    lemma_vals_total::<E>(slabs@);
                }
                let mut none = NoRuns::<X> { none: None };
                let _ins = splice_feed_bounded::<E, W, I, G, F, NoRuns<X>>(
                    slabs,
                    index,
                    total_len,
                    max_segments,
                    counter,
                    at,
                    n,
                    f,
                    &mut none,
                );
                proof {
                    let cur = removed(src, cuts0);
                    assert(cuts.cuts@.drop_last() =~= cuts0);
                    assert(_ins@ =~= Seq::<E::V>::empty());
                    assert(vals_of::<E>(slabs@) =~= cut(cur, at as int, n as int));
                    lemma_vals_total::<E>(slabs@);
                }
            },
        }
    }
}

/// Put the destination's items (from `feed`) back between the ranges
/// the column kept of the source: the column becomes `applied(dst, src,
/// sps)`.
#[verifier::spinoff_prover]
#[inline(always)]
fn put_gaps<E: Slabs, W, I: IndexOps<W>, G: Weigh<E, W>, F: Visitor<E>, R: RangeFeed<E>>(
    slabs: &mut Vec<Slab<E::Tail>>,
    index: &mut I,
    total_len: &mut usize,
    max_segments: usize,
    counter: &mut usize,
    sps: &[Splice],
    dst_len: usize,
    f: &mut F,
    feed: &mut R,
    Ghost(src): Ghost<Seq<E::V>>,
)
    requires
        col_ok::<E, W, I, G>(old(slabs)@, *old(index), *old(total_len), max_segments),
        *old(total_len) + 3 <= usize::MAX,
        normal(sps@, src.len() as int, dst_len as int),
        vals_of::<E>(old(slabs)@) == kept(src, sps@),
        old(feed).inv(),
        old(feed).src().len() == dst_len,
        old(feed).at() == 0,
    ensures
        col_ok::<E, W, I, G>(final(slabs)@, *final(index), *final(total_len), max_segments),
        *final(total_len) + 3 <= usize::MAX,
        vals_of::<E>(final(slabs)@) == applied(old(feed).src(), src, sps@),
{
    hide(Gaps::inv);
    hide(col_ok);
    hide(Cursor::wf);
    hide(vals_of);
    hide(all_ok);
    hide(normal);
    hide(produced);
    hide(kept);
    let ghost dst = feed.src();
    let ghost k = kept(src, sps@);
    proof {
        lemma_col_len::<E, W, I, G>(slabs@, *index, *total_len, max_segments);
    }
    let mut gaps = Gaps::<E::V>::new(sps, dst_len, Ghost(dst), Ghost(src));
    let mut cur = Cursor::<E>::new::<W, I, G>(slabs, index, *total_len, Ghost(max_segments), 0);
    proof {
        assert(k.take(0) =~= Seq::<E::V>::empty());
        assert(produced(k, dst, gaps.gaps@) =~= Seq::<E::V>::empty()) by {
            reveal(produced);
        }
        assert(normal(gaps.gaps@, dst.len() as int, k.len() as int)) by {
            reveal(normal);
        }
    }
    loop
        invariant
            cur.ok::<W, I, G>(slabs@, *index, *total_len, max_segments),
            cur.on_value(slabs@),
            cur.o@ == k,
            k == kept(src, sps@),
            gaps.inv(),
            gaps.sps@ == sps@,
            gaps.dst@ == dst,
            gaps.src@ == src,
            cur.orig == end_of(gaps.gaps@),
            cur.p@ == produced(k, dst, gaps.gaps@),
            normal(gaps.gaps@, dst.len() as int, k.len() as int),
            feed.inv(),
            feed.src() == dst,
            gaps.gaps@.len() == 0 ==> feed.at() == 0,
            gaps.gaps@.len() > 0 ==> feed.at() == gaps.gaps@.last().range.end,
        ensures
            cur.ok::<W, I, G>(slabs@, *index, *total_len, max_segments),
            cur.o@ == k,
            cur.orig == end_of(gaps.gaps@),
            cur.p@ == produced(k, dst, gaps.gaps@),
            applied(k, dst, gaps.gaps@) == applied(dst, src, sps@),
        decreases gaps.left(),
    {
        let ghost gaps0 = gaps.gaps@;
        match gaps.next(sps) {
            None => break,
            Some((at, gap)) => {
                let ghost gs = gaps.gaps@;
                let ghost sp = gs.last();
                let ghost (start, end) = (gap.start, gap.end);
                proof {
                    assert(gaps.dst@ == dst);
                    assert(gaps.src@ == src);
                    assert(gaps.sps@ == sps@);
                    assert(normal(gs, dst.len() as int, k.len() as int));
                    assert(gs.take(gs.len() - 1) =~= gaps0);
                    assert(gs.take(gs.len() as int) =~= gs);
                    assert(gs[gs.len() - 1] == sp);
                    if gaps0.len() > 0 {
                        assert(gs[gs.len() - 2] == gaps0.last());
                    }
                    lemma_splice_at(gs, dst.len() as int, k.len() as int, gs.len() - 1);
                    lemma_produced_next(k, dst, gs, gs.len() - 1);
                    lemma_cur_left::<E, W, I, G>(cur, slabs@, *index, *total_len, max_segments);
                }
                let to = at - cur.orig;
                cur.advance_with::<W, I, G, F>(slabs, index, total_len, max_segments, counter, to, f);
                feed.shift(gap);
                proof {
                    lemma_cur_left::<E, W, I, G>(cur, slabs@, *index, *total_len, max_segments);
                }
                insert_range(&mut cur, slabs, max_segments, feed);
                proof {
                    assert(cur.p@ =~= produced(k, dst, gs));
                }
            },
        }
    }
    cur.close::<W, I, G>(slabs, index, total_len, max_segments, counter);
    proof {
        lemma_cur_closed::<E, W, I, G>(cur, slabs@, *index, *total_len, max_segments);
    }
}

/// `copy_ranges`, inverted: the column holds the source; cut the gaps
/// between `sps`' ranges out of it, then put the destination's items
/// (from `feed`) back between the ranges kept. The column becomes
/// `applied(dst, src, sps)`, as [`copy_direct`] makes it of the
/// destination.
#[inline(always)]
pub(crate) fn copy_invert<E: Slabs, W, I: IndexOps<W>, G: Weigh<E, W>, F: Visitor<E>, R: RangeFeed<E>>(
    slabs: &mut Vec<Slab<E::Tail>>,
    index: &mut I,
    total_len: &mut usize,
    max_segments: usize,
    counter: &mut usize,
    sps: &[Splice],
    dst_len: usize,
    f: &mut F,
    feed: &mut R,
)
    requires
        col_ok::<E, W, I, G>(old(slabs)@, *old(index), *old(total_len), max_segments),
        *old(total_len) + 3 <= usize::MAX,
        normal(sps@, *old(total_len) as int, dst_len as int),
        old(feed).inv(),
        old(feed).src().len() == dst_len,
        old(feed).at() == 0,
    ensures
        col_ok::<E, W, I, G>(final(slabs)@, *final(index), *final(total_len), max_segments),
        *final(total_len) + 3 <= usize::MAX,
        vals_of::<E>(final(slabs)@) == applied(old(feed).src(), vals_of::<E>(old(slabs)@), sps@),
{
    let ghost src = vals_of::<E>(slabs@);
    proof {
        lemma_vals_total::<E>(slabs@);
    }
    cut_gaps::<E, W, I, G, F, R::X>(slabs, index, total_len, max_segments, counter, sps, f, Ghost(None));
    put_gaps::<E, W, I, G, F, R>(slabs, index, total_len, max_segments, counter, sps, dst_len, f, feed, Ghost(src));
}

/// A column's items, a range at a time, as the [`RangeFeed`] of its own
/// edit model: an iterator core over its slabs, moved to each range with
/// its index, each run's value written as its owned form
/// (`ReadsAgree::ins`).
pub(crate) struct SrcFeed<'a, E: Slabs, W, I: IndexOps<W>, X: Decoders<'a, Tail = E::Tail>> {
    pub(crate) slabs: &'a [Slab<E::Tail>],
    pub(crate) index: &'a I,
    pub(crate) core: ICore<X::D>,
    /// where the range shifted to ends
    pub(crate) end: usize,
    pub(crate) _p: PhantomData<(E, W)>,
}

impl<'a, E, W, I: IndexOps<W>, X> RangeFeed<E> for SrcFeed<'a, E, W, I, X> where
    E: ReadsAgree<'a, X>,
    X: Decoders<'a, Tail = E::Tail>,
 {
    type X = <E as ReadsAgree<'a, X>>::Ins;

    /// A reader over the column's slabs, its window inside them, and
    /// their index.
    open spec fn inv(&self) -> bool {
        &&& self.core.wf::<X>(self.slabs@)
        &&& self.core.pos <= self.end <= iter_vals_of::<X>(self.slabs@).len()
        &&& self.core.out::<X>(self.slabs@) == iter_vals_of::<X>(self.slabs@).subrange(self.core.pos as int, self.end as int)
        &&& all_ok::<E>(self.slabs@)
        &&& self.index.ok()
        &&& wlens::<W, I>(self.index.ws()) == slab_lens(self.slabs@)
    }

    open spec fn src(&self) -> Seq<E::V> {
        vals_of::<E>(self.slabs@)
    }

    open spec fn at(&self) -> int {
        self.core.pos as int
    }

    open spec fn end(&self) -> int {
        self.end as int
    }

    #[inline(always)]
    fn shift(&mut self, range: Range<usize>) {
        hide(ICore::wf);
        hide(ICore::out);
        hide(all_ok);
        hide(vals_of);
        hide(iter_vals_of);
        hide(sum);
        let ghost s = self.slabs@;
        let ghost v = iter_vals_of::<X>(s);
        proof {
            lemma_vals_own::<E, X>(s);
            assert(v.len() == vals_of::<E>(s).len());
        }
        self.core.set_max::<X>(self.slabs, range.end);
        proof {
            assert(self.core.out::<X>(s) =~= v.subrange(self.core.pos as int, range.end as int)) by {
                reveal(ICore::out);
            }
        }
        if range.start > self.core.pos {
            let index = self.index;
            let slabs = self.slabs;
            let find = |t: usize| -> (r: (usize, usize))
                requires
                    index.ok(),
                    wlens::<W, I>(index.ws()) == slab_lens(slabs@),
                ensures
                    find_ok(slabs@, t, r),
                {
                    let r = index.ix_find(t);
                    proof {
                        assert(index.ws().map_values(|w: W| I::wlen(w)) =~= wlens::<W, I>(index.ws()));
                    }
                    // past the slabs, the whole position lies beyond them
                    if r.0 >= slabs.len() {
                        (slabs.len(), 0)
                    } else {
                        r
                    }
                };
            let start_of = |i: usize| -> (n: usize)
                requires
                    index.ok(),
                    wlens::<W, I>(index.ws()) == slab_lens(slabs@),
                    i <= slabs@.len(),
                ensures
                    n == sum(slab_lens(slabs@), 0, i as int),
                {
                    let n = index.ix_before(i);
                    proof {
                        assert(index.ws().map_values(|w: W| I::wlen(w)) =~= wlens::<W, I>(index.ws()));
                    }
                    n
                };
            let ghost p0 = self.core.pos as int;
            let n = range.start - self.core.pos - 1;
            let _ = self.core.nth::<X, _, _>(slabs, n, &find, &start_of);
            proof {
                assert(v.subrange(p0, range.end as int).skip(n + 1) =~= v.subrange(range.start as int, range.end as int));
            }
        }
        self.end = range.end;
    }

    #[inline(always)]
    fn next_run(&mut self) -> (r: Option<(Self::X, usize)>) {
        let ghost s = self.slabs@;
        let ghost v = iter_vals_of::<X>(s);
        let ghost (p0, e) = (self.core.pos as int, self.end as int);
        proof {
            lemma_vals_own::<E, X>(s);
        }
        match self.core.next_run_max::<X>(self.slabs, usize::MAX) {
            Some(run) => {
                let x = <E as ReadsAgree<'a, X>>::ins(run.value);
                proof {
                    let n = run.count as int;
                    let o = v.subrange(p0, e);
                    assert(o.skip(n) =~= v.subrange(p0 + n, e));
                    let f = |w: <X::D as Decode>::Item| E::own(w);
                    assert(vals_of::<E>(s).subrange(p0, p0 + n) =~= o.take(n).map_values(f));
                    assert(o.take(n).map_values(f) =~= Seq::new(n as nat, |_i: int| x.val()));
                }
                Some((x, run.count))
            },
            None => {
                // nothing left in the range: the reader's window is empty
                self.core.set_max::<X>(self.slabs, self.end);
                proof {
                    assert(self.core.out::<X>(s) =~= v.subrange(p0, e));
                }
                None
            },
        }
    }
}

} // verus!
