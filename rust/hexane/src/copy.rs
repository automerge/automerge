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
//!   cut a [`splice_feed`]), then puts the destination's items back
//!   between the ranges kept with one cursor (`splices::Gaps`, the feed
//!   now over the destination). The column becomes the same
//!   `applied(dst, src, sps)`.
//!
//! Both keep the column invariant, and both end: every loop has a
//! measure, the feed's included.
//!
//! Trusted: the [`RangeFeed`] contract of the feed over a column's runs
//! (`Ranged`, at the bottom of this file: that `Shiftable::shift` and the
//! run iterator give exactly the source's items in the range), as well
//! as what the cursor trusts.

#![allow(clippy::too_many_arguments)]

use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::{col_ok, lemma_vals_total, vals_of};
use crate::cursor::{splice_feed, Cursor, Insertable, RunFeed, Slabs, Visitor, Weigh, WeightSink};
use crate::index::IndexOps;
#[cfg(verus_keep_ghost)]
use crate::splices::{applied, copied, cut, end_of, kept, normal, produced, ranges_in, removed};
use crate::splices::{Cuts, Gaps};
use crate::Splice;
use std::ops::Range;
use vstd::prelude::*;

verus! {

/// A source's items, a range at a time: shifted to a range, it gives the
/// source's items in it, in runs, then `None`. It moves only forwards.
pub(crate) trait RangeFeed<E: Slabs> {
    type X: Insertable<E>;

    /// The source's items.
    spec fn src(&self) -> Seq<E::V>;

    /// Where the next item comes from, and where the range ends.
    spec fn at(&self) -> int;

    spec fn end(&self) -> int;

    fn shift(&mut self, range: Range<usize>)
        requires
            old(self).at() <= range.start <= range.end <= old(self).src().len(),
        ensures
            final(self).src() == old(self).src(),
            final(self).at() == range.start,
            final(self).end() == range.end,
    ;

    fn next_run(&mut self) -> (r: Option<(Self::X, usize)>)
        ensures
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
        0 <= old(feed).at() <= old(feed).end() <= old(feed).src().len(),
    ensures
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
            feed.src() == src,
            feed.end() == e0,
            0 <= a0 <= feed.at() <= e0 <= src.len(),
            cur.p@ == p0 + src.subrange(a0, feed.at()),
        ensures
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

/// `copy_ranges`, directly: one cursor over the destination applies the
/// normalized splices `sps` in turn, each range's items from `feed`. The
/// column becomes `applied(dst, src, sps)`.
#[inline(always)]
pub(crate) fn copy_direct<E: Slabs, W, I: IndexOps<W>, G: Weigh<E::Tail, W>, F: Visitor<E>, S: WeightSink<W>, R: RangeFeed<E>>(
    slabs: &mut Vec<Slab<E::Tail>>,
    index: &mut I,
    total_len: &mut usize,
    max_segments: usize,
    counter: &mut usize,
    sps: &[Splice],
    f: &mut F,
    g: &mut S,
    feed: &mut R,
)
    requires
        col_ok::<E, W, I, G>(old(slabs)@, *old(index), *old(total_len), max_segments),
        *old(total_len) + 3 <= usize::MAX,
        normal(sps@, old(feed).src().len() as int, *old(total_len) as int),
        old(feed).at() == 0,
    ensures
        col_ok::<E, W, I, G>(final(slabs)@, *final(index), *final(total_len), max_segments),
        vals_of::<E>(final(slabs)@) == applied(vals_of::<E>(old(slabs)@), old(feed).src(), sps@),
{
    let ghost dst = vals_of::<E>(slabs@);
    let ghost src = feed.src();
    proof {
        lemma_vals_total::<E>(slabs@);
        assert(sps@.take(0) =~= Seq::<Splice>::empty());
        assert(dst.take(0) =~= Seq::<E::V>::empty());
        assert(produced(dst, src, Seq::<Splice>::empty()) =~= Seq::<E::V>::empty());
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
            feed.src() == src,
            k == 0 ==> feed.at() == 0,
            k > 0 ==> feed.at() == sps@[k - 1].range.end,
        decreases sps@.len() - k,
    {
        let pos = sps[k].pos;
        let del = sps[k].delete;
        let start = sps[k].range.start;
        let end = sps[k].range.end;
        let ghost (sp, p0, orig0) = (sps@[k as int], cur.p@, cur.orig as int);
        proof {
            assert(sps@.take(k + 1).drop_last() =~= sps@.take(k as int));
            assert(sps@.take(k + 1).last() == sp);
            assert(sp.pos + sp.delete <= dst.len());
            if k > 0 {
                assert(sps@.take(k as int).last() == sps@[k - 1]);
                assert(sps@[k - 1].pos + sps@[k - 1].delete <= sp.pos);
                assert(sps@[k - 1].range.end <= sp.range.start);
            }
            assert(sp.range.start <= sp.range.end <= src.len());
        }
        let to = pos - cur.orig;
        cur.advance_with::<W, I, G, F>(slabs, index, total_len, max_segments, counter, to, f);
        cur.delete_with::<W, I, G, F, S>(slabs, index, total_len, max_segments, counter, del, f, g);
        feed.shift(start..end);
        insert_range(&mut cur, slabs, max_segments, feed);
        proof {
            assert(copied(src, sp) =~= src.subrange(start as int, end as int));
            assert(cur.p@ =~= produced(dst, src, sps@.take(k + 1)));
        }
        k = k + 1;
    }
    cur.close::<W, I, G>(slabs, index, total_len, max_segments, counter);
    proof {
        cur.lemma_clean(slabs@);
        assert(sps@.take(sps@.len() as int) =~= sps@);
        assert(dst.skip(cur.orig as int) =~= dst.subrange(end_of(sps@), dst.len() as int));
    }
}

/// Cut the gaps between `sps`' ranges out of the column, back to front:
/// it keeps exactly the ranges' items.
#[inline(always)]
fn cut_gaps<E: Slabs, W, I: IndexOps<W>, G: Weigh<E::Tail, W>, F: Visitor<E>, S: WeightSink<W>, X: Insertable<E>>(
    slabs: &mut Vec<Slab<E::Tail>>,
    index: &mut I,
    total_len: &mut usize,
    max_segments: usize,
    counter: &mut usize,
    sps: &[Splice],
    f: &mut F,
    g: &mut S,
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
                let _ins = splice_feed::<E, W, I, G, F, S, NoRuns<X>>(
                    slabs,
                    index,
                    total_len,
                    max_segments,
                    counter,
                    at,
                    n,
                    f,
                    g,
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
#[verifier::rlimit(40)]
#[inline(always)]
fn put_gaps<E: Slabs, W, I: IndexOps<W>, G: Weigh<E::Tail, W>, F: Visitor<E>, R: RangeFeed<E>>(
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
        old(feed).src().len() == dst_len,
        old(feed).at() == 0,
    ensures
        col_ok::<E, W, I, G>(final(slabs)@, *final(index), *final(total_len), max_segments),
        vals_of::<E>(final(slabs)@) == applied(old(feed).src(), src, sps@),
{
    hide(Gaps::inv);
    let ghost dst = feed.src();
    let ghost k = kept(src, sps@);
    proof {
        lemma_vals_total::<E>(slabs@);
    }
    let mut gaps = Gaps::<E::V>::new(sps, dst_len, Ghost(dst), Ghost(src));
    let mut cur = Cursor::<E>::new::<W, I, G>(slabs, index, *total_len, Ghost(max_segments), 0);
    proof {
        assert(k.take(0) =~= Seq::<E::V>::empty());
        assert(produced(k, dst, Seq::<Splice>::empty()) =~= Seq::<E::V>::empty());
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
                    assert(gs.drop_last() =~= gaps0);
                    assert(gs[gs.len() - 1] == sp);
                    assert(sp.pos + sp.delete <= k.len());
                    if gaps0.len() > 0 {
                        assert(gs[gs.len() - 2] == gaps0.last());
                        assert(gs[gs.len() - 2].pos + gs[gs.len() - 2].delete <= sp.pos);
                        assert(gs[gs.len() - 2].range.end <= sp.range.start);
                    }
                    assert(sp.range.start <= sp.range.end <= dst.len());
                }
                let to = at - cur.orig;
                cur.advance_with::<W, I, G, F>(slabs, index, total_len, max_segments, counter, to, f);
                feed.shift(gap);
                insert_range(&mut cur, slabs, max_segments, feed);
                proof {
                    assert(copied(dst, sp) =~= dst.subrange(start as int, end as int));
                    assert(cur.p@ =~= produced(k, dst, gs));
                }
            },
        }
    }
    cur.close::<W, I, G>(slabs, index, total_len, max_segments, counter);
    proof {
        cur.lemma_clean(slabs@);
        assert(k.skip(cur.orig as int) =~= k.subrange(end_of(gaps.gaps@), k.len() as int));
    }
}

/// `copy_ranges`, inverted: the column holds the source; cut the gaps
/// between `sps`' ranges out of it, then put the destination's items
/// (from `feed`) back between the ranges kept. The column becomes
/// `applied(dst, src, sps)`, as [`copy_direct`] makes it of the
/// destination.
#[inline(always)]
pub(crate) fn copy_invert<E: Slabs, W, I: IndexOps<W>, G: Weigh<E::Tail, W>, F: Visitor<E>, S: WeightSink<W>, R: RangeFeed<E>>(
    slabs: &mut Vec<Slab<E::Tail>>,
    index: &mut I,
    total_len: &mut usize,
    max_segments: usize,
    counter: &mut usize,
    sps: &[Splice],
    dst_len: usize,
    f: &mut F,
    g: &mut S,
    feed: &mut R,
)
    requires
        col_ok::<E, W, I, G>(old(slabs)@, *old(index), *old(total_len), max_segments),
        *old(total_len) + 3 <= usize::MAX,
        normal(sps@, *old(total_len) as int, dst_len as int),
        old(feed).src().len() == dst_len,
        old(feed).at() == 0,
    ensures
        col_ok::<E, W, I, G>(final(slabs)@, *final(index), *final(total_len), max_segments),
        vals_of::<E>(final(slabs)@) == applied(old(feed).src(), vals_of::<E>(old(slabs)@), sps@),
{
    let ghost src = vals_of::<E>(slabs@);
    proof {
        lemma_vals_total::<E>(slabs@);
    }
    cut_gaps::<E, W, I, G, F, S, R::X>(slabs, index, total_len, max_segments, counter, sps, f, g, Ghost(None));
    put_gaps::<E, W, I, G, F, R>(slabs, index, total_len, max_segments, counter, sps, dst_len, f, feed, Ghost(src));
}

} // verus!

// ── The source feed ─────────────────────────────────────────────────────────
//
// Outside `verus!`, so Verus takes it on trust; its spec items exist only
// when Verus compiles the crate. A column's run iterator, shifted to each
// range in turn, hands each run to the rebuild as an `Inserting`.

use crate::cursor::Inserting;
use crate::edit::SlabEdit;
use crate::encoding::ColumnEncoding;
use crate::shift::Shiftable;
use crate::{AsColumnRef, Run};
use std::marker::PhantomData;

/// A column's runs, a range at a time, as the [`RangeFeed`] of encoding
/// `E`'s slab model: [`Shiftable::shift`] to the range, then its runs.
pub(crate) struct Ranged<E, It>(It, PhantomData<fn() -> E>);

impl<E, It> Ranged<E, It> {
    #[inline(always)]
    pub(crate) fn new(it: It) -> Self {
        Ranged(it, PhantomData)
    }
}

impl<
        E: SlabEdit,
        V: AsColumnRef<<E as ColumnEncoding>::Value>,
        It: Iterator<Item = Run<V>> + Shiftable,
    > RangeFeed<<E as SlabEdit>::Model> for Ranged<E, It>
{
    type X = Inserting<E, V>;

    #[cfg(verus_keep_ghost)]
    fn src(&self) -> Seq<<<E as SlabEdit>::Model as Slabs>::V> {
        unimplemented!()
    }

    #[cfg(verus_keep_ghost)]
    fn at(&self) -> int {
        unimplemented!()
    }

    #[cfg(verus_keep_ghost)]
    fn end(&self) -> int {
        unimplemented!()
    }

    #[inline(always)]
    fn shift(&mut self, range: Range<usize>) {
        self.0.shift(range)
    }

    #[inline(always)]
    fn next_run(&mut self) -> Option<(Inserting<E, V>, usize)> {
        self.0.next().map(|r| (Inserting::new(r.value), r.count))
    }
}
