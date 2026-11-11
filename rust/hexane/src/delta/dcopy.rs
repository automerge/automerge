//! A delta column's `copy_ranges`, verified with Verus.
//!
//! [`DeltaColumn::copy_ranges`](crate::DeltaColumn::copy_ranges)
//! normalizes its splices (`splices::Normalize`) and carries them out one
//! of two ways, as `Column::copy_ranges` does (`copy`), but in the
//! columns' values as their iterators read them (`ddomain::rvals`): each
//! is a chain of the delta edit cursor's steps
//! (`dedit_core::DeltaEditCore`), which re-anchor what they insert, and of
//! delta splices. The source's values come from a [`DSrc`]: shifted to a
//! range, it gives the source's runs in it, each after the running sum
//! before it, which realize the source's values there. Proved:
//!
//! - [`copy_direct`] applies the list to the destination with one
//!   cursor, seeking to each splice, deleting, and inserting the runs the
//!   source gives for the range: the destination's values become
//!   `applied(dst, src, sps)`.
//! - [`copy_invert`] starts from the source's column: it cuts the gaps
//!   between the ranges out of it, back to front (`splices::Cuts`, each
//!   cut a delta splice that inserts nothing), then puts the
//!   destination's values back between the ranges kept with one cursor
//!   (`splices::Gaps`, the runs now the destination's). The values become
//!   the same `applied(dst, src, sps)`.
//!
//! Each ends by checking the domain, and panics where the values would
//! leave it. Trusted: what the cursor and the splices trust.

use crate::btree::{SlabAgg, SlabBTree};
use crate::column::Slab;
use crate::cursor::Visitor;
use crate::delta::agg::DeltaOps;
#[cfg(verus_keep_ghost)]
use crate::delta::agg::{psum, reps};
#[cfg(verus_keep_ghost)]
use crate::delta::dbound::lemma_rz_sub;
#[cfg(verus_keep_ghost)]
use crate::delta::ddomain::{dslabs, item_vals, items_vals, rvals, SpliceItem};
use crate::delta::ddomain::{DeltaWrite, NoItems};
#[cfg(verus_keep_ghost)]
use crate::delta::dedit::{lemma_rz_crop, lemma_rz_nulls, lemma_rz_run, rz};
use crate::delta::dedit_core::{DCol, DeltaEditCore};
#[cfg(verus_keep_ghost)]
use crate::delta::diter::{at_run, crop, dbefore, dindex_ok, lemma_crop};
use crate::delta::diter::{d_next_run, d_nth};
#[cfg(verus_keep_ghost)]
use crate::iter_core::vals_of;
use crate::iter_core::{Decode, Decoders, ICore};
use crate::rle::dec::RleReads;
use crate::rle::edit::RleModel;
use crate::rle::lift::Lift;
use crate::rle::model::{RleOps, WriteOps};
#[cfg(verus_keep_ghost)]
use crate::splices::{applied, copied, cut, end_of, kept, normal, produced, ranges_in, removed};
use crate::splices::{Cuts, Gaps};
use crate::Splice;
use std::marker::PhantomData;
use std::ops::Range;
use vstd::prelude::*;

verus! {

/// A delta column's values, a range at a time, as runs: a reader over
/// its slabs with the running sum before it (as `DeltaIter` keeps it),
/// moved to each range by the column's index.
pub(crate) struct DSrc<'a, X: Decoders<'a>, B> {
    pub(crate) slabs: &'a [Slab<X::Tail>],
    pub(crate) index: &'a SlabBTree<SlabAgg>,
    pub(crate) core: ICore<X::D>,
    pub(crate) running: i64,
    /// where the range shifted to ends
    pub(crate) end: usize,
    pub(crate) _p: PhantomData<B>,
}

/// `n` equal items of a column's deltas from `pos`, after the running sum
/// `prefix` there, realize the run's values: the column's values there.
proof fn lemma_src_run<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    s: Seq<Slab<X::Tail>>,
    pos: int,
    n: usize,
    v: <X::D as Decode>::Item,
    prefix: i64,
)
    requires
        0 <= pos,
        pos + n <= vals_of::<X>(s).len(),
        vals_of::<X>(s).subrange(pos, pos + n) == Seq::new(n as nat, |_i: int| v),
        prefix == crop(psum(dbefore::<X, B>(s, pos))),
    ensures
        rz(0, dslabs::<X, B>(s)).subrange(pos, pos + n) == item_vals(SpliceItem::Run(prefix, B::dv(v), n)),
{
    let vv = vals_of::<X>(s);
    let ds = dslabs::<X, B>(s);
    lemma_rz_sub(ds, pos, pos + n);
    assert(ds.take(pos) =~= dbefore::<X, B>(s, pos));
    let run = reps(n as nat, B::dv(v));
    assert(ds.subrange(pos, pos + n) =~= run) by {
        assert forall|i: int| 0 <= i < n implies ds.subrange(pos, pos + n)[i] == run[i] by {
            assert(vv.subrange(pos, pos + n)[i] == v);
        }
    }
    lemma_crop(prefix as int);
    lemma_rz_crop(psum(ds.take(pos)), prefix as int, run);
    match B::dv(v) {
        Some(d) => lemma_rz_run(prefix as int, d, n as nat),
        None => lemma_rz_nulls(prefix as int, n as nat),
    }
}

impl<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>> DSrc<'a, X, B> {
    /// The reader's invariant, its window the range, and the index's.
    pub(crate) open spec fn inv(&self) -> bool {
        &&& at_run::<X, B>(self.core, self.running, self.slabs@)
        &&& dindex_ok::<X, B>(Some(self.index), self.slabs@)
        &&& self.core.pos <= self.end <= vals_of::<X>(self.slabs@).len()
        &&& self.core.out::<X>(self.slabs@) == vals_of::<X>(self.slabs@).subrange(
            self.core.pos as int,
            self.end as int,
        )
    }

    /// The column's values.
    pub(crate) open spec fn src(&self) -> Seq<Option<i64>> {
        rz(0, dslabs::<X, B>(self.slabs@))
    }

    /// Where the next value comes from, and where the range ends.
    pub(crate) open spec fn at(&self) -> int {
        self.core.pos as int
    }

    pub(crate) open spec fn end(&self) -> int {
        self.end as int
    }

    /// Move to `range`, which lies at or after the reader.
    #[inline(always)]
    pub(crate) fn shift(&mut self, range: Range<usize>)
        requires
            old(self).inv(),
            old(self).at() <= range.start <= range.end <= old(self).src().len(),
        ensures
            final(self).inv(),
            final(self).src() == old(self).src(),
            final(self).at() == range.start,
            final(self).end() == range.end,
    {
        let ghost s = self.slabs@;
        let ghost v = vals_of::<X>(s);
        self.core.set_max::<X>(self.slabs, range.end);
        proof {
            assert(self.core.out::<X>(s) =~= v.subrange(self.core.pos as int, range.end as int));
        }
        if range.start > self.core.pos {
            let ghost p0 = self.core.pos as int;
            let n = range.start - self.core.pos - 1;
            let _ = d_nth::<X, B>(&mut self.core, self.slabs, Some(self.index), n, &mut self.running);
            proof {
                assert(v.subrange(p0, range.end as int).skip(n + 1) =~= v.subrange(
                    range.start as int,
                    range.end as int,
                ));
            }
        }
        self.end = range.end;
    }

    /// The next run of the range: `n` deltas `d` (or nulls) after the
    /// running sum `prefix`, which realize the column's values there.
    #[inline(always)]
    pub(crate) fn next_run(&mut self) -> (r: Option<(i64, Option<i64>, usize)>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).src() == old(self).src(),
            final(self).end() == old(self).end(),
            match r {
                Some((prefix, d, n)) => {
                    &&& n >= 1
                    &&& old(self).at() + n <= old(self).end()
                    &&& final(self).at() == old(self).at() + n
                    &&& old(self).src().subrange(old(self).at(), old(self).at() + n) == item_vals(
                        SpliceItem::Run(prefix, d, n),
                    )
                },
                None => final(self).at() == old(self).at() && old(self).at() >= old(self).end(),
            },
    {
        let ghost s = self.slabs@;
        let ghost v = vals_of::<X>(s);
        let ghost (p0, e) = (self.core.pos as int, self.end as int);
        let prefix = self.running;
        match d_next_run::<X, B>(&mut self.core, self.slabs, usize::MAX, &mut self.running) {
            Some(run) => {
                let d = B::to_opt(run.value);
                proof {
                    let n = run.count as int;
                    let o = v.subrange(p0, e);
                    assert(o.skip(n) =~= v.subrange(p0 + n, e));
                    assert(v.subrange(p0, p0 + n) =~= o.take(n));
                    lemma_src_run::<X, B>(s, p0, run.count, run.value, prefix);
                }
                Some((prefix, d, run.count))
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

/// Insert the runs `feed` gives until its range ends: the values the
/// source holds there.
#[inline(never)]
fn insert_range<'a, L, O, B>(e: &mut DeltaEditCore<L, O, B>, feed: &mut DSrc<'a, RleReads<O>, B>) where
    L: for<'t> Lift<'t, W = L> + for<'t> WriteOps<'t, R = O>,
    O: for<'b> RleOps<'b>,
    B: for<'c> DeltaWrite<'c, RleModel<L>, RleReads<O>>,

    requires
        old(e).inv(),
        old(e).edit.held(),
        old(feed).inv(),
        old(feed).at() <= old(feed).end(),
    ensures
        final(e).inv(),
        final(e).edit.held(),
        final(e).same(*old(e)),
        final(e).at() == old(e).at(),
        final(e).rwritten() == old(e).rwritten() + old(feed).src().subrange(old(feed).at(), old(feed).end()),
        final(feed).inv(),
        final(feed).src() == old(feed).src(),
        final(feed).at() == old(feed).end(),
        final(feed).end() == old(feed).end(),
{
    let ghost (w0, a0, e0, src) = (e.rwritten(), feed.at(), feed.end(), feed.src());
    proof {
        assert(w0 + src.subrange(a0, a0) =~= w0);
    }
    loop
        invariant
            e.inv(),
            e.edit.held(),
            e.same(*old(e)),
            e.at() == old(e).at(),
            w0 == old(e).rwritten(),
            a0 == old(feed).at(),
            e0 == old(feed).end(),
            src == old(feed).src(),
            feed.inv(),
            feed.src() == src,
            feed.end() == e0,
            a0 <= feed.at() <= e0,
            e.rwritten() == w0 + src.subrange(a0, feed.at()),
        ensures
            e.inv(),
            e.edit.held(),
            e.same(*old(e)),
            e.at() == old(e).at(),
            feed.inv(),
            feed.src() == src,
            feed.end() == e0,
            feed.at() == e0,
            e.rwritten() == w0 + src.subrange(a0, e0),
        decreases e0 - feed.at(),
    {
        let ghost a = feed.at();
        match feed.next_run() {
            None => break,
            Some((prefix, d, n)) => {
                match d {
                    None => e.insert_nulls(n),
                    Some(x) => e.insert_run(prefix, x, n),
                }
                proof {
                    assert(w0 + src.subrange(a0, a + n) =~= w0 + src.subrange(a0, a) + src.subrange(a, a + n));
                }
            },
        }
    }
}

/// One splice: carry the values to `pos`, delete `del`, and insert the
/// source's values in `start..end`.
#[inline(always)]
fn apply_one<'a, L, O, B>(
    e: &mut DeltaEditCore<L, O, B>,
    feed: &mut DSrc<'a, RleReads<O>, B>,
    pos: usize,
    del: usize,
    start: usize,
    end: usize,
) where
    L: for<'t> Lift<'t, W = L> + for<'t> WriteOps<'t, R = O>,
    O: for<'b> RleOps<'b>,
    B: for<'c> DeltaWrite<'c, RleModel<L>, RleReads<O>>,

    requires
        old(e).inv(),
        old(e).edit.held(),
        old(e).at() <= pos,
        pos + del <= old(e).edit.orig_vals().len(),
        old(feed).inv(),
        old(feed).at() <= start <= end <= old(feed).src().len(),
    ensures
        final(e).inv(),
        final(e).edit.held(),
        final(e).same(*old(e)),
        final(e).at() == pos + del,
        final(e).rwritten() == old(e).rwritten() + old(e).rorig().subrange(old(e).at(), pos as int) + old(
            feed,
        ).src().subrange(start as int, end as int),
        final(feed).inv(),
        final(feed).src() == old(feed).src(),
        final(feed).at() == end,
{
    let to = pos - e.edit.pos();
    e.advance(to);
    if del > 0 {
        e.delete(del);
    }
    feed.shift(start..end);
    insert_range(e, feed);
}

/// `copy_ranges`, directly: one cursor over the destination `dst`
/// applies the normalized splices `sps` in turn, each range's values
/// from `src`. Its values become `applied(dst, src, sps)`; panics if they
/// would leave `min..=max` or span 2^63 or more.
#[verifier::spinoff_prover]
#[verifier::rlimit(40)]
pub(crate) fn copy_direct<L, O, B>(dst: &mut DCol<L, O, B>, sps: &[Splice], src: &DCol<L, O, B>, min: i64, max: i64) where
    L: for<'t> Lift<'t, W = L> + for<'t> WriteOps<'t, R = O>,
    O: for<'b> RleOps<'b>,
    B: for<'c> DeltaWrite<'c, RleModel<L>, RleReads<O>>,

    requires
        normal(sps@, src@.len() as int, old(dst)@.len() as int),
    ensures
        rvals::<RleModel<L>, RleReads<O>, B>(final(dst)@) == applied(
            rvals::<RleModel<L>, RleReads<O>, B>(old(dst)@),
            rvals::<RleModel<L>, RleReads<O>, B>(src@),
            sps@,
        ),
{
    let ghost rd = rvals::<RleModel<L>, RleReads<O>, B>(dst@);
    let ghost rs = rvals::<RleModel<L>, RleReads<O>, B>(src@);
    let mut feed = src.dsrc();
    let mut e = DeltaEditCore::<L, O, B>::begin(dst, 0);
    proof {
        e.edit.lemma_at();
        assert(e.rwritten() =~= Seq::<Option<i64>>::empty());
        assert(sps@.take(0) =~= Seq::<Splice>::empty());
        assert(produced(rd, rs, Seq::<Splice>::empty()) =~= Seq::<Option<i64>>::empty());
    }
    let mut k: usize = 0;
    while k < sps.len()
        invariant
            e.inv(),
            e.edit.held(),
            e.rorig() == rd,
            e.edit.orig_vals().len() == rd.len(),
            k <= sps@.len(),
            normal(sps@, rs.len() as int, rd.len() as int),
            e.at() == end_of(sps@.take(k as int)),
            e.rwritten() == produced(rd, rs, sps@.take(k as int)),
            feed.inv(),
            feed.src() == rs,
            k == 0 ==> feed.at() == 0,
            k > 0 ==> feed.at() == sps@[k - 1].range.end,
        decreases sps@.len() - k,
    {
        let pos = sps[k].pos;
        let del = sps[k].delete;
        let start = sps[k].range.start;
        let end = sps[k].range.end;
        let ghost sp = sps@[k as int];
        proof {
            assert(sps@.take(k + 1).drop_last() =~= sps@.take(k as int));
            assert(sps@.take(k + 1).last() == sp);
            assert(sp.pos + sp.delete <= rd.len());
            if k > 0 {
                assert(sps@.take(k as int).last() == sps@[k - 1]);
                assert(sps@[k - 1].pos + sps@[k - 1].delete <= sp.pos);
                assert(sps@[k - 1].range.end <= sp.range.start);
            }
            assert(sp.range.start <= sp.range.end <= rs.len());
        }
        apply_one(&mut e, &mut feed, pos, del, start, end);
        proof {
            assert(copied(rs, sp) =~= rs.subrange(start as int, end as int));
            assert(e.rwritten() =~= produced(rd, rs, sps@.take(k + 1)));
        }
        k += 1;
    }
    proof {
        assert(sps@.take(sps@.len() as int) =~= sps@);
        assert(rd.skip(e.at()) =~= rd.subrange(end_of(sps@), rd.len() as int));
    }
    e.end(dst, min, max);
}

/// Cut the gaps between `sps`' ranges out of `col`, back to front, each
/// a delta splice inserting nothing: it keeps exactly the ranges' values.
#[inline(always)]
fn cut_gaps<L, O, B, F: Visitor<RleModel<L>>>(col: &mut DCol<L, O, B>, sps: &[Splice], min: i64, max: i64, f: &mut F) where
    L: for<'t> Lift<'t, W = L> + for<'t> WriteOps<'t, R = O>,
    O: for<'b> RleOps<'b>,
    B: for<'c> DeltaWrite<'c, RleModel<L>, RleReads<O>>,

    requires
        ranges_in(sps@, old(col)@.len() as int),
    ensures
        rvals::<RleModel<L>, RleReads<O>, B>(final(col)@) == kept(rvals::<RleModel<L>, RleReads<O>, B>(old(col)@), sps@),
{
    let ghost src = rvals::<RleModel<L>, RleReads<O>, B>(col@);
    let mut cuts = Cuts::<Option<i64>>::new(sps, col.len(), Ghost(src));
    loop
        invariant
            cuts.inv(),
            cuts.sps@ == sps@,
            cuts.src@ == src,
            src == rvals::<RleModel<L>, RleReads<O>, B>(old(col)@),
            rvals::<RleModel<L>, RleReads<O>, B>(col@) == removed(src, cuts.cuts@),
        ensures
            rvals::<RleModel<L>, RleReads<O>, B>(col@) == kept(src, sps@),
        decreases cuts.left(),
    {
        let ghost cuts0 = cuts.cuts@;
        match cuts.next(sps) {
            None => break,
            Some((at, n)) => {
                let _taken = col.splice_delta::<F, NoItems>(at, n, None, min, max, f);
                proof {
                    let cur = removed(src, cuts0);
                    assert(cuts.cuts@.drop_last() =~= cuts0);
                    assert(_taken@ =~= Seq::<SpliceItem>::empty());
                    assert(items_vals(_taken@) =~= Seq::<Option<i64>>::empty());
                    assert(rvals::<RleModel<L>, RleReads<O>, B>(col@) =~= cut(cur, at as int, n as int));
                }
            },
        }
    }
}

/// Put the destination's values (`dst`'s) back between the ranges `col`
/// kept of the source `src`: its values become `applied(dst, src,
/// sps)`.
#[verifier::spinoff_prover]
#[verifier::rlimit(40)]
#[inline(always)]
fn put_gaps<L, O, B>(
    col: &mut DCol<L, O, B>,
    sps: &[Splice],
    dst: &DCol<L, O, B>,
    min: i64,
    max: i64,
    Ghost(src): Ghost<Seq<Option<i64>>>,
) where
    L: for<'t> Lift<'t, W = L> + for<'t> WriteOps<'t, R = O>,
    O: for<'b> RleOps<'b>,
    B: for<'c> DeltaWrite<'c, RleModel<L>, RleReads<O>>,

    requires
        normal(sps@, src.len() as int, dst@.len() as int),
        rvals::<RleModel<L>, RleReads<O>, B>(old(col)@) == kept(src, sps@),
    ensures
        rvals::<RleModel<L>, RleReads<O>, B>(final(col)@) == applied(rvals::<RleModel<L>, RleReads<O>, B>(dst@), src, sps@),
{
    hide(Gaps::inv);
    let ghost rd = rvals::<RleModel<L>, RleReads<O>, B>(dst@);
    let ghost k = kept(src, sps@);
    let mut feed = dst.dsrc();
    let dst_len = dst.len();
    let mut gaps = Gaps::<Option<i64>>::new(sps, dst_len, Ghost(rd), Ghost(src));
    let mut e = DeltaEditCore::<L, O, B>::begin(col, 0);
    proof {
        e.edit.lemma_at();
        assert(e.rwritten() =~= Seq::<Option<i64>>::empty());
        assert(produced(k, rd, Seq::<Splice>::empty()) =~= Seq::<Option<i64>>::empty());
    }
    loop
        invariant
            e.inv(),
            e.edit.held(),
            e.rorig() == k,
            e.edit.orig_vals().len() == k.len(),
            k == kept(src, sps@),
            gaps.inv(),
            gaps.sps@ == sps@,
            gaps.dst@ == rd,
            gaps.src@ == src,
            e.at() == end_of(gaps.gaps@),
            e.rwritten() == produced(k, rd, gaps.gaps@),
            normal(gaps.gaps@, rd.len() as int, k.len() as int),
            feed.inv(),
            feed.src() == rd,
            gaps.gaps@.len() == 0 ==> feed.at() == 0,
            gaps.gaps@.len() > 0 ==> feed.at() == gaps.gaps@.last().range.end,
        ensures
            e.inv(),
            e.edit.held(),
            e.rorig() == k,
            e.edit.orig_vals().len() == k.len(),
            e.at() == end_of(gaps.gaps@),
            e.rwritten() == produced(k, rd, gaps.gaps@),
            normal(gaps.gaps@, rd.len() as int, k.len() as int),
            applied(k, rd, gaps.gaps@) == applied(rd, src, sps@),
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
                    assert(normal(gs, rd.len() as int, k.len() as int));
                    assert(gs.drop_last() =~= gaps0);
                    assert(gs[gs.len() - 1] == sp);
                    assert(sp.pos + sp.delete <= k.len());
                    if gaps0.len() > 0 {
                        assert(gs[gs.len() - 2] == gaps0.last());
                        assert(gs[gs.len() - 2].pos + gs[gs.len() - 2].delete <= sp.pos);
                        assert(gs[gs.len() - 2].range.end <= sp.range.start);
                    }
                    assert(sp.range.start <= sp.range.end <= rd.len());
                }
                let to = at - e.edit.pos();
                e.advance(to);
                feed.shift(gap);
                insert_range(&mut e, &mut feed);
                proof {
                    assert(copied(rd, sp) =~= rd.subrange(start as int, end as int));
                    assert(e.rwritten() =~= produced(k, rd, gs));
                }
            },
        }
    }
    proof {
        if gaps.gaps@.len() > 0 {
            assert(gaps.gaps@.last().pos + gaps.gaps@.last().delete <= k.len());
        }
        assert(k.skip(e.at()) =~= k.subrange(end_of(gaps.gaps@), k.len() as int));
    }
    e.end(col, min, max);
}

/// `copy_ranges`, inverted: `col` holds the source; cut the gaps between
/// `sps`' ranges out of it, then put the values of the destination `dst`
/// back between the ranges kept. Its values become `applied(dst, src,
/// sps)`, as [`copy_direct`] makes them of the destination; panics if
/// they would leave `min..=max` or span 2^63 or more.
pub(crate) fn copy_invert<L, O, B, F: Visitor<RleModel<L>>>(
    col: &mut DCol<L, O, B>,
    sps: &[Splice],
    dst: &DCol<L, O, B>,
    min: i64,
    max: i64,
    f: &mut F,
) where
    L: for<'t> Lift<'t, W = L> + for<'t> WriteOps<'t, R = O>,
    O: for<'b> RleOps<'b>,
    B: for<'c> DeltaWrite<'c, RleModel<L>, RleReads<O>>,

    requires
        normal(sps@, old(col)@.len() as int, dst@.len() as int),
    ensures
        rvals::<RleModel<L>, RleReads<O>, B>(final(col)@) == applied(
            rvals::<RleModel<L>, RleReads<O>, B>(dst@),
            rvals::<RleModel<L>, RleReads<O>, B>(old(col)@),
            sps@,
        ),
{
    let ghost src = rvals::<RleModel<L>, RleReads<O>, B>(col@);
    cut_gaps(col, sps, min, max, f);
    put_gaps(col, sps, dst, min, max, Ghost(src));
}

} // verus!
