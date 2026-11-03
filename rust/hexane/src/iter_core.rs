//! The column iterator's core ([`crate::Iter`]), verified with Verus.
//!
//! An iterator reads a window of a column: it stands at `pos`, with
//! `items_left` more to give.  The column stands for its slabs' values
//! one after another (`vals_of`).  The iterator's invariant (`wf`) ties
//! its slab, the decoder it holds on that slab and its counters to `pos`,
//! and each operation is proved to give exactly the values left in the
//! window (`out`), in order — past the end of the column there are none.
//!
//! The slab decoders are the encodings' own, through each encoding's
//! decoder model (`Decode`, `Decoders`, named by
//! `ColumnEncoding::ReadModel`), which is verified against its format:
//! RLE columns' (`rle::dec::RleReads`) and bool columns'
//! (`bool_wire::BoolReads`).  The index lookup a long seek uses is given
//! as a closure whose answers are assumed right (`find_ok`; the index
//! itself is verified, see [`crate::index::IndexOps`]).
//!
//! The constructors also take it as given that there are fewer than
//! `usize::MAX` slabs. A slab is not zero-sized, so no slice of them is
//! that long, but Verus does not model that; it lets the step to the next
//! slab test `slab_idx + 1`, which compiles to a tighter loop than the
//! equivalent subtraction.

use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::{lemma_concat_append, slab_lens};
#[cfg(verus_keep_ghost)]
use crate::fenwick::{lemma_sum_le, lemma_sum_one, lemma_sum_split, sum, total};
#[cfg(verus_keep_ghost)]
use crate::rle::writer::concat;
use crate::Run;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

// ── The decoders ───────────────────────────────────────────────────────────

/// A slab decoder, specified against the values it has left.
pub(crate) trait Decode: Sized {
    type Item: Copy;

    /// What the decoder keeps while reading.
    spec fn inv(&self) -> bool;

    /// The values still to come.
    spec fn rest(&self) -> Seq<Self::Item>;

    fn d_next(&mut self) -> (r: Option<Self::Item>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            match r {
                Some(v) => old(self).rest().len() > 0 && v == old(self).rest()[0] && final(self).rest()
                    == old(self).rest().skip(1),
                None => old(self).rest().len() == 0 && final(self).rest() == old(self).rest(),
            },
    ;

    fn d_nth(&mut self, n: usize) -> (r: Option<Self::Item>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            n < old(self).rest().len() ==> r == Some(old(self).rest()[n as int]) && final(self).rest()
                == old(self).rest().skip(n + 1),
            n >= old(self).rest().len() ==> r is None && final(self).rest().len() == 0,
    ;

    /// The next run of equal values, at most `max` of them.
    fn d_run_max(&mut self, max: usize) -> (r: Option<Run<Self::Item>>)
        requires
            old(self).inv(),
            max >= 1,
        ensures
            final(self).inv(),
            match r {
                Some(run) => {
                    &&& 1 <= run.count <= max
                    &&& run.count <= old(self).rest().len()
                    &&& old(self).rest().take(run.count as int) == Seq::new(
                        run.count as nat,
                        |_i: int| run.value,
                    )
                    &&& final(self).rest() == old(self).rest().skip(run.count as int)
                },
                None => old(self).rest().len() == 0 && final(self).rest() == old(self).rest(),
            },
    ;

    fn d_eq(a: &Self::Item, b: &Self::Item) -> (r: bool)
        ensures
            r == (*a == *b),
    ;
}

/// Decoders on a column's slabs, which live for `'a`.
pub(crate) trait Decoders<'a> {
    type Tail: Copy + Clone + std::fmt::Debug + Default;

    type D: Decode;

    /// A slab is well-formed.
    spec fn ok(s: Slab<Self::Tail>) -> bool;

    /// A slab's values.
    spec fn vals(s: Slab<Self::Tail>) -> Seq<<Self::D as Decode>::Item>;

    fn open(s: &'a Slab<Self::Tail>) -> (d: Self::D)
        requires
            Self::ok(*s),
        ensures
            d.inv(),
            d.rest() == Self::vals(*s),
    ;

    /// A decoder with nothing to give.
    fn none() -> (d: Self::D)
        ensures
            d.inv(),
            d.rest().len() == 0,
    ;
}

// ── The column model ───────────────────────────────────────────────────────

/// Each slab's values.
pub(crate) open spec fn each<'a, X: Decoders<'a>>(s: Seq<Slab<X::Tail>>) -> Seq<
    Seq<<X::D as Decode>::Item>,
> {
    s.map_values(|x: Slab<X::Tail>| X::vals(x))
}

/// The slabs' values, one after another.
pub(crate) open spec fn vals_of<'a, X: Decoders<'a>>(s: Seq<Slab<X::Tail>>) -> Seq<
    <X::D as Decode>::Item,
> {
    concat(each::<X>(s))
}

/// Each slab is well-formed and holds as many values as it says.
pub(crate) open spec fn lens_ok<'a, X: Decoders<'a>>(s: Seq<Slab<X::Tail>>) -> bool {
    forall|i: int|
        #![trigger X::vals(s[i])]
        #![trigger X::ok(s[i])]
        0 <= i < s.len() ==> X::ok(s[i]) && X::vals(s[i]).len() == s[i].len
}

/// The index's answer `r` for item `t`: the slab holding it and the
/// item's offset there, or past the end, a slab count of at least the
/// column's and a position at or past its end.
pub(crate) open spec fn find_ok<T: Copy + Clone + std::fmt::Debug + Default>(
    s: Seq<Slab<T>>,
    t: usize,
    r: (usize, usize),
) -> bool {
    &&& r.0 <= s.len()
    &&& r.0 < s.len() ==> sum(slab_lens(s), 0, r.0 as int) + r.1 == t && r.1 < s[r.0 as int].len
    &&& r.0 == s.len() ==> r.1 <= t && t - r.1 >= total(slab_lens(s))
}

proof fn lemma_concat_at<V>(cs: Seq<Seq<V>>, k: int)
    requires
        0 <= k < cs.len(),
    ensures
        concat(cs) == concat(cs.take(k)) + cs[k] + concat(cs.skip(k + 1)),
{
    assert(cs =~= cs.take(k) + (seq![cs[k]] + cs.skip(k + 1)));
    lemma_concat_append(cs.take(k), seq![cs[k]] + cs.skip(k + 1));
    lemma_concat_append(seq![cs[k]], cs.skip(k + 1));
    let e = Seq::<Seq<V>>::empty();
    assert(seq![cs[k]].drop_last() =~= e);
    assert(concat(e) == Seq::<V>::empty());
    assert(concat(seq![cs[k]]) =~= cs[k]);
    assert(concat(cs) =~= concat(cs.take(k)) + cs[k] + concat(cs.skip(k + 1)));
}

proof fn lemma_concat_len<V>(cs: Seq<Seq<V>>, k: int)
    requires
        0 <= k <= cs.len(),
    ensures
        concat(cs.take(k)).len() == sum(cs.map_values(|c: Seq<V>| c.len()), 0, k),
    decreases k,
{
    if k > 0 {
        lemma_concat_len(cs, k - 1);
        assert(cs.take(k).drop_last() =~= cs.take(k - 1));
    } else {
        assert(cs.take(0) =~= Seq::<Seq<V>>::empty());
    }
}

/// From slab `k` at offset `j` on, the column holds the rest of slab `k`
/// and the slabs after it, and the values before number the items before.
proof fn lemma_from<'a, X: Decoders<'a>>(s: Seq<Slab<X::Tail>>, k: int, j: int)
    requires
        lens_ok::<X>(s),
        0 <= k < s.len(),
        0 <= j <= s[k].len,
    ensures
        vals_of::<X>(s).len() == total(slab_lens(s)),
        vals_of::<X>(s).skip(sum(slab_lens(s), 0, k) + j) == X::vals(s[k]).skip(j) + vals_of::<X>(
            s.skip(k + 1),
        ),
        sum(slab_lens(s), 0, k) + s[k].len <= total(slab_lens(s)),
{
    let cs = each::<X>(s);
    assert(cs.map_values(|c: Seq<<X::D as Decode>::Item>| c.len()) =~= slab_lens(s));
    lemma_concat_at(cs, k);
    lemma_concat_len(cs, k);
    lemma_concat_len(cs, cs.len() as int);
    assert(cs.take(cs.len() as int) =~= cs);
    assert(cs.skip(k + 1) =~= each::<X>(s.skip(k + 1)));
    let v = X::vals(s[k]);
    let pre = concat(cs.take(k));
    let post = vals_of::<X>(s.skip(k + 1));
    assert((pre + v + post).skip(pre.len() + j) =~= v.skip(j) + post);
    lemma_sum_split(slab_lens(s), 0, k, k + 1);
    lemma_sum_one(slab_lens(s), k);
    lemma_sum_le(slab_lens(s), 0, k + 1, 0, s.len() as int);
}

// ── The iterator ───────────────────────────────────────────────────────────

/// The iterator's state; [`crate::Iter`] pairs it with the slabs.
///
/// The field order is fixed because it changes the code LLVM makes for
/// the inlined loops: with the two counters a run takes from side by side
/// they update as a pair, and `pos` apart keeps `next` from pairing it
/// with `slab_remaining` (a fifth slower on bools).
#[repr(C)]
pub(crate) struct ICore<D> {
    pub(crate) items_left: usize,
    pub(crate) slab_remaining: usize,
    pub(crate) decoder: D,
    pub(crate) slab_idx: usize,
    pub(crate) pos: usize,
}

impl<D: Decode> ICore<D> {
    /// The iterator stands at `pos` of the slabs `s`: in slab `slab_idx`,
    /// `slab_remaining` from its end, with a decoder there — or past the
    /// last slab, at or past the column's end.  (Fewer than `usize::MAX`
    /// slabs, so stepping past one cannot overflow; see the module docs.)
    pub(crate) open spec fn wf<'a, X: Decoders<'a, D = D>>(&self, s: Seq<Slab<X::Tail>>) -> bool {
        let si = self.slab_idx as int;
        &&& lens_ok::<X>(s)
        &&& total(slab_lens(s)) <= usize::MAX
        &&& s.len() < usize::MAX
        &&& self.pos + self.items_left <= usize::MAX
        &&& self.decoder.inv()
        &&& si <= s.len()
        &&& si < s.len() ==> {
            &&& self.slab_remaining <= s[si].len
            &&& self.pos == sum(slab_lens(s), 0, si) + (s[si].len - self.slab_remaining)
            &&& self.decoder.rest() == X::vals(s[si]).skip(s[si].len - self.slab_remaining)
        }
        &&& si == s.len() ==> {
            &&& self.slab_remaining == 0
            &&& self.decoder.rest().len() == 0
            &&& self.pos >= total(slab_lens(s))
        }
    }

    /// The values the iterator has left to give.
    pub(crate) open spec fn out<'a, X: Decoders<'a, D = D>>(&self, s: Seq<Slab<X::Tail>>) -> Seq<
        D::Item,
    > {
        let v = vals_of::<X>(s);
        if self.pos >= v.len() {
            Seq::empty()
        } else if self.pos + self.items_left <= v.len() {
            v.subrange(self.pos as int, self.pos + self.items_left)
        } else {
            v.subrange(self.pos as int, v.len() as int)
        }
    }

    /// Standing in a slab, the column from `pos` on is the decoder's
    /// values, then the slabs after.
    proof fn lemma_here<'a, X: Decoders<'a, D = D>>(&self, s: Seq<Slab<X::Tail>>)
        requires
            self.wf::<X>(s),
        ensures
            vals_of::<X>(s).len() == total(slab_lens(s)),
            self.slab_idx < s.len() ==> {
                &&& self.decoder.rest().len() == self.slab_remaining
                &&& vals_of::<X>(s).skip(self.pos as int) == self.decoder.rest() + vals_of::<X>(
                    s.skip(self.slab_idx + 1),
                )
                &&& self.pos + self.slab_remaining <= total(slab_lens(s))
            },
    {
        if self.slab_idx < s.len() {
            let si = self.slab_idx as int;
            lemma_from::<X>(s, si, s[si].len - self.slab_remaining);
        } else {
            if s.len() > 0 {
                lemma_from::<X>(s, 0, 0);
            } else {
                assert(each::<X>(s) =~= Seq::<Seq<D::Item>>::empty());
                assert(slab_lens(s) =~= Seq::<nat>::empty());
            }
        }
    }

    /// An iterator with nothing in hand, standing at `pos` at or past the
    /// end.
    pub(crate) fn idle<'a, X: Decoders<'a, D = D>>(slabs: &'a [Slab<X::Tail>], pos: usize) -> (it: Self)
        requires
            lens_ok::<X>(slabs@),
            slabs@.len() < usize::MAX,
            total(slab_lens(slabs@)) <= usize::MAX,
            pos >= total(slab_lens(slabs@)),
        ensures
            it.wf::<X>(slabs@),
            it.out::<X>(slabs@) == Seq::<D::Item>::empty(),
            it.pos == pos,
    {
        let it = ICore { slab_idx: slabs.len(), decoder: X::none(), items_left: 0, slab_remaining: 0, pos };
        proof {
            it.lemma_here::<X>(slabs@);
        }
        it
    }

    /// An iterator at item `at` of slab `si`, with `n` to give.
    pub(crate) fn at<'a, X: Decoders<'a, D = D>>(
        slabs: &'a [Slab<X::Tail>],
        si: usize,
        at: usize,
        pos: usize,
        n: usize,
    ) -> (it: Self)
        requires
            lens_ok::<X>(slabs@),
            slabs@.len() < usize::MAX,
            total(slab_lens(slabs@)) <= usize::MAX,
            si < slabs@.len(),
            at <= slabs@[si as int].len,
            pos == sum(slab_lens(slabs@), 0, si as int) + at,
            pos + n <= usize::MAX,
        ensures
            it.wf::<X>(slabs@),
            it.pos == pos,
            it.items_left == n,
            it.slab_idx == si,
    {
        let slab = &slabs[si];
        let mut decoder = X::open(slab);
        if at > 0 {
            decoder.d_nth(at - 1);
        }
        proof {
            let v = X::vals(slabs@[si as int]);
            if at > 0 {
                assert(v.skip(at - 1 + 1) =~= v.skip(at as int));
            } else {
                assert(v.skip(0) =~= v);
            }
        }
        ICore { slab_idx: si, decoder, items_left: n, slab_remaining: slab.len - at, pos }
    }

    /// Yield the next value of the window.
    #[inline(always)]
    pub(crate) fn next<'a, X: Decoders<'a, D = D>>(&mut self, slabs: &'a [Slab<X::Tail>]) -> (r: Option<D::Item>)
        requires
            old(self).wf::<X>(slabs@),
        ensures
            final(self).wf::<X>(slabs@),
            match r {
                Some(v) => {
                    &&& old(self).out::<X>(slabs@).len() > 0
                    &&& v == old(self).out::<X>(slabs@)[0]
                    &&& final(self).out::<X>(slabs@) == old(self).out::<X>(slabs@).skip(1)
                    &&& final(self).pos == old(self).pos + 1
                },
                None => old(self).out::<X>(slabs@).len() == 0 && final(self).out::<X>(slabs@).len() == 0,
            },
    {
        let ghost c0 = *self;
        proof {
            self.lemma_here::<X>(slabs@);
        }
        if self.items_left == 0 {
            return None;
        }
        loop
            invariant
                c0 == *old(self),
                self.wf::<X>(slabs@),
                self.items_left == c0.items_left,
                self.items_left > 0,
                self.pos == c0.pos,
                self.out::<X>(slabs@) == c0.out::<X>(slabs@),
            decreases slabs@.len() - self.slab_idx,
        {
            proof {
                self.lemma_here::<X>(slabs@);
            }
            let ghost c1 = *self;
            if let Some(v) = self.decoder.d_next() {
                self.items_left -= 1;
                self.slab_remaining -= 1;
                self.pos += 1;
                proof {
                    let si = self.slab_idx as int;
                    let s = slabs@;
                    let rest = X::vals(s[si]).skip(s[si].len - c1.slab_remaining);
                    assert(rest.skip(1) =~= X::vals(s[si]).skip(s[si].len - self.slab_remaining));
                    let vv = vals_of::<X>(s);
                    assert(vv[c1.pos as int] == vv.skip(c1.pos as int)[0]);
                    assert(self.out::<X>(s) =~= c1.out::<X>(s).skip(1));
                }
                return Some(v);
            }
            if self.slab_idx + 1 >= slabs.len() {
                self.slab_idx = slabs.len();
                self.items_left = 0;
                proof {
                    let s = slabs@;
                    if c1.slab_idx < s.len() {
                        assert(c1.slab_remaining == 0);
                        assert(s.skip(c1.slab_idx + 1) =~= Seq::<Slab<X::Tail>>::empty());
                        assert(each::<X>(s.skip(c1.slab_idx + 1)) =~= Seq::<Seq<D::Item>>::empty());
                    }
                    assert(c1.out::<X>(s).len() == 0);
                }
                return None;
            }
            proof {
                let s = slabs@;
                let si = self.slab_idx as int;
                assert(self.slab_remaining == 0);
                lemma_sum_split(slab_lens(s), 0, si, si + 1);
                lemma_sum_one(slab_lens(s), si);
            }
            self.slab_idx += 1;
            self.slab_remaining = slabs[self.slab_idx].len;
            self.decoder = X::open(&slabs[self.slab_idx]);
            proof {
                let s = slabs@;
                let si = self.slab_idx as int;
                assert(X::vals(s[si]).skip(0) =~= X::vals(s[si]));
            }
        }
    }

    /// Jump to the start of slab `si`, item `start` of the column; past the
    /// last slab, stop at `start`.  Returns whether there is a slab.
    pub(crate) fn advance_to_slab<'a, X: Decoders<'a, D = D>>(&mut self, slabs: &'a [Slab<X::Tail>], si: usize, start: usize) -> (r: bool)
        requires
            old(self).wf::<X>(slabs@),
            si <= slabs@.len(),
            si < slabs@.len() ==> {
                &&& start == sum(slab_lens(slabs@), 0, si as int)
                &&& old(self).pos <= start
                &&& start - old(self).pos <= old(self).items_left
            },
            si == slabs@.len() ==> start >= total(slab_lens(slabs@)),
        ensures
            final(self).wf::<X>(slabs@),
            r == (si < slabs@.len()),
            final(self).pos == start,
            final(self).slab_idx == si,
            r ==> {
                &&& final(self).slab_remaining == slabs@[si as int].len
                &&& final(self).items_left == old(self).items_left - (start - old(self).pos)
            },
            !r ==> final(self).items_left == 0,
    {
        if si >= slabs.len() {
            self.slab_idx = si;
            self.pos = start;
            self.items_left = 0;
            // else a later re-extend decodes items already passed
            self.slab_remaining = 0;
            self.decoder = X::none();
            return false;
        }
        let skipped = start - self.pos;
        self.pos = start;
        self.slab_idx = si;
        self.slab_remaining = slabs[si].len;
        self.decoder = X::open(&slabs[si]);
        self.items_left -= skipped;
        proof {
            assert(X::vals(slabs@[si as int]).skip(0) =~= X::vals(slabs@[si as int]));
        }
        true
    }

    /// `nth`, for `n` inside the window.
    fn nth_in<'a, X: Decoders<'a, D = D>, F: Fn(usize) -> (usize, usize)>(
        &mut self,
        slabs: &'a [Slab<X::Tail>],
        n: usize,
        find: &F,
    ) -> (r: Option<D::Item>)
        requires
            old(self).wf::<X>(slabs@),
            n < old(self).items_left,
            forall|t: usize| #[trigger] find.requires((t,)),
            forall|t: usize, x: (usize, usize)| #[trigger] find.ensures((t,), x) ==> find_ok(slabs@, t, x),
        ensures
            final(self).wf::<X>(slabs@),
            n < old(self).out::<X>(slabs@).len() ==> {
                &&& r == Some(old(self).out::<X>(slabs@)[n as int])
                &&& final(self).out::<X>(slabs@) == old(self).out::<X>(slabs@).skip(n + 1)
                &&& final(self).pos == old(self).pos + n + 1
            },
            n >= old(self).out::<X>(slabs@).len() ==> r is None && final(self).out::<X>(slabs@).len() == 0,
    {
        let ghost c0 = *self;
        let ghost s = slabs@;
        let ghost vv = vals_of::<X>(s);
        proof {
            self.lemma_here::<X>(s);
        }
        // Fast path: target is within the current slab.
        if n < self.slab_remaining {
            self.items_left -= n + 1;
            self.slab_remaining -= n + 1;
            self.pos += n + 1;
            let r = self.decoder.d_nth(n);
            proof {
                let si = self.slab_idx as int;
                let rest0 = c0.decoder.rest();
                assert(rest0.skip(n + 1) =~= X::vals(s[si]).skip(s[si].len - self.slab_remaining));
                assert(vv.skip(c0.pos as int)[n as int] == vv[c0.pos + n]);
                assert(self.out::<X>(s) =~= c0.out::<X>(s).skip(n + 1));
            }
            return r;
        }
        // Use the column's index for an O(log S) slab lookup.
        let target = self.pos + n;
        let (si, off) = find(target);
        proof {
            let lens = slab_lens(s);
            if si < s.len() {
                // the slab holding `target` starts at or after the cursor
                if c0.slab_idx < s.len() {
                    let cur = c0.slab_idx as int;
                    lemma_sum_split(lens, 0, cur, cur + 1);
                    lemma_sum_one(lens, cur);
                    if (si as int) <= cur {
                        lemma_sum_le(lens, 0, si + 1, 0, cur + 1);
                        lemma_sum_split(lens, 0, si as int, si + 1);
                        lemma_sum_one(lens, si as int);
                    } else {
                        lemma_sum_le(lens, 0, cur + 1, 0, si as int);
                    }
                } else {
                    lemma_from::<X>(s, si as int, 0);
                }
            }
        }
        if !self.advance_to_slab::<X>(slabs, si, target - off) {
            proof {
                assert(c0.out::<X>(s).len() <= n) by {
                    assert(target >= total(slab_lens(s)));
                }
            }
            return None;
        }
        let ghost c1 = *self;
        self.slab_remaining -= off + 1;
        self.items_left -= off + 1;
        self.pos += off + 1;
        let r = self.decoder.d_nth(off);
        proof {
            let k = si as int;
            lemma_from::<X>(s, k, 0);
            lemma_from::<X>(s, k, off + 1);
            let v = X::vals(s[k]);
            assert(v.skip(0) =~= v);
            assert(c1.decoder.rest() =~= v);
            assert(v.skip(off + 1) =~= X::vals(s[k]).skip(s[k].len - self.slab_remaining));
            assert(vv.skip(sum(slab_lens(s), 0, k) as int)[off as int] == vv[target as int]);
            assert(vv.skip(c0.pos as int)[n as int] == vv[target as int]);
            assert(self.out::<X>(s) =~= c0.out::<X>(s).skip(n + 1));
        }
        r
    }

    /// The `n`th value left in the window (0-based), consuming it and those
    /// before it; past the window, consume the window.
    pub(crate) fn nth<'a, X: Decoders<'a, D = D>, F: Fn(usize) -> (usize, usize)>(
        &mut self,
        slabs: &'a [Slab<X::Tail>],
        n: usize,
        find: &F,
    ) -> (r: Option<D::Item>)
        requires
            old(self).wf::<X>(slabs@),
            forall|t: usize| #[trigger] find.requires((t,)),
            forall|t: usize, x: (usize, usize)| #[trigger] find.ensures((t,), x) ==> find_ok(slabs@, t, x),
        ensures
            final(self).wf::<X>(slabs@),
            n < old(self).out::<X>(slabs@).len() ==> {
                &&& r == Some(old(self).out::<X>(slabs@)[n as int])
                &&& final(self).out::<X>(slabs@) == old(self).out::<X>(slabs@).skip(n + 1)
                &&& final(self).pos == old(self).pos + n + 1
            },
            n >= old(self).out::<X>(slabs@).len() ==> r is None && final(self).out::<X>(slabs@).len() == 0,
    {
        if n >= self.items_left {
            proof {
                assert(self.out::<X>(slabs@).len() <= self.items_left);
            }
            if self.items_left > 0 {
                let k = self.items_left - 1;
                let ghost c0 = *self;
                self.nth_in::<X, F>(slabs, k, find);
                proof {
                    if k < c0.out::<X>(slabs@).len() {
                        assert(c0.out::<X>(slabs@).skip(k + 1).len() == 0);
                    }
                }
            }
            return None;
        }
        self.nth_in::<X, F>(slabs, n, find)
    }

    /// The next run of equal values in the window, at most `max`, within
    /// one slab: what a `fold` passes on.
    #[inline(always)]
    pub(crate) fn run_once<'a, X: Decoders<'a, D = D>>(&mut self, slabs: &'a [Slab<X::Tail>], max: usize) -> (r: Option<Run<D::Item>>)
        requires
            old(self).wf::<X>(slabs@),
            max >= 1,
            old(self).items_left > 0,
        ensures
            final(self).wf::<X>(slabs@),
            match r {
                Some(run) => {
                    let o = old(self).out::<X>(slabs@);
                    &&& 1 <= run.count <= max
                    &&& run.count <= o.len()
                    &&& o.take(run.count as int) == Seq::new(run.count as nat, |_i: int| run.value)
                    &&& final(self).out::<X>(slabs@) == o.skip(run.count as int)
                    &&& final(self).pos == old(self).pos + run.count
                    &&& final(self).items_left == old(self).items_left - run.count
                },
                None => old(self).out::<X>(slabs@).len() == 0 && final(self).out::<X>(slabs@).len() == 0,
            },
    {
        let ghost c0 = *self;
        let ghost s = slabs@;
        loop
            invariant
                c0 == *old(self),
                s == slabs@,
                self.wf::<X>(s),
                self.pos == c0.pos,
                self.items_left == c0.items_left,
                self.items_left > 0,
                max >= 1,
                self.out::<X>(s) == c0.out::<X>(s),
            decreases s.len() - self.slab_idx,
        {
            proof {
                self.lemma_here::<X>(s);
            }
            let cap = if self.items_left < self.slab_remaining {
                self.items_left
            } else {
                self.slab_remaining
            };
            let cap = if cap < max {
                cap
            } else {
                max
            };
            if cap > 0 {
                let ghost d0 = self.decoder;
                if let Some(run) = self.decoder.d_run_max(cap) {
                    let count = run.count;
                    self.items_left -= count;
                    self.slab_remaining -= count;
                    self.pos += count;
                    proof {
                        let si = self.slab_idx as int;
                        let vv = vals_of::<X>(s);
                        let rest = d0.rest();
                        assert(rest.skip(count as int) =~= X::vals(s[si]).skip(s[si].len - self.slab_remaining));
                        let o = c0.out::<X>(s);
                        assert(o.take(count as int) =~= rest.take(count as int)) by {
                            assert(vv.skip(c0.pos as int) == rest + vals_of::<X>(s.skip(si + 1)));
                            assert forall|i: int| 0 <= i < count implies o.take(count as int)[i] == rest.take(count as int)[i] by {
                                assert(o[i] == vv[c0.pos + i]);
                                assert(vv[c0.pos + i] == vv.skip(c0.pos as int)[i]);
                            }
                        }
                        assert(self.out::<X>(s) =~= o.skip(count as int));
                    }
                    return Some(run);
                }
            }
            if self.slab_idx + 1 >= slabs.len() {
                proof {
                    if self.slab_idx < s.len() {
                        assert(self.slab_remaining == 0);
                        assert(s.skip(self.slab_idx + 1) =~= Seq::<Slab<X::Tail>>::empty());
                        assert(each::<X>(s.skip(self.slab_idx + 1)) =~= Seq::<Seq<D::Item>>::empty());
                    }
                }
                self.slab_idx = slabs.len();
                self.items_left = 0;
                return None;
            }
            proof {
                let si = self.slab_idx as int;
                assert(self.slab_remaining == 0);
                lemma_sum_split(slab_lens(s), 0, si, si + 1);
                lemma_sum_one(slab_lens(s), si);
            }
            self.slab_idx += 1;
            self.slab_remaining = slabs[self.slab_idx].len;
            self.decoder = X::open(&slabs[self.slab_idx]);
            proof {
                assert(X::vals(s[self.slab_idx as int]).skip(0) =~= X::vals(s[self.slab_idx as int]));
            }
        }
    }

    /// The next run of equal values in the window, at most `max`, merged
    /// across slab boundaries.
    #[allow(clippy::question_mark)] // `?` puts the merge loop's proof over its rlimit
    #[verifier::rlimit(30)]
    #[verifier::spinoff_prover]
    pub(crate) fn next_run_max<'a, X: Decoders<'a, D = D>>(&mut self, slabs: &'a [Slab<X::Tail>], max: usize) -> (r: Option<Run<D::Item>>)
        requires
            old(self).wf::<X>(slabs@),
        ensures
            final(self).wf::<X>(slabs@),
            match r {
                Some(run) => {
                    let o = old(self).out::<X>(slabs@);
                    &&& 1 <= run.count <= max
                    &&& run.count <= o.len()
                    &&& o.take(run.count as int) == Seq::new(run.count as nat, |_i: int| run.value)
                    &&& final(self).out::<X>(slabs@) == o.skip(run.count as int)
                    &&& final(self).pos == old(self).pos + run.count
                },
                None => max == 0 || old(self).out::<X>(slabs@).len() == 0,
            },
    {
        let ghost c0 = *self;
        let ghost s = slabs@;
        if max == 0 || self.items_left == 0 {
            proof {
                assert(self.out::<X>(s).len() <= self.items_left);
            }
            return None;
        }
        let first = match self.run_once::<X>(slabs, max) {
            Some(r) => r,
            None => return None,
        };
        let ghost max0 = max;
        let value = first.value;
        let mut total_count = first.count;
        let mut max = max - first.count;
        let ghost o0 = c0.out::<X>(s);
        while self.slab_remaining == 0 && self.items_left > 0
            invariant
                s == slabs@,
                self.wf::<X>(s),
                1 <= total_count <= o0.len(),
                o0 == c0.out::<X>(s),
                o0.take(total_count as int) == Seq::new(total_count as nat, |_i: int| value),
                self.out::<X>(s) == o0.skip(total_count as int),
                self.pos == c0.pos + total_count,
                total_count + max == max0,
            decreases s.len() - self.slab_idx,
        {
            if self.slab_idx + 1 >= slabs.len() {
                break;
            }
            proof {
                self.lemma_here::<X>(s);
                let si = self.slab_idx as int;
                lemma_sum_split(slab_lens(s), 0, si, si + 1);
                lemma_sum_one(slab_lens(s), si);
            }
            self.slab_idx += 1;
            self.slab_remaining = slabs[self.slab_idx].len;
            self.decoder = X::open(&slabs[self.slab_idx]);
            proof {
                assert(X::vals(s[self.slab_idx as int]).skip(0) =~= X::vals(s[self.slab_idx as int]));
                self.lemma_here::<X>(s);
            }
            let cap = if self.items_left < self.slab_remaining {
                self.items_left
            } else {
                self.slab_remaining
            };
            let cap = if cap < max {
                cap
            } else {
                max
            };
            if cap == 0 {
                break;
            }
            let ghost d0 = self.decoder;
            let ghost c1 = *self;
            if let Some(next) = self.decoder.d_run_max(cap) {
                if D::d_eq(&next.value, &value) {
                    let c = next.count;
                    total_count += c;
                    max -= c;
                    self.items_left -= c;
                    self.slab_remaining -= c;
                    self.pos += c;
                    proof {
                        let si = self.slab_idx as int;
                        let vv = vals_of::<X>(s);
                        let rest = d0.rest();
                        assert(rest.skip(c as int) =~= X::vals(s[si]).skip(s[si].len - self.slab_remaining));
                        let o1 = c1.out::<X>(s);
                        assert(o1.take(c as int) =~= rest.take(c as int)) by {
                            assert(vv.skip(c1.pos as int) == rest + vals_of::<X>(s.skip(si + 1)));
                            assert forall|i: int| 0 <= i < c implies o1.take(c as int)[i] == rest.take(c as int)[i] by {
                                assert(o1[i] == vv[c1.pos + i]);
                                assert(vv[c1.pos + i] == vv.skip(c1.pos as int)[i]);
                            }
                        }
                        assert(self.out::<X>(s) =~= o1.skip(c as int));
                        assert(o0.take(total_count as int) =~= Seq::new(total_count as nat, |_i: int| value)) by {
                            assert forall|i: int| 0 <= i < total_count implies o0.take(total_count as int)[i] == value by {
                                if i < total_count - c {
                                    assert(o0.take(total_count - c)[i] == value);
                                } else {
                                    assert(o1[i - (total_count - c)] == o0[i]);
                                    assert(o1.take(c as int)[i - (total_count - c)] == value);
                                }
                            }
                        }
                        assert(self.out::<X>(s) =~= o0.skip(total_count as int));
                    }
                    continue;
                } else {
                    // value doesn't match — reset the decoder so the run
                    // it consumed can be re-read by later calls
                    self.decoder = X::open(&slabs[self.slab_idx]);
                    proof {
                        assert(X::vals(s[self.slab_idx as int]).skip(0) =~= X::vals(s[self.slab_idx as int]));
                    }
                }
            }
            break;
        }
        Some(Run { count: total_count, value })
    }

    /// Set the window to end at `end`.
    #[inline]
    pub(crate) fn set_max<'a, X: Decoders<'a, D = D>>(&mut self, Ghost(slabs): Ghost<&'a [Slab<X::Tail>]>, end: usize)
        requires
            old(self).wf::<X>(slabs@),
        ensures
            final(self).wf::<X>(slabs@),
            final(self).pos == old(self).pos,
            final(self).items_left == if end >= old(self).pos { end - old(self).pos } else { 0 },
            final(self).slab_idx == old(self).slab_idx,
    {
        self.items_left = end.saturating_sub(self.pos);
    }
}

} // verus!
