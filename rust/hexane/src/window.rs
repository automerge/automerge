//! The repositioning iterators' contract and the adapters built on it
//! ([`crate::shift`]), verified with Verus.
//!
//! An iterator, as the adapters see it (`Stream`), gives items that each
//! stand for one or more values (`spans`) of a fixed sequence (`vals`),
//! one value per position: from its position `at`, the values it has
//! `left`, in order. A column iterator's item stands for its value (it is
//! `unit`), a run adapter's for its run. A [`Shiftable`](crate::Shiftable)
//! iterator (`Window`) gives the values of a window, from `at` up to
//! `end` and no further than the values go (`iter_core::window`), and
//! can move it: `shift` moves only forwards.
//!
//! Proved here, against that contract: `Shiftable`'s provided methods
//! (`advance_to`, `advance_by`, `shift`, `shift_next`, `seek_to`), from
//! the methods every implementation gives (`get_pos`, `get_max`,
//! `set_max`, `next`, `nth`), for unit iterators (`shift` for any);
//! `Ranges`, over any iterator, run adapters included, which gives what
//! stands for the values of each range in turn; and `Unshift`, which
//! gives its lookahead and then what its inner iterator has left, and,
//! over a unit iterator (`ItemPerValue`), after `shift(range)` exactly
//! the values in `range`.
//!
//! Trusted: that every iterator keeps the contract (`Stream`, `Window`;
//! implemented in `shift` for every `Iterator` and every `Shiftable`),
//! and is unit where it is `ItemPerValue` (what `Unshift::shift`
//! requires) or a `Shiftable` that keeps the provided moves. The crate's
//! own iterators move by their verified cores (`ICore::shift` and the
//! prefix and delta versions), whose specifications are the contract's,
//! and the run adapters by theirs (`ICore::shift_run`,
//! `diter::d_shift_run`); the run adapters are not `ItemPerValue`, so an
//! `Unshift` over one cannot be shifted.

#[cfg(verus_keep_ghost)]
use crate::cursor::lemma_concat_append;
#[cfg(verus_keep_ghost)]
use crate::iter_core::{lemma_window_past, past, window};
#[cfg(verus_keep_ghost)]
use crate::rle::writer::concat;
use std::ops::Range;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// An iterator: it gives the items `items()`, each standing for one or
/// more values at consecutive positions (`spans`: a column iterator's
/// item for its value, a run adapter's for its run), so that what it has
/// left, value by value (`left()`), is the values `vals()` it ranges over
/// from its position `at()` on. It is `unit()` when each item stands for
/// itself alone. Trusted, for every `Iterator` (`shift`): any iterator
/// whose `nth(n)` gives what `n + 1` calls to `next` would give the last
/// of, and which gives nothing once it has given nothing, has such a
/// model.
pub(crate) trait Stream {
    type Item;

    /// The values the iterator ranges over, one per position.
    spec fn vals(&self) -> Seq<Self::Item>;

    /// The position of the value it gives next.
    spec fn at(&self) -> int;

    /// The values it has left to give.
    spec fn left(&self) -> Seq<Self::Item>;

    /// The items it has left to give.
    spec fn items(&self) -> Seq<Self::Item>;

    /// The values item `x` stands for.
    spec fn spans(x: Self::Item) -> Seq<Self::Item>;

    /// Each item stands for itself alone.
    spec fn unit() -> bool;

    /// What it has left is the values from its position on.
    proof fn lemma_left(&self)
        ensures
            0 <= self.at(),
            self.left().len() > 0 ==> self.at() + self.left().len() <= self.vals().len(),
            forall|k: int| 0 <= k < self.left().len() ==> #[trigger] self.left()[k] == self.vals()[self.at() + k],
    ;

    /// What it has left, value by value, is what its items stand for, and
    /// each stands for some.
    proof fn lemma_items(&self)
        ensures
            self.left() == concat(self.items().map_values(|x: Self::Item| Self::spans(x))),
            forall|k: int| 0 <= k < self.items().len() ==> Self::spans(#[trigger] self.items()[k]).len() > 0,
    ;

    /// A unit iterator's item stands for itself.
    proof fn lemma_unit(x: Self::Item)
        requires
            Self::unit(),
        ensures
            Self::spans(x) == seq![x],
    ;

    /// `Iterator::next`.
    fn s_next(&mut self) -> (r: Option<Self::Item>)
        ensures
            final(self).vals() == old(self).vals(),
            match r {
                Some(x) => {
                    &&& old(self).items().len() > 0
                    &&& x == old(self).items()[0]
                    &&& final(self).items() == old(self).items().skip(1)
                    &&& final(self).at() == old(self).at() + Self::spans(x).len()
                },
                None => old(self).items().len() == 0 && final(self).items().len() == 0,
            },
    ;

    /// `Iterator::nth`.
    fn s_nth(&mut self, n: usize) -> (r: Option<Self::Item>)
        ensures
            final(self).vals() == old(self).vals(),
            n < old(self).items().len() ==> {
                &&& r == Some(old(self).items()[n as int])
                &&& final(self).items() == old(self).items().skip(n + 1)
                &&& final(self).at() == old(self).at() + concat(
                    old(self).items().take(n + 1).map_values(|x: Self::Item| Self::spans(x)),
                ).len()
            },
            n >= old(self).items().len() ==> r is None && final(self).items().len() == 0,
    ;
}

/// The values items `xs` stand for, one after another.
pub(crate) open spec fn flat<S: Stream + ?Sized>(xs: Seq<S::Item>) -> Seq<S::Item> {
    concat(xs.map_values(|x: S::Item| S::spans(x)))
}

/// A unit iterator's items are its values.
pub(crate) proof fn lemma_flat_unit<S: Stream + ?Sized>(xs: Seq<S::Item>)
    requires
        S::unit(),
    ensures
        flat::<S>(xs) == xs,
    decreases xs.len(),
{
    if xs.len() > 0 {
        lemma_flat_unit::<S>(xs.drop_last());
        S::lemma_unit(xs.last());
        assert(xs.map_values(|x: S::Item| S::spans(x)).drop_last() =~= xs.drop_last().map_values(
            |x: S::Item| S::spans(x),
        ));
        assert(xs.drop_last() + seq![xs.last()] =~= xs);
    } else {
        assert(xs.map_values(|x: S::Item| S::spans(x)) =~= Seq::<Seq<S::Item>>::empty());
    }
}

/// The first item's values come first.
pub(crate) proof fn lemma_flat_first<S: Stream + ?Sized>(xs: Seq<S::Item>)
    requires
        xs.len() > 0,
    ensures
        flat::<S>(xs) == S::spans(xs[0]) + flat::<S>(xs.skip(1)),
{
    let m = xs.map_values(|x: S::Item| S::spans(x));
    let one = seq![S::spans(xs[0])];
    assert(m =~= one + xs.skip(1).map_values(|x: S::Item| S::spans(x)));
    lemma_concat_append(one, xs.skip(1).map_values(|x: S::Item| S::spans(x)));
    assert(one.drop_last() =~= Seq::<Seq<S::Item>>::empty());
    assert(concat(Seq::<Seq<S::Item>>::empty()) == Seq::<S::Item>::empty());
    assert(concat(one) == concat(one.drop_last()) + one.last());
    assert(concat(one) =~= S::spans(xs[0]));
}

/// A unit iterator's items are the values it has left.
pub(crate) proof fn lemma_unit_state<S: Stream + ?Sized>(s: &S)
    requires
        S::unit(),
    ensures
        s.left() == s.items(),
{
    s.lemma_items();
    lemma_flat_unit::<S>(s.items());
}

/// Moving `n + 1` items on a unit iterator moves `n + 1` values on.
pub(crate) proof fn lemma_unit_take<S: Stream + ?Sized>(xs: Seq<S::Item>, n: int)
    requires
        S::unit(),
        0 <= n <= xs.len(),
    ensures
        flat::<S>(xs.take(n)).len() == n,
{
    lemma_flat_unit::<S>(xs.take(n));
}

/// A `Shiftable` iterator: what it has left is the window `at()..end()`
/// of its values, and it moves the window. Trusted, for every
/// `Shiftable` (`shift`); its provided methods are proved below.
pub(crate) trait Window: Stream {
    /// The end of the window.
    spec fn end(&self) -> int;

    /// What it has left is the window's values.
    proof fn lemma_window(&self)
        ensures
            self.left() == window(self.vals(), self.at(), self.end()),
    ;

    /// `Shiftable::get_pos`.
    fn w_pos(&self) -> (r: usize)
        ensures
            r == self.at(),
    ;

    /// `Shiftable::set_max`: the window ends at `end`, or where the
    /// iterator stands if that is past `end`.
    fn w_set_max(&mut self, end: usize)
        ensures
            final(self).vals() == old(self).vals(),
            final(self).at() == old(self).at(),
            final(self).end() == if end >= old(self).at() { end as int } else { old(self).at() },
    ;

    /// `Shiftable::advance_to`: advance so that the value at `target`
    /// comes next; at or past it already, stay.
    fn w_advance_to(&mut self, target: usize)
        ensures
            final(self).vals() == old(self).vals(),
            final(self).left() == past(old(self).left(), target - old(self).at()),
            target <= old(self).at() ==> final(self).at() == old(self).at(),
            old(self).at() < target <= old(self).at() + old(self).left().len() ==> final(self).at() == target,
    ;

    /// `Shiftable::shift`: the window moves to `start..end`, only forwards.
    fn w_shift(&mut self, start: usize, end: usize)
        ensures
            final(self).vals() == old(self).vals(),
            final(self).left() == window(
                old(self).vals(),
                if start <= old(self).at() { old(self).at() } else { start as int },
                end as int,
            ),
            start <= old(self).at() ==> final(self).at() == old(self).at(),
            old(self).at() < start <= end && start <= old(self).vals().len() ==> final(self).at() == start,
    ;

    /// `Shiftable::shift_next`: the window moves to `start..end`, at or
    /// after the iterator, and gives its first item.
    fn w_shift_next(&mut self, start: usize, end: usize) -> (r: Option<Self::Item>)
        requires
            start >= old(self).at(),
        ensures
            final(self).vals() == old(self).vals(),
            ({
                let w = window(old(self).vals(), start as int, end as int);
                &&& w.len() > 0 ==> (r matches Some(x) && {
                    let n = Self::spans(x).len() as int;
                    &&& 0 < n <= w.len()
                    &&& Self::spans(x) == w.take(n)
                    &&& final(self).left() == w.skip(n)
                    &&& final(self).at() == start + n
                })
                &&& w.len() == 0 ==> r is None && final(self).left().len() == 0
            }),
    ;
}

// ── Shiftable's provided methods ───────────────────────────────────────────

/// `Shiftable::advance_to`: advance so that the value at `target` comes
/// next; at or past it already, stay. Keeps `Window::w_advance_to`'s
/// contract.
#[inline]
pub(crate) fn advance_to<W: Window + ?Sized>(w: &mut W, target: usize)
    requires
        W::unit(),
    ensures
        final(w).vals() == old(w).vals(),
        final(w).left() == past(old(w).left(), target - old(w).at()),
        target <= old(w).at() ==> final(w).at() == old(w).at(),
        old(w).at() < target <= old(w).at() + old(w).left().len() ==> final(w).at() == target,
{
    proof {
        lemma_unit_state::<W>(w);
    }
    let pos = w.w_pos();
    if target > pos {
        let ghost it = w.items();
        w.s_nth(target - pos - 1);
        proof {
            lemma_unit_state::<W>(w);
            if target - pos - 1 < it.len() {
                lemma_unit_take::<W>(it, target - pos);
            }
        }
    }
}

/// `Shiftable::advance_by`: move `amount` values on.
#[inline]
pub(crate) fn advance_by<W: Window + ?Sized>(w: &mut W, amount: usize)
    requires
        W::unit(),
    ensures
        final(w).vals() == old(w).vals(),
        final(w).left() == past(old(w).left(), amount as int),
        amount <= old(w).left().len() ==> final(w).at() == old(w).at() + amount,
{
    proof {
        lemma_unit_state::<W>(w);
    }
    if amount > 0 {
        let ghost o = w.left();
        w.s_nth(amount - 1);
        proof {
            lemma_unit_state::<W>(w);
            if amount <= o.len() {
                lemma_unit_take::<W>(o, amount as int);
            }
            if amount > o.len() {
                assert(w.left() =~= Seq::<W::Item>::empty());
            }
        }
    }
}

/// `Shiftable::shift`: `set_max(end)`, then `advance_to(start)` (the
/// iterator's own, which may be faster). Keeps `Window::w_shift`'s
/// contract.
#[inline]
pub(crate) fn shift<W: Window + ?Sized>(w: &mut W, start: usize, end: usize)
    ensures
        final(w).vals() == old(w).vals(),
        final(w).left() == window(
            old(w).vals(),
            if start <= old(w).at() { old(w).at() } else { start as int },
            end as int,
        ),
        start <= old(w).at() ==> final(w).at() == old(w).at(),
        old(w).at() < start <= end && start <= old(w).vals().len() ==> final(w).at() == start,
{
    let ghost (v, p) = (w.vals(), w.at());
    w.w_set_max(end);
    proof {
        w.lemma_window();
        w.lemma_left();
        if end < p {
            assert(window(v, p, p) =~= window(v, p, end as int));
        }
        if start > p {
            lemma_window_past(v, p, end as int, start - p);
        }
    }
    w.w_advance_to(start);
}

/// `Shiftable::shift_next`: `set_max(end)`, then the value at `start`.
/// Keeps `Window::w_shift_next`'s contract.
#[inline]
pub(crate) fn shift_next<W: Window + ?Sized>(w: &mut W, start: usize, end: usize) -> (r: Option<W::Item>)
    requires
        W::unit(),
        start >= old(w).at(),
    ensures
        final(w).vals() == old(w).vals(),
        ({
            let x = window(old(w).vals(), start as int, end as int);
            &&& x.len() > 0 ==> {
                &&& r == Some(x[0])
                &&& final(w).left() == x.skip(1)
                &&& final(w).at() == start + 1
            }
            &&& x.len() == 0 ==> r is None && final(w).left().len() == 0
        }),
{
    let ghost (v, p) = (w.vals(), w.at());
    w.w_set_max(end);
    let ghost o1 = w.left();
    proof {
        w.lemma_window();
        w.lemma_left();
        lemma_unit_state::<W>(w);
        if end < p {
            assert(window(v, p, p) =~= window(v, p, end as int));
        }
        lemma_window_past(v, p, end as int, start - p);
    }
    let pos = w.w_pos();
    let r = w.s_nth(start - pos);
    proof {
        lemma_unit_state::<W>(w);
        if start - p < o1.len() {
            lemma_unit_take::<W>(o1, start - p + 1);
        }
        let (x, n) = (window(v, start as int, end as int), start - p);
        if n < o1.len() {
            assert(x =~= o1.skip(n));
            assert(x.skip(1) =~= o1.skip(n + 1));
        }
    }
    r
}

/// `Shiftable::seek_to`: the value at `at`, moving past it; `None`, moving
/// nowhere, if `at` is behind the iterator.
#[inline]
pub(crate) fn seek_to<W: Window + ?Sized>(w: &mut W, at: usize) -> (r: Option<W::Item>)
    requires
        W::unit(),
    ensures
        final(w).vals() == old(w).vals(),
        at < old(w).at() ==> r is None && final(w).left() == old(w).left() && final(w).at() == old(w).at(),
        at >= old(w).at() ==> {
            let (o, k) = (old(w).left(), at - old(w).at());
            &&& k < o.len() ==> r == Some(o[k]) && final(w).left() == o.skip(k + 1) && final(w).at() == at + 1
            &&& k >= o.len() ==> r is None && final(w).left().len() == 0
        },
{
    proof {
        lemma_unit_state::<W>(w);
    }
    let pos = w.w_pos();
    if at >= pos {
        let ghost o = w.left();
        let r = w.s_nth(at - pos);
        proof {
            lemma_unit_state::<W>(w);
            if at - pos < o.len() {
                lemma_unit_take::<W>(o, at - pos + 1);
            }
        }
        r
    } else {
        None
    }
}

/// `Shiftable::ranges`: the window emptied, so that the first `next`
/// takes the first range.
#[inline]
pub(crate) fn empty_window<W: Window + ?Sized>(w: &mut W)
    ensures
        final(w).vals() == old(w).vals(),
        final(w).at() == old(w).at(),
        final(w).left().len() == 0,
{
    let pos = w.w_pos();
    w.w_set_max(pos);
    proof {
        w.lemma_window();
    }
}

// ── Ranges ─────────────────────────────────────────────────────────────────

/// The ranges a caller gives `Shiftable::ranges`: nothing is assumed of
/// them; `given` is the ranges given so far, and `ended` that they ended.
pub(crate) trait RangeList {
    spec fn given(&self) -> Seq<Range<usize>>;

    spec fn ended(&self) -> bool;

    fn next_range(&mut self) -> (r: Option<Range<usize>>)
        ensures
            match r {
                Some(x) => final(self).given() == old(self).given().push(x),
                None => final(self).given() == old(self).given() && final(self).ended(),
            },
    ;
}

/// The values of each range of `rs`, in turn.
pub(crate) open spec fn windows<V>(v: Seq<V>, rs: Seq<Range<usize>>) -> Seq<V> {
    concat(rs.map_values(|r: Range<usize>| window(v, r.start as int, r.end as int)))
}

/// What a `Ranges` gives: what is left of the window it is in, then the
/// values of each range it takes, in turn. A range starting behind the
/// iterator panics (`ranges_out_of_order`).
pub(crate) open spec fn ranges_give<W: Window, L: RangeList>(w: W, l0: L, l: L) -> Seq<W::Item> {
    w.left() + windows(w.vals(), l.given().skip(l0.given().len() as int))
}

/// Panics: `Shiftable::ranges` was given a range starting behind the
/// iterator.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn ranges_out_of_order()
    ensures
        false,
{
    panic!("ranges must be ascending and non-overlapping")
}

/// `Ranges::next`: the next value of the window, or of the next range
/// with any. With what it gives over the call (`ranges_give`, from the
/// ranges it takes): the first, and the rest left.
#[verifier::exec_allows_no_decreases_clause]
pub(crate) fn ranges_next<W: Window, L: RangeList>(w: &mut W, l: &mut L) -> (r: Option<W::Item>)
    ensures
        final(w).vals() == old(w).vals(),
        ({
            let g = ranges_give(*old(w), *old(l), *final(l));
            match r {
                Some(x) => {
                    let n = W::spans(x).len() as int;
                    &&& 0 < n <= g.len()
                    &&& W::spans(x) == g.take(n)
                    &&& final(w).left() == g.skip(n)
                },
                None => g.len() == 0 && final(l).ended() && final(w).left().len() == 0,
            }
        }),
{
    let ghost (w0, l0) = (*w, *l);
    loop
        invariant
            w0 == *old(w),
            l0 == *old(l),
            w.vals() == w0.vals(),
            l0.given().len() <= l.given().len(),
            l.given().subrange(0, l0.given().len() as int) == l0.given(),
            ranges_give(w0, l0, *l) == w.left(),
    {
        let ghost (w1, l1) = (*w, *l);
        proof {
            w1.lemma_items();
        }
        if let Some(x) = w.s_next() {
            proof {
                // the first item stands for the first values left
                w.lemma_items();
                lemma_flat_first::<W>(w1.items());
                assert(w1.items().skip(1) == w.items());
                assert(W::spans(x).len() > 0) by {
                    assert(x == w1.items()[0]);
                }
            }
            return Some(x);
        }
        proof {
            w.lemma_items();
            assert(w1.items().map_values(|x: W::Item| W::spans(x)) =~= Seq::<Seq<W::Item>>::empty());
            assert(w.items().map_values(|x: W::Item| W::spans(x)) =~= Seq::<Seq<W::Item>>::empty());
        }
        let ghost t = l1.given().skip(l0.given().len() as int);
        let r = match l.next_range() {
            Some(r) => r,
            None => {
                proof {
                    assert(l.given().skip(l0.given().len() as int) =~= t);
                }
                return None;
            },
        };
        if r.start < w.w_pos() {
            ranges_out_of_order();
        }
        w.w_shift(r.start, r.end);
        proof {
            let v = w0.vals();
            let t1 = l.given().skip(l0.given().len() as int);
            assert(t1 =~= t.push(r));
            assert(t1.map_values(|r: Range<usize>| window(v, r.start as int, r.end as int)) =~= t.map_values(
                |r: Range<usize>| window(v, r.start as int, r.end as int),
            ) + seq![window(v, r.start as int, r.end as int)]);
            lemma_concat_append(
                t.map_values(|r: Range<usize>| window(v, r.start as int, r.end as int)),
                seq![window(v, r.start as int, r.end as int)],
            );
            assert(concat(seq![window(v, r.start as int, r.end as int)]) =~= window(
                v,
                r.start as int,
                r.end as int,
            )) by {
                let one = seq![window(v, r.start as int, r.end as int)];
                assert(one.drop_last() =~= Seq::<Seq<W::Item>>::empty());
                assert(concat(Seq::<Seq<W::Item>>::empty()) == Seq::<W::Item>::empty());
                assert(concat(one) == concat(one.drop_last()) + one.last());
            }
            // nothing was left before the range: all of it is the range's
            assert(w1.left().len() == 0);
            assert(w0.left() + windows(v, t) =~= Seq::<W::Item>::empty());
            assert(ranges_give(w0, l0, *l) =~= (w0.left() + windows(v, t)) + window(
                v,
                r.start as int,
                r.end as int,
            ));
            assert(l.given().subrange(0, l0.given().len() as int) =~= l0.given());
        }
    }
}

// ── Unshift ────────────────────────────────────────────────────────────────

/// What an `Unshift` gives: its lookahead, then the items its inner
/// iterator has left; nothing while hidden.
pub(crate) open spec fn u_left<S: Stream>(inner: S, next: Option<S::Item>, hidden: bool) -> Seq<S::Item> {
    if hidden {
        Seq::empty()
    } else {
        match next {
            Some(x) => seq![x] + inner.items(),
            None => Seq::empty(),
        }
    }
}

/// An `Unshift`'s invariant: its lookahead stands for the values just
/// before its inner iterator's position; with none, the inner iterator
/// has nothing left; and only a lookahead is hidden.
pub(crate) open spec fn u_inv<S: Stream>(inner: S, next: Option<S::Item>, hidden: bool) -> bool {
    &&& next matches Some(x) ==> {
        let n = S::spans(x).len() as int;
        &&& 0 < n <= inner.at() <= inner.vals().len()
        &&& S::spans(x) == inner.vals().subrange(inner.at() - n, inner.at())
    }
    &&& next is None ==> inner.items().len() == 0
    &&& hidden ==> next is Some
}

/// An iterator's first item stands for the values from its position on.
proof fn lemma_first<S: Stream>(s: S)
    requires
        s.items().len() > 0,
    ensures
        ({
            let n = S::spans(s.items()[0]).len() as int;
            &&& 0 < n
            &&& 0 <= s.at()
            &&& s.at() + n <= s.vals().len()
            &&& S::spans(s.items()[0]) == s.vals().subrange(s.at(), s.at() + n)
        }),
{
    s.lemma_items();
    s.lemma_left();
    lemma_flat_first::<S>(s.items());
    let x = s.items()[0];
    assert(S::spans(x) =~= s.vals().subrange(s.at(), s.at() + S::spans(x).len())) by {
        assert forall|k: int| 0 <= k < S::spans(x).len() implies S::spans(x)[k] == s.vals()[s.at() + k] by {
            assert(s.left()[k] == S::spans(x)[k]);
        }
    }
}

/// `Unshift::new`: pull the first item as the lookahead.
pub(crate) fn u_new<S: Stream>(inner: &mut S) -> (next: Option<S::Item>)
    ensures
        final(inner).vals() == old(inner).vals(),
        u_inv(*final(inner), next, false),
        u_left(*final(inner), next, false) == old(inner).items(),
{
    let ghost i0 = *inner;
    let next = inner.s_next();
    proof {
        if next is Some {
            let x = next->Some_0;
            lemma_first::<S>(i0);
            assert(x == i0.items()[0]);
            assert(inner.at() == i0.at() + S::spans(x).len());
            assert(S::spans(x) == inner.vals().subrange(inner.at() - S::spans(x).len(), inner.at()));
            assert(seq![i0.items()[0]] + i0.items().skip(1) =~= i0.items());
        }
    }
    next
}

/// `Unshift::peek`: the item `next` gives, without giving it.
#[allow(unused_variables)] // `inner` is for the spec
pub(crate) fn u_peek<'b, S: Stream>(inner: &S, next: &'b Option<S::Item>, hidden: bool) -> (r: Option<
    &'b S::Item,
>)
    requires
        u_inv(*inner, *next, hidden),
    ensures
        u_left(*inner, *next, hidden).len() > 0 ==> r == Some(&u_left(*inner, *next, hidden)[0]),
        u_left(*inner, *next, hidden).len() == 0 ==> r is None,
{
    if hidden {
        return None;
    }
    next.as_ref()
}

/// `Unshift::next`: the lookahead, pulling the next one.
pub(crate) fn u_next<S: Stream>(inner: &mut S, next: &mut Option<S::Item>, hidden: bool) -> (r: Option<S::Item>)
    requires
        u_inv(*old(inner), *old(next), hidden),
    ensures
        final(inner).vals() == old(inner).vals(),
        u_inv(*final(inner), *final(next), hidden),
        ({
            let (g, g1) = (u_left(*old(inner), *old(next), hidden), u_left(*final(inner), *final(next), hidden));
            match r {
                Some(x) => g.len() > 0 && x == g[0] && g1 == g.skip(1),
                None => g.len() == 0 && g1.len() == 0,
            }
        }),
{
    if hidden {
        return None;
    }
    let ghost i0 = *inner;
    let mut n = inner.s_next();
    std::mem::swap(&mut n, next);
    proof {
        let g = u_left(i0, n, hidden);
        if *next is Some {
            lemma_first::<S>(i0);
            assert(seq![i0.items()[0]] + i0.items().skip(1) =~= i0.items());
        }
        if n is Some {
            assert(g.skip(1) =~= i0.items());
        }
    }
    n
}

/// `Unshift::nth`: the lookahead is item 0, so item `n` is the inner
/// iterator's `n - 1`.
pub(crate) fn u_nth<S: Stream>(inner: &mut S, next: &mut Option<S::Item>, hidden: bool, n: usize) -> (r: Option<
    S::Item,
>)
    requires
        u_inv(*old(inner), *old(next), hidden),
    ensures
        final(inner).vals() == old(inner).vals(),
        u_inv(*final(inner), *final(next), hidden),
        ({
            let (g, g1) = (u_left(*old(inner), *old(next), hidden), u_left(*final(inner), *final(next), hidden));
            &&& n < g.len() ==> r == Some(g[n as int]) && g1 == g.skip(n + 1)
            &&& n >= g.len() ==> r is None && g1.len() == 0
        }),
{
    if n == 0 {
        return u_next(inner, next, hidden);
    }
    if hidden {
        return None;
    }
    let ghost (i0, x0) = (*inner, *next);
    let out = inner.s_nth(n - 1);
    let ghost i1 = *inner;
    *next = inner.s_next();
    proof {
        let g = u_left(i0, x0, hidden);
        if *next is Some {
            lemma_first::<S>(i1);
            assert(seq![i1.items()[0]] + i1.items().skip(1) =~= i1.items());
        }
        if x0 is Some && n - 1 < i0.items().len() {
            assert(g.skip(n + 1) =~= i1.items());
        }
    }
    out
}

/// `Unshift::shift`, over an iterator whose items each stand for one
/// value (`ItemPerValue`): the lookahead becomes the value at `start`,
/// and the window ends at `end`. The lookahead's own value may start the
/// range, and is then kept (hidden while the range is empty, for a later
/// one starting there); a range further back panics
/// (`Shiftable::shift_next`).
pub(crate) fn u_shift<W: Window>(inner: &mut W, next: &mut Option<W::Item>, hidden: &mut bool, start: usize, end: usize)
    requires
        W::unit(),
        u_inv(*old(inner), *old(next), *old(hidden)),
    ensures
        final(inner).vals() == old(inner).vals(),
        u_inv(*final(inner), *final(next), *final(hidden)),
        u_left(*final(inner), *final(next), *final(hidden)) == window(old(inner).vals(), start as int, end as int),
{
    let ghost (i0, v) = (*inner, inner.vals());
    // the lookahead is the value just before the inner's position
    if next.is_some() && start.checked_add(1) == Some(inner.w_pos()) {
        inner.w_set_max(end);
        *hidden = start >= end;
        proof {
            let x = (*next)->Some_0;
            W::lemma_unit(x);
            assert(seq![x][0] == v.subrange(start as int, start + 1)[0]);
            inner.lemma_window();
            lemma_unit_state::<W>(inner);
            if start < end {
                assert(seq![v[start as int]] + window(v, start + 1, end as int) =~= window(v, start as int, end as int));
            } else {
                assert(window(v, start as int, end as int) =~= Seq::<W::Item>::empty());
            }
        }
        return;
    }
    *hidden = false;
    if start < inner.w_pos() {
        shift_backwards();
    }
    *next = inner.w_shift_next(start, end);
    proof {
        lemma_unit_state::<W>(inner);
        let x = window(v, start as int, end as int);
        if *next is Some {
            let y = (*next)->Some_0;
            W::lemma_unit(y);
            // the lookahead is the window's first value, at `start`
            assert(seq![y][0] == x.take(1)[0]);
            assert(x[0] == v[start as int]);
            assert(seq![y] =~= v.subrange(start as int, start + 1));
            assert(seq![x[0]] + x.skip(1) =~= x);
        } else {
            assert(x =~= Seq::<W::Item>::empty());
        }
    }
}

/// Panics: `shift_next` to a range starting behind the iterator.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn shift_backwards()
    ensures
        false,
{
    panic!("assertion failed: range.start >= self.get_pos()")
}

} // verus!
