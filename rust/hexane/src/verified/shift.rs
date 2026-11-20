//! Repositioning the verified iterators, as the old API's `Shiftable`
//! repositions its own: the trait [`VShiftable`], and the adapters
//! [`VerifiedUnshift`] (a lookahead) and [`VerifiedRanges`] (several
//! windows, one after another).
//!
//! The adapters are `window.rs`'s, which are proved against its model of
//! an iterator (its traits `Stream` and `Window`). Every `Iterator` has
//! that model by a trusted bridge, for the old API. A verified iterator
//! is given it instead by a view of it that is no iterator
//! (`ShiftView`), whose model is the iterator's `VShiftable`
//! specification, proved of each iterator here: nothing trusted stands
//! between the adapters and the iterator.

#[cfg(verus_keep_ghost)]
use super::{lemma_svs_add, lemma_svs_sub, lemma_svs_window, VGet};
use super::{seal, Get, VCodec, VIndex, VValue, VerifiedIter};
#[cfg(verus_keep_ghost)]
use crate::iter_core::{past, vals_of, window};
#[cfg(verus_keep_ghost)]
use crate::rle::writer::concat;
use crate::window::{
    empty_window, ranges_out_of_order, u_new, u_next, u_nth, u_peek, u_shift, Window,
};
#[cfg(verus_keep_ghost)]
use crate::window::{lemma_flat_first, u_inv, u_left, windows, Stream};
use std::ops::Range;
use vstd::prelude::*;

verus! {

/// A verified iterator that can be repositioned, as the old API's
/// [`Shiftable`](crate::Shiftable) can. Hexane's verified iterators are
/// (and no other type: the trait is sealed).
///
/// What a specification says of one: it ranges over values, one per
/// position (`shift_vals`), as the items it gives; it stands at a
/// position (`shift_at`), and its window ends at `shift_end`; what it
/// has left to give is its items (`shift_items`), each standing for one
/// value or more at consecutive positions (`shift_spans`): an item of a
/// value iterator stands for itself alone (`shift_unit`). What it has
/// left, value by value (`shift_left`), is what its items stand for, one
/// after another, and the values of its window (`lemma_shift`).
///
/// The moves only go forward: a window starting before the iterator
/// starts where it stands.
pub trait VShiftable: Iterator + Sized + seal::ShiftSeal {
    /// The values the iterator ranges over, one per position.
    spec fn shift_vals(&self) -> Seq<Self::Item>;

    /// The position of the value it gives next.
    spec fn shift_at(&self) -> int;

    /// The end of its window.
    spec fn shift_end(&self) -> int;

    /// The values it has left to give.
    spec fn shift_left(&self) -> Seq<Self::Item>;

    /// The items it has left to give.
    spec fn shift_items(&self) -> Seq<Self::Item>;

    /// The values the item `x` stands for.
    spec fn shift_spans(x: Self::Item) -> Seq<Self::Item>;

    /// Each item stands for itself alone.
    spec fn shift_unit() -> bool;

    /// What the iterator has left: the values of its window, which are
    /// what its items stand for, one after another, each some.
    proof fn lemma_shift(&self)
        ensures
            0 <= self.shift_at(),
            self.shift_left() == window(self.shift_vals(), self.shift_at(), self.shift_end()),
            self.shift_left() == concat(self.shift_items().map_values(|x: Self::Item| Self::shift_spans(x))),
            forall|k: int|
                0 <= k < self.shift_items().len() ==> Self::shift_spans(#[trigger] self.shift_items()[k]).len() > 0,
    ;

    /// An item of a unit iterator stands for itself.
    proof fn lemma_shift_unit(x: Self::Item)
        requires
            Self::shift_unit(),
        ensures
            Self::shift_spans(x) == seq![x],
    ;

    /// `Iterator::next`, as the model has it.
    fn shift_step(&mut self) -> (r: Option<Self::Item>)
        ensures
            final(self).shift_vals() == old(self).shift_vals(),
            match r {
                Some(x) => {
                    &&& old(self).shift_items().len() > 0
                    &&& x == old(self).shift_items()[0]
                    &&& final(self).shift_items() == old(self).shift_items().skip(1)
                    &&& final(self).shift_at() == old(self).shift_at() + Self::shift_spans(x).len()
                },
                None => old(self).shift_items().len() == 0 && final(self).shift_items().len() == 0,
            },
    ;

    /// `Iterator::nth`, as the model has it.
    fn shift_nth(&mut self, n: usize) -> (r: Option<Self::Item>)
        ensures
            final(self).shift_vals() == old(self).shift_vals(),
            n < old(self).shift_items().len() ==> {
                &&& r == Some(old(self).shift_items()[n as int])
                &&& final(self).shift_items() == old(self).shift_items().skip(n + 1)
                &&& final(self).shift_at() == old(self).shift_at() + concat(
                    old(self).shift_items().take(n + 1).map_values(|x: Self::Item| Self::shift_spans(x)),
                ).len()
            },
            n >= old(self).shift_items().len() ==> r is None && final(self).shift_items().len() == 0,
    ;

    /// A copy of the iterator, the same as it to a specification.
    fn dup(&self) -> (r: Self)
        ensures
            r.shift_vals() == self.shift_vals(),
            r.shift_at() == self.shift_at(),
            r.shift_end() == self.shift_end(),
            r.shift_left() == self.shift_left(),
            r.shift_items() == self.shift_items(),
    ;

    /// The position of the value it gives next.
    fn get_pos(&self) -> (r: usize)
        ensures
            r == self.shift_at(),
    ;

    /// The end of its window.
    fn get_max(&self) -> (r: usize)
        ensures
            r == self.shift_end(),
    ;

    /// Its window, from where it stands: `get_pos()..get_max()`.
    fn get_range(&self) -> (r: Range<usize>)
        ensures
            r.start == self.shift_at(),
            r.end == self.shift_end(),
    {
        self.get_pos()..self.get_max()
    }

    /// The window ends at `end`, or where the iterator stands if that is
    /// past `end`.
    fn set_max(&mut self, end: usize)
        ensures
            final(self).shift_vals() == old(self).shift_vals(),
            final(self).shift_at() == old(self).shift_at(),
            final(self).shift_end() == if end >= old(self).shift_at() {
                end as int
            } else {
                old(self).shift_at()
            },
    ;

    /// Move on so that the value at `target` comes next; at or past it
    /// already, stay.
    fn advance_to(&mut self, target: usize)
        ensures
            final(self).shift_vals() == old(self).shift_vals(),
            final(self).shift_left() == past(old(self).shift_left(), target - old(self).shift_at()),
            target <= old(self).shift_at() ==> final(self).shift_at() == old(self).shift_at(),
            old(self).shift_at() < target <= old(self).shift_at() + old(self).shift_left().len()
                ==> final(self).shift_at() == target,
    ;

    /// Move `amount` values on, where each item stands for one value.
    fn advance_by(&mut self, amount: usize)
        requires
            Self::shift_unit(),
        ensures
            final(self).shift_vals() == old(self).shift_vals(),
            final(self).shift_left() == past(old(self).shift_left(), amount as int),
            amount <= old(self).shift_left().len() ==> final(self).shift_at() == old(self).shift_at() + amount,
    ;

    /// Move through the value at `at` and give it, where each item stands
    /// for one value; at a place before the iterator, give nothing and
    /// stay.
    fn seek_to(&mut self, at: usize) -> (r: Option<Self::Item>)
        requires
            Self::shift_unit(),
        ensures
            final(self).shift_vals() == old(self).shift_vals(),
            at < old(self).shift_at() ==> {
                &&& r is None
                &&& final(self).shift_left() == old(self).shift_left()
                &&& final(self).shift_at() == old(self).shift_at()
            },
            at >= old(self).shift_at() ==> {
                let (o, k) = (old(self).shift_left(), at - old(self).shift_at());
                &&& k < o.len() ==> {
                    &&& r == Some(o[k])
                    &&& final(self).shift_left() == o.skip(k + 1)
                    &&& final(self).shift_at() == at + 1
                }
                &&& k >= o.len() ==> r is None && final(self).shift_left().len() == 0
            },
    ;

    /// Move the window to `range`, only forwards: it starts where the
    /// iterator stands if `range` starts before that.
    fn shift(&mut self, range: Range<usize>)
        ensures
            final(self).shift_vals() == old(self).shift_vals(),
            final(self).shift_left() == window(
                old(self).shift_vals(),
                if range.start <= old(self).shift_at() {
                    old(self).shift_at()
                } else {
                    range.start as int
                },
                range.end as int,
            ),
            range.start <= old(self).shift_at() ==> final(self).shift_at() == old(self).shift_at(),
            old(self).shift_at() < range.start <= range.end && range.start <= old(self).shift_vals().len()
                ==> final(self).shift_at() == range.start,
    ;

    /// Move the window to `range`, at the iterator or after it, and give
    /// its first item.
    fn shift_next(&mut self, range: Range<usize>) -> (r: Option<Self::Item>)
        requires
            range.start >= old(self).shift_at(),
        ensures
            final(self).shift_vals() == old(self).shift_vals(),
            ({
                let w = window(old(self).shift_vals(), range.start as int, range.end as int);
                &&& w.len() > 0 ==> (r matches Some(x) && {
                    let n = Self::shift_spans(x).len() as int;
                    &&& 0 < n <= w.len()
                    &&& Self::shift_spans(x) == w.take(n)
                    &&& final(self).shift_left() == w.skip(n)
                    &&& final(self).shift_at() == range.start + n
                })
                &&& w.len() == 0 ==> r is None && final(self).shift_left().len() == 0
            }),
    ;
}

/// A verified iterator as `window.rs`'s model sees it: no iterator, so
/// that its model is its `VShiftable` specification, which is proved.
pub(crate) struct ShiftView<I>(pub(crate) I);

impl<I: VShiftable> crate::window::Stream for ShiftView<I> {
    type Item = I::Item;

    open spec fn vals(&self) -> Seq<I::Item> {
        self.0.shift_vals()
    }

    open spec fn at(&self) -> int {
        self.0.shift_at()
    }

    open spec fn left(&self) -> Seq<I::Item> {
        self.0.shift_left()
    }

    open spec fn items(&self) -> Seq<I::Item> {
        self.0.shift_items()
    }

    open spec fn spans(x: I::Item) -> Seq<I::Item> {
        I::shift_spans(x)
    }

    open spec fn unit() -> bool {
        I::shift_unit()
    }

    proof fn lemma_left(&self) {
        self.0.lemma_shift();
    }

    proof fn lemma_items(&self) {
        self.0.lemma_shift();
        let xs = self.0.shift_items();
        assert(xs.map_values(|x: I::Item| Self::spans(x)) =~= xs.map_values(|x: I::Item| I::shift_spans(x)));
    }

    proof fn lemma_unit(x: I::Item) {
        I::lemma_shift_unit(x);
    }

    #[inline(always)]
    fn s_next(&mut self) -> (r: Option<I::Item>) {
        self.0.shift_step()
    }

    #[inline(always)]
    fn s_nth(&mut self, n: usize) -> (r: Option<I::Item>) {
        let r = self.0.shift_nth(n);
        proof {
            let xs = old(self).0.shift_items().take(n + 1);
            assert(xs.map_values(|x: I::Item| Self::spans(x)) =~= xs.map_values(|x: I::Item| I::shift_spans(x)));
        }
        r
    }
}

impl<I: VShiftable> crate::window::Window for ShiftView<I> {
    open spec fn end(&self) -> int {
        self.0.shift_end()
    }

    proof fn lemma_window(&self) {
        self.0.lemma_shift();
    }

    #[inline(always)]
    fn w_pos(&self) -> (r: usize) {
        self.0.get_pos()
    }

    #[inline(always)]
    fn w_set_max(&mut self, end: usize) {
        self.0.set_max(end)
    }

    #[inline(always)]
    fn w_advance_to(&mut self, target: usize) {
        self.0.advance_to(target)
    }

    #[inline(always)]
    fn w_shift(&mut self, start: usize, end: usize) {
        self.0.shift(start..end)
    }

    #[inline(always)]
    fn w_shift_next(&mut self, start: usize, end: usize) -> (r: Option<I::Item>) {
        self.0.shift_next(start..end)
    }
}

// ── The adapters ───────────────────────────────────────────────────────────

/// A verified iterator with a lookahead, as the old API's
/// [`Unshift`](crate::Unshift) is: the item it gives next can be looked
/// at first ([`peek`](Self::peek)). [`VerifiedUnshift::new`] makes one
/// of any [`VShiftable`] iterator, and [`VerifiedIter::unshift`] of a
/// column's.
///
/// What it has left to give is `unshift_left()`.
pub struct VerifiedUnshift<I: VShiftable> {
    inner: ShiftView<I>,
    next: Option<I::Item>,
    hidden: bool,
}

impl<I: VShiftable> VerifiedUnshift<I> {
    /// Its lookahead stands for the values just before its iterator's
    /// position; with none, the iterator has nothing left.
    #[verifier::type_invariant]
    spec fn inv(&self) -> bool {
        u_inv(self.inner, self.next, self.hidden)
    }

    /// What it has left to give: its lookahead, then what its iterator
    /// has left.
    pub closed spec fn unshift_left(&self) -> Seq<I::Item> {
        u_left(self.inner, self.next, self.hidden)
    }

    /// The values its iterator ranges over.
    pub closed spec fn unshift_vals(&self) -> Seq<I::Item> {
        self.inner.0.shift_vals()
    }

    /// A lookahead of `iter`: what it has left is what `iter` had.
    #[inline(always)]
    pub fn new(iter: I) -> (r: Self)
        ensures
            r.unshift_vals() == iter.shift_vals(),
            r.unshift_left() == iter.shift_items(),
    {
        let mut inner = ShiftView(iter);
        let next = u_new(&mut inner);
        VerifiedUnshift { inner, next, hidden: false }
    }

    /// Its state, taken out: it is left an empty one.
    #[inline(always)]
    fn take(&mut self) -> (r: (ShiftView<I>, Option<I::Item>, bool))
        ensures
            u_inv(r.0, r.1, r.2),
            r.0 == old(self).inner,
            r.1 == old(self).next,
            r.2 == old(self).hidden,
            final(self).unshift_vals() == old(self).unshift_vals(),
    {
        proof {
            use_type_invariant(&*self);
        }
        let mut empty = ShiftView(self.inner.0.dup());
        empty_window(&mut empty);
        proof {
            empty.lemma_items();
            assert(empty.0.shift_items().map_values(|x: I::Item| ShiftView::<I>::spans(x)).len() == 0);
        }
        let mut st = VerifiedUnshift { inner: empty, next: None, hidden: false };
        core::mem::swap(self, &mut st);
        proof {
            use_type_invariant(&st);
        }
        let VerifiedUnshift { inner: i, next: n, hidden: h } = st;
        (i, n, h)
    }

    /// The item it gives next, without giving it.
    #[inline(always)]
    pub fn peek(&self) -> (r: Option<&I::Item>)
        ensures
            self.unshift_left().len() > 0 ==> r == Some(&self.unshift_left()[0]),
            self.unshift_left().len() == 0 ==> r is None,
    {
        proof {
            use_type_invariant(self);
        }
        u_peek(&self.inner, &self.next, self.hidden)
    }

    /// Move to `range`, at its iterator or after it, where each item
    /// stands for one value: what is left is then the values of `range`.
    /// The lookahead's own value may start `range`.
    ///
    /// # Panics
    ///
    /// If `range` starts further back, as the old API's does.
    #[inline(always)]
    pub fn shift(&mut self, range: Range<usize>)
        requires
            I::shift_unit(),
        ensures
            final(self).unshift_vals() == old(self).unshift_vals(),
            final(self).unshift_left() == window(old(self).unshift_vals(), range.start as int, range.end as int),
    {
        proof {
            use_type_invariant(&*self);
        }
        // the state is taken out, moved on, and put back whole
        let (mut inner, mut next, mut hidden) = self.take();
        u_shift(&mut inner, &mut next, &mut hidden, range.start, range.end);
        *self = VerifiedUnshift { inner, next, hidden };
    }
}

#[cfg(verus_keep_ghost)]
impl<I: VShiftable> vstd::std_specs::iter::IteratorSpecImpl for VerifiedUnshift<I> {
    /// It keeps to what Verus asks of an iterator: `next` gives
    /// `remaining()`, one item after another, and then `None`.
    open spec fn obeys_prophetic_iter_laws(&self) -> bool {
        true
    }

    open spec fn remaining(&self) -> Seq<I::Item> {
        self.unshift_left()
    }

    open spec fn will_return_none(&self) -> bool {
        true
    }

    open spec fn decrease(&self) -> Option<nat> {
        Some(self.unshift_left().len())
    }

    open spec fn peek(&self, index: int) -> Option<I::Item> {
        if 0 <= index < self.unshift_left().len() {
            Some(self.unshift_left()[index])
        } else {
            None
        }
    }
}

impl<I: VShiftable> Iterator for VerifiedUnshift<I> {
    type Item = I::Item;

    /// The lookahead, pulling the next one.
    #[inline(always)]
    fn next(&mut self) -> (r: Option<I::Item>)
        ensures
            final(self).unshift_vals() == old(self).unshift_vals(),
            match r {
                Some(x) => {
                    &&& old(self).unshift_left().len() > 0
                    &&& x == old(self).unshift_left()[0]
                    &&& final(self).unshift_left() == old(self).unshift_left().skip(1)
                },
                None => old(self).unshift_left().len() == 0 && final(self).unshift_left().len() == 0,
            },
    {
        proof {
            use_type_invariant(&*self);
        }
        let (mut inner, mut next, hidden) = self.take();
        let r = u_next(&mut inner, &mut next, hidden);
        *self = VerifiedUnshift { inner, next, hidden };
        proof {
            let g = old(self).unshift_left();
            if g.len() > 0 {
                assert(g.skip(1) =~= g.drop_first());
            }
        }
        r
    }

    /// The `n`-th item left, giving it and those before it.
    #[inline(always)]
    fn nth(&mut self, n: usize) -> (r: Option<I::Item>)
        ensures
            final(self).unshift_vals() == old(self).unshift_vals(),
            n < old(self).unshift_left().len() ==> {
                &&& r == Some(old(self).unshift_left()[n as int])
                &&& final(self).unshift_left() == old(self).unshift_left().skip(n + 1)
            },
            n >= old(self).unshift_left().len() ==> r is None && final(self).unshift_left().len() == 0,
    {
        proof {
            use_type_invariant(&*self);
        }
        let (mut inner, mut next, hidden) = self.take();
        let r = u_nth(&mut inner, &mut next, hidden, n);
        *self = VerifiedUnshift { inner, next, hidden };
        r
    }
}

/// The values of several windows of a verified iterator, one window
/// after another, as the old API's [`Ranges`](crate::Ranges) gives them:
/// [`VerifiedRanges::new`] makes one of any [`VShiftable`] iterator,
/// and [`VerifiedIter::ranges`] of a column's.
///
/// What it has left to give is `ranges_left()`: what is left of the
/// window it is in, then the values of each range it has not taken; an
/// item stands for `I::shift_spans` of it.
pub struct VerifiedRanges<'r, I: VShiftable> {
    view: ShiftView<I>,
    ranges: &'r [Range<usize>],
    i: usize,
}

impl<'r, I: VShiftable> VerifiedRanges<'r, I> {
    #[verifier::type_invariant]
    spec fn inv(&self) -> bool {
        self.i <= self.ranges@.len()
    }

    /// What it has left to give, value by value.
    pub closed spec fn ranges_left(&self) -> Seq<I::Item> {
        self.view.0.shift_left() + windows(self.view.0.shift_vals(), self.ranges@.skip(self.i as int))
    }

    /// The values its iterator ranges over.
    pub closed spec fn ranges_vals(&self) -> Seq<I::Item> {
        self.view.0.shift_vals()
    }

    /// The values of each of `ranges` of what `iter` ranges over, one
    /// range after another (what is left of `iter`'s own window is
    /// dropped). The ranges ascend and do not overlap (they may touch),
    /// and start where `iter` stands or later.
    ///
    /// # Panics
    ///
    /// When it comes to a range that starts before where it stands, as
    /// the old API's `Ranges` does.
    #[inline(always)]
    pub fn new(iter: I, ranges: &'r [Range<usize>]) -> (r: Self)
        ensures
            r.ranges_vals() == iter.shift_vals(),
            r.ranges_left() == windows(iter.shift_vals(), ranges@),
    {
        let mut view = ShiftView(iter);
        empty_window(&mut view);
        proof {
            assert(ranges@.skip(0) =~= ranges@);
            assert(view.0.shift_left() =~= Seq::<I::Item>::empty());
            assert(view.0.shift_left() + windows(view.0.shift_vals(), ranges@.skip(0)) =~= windows(
                view.0.shift_vals(),
                ranges@,
            ));
        }
        VerifiedRanges { view, ranges, i: 0 }
    }
}

/// The values of each range of `rs` from the `i`-th on: the first of
/// them, and the rest.
proof fn lemma_windows_front<T>(v: Seq<T>, rs: Seq<Range<usize>>, i: int)
    requires
        0 <= i < rs.len(),
    ensures
        windows(v, rs.skip(i)) == window(v, rs[i].start as int, rs[i].end as int) + windows(v, rs.skip(i + 1)),
{
    let f = |r: Range<usize>| window(v, r.start as int, r.end as int);
    let m = rs.skip(i).map_values(f);
    assert(m =~= seq![f(rs[i])] + rs.skip(i + 1).map_values(f));
    crate::cursor::lemma_concat_append(seq![f(rs[i])], rs.skip(i + 1).map_values(f));
    let one = seq![f(rs[i])];
    assert(one.drop_last() =~= Seq::<Seq<T>>::empty());
    assert(concat(one) == concat(one.drop_last()) + one.last());
    assert(concat(one) =~= f(rs[i]));
}

/// `VerifiedRanges::next`: the next value of the window, or of the next
/// range with any.
fn ranges_step<W: Window>(w: &mut W, rs: &[Range<usize>], i: &mut usize) -> (r: Option<W::Item>)
    requires
        *old(i) <= rs.len(),
    ensures
        *final(i) <= rs.len(),
        final(w).vals() == old(w).vals(),
        ({
            let g = old(w).left() + windows(old(w).vals(), rs@.skip(*old(i) as int));
            let g1 = final(w).left() + windows(final(w).vals(), rs@.skip(*final(i) as int));
            match r {
                Some(x) => {
                    let n = W::spans(x).len() as int;
                    &&& 0 < n <= g.len()
                    &&& W::spans(x) == g.take(n)
                    &&& g1 == g.skip(n)
                },
                None => g.len() == 0 && g1.len() == 0,
            }
        }),
{
    let ghost (w0, i0) = (*w, *i);
    loop
        invariant
            w0 == *old(w),
            i0 == *old(i),
            *i <= rs.len(),
            w.vals() == w0.vals(),
            w.left() + windows(w.vals(), rs@.skip(*i as int)) == w0.left() + windows(w0.vals(), rs@.skip(i0 as int)),
        decreases rs.len() - *i,
    {
        let ghost (w1, i1) = (*w, *i);
        proof {
            w1.lemma_items();
        }
        if let Some(x) = w.s_next() {
            proof {
                w.lemma_items();
                lemma_flat_first::<W>(w1.items());
                assert(w1.items().skip(1) == w.items());
                assert(W::spans(x).len() > 0) by {
                    assert(x == w1.items()[0]);
                }
                let rest = windows(w.vals(), rs@.skip(i1 as int));
                assert(w1.left() + rest =~= W::spans(x) + (w.left() + rest));
            }
            return Some(x);
        }
        proof {
            w.lemma_items();
            assert(w1.items().map_values(|x: W::Item| W::spans(x)) =~= Seq::<Seq<W::Item>>::empty());
            assert(w.items().map_values(|x: W::Item| W::spans(x)) =~= Seq::<Seq<W::Item>>::empty());
        }
        if *i >= rs.len() {
            proof {
                assert(rs@.skip(i1 as int).map_values(|r: Range<usize>| window(w.vals(), r.start as int, r.end as int))
                    =~= Seq::<Seq<W::Item>>::empty());
                assert(w.left() + windows(w.vals(), rs@.skip(i1 as int)) =~= Seq::<W::Item>::empty());
            }
            return None;
        }
        let start = rs[*i].start;
        let end = rs[*i].end;
        if start < w.w_pos() {
            ranges_out_of_order();
        }
        w.w_shift(start, end);
        *i = *i + 1;
        proof {
            let v = w0.vals();
            lemma_windows_front(v, rs@, i1 as int);
            assert(w1.left() =~= Seq::<W::Item>::empty());
            assert(w.left() + windows(v, rs@.skip(i1 + 1)) =~= w1.left() + windows(v, rs@.skip(i1 as int)));
        }
    }
}

#[cfg(verus_keep_ghost)]
impl<'r, I: VShiftable> vstd::std_specs::iter::IteratorSpecImpl for VerifiedRanges<'r, I> {
    /// Where each item stands for one value, it keeps to what Verus asks
    /// of an iterator: `next` gives `remaining()`, one item after
    /// another, and then `None`.
    open spec fn obeys_prophetic_iter_laws(&self) -> bool {
        I::shift_unit()
    }

    open spec fn remaining(&self) -> Seq<I::Item> {
        self.ranges_left()
    }

    open spec fn will_return_none(&self) -> bool {
        true
    }

    open spec fn decrease(&self) -> Option<nat> {
        Some(self.ranges_left().len())
    }

    open spec fn peek(&self, index: int) -> Option<I::Item> {
        if 0 <= index < self.ranges_left().len() {
            Some(self.ranges_left()[index])
        } else {
            None
        }
    }
}

impl<'r, I: VShiftable> Iterator for VerifiedRanges<'r, I> {
    type Item = I::Item;

    /// The next item of the window, or of the next range with any.
    #[inline(always)]
    fn next(&mut self) -> (r: Option<I::Item>)
        ensures
            final(self).ranges_vals() == old(self).ranges_vals(),
            match r {
                Some(x) => {
                    let (g, n) = (old(self).ranges_left(), I::shift_spans(x).len() as int);
                    &&& 0 < n <= g.len()
                    &&& I::shift_spans(x) == g.take(n)
                    &&& final(self).ranges_left() == g.skip(n)
                },
                None => old(self).ranges_left().len() == 0 && final(self).ranges_left().len() == 0,
            },
    {
        proof {
            use_type_invariant(&*self);
        }
        let mut view = ShiftView(self.view.0.dup());
        let mut i = self.i;
        let r = ranges_step(&mut view, self.ranges, &mut i);
        *self = VerifiedRanges { view, ranges: self.ranges, i };
        proof {
            if I::shift_unit() && r is Some {
                {
                    let x = r->Some_0;
                    I::lemma_shift_unit(x);
                    let g = old(self).ranges_left();
                    assert(g.take(1) == seq![x]);
                    assert(seq![x][0] == x);
                    assert(g.take(1)[0] == g[0]);
                    assert(g.skip(1) =~= g.drop_first());
                }
            }
        }
        r
    }
}

// ── The value iterator ─────────────────────────────────────────────────────

/// The values of windows of items, as a specification has them, are the
/// windows of the values.
pub proof fn lemma_svs_windows<T: VGet>(v: Seq<T>, rs: Seq<Range<usize>>)
    ensures
        T::svs(windows(v, rs)) == windows(T::svs(v), rs),
    decreases rs.len(),
{
    T::lemma_svs(v);
    if rs.len() == 0 {
        let f = |r: Range<usize>| window(v, r.start as int, r.end as int);
        let g = |r: Range<usize>| window(T::svs(v), r.start as int, r.end as int);
        assert(rs.map_values(f) =~= Seq::<Seq<T>>::empty());
        assert(rs.map_values(g) =~= Seq::<Seq<T::S>>::empty());
        T::lemma_svs(Seq::<T>::empty());
        assert(T::svs(Seq::<T>::empty()) =~= Seq::<T::S>::empty());
    } else {
        let p = rs.drop_last();
        let r = rs.last();
        let f = |r: Range<usize>| window(v, r.start as int, r.end as int);
        let g = |r: Range<usize>| window(T::svs(v), r.start as int, r.end as int);
        lemma_svs_windows(v, p);
        assert(rs.map_values(f).drop_last() =~= p.map_values(f));
        assert(rs.map_values(g).drop_last() =~= p.map_values(g));
        assert(windows(v, rs) == windows(v, p) + window(v, r.start as int, r.end as int));
        assert(windows(T::svs(v), rs) == windows(T::svs(v), p) + window(T::svs(v), r.start as int, r.end as int));
        lemma_svs_add(windows(v, p), window(v, r.start as int, r.end as int));
        lemma_svs_window(v, r.start as int, r.end as int);
    }
}

/// Values that are the same in a specification are the same.
pub(crate) proof fn lemma_svs_inj<T: VGet>(a: Seq<T>, b: Seq<T>)
    requires
        T::svs(a) == T::svs(b),
    ensures
        a == b,
{
    T::lemma_svs(a);
    T::lemma_svs(b);
    assert forall|i: int| 0 <= i < a.len() implies a[i] == b[i] by {
        assert(a.len() == b.len());
        assert(T::svs(a)[i] == a[i].sv());
        assert(T::svs(b)[i] == b[i].sv());
        T::lemma_sv_inj(a[i], b[i]);
    }
    assert(a =~= b);
}

/// What is left after moving on, as a specification has it, is the same
/// move of the values as it has them.
proof fn lemma_svs_past<T: VGet>(o: Seq<T>, k: int)
    ensures
        T::svs(past(o, k)) == past(T::svs(o), k),
{
    T::lemma_svs(o);
    if 0 < k <= o.len() {
        lemma_svs_sub(o, k, o.len() as int);
    } else if k > o.len() {
        T::lemma_svs(Seq::<T>::empty());
        assert(T::svs(Seq::<T>::empty()) =~= Seq::<T::S>::empty());
    }
}

/// Items that each stand for themselves alone are the values they stand
/// for.
pub(crate) proof fn lemma_singles<T>(xs: Seq<T>)
    ensures
        concat(xs.map_values(|x: T| seq![x])) == xs,
    decreases xs.len(),
{
    if xs.len() > 0 {
        let p = xs.drop_last();
        lemma_singles(p);
        let m = xs.map_values(|x: T| seq![x]);
        assert(m.drop_last() =~= p.map_values(|x: T| seq![x]));
        assert(m.last() == seq![xs.last()]);
        assert(concat(m) == concat(m.drop_last()) + m.last());
        assert(xs =~= p + seq![xs.last()]);
    } else {
        assert(xs.map_values(|x: T| seq![x]) =~= Seq::<Seq<T>>::empty());
    }
}

impl<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>> seal::ShiftSeal for VerifiedIter<'a, V, C, K> {

}

impl<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>> VShiftable for VerifiedIter<'a, V, C, K> {
    closed spec fn shift_vals(&self) -> Seq<Get<'a, V>> {
        vals_of::<V::X>(self.slabs@)
    }

    closed spec fn shift_at(&self) -> int {
        self.it.pos as int
    }

    closed spec fn shift_end(&self) -> int {
        self.it.pos + self.it.items_left
    }

    closed spec fn shift_left(&self) -> Seq<Get<'a, V>> {
        self.it.out::<V::X>(self.slabs@)
    }

    closed spec fn shift_items(&self) -> Seq<Get<'a, V>> {
        self.it.out::<V::X>(self.slabs@)
    }

    open spec fn shift_spans(x: Get<'a, V>) -> Seq<Get<'a, V>> {
        seq![x]
    }

    open spec fn shift_unit() -> bool {
        true
    }

    proof fn lemma_shift(&self) {
        self.it.lemma_out_window::<V::X>(self.slabs@);
        let xs = self.it.out::<V::X>(self.slabs@);
        lemma_singles(xs);
        assert(xs.map_values(|x: Get<'a, V>| Self::shift_spans(x)) =~= xs.map_values(|x: Get<'a, V>| seq![x]));
    }

    proof fn lemma_shift_unit(x: Get<'a, V>) {
    }

    #[inline(always)]
    fn shift_step(&mut self) -> (r: Option<Get<'a, V>>) {
        let r = self.nth(0);
        proof {
            lemma_svs_inj(vals_of::<V::X>(self.slabs@), vals_of::<V::X>(old(self).slabs@));
        }
        r
    }

    #[inline(always)]
    fn shift_nth(&mut self, n: usize) -> (r: Option<Get<'a, V>>) {
        let r = self.nth(n);
        proof {
            lemma_svs_inj(vals_of::<V::X>(self.slabs@), vals_of::<V::X>(old(self).slabs@));
            if n < old(self).it.out::<V::X>(old(self).slabs@).len() {
                let xs = old(self).it.out::<V::X>(old(self).slabs@).take(n + 1);
                lemma_singles(xs);
                assert(xs.map_values(|x: Get<'a, V>| Self::shift_spans(x)) =~= xs.map_values(|x: Get<'a, V>| seq![x]));
            }
        }
        r
    }

    #[inline(always)]
    fn dup(&self) -> (r: Self) {
        proof {
            use_type_invariant(self);
        }
        VerifiedIter { it: self.it.dup(), slabs: self.slabs, col: self.col }
    }

    #[inline(always)]
    fn get_pos(&self) -> (r: usize) {
        self.pos()
    }

    #[inline(always)]
    fn get_max(&self) -> (r: usize) {
        proof {
            use_type_invariant(&*self);
        }
        self.end_pos()
    }

    #[inline(always)]
    fn set_max(&mut self, end: usize) {
        proof {
            use_type_invariant(&*self);
        }
        VerifiedIter::set_max(self, end);
        proof {
            use_type_invariant(&*self);
            lemma_svs_inj(vals_of::<V::X>(self.slabs@), vals_of::<V::X>(old(self).slabs@));
        }
    }

    #[inline(always)]
    fn advance_to(&mut self, target: usize) {
        proof {
            use_type_invariant(&*self);
            <Get<'a, V> as VGet>::lemma_svs(self.it.out::<V::X>(self.slabs@));
        }
        VerifiedIter::advance_to(self, target);
        proof {
            use_type_invariant(&*self);
            let k = target - old(self).shift_at();
            lemma_svs_inj(vals_of::<V::X>(self.slabs@), vals_of::<V::X>(old(self).slabs@));
            lemma_svs_past(old(self).it.out::<V::X>(old(self).slabs@), k);
            lemma_svs_inj(self.it.out::<V::X>(self.slabs@), past(old(self).it.out::<V::X>(old(self).slabs@), k));
        }
    }

    #[inline(always)]
    fn advance_by(&mut self, amount: usize) {
        proof {
            use_type_invariant(&*self);
            <Get<'a, V> as VGet>::lemma_svs(self.it.out::<V::X>(self.slabs@));
        }
        VerifiedIter::advance_by(self, amount);
        proof {
            use_type_invariant(&*self);
            lemma_svs_inj(vals_of::<V::X>(self.slabs@), vals_of::<V::X>(old(self).slabs@));
            lemma_svs_past(old(self).it.out::<V::X>(old(self).slabs@), amount as int);
            lemma_svs_inj(self.it.out::<V::X>(self.slabs@), past(old(self).it.out::<V::X>(old(self).slabs@), amount as int));
        }
    }

    #[inline(always)]
    fn seek_to(&mut self, at: usize) -> (r: Option<Get<'a, V>>) {
        let p = self.pos();
        if at >= p {
            let r = self.nth(at - p);
            proof {
                lemma_svs_inj(vals_of::<V::X>(self.slabs@), vals_of::<V::X>(old(self).slabs@));
            }
            r
        } else {
            None
        }
    }

    #[inline(always)]
    fn shift(&mut self, range: Range<usize>) {
        proof {
            use_type_invariant(&*self);
        }
        let ghost (start, end) = (range.start, range.end);
        VerifiedIter::shift(self, range);
        proof {
            use_type_invariant(&*self);
            let v = vals_of::<V::X>(old(self).slabs@);
            let a = if start <= old(self).shift_at() {
                old(self).shift_at()
            } else {
                start as int
            };
            lemma_svs_inj(vals_of::<V::X>(self.slabs@), v);
            lemma_svs_window(v, a, end as int);
            lemma_svs_inj(self.it.out::<V::X>(self.slabs@), window(v, a, end as int));
            <Get<'a, V> as VGet>::lemma_svs(v);
        }
    }

    #[inline(always)]
    fn shift_next(&mut self, range: Range<usize>) -> (r: Option<Get<'a, V>>) {
        proof {
            use_type_invariant(&*self);
        }
        let ghost (start, end) = (range.start, range.end);
        let r = VerifiedIter::shift_next(self, range);
        proof {
            use_type_invariant(&*self);
            let v = vals_of::<V::X>(old(self).slabs@);
            let w = window(v, start as int, end as int);
            lemma_svs_inj(vals_of::<V::X>(self.slabs@), v);
            lemma_svs_window(v, start as int, end as int);
            <Get<'a, V> as VGet>::lemma_svs(w);
            if w.len() > 0 {
                let x = r->Some_0;
                <Get<'a, V> as VGet>::lemma_sv_inj(x, w[0]);
                assert(seq![x] =~= w.take(1));
                lemma_svs_sub(w, 1, w.len() as int);
                lemma_svs_inj(self.it.out::<V::X>(self.slabs@), w.skip(1));
            }
        }
        r
    }
}

impl<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>> VerifiedIter<'a, V, C, K> {
    /// The iterator's repositioning model ([`VShiftable`]) in the terms
    /// of its own specification: the values it ranges over are its
    /// column's, as items; its position is `pos()`; what it has left is
    /// `items()`, each item a value.
    pub proof fn lemma_shift_model(&self)
        ensures
            <Get<'a, V> as VGet>::svs(self.shift_vals()) == self.column(),
            self.shift_at() == self.pos(),
            self.shift_items() == self.items(),
            self.shift_left() == self.items(),
            Self::shift_unit(),
    {
    }

    /// A lookahead: the iterator wrapped so that the value it gives next
    /// can be looked at first ([`VerifiedUnshift::peek`]), as the old
    /// API's `Shiftable::unshift` gives.
    #[inline(always)]
    pub fn unshift(self) -> (r: VerifiedUnshift<Self>)
        ensures
            r.unshift_vals() == self.shift_vals(),
            r.unshift_left() == self.shift_items(),
    {
        VerifiedUnshift::new(self)
    }

    /// The values of each of `ranges` of the column, one range after
    /// another, as the old API's `Shiftable::ranges` gives them (what is
    /// left of the iterator's own window is dropped). The ranges ascend
    /// and do not overlap (they may touch), and start where the iterator
    /// stands or later.
    ///
    /// # Panics
    ///
    /// When it comes to a range that starts before where it stands.
    #[inline(always)]
    pub fn ranges<'r>(self, ranges: &'r [Range<usize>]) -> (r: VerifiedRanges<'r, Self>)
        ensures
            r.ranges_vals() == self.shift_vals(),
            r.ranges_left() == windows(self.shift_vals(), ranges@),
    {
        VerifiedRanges::new(self, ranges)
    }
}

} // verus!
