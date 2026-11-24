//! Repositioning the iterators: the trait [`Shiftable`], and the adapters
//! [`Unshift`] (a lookahead) and [`Ranges`] (several windows, one after
//! another).
//!
//! The adapters are `window.rs`'s, which are proved against its model of
//! an iterator (its traits `Stream` and `Window`). An iterator is given
//! that model by a view of it that is no iterator (`ShiftView`), whose
//! model is the iterator's `Shiftable` specification, proved of each
//! iterator here: nothing trusted stands between the adapters and the
//! iterator.
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([`verified`](crate::verified#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`shift/imp.rs`), which has
//! its contract word for word and is proved there.

#[cfg(verus_keep_ghost)]
use super::runs_of;
#[cfg(verus_keep_ghost)]
use super::VGet;
use super::{Codec, ColumnValueRef, Get, Iter, Runs, VIndex};
#[cfg(verus_keep_ghost)]
use crate::iter_core::{past, vals_of, window};
#[cfg(verus_keep_ghost)]
use crate::rle::writer::concat;
#[cfg(verus_keep_ghost)]
use crate::window::Window;
#[cfg(verus_keep_ghost)]
use crate::window::{u_left, windows, Stream};
use crate::Run;
use std::ops::Range;
use vstd::prelude::*;

mod imp;
#[allow(unused_imports)]
pub(crate) use imp::ShiftView;
#[allow(unused_imports)]
use imp::*;
#[cfg(verus_keep_ghost)]
#[allow(unused_imports)]
pub(crate) use imp::{lemma_concat_one, lemma_run_units, lemma_svs_inj};

verus! {

/// An iterator that can be repositioned. Hexane's iterators are; and a
/// client's own iterator may be, as it may implement `Shiftable`. Its
/// laws are this trait's: a client checked by Verus proves them, and a
/// client in plain Rust promises them. Hexane's adapters ([`Unshift`],
/// [`Ranges`]) are proved of any iterator that keeps them.
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
pub trait Shiftable: Iterator + Sized {
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

    /// Move `amount` values on.
    fn advance_by(&mut self, amount: usize)
        ensures
            final(self).shift_vals() == old(self).shift_vals(),
            final(self).shift_left() == past(old(self).shift_left(), amount as int),
            amount <= old(self).shift_left().len() ==> final(self).shift_at() == old(self).shift_at() + amount,
    ;

    /// Move to the value at `at` and give the item from there, which
    /// stands for it and for as many after it as it counts (an iterator
    /// of runs gives the run from there); at a place before the iterator,
    /// give nothing and stay.
    fn seek_to(&mut self, at: usize) -> (r: Option<Self::Item>)
        ensures
            final(self).shift_vals() == old(self).shift_vals(),
            at < old(self).shift_at() ==> {
                &&& r is None
                &&& final(self).shift_left() == old(self).shift_left()
                &&& final(self).shift_at() == old(self).shift_at()
            },
            at >= old(self).shift_at() ==> {
                let (o, k) = (old(self).shift_left(), at - old(self).shift_at());
                &&& k < o.len() ==> (r matches Some(x) && {
                    let n = Self::shift_spans(x).len() as int;
                    &&& 0 < n <= o.len() - k
                    &&& Self::shift_spans(x) == o.skip(k).take(n)
                    &&& final(self).shift_left() == o.skip(k + n)
                    &&& final(self).shift_at() == at + n
                })
                &&& k < o.len() && Self::shift_unit() ==> {
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

// ── The adapters ───────────────────────────────────────────────────────────

/// An iterator with a lookahead: the item it gives next can be looked
/// at first ([`peek`](Self::peek)). [`Unshift::new`] makes one
/// of any [`Shiftable`] iterator, and [`Iter::unshift`] of a
/// column's.
///
/// What it has left to give is `unshift_left()`.
pub struct Unshift<I: Shiftable> {
    inner: ShiftView<I>,
    next: Option<I::Item>,
    hidden: bool,
}

impl<I: Shiftable> Unshift<I> {
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
        Self::new_impl(iter)
    }

    /// The item it gives next, without giving it.
    #[inline(always)]
    pub fn peek(&self) -> (r: Option<&I::Item>)
        ensures
            self.unshift_left().len() > 0 ==> r == Some(&self.unshift_left()[0]),
            self.unshift_left().len() == 0 ==> r is None,
    {
        self.peek_impl()
    }

    /// Move to `range`, at its iterator or after it, where each item
    /// stands for one value: what is left is then the values of `range`.
    /// The lookahead's own value may start `range`.
    ///
    /// # Panics
    ///
    /// If `range` starts further back.
    #[inline(always)]
    pub fn shift(&mut self, range: Range<usize>)
        requires
            I::shift_unit(),
        ensures
            final(self).unshift_vals() == old(self).unshift_vals(),
            final(self).unshift_left() == window(old(self).unshift_vals(), range.start as int, range.end as int),
    {
        self.shift_impl(range)
    }
}

#[cfg(verus_keep_ghost)]
impl<I: Shiftable> vstd::std_specs::iter::IteratorSpecImpl for Unshift<I> {
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

impl<I: Shiftable> Iterator for Unshift<I> {
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
        self.next_impl()
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
        self.nth_impl(n)
    }
}

/// The values of several windows of an iterator, one window after
/// another:
/// [`Ranges::new`] makes one of any [`Shiftable`] iterator,
/// and [`Iter::ranges`] of a column's.
///
/// What it has left to give is `ranges_left()`: what is left of the
/// window it is in, then the values of each range it has not taken; an
/// item stands for `I::shift_spans` of it.
pub struct Ranges<'r, I: Shiftable> {
    view: ShiftView<I>,
    ranges: &'r [Range<usize>],
    i: usize,
}

impl<'r, I: Shiftable> Ranges<'r, I> {
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
    /// When it comes to a range that starts before where it stands.
    #[inline(always)]
    pub fn new(iter: I, ranges: &'r [Range<usize>]) -> (r: Self)
        ensures
            r.ranges_vals() == iter.shift_vals(),
            r.ranges_left() == windows(iter.shift_vals(), ranges@),
    {
        Self::new_impl(iter, ranges)
    }
}

#[cfg(verus_keep_ghost)]
impl<'r, I: Shiftable> vstd::std_specs::iter::IteratorSpecImpl for Ranges<'r, I> {
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

impl<'r, I: Shiftable> Iterator for Ranges<'r, I> {
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
        self.next_impl()
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
    imp::lemma_svs_windows_impl::<T>(v, rs)
}

/// Items that each stand for themselves alone are the values they stand
/// for.
pub proof fn lemma_singles<T>(xs: Seq<T>)
    ensures
        concat(xs.map_values(|x: T| seq![x])) == xs,
    decreases xs.len(),
{
    imp::lemma_singles_impl::<T>(xs)
}

impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Shiftable for Iter<'a, V, C, K> {
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
        self.lemma_shift_impl()
    }

    proof fn lemma_shift_unit(x: Get<'a, V>) {
    }

    #[inline(always)]
    fn shift_step(&mut self) -> (r: Option<Get<'a, V>>) {
        self.shift_step_impl()
    }

    #[inline(always)]
    fn shift_nth(&mut self, n: usize) -> (r: Option<Get<'a, V>>) {
        self.shift_nth_impl(n)
    }

    #[inline(always)]
    fn dup(&self) -> (r: Self) {
        self.dup_impl()
    }

    #[inline(always)]
    fn get_pos(&self) -> (r: usize) {
        self.pos()
    }

    #[inline(always)]
    fn get_max(&self) -> (r: usize) {
        self.get_max_impl()
    }

    #[inline(always)]
    fn set_max(&mut self, end: usize) {
        self.set_max_v_shiftable_impl(end)
    }

    #[inline(always)]
    fn advance_to(&mut self, target: usize) {
        self.advance_to_v_shiftable_impl(target)
    }

    #[inline(always)]
    fn advance_by(&mut self, amount: usize) {
        self.advance_by_v_shiftable_impl(amount)
    }

    #[inline(always)]
    fn seek_to(&mut self, at: usize) -> (r: Option<Get<'a, V>>) {
        self.seek_to_impl(at)
    }

    #[inline(always)]
    fn shift(&mut self, range: Range<usize>) {
        self.shift_v_shiftable_impl(range)
    }

    #[inline(always)]
    fn shift_next(&mut self, range: Range<usize>) -> (r: Option<Get<'a, V>>) {
        self.shift_next_v_shiftable_impl(range)
    }
}

impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Iter<'a, V, C, K> {
    /// The iterator's repositioning model ([`Shiftable`]) in the terms
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
        self.lemma_shift_model_impl()
    }

    /// A lookahead: the iterator wrapped so that the value it gives next
    /// can be looked at first ([`Unshift::peek`]).
    #[inline(always)]
    pub fn unshift(self) -> (r: Unshift<Self>)
        ensures
            r.unshift_vals() == self.shift_vals(),
            r.unshift_left() == self.shift_items(),
    {
        self.unshift_impl()
    }

    /// The values of each of `ranges` of the column, one range after
    /// another (what is
    /// left of the iterator's own window is dropped). The ranges ascend
    /// and do not overlap (they may touch), and start where the iterator
    /// stands or later.
    ///
    /// # Panics
    ///
    /// When it comes to a range that starts before where it stands.
    #[inline(always)]
    pub fn ranges<'r>(self, ranges: &'r [Range<usize>]) -> (r: Ranges<'r, Self>)
        ensures
            r.ranges_vals() == self.shift_vals(),
            r.ranges_left() == windows(self.shift_vals(), ranges@),
    {
        self.ranges_impl(ranges)
    }
}

// ── Runs, repositioned ─────────────────────────────────────────────────────

/// Each of `xs` as a run of one: what an iterator of runs ranges over,
/// value by value.
pub open spec fn unit_runs<T>(xs: Seq<T>) -> Seq<Run<T>> {
    Seq::new(xs.len(), |i: int| Run { count: 1, value: xs[i] })
}

/// The values the run `r` stands for, each a run of one.
pub open spec fn run_units<T>(r: Run<T>) -> Seq<Run<T>> {
    Seq::new(r.count as nat, |_i: int| Run { count: 1, value: r.value })
}

/// What each of the runs `rs` stands for.
pub open spec fn units_of_runs<T>(rs: Seq<Run<T>>) -> Seq<Seq<Run<T>>> {
    rs.map_values(|r: Run<T>| run_units(r))
}

/// The runs of a column, repositioned as its values are: the model is
/// each value as a run of one, and an item, a run, stands for as many of
/// those as it counts.
impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Shiftable for Runs<'a, V, C, K> {
    closed spec fn shift_vals(&self) -> Seq<Run<Get<'a, V>>> {
        unit_runs(self.0.shift_vals())
    }

    closed spec fn shift_at(&self) -> int {
        self.0.shift_at()
    }

    closed spec fn shift_end(&self) -> int {
        self.0.shift_end()
    }

    closed spec fn shift_left(&self) -> Seq<Run<Get<'a, V>>> {
        unit_runs(self.0.shift_left())
    }

    closed spec fn shift_items(&self) -> Seq<Run<Get<'a, V>>> {
        runs_of(self.0.shift_left())
    }

    open spec fn shift_spans(x: Run<Get<'a, V>>) -> Seq<Run<Get<'a, V>>> {
        run_units(x)
    }

    open spec fn shift_unit() -> bool {
        false
    }

    proof fn lemma_shift(&self) {
        self.lemma_shift_impl()
    }

    proof fn lemma_shift_unit(x: Run<Get<'a, V>>) {
    }

    #[inline(always)]
    fn shift_step(&mut self) -> (r: Option<Run<Get<'a, V>>>) {
        self.take_run()
    }

    #[inline(always)]
    fn shift_nth(&mut self, n: usize) -> (r: Option<Run<Get<'a, V>>>) {
        self.shift_nth_impl(n)
    }

    #[inline(always)]
    fn dup(&self) -> (r: Self) {
        Runs(Shiftable::dup(&self.0))
    }

    #[inline(always)]
    fn get_pos(&self) -> (r: usize) {
        Shiftable::get_pos(&self.0)
    }

    #[inline(always)]
    fn get_max(&self) -> (r: usize) {
        Shiftable::get_max(&self.0)
    }

    #[inline(always)]
    fn set_max(&mut self, end: usize) {
        Shiftable::set_max(&mut self.0, end)
    }

    #[inline(always)]
    fn advance_to(&mut self, target: usize) {
        Shiftable::advance_to(&mut self.0, target)
    }

    #[inline(always)]
    fn advance_by(&mut self, amount: usize) {
        Shiftable::advance_by(&mut self.0, amount)
    }

    #[inline(always)]
    fn seek_to(&mut self, at: usize) -> (r: Option<Run<Get<'a, V>>>) {
        self.seek_to_impl(at)
    }

    #[inline(always)]
    fn shift(&mut self, range: Range<usize>) {
        Shiftable::shift(&mut self.0, range)
    }

    #[inline(always)]
    fn shift_next(&mut self, range: Range<usize>) -> (r: Option<Run<Get<'a, V>>>) {
        self.shift_next_impl(range)
    }
}

impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Runs<'a, V, C, K> {
    /// The iterator's repositioning model ([`Shiftable`]) in the terms
    /// of its own specification: the values it ranges over are its
    /// column's, each a run of one; it stands where its iterator of
    /// values does; its items are the runs of what that has left.
    pub proof fn lemma_shift_model(&self)
        ensures
            self.shift_vals().len() == self.iter().column().len(),
            forall|p: int|
                0 <= p < self.iter().column().len() ==> {
                    &&& (#[trigger] self.shift_vals()[p]).count == 1
                    &&& self.shift_vals()[p].value.sv() == self.iter().column()[p]
                },
            self.shift_at() == self.iter().pos(),
            self.shift_items() == runs_of(self.iter().items()),
            self.shift_left() == unit_runs(self.iter().items()),
            !Self::shift_unit(),
    {
        self.lemma_shift_model_impl()
    }

    /// A lookahead: the runs wrapped so that the run they give next can
    /// be looked at first ([`Unshift::peek`]). (It cannot be moved to a window: its
    /// items are not each one value.)
    #[inline(always)]
    pub fn unshift(self) -> (r: Unshift<Self>)
        ensures
            r.unshift_vals() == self.shift_vals(),
            r.unshift_left() == self.shift_items(),
    {
        self.unshift_impl()
    }

    /// The runs of each of `ranges` of the column, one range after
    /// another: a run
    /// that crosses the edge of a range is cut there. The ranges ascend
    /// and do not overlap (they may touch), and start where the iterator
    /// stands or later.
    ///
    /// # Panics
    ///
    /// When it comes to a range that starts before where it stands.
    #[inline(always)]
    pub fn ranges<'r>(self, ranges: &'r [Range<usize>]) -> (r: Ranges<'r, Self>)
        ensures
            r.ranges_vals() == self.shift_vals(),
            r.ranges_left() == windows(self.shift_vals(), ranges@),
    {
        self.ranges_impl(ranges)
    }
}

} // verus!
