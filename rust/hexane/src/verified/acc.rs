//! Accumulator columns: columns kept with what their values add up to in
//! a type of the client's own, verified with Verus.
//!
//! A prefix column ([`PrefixColumn`](super::PrefixColumn))
//! adds its values up as integers, in one of this crate's integer
//! types. An accumulator column adds them up in a type its client
//! gives, an accumulator ([`Accum`]): the client says what a value adds
//! ([`Adds`]), how two of the accumulators are added, and what no
//! values add up to. A set of the marks that are open at a position is
//! one, where a value opens a mark or closes it.
//!
//! The prefix core asks of such a type that its `+` is associative and
//! that its `-` undoes it: the client *proves* those laws of its type
//! ([`VGroup`]), and what the column answers is proved from them.
//!
//! An accumulator column is a [`Column`] kept with an
//! [`AccIndex`]: of each slab the index holds, besides the count of its
//! values, what they add up to (`accumulated`). What a column of the
//! kind says of its values, and its bytes when saved, are a plain
//! column's, and there is one of any values: nothing overflows, so a
//! load refuses no saved column that a plain column takes, and no edit
//! panics for what the values add up to.
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([Reviewing the specification](crate#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`acc/imp.rs`), which has
//! its contract word for word and is proved there.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use super::codec::Codec;
use super::kind::{Index, Kind, VKeeps};
use super::seal;
use super::value::RleValue;
#[cfg(verus_keep_ghost)]
use super::Held;
#[cfg(verus_keep_ghost)]
use super::VGet;
use super::{Column, ColumnValueRef, Form, Get, VSpec};
#[cfg(verus_keep_ghost)]
use crate::attrib::Accumulate;
#[cfg(verus_keep_ghost)]
use crate::btree::{PrefixAggregate, SlabAggregate};
#[cfg(verus_keep_ghost)]
use crate::cursor::ReadsAgree;
#[cfg(verus_keep_ghost)]
use crate::cursor::{Slabs, Weigh};
#[cfg(verus_keep_ghost)]
use crate::index::IndexOps;
#[cfg(verus_keep_ghost)]
use crate::iter_core::Decoders;
#[cfg(verus_keep_ghost)]
use crate::prefix_core::before;
#[cfg(verus_keep_ghost)]
use crate::prefix_core::{PrefixOps, PrefixWeight};
#[cfg(verus_keep_ghost)]
use crate::weights::{CopyWeigh, LoadWeigh};
use crate::Leb128;
use std::fmt::Debug;
use std::ops::Range;
#[allow(unused_imports)]
use vstd::prelude::*;

mod imp;
#[cfg(verus_keep_ghost)]
#[allow(unused_imports)]
pub(crate) use imp::lemma_vacc;
#[allow(unused_imports)]
use imp::*;
#[allow(unused_imports)]
pub(crate) use imp::{AccModel, AccOf, AccWeight};

verus! {

// ── What the values are added up in ─────────────────────────────────────────

/// The laws of a type an accumulator column adds its values up in: what
/// no values add up to (`zero`), what two of them add up to, one after
/// the other (`plus`), and what is to be added to one to give another
/// (`minus`).
///
/// `plus` is associative, and `zero` added on either side changes
/// nothing (`lemma_plus`); and `minus(a, b)`, added after `b`, gives
/// `a` (`lemma_minus`). That makes the type a group. `plus` need not
/// give the same for `a` and `b` as for `b` and `a`: a column adds its
/// values in the order it holds them.
///
/// The laws are of the type's values as Verus compares them (`==`).
/// For a type of plain data, numbers and structures of them, that is
/// what Rust's `==` compares.
pub trait VGroup: Sized {
    /// What no values add up to.
    spec fn zero() -> Self;

    /// `a` and then `b`, added.
    spec fn plus(a: Self, b: Self) -> Self;

    /// What, added after `b`, gives `a`.
    spec fn minus(a: Self, b: Self) -> Self;

    /// `plus` is associative, and `zero` is its identity.
    proof fn lemma_plus(a: Self, b: Self, c: Self)
        ensures
            Self::plus(Self::plus(a, b), c) == Self::plus(a, Self::plus(b, c)),
            Self::plus(Self::zero(), a) == a,
            Self::plus(a, Self::zero()) == a,
    ;

    /// Adding back what was taken away restores what there was.
    proof fn lemma_minus(a: Self, b: Self)
        ensures
            Self::plus(b, Self::minus(a, b)) == a,
    ;
}

/// `n` copies of `a`, added.
pub open spec fn times<A: VGroup>(n: nat, a: A) -> A
    decreases n,
{
    if n == 0 {
        A::zero()
    } else {
        A::plus(times::<A>((n - 1) as nat, a), a)
    }
}

/// What is added to the same and gives the same is the same: from the
/// laws of a [`VGroup`] alone.
pub proof fn lemma_cancel<A: VGroup>(b: A, t: A, r: A)
    requires
        A::plus(b, t) == A::plus(b, r),
    ensures
        t == r,
{
    imp::lemma_cancel_impl::<A>(b, t, r)
}

/// A type an accumulator column adds its values up in: an accumulator.
/// Its laws are a [`VGroup`]'s, and these are the functions that
/// compute what the laws are of.
pub trait Accum: VGroup + Debug {
    /// What no values add up to.
    fn new_zero() -> (r: Self)
        ensures
            r == Self::zero(),
    ;

    /// Adds `b`, after what there is.
    fn add_acc(&mut self, b: &Self)
        ensures
            *final(self) == Self::plus(*old(self), *b),
    ;

    /// Takes `b` away: leaves what, added after `b`, gives what there
    /// was.
    fn sub_acc(&mut self, b: &Self)
        ensures
            *final(self) == Self::minus(*old(self), *b),
    ;

    /// A copy.
    fn dup(&self) -> (r: Self)
        ensures
            r == *self,
    ;

    /// Adds `b`, `n` times over: what a run of `n` equal values adds.
    /// As it is given here it adds `b` one time after another, which
    /// takes as long as the run is; a type that has a shorter way gives
    /// its own. One whose columns are loaded from bytes that are not
    /// trusted should: a few bytes can be a run of billions of values.
    fn add_times(&mut self, b: &Self, n: usize)
        ensures
            *final(self) == Self::plus(*old(self), times::<Self>(n as nat, *b)),
    {
        let ghost start = *self;
        let mut k: usize = 0;
        proof {
            Self::lemma_plus(start, start, start);
        }
        while k < n
            invariant
                k <= n,
                *self == Self::plus(start, times::<Self>(k as nat, *b)),
            decreases n - k,
        {
            proof {
                Self::lemma_plus(start, times::<Self>(k as nat, *b), *b);
            }
            self.add_acc(b);
            k += 1;
        }
    }
}

// ── What a value adds ───────────────────────────────────────────────────────

/// A value type whose values an accumulator column can add up: the
/// accumulator they are added up in, and what a value adds.
///
/// It is for a client to give of a type of its own
/// ([`Newtype`](super::Newtype)).
pub trait Adds: RleValue {
    /// The accumulator the values are added up in.
    type Acc: Accum;

    /// What the value `v` adds.
    spec fn adds_of(v: Self) -> Self::Acc;

    /// What `v` adds.
    fn acc_of(v: Self) -> (r: Self::Acc)
        ensures
            r == Self::adds_of(v),
    ;
}

/// A value type an accumulator column can hold: a type that says what
/// its values add ([`Adds`]), or an `Option` of one, whose `None` adds
/// nothing. No other type can implement this trait.
pub trait Accumulated: VSpec + for <'a>Form<'a> + seal::AccSeal {
    /// The accumulator the values are added up in.
    type Acc: Accum;

    /// What the value `v` adds.
    spec fn adds(v: <Self as VSpec>::S) -> Self::Acc;

    /// What `v` adds.
    #[doc(hidden)]
    fn to_acc<'a>(v: Get<'a, Self>) -> (r: Self::Acc)
        ensures
            r == Self::adds(v.sv()),
    ;
}

impl<T: Adds> Accumulated for T {
    type Acc = <T as Adds>::Acc;

    open spec fn adds(v: T) -> <T as Adds>::Acc {
        T::adds_of(v)
    }

    #[inline(always)]
    fn to_acc<'a>(v: T) -> <T as Adds>::Acc {
        T::acc_of(v)
    }
}

impl<T: Adds> Accumulated for Option<T> {
    type Acc = <T as Adds>::Acc;

    open spec fn adds(v: Option<T>) -> <T as Adds>::Acc {
        match v {
            Some(x) => T::adds_of(x),
            None => <<T as Adds>::Acc as VGroup>::zero(),
        }
    }

    #[inline(always)]
    fn to_acc<'a>(v: Option<T>) -> <T as Adds>::Acc {
        imp::vaccumulated_to_acc_option_t::<T>(v)
    }
}

// ── What some values add up to ──────────────────────────────────────────────

/// What the values `vs` add up to, in the order they are in.
pub open spec fn accumulated<V: Accumulated>(vs: Seq<<V as VSpec>::S>) -> V::Acc
    decreases vs.len(),
{
    if vs.len() == 0 {
        <V::Acc as VGroup>::zero()
    } else {
        <V::Acc as VGroup>::plus(accumulated::<V>(vs.drop_last()), V::adds(vs.last()))
    }
}

/// What two sequences add up to, one after the other, is what each
/// adds up to, added.
pub proof fn lemma_acc_cat<V: Accumulated>(a: Seq<<V as VSpec>::S>, b: Seq<<V as VSpec>::S>)
    ensures
        accumulated::<V>(a + b) == <V::Acc as VGroup>::plus(accumulated::<V>(a), accumulated::<V>(b)),
    decreases b.len(),
{
    imp::lemma_acc_cat_impl::<V>(a, b)
}

/// `n` copies of a value add up to what it adds, `n` times over.
pub proof fn lemma_acc_run<V: Accumulated>(n: nat, x: <V as VSpec>::S)
    ensures
        accumulated::<V>(Seq::new(n, |_i: int| x)) == times::<V::Acc>(n, V::adds(x)),
    decreases n,
{
    imp::lemma_acc_run_impl::<V>(n, x)
}

/// What the values before position `j` add up to is what those before
/// `i` do, and then those from `i` up to `j`.
pub proof fn lemma_acc_range<V: Accumulated>(vs: Seq<<V as VSpec>::S>, i: int, j: int)
    requires
        0 <= i <= j <= vs.len(),
    ensures
        <V::Acc as VGroup>::plus(accumulated::<V>(vs.take(i)), accumulated::<V>(vs.subrange(i, j))) == accumulated::<V>(
            vs.take(j),
        ),
{
    imp::lemma_acc_range_impl::<V>(vs, i, j)
}

// ── The kind of index, and the column ───────────────────────────────────────

/// The index of an accumulator column: of each slab it counts the
/// values and holds what they add up to.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AccIndex;

impl<V: Accumulated + ColumnValueRef<C>, C: Codec> VKeeps<V, C> for AccIndex {
    /// Every column: nothing overflows.
    open spec fn keeps(vs: Seq<<V as VSpec>::S>) -> bool {
        true
    }
}

impl<V: Accumulated + ColumnValueRef<C>, C: Codec> Index<V, C> for AccIndex {

}

/// A column kept with what its values add up to in a type of the
/// client's own: a [`Column`] with an [`AccIndex`].
pub type AccColumn<V, C = Leb128> = Column<V, C, AccIndex>;

/// What an accumulator column answers from its index.
impl<V: Accumulated + ColumnValueRef<C>, C: Codec> Column<V, C, AccIndex> {
    /// What the values before position `index` add up to; where `index`
    /// is past the end, what all the values do.
    ///
    /// The index gives what the slabs before the one that holds the
    /// position add up to, and of that slab the runs before it are
    /// read: no value of another slab is.
    #[inline(always)]
    pub fn get_prefix(&self, index: usize) -> (r: V::Acc)
        ensures
            index <= self@.len() ==> r == accumulated::<V>(self@.take(index as int)),
            index >= self@.len() ==> r == accumulated::<V>(self@),
    {
        self.get_prefix_impl(index)
    }

    /// What the values through position `index` add up to, the one at
    /// `index` among them; where `index` is at or past the end, what
    /// all the values do. It is `get_prefix(index + 1)`.
    #[inline(always)]
    pub fn get_total(&self, index: usize) -> (r: V::Acc)
        ensures
            index < self@.len() ==> r == accumulated::<V>(self@.take(index + 1)),
            index >= self@.len() ==> r == accumulated::<V>(self@),
    {
        self.get_total_impl(index)
    }

    /// What the values at the positions from `start` up to, and not
    /// including, `end` add up to: of those of them that the column
    /// has, and nothing where there are none, as where `end` is not
    /// after `start`.
    ///
    /// It is what, added after what the values before `start` add up
    /// to, gives what those before `end` do, found in one pass.
    #[inline(always)]
    pub fn sum_range(&self, range: Range<usize>) -> (r: V::Acc)
        ensures
            range.start <= range.end <= self@.len() ==> r == accumulated::<V>(self@.subrange(range.start as int, range.end as int)),
            range.start <= self@.len() <= range.end ==> r == accumulated::<V>(self@.subrange(range.start as int, self@.len() as int)),
            range.start >= range.end || range.start >= self@.len() ==> r == <V::Acc as VGroup>::zero(),
    {
        self.sum_range_impl(range)
    }
}

} // verus!
