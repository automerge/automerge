//! Prefix columns of the verified public API: columns kept with the
//! sums of their values, which answer what the values before a position
//! add up to without reading them all.
//!
//! A prefix column is a [`Column`] kept with a [`SumIndex`]:
//! of each slab the index holds, besides the count of its values, what
//! they add up to (`summed`). What a column of the kind says of its
//! values, and its bytes when saved, are a plain column's. What
//! differs is that there is such a column only of values whose sums
//! fit the type they are kept in (`sums_fit`): a load refuses a saved
//! column of other values, and an edit that would leave other values
//! panics.
//!
//! The sums a prefix column answers with are of the type it keeps them
//! in, and what is proved of them is said of integers: the sum before
//! a position ([`get_prefix`](Column::get_prefix)) *is* what
//! the values before it add up to, for the sums of a prefix column fit.
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([`verified`](crate::verified#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`prefix/imp.rs`), which has
//! its contract word for word and is proved there.

use super::kind::{Kind, VIndex, VKeeps};
#[cfg(verus_keep_ghost)]
use super::sums::{summed, sums_fit};
use super::sums::{PrefixValue, SumModel, SumOf, SumWeight, UnsignedPrefix, VSum};
use super::{Codec, Column, ColumnValueRef};
#[cfg(verus_keep_ghost)]
use super::{Get, Held, VGet};
#[cfg(verus_keep_ghost)]
use crate::btree::{PrefixAggregate, SlabAggregate};
#[cfg(verus_keep_ghost)]
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::{Slabs, Weigh};
#[cfg(verus_keep_ghost)]
use crate::index::IndexOps;
#[cfg(verus_keep_ghost)]
use crate::iter_core::Decoders;
#[cfg(verus_keep_ghost)]
use crate::prefix_core::{before, PrefixWeight};
#[cfg(verus_keep_ghost)]
use crate::prefix_search::PrefixOrd;
use crate::Leb128;
use vstd::prelude::*;

mod imp;
#[allow(unused_imports)]
use imp::*;

verus! {

/// The index of a prefix column: of each slab it counts the values and
/// holds what they add up to.
pub struct SumIndex;

impl<V: PrefixValue + ColumnValueRef<C>, C: Codec> VKeeps<V, C> for SumIndex {
    /// The columns whose sums fit the type they are kept in.
    open spec fn keeps(vs: Seq<<V as super::VSpec>::S>) -> bool {
        sums_fit::<V>(vs)
    }
}

impl<V: PrefixValue + ColumnValueRef<C>, C: Codec> VIndex<V, C> for SumIndex {

}

/// A column kept with the sums of its values: a [`Column`]
/// with a [`SumIndex`].
pub type PrefixColumn<V, C = Leb128> = Column<V, C, SumIndex>;

/// What a prefix column answers from its sums.
impl<V: PrefixValue + ColumnValueRef<C>, C: Codec> Column<V, C, SumIndex> {
    /// What the values before position `index` add up to; where `index`
    /// is past the end, what all the values do.
    ///
    /// The index gives the sum of the slabs before the one that holds
    /// the position, and of that slab the runs before it are read: no
    /// value of another slab is.
    #[inline(always)]
    pub fn get_prefix(&self, index: usize) -> (r: V::Sum)
        ensures
            index <= self@.len() ==> r.as_int() == summed::<V>(self@.take(index as int)),
            index >= self@.len() ==> r.as_int() == summed::<V>(self@),
    {
        self.get_prefix_impl(index)
    }

    /// What the values through position `index` add up to, the one at
    /// `index` among them; where `index` is at or past the end, what
    /// all the values do. It is `get_prefix(index + 1)`.
    #[inline(always)]
    pub fn get_total(&self, index: usize) -> (r: V::Sum)
        ensures
            index < self@.len() ==> r.as_int() == summed::<V>(self@.take(index + 1)),
            index >= self@.len() ==> r.as_int() == summed::<V>(self@),
    {
        self.get_total_impl(index)
    }

    /// What the values at the positions from `start` up to, and not
    /// including, `end` add up to: of those of them that the column
    /// has, and nothing where there are none, as where `end` is not
    /// after `start`.
    ///
    /// It is the sum before `end` less the sum before `start`, found in
    /// one pass.
    #[inline(always)]
    pub fn sum_range(&self, start: usize, end: usize) -> (r: V::Sum)
        ensures
            start <= end <= self@.len() ==> r.as_int() == summed::<V>(self@.subrange(start as int, end as int)),
            start <= self@.len() <= end ==> r.as_int() == summed::<V>(
                self@.subrange(start as int, self@.len() as int),
            ),
            start >= end || start >= self@.len() ==> r.as_int() == 0,
    {
        self.sum_range_impl(start, end)
    }
}

/// What a prefix column of sums that are never negative answers: where
/// its sums reach a target.
impl<V: PrefixValue + ColumnValueRef<C>, C: Codec> Column<V, C, SumIndex> where V::Sum: UnsignedPrefix {
    /// The first position that the values before it add up to `target`
    /// or more: no earlier position's do, and where the result is a
    /// position of the column or its end, that one's do. Where the
    /// values of the whole column add up to less, it is one past the
    /// column's end, `len() + 1`.
    ///
    /// The index finds the slab where the sums reach the target, and of
    /// that slab the runs are read: no value of another slab is.
    #[inline(always)]
    pub fn get_index_for_prefix(&self, target: V::Sum) -> (r: usize)
        ensures
            r <= self@.len() + 1,
            forall|i: int| 0 <= i < r ==> summed::<V>(#[trigger] self@.take(i)) < target.as_int(),
            r <= self@.len() ==> target.as_int() <= summed::<V>(self@.take(r as int)),
    {
        self.get_index_for_prefix_impl(target)
    }

    /// The first position that the values through it add up to
    /// `target` or more: no earlier position's do. Where the values of
    /// the whole column add up to less, it is the column's end,
    /// `len()`. It is `get_index_for_prefix(target)` less one, and zero
    /// for a target of zero.
    #[inline(always)]
    pub fn get_index_for_total(&self, target: V::Sum) -> (r: usize)
        ensures
            r <= self@.len(),
            forall|j: int| 0 < j <= r ==> summed::<V>(#[trigger] self@.take(j)) < target.as_int(),
            r < self@.len() ==> target.as_int() <= summed::<V>(self@.take(r + 1)),
    {
        self.get_index_for_total_impl(target)
    }
}

} // verus!
