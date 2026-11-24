//! The kinds of index a [`Column`](super::Column) is
//! kept with.
//!
//! A column's values lie in slabs, and an index holds a weight of each
//! slab. For every column the weight counts the slab's values, which is
//! what a read or an edit at a position needs. A kind of index says what
//! else the weight holds, and so what else the column can answer without
//! reading every slab.
//!
//! The kind is the third parameter of a column's type. A plain column's
//! is [`LenIndex`], which is the parameter's default: `Column<V>`
//! and `Column<V, C>` are plain columns. [`LenBitIndex`] keeps
//! the same counts in a Fenwick tree in place of a B-tree. What a column
//! says of its values does not depend on the kind, nor do its bytes when
//! saved.
//! What does is which columns there are: an index is kept for the
//! columns whose weights it can hold (`VKeeps::keeps`), which for a
//! plain column is every one.
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([`verified`](crate::verified#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`kind/imp.rs`), which has
//! its contract word for word and is proved there.

use super::seal;
use super::{Codec, ColumnValueRef};
#[cfg(verus_keep_ghost)]
use super::{Held, VGet};
#[cfg(verus_keep_ghost)]
use crate::btree::SlabAggregate;
#[cfg(verus_keep_ghost)]
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::{Slabs, Weigh};
#[cfg(verus_keep_ghost)]
use crate::index::{BitIndex, IndexOps};
#[cfg(verus_keep_ghost)]
use crate::weights::{CopyWeigh, LoadWeigh};
use crate::Leb128;
use vstd::prelude::*;

mod imp;
#[allow(unused_imports)]
use imp::*;
#[allow(unused_imports)]
pub(crate) use imp::{KCore, Kind};

verus! {

/// The columns of `V` a kind of index is kept for.
pub trait VKeeps<V: ColumnValueRef<C>, C: Codec = Leb128>: Sized {
    /// An index of this kind can be kept for a column of the values
    /// `vs`: what it holds of them, it can hold. A column of the kind
    /// always has such values; a load refuses bytes that are a saved
    /// column of other values; and an edit that would make a column of
    /// other values panics.
    spec fn keeps(vs: Seq<V::S>) -> bool;
}

/// A kind of index that a [`Column`](super::Column) of
/// `V` can be kept with, its numbers written with codec `C`. The kinds
/// are this crate's: no other type can implement this trait.
#[allow(private_bounds)]
pub trait VIndex<V: ColumnValueRef<C>, C: Codec = Leb128>: Kind<V, C> {

}

/// The index of a plain column: of each slab it counts the values.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LenIndex;

impl<V: ColumnValueRef<C>, C: Codec> VKeeps<V, C> for LenIndex {
    /// Every column: a count of a slab's values can always be held.
    open spec fn keeps(vs: Seq<V::S>) -> bool {
        true
    }
}

impl<V: ColumnValueRef<C>, C: Codec> VIndex<V, C> for LenIndex {

}

/// The index of a plain column kept as a Fenwick tree (a binary indexed
/// tree) of the slabs' counts
/// (`BitIndex<usize>`), in place of the B-tree
/// of [`LenIndex`]. A column says the same of its values with
/// either, and saves the same bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LenBitIndex;

impl<V: ColumnValueRef<C>, C: Codec> VKeeps<V, C> for LenBitIndex {
    /// Every column, as for [`LenIndex`].
    open spec fn keeps(vs: Seq<V::S>) -> bool {
        true
    }
}

impl<V: ColumnValueRef<C>, C: Codec> VIndex<V, C> for LenBitIndex {

}

} // verus!
