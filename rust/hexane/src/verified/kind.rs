//! The kinds of index a [`VerifiedColumn`](super::VerifiedColumn) is
//! kept with.
//!
//! A column's values lie in slabs, and an index holds a weight of each
//! slab. For every column the weight counts the slab's values, which is
//! what a read or an edit at a position needs. A kind of index says what
//! else the weight holds, and so what else the column can answer without
//! reading every slab.
//!
//! The kind is the third parameter of a column's type. A plain column's
//! is [`LenIndex`], which is the parameter's default: `VerifiedColumn<V>`
//! and `VerifiedColumn<V, C>` are plain columns. What a column says of
//! its values does not depend on the kind, nor do its bytes when saved.
//! What does is which columns there are: an index is kept for the
//! columns whose weights it can hold ([`VKeeps::keeps`]), which for a
//! plain column is every one.

use super::seal;
#[cfg(verus_keep_ghost)]
use super::{Held, VGet};
use super::{VCodec, VValue};
use crate::btree::{SlabAggregate, SlabBTree};
#[cfg(verus_keep_ghost)]
use crate::column::Slab;
use crate::column_core::ColumnCore;
#[cfg(verus_keep_ghost)]
use crate::cursor::{all_ok, vals_of, Slabs, Weigh};
use crate::weights::{CopyWeigh, LenModel, LoadWeigh};
use crate::Leb128;
use vstd::prelude::*;

verus! {

/// The core of a column of `V` kept with an index of kind `K`.
pub(crate) type KCore<V, C, K> = ColumnCore<
    <V as seal::Sealed<C>>::E,
    <K as Kind<V, C>>::W,
    SlabBTree<<K as Kind<V, C>>::W>,
    <K as Kind<V, C>>::G,
>;

/// The columns of `V` a kind of index is kept for.
pub trait VKeeps<V: VValue<C>, C: VCodec = Leb128>: Sized {
    /// An index of this kind can be kept for a column of the values
    /// `vs`: what it holds of them, it can hold. A column of the kind
    /// always has such values; a load refuses bytes that are a saved
    /// column of other values; and an edit that would make a column of
    /// other values panics.
    spec fn keeps(vs: Seq<V::S>) -> bool;
}

/// What a kind of index is to the verified core, for columns of `V`
/// under codec `C`.
pub(crate) trait Kind<V: VValue<C>, C: VCodec>: VKeeps<V, C> {
    /// What the index holds of a slab.
    type W: SlabAggregate;

    /// How a slab is weighed, and what the weights say of a column.
    type G: CopyWeigh<V::E, Self::W> + for <'a>LoadWeigh<'a, V::E, V::X, Self::W>;

    /// A load refuses no column the kind keeps.
    proof fn lemma_refuses(vs: Seq<Held<V, C>>)
        requires
            Self::keeps(<Held<V, C> as VGet>::svs(vs)),
        ensures
            !<Self::G as LoadWeigh<'static, V::E, V::X, Self::W>>::may_refuse(vs),
    ;

    /// The columns the weights are kept for are columns the kind keeps.
    proof fn lemma_kept(s: Seq<Slab<<V::E as Slabs>::Tail>>)
        requires
            all_ok::<V::E>(s),
            <Self::G as Weigh<V::E, Self::W>>::dom_ok(s),
        ensures
            Self::keeps(<Held<V, C> as VGet>::svs(vals_of::<V::E>(s))),
    ;
}

/// A kind of index that a [`VerifiedColumn`](super::VerifiedColumn) of
/// `V` can be kept with, its numbers written with codec `C`. The kinds
/// are this crate's: no other type can implement this trait.
#[allow(private_bounds)]
pub trait VIndex<V: VValue<C>, C: VCodec = Leb128>: Kind<V, C> {

}

/// The index of a plain column: of each slab it counts the values.
pub struct LenIndex;

impl<V: VValue<C>, C: VCodec> VKeeps<V, C> for LenIndex {
    /// Every column: a count of a slab's values can always be held.
    open spec fn keeps(vs: Seq<V::S>) -> bool {
        true
    }
}

impl<V: VValue<C>, C: VCodec> Kind<V, C> for LenIndex {
    type W = usize;

    type G = LenModel;

    proof fn lemma_refuses(vs: Seq<Held<V, C>>) {
    }

    proof fn lemma_kept(s: Seq<Slab<<V::E as Slabs>::Tail>>) {
    }
}

impl<V: VValue<C>, C: VCodec> VIndex<V, C> for LenIndex {

}

} // verus!
