//! The implementation of `verified::kind` and its proofs: what its
//! specification (`kind.rs`) says. Each function named `..._impl` here has
//! the contract of the function there that calls it, word for word; a
//! function named after a trait, a method and a type (`vpack_pack_u64`)
//! is the body of that method in that type's impl of the trait, with the
//! trait's contract said of the type.

#![allow(unused_imports)]

use super::*;

use crate::btree::{SlabAggregate, SlabBTree};
#[cfg(verus_keep_ghost)]
use crate::column::Slab;
use crate::column_core::ColumnCore;
use crate::cursor::Weigh;
#[cfg(verus_keep_ghost)]
use crate::cursor::{all_ok, vals_of, Slabs};
use crate::index::{BitIndex, IndexOps};
use crate::weights::{CopyWeigh, LenModel, LoadWeigh};
use crate::Leb128;
use vstd::prelude::*;

verus! {

/// The core of a column of `V` kept with an index of kind `K`.
pub(crate) type KCore<V, C, K> = ColumnCore<
    <V as seal::Sealed<C>>::E,
    <K as Kind<V, C>>::W,
    <K as Kind<V, C>>::I,
    <K as Kind<V, C>>::G,
>;

/// What a kind of index is to the verified core, for columns of `V`
/// under codec `C`.
pub(crate) trait Kind<V: ColumnValueRef<C>, C: Codec>: VKeeps<V, C> {
    /// What the index holds of a slab.
    type W: SlabAggregate;

    /// The index of those: a B-tree of them, or a Fenwick tree.
    type I: IndexOps<Self::W>;

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

    /// A load weighs each slab as it is (`Weigh::compute`): it does not
    /// accumulate the weights as the runs pass.
    proof fn lemma_computes()
        ensures
            !<Self::G as LoadWeigh<'static, V::E, V::X, Self::W>>::accumulates(),
    ;

    /// What the check after a splice may know of the column of `core`
    /// before it (`Weigh::Pre`): of a prefix column too long for its sums
    /// to be sure to fit, what it adds up to.
    fn pre(core: &KCore<V, C, Self>) -> (r: <Self::G as Weigh<V::E, Self::W>>::Pre)
        ensures
            <Self::G as Weigh<V::E, Self::W>>::pre_ok(r, core.slabs_spec()),
    ;
}

impl<V: ColumnValueRef<C>, C: Codec> Kind<V, C> for LenIndex {
    type W = usize;

    type I = SlabBTree<usize>;

    type G = LenModel;

    proof fn lemma_refuses(vs: Seq<Held<V, C>>) {
    }

    proof fn lemma_kept(s: Seq<Slab<<V::E as Slabs>::Tail>>) {
    }

    proof fn lemma_computes() {
    }

    #[inline(always)]
    fn pre(_core: &KCore<V, C, LenIndex>) {
    }
}

impl<V: ColumnValueRef<C>, C: Codec> Kind<V, C> for LenBitIndex {
    type W = usize;

    type I = BitIndex<usize>;

    type G = LenModel;

    proof fn lemma_refuses(vs: Seq<Held<V, C>>) {
    }

    proof fn lemma_kept(s: Seq<Slab<<V::E as Slabs>::Tail>>) {
    }

    proof fn lemma_computes() {
    }

    #[inline(always)]
    fn pre(_core: &KCore<V, C, LenBitIndex>) {
    }
}

} // verus!
