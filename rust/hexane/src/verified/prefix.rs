//! Prefix columns of the verified public API: columns kept with the
//! sums of their values.
//!
//! A prefix column is a [`VerifiedColumn`] kept with a [`SumIndex`]:
//! of each slab the index holds, besides the count of its values, what
//! they add up to (`summed`). What a column of the kind says of its
//! values, and its bytes when saved, are a plain column's. What
//! differs is that there is such a column only of values whose sums
//! fit the type they are kept in (`sums_fit`): a load refuses a saved
//! column of other values, and an edit that would leave other values
//! panics.

use super::kind::{Kind, VIndex, VKeeps};
#[cfg(verus_keep_ghost)]
use super::sums::{lemma_svs_slabs, sums_fit};
use super::sums::{SumModel, SumWeight, VSummed};
#[cfg(verus_keep_ghost)]
use super::Held;
use super::{VCodec, VValue, VerifiedColumn};
#[cfg(verus_keep_ghost)]
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::Slabs;
use crate::Leb128;
use vstd::prelude::*;

verus! {

/// The index of a prefix column: of each slab it counts the values and
/// holds what they add up to.
pub struct SumIndex;

impl<V: VSummed + VValue<C>, C: VCodec> VKeeps<V, C> for SumIndex {
    /// The columns whose sums fit the type they are kept in.
    open spec fn keeps(vs: Seq<<V as super::VSpec>::S>) -> bool {
        sums_fit::<V>(vs)
    }
}

impl<V: VSummed + VValue<C>, C: VCodec> Kind<V, C> for SumIndex {
    type W = SumWeight<V::Sum>;

    type G = SumModel<V, C>;

    proof fn lemma_refuses(vs: Seq<Held<V, C>>) {
    }

    proof fn lemma_kept(s: Seq<Slab<<V::E as Slabs>::Tail>>) {
        lemma_svs_slabs::<V, C>(s);
    }
}

impl<V: VSummed + VValue<C>, C: VCodec> VIndex<V, C> for SumIndex {

}

/// A column kept with the sums of its values: a [`VerifiedColumn`]
/// with a [`SumIndex`].
pub type VerifiedPrefixColumn<V, C = Leb128> = VerifiedColumn<V, C, SumIndex>;

} // verus!
