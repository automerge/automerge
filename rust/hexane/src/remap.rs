//! Re-encoding a column through a function ([`Column::remap`]), verified
//! with Verus.
//!
//! `ColumnCore::remap` reads the column a run at a time
//! (`ICore::next_run`), has the caller's function map each run's value
//! (`Mapping`), and appends a run of what it gives to the encoding's
//! verified encoder (`Encodes`: the RLE encoder `rle::enc::REncoder`, the
//! bool encoder `bool_enc::BEncoder`), whose slabs make the new column
//! (`ColumnCore::from_cut`). So the new column holds, at each position,
//! what the function gave for the value there, called once per run.
//!
//! Trusted: nothing of the function (`Column::remap`'s `Remapping`), whose
//! results the specification names but does not constrain.
//!
//! [`Column::remap`]: crate::Column::remap

use crate::column::Slab;
use crate::cursor::Slabs;
#[cfg(verus_keep_ghost)]
use crate::cursor::{all_ok, vals_of};
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// An encoding's verified encoder, as `remap` drives it: values
/// appended, then cut into well-formed slabs of the encoding's model `E`.
pub(crate) trait Encodes<E: Slabs>: Sized {
    /// What is appended: a value in the encoder's own form.
    type In;

    /// What the encoder keeps while appending.
    spec fn e_inv(&self) -> bool;

    /// The values appended, as the slabs will hold them.
    spec fn e_values(&self) -> Seq<E::V>;

    /// The value `x` stands for.
    spec fn val(x: Self::In) -> E::V;

    /// An empty encoder, cutting slabs of the segment budget `max`.
    fn with_budget(max: usize) -> (e: Self)
        requires
            max >= 2,
        ensures
            e.e_inv(),
            e.e_values().len() == 0,
    ;

    /// Append `n` copies of `x`.
    fn put(&mut self, x: Self::In, n: usize)
        requires
            old(self).e_inv(),
        ensures
            final(self).e_inv(),
            final(self).e_values() == old(self).e_values() + Seq::new(n as nat, |_i: int| Self::val(x)),
    ;

    /// The slabs: well-formed, holding the values appended.
    fn take(&mut self) -> (v: Vec<Slab<E::Tail>>)
        requires
            old(self).e_inv(),
        ensures
            all_ok::<E>(v@),
            vals_of::<E>(v@) == old(self).e_values(),
    ;
}

/// The caller's function, from a value read to one to append: `maps(v,
/// w)` is that it gave `w` for `v`, and nothing more is known of it.
pub(crate) trait Mapping<V, W> {
    spec fn maps(&self, v: V, w: W) -> bool;

    fn apply(&self, v: V) -> (w: W)
        ensures
            self.maps(v, w),
    ;
}

} // verus!
