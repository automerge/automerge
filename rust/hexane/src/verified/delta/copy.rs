//! Ranges of one [`VerifiedDeltaColumn`] copied into another at several
//! places in one pass: [`VerifiedDeltaColumn::copy_ranges`].
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([`verified`](crate::verified#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`copy/imp.rs`), which has
//! its contract word for word and is proved there.

#[cfg(verus_keep_ghost)]
use super::VDelta;
use super::{VDeltaValue, VerifiedDeltaColumn};
#[cfg(verus_keep_ghost)]
use crate::splices::{applied, in_order, ranges_ascend, splices_do};
#[cfg(verus_keep_ghost)]
use crate::verified::after_call;
use crate::verified::codec::VCodec;
use crate::Splice;
use vstd::prelude::*;

mod imp;
#[allow(unused_imports)]
use imp::*;

verus! {

impl<T: VDeltaValue<C>, C: VCodec> VerifiedDeltaColumn<T, C> {
    /// Copy ranges of `src` into the column, at several places in one
    /// pass, as [`VerifiedColumn::copy_ranges`](crate::verified::VerifiedColumn::copy_ranges)
    /// does: the column becomes `applied(self@, src@, splices@)`, and
    /// counts an edit more if a splice deletes a value or copies one
    /// (`splices_do`).
    ///
    /// Where most of the column comes from `src`, the column keeps
    /// `src`'s slabs and puts its own values back around them, as
    /// [`DeltaColumn::copy_ranges`](crate::DeltaColumn::copy_ranges)
    /// does.
    ///
    /// # Panics
    ///
    /// Where splices are out of order or out of bounds, which the
    /// `requires` rules out of a verified caller (in the old words); and
    /// where the column's values would span 2^63 or more, as for
    /// `splice`. The column is then left an empty one.
    #[inline(always)]
    pub fn copy_ranges(&mut self, src: Self, splices: &[Splice])
        requires
            in_order(splices@, old(self)@.len() as int),
            ranges_ascend(splices@),
        ensures
            final(self)@ == applied(old(self)@, src@, splices@),
            final(self).edits() == after_call(old(self).edits(), splices_do(splices@, src@.len() as int)),
    {
        self.copy_ranges_impl(src, splices)
    }
}

} // verus!
