//! Ranges of one [`Column`] copied into another at several places
//! in one pass: [`Column::copy_ranges`].
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([Reviewing the specification](crate#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`copy/imp.rs`), which has
//! its contract word for word and is proved there.

use super::model::Model;
#[cfg(verus_keep_ghost)]
use super::{after_call, Held, VGet};
use super::{Codec, Column, ColumnValueRef, Index};
#[cfg(verus_keep_ghost)]
use crate::splices::{applied, in_order, ranges_ascend, splices_do};
use crate::Splice;
use vstd::prelude::*;

mod imp;
#[allow(unused_imports)]
use imp::*;

verus! {

impl<V: ColumnValueRef<C>, C: Codec, K: Index<V, C>> Column<V, C, K> {
    /// Copy ranges of `src` into the column, at several places in one
    /// pass. Each splice `sp` of `splices`, one after another, deletes
    /// `sp.delete` values at `sp.pos` and puts there the values of `src`
    /// in `sp.range`, cut to `src`'s length (none, where the range is
    /// reversed); `sp.pos` is a place in the column as it was. The column
    /// becomes `applied(self@, src@, splices@)`.
    ///
    /// The splices ascend in the column and stay within it (`in_order`),
    /// and their ranges ascend in `src` (`ranges_ascend`). The column
    /// counts an edit more if a splice deletes a value or copies one
    /// (`splices_do`), and none if not.
    ///
    /// `src` is taken: where most of the column comes from it, the column
    /// keeps `src`'s slabs and puts its own values back around them, as
    /// [`Column::copy_ranges`](crate::Column::copy_ranges) does.
    ///
    /// # Panics
    ///
    /// Where splices are out of order or out of bounds, which the
    /// `requires` rules out of a verified caller; and
    /// where the column's kind of index is not kept for the values it
    /// would hold (`VKeeps::keeps`), as for `splice`.
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
