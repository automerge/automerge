//! Ranges of one [`VerifiedColumn`] copied into another at several places
//! in one pass: [`VerifiedColumn::copy_ranges`].

use super::model::Model;
#[cfg(verus_keep_ghost)]
use super::{after_call, Held, VGet};
use super::{VCodec, VIndex, VValue, VerifiedColumn};
#[cfg(verus_keep_ghost)]
use crate::splices::{applied, in_order, lemma_applied_map, ranges_ascend, splices_do};
use crate::splices::{does_anything, normalize_slice};
use crate::Splice;
use std::ops::Range;
use vstd::prelude::*;

verus! {

/// Splices applied to values, as specifications have them, are the
/// values of the splices applied.
proof fn lemma_svs_applied<V: VValue<C>, C: VCodec>(d: Seq<Held<V, C>>, s: Seq<Held<V, C>>, sps: Seq<Splice>)
    requires
        in_order(sps, d.len() as int),
    ensures
        <Held<V, C> as VGet>::svs(applied(d, s, sps)) == applied(
            <Held<V, C> as VGet>::svs(d),
            <Held<V, C> as VGet>::svs(s),
            sps,
        ),
{
    let f = |h: Held<V, C>| h.sv();
    let r = applied(d, s, sps);
    lemma_applied_map(d, s, sps, f);
    <Held<V, C> as VGet>::lemma_svs(d);
    <Held<V, C> as VGet>::lemma_svs(s);
    <Held<V, C> as VGet>::lemma_svs(r);
    assert(<Held<V, C> as VGet>::svs(d) =~= d.map_values(f));
    assert(<Held<V, C> as VGet>::svs(s) =~= s.map_values(f));
    assert(<Held<V, C> as VGet>::svs(r) =~= r.map_values(f));
}

/// No splices leave the destination as it was.
proof fn lemma_applied_none<T>(d: Seq<T>, s: Seq<T>, sps: Seq<Splice>)
    requires
        sps.len() == 0,
    ensures
        applied(d, s, sps) == d,
{
    assert(applied(d, s, sps) =~= d);
}

impl<V: VValue<C>, C: VCodec, K: VIndex<V, C>> VerifiedColumn<V, C, K> {
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
    /// `requires` rules out of a verified caller (in the old words); and
    /// where the column's kind of index is not kept for the values it
    /// would hold (`VKeeps::keeps`), as for `splice`.
    pub fn copy_ranges(&mut self, src: Self, splices: &[Splice])
        requires
            in_order(splices@, old(self)@.len() as int),
            ranges_ascend(splices@),
        ensures
            final(self)@ == applied(old(self)@, src@, splices@),
            final(self).edits() == after_call(old(self).edits(), splices_do(splices@, src@.len() as int)),
    {
        hide(applied);
        hide(in_order);
        let mut src = src;
        let ghost (d, s) = (self.core@, src.core@);
        proof {
            <Held<V, C> as VGet>::lemma_svs(d);
            <Held<V, C> as VGet>::lemma_svs(s);
        }
        let counter = self.core.counter();
        let src_len = src.core.len();
        let changed = does_anything(splices, src_len);
        let list = normalize_slice(splices, src_len, self.core.len());
        let sps = list.splices();
        if sps.len() > 0 {
            let mut ranges: Vec<Range<usize>> = Vec::new();
            let mut i: usize = 0;
            while i < sps.len()
                invariant
                    i <= sps.len(),
                decreases sps.len() - i,
            {
                ranges.push(sps[i].range.start..sps[i].range.end);
                i += 1;
            }
            // the old API's measure of which way pays (its
            // `COPY_INVERT_FACTOR` is 1)
            let invert = self.core.invert_pays(&src.core, ranges.as_slice(), sps.len(), 1);
            let mut f = <V::E as Model<V::X>>::blind();
            self.core.copy::<V::X, _>(&list, &mut src.core, invert, &mut f);
        } else {
            proof {
                // no splice that does anything: the column as it was
                list.lemma_applied(d, s);
                lemma_applied_none(d, s, list@);
            }
        }
        proof {
            lemma_svs_applied::<V, C>(d, s, splices@);
        }
        self.count_call(counter, changed);
    }
}

} // verus!
