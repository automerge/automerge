//! The implementation of `verified::copy` and its proofs: what its
//! specification (`copy.rs`) says. Each function named `..._impl` here has
//! the contract of the function there that calls it, word for word; a
//! function named after a trait, a method and a type (`vpack_pack_u64`)
//! is the body of that method in that type's impl of the trait, with the
//! trait's contract said of the type.

#![allow(unused_imports)]

use super::*;

#[cfg(verus_keep_ghost)]
use crate::splices::{applied, in_order, lemma_applied_map, ranges_ascend, splices_do};
use crate::splices::{does_anything, normalize_slice};
use crate::Splice;
use std::ops::Range;
use vstd::prelude::*;

verus! {

/// Splices applied to values, as specifications have them, are the
/// values of the splices applied.
pub(super) proof fn lemma_svs_applied<V: VValue<C>, C: VCodec>(d: Seq<Held<V, C>>, s: Seq<Held<V, C>>, sps: Seq<Splice>)
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
pub(super) proof fn lemma_applied_none<T>(d: Seq<T>, s: Seq<T>, sps: Seq<Splice>)
    requires
        sps.len() == 0,
    ensures
        applied(d, s, sps) == d,
{
    assert(applied(d, s, sps) =~= d);
}

impl<V: VValue<C>, C: VCodec, K: VIndex<V, C>> VerifiedColumn<V, C, K> {
    pub(super) fn copy_ranges_impl(&mut self, src: Self, splices: &[Splice])
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
