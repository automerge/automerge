//! The implementation of `verified::delta::copy` and its proofs: what its
//! specification (`copy.rs`) says. Each function named `..._impl` here has
//! the contract of the function there that calls it, word for word; a
//! function named after a trait, a method and a type (`vpack_pack_u64`)
//! is the body of that method in that type's impl of the trait, with the
//! trait's contract said of the type.

#![allow(unused_imports)]

#[cfg(verus_keep_ghost)]
use super::super::{all_dom, in_dom, vals};
use super::*;

use crate::delta::dcopy;
#[cfg(verus_keep_ghost)]
use crate::splices::{
    applied, in_order, lemma_applied_all, lemma_applied_map, ranges_ascend, splices_do,
};
use crate::splices::{does_anything, normalize_slice};
#[cfg(verus_keep_ghost)]
use crate::verified::after_call;
use crate::verified::codec::Codec;
use crate::verified::model::NoVisit;
use crate::Splice;
use std::ops::Range;
use vstd::prelude::*;

verus! {

/// No splices leave the destination as it was.
pub(super) proof fn lemma_applied_none<V>(d: Seq<V>, s: Seq<V>, sps: Seq<Splice>)
    requires
        sps.len() == 0,
    ensures
        applied(d, s, sps) == d,
{
    assert(applied(d, s, sps) =~= d);
}

/// What a delta column of `T` keeps, with splices in order applied to it
/// from another: the values of the splices applied to the values, and
/// only values a delta column of `T` can keep.
pub(super) proof fn lemma_applied_kept<T: VDelta>(d: Seq<Option<i64>>, s: Seq<Option<i64>>, sps: Seq<Splice>)
    requires
        in_order(sps, d.len() as int),
        all_dom::<T>(d),
        all_dom::<T>(s),
    ensures
        all_dom::<T>(applied(d, s, sps)),
        vals::<T>(applied(d, s, sps)) == applied(vals::<T>(d), vals::<T>(s), sps),
{
    let p = |x: Option<i64>| in_dom::<T>(x);
    assert forall|i: int| 0 <= i < d.len() implies p(#[trigger] d[i]) by {
        assert(in_dom::<T>(d[i]));
    }
    assert forall|i: int| 0 <= i < s.len() implies p(#[trigger] s[i]) by {
        assert(in_dom::<T>(s[i]));
    }
    lemma_applied_all(d, s, sps, p);
    let r = applied(d, s, sps);
    assert forall|i: int| 0 <= i < r.len() implies in_dom::<T>(#[trigger] r[i]) by {
        assert(p(r[i]));
    }
    lemma_applied_map(d, s, sps, |x: Option<i64>| T::of(x));
    assert(vals::<T>(d) =~= d.map_values(|x: Option<i64>| T::of(x)));
    assert(vals::<T>(s) =~= s.map_values(|x: Option<i64>| T::of(x)));
    assert(vals::<T>(r) =~= r.map_values(|x: Option<i64>| T::of(x)));
}

impl<T: DeltaValue<C>, C: Codec> DeltaColumn<T, C> {
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
        proof {
            use_type_invariant(&*self);
            use_type_invariant(&src);
        }
        let ghost (k0, ks) = (self.kept(), src.kept());
        let src_len = src.len();
        let changed = does_anything(splices, src_len);
        let list = normalize_slice(splices, src_len, self.len());
        // the column's values are kept in bounds while it is whole: a
        // panic part way leaves an empty one
        let mut me = Self::new();
        core::mem::swap(self, &mut me);
        let counter = me.core.counter();
        let mut dst = T::to_edit(me.core);
        let mut from = T::to_edit(src.core);
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
            let invert = dst.invert_pays(&from, ranges.as_slice(), sps.len(), 1);
            let mut f = NoVisit::<T::L> { seen: Ghost(Seq::empty()) };
            dcopy::copy(&mut dst, &list, &mut from, invert, T::lo(), T::hi(), &mut f);
        } else {
            proof {
                list.lemma_applied(k0, ks);
                lemma_applied_none(k0, ks, list@);
            }
        }
        let mut store = T::of_edit(dst);
        // the shared cursor moves the count as it writes, by a number
        // that depends on the copy: this makes it one an edit
        store.set_counter(
            if changed {
                counter.wrapping_add(1)
            } else {
                counter
            },
        );
        proof {
            lemma_applied_kept::<T>(k0, ks, splices@);
        }
        *self = DeltaColumn { core: store };
    }
}

} // verus!
