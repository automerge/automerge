//! Ranges of one [`VerifiedDeltaColumn`] copied into another at several
//! places in one pass: [`VerifiedDeltaColumn::copy_ranges`].

#[cfg(verus_keep_ghost)]
use super::{all_dom, in_dom, vals, VDelta};
use super::{VDeltaValue, VerifiedDeltaColumn};
use crate::delta::dcopy;
#[cfg(verus_keep_ghost)]
use crate::splices::{
    applied, in_order, lemma_applied_all, lemma_applied_map, ranges_ascend, splices_do,
};
use crate::splices::{does_anything, normalize_slice};
#[cfg(verus_keep_ghost)]
use crate::verified::after_edit;
use crate::verified::codec::VCodec;
use crate::verified::model::NoVisit;
use crate::Splice;
use std::ops::Range;
use vstd::prelude::*;

verus! {

/// No splices leave the destination as it was.
proof fn lemma_applied_none<V>(d: Seq<V>, s: Seq<V>, sps: Seq<Splice>)
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
proof fn lemma_applied_kept<T: VDelta>(d: Seq<Option<i64>>, s: Seq<Option<i64>>, sps: Seq<Splice>)
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
    pub fn copy_ranges(&mut self, src: Self, splices: &[Splice])
        requires
            in_order(splices@, old(self)@.len() as int),
            ranges_ascend(splices@),
        ensures
            final(self)@ == applied(old(self)@, src@, splices@),
            final(self).edits() == if splices_do(splices@, src@.len() as int) {
                after_edit(old(self).edits())
            } else {
                old(self).edits()
            },
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
        *self = VerifiedDeltaColumn { core: store };
    }
}

} // verus!
