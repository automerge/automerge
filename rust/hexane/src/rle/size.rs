//! A bound on a slab's bytes, verified with Verus.
//!
//! A canonical slab is its values' encodings plus headers: one header and
//! at most one count for every group, and no group is empty. So where no
//! header is longer than `h` bytes, a slab of `n` values is at most
//! `2 * h * n` bytes longer than the encodings of its values
//! (`lemma_slab_size`). A caller that knows how many values it saved then
//! knows the bytes are short of 4 GiB, which is what the loader asks.

#[cfg(verus_keep_ghost)]
use crate::rle::dec::{lemma_slab, slab_groups, slab_values, valid_slab};
#[cfg(verus_keep_ghost)]
use crate::rle::model::{
    canonical, encs, group_bytes, group_len, group_ok, group_values, groups_bytes, groups_len,
    groups_values, lemma_encs_concat, Group, RleOps,
};
use vstd::prelude::*;

verus! {

/// No header and no count is longer than `h` bytes.
pub(crate) open spec fn heads_bound<'a, O: RleOps<'a>>(h: nat) -> bool {
    &&& forall|n: i64| (#[trigger] O::enc_signed(n)).len() <= h
    &&& forall|n: u64| (#[trigger] O::enc_unsigned(n)).len() <= h
}

/// A group is at most two headers longer than its values' encodings.
proof fn lemma_group_size<'a, O: RleOps<'a>>(g: Group<O::V>, h: nat)
    requires
        group_ok::<O>(g),
        heads_bound::<O>(h),
    ensures
        group_len(g) >= 1,
        group_bytes::<O>(g).len() <= 2 * h + encs::<O>(group_values(g, O::null())).len(),
{
    match g {
        Group::Run { count, value } => {
            let vs = Seq::new(count, |_i: int| value);
            assert(vs.last() == value);
            assert(encs::<O>(vs) == encs::<O>(vs.drop_last()) + O::enc(vs.last()));
            assert(O::enc_signed(count as i64).len() <= h);
        },
        Group::Lit { values } => {
            assert(O::enc_signed(-(values.len() as int) as i64).len() <= h);
        },
        Group::Null { count } => {
            assert(O::enc_signed(0).len() <= h);
            assert(O::enc_unsigned(count as u64).len() <= h);
        },
    }
}

/// Canonical groups are at most two headers a value longer than their
/// values' encodings.
proof fn lemma_groups_size<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, h: nat)
    requires
        canonical::<O>(gs),
        heads_bound::<O>(h),
    ensures
        groups_bytes::<O>(gs).len() <= 2 * h * groups_len(gs) + encs::<O>(
            groups_values(gs, O::null()),
        ).len(),
    decreases gs.len(),
{
    if gs.len() > 0 {
        let front = gs.drop_last();
        let g = gs.last();
        assert(canonical::<O>(front)) by {
            assert forall|i: int| 0 <= i < front.len() implies group_ok::<O>(#[trigger] front[i]) by {
                assert(front[i] == gs[i]);
            }
            assert forall|i: int| 0 < i < front.len() implies crate::rle::model::adjacent_ok(
                front[i - 1],
                #[trigger] front[i],
            ) by {
                assert(front[i] == gs[i]);
                assert(front[i - 1] == gs[i - 1]);
            }
        }
        lemma_groups_size::<O>(front, h);
        assert(group_ok::<O>(gs[gs.len() - 1]));
        lemma_group_size::<O>(g, h);
        lemma_encs_concat::<O>(groups_values(front, O::null()), group_values(g, O::null()));
        let a = groups_len(front);
        let c = group_len(g);
        assert(2 * h * (a + c) >= 2 * h * a + 2 * h) by (nonlinear_arith)
            requires
                c >= 1,
        ;
    }
}

/// A valid slab is at most two headers a value longer than its values'
/// encodings.
pub(crate) proof fn lemma_slab_size<'a, O: RleOps<'a>>(d: Seq<u8>, h: nat)
    requires
        valid_slab::<O>(d),
        heads_bound::<O>(h),
    ensures
        d.len() <= 2 * h * slab_values::<O>(d).len() + encs::<O>(slab_values::<O>(d)).len(),
{
    lemma_slab::<O>(d);
    lemma_groups_size::<O>(slab_groups::<O>(d), h);
}

} // verus!
