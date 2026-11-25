use crate::spec::candidate_edges;
use vstd::prelude::*;

verus! {

// Fragment occurrences and hashes have separate index spaces. `required_by[h]`
// lists the fragments whose boundary needs h from another selected fragment.
pub open spec fn well_formed(members: Seq<Vec<usize>>, required_by: Seq<Vec<usize>>, counts: Seq<usize>) -> bool {
    &&& members.len() <= usize::MAX
    &&& required_by.len() == counts.len() <= usize::MAX
    &&& forall|f: int, i: int| 0 <= f < members.len() && 0 <= i < members[f]@.len()
        ==> #[trigger] members[f]@[i] < counts.len()
    &&& forall|h: int, i: int| 0 <= h < counts.len() && 0 <= i < required_by[h]@.len()
        ==> #[trigger] required_by[h]@[i] < members.len()
    &&& forall|h: usize| h < counts.len() ==> #[trigger] counts[h as int]
        == candidate_edges(members, Seq::empty(), h)
}

pub open spec fn supplies(members: Seq<Vec<usize>>, choice: Set<usize>, hash: usize) -> bool {
    exists|f: usize| f < members.len() && choice.contains(f) && #[trigger] members[f as int]@.contains(hash)
}

pub open spec fn closed(members: Seq<Vec<usize>>, required_by: Seq<Vec<usize>>, choice: Set<usize>) -> bool {
    &&& forall|f: usize| #[trigger] choice.contains(f) ==> f < members.len()
    &&& forall|h: usize, f: usize| h < required_by.len() && choice.contains(f)
        && #[trigger] required_by[h as int]@.contains(f) ==> supplies(members, choice, h)
}

pub open spec fn biggest(
    members: Seq<Vec<usize>>, required_by: Seq<Vec<usize>>, excluded: Seq<bool>,
) -> bool {
    let selected = Set::range(0usize, members.len() as usize).filter(|f: usize| !excluded[f as int]);
    &&& excluded.len() == members.len()
    &&& closed(members, required_by, selected)
    &&& forall|alternative: Set<usize>| #[trigger] closed(members, required_by, alternative)
        ==> alternative.subset_of(selected)
}

} // verus!
