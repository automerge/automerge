use crate::candidate_origin::Candidate;
use crate::pending::Origin;
use vstd::prelude::*;

verus! {

pub(crate) open spec fn tagged<G>(groups:Seq<G>,tag:Origin)->Seq<Candidate<G>> {
    Seq::new(groups.len(),|i:int| Candidate{origin:tag,group:groups[i]})
}

pub(crate) proof fn append_tagged<G>(input:Seq<G>,tag:Origin,i:int)
    requires 0<=i<input.len(),
    ensures tagged(input.take(i+1),tag) =~= tagged(input.take(i),tag).push(Candidate{origin:tag,group:input[i]}),
{}

pub(crate) open spec fn groups_of<G>(candidates:Seq<Candidate<G>>)->Seq<G> {
    Seq::new(candidates.len(),|i:int| candidates[i].group)
}

pub(crate) proof fn append_group<G>(candidates:Seq<Candidate<G>>,i:int)
    requires 0<=i<candidates.len(),
    ensures groups_of(candidates.take(i+1)) =~= groups_of(candidates.take(i)).push(candidates[i].group),
{}

} // verus!
