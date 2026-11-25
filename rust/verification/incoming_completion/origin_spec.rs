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

} // verus!
