use crate::clock_model::{
    accepted_clock, aligned_slots, closed_maxima_are_exact, normalized_maxima,
};
use crate::history_spec::{actor_chains, causal_set, History};
use crate::publication::Publish;
use crate::publication_spec::{merged, publication_trace, reclaimed};
use std::num::NonZeroU32;
use vstd::prelude::*;

verus! {

#[verifier::reject_recursive_types(S)]
#[verifier::reject_recursive_types(V)]
#[verifier::reject_recursive_types(P)]
#[verifier::reject_recursive_types(H)]
#[verifier::reject_recursive_types(C)]
pub(crate) ghost struct Model<S,V,H,G,P,W,C,D> {
    pub before: spec_fn(S)->Document<H,W,C,D>,
    pub observe: spec_fn(V)->SigningView<H,G,W,C>,
    pub import: spec_fn(C,Seq<H>)->C,
    pub proof_records: spec_fn(P)->Set<W>,
}

/// `identity` denotes the full abstract document for the abort guarantee,
/// not a pointer or a patch log. Content captures the document apply leaf.
pub(crate) ghost struct Document<H,W,C,D> {
    pub identity: D,
    pub applied: Set<H>,
    pub accepted: Set<H>,
    pub evidence: Set<W>,
    pub content: C,
}

#[verifier::reject_recursive_types(H)]
pub(crate) ghost struct SigningView<H,G,W,C> {
    pub graph: History<H,usize>,
    pub slots: Seq<Option<NonZeroU32>>,
    pub evidence: Set<W>,
    pub pending: Seq<G>,
    pub content: C,
}

pub(crate) open spec fn selected_witness<P,W>(proofs:Seq<P>,records:spec_fn(P)->Set<W>,end:int,w:W)->bool {
    exists|i:int| 0<=i<end && #[trigger] records(proofs[i]).contains(w)
}

// Leaf effects are narrower than publication correctness. In particular an
// IncludeMembers event only promises the actual slot fold, not exact accepted
// history. Reclamation may remove evidence but may not introduce more.
pub(crate) open spec fn effect<V,H:'static,G,P:'static,W,C>(observe:spec_fn(V)->SigningView<H,G,W,C>,records:spec_fn(P)->Set<W>,before:V,step:Publish<&P,&[H]>,after:V)->bool {
    let old=observe(before);
    let new=observe(after);
    &&& new.graph==old.graph && new.content==old.content && new.pending==old.pending
    &&& match step {
        Publish::MergeProof(proof)=>new.slots==old.slots && new.evidence.subset_of(old.evidence.union(records(*proof))),
        Publish::IncludeMembers(members)=>normalized_maxima(old.graph,members@,old.slots,new.slots) && new.evidence.subset_of(old.evidence),
        Publish::Reclaim | Publish::RemoveDrafts=>new.slots==old.slots && new.evidence.subset_of(old.evidence),
    }
}

pub(crate) open spec fn publish_laws<V,H:'static,G,P:'static,W,C,F:for<'event> Fn(V,Publish<&'event P,&'event[H]>)->V>(observe:spec_fn(V)->SigningView<H,G,W,C>,records:spec_fn(P)->Set<W>,step:F)->bool {
    forall|before:V,event:Publish<&P,&[H]>,after:V| #[trigger] step.ensures((before,event),after) ==> effect(observe,records,before,event,after)
}

proof fn merged_effects<'a,V,H:'static,G,P:'static,W,C,F:for<'event> Fn(V,Publish<&'event P,&'event[H]>)->V>(
    initial:V,proofs:Seq<P>,members:&'a[H],step:F,states:Seq<V>,i:int,observe:spec_fn(V)->SigningView<H,G,W,C>,records:spec_fn(P)->Set<W>,
)
    requires publish_laws(observe,records,step),merged(initial,proofs,members,step,states,i),
    ensures observe(states[i]).graph==observe(initial).graph,
        observe(states[i]).slots==observe(initial).slots,observe(states[i]).pending==observe(initial).pending,
        observe(states[i]).content==observe(initial).content,
        forall|w:W| #[trigger] observe(states[i]).evidence.contains(w) ==>
            observe(initial).evidence.contains(w) || selected_witness(proofs,records,i,w),
    decreases i,
{
    if i>0 {
        let prefix=states.drop_last();
        assert(merged(initial,proofs,members,step,prefix,i-1)) by {
            assert forall|j:int| 0<=j<i-1 implies #[trigger] step.ensures((prefix[j],Publish::MergeProof(&proofs[j])),prefix[j+1]) by {
                assert(step.ensures((states[j],Publish::MergeProof(&proofs[j])),states[j+1]));
            }
        }
        merged_effects(initial,proofs,members,step,prefix,i-1,observe,records);
        assert(prefix[i-1]==states[i-1]);
        let before=states[i-1];
        let after=states[i];
        assert(step.ensures((before,Publish::MergeProof(&proofs[i-1])),after));
        assert(effect(observe,records,before,Publish::MergeProof(&proofs[i-1]),after));
        assert forall|w:W| #[trigger] observe(after).evidence.contains(w) implies
            observe(initial).evidence.contains(w) || selected_witness(proofs,records,i,w) by {
            if records(proofs[i-1]).contains(w) {assert(selected_witness(proofs,records,i,w));}
            else {assert(observe(before).evidence.contains(w));}
        }
    }
}

pub(crate) proof fn published_effect<'a,V,H:'static,G,P:'static,W,C,F:for<'event> Fn(V,Publish<&'event P,&'event[H]>)->V>(
    initial:V,proofs:Seq<P>,members:&'a[H],step:F,done:V,observe:spec_fn(V)->SigningView<H,G,W,C>,records:spec_fn(P)->Set<W>,
)
    requires publish_laws(observe,records,step),publication_trace(initial,proofs,members,step,done),
        actor_chains(observe(initial).graph),aligned_slots(observe(initial).graph,observe(initial).slots),
        causal_set(observe(initial).graph,accepted_clock(observe(initial).graph,observe(initial).slots).union(members@.to_set())),
    ensures observe(done).graph==observe(initial).graph,observe(done).content==observe(initial).content,
        observe(done).pending==observe(initial).pending,
        accepted_clock(observe(done).graph,observe(done).slots) =~=
            accepted_clock(observe(initial).graph,observe(initial).slots).union(members@.to_set()),
        forall|w:W| #[trigger] observe(done).evidence.contains(w) ==>
            observe(initial).evidence.contains(w) || selected_witness(proofs,records,proofs.len() as int,w),
{
    let (states,accepted,reclaimed_state)=choose|states:Seq<V>,accepted:V,after:V|
        #[trigger] reclaimed(initial,proofs,members,step,states,accepted,after)
            && step.ensures((after,Publish::RemoveDrafts),done);
    let before=states.last();
    merged_effects(initial,proofs,members,step,states,proofs.len() as int,observe,records);
    assert(step.ensures((before,Publish::IncludeMembers(members)),accepted));
    assert(effect(observe,records,before,Publish::IncludeMembers(members),accepted));
    assert(step.ensures((accepted,Publish::Reclaim),reclaimed_state));
    assert(effect(observe,records,accepted,Publish::Reclaim,reclaimed_state));
    assert(effect(observe,records,reclaimed_state,Publish::RemoveDrafts,done));
    closed_maxima_are_exact(observe(initial).graph,members@,observe(initial).slots,observe(accepted).slots);
}

} // verus!
