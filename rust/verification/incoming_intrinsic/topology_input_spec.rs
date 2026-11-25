use crate::lookup_spec::{model, modeled};
use crate::topology_input::Parent;
use vstd::prelude::*;

verus! {

broadcast use vstd::seq_lib::group_seq_properties;

pub(crate) open spec fn projected<'a,K:'a,P:Fn(K)->&'a[K],C:Fn(K)->Parent>(candidates:Seq<K>,parents:P,classify:C)->bool {
    &&& candidates.len()<=usize::MAX
    &&& forall|i:int| 0<=i<candidates.len() ==> modeled(parents,#[trigger] candidates[i])
    &&& forall|i:int,j:int| 0<=i<candidates.len() && 0<=j<model(parents,candidates[i])@.len() ==> {
        &&& modeled(classify, #[trigger] (model(parents,candidates[i])@)[j])
        &&& match model(classify,(model(parents,candidates[i])@)[j]) { Parent::Candidate(p)=>p<candidates.len(), _=>true }
    }
}

pub(crate) open spec fn row<K,C:Fn(K)->Parent>(child:int, deps:Seq<K>,classify:C)->Seq<(usize,Parent)> {
    Seq::new(deps.len(),|j:int| (child as usize,model(classify,deps[j])))
}

#[verifier::opaque]
pub(crate) open spec fn trace<'a,K:'a,P:Fn(K)->&'a[K],C:Fn(K)->Parent>(candidates:Seq<K>,parents:P,classify:C,end:int)->Seq<(usize,Parent)>
    decreases end,
{
    if end<=0 { Seq::empty() } else {
        trace(candidates,parents,classify,end-1)+row(end-1,model(parents,candidates[end-1])@,classify)
    }
}

pub(crate) open spec fn unapplied(e:(usize,Parent), child:usize)->bool {
    e.0==child && !(e.1 is Applied)
}

pub(crate) open spec fn from_parent(e:(usize,Parent), parent:usize)->bool {
    e.1==Parent::Candidate(parent)
}

// Every raw dependency occurrence is accounted for. Applied parents contribute
// nothing; missing parents contribute only to waiting, not to reverse edges.
pub(crate) open spec fn counts(events:Seq<(usize,Parent)>,waiting:Seq<usize>,reverse:Seq<Vec<usize>>,n:nat)->bool {
    &&& waiting.len()==n && reverse.len()==n
    &&& forall|i:int| 0<=i<n ==> #[trigger] waiting[i]==events.filter(|e:(usize,Parent)| unapplied(e,i as usize)).len()
    &&& forall|p:int| 0<=p<n ==> (#[trigger] reverse[p])@ =~=
        events.filter(|e:(usize,Parent)| from_parent(e,p as usize)).map_values(|e:(usize,Parent)| e.0)
}

pub(crate) open spec fn untouched(waiting:Seq<usize>,start:int)->bool {
    forall|i:int| start<=i<waiting.len() ==> #[trigger] waiting[i]==0
}

pub(crate) proof fn advance<'a,K:'a,P:Fn(K)->&'a[K],C:Fn(K)->Parent>(candidates:Seq<K>,parents:P,classify:C,i:int)
    requires 0<=i<candidates.len(),
    ensures trace(candidates,parents,classify,i+1) =~= trace(candidates,parents,classify,i)+row(i,model(parents,candidates[i])@,classify),
{ reveal(trace); }

pub(crate) proof fn record_edge<K,C:Fn(K)->Parent>(prefix:Seq<(usize,Parent)>,i:int,deps:Seq<K>,classify:C,j:int,
    before:Seq<usize>,old_reverse:Seq<Vec<usize>>,after:Seq<usize>,reverse:Seq<Vec<usize>>,
)
    requires 0<=j<deps.len(),0<=i<before.len()<=usize::MAX,
        counts(prefix+row(i,deps,classify).take(j),before,old_reverse,before.len()),
        after==if model(classify,deps[j]) is Applied {before} else {before.update(i,(before[i]+1) as usize)},
        before[i] < usize::MAX,
        reverse.len()==old_reverse.len(),
        forall|p:int| 0<=p<reverse.len() ==> (#[trigger] reverse[p])@ == if model(classify,deps[j])==Parent::Candidate(p as usize) {
            old_reverse[p]@.push(i as usize)
        } else {old_reverse[p]@},
    ensures counts(prefix+row(i,deps,classify).take(j+1),after,reverse,before.len()),
{
    let events=prefix+row(i,deps,classify).take(j);
    let e=(i as usize,model(classify,deps[j]));
    row(i,deps,classify).lemma_take_succ_push(j);
    assert(prefix+row(i,deps,classify).take(j+1) =~= events.push(e));
    assert forall|k:int| 0<=k<before.len() implies #[trigger] after[k]==events.push(e).filter(|v:(usize,Parent)| unapplied(v,k as usize)).len() by {
        events.lemma_filter_push(e,|v:(usize,Parent)| unapplied(v,k as usize));
    }
    assert forall|p:int| 0<=p<reverse.len() implies (#[trigger] reverse[p])@ =~=
        events.push(e).filter(|v:(usize,Parent)| from_parent(v,p as usize)).map_values(|v:(usize,Parent)| v.0) by {
        let filter=|v:(usize,Parent)| from_parent(v,p as usize);
        events.lemma_filter_push(e,filter);
        events.filter(filter).lemma_push_map_commute(|v:(usize,Parent)| v.0,e);
    }
}

} // verus!
