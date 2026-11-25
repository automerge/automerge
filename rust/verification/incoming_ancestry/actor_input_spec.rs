use crate::actor_chains::{Candidate, Dependency};
use crate::actor_input::Observation;
use crate::chain_spec::well_formed;
use crate::lookup_spec::{model, modeled};
use vstd::prelude::*;

verus! {

pub(crate) open spec fn earlier<A>(dep:Dependency<A>,i:int)->bool {
    match dep {Dependency::Candidate(p)=>p<i,Dependency::Applied{seq,..}=>seq>0}
}

pub(crate) open spec fn source_inputs<'a,K:'a,A,R:Fn(K)->Observation<'a,K,A>,L:Fn(K)->Dependency<A>>(hashes:Seq<K>,read:R,label:L)->bool {
    &&& hashes.len()<=usize::MAX
    &&& forall|i:int| 0<=i<hashes.len() ==> modeled(read, #[trigger] hashes[i]) && model(read,hashes[i]).seq>0
    &&& forall|i:int,j:int| 0<=i<hashes.len() && 0<=j<model(read,hashes[i]).parents@.len() ==>
        modeled(label, #[trigger] (model(read,hashes[i]).parents@)[j]) && earlier(model(label,model(read,hashes[i]).parents@[j]),i)
}

pub(crate) open spec fn row_is<'a,K:'a,A,L:Fn(K)->Dependency<A>>(row:Candidate<A>,observed:Observation<'a,K,A>,label:L)->bool {
    &&& row.actor==observed.actor && row.seq==observed.seq && row.actor_already_applied==observed.actor_already_applied
    &&& row.deps@.len()==observed.parents@.len()
    &&& forall|j:int| 0<=j<row.deps@.len() ==> #[trigger] row.deps@[j]==model(label,observed.parents@[j])
}

pub(crate) open spec fn prefix_inputs<'a,K:'a,A,R:Fn(K)->Observation<'a,K,A>,L:Fn(K)->Dependency<A>>(hashes:Seq<K>,read:R,label:L,rows:Seq<Candidate<A>>)->bool {
    &&& rows.len()<=hashes.len()
    &&& forall|i:int| 0<=i<rows.len() ==> row_is(#[trigger] rows[i],model(read,hashes[i]),label)
}

pub(crate) open spec fn exact_inputs<'a,K:'a,A,R:Fn(K)->Observation<'a,K,A>,L:Fn(K)->Dependency<A>>(hashes:Seq<K>,read:R,label:L,rows:Seq<Candidate<A>>)->bool {
    rows.len()==hashes.len() && prefix_inputs(hashes,read,label,rows)
}

pub(crate) proof fn extend<'a,K:'a,A,R:Fn(K)->Observation<'a,K,A>,L:Fn(K)->Dependency<A>>(hashes:Seq<K>,read:R,label:L,i:int,rows:Seq<Candidate<A>>,row:Candidate<A>)
    requires 0<=i<hashes.len(),rows.len()==i,source_inputs(hashes,read,label),prefix_inputs(hashes,read,label,rows),well_formed(rows),
        row_is(row,model(read,hashes[i]),label),
    ensures prefix_inputs(hashes,read,label,rows.push(row)),well_formed(rows.push(row)),
{
    let next=rows.push(row);
    assert forall|p:int,q:int| 0<=p<next.len() && 0<=q<next[p].deps@.len() implies match #[trigger] next[p].deps@[q] {
        Dependency::Candidate(edge)=>edge<p,Dependency::Applied{seq,..}=>seq>0,
    } by {
        if p==i {
            let parent=model(read,hashes[i]).parents@[q];
            assert(next[p].deps@[q]==model(label,parent));
        } else {
            assert(next[p].deps@[q]==rows[p].deps@[q]);
        }
    }
}

} // verus!
