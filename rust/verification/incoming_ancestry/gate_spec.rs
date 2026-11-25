use crate::ancestry_gate::Guard;
use crate::lookup_spec::{model, modeled};
use crate::slot_spec::{conflict_result, conflicts, consistent};
use crate::walk_spec::{keeps, reached, walk_result, Graph};
use std::hash::Hash;
use vstd::{laws_eq::obeys_concrete_eq, prelude::*, std_specs::hash::obeys_key_model};

verus! {

pub(crate) open spec fn hash_laws<K:Eq+Hash>()->bool { obeys_key_model::<K>() && obeys_concrete_eq::<K>() }
pub(crate) open spec fn slot_laws<S:Eq+Hash>()->bool { hash_laws::<S>() }

pub(crate) open spec fn slot_inputs<K,S,F:Fn(K)->(S,bool)>(hashes:Seq<K>,slot:F)->bool {
    hashes.no_duplicates() && forall|i:int| 0<=i<hashes.len() ==> modeled(slot,#[trigger] hashes[i])
}

pub(crate) open spec fn screened<K,S,F:Fn(K)->(S,bool)>(guard:&Guard<'_,K,S,F>)->bool {
    slot_inputs(guard.hashes@,guard.slot) && conflict_result(guard.hashes@,guard.slot,guard.conflict)
}

pub(crate) open spec fn in_cone<K>(graph:Graph<K>,roots:Seq<K>,k:K)->bool {
    reached(graph,roots,k) && keeps((graph.visit)(k))
}

pub(crate) open spec fn cone_is_in_batch<K>(graph:Graph<K>,roots:Seq<K>,batch:Seq<K>)->bool {
    forall|k:K| #[trigger] in_cone(graph,roots,k) ==> batch.contains(k)
}

pub(crate) open spec fn bad_slot<K,S,F:Fn(K)->(S,bool)>(graph:Graph<K>,roots:Seq<K>,slot:F,k:K)->bool {
    &&& in_cone(graph,roots,k)
    &&& (model(slot,k).1 || exists|other:K| #[trigger] in_cone(graph,roots,other) && k!=other
        && model(slot,k).0==model(slot,other).0)
}

pub(crate) open spec fn cone_result<K,S,F:Fn(K)->(S,bool)>(graph:Graph<K>,roots:Seq<K>,slot:F,out:Option<K>)->bool {
    match out {
        None=>forall|k:K| !#[trigger] bad_slot(graph,roots,slot,k),
        Some(k)=>bad_slot(graph,roots,slot,k),
    }
}

// A clean superset is sufficient for the fast path. Nothing about the
// status of an unrelated conflicting slot is used on the slow path.
pub(crate) proof fn clean_batch_implies_clean_cone<K,S,F:Fn(K)->(S,bool)>(graph:Graph<K>,roots:Seq<K>,batch:Seq<K>,slot:F)
    requires consistent(batch,slot,batch.len() as int),cone_is_in_batch(graph,roots,batch),
    ensures cone_result(graph,roots,slot,None),
{
    assert forall|k:K| !#[trigger] bad_slot(graph,roots,slot,k) by {
        if in_cone(graph,roots,k) {
            let i=choose|i:int| 0<=i<batch.len() && batch[i]==k;
            assert(!conflicts(batch,slot,i));
            if bad_slot(graph,roots,slot,k) {
                let other=choose|other:K| #[trigger] in_cone(graph,roots,other) && k!=other && model(slot,k).0==model(slot,other).0;
                let j=choose|j:int| 0<=j<batch.len() && batch[j]==other;
                if j<i { assert(conflicts(batch,slot,i)); }
                else { assert(i<j); assert(conflicts(batch,slot,j)); }
            }
        }
    }
}

pub(crate) proof fn prepare_scan<K,S,F:Fn(K)->(S,bool)>(graph:Graph<K>,roots:Seq<K>,batch:Seq<K>,slot:F,cone:Seq<K>)
    requires slot_inputs(batch,slot),cone_is_in_batch(graph,roots,batch),walk_result(graph,roots,cone),
    ensures slot_inputs(cone,slot),
{
    assert forall|i:int| 0<=i<cone.len() implies modeled(slot,#[trigger] cone[i]) by {
        let k=cone[i];
        assert(cone.contains(k));
        assert(in_cone(graph,roots,k));
        let j=choose|j:int| 0<=j<batch.len() && batch[j]==k;
        assert(modeled(slot,batch[j]));
    }
}

pub(crate) proof fn finish_scan<K,S,F:Fn(K)->(S,bool)>(graph:Graph<K>,roots:Seq<K>,slot:F,cone:Seq<K>,out:Option<K>)
    requires walk_result(graph,roots,cone),conflict_result(cone,slot,out),
    ensures cone_result(graph,roots,slot,out),
{
    match out {
        None=>clean_batch_implies_clean_cone(graph,roots,cone,slot),
        Some(k)=>{
            let i=choose|i:int| 0<=i<cone.len() && #[trigger] cone[i]==k && conflicts(cone,slot,i);
            assert(cone.contains(k));
            if !model(slot,k).1 {
                let j=choose|j:int| 0<=j<i && model(slot,#[trigger] cone[j]).0==model(slot,k).0;
                let other=cone[j];
                assert(cone.contains(other));
                assert(in_cone(graph,roots,other));
                assert(other!=k);
            }
            assert(bad_slot(graph,roots,slot,k));
        },
    }
}

} // verus!
