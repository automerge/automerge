use crate::lookup_spec::{model, modeled};
use crate::membership_proof::equal_cardinality;
use std::hash::Hash;
use vstd::prelude::*;
use vstd::{laws_eq::obeys_concrete_eq, std_specs::hash::obeys_key_model};

verus! {

pub(crate) open spec fn hash_laws<K:Eq+Hash>()->bool {
    obeys_key_model::<K>() && obeys_concrete_eq::<K>()
}

pub(crate) open spec fn hash_projection<T,K,F:Fn(&T)->K>(values:Seq<T>, hash:F)->bool {
    forall|i:int| 0<=i<values.len() ==> #[trigger] modeled(hash,&values[i])
}

pub(crate) open spec fn hashes<T,K,F:Fn(&T)->K>(values:Seq<T>,hash:F)->Seq<K> {
    Seq::new(values.len(),|i:int| model(hash,&values[i]))
}

pub(crate) open spec fn same_hashes<T,K,F:Fn(&T)->K>(left:Seq<T>,right:Seq<T>,hash:F)->bool {
    hashes(left,hash).no_duplicates() && hashes(left,hash).to_set()==hashes(right,hash).to_set()
}

pub(crate) proof fn lengths_differ<T,K,F:Fn(&T)->K>(left:Seq<T>,right:Seq<T>,hash:F)
    requires left.len()!=right.len(),hashes(right,hash).no_duplicates(),
    ensures !same_hashes(left,right,hash),
{
    hashes(right,hash).unique_seq_to_set();
    if hashes(left,hash).no_duplicates() { hashes(left,hash).unique_seq_to_set(); }
}

pub(crate) proof fn set_step<T,K,F:Fn(&T)->K>(values:Seq<T>,hash:F,i:int)
    requires 0<=i<values.len(),
    ensures hashes(values.take(i+1),hash).to_set() =~= hashes(values.take(i),hash).to_set().insert(model(hash,&values[i])),
{
    let prefix=hashes(values.take(i),hash);
    let key=model(hash,&values[i]);
    assert(hashes(values.take(i+1),hash) =~= prefix.push(key));
    prefix.lemma_push_to_set_commute(key);
}

pub(crate) proof fn equal_sets<T,K,F:Fn(&T)->K>(left:Seq<T>,right:Seq<T>,hash:F,set:Set<K>)
    requires left.len()==right.len(),hashes(left,hash).no_duplicates(),hashes(right,hash).no_duplicates(),
        set==hashes(left,hash).to_set(),forall|i:int| 0<=i<right.len() ==> set.contains(#[trigger] model(hash,&right[i])),
    ensures same_hashes(left,right,hash),
{
    let other=hashes(right,hash);
    let our=hashes(left,hash);
    our.unique_seq_to_set();
    other.unique_seq_to_set();
    assert forall|i:int| 0<=i<other.len() implies set.contains(#[trigger] other[i]) by {assert(other[i]==model(hash,&right[i]));}
    equal_cardinality(other,set);
}

// On intrinsically validated deliveries this is an equivalence. The head and
// attachment are compared by their exact identities; only member order is
// ignored. Include attachment equality so disjoint proofs cannot be merged.
pub(crate) open spec fn delivery_id<H,K,P>(head:H,members:Seq<K>,attachment:P)->(H,Set<K>,P) {
    (head,members.to_set(),attachment)
}

pub(crate) proof fn exact_delivery_equivalence<T,K,F:Fn(&T)->K,H,P>(head_a:H,head_b:H, left:Seq<T>,right:Seq<T>, attachments_a:P,attachments_b:P,hash:F)
    requires hashes(left,hash).no_duplicates(),hashes(right,hash).no_duplicates(),
    ensures (head_a==head_b && attachments_a==attachments_b && same_hashes(left,right,hash)) <==>
        delivery_id(head_a,hashes(left,hash),attachments_a)==delivery_id(head_b,hashes(right,hash),attachments_b),
{}

} // verus!
