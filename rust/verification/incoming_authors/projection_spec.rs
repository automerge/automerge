use crate::actor_chains::{AuthorSource, Resolution};
use crate::lookup_spec::{model, modeled};
use std::hash::Hash;
use vstd::prelude::*;
use vstd::{laws_eq::obeys_concrete_eq, std_specs::hash::obeys_key_model};

verus! {

pub(crate) open spec fn hash_laws<K:Eq+Hash>()->bool { obeys_key_model::<K>() && obeys_concrete_eq::<K>() }

pub(crate) open spec fn source_views<K,T,I:Fn(usize)->Option<T>,A:Fn(usize)->Option<T>>(
    hashes:Seq<K>,outcomes:Seq<Resolution>,initial:I,applied:A,
)->bool {
    forall|i:int| 0<=i<outcomes.len() ==> match #[trigger] outcomes[i] {
        Resolution::Broken(cause)=>cause<hashes.len(),
        Resolution::Linked(AuthorSource::Initial(root))=>modeled(initial,root),
        Resolution::Linked(AuthorSource::Applied(slot))=>modeled(applied,slot),
    }
}

pub(crate) open spec fn inherited<T,I:Fn(usize)->Option<T>,A:Fn(usize)->Option<T>>(source:AuthorSource,initial:I,applied:A)->Option<T> {
    match source {AuthorSource::Initial(root)=>model(initial,root),AuthorSource::Applied(slot)=>model(applied,slot)}
}

pub(crate) open spec fn at_hash<K,T,I:Fn(usize)->Option<T>,A:Fn(usize)->Option<T>>(
    hashes:Seq<K>,outcome:Resolution,k:K,initial:I,applied:A,authors:Map<K,Option<T>>,broken:Map<K,K>,
)->bool {
    match outcome {
        Resolution::Linked(source)=>authors.contains_key(k) && authors[k]==inherited(source,initial,applied) && !broken.contains_key(k),
        Resolution::Broken(cause)=>broken.contains_key(k) && broken[k]==hashes[cause as int] && !authors.contains_key(k),
    }
}

pub(crate) open spec fn materialized<K,T,I:Fn(usize)->Option<T>,A:Fn(usize)->Option<T>>(
    hashes:Seq<K>,outcomes:Seq<Resolution>,initial:I,applied:A,end:int,authors:Map<K,Option<T>>,broken:Map<K,K>,
)->bool {
    &&& forall|i:int| 0<=i<end ==> at_hash(hashes,#[trigger] outcomes[i],hashes[i],initial,applied,authors,broken)
    &&& authors.dom().union(broken.dom()) =~= hashes.take(end).to_set()
}

pub(crate) proof fn record<K,T,I:Fn(usize)->Option<T>,A:Fn(usize)->Option<T>>(
    hashes:Seq<K>,outcomes:Seq<Resolution>,initial:I,applied:A,i:int,old_authors:Map<K,Option<T>>,old_broken:Map<K,K>,authors:Map<K,Option<T>>,broken:Map<K,K>,
)
    requires 0<=i<outcomes.len()==hashes.len(),hashes.no_duplicates(),source_views(hashes,outcomes,initial,applied),
        materialized(hashes,outcomes,initial,applied,i,old_authors,old_broken),
        match outcomes[i] {
            Resolution::Linked(source)=>authors==old_authors.insert(hashes[i],inherited(source,initial,applied)) && broken==old_broken,
            Resolution::Broken(cause)=>authors==old_authors && broken==old_broken.insert(hashes[i],hashes[cause as int]),
        },
    ensures materialized(hashes,outcomes,initial,applied,i+1,authors,broken),
{
    assert(!hashes.take(i).contains(hashes[i])) by {
        if hashes.take(i).contains(hashes[i]) {
            let j=choose|j:int| 0<=j<i && hashes.take(i)[j]==hashes[i];
            assert(hashes.take(i)[j]==hashes[j]);
        }
    }
    assert(!hashes.take(i).to_set().contains(hashes[i]));
    assert(!old_authors.contains_key(hashes[i]) && !old_broken.contains_key(hashes[i]));
    hashes.lemma_take_succ_push(i);
    hashes.take(i).lemma_push_to_set_commute(hashes[i]);
    assert forall|j:int| 0<=j<i+1 implies at_hash(hashes,#[trigger] outcomes[j],hashes[j],initial,applied,authors,broken) by {
        if j<i {assert(hashes[j]!=hashes[i]);assert(at_hash(hashes,outcomes[j],hashes[j],initial,applied,old_authors,old_broken));}
    }
}

} // verus!
