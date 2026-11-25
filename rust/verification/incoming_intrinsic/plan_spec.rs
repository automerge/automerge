use crate::lookup_spec::{model, modeled};
use vstd::prelude::*;

verus! {

broadcast use {vstd::seq_lib::group_seq_properties, vstd::set::group_set_lemmas};

pub(crate) open spec fn member_views<'a,T:'a,H,C:Fn(usize)->&'a[T],P:Fn(&'a T)->H>(selected:Seq<usize>,contents:C,hash:P) -> bool {
    &&& forall|i:int| 0<=i<selected.len() ==> modeled(contents,#[trigger] selected[i])
    &&& forall|i:int,j:int| 0<=i<selected.len() && 0<=j<model(contents,selected[i])@.len()
        ==> #[trigger] modeled(hash,&(model(contents,selected[i])@)[j])
}

pub(crate) open spec fn hashes<'a,T:'a,H,P:Fn(&'a T)->H>(members:Seq<T>,hash:P) -> Seq<H> {
    Seq::new(members.len(), |i:int| model(hash,&members[i]))
}

#[verifier::opaque]
pub(crate) open spec fn flatten<'a,T:'a,H,C:Fn(usize)->&'a[T],P:Fn(&'a T)->H>(selected:Seq<usize>,contents:C,hash:P) -> Seq<H>
    decreases selected.len(),
{
    if selected.len()==0 { Seq::empty() }
    else { flatten(selected.drop_last(),contents,hash) + hashes(model(contents,selected.last())@,hash) }
}

pub(crate) proof fn extend_hashes<'a,T:'a,H,P:Fn(&'a T)->H>(members:Seq<T>,j:int,hash:P)
    requires 0<=j<members.len(),
    ensures hashes(members.take(j+1),hash) =~= hashes(members.take(j),hash).push(model(hash,&members[j])),
{}

pub(crate) proof fn extend_groups<'a,T:'a,H,C:Fn(usize)->&'a[T],P:Fn(&'a T)->H>(selected:Seq<usize>,i:int,contents:C,hash:P)
    requires 0<=i<selected.len(),
    ensures flatten(selected.take(i+1),contents,hash) =~=
        flatten(selected.take(i),contents,hash) + hashes(model(contents,selected[i])@,hash),
{
    reveal(flatten);
    assert(selected.take(i+1).drop_last() =~= selected.take(i));
}

pub(crate) open spec fn remaining<S:Fn(usize)->usize>(waiting:Seq<usize>,excluded:Seq<usize>,source:S,end:int,remaining:Set<usize>) -> bool {
    forall|candidate:usize| #[trigger] remaining.contains(candidate) <==> waiting.contains(candidate)
        || (exists|i:int| 0<=i<end && model(source,#[trigger] excluded[i]) == candidate)
}

pub(crate) proof fn extend_remaining<S:Fn(usize)->usize>(waiting:Seq<usize>,excluded:Seq<usize>,source:S,i:int,prior:Set<usize>,out:Set<usize>)
    requires 0<=i<excluded.len(),remaining(waiting,excluded,source,i,prior),out==prior.insert(model(source,excluded[i])),
    ensures remaining(waiting,excluded,source,i+1,out),
{}

pub(crate) open spec fn inverse<K>(hashes:Seq<K>,end:int,positions:Map<K,usize>)->bool {
    &&& forall|j:int| 0<=j<end ==> positions.contains_key(#[trigger] hashes[j]) && positions[hashes[j]]==j
    &&& positions.dom() =~= hashes.take(end).to_set()
}

pub(crate) proof fn position_added<K>(hashes:Seq<K>,i:int,prior:Map<K,usize>,out:Map<K,usize>)
    requires 0<=i<hashes.len()<=usize::MAX, hashes.no_duplicates(), inverse(hashes,i,prior),
        out==prior.insert(hashes[i],i as usize),
    ensures inverse(hashes,i+1,out),
{
    hashes.lemma_take_succ_push(i);
    hashes.take(i).lemma_push_to_set_commute(hashes[i]);
}

pub(crate) open spec fn ordered_selection(indices:Seq<usize>,len:nat)->bool {
    &&& len<=usize::MAX && indices.len()<=usize::MAX
    &&& forall|i:int| 0<=i<indices.len() ==> #[trigger] indices[i]<len
    &&& forall|i:int,j:int| 0<=i<j<indices.len() ==> #[trigger] indices[i]<#[trigger] indices[j]
}

pub(crate) open spec fn emitted<T,V,F:Fn(T)->V>(input:Seq<T>,selected:Seq<usize>,payload:F,processed:int,next:usize,out:Seq<V>)->bool {
    &&& next<=selected.len() && out.len()==next
    &&& forall|j:int| 0<=j<selected.len() ==> (j<next <==> (#[trigger] selected[j])<processed)
    &&& forall|j:int| 0<=j<next ==> #[trigger] out[j]==model(payload,input[selected[j] as int])
}

pub(crate) proof fn finish_selection<T,V,F:Fn(T)->V>(input:Seq<T>,selected:Seq<usize>,payload:F,next:usize,out:Seq<V>)
    requires ordered_selection(selected,input.len()),emitted(input,selected,payload,input.len() as int,next,out),
    ensures out.len()==selected.len(),forall|i:int| 0<=i<selected.len() ==> #[trigger] out[i]==model(payload,input[selected[i] as int]),
{
    if next<selected.len() { assert(selected[next as int]<input.len()); }
}

} // verus!
