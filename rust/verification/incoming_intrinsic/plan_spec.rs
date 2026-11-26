use vstd::prelude::*;

verus! {

broadcast use {vstd::seq_lib::group_seq_properties, vstd::set::group_set_lemmas};

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

} // verus!
