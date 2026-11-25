use crate::lookup_spec::{model, modeled};
use crate::scoped::{Bucket, Failure, Operations};
use crate::scoped_spec::{bucket, found_in, ready_to_search};
use std::collections::HashMap;
use std::hash::Hash;
use vstd::prelude::*;
use vstd::{
    laws_eq::obeys_concrete_eq,
    pervasive::obeys_clone_eq,
    std_specs::hash::{hash_map_inv, obeys_key_model},
};

verus! {

pub(crate) open spec fn cache_laws<A:Clone+Eq+Hash>()->bool {
    obeys_key_model::<A>() && obeys_concrete_eq::<A>() && obeys_clone_eq::<A>()
}

pub(crate) open spec fn authentic_keys<A,K,E,D:Fn(A)->Result<K,E>>(view:Map<A,K>,decode:D)->bool {
    forall|a:A| #[trigger] view.contains_key(a) ==> decode.ensures((a,),Ok(view[a]))
}

pub(crate) open spec fn decoded_cache<A:Eq+Hash,K,E,D:Fn(A)->Result<K,E>>(keys:&HashMap<A,K>,decode:D)->bool {
    hash_map_inv(keys) && authentic_keys(keys@,decode)
}

pub(crate) open spec fn preserved<A,K>(before:Map<A,K>,after:Map<A,K>)->bool {
    forall|a:A| #[trigger] before.contains_key(a) ==> after.contains_key(a) && after[a]==before[a]
}

pub(crate) proof fn keep_original<A,K>(base:Map<A,K>,before:Map<A,K>,after:Map<A,K>)
    requires preserved(base,before),preserved(before,after), ensures preserved(base,after),
{
    assert forall|a:A| #[trigger] base.contains_key(a) implies after.contains_key(a) && after[a]==base[a] by {
        assert(before.contains_key(a));
    }
}

pub(crate) proof fn append_key<A,K,E,D:Fn(A)->Result<K,E>>(before:Map<A,K>,after:Map<A,K>,decode:D,a:A,k:K)
    requires authentic_keys(before,decode), !before.contains_key(a),decode.ensures((a,),Ok(k)),after==before.insert(a,k),
    ensures authentic_keys(after,decode),preserved(before,after),
{
    assert forall|b:A| #[trigger] after.contains_key(b) implies decode.ensures((b,),Ok(after[b])) by {
        if b!=a {assert(before.contains_key(b));}
    }
}

pub(crate) open spec fn has_author<A>(hint:Option<Option<A>>)->bool {
    hint.is_some() && hint.unwrap().is_some()
}

pub(crate) open spec fn tentative_key<H,A,K,E,U:Fn(H)->Option<Option<A>>,D:Fn(A)->Result<K,E>>(hash:H,author:U,decode:D,key:K)->bool {
    let hint=model(author,hash);
    has_author(hint) && decode.ensures((hint.unwrap().unwrap(),),Ok(key))
}

pub(crate) open spec fn ready_at<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<Option<A>>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(attachment:&'s Att,hash:H,ops:&Operations<R,U,D,C,B>)->bool {
    let entries=bucket(attachment,hash,ops.entries);
    let hint=model(ops.author,hash);
    &&& ops.entries.requires((attachment,hash))
    &&& ops.entries.ensures((attachment,hash),entries)
    &&& forall|out:Bucket<V,N>| #[trigger] ops.entries.ensures((attachment,hash),out) ==> out==entries
    &&& entries.len>0 ==> modeled(ops.author,hash)
    &&& entries.len>0 && has_author(hint) ==> ops.decode.requires((hint.unwrap().unwrap(),))
    &&& forall|key:K| entries.len>0 && #[trigger] tentative_key(hash,ops.author,ops.decode,key)
        ==> ready_to_search(entries,&key,hash,ops.check)
}

pub(crate) open spec fn checked_tentative<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<Option<A>>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(attachment:&'s Att,hash:H,ops:&Operations<R,U,D,C,B>,w:W)->bool {
    let entries=bucket(attachment,hash,ops.entries);
    entries.len>0 && searched(attachment,hash,ops,Some(w))
}

pub(crate) open spec fn searched<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<Option<A>>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(attachment:&'s Att,hash:H,ops:&Operations<R,U,D,C,B>,result:Option<W>)->bool {
    exists|key:K| #[trigger] tentative_key(hash,ops.author,ops.decode,key) && found_in(bucket(attachment,hash,ops.entries),&key,hash,ops.check,result)
}

pub(crate) proof fn record_search<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<Option<A>>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(attachment:&'s Att,hash:H,ops:&Operations<R,U,D,C,B>,entries:Bucket<V,N>,key:K,result:Option<W>)
    requires entries==bucket(attachment,hash,ops.entries),entries.len>0,tentative_key(hash,ops.author,ops.decode,key),found_in(entries,&key,hash,ops.check,result),
    ensures searched(attachment,hash,ops,result),result.is_some() ==> checked_tentative(attachment,hash,ops,result.unwrap()),
{}

pub(crate) open spec fn early_step<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<Option<A>>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(attachment:&'s Att,hash:H,ops:&Operations<R,U,D,C,B>,before:S,after:S)->bool {
    &&& bucket(attachment,hash,ops.entries).len>0
    &&& if model(ops.author,hash).is_none() {after==before}
        else {exists|w:W| #[trigger] checked_tentative(attachment,hash,ops,w) && ops.retain.ensures((before,w),after)}
}

pub(crate) proof fn accepted<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<Option<A>>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(attachment:&'s Att,hash:H,ops:&Operations<R,U,D,C,B>,before:S,after:S,w:W)
    requires checked_tentative(attachment,hash,ops,w),ops.retain.ensures((before,w),after),
    ensures early_step(attachment,hash,ops,before,after),
{}

pub(crate) open spec fn error_at<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<Option<A>>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(attachment:&'s Att,hash:H,ops:&Operations<R,U,D,C,B>,failure:Failure<H,E>)->bool {
    let entries=bucket(attachment,hash,ops.entries);
    let hint=model(ops.author,hash);
    match failure {
        Failure::MissingSignature(h)=>h==hash && (entries.len==0 || searched(attachment,hash,ops,None)),
        Failure::MissingAuthor(h)=>h==hash && entries.len>0 && hint==Some(None),
        Failure::InvalidKey(h,e)=>h==hash && entries.len>0 && has_author(hint) && ops.decode.ensures((hint.unwrap().unwrap(),),Err(e)),
    }
}

pub(crate) open spec fn early_prefix<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<Option<A>>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(hashes:Seq<H>,attachment:&'s Att,initial:S,ops:&Operations<R,U,D,C,B>,states:Seq<S>,end:int)->bool {
    &&& 0<=end<=hashes.len()
    &&& states.len()==end+1 && states[0]==initial
    &&& forall|i:int| 0<=i<end ==> #[trigger] early_step(attachment,hashes[i],ops,states[i],states[i+1])
}

pub(crate) open spec fn early_ready<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<Option<A>>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(hashes:Seq<H>,attachment:&'s Att,initial:S,ops:&Operations<R,U,D,C,B>)->bool {
    &&& forall|i:int| 0<=i<hashes.len() ==> #[trigger] ready_at(attachment,hashes[i],ops)
    &&& forall|i:int,states:Seq<S>,w:W| 0<=i<hashes.len() && #[trigger] early_prefix(hashes,attachment,initial,ops,states,i)
        && #[trigger] checked_tentative(attachment,hashes[i],ops,w) ==> ops.retain.requires((states[i],w))
}

#[verifier::opaque]
pub(crate) open spec fn provisional_result<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<Option<A>>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(hashes:Seq<H>,attachment:&'s Att,initial:S,ops:&Operations<R,U,D,C,B>,result:Result<S,Failure<H,E>>)->bool {
    match result {
        Ok(store)=>exists|states:Seq<S>| #[trigger] early_prefix(hashes,attachment,initial,ops,states,hashes.len() as int) && states.last()==store,
        Err(failure)=>rejected(hashes,attachment,initial,ops,failure),
    }
}

pub(crate) open spec fn rejected<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<Option<A>>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(hashes:Seq<H>,attachment:&'s Att,initial:S,ops:&Operations<R,U,D,C,B>,failure:Failure<H,E>)->bool {
    exists|i:int,states:Seq<S>| 0<=i<hashes.len() && #[trigger] early_prefix(hashes,attachment,initial,ops,states,i)
        && error_at(attachment,hashes[i],ops,failure)
}

pub(crate) proof fn record<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<Option<A>>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(hashes:Seq<H>,attachment:&'s Att,initial:S,ops:&Operations<R,U,D,C,B>,states:Seq<S>,i:int,next:S)
    requires 0<=i<hashes.len(),early_prefix(hashes,attachment,initial,ops,states,i),early_step(attachment,hashes[i],ops,states[i],next),
    ensures early_prefix(hashes,attachment,initial,ops,states.push(next),i+1),
{
    let path=states.push(next);
    assert forall|j:int| 0<=j<i+1 implies #[trigger] early_step(attachment,hashes[j],ops,path[j],path[j+1]) by {
        if j<i { assert(early_step(attachment,hashes[j],ops,states[j],states[j+1])); }
    }
}

pub(crate) proof fn stop<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<Option<A>>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(hashes:Seq<H>,attachment:&'s Att,initial:S,ops:&Operations<R,U,D,C,B>,states:Seq<S>,i:int,failure:Failure<H,E>)
    requires 0<=i<hashes.len(),early_prefix(hashes,attachment,initial,ops,states,i),error_at(attachment,hashes[i],ops,failure),
    ensures provisional_result(hashes,attachment,initial,ops,Err(failure)),
{ reveal(provisional_result); assert(rejected(hashes,attachment,initial,ops,failure)); }

pub(crate) proof fn finish<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<Option<A>>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(hashes:Seq<H>,attachment:&'s Att,initial:S,ops:&Operations<R,U,D,C,B>,states:Seq<S>,store:S)
    requires early_prefix(hashes,attachment,initial,ops,states,hashes.len() as int),states[hashes.len() as int]==store,
    ensures provisional_result(hashes,attachment,initial,ops,Ok(store)),
{ reveal(provisional_result); }

} // verus!
