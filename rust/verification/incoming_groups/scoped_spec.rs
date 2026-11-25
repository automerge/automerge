use crate::lookup_spec::{model, modeled};
use crate::scoped::{Bucket, Failure, Operations};
use vstd::prelude::*;

verus! {

pub(crate) open spec fn attempt<K,H,V,W,C:Fn(&K,H,V)->Option<W>>(check:C,key:&K,hash:H,sig:V)->Option<W> {
    choose|out:Option<W>| check.ensures((key,hash,sig),out)
}

pub(crate) open spec fn can_try<K,H,V,W,C:Fn(&K,H,V)->Option<W>>(check:C,key:&K,hash:H,sig:V)->bool {
    &&& check.requires((key,hash,sig))
    &&& check.ensures((key,hash,sig),attempt(check,key,hash,sig))
    &&& forall|out:Option<W>| #[trigger] check.ensures((key,hash,sig),out) ==> out==attempt(check,key,hash,sig)
}

pub(crate) open spec fn ready_to_search<K,H,V,W,C:Fn(&K,H,V)->Option<W>,const N:usize>(entries:Bucket<V,N>,key:&K,hash:H,check:C)->bool {
    &&& entries.len<=N
    &&& forall|i:int| 0<=i<entries.len ==> match #[trigger] entries.entries@[i] {
        Some(sig)=>can_try(check,key,hash,sig), None=>true,
    }
}

pub(crate) open spec fn no_hit<K,H,V,W,C:Fn(&K,H,V)->Option<W>,const N:usize>(entries:Bucket<V,N>,key:&K,hash:H,check:C,end:int)->bool {
    forall|i:int| 0<=i<end ==> match #[trigger] entries.entries@[i] {
        Some(sig)=>attempt(check,key,hash,sig).is_none(), None=>true,
    }
}

pub(crate) open spec fn first_hit<K,H,V,W,C:Fn(&K,H,V)->Option<W>,const N:usize>(entries:Bucket<V,N>,key:&K,hash:H,check:C,i:int,w:W)->bool {
    &&& 0<=i<entries.len
    &&& entries.entries@[i].is_some()
    &&& attempt(check,key,hash,entries.entries@[i].unwrap())==Some(w)
    &&& no_hit(entries,key,hash,check,i)
}

pub(crate) open spec fn witnessed<K,H,V,W,C:Fn(&K,H,V)->Option<W>,const N:usize>(entries:Bucket<V,N>,key:&K,hash:H,check:C,w:W)->bool {
    exists|i:int| #[trigger] first_hit(entries,key,hash,check,i,w)
}

#[verifier::opaque]
pub(crate) open spec fn found_in<K,H,V,W,C:Fn(&K,H,V)->Option<W>,const N:usize>(entries:Bucket<V,N>,key:&K,hash:H,check:C,out:Option<W>)->bool {
    match out {
        None=> no_hit(entries,key,hash,check,entries.len as int),
        Some(w)=>witnessed(entries,key,hash,check,w),
    }
}

pub(crate) proof fn report_hit<K,H,V,W,C:Fn(&K,H,V)->Option<W>,const N:usize>(entries:Bucket<V,N>,key:&K,hash:H,check:C,i:int,w:W)
    requires first_hit(entries,key,hash,check,i,w),
    ensures found_in(entries,key,hash,check,Some(w)),
{
    assert(witnessed(entries,key,hash,check,w));
    reveal(found_in);
}

pub(crate) open spec fn bucket<'s,Att:'s,H,V,R:Fn(&'s Att,H)->Bucket<V,N>,const N:usize>(attachment:&'s Att,hash:H,entries:R)->Bucket<V,N> {
    choose|out:Bucket<V,N>| entries.ensures((attachment,hash),out)
}

pub(crate) open spec fn key_for<H,A,K,E,U:Fn(H)->Option<A>,D:Fn(A)->Result<K,E>>(hash:H,author:U,decode:D,key:K)->bool {
    model(author,hash).is_some() && decode.ensures((model(author,hash).unwrap(),),Ok(key))
}

pub(crate) open spec fn frontier_ready<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<A>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(
    attachment:&'s Att,hash:H,ops:&Operations<R,U,D,C,B>,
)->bool {
    let entries=bucket(attachment,hash,ops.entries);
    &&& ops.entries.requires((attachment,hash))
    &&& ops.entries.ensures((attachment,hash),entries)
    &&& forall|out:Bucket<V,N>| #[trigger] ops.entries.ensures((attachment,hash),out) ==> out==entries
    &&& entries.present ==> modeled(ops.author,hash)
    &&& entries.present && model(ops.author,hash).is_some() ==> ops.decode.requires((model(ops.author,hash).unwrap(),))
    &&& forall|k:K| entries.present && #[trigger] key_for(hash,ops.author,ops.decode,k) ==> ready_to_search(entries,&k,hash,ops.check)
}

pub(crate) open spec fn checked<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<A>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(
    attachment:&'s Att,hash:H,ops:&Operations<R,U,D,C,B>,witness:W,
)->bool {
    let entries=bucket(attachment,hash,ops.entries);
    entries.present && exists|key:K| #[trigger] key_for(hash,ops.author,ops.decode,key)
        && found_in(entries,&key,hash,ops.check,Some(witness))
}

pub(crate) open spec fn unchecked_bucket<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<A>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(
    attachment:&'s Att,hash:H,ops:&Operations<R,U,D,C,B>,
)->bool {
    let entries=bucket(attachment,hash,ops.entries);
    exists|key:K| #[trigger] key_for(hash,ops.author,ops.decode,key) && found_in(entries,&key,hash,ops.check,None)
}

pub(crate) open spec fn frontier_error<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<A>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(
    attachment:&'s Att,hash:H,ops:&Operations<R,U,D,C,B>,error:Failure<H,E>,
)->bool {
    let present=bucket(attachment,hash,ops.entries).present;
    let author=model(ops.author,hash);
    match error {
        Failure::MissingSignature(h)=>h==hash && (!present || unchecked_bucket(attachment,hash,ops)),
        Failure::MissingAuthor(h)=>h==hash && present && author.is_none(),
        Failure::InvalidKey(h,e)=>h==hash && present && author.is_some() && ops.decode.ensures((author.unwrap(),),Err(e)),
    }
}

pub(crate) proof fn missing_with_key<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<A>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(attachment:&'s Att,hash:H,ops:&Operations<R,U,D,C,B>,key:K,entries:Bucket<V,N>)
    requires entries==bucket(attachment,hash,ops.entries), key_for(hash,ops.author,ops.decode,key), found_in(entries,&key,hash,ops.check,None),
    ensures frontier_error(attachment,hash,ops,Failure::MissingSignature(hash)),
{
    assert(unchecked_bucket(attachment,hash,ops));
}

pub(crate) proof fn checked_with_key<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<A>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(attachment:&'s Att,hash:H,ops:&Operations<R,U,D,C,B>,key:K,entries:Bucket<V,N>,witness:W)
    requires entries==bucket(attachment,hash,ops.entries), entries.present, key_for(hash,ops.author,ops.decode,key),
        found_in(entries,&key,hash,ops.check,Some(witness)),
    ensures checked(attachment,hash,ops,witness),
{}

pub(crate) open spec fn retained_step<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<A>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(attachment:&'s Att,hash:H,ops:&Operations<R,U,D,C,B>,before:S,after:S)->bool {
    exists|w:W| #[trigger] checked(attachment,hash,ops,w) && ops.retain.ensures((before,w),after)
}

pub(crate) open spec fn verified_prefix<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<A>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(hashes:Seq<H>,attachment:&'s Att,initial:S,ops:&Operations<R,U,D,C,B>,states:Seq<S>,end:int)->bool {
    &&& 0<=end<=hashes.len()
    &&& states.len()==end+1 && states[0]==initial
    &&& forall|i:int| 0<=i<end ==> #[trigger] retained_step(attachment,hashes[i],ops,states[i],states[i+1])
}

pub(crate) open spec fn batch_ready<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<A>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(hashes:Seq<H>,attachment:&'s Att,initial:S,ops:&Operations<R,U,D,C,B>)->bool {
    &&& forall|i:int| 0<=i<hashes.len() ==> #[trigger] frontier_ready(attachment,hashes[i],ops)
    &&& forall|i:int,states:Seq<S>,w:W| 0<=i<hashes.len() && #[trigger] verified_prefix(hashes,attachment,initial,ops,states,i)
        && #[trigger] checked(attachment,hashes[i],ops,w) ==> ops.retain.requires((states[i],w))
}

#[verifier::opaque]
pub(crate) open spec fn verification_result<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<A>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(hashes:Seq<H>,attachment:&'s Att,initial:S,ops:&Operations<R,U,D,C,B>,out:Result<S,Failure<H,E>>)->bool {
    match out {
        Ok(store)=>exists|states:Seq<S>| #[trigger] verified_prefix(hashes,attachment,initial,ops,states,hashes.len() as int) && states.last()==store,
        Err(failure)=>rejected(hashes,attachment,initial,ops,failure),
    }
}

pub(crate) open spec fn rejected<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<A>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(hashes:Seq<H>,attachment:&'s Att,initial:S,ops:&Operations<R,U,D,C,B>,failure:Failure<H,E>)->bool {
    exists|i:int, states:Seq<S>| 0<=i<hashes.len() && #[trigger] verified_prefix(hashes,attachment,initial,ops,states,i)
        && frontier_error(attachment,hashes[i],ops,failure)
}

pub(crate) proof fn extend_verified<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<A>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(hashes:Seq<H>,attachment:&'s Att,initial:S,ops:&Operations<R,U,D,C,B>,states:Seq<S>,i:int,next:S)
    requires 0<=i<hashes.len(),verified_prefix(hashes,attachment,initial,ops,states,i),
        retained_step(attachment,hashes[i],ops,states[i],next),
    ensures verified_prefix(hashes,attachment,initial,ops,states.push(next),i+1),
{
    let path=states.push(next);
    assert forall|j:int| 0<=j<i+1 implies #[trigger] retained_step(attachment,hashes[j],ops,path[j],path[j+1]) by {
        if j<i { assert(retained_step(attachment,hashes[j],ops,states[j],states[j+1])); }
    }
}

pub(crate) proof fn stop<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<A>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(hashes:Seq<H>,attachment:&'s Att,initial:S,ops:&Operations<R,U,D,C,B>,states:Seq<S>,i:int,failure:Failure<H,E>)
    requires 0<=i<hashes.len(),verified_prefix(hashes,attachment,initial,ops,states,i),frontier_error(attachment,hashes[i],ops,failure),
    ensures verification_result(hashes,attachment,initial,ops,Err(failure)),
{
    reveal(verification_result);
    assert(rejected(hashes,attachment,initial,ops,failure));
}

pub(crate) proof fn finish<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<A>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(hashes:Seq<H>,attachment:&'s Att,initial:S,ops:&Operations<R,U,D,C,B>,states:Seq<S>,store:S)
    requires verified_prefix(hashes,attachment,initial,ops,states,hashes.len() as int),states[hashes.len() as int]==store,
    ensures verification_result(hashes,attachment,initial,ops,Ok(store)),
{ reveal(verification_result); }

} // verus!
