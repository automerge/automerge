use crate::evidence_spec::{cache_reuse, evidence_exact, Group};
use crate::lookup_spec::{model, modeled};
use crate::scoped::{Bucket, Failure, Operations};
use crate::scoped_cache::{Owned, Scope};
use crate::scoped_soundness::{exact_store, primitives, successful_store_is_exact, Meaning};
use crate::scoped_spec::{batch_ready, verification_result};
use vstd::prelude::*;

verus! {

pub(crate) open spec fn known_owner<'s,A:'s,S:'s,H,O:Fn(&'s S,H)->Option<&'s A>>(owner:O,store:&'s S,hash:H)->Option<&'s A> {
    choose|value:Option<&'s A>| owner.ensures((store,hash),value)
}

pub(crate) open spec fn owner_ready<'s,A:'s,S:'s,H,O:Fn(&'s S,H)->Option<&'s A>>(owner:O,store:&'s S,hash:H)->bool {
    &&& owner.requires((store,hash))
    &&& owner.ensures((store,hash), known_owner(owner,store,hash))
    &&& forall|v:Option<&'s A>| #[trigger] owner.ensures((store,hash),v) ==> v==known_owner(owner,store,hash)
}

pub(crate) open spec fn cache_ready<'s,H:'s,Att:'s,S:'s,A:'s,R:Fn(H)->Option<A>,O:Fn(&'s S,H)->Option<&'s A>>(scope:&Scope<'s,H,Att,S>,author:R,owner:O)->bool {
    &&& forall|i:int| 0<=i<scope.hashes@.len() ==> modeled(author,#[trigger] scope.hashes@[i])
    &&& forall|i:int| 0<=i<scope.hashes@.len() && model(author,scope.hashes@[i]).is_some()
        ==> #[trigger] owner_ready(owner,scope.cached,scope.hashes@[i])
}

pub(crate) open spec fn matches_author<'s,A:'s,S:'s,H,R:Fn(H)->Option<A>,O:Fn(&'s S,H)->Option<&'s A>>(store:&'s S,author:R,owner:O,hash:H)->bool {
    match (model(author,hash),known_owner(owner,store,hash)) { (Some(a),Some(cached))=>a==*cached, _=>false }
}

pub(crate) open spec fn prefix_matches<'s,H:'s,Att:'s,S:'s,A:'s,R:Fn(H)->Option<A>,O:Fn(&'s S,H)->Option<&'s A>>(scope:&Scope<'s,H,Att,S>,author:R,owner:O,end:int)->bool {
    forall|i:int| 0<=i<end ==> matches_author(scope.cached,author,owner,#[trigger] scope.hashes@[i])
}

pub(crate) open spec fn cache_ok<'s,H:'s,Att:'s,S:'s,A:'s,R:Fn(H)->Option<A>,O:Fn(&'s S,H)->Option<&'s A>>(scope:&Scope<'s,H,Att,S>,author:R,owner:O)->bool {
    prefix_matches(scope,author,owner,scope.hashes@.len() as int)
}

pub(crate) open spec fn settled<'s,H:'s,Att:'s,S:'s,A:'s,E,K,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<A>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,O:Fn(&'s S,H)->Option<&'s A>,Copy:FnOnce(&'s S)->S,const N:usize>(
    scope:&Scope<'s,H,Att,S>,empty:S,ops:&Operations<R,U,D,C,B>,owner:O,copy:Copy,result:Result<S,Failure<H,E>>,
)->bool {
    if cache_ok(scope,ops.author,owner) {
        match result {Ok(store)=>copy.ensures((scope.cached,),store),Err(_)=>false}
    } else {
        verification_result(scope.hashes@,scope.attachment,empty,ops,result)
    }
}

pub(crate) open spec fn prepared_scope<H,Att,S,E,F:FnOnce(&Vec<H>,&Att)->Result<S,E>>(hashes:Vec<H>,attachment:Att,preverify:F,out:Result<Owned<H,Att,S>,E>)->bool {
    match out {
        Ok(owned)=>owned.hashes==hashes && owned.attachment==attachment && preverify.ensures((&hashes,&attachment),Ok(owned.cached)),
        Err(error)=>preverify.ensures((&hashes,&attachment),Err(error)),
    }
}

// Compose the checked choice with the trusted individual store reads and
// clone. The preverification theorem provides the early evidence premise;
// it need not predict the eventual author, which is tested by the choice.
pub(crate) proof fn reused_store_is_exact<'s,H:'s,Att:'s,S:'s,A:'s,V,R:Fn(H)->Option<A>,O:Fn(&'s S,H)->Option<&'s A>,C:FnOnce(&'s S)->S>(
    scope:&Scope<'s,H,Att,S>,author:R,owner:O,copy:C,early:Group<H,A,V,S>,settled:Group<H,A,V,S>,store:S,
)
    requires copy.ensures((scope.cached,),store),cache_ok(scope,author,owner),
        evidence_exact(early,scope.hashes@,scope.hashes@.len() as int,*scope.cached),
        early.retained==settled.retained,early.attached==settled.attached,early.valid==settled.valid,
        early.has_supported_entry==settled.has_supported_entry,
        forall|h:H| #[trigger] scope.hashes@.contains(h) ==> (settled.required_now)(h)
            && model(author,h)==(settled.established_author)(h)
            && {
                let actual=(early.retained)(*scope.cached);
                match known_owner(owner,scope.cached,h) {Some(a)=>actual.contains_key(h) && actual[h].0==*a, None=>!actual.contains_key(h)}
            },
        forall|cloned:S| #[trigger] copy.ensures((scope.cached,),cloned) ==>
            (settled.retained)(cloned)==(settled.retained)(*scope.cached),
    ensures evidence_exact(settled,scope.hashes@,scope.hashes@.len() as int,store),
{
    let hashes=scope.hashes@;
    assert forall|h:H| #[trigger] hashes.contains(h) implies {
        let entries=(early.retained)(*scope.cached);
        entries.contains_key(h) && (settled.established_author)(h)==Some(entries[h].0)
    } by {
        let i=choose|i:int| 0<=i<hashes.len() && #[trigger] hashes[i]==h;
        assert(matches_author(scope.cached,author,owner,hashes[i]));
    }
    cache_reuse(early,settled,hashes,*scope.cached);
    assert(copy.ensures((scope.cached,),store));
}

pub(crate) open spec fn finished_group<H,A,K,V,W,S>(meaning:Meaning<H,A,K,V,W,S>)->Group<H,A,V,S> {
    Group {
        attached: meaning.attached,
        has_supported_entry: |h:H| exists|s:V| #[trigger] (meaning.attached)(h,s),
        established_author: meaning.historical_author,
        valid: meaning.verifies,
        required_now: |h:H| true,
        retained: meaning.stored,
    }
}

/// The result of the production settlement driver is exact regardless of
/// which branch it used. No whole-group success contract is needed for the
/// fallback: that branch invokes the checked fresh verification algorithm.
pub(crate) proof fn successful_settlement_is_exact<'s,H:'s,Att:'s,S:'s,A:'s,E,K,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<A>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,O:Fn(&'s S,H)->Option<&'s A>,Copy:FnOnce(&'s S)->S,const N:usize>(
    scope:&Scope<'s,H,Att,S>,empty:S,ops:&Operations<R,U,D,C,B>,owner:O,copy:Copy,
    tentative:Group<H,A,V,S>,meaning:Meaning<H,A,K,V,W,S>,store:S,
)
    requires settled(scope,empty,ops,owner,copy,Ok(store)),
        primitives(scope.attachment,ops,meaning),
        !cache_ok(scope,ops.author,owner) ==> {
            &&& scope.hashes@.no_duplicates()
            &&& (meaning.stored)(empty).is_empty()
            &&& batch_ready(scope.hashes@,scope.attachment,empty,ops)
        },
        cache_ok(scope,ops.author,owner) ==> {
            &&& evidence_exact(tentative,scope.hashes@,scope.hashes@.len() as int,*scope.cached)
            &&& tentative.attached==meaning.attached && tentative.valid==meaning.verifies && tentative.retained==meaning.stored
            &&& tentative.has_supported_entry==finished_group(meaning).has_supported_entry
            &&& forall|h:H| #[trigger] scope.hashes@.contains(h) ==> {
                let entries=(meaning.stored)(*scope.cached);
                match known_owner(owner,scope.cached,h) { Some(a)=>entries.contains_key(h) && entries[h].0==*a, None=>!entries.contains_key(h) }
            }
            &&& forall|cloned:S| #[trigger] copy.ensures((scope.cached,),cloned) ==>
                (meaning.stored)(cloned)==(meaning.stored)(*scope.cached)
        },
    ensures exact_store(scope.hashes@,store,meaning),
{
    if cache_ok(scope,ops.author,owner) {
        reused_store_is_exact(scope,ops.author,owner,copy,tentative,finished_group(meaning),store);
        assert(scope.hashes@.take(scope.hashes@.len() as int) =~= scope.hashes@);
    } else {
        successful_store_is_exact(scope.hashes@,scope.attachment,empty,ops,meaning,store);
    }
}

} // verus!
