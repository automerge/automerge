use crate::lookup_spec::model;
use crate::scoped::{Bucket, Failure, Operations};
use crate::scoped_spec::*;
use vstd::prelude::*;

verus! {

#[verifier::reject_recursive_types(H)]
#[verifier::reject_recursive_types(A)]
#[verifier::reject_recursive_types(K)]
#[verifier::reject_recursive_types(V)]
#[verifier::reject_recursive_types(W)]
#[verifier::reject_recursive_types(S)]
pub(crate) ghost struct Meaning<H,A,K,V,W,S> {
    pub attached: spec_fn(H,V)->bool,
    pub historical_author: spec_fn(H)->Option<A>,
    pub key_owner: spec_fn(K)->A,
    pub exact_witness: spec_fn(W)->(H,A,V),
    pub verifies: spec_fn(H,A,V)->bool,
    pub stored: spec_fn(S)->Map<H,(A,V)>,
}

pub(crate) open spec fn read_typed<V,const N:usize>(entries:Bucket<V,N>,sig:V)->bool {
    exists|i:int| 0<=i<entries.len && #[trigger] entries.entries@[i]==Some(sig)
}

// Each contract is for one primitive operation, not for a whole frontier
// check. In particular a crypto success binds the parameters used in that
// call; it says nothing about whether they were the right parameters.
pub(crate) open spec fn full_bucket<V,const N:usize>(entries:Bucket<V,N>)->bool {
    &&& entries.len<=N
    &&& forall|i:int| 0<=i<entries.len ==> (#[trigger] entries.entries@[i]).is_some()
}

pub(crate) open spec fn primitives<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<A>,D:Fn(A)->Result<K,E>,C: Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(
    attachment:&'s Att,ops:&Operations<R,U,D,C,B>,meaning:Meaning<H,A,K,V,W,S>,
)->bool {
    &&& wire_primitives(attachment,ops,meaning)
    &&& forall|h:H| #[trigger] model(ops.author,h)==(meaning.historical_author)(h)
}

pub(crate) open spec fn wire_primitives<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U,D:Fn(A)->Result<K,E>,C: Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(
    attachment:&'s Att,ops:&Operations<R,U,D,C,B>,meaning:Meaning<H,A,K,V,W,S>,
)->bool {
    &&& forall|h:H| #[trigger] full_bucket(bucket(attachment,h,ops.entries))
    &&& forall|h:H,sig:V| #[trigger] read_typed(bucket(attachment,h,ops.entries),sig) <==> (meaning.attached)(h,sig)
    &&& forall|a:A,key:K| #[trigger] ops.decode.ensures((a,),Ok(key)) ==> (meaning.key_owner)(key)==a
    &&& forall|key:K,h:H,sig:V,w:W| #[trigger] ops.check.ensures((&key,h,sig),Some(w)) ==>
        (meaning.exact_witness)(w)==(h,(meaning.key_owner)(key),sig) && (meaning.verifies)(h,(meaning.key_owner)(key),sig)
    &&& forall|before:S,w:W,after:S| #[trigger] ops.retain.ensures((before,w),after) ==> {
        let (h,a,sig)=(meaning.exact_witness)(w);
        !(meaning.stored)(before).contains_key(h) ==> (meaning.stored)(after)==(meaning.stored)(before).insert(h,(a,sig))
    }
}

pub(crate) open spec fn local_witness<H,A,K,V,W,S>(meaning:Meaning<H,A,K,V,W,S>,hash:H,witness:W)->bool {
    let (h,author,sig)=(meaning.exact_witness)(witness);
    h==hash && (meaning.historical_author)(h)==Some(author) && (meaning.attached)(h,sig) && (meaning.verifies)(h,author,sig)
}

pub(crate) open spec fn exact_store<H,A,K,V,W,S>(hashes:Seq<H>,store:S,meaning:Meaning<H,A,K,V,W,S>)->bool {
    let evidence=(meaning.stored)(store);
    &&& evidence.dom() =~= hashes.to_set()
    &&& forall|h:H| #[trigger] evidence.contains_key(h) ==> {
        let (a,sig)=evidence[h];
        (meaning.historical_author)(h)==Some(a) && (meaning.attached)(h,sig) && (meaning.verifies)(h,a,sig)
    }
}

proof fn exact_parameters<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<A>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(attachment:&'s Att,hash:H,ops:&Operations<R,U,D,C,B>,meaning:Meaning<H,A,K,V,W,S>,witness:W)
    requires primitives(attachment,ops,meaning),frontier_ready(attachment,hash,ops),checked(attachment,hash,ops,witness),
    ensures local_witness(meaning,hash,witness),
{
    let entries=bucket(attachment,hash,ops.entries);
    let key=choose|key:K| #[trigger] key_for(hash,ops.author,ops.decode,key) && found_in(entries,&key,hash,ops.check,Some(witness));
    bind_exact_arguments(attachment,hash,ops,meaning,model(ops.author,hash).unwrap(),key,witness);
}

pub(crate) proof fn bind_exact_arguments<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(
    attachment:&'s Att,hash:H,ops:&Operations<R,U,D,C,B>,meaning:Meaning<H,A,K,V,W,S>,author:A,key:K,witness:W,
)
    requires wire_primitives(attachment,ops,meaning),ops.decode.ensures((author,),Ok(key)),
        ready_to_search(bucket(attachment,hash,ops.entries),&key,hash,ops.check),
        found_in(bucket(attachment,hash,ops.entries),&key,hash,ops.check,Some(witness)),
    ensures (meaning.exact_witness)(witness).0==hash, (meaning.exact_witness)(witness).1==author,
        (meaning.attached)(hash,(meaning.exact_witness)(witness).2),
        (meaning.verifies)(hash,author,(meaning.exact_witness)(witness).2),
{
    reveal(found_in);
    let entries=bucket(attachment,hash,ops.entries);
    let i=choose|i:int| #[trigger] first_hit(entries,&key,hash,ops.check,i,witness);
    let sig=entries.entries@[i].unwrap();
    assert(entries.entries@[i]==Some(sig));
    assert(read_typed(entries,sig));
    assert((meaning.attached)(hash,sig));
    assert(ready_to_search(entries,&key,hash,ops.check));
    assert(can_try(ops.check,&key,hash,sig));
    assert(ops.check.ensures((&key,hash,sig),Some(witness)));
    assert((meaning.key_owner)(key)==author);
}

proof fn sound_prefix<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<A>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(
    hashes:Seq<H>,attachment:&'s Att,initial:S,ops:&Operations<R,U,D,C,B>,meaning:Meaning<H,A,K,V,W,S>,states:Seq<S>,end:int,
)
    requires hashes.no_duplicates(),primitives(attachment,ops,meaning),batch_ready(hashes,attachment,initial,ops),
        (meaning.stored)(initial).is_empty(),verified_prefix(hashes,attachment,initial,ops,states,end),
    ensures exact_store(hashes.take(end),states[end],meaning),
    decreases end,
{
    if end>0 {
        let prior=states.drop_last();
        assert(verified_prefix(hashes,attachment,initial,ops,prior,end-1)) by {
            assert forall|i:int| 0<=i<end-1 implies #[trigger] retained_step(attachment,hashes[i],ops,prior[i],prior[i+1]) by {
                assert(retained_step(attachment,hashes[i],ops,states[i],states[i+1]));
            }
        }
        sound_prefix(hashes,attachment,initial,ops,meaning,prior,end-1);
        let hash=hashes[end-1];
        let before=states[end-1];
        let after=states[end];
        assert(retained_step(attachment,hash,ops,before,after));
        let w=choose|w:W| #[trigger] checked(attachment,hash,ops,w) && ops.retain.ensures((before,w),after);
        exact_parameters(attachment,hash,ops,meaning,w);
        assert(!hashes.take(end-1).contains(hash)) by {
            if hashes.take(end-1).contains(hash) {
                let j=choose|j:int| 0<=j<end-1 && #[trigger] hashes.take(end-1)[j]==hash;
                assert(hashes[j]==hashes[end-1]);
            }
        }
        assert(!(meaning.stored)(before).contains_key(hash));
        hashes.lemma_take_succ_push(end-1);
        hashes.take(end-1).lemma_push_to_set_commute(hash);
    } else {
        assert(hashes.take(0) =~= Seq::<H>::empty());
    }
}

/// The successful store returned by the checked production path contains
/// exactly one group-local, correctly authored witness at every frontier.
pub(crate) proof fn successful_store_is_exact<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<A>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(
    hashes:Seq<H>,attachment:&'s Att,initial:S,ops:&Operations<R,U,D,C,B>,meaning:Meaning<H,A,K,V,W,S>,store:S,
)
    requires hashes.no_duplicates(),primitives(attachment,ops,meaning),batch_ready(hashes,attachment,initial,ops),
        (meaning.stored)(initial).is_empty(),verification_result(hashes,attachment,initial,ops,Ok(store)),
    ensures exact_store(hashes,store,meaning),
{
    reveal(verification_result);
    let states=choose|states:Seq<S>| #[trigger] verified_prefix(hashes,attachment,initial,ops,states,hashes.len() as int)
        && states.last()==store;
    sound_prefix(hashes,attachment,initial,ops,meaning,states,hashes.len() as int);
    assert(hashes.take(hashes.len() as int) =~= hashes);
}

} // verus!
