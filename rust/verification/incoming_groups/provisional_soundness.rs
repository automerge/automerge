use crate::evidence_spec::{evidence_exact, Group};
use crate::lookup_spec::model;
use crate::provisional_spec::*;
use crate::scoped::{Bucket, Failure, Operations};
use crate::scoped_soundness::{
    bind_exact_arguments, full_bucket, read_typed, wire_primitives, Meaning,
};
use crate::scoped_spec::{bucket, found_in};
use vstd::prelude::*;

verus! {

broadcast use vstd::seq_lib::group_seq_properties;

pub(crate) open spec fn partial_group<H,A,K,V,W,S,U:Fn(H)->Option<Option<A>>>(meaning:Meaning<H,A,K,V,W,S>,tentative:U)->Group<H,A,V,S> {
    Group {
        attached: meaning.attached,
        has_supported_entry: |h:H| exists|sig:V| #[trigger] (meaning.attached)(h,sig),
        established_author: meaning.historical_author,
        valid: meaning.verifies,
        required_now: |h:H| has_author(model(tentative,h)),
        retained: meaning.stored,
    }
}

pub(crate) open spec fn hint_identity<A>(hint: Option<Option<A>>) -> Option<A> {
    match hint { Some(author) => author, None => None }
}

pub(crate) open spec fn provisional_primitives<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<Option<A>>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(attachment:&'s Att,ops:&Operations<R,U,D,C,B>,meaning:Meaning<H,A,K,V,W,S>)->bool {
    &&& wire_primitives(attachment,ops,meaning)
    &&& forall|h:H| hint_identity(#[trigger] model(ops.author,h))==(meaning.historical_author)(h)
}

proof fn typed_presence<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<Option<A>>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(attachment:&'s Att,hash:H,ops:&Operations<R,U,D,C,B>,meaning:Meaning<H,A,K,V,W,S>)
    requires wire_primitives(attachment,ops,meaning),bucket(attachment,hash,ops.entries).len>0,
    ensures (partial_group(meaning,ops.author).has_supported_entry)(hash),
{
    let entries=bucket(attachment,hash,ops.entries);
    assert(full_bucket(entries));
    assert(entries.entries@[0].is_some());
    let sig=entries.entries@[0].unwrap();
    assert(read_typed(entries,sig));
    assert((meaning.attached)(hash,sig));
}

proof fn provisional_step_is_local<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<Option<A>>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(
    attachment:&'s Att,hash:H,ops:&Operations<R,U,D,C,B>,meaning:Meaning<H,A,K,V,W,S>,before:S,after:S,
)
    requires provisional_primitives(attachment,ops,meaning),ready_at(attachment,hash,ops),early_step(attachment,hash,ops,before,after),
        !(meaning.stored)(before).contains_key(hash),
    ensures crate::evidence_spec::local_step(partial_group(meaning,ops.author),hash,before,after),
{
    typed_presence(attachment,hash,ops,meaning);
    if model(ops.author,hash).is_some() {
        let w=choose|w:W| #[trigger] checked_tentative(attachment,hash,ops,w) && ops.retain.ensures((before,w),after);
        assert(searched(attachment,hash,ops,Some(w)));
        let entries=bucket(attachment,hash,ops.entries);
        let key=choose|k:K| #[trigger] tentative_key(hash,ops.author,ops.decode,k) && found_in(entries,&k,hash,ops.check,Some(w));
        let author=model(ops.author,hash).unwrap().unwrap();
        bind_exact_arguments(attachment,hash,ops,meaning,author,key,w);
        assert(has_author(model(ops.author,hash)));
        assert((meaning.historical_author)(hash)==Some(author));
    }
}

proof fn partial_prefix<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<Option<A>>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(
    hashes:Seq<H>,attachment:&'s Att,initial:S,ops:&Operations<R,U,D,C,B>,meaning:Meaning<H,A,K,V,W,S>,states:Seq<S>,end:int,
)
    requires hashes.no_duplicates(),provisional_primitives(attachment,ops,meaning),early_ready(hashes,attachment,initial,ops),
        (meaning.stored)(initial).is_empty(),early_prefix(hashes,attachment,initial,ops,states,end),
    ensures evidence_exact(partial_group(meaning,ops.author),hashes,end,states[end]),
    decreases end,
{
    let group=partial_group(meaning,ops.author);
    if end>0 {
        let prior=states.drop_last();
        assert(prior.len()==end);
        assert(early_prefix(hashes,attachment,initial,ops,prior,end-1)) by {
            assert forall|i:int| 0<=i<end-1 implies #[trigger] early_step(attachment,hashes[i],ops,prior[i],prior[i+1]) by {
                assert(early_step(attachment,hashes[i],ops,states[i],states[i+1]));
            }
        }
        partial_prefix(hashes,attachment,initial,ops,meaning,prior,end-1);
        let hash=hashes[end-1];
        assert(!hashes.take(end-1).contains(hash)) by {
            if hashes.take(end-1).contains(hash) {
                let j=choose|j:int| 0<=j<end-1 && #[trigger] hashes.take(end-1)[j]==hash;
                assert(hashes[j]==hashes[end-1]);
            }
        }
        let before=states[end-1];
        let after=states[end];
        assert(prior[end-1]==before);
        assert(evidence_exact(group,hashes,end-1,before));
        assert(!(group.retained)(before).contains_key(hash));
        provisional_step_is_local(attachment,hash,ops,meaning,before,after);
        hashes.lemma_take_succ_push(end-1);
    } else {
        assert(hashes.take(0) =~= Seq::<H>::empty());
    }
}

/// A successful preverification has a typed attachment at every frontier
/// and exactly the evidence whose provisional author could be established.
/// No author resolution assumption stronger than the supplied hint is used.
pub(crate) proof fn successful_preverification_is_exact<'s,Att:'s,H,A,K,E,S,V,W,R:Fn(&'s Att,H)->Bucket<V,N>,U:Fn(H)->Option<Option<A>>,D:Fn(A)->Result<K,E>,C:Fn(&K,H,V)->Option<W>,B:Fn(S,W)->S,const N:usize>(
    hashes:Seq<H>,attachment:&'s Att,initial:S,ops:&Operations<R,U,D,C,B>,meaning:Meaning<H,A,K,V,W,S>,store:S,
)
    requires hashes.no_duplicates(),provisional_primitives(attachment,ops,meaning),early_ready(hashes,attachment,initial,ops),
        (meaning.stored)(initial).is_empty(),provisional_result(hashes,attachment,initial,ops,Ok(store)),
    ensures evidence_exact(partial_group(meaning,ops.author),hashes,hashes.len() as int,store),
{
    reveal(provisional_result);
    let states=choose|states:Seq<S>| #[trigger] early_prefix(hashes,attachment,initial,ops,states,hashes.len() as int)
        && states.last()==store;
    partial_prefix(hashes,attachment,initial,ops,meaning,states,hashes.len() as int);
}

} // verus!
