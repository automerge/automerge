use crate::candidate_origin::Candidate;
use crate::lookup_spec::{model, modeled};
use std::hash::Hash;
use vstd::{laws_eq::obeys_concrete_eq, prelude::*, std_specs::hash::obeys_key_model};

verus! {

pub(crate) open spec fn hash_laws<H:Eq+Hash>()->bool {obeys_key_model::<H>() && obeys_concrete_eq::<H>()}

pub(crate) open spec fn sources<'a,G:'a,T:'a,H,F:Fn(&'a Candidate<G>)->&'a[T],K:Fn(&'a T)->H>(candidates:Seq<Candidate<G>>,members:F,key:K)->bool {
    &&& forall|i:int| 0<=i<candidates.len() ==> #[trigger] modeled(members,&candidates[i])
    &&& forall|i:int,j:int| 0<=i<candidates.len() && 0<=j<model(members,&candidates[i])@.len() ==>
        #[trigger] modeled(key,&(model(members,&candidates[i])@)[j])
}

pub(crate) open spec fn before(a:int,b:int,i:int,j:int)->bool { a<i || (a==i && b<j) }

pub(crate) open spec fn at<'a,G:'a,T:'a,F:Fn(&'a Candidate<G>)->&'a[T]>(candidates:Seq<Candidate<G>>,members:F,a:int,b:int)->&'a T {
    &model(members,&candidates[a])@[b]
}

pub(crate) open spec fn source_pos<'a,G:'a,T:'a,H,F:Fn(&'a Candidate<G>)->&'a[T],K:Fn(&'a T)->H>(
    candidates:Seq<Candidate<G>>,members:F,key:K,a:int,b:int,i:int,j:int,hash:H,
)->bool {
    0<=a<candidates.len() && 0<=b<model(members,&candidates[a])@.len() && before(a,b,i,j)
        && model(key,at(candidates,members,a,b))==hash
}

pub(crate) open spec fn last<'a,G:'a,T:'a,H,F:Fn(&'a Candidate<G>)->&'a[T],K:Fn(&'a T)->H>(
    candidates:Seq<Candidate<G>>,members:F,key:K,a:int,b:int,i:int,j:int,hash:H,
)->bool {
    &&& source_pos(candidates,members,key,a,b,i,j,hash)
    &&& forall|x:int,y:int| #[trigger] source_pos(candidates,members,key,x,y,i,j,hash) ==> !before(a,b,x,y)
}

pub(crate) open spec fn indexed<'a,G:'a,T:'a,H,F:Fn(&'a Candidate<G>)->&'a[T],K:Fn(&'a T)->H>(
    candidates:Seq<Candidate<G>>,members:F,key:K,i:int,j:int,table:Map<H,&'a T>,
)->bool {
    &&& forall|hash:H| #[trigger] table.contains_key(hash) <==> exists|a:int,b:int| #[trigger] source_pos(candidates,members,key,a,b,i,j,hash)
    &&& forall|hash:H| #[trigger] table.contains_key(hash) ==> exists|a:int,b:int|
        #[trigger] last(candidates,members,key,a,b,i,j,hash) && table[hash]==at(candidates,members,a,b)
}

pub(crate) open spec fn covered_hash<'a,G:'a,T:'a,H,F:Fn(&'a Candidate<G>)->&'a[T],K:Fn(&'a T)->H>(candidates:Seq<Candidate<G>>,members:F,key:K,hash:H)->bool {
    exists|a:int,b:int| #[trigger] source_pos(candidates,members,key,a,b,candidates.len() as int,0,hash)
}

pub(crate) open spec fn covered<'a,G:'a,T:'a,H,F:Fn(&'a Candidate<G>)->&'a[T],K:Fn(&'a T)->H>(
    candidates:Seq<Candidate<G>>,schedule:Seq<H>,members:F,key:K,
)->bool {
    forall|i:int| 0<=i<schedule.len() ==> covered_hash(candidates,members,key,#[trigger] schedule[i])
}

pub(crate) open spec fn exact_payloads<'a,G:'a,T:'a,H,F:Fn(&'a Candidate<G>)->&'a[T],K:Fn(&'a T)->H>(
    candidates:Seq<Candidate<G>>,schedule:Seq<H>,members:F,key:K,out:Seq<&'a T>,
)->bool {
    &&& out.len()==schedule.len()
    &&& forall|i:int| 0<=i<out.len() ==> model(key,#[trigger] out[i])==schedule[i]
        && exists|a:int,b:int| #[trigger] last(candidates,members,key,a,b,candidates.len() as int,0,schedule[i])
            && out[i]==at(candidates,members,a,b)
}

pub(crate) proof fn add_payload<'a,G:'a,T:'a,H,F:Fn(&'a Candidate<G>)->&'a[T],K:Fn(&'a T)->H>(
    candidates:Seq<Candidate<G>>,members:F,key:K,i:int,j:int,old:Map<H,&'a T>,new:Map<H,&'a T>,
)
    requires 0<=i<candidates.len(),0<=j<model(members,&candidates[i])@.len(),
        indexed(candidates,members,key,i,j,old),new==old.insert(model(key,at(candidates,members,i,j)),at(candidates,members,i,j)),
    ensures indexed(candidates,members,key,i,j+1,new),
{
    let change=at(candidates,members,i,j);
    let hash=model(key,change);
    assert forall|h:H| #[trigger] new.contains_key(h) <==> exists|a:int,b:int| #[trigger] source_pos(candidates,members,key,a,b,i,j+1,h) by {
        if h==hash { assert(source_pos(candidates,members,key,i,j,i,j+1,hash)); }
        else if new.contains_key(h) {
            assert(old.contains_key(h));
            let (a,b)=choose|a:int,b:int| #[trigger] source_pos(candidates,members,key,a,b,i,j,h);
            assert(source_pos(candidates,members,key,a,b,i,j+1,h));
        }
        if exists|a:int,b:int| #[trigger] source_pos(candidates,members,key,a,b,i,j+1,h) {
            let (a,b)=choose|a:int,b:int| #[trigger] source_pos(candidates,members,key,a,b,i,j+1,h);
            if a==i && b==j { assert(h==hash); }
            else { assert(source_pos(candidates,members,key,a,b,i,j,h)); assert(old.contains_key(h)); }
        }
    }
    assert forall|h:H| #[trigger] new.contains_key(h) implies exists|a:int,b:int|
        #[trigger] last(candidates,members,key,a,b,i,j+1,h)&&new[h]==at(candidates,members,a,b) by {
        if h==hash {
            assert(source_pos(candidates,members,key,i,j,i,j+1,h));
            assert(last(candidates,members,key,i,j,i,j+1,h));
        } else {
            assert(old.contains_key(h));
            let (a,b)=choose|a:int,b:int| #[trigger] last(candidates,members,key,a,b,i,j,h)&&old[h]==at(candidates,members,a,b);
            assert forall|x:int,y:int| #[trigger] source_pos(candidates,members,key,x,y,i,j+1,h) implies !before(a,b,x,y) by {
                assert(source_pos(candidates,members,key,x,y,i,j,h));
            }
            assert(last(candidates,members,key,a,b,i,j+1,h));
        }
    }
}

pub(crate) proof fn finish_group<'a,G:'a,T:'a,H,F:Fn(&'a Candidate<G>)->&'a[T],K:Fn(&'a T)->H>(
    candidates:Seq<Candidate<G>>,members:F,key:K,i:int,table:Map<H,&'a T>,
)
    requires 0<=i<candidates.len(),indexed(candidates,members,key,i,model(members,&candidates[i])@.len() as int,table),
    ensures indexed(candidates,members,key,i+1,0,table),
{
    let end=model(members,&candidates[i])@.len() as int;
    assert forall|a:int,b:int,h:H| #[trigger] source_pos(candidates,members,key,a,b,i+1,0,h)
        <==> #[trigger] source_pos(candidates,members,key,a,b,i,end,h) by {};
    assert forall|h:H| #[trigger] table.contains_key(h) <==> exists|a:int,b:int| #[trigger] source_pos(candidates,members,key,a,b,i+1,0,h) by {
        if table.contains_key(h) {
            let (a,b)=choose|a:int,b:int| #[trigger] source_pos(candidates,members,key,a,b,i,end,h);
            assert(source_pos(candidates,members,key,a,b,i+1,0,h));
        }
        if exists|a:int,b:int| #[trigger] source_pos(candidates,members,key,a,b,i+1,0,h) {
            let (a,b)=choose|a:int,b:int| #[trigger] source_pos(candidates,members,key,a,b,i+1,0,h);
            assert(source_pos(candidates,members,key,a,b,i,end,h));
        }
    }
    assert forall|h:H| #[trigger] table.contains_key(h) implies exists|a:int,b:int|
        #[trigger] last(candidates,members,key,a,b,i+1,0,h) && table[h]==at(candidates,members,a,b) by {
        let (a,b)=choose|a:int,b:int| #[trigger] last(candidates,members,key,a,b,i,end,h) && table[h]==at(candidates,members,a,b);
        assert forall|x:int,y:int| #[trigger] source_pos(candidates,members,key,x,y,i+1,0,h) implies !before(a,b,x,y) by {
            assert(source_pos(candidates,members,key,x,y,i,end,h));
        }
        assert(last(candidates,members,key,a,b,i+1,0,h));
    }
}

pub(crate) proof fn finish_payloads<'a,G:'a,T:'a,H,F:Fn(&'a Candidate<G>)->&'a[T],K:Fn(&'a T)->H>(
    candidates:Seq<Candidate<G>>,schedule:Seq<H>,members:F,key:K,table:Map<H,&'a T>,out:Seq<&'a T>,
)
    requires covered(candidates,schedule,members,key),indexed(candidates,members,key,candidates.len() as int,0,table),
        out.len()==schedule.len(),forall|i:int| 0<=i<out.len() ==> #[trigger] out[i]==table[schedule[i]],
    ensures exact_payloads(candidates,schedule,members,key,out),
{
    assert forall|i:int| 0<=i<out.len() implies model(key,#[trigger] out[i])==schedule[i]
        && exists|a:int,b:int| #[trigger] last(candidates,members,key,a,b,candidates.len() as int,0,schedule[i])
        && out[i]==at(candidates,members,a,b) by {
        assert(table.contains_key(schedule[i]));
        let (a,b)=choose|a:int,b:int| #[trigger] last(candidates,members,key,a,b,candidates.len() as int,0,schedule[i])
            && table[schedule[i]]==at(candidates,members,a,b);
        assert(out[i]==at(candidates,members,a,b));
    }
}

} // verus!
