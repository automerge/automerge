use crate::lookup_spec::{model, modeled};
use crate::snapshot_index::Keep;
use vstd::prelude::*;

verus! {

broadcast use vstd::seq_lib::group_seq_properties;

pub(crate) open spec fn views<'g,'m,G:'g,T:'m,K,V,M:Fn(&'g G)->&'m[T],P:Fn(&'m T)->Option<(K,V)>>(
    groups:Seq<G>,members:M,project:P,
)->bool {
    &&& forall|i:int| 0<=i<groups.len() ==> #[trigger] modeled(members,&groups[i])
    &&& forall|i:int,j:int| 0<=i<groups.len() && 0<=j<model(members,&groups[i])@.len()
        ==> #[trigger] modeled(project,&(model(members,&groups[i])@)[j])
}

pub(crate) open spec fn row<'m,T:'m,K,V,P:Fn(&'m T)->Option<(K,V)>>(changes:Seq<T>,project:P)->Seq<Option<(K,V)>> {
    Seq::new(changes.len(),|i:int| model(project,&changes[i]))
}

#[verifier::opaque]
pub(crate) open spec fn flatten<'g,'m,G:'g,T:'m,K,V,M:Fn(&'g G)->&'m[T],P:Fn(&'m T)->Option<(K,V)>>(
    groups:Seq<G>,members:M,project:P,end:int,
)->Seq<Option<(K,V)>>
    decreases end,
{
    if end<=0 {Seq::empty()} else {flatten(groups,members,project,end-1)+row(model(members,&groups[end-1])@,project)}
}

pub(crate) open spec fn has_key<K,V>(entry:Option<(K,V)>,key:K)->bool {
    match entry {Some((k,_))=>k==key,None=>false}
}

pub(crate) open spec fn chosen<K,V>(offers:Seq<Option<(K,V)>>,keep:Keep,index:int,key:K,value:V)->bool {
    &&& 0<=index<offers.len() && offers[index]==Some((key,value))
    &&& forall|j:int| 0<=j<offers.len() && (if keep is First {j<index} else {j>index}) ==> !has_key(#[trigger] offers[j],key)
}

pub(crate) open spec fn indexed<K,V>(offers:Seq<Option<(K,V)>>,keep:Keep,dict:Map<K,V>)->bool {
    &&& forall|i:int| 0<=i<offers.len() ==> match #[trigger] offers[i] {Some((k,_))=>dict.contains_key(k), None=>true}
    &&& forall|k:K| #[trigger] dict.contains_key(k) ==> exists|i:int| #[trigger] chosen(offers,keep,i,k,dict[k])
}

pub(crate) proof fn complete_group<'g,'m,G:'g,T:'m,K,V,M:Fn(&'g G)->&'m[T],P:Fn(&'m T)->Option<(K,V)>>(
    groups:Seq<G>,members:M,project:P,i:int,
)
    requires 0<=i<groups.len(),
    ensures flatten(groups,members,project,i+1) =~= flatten(groups,members,project,i)+row(model(members,&groups[i])@,project),
{ reveal(flatten); }

proof fn add_offer<K,V>(offers:Seq<Option<(K,V)>>,keep:Keep,before:Map<K,V>,out:Map<K,V>,entry:Option<(K,V)>)
    requires indexed(offers,keep,before),
        out==match entry {Some((k,v)) if keep is Last || !before.contains_key(k)=>before.insert(k,v),_=>before},
    ensures indexed(offers.push(entry),keep,out),
{
    let next=offers.push(entry);
    assert forall|i:int| 0<=i<next.len() implies match #[trigger] next[i] {Some((k,_))=>out.contains_key(k),None=>true} by {
        if i<offers.len() { assert(next[i]==offers[i]); }
    }
    assert forall|k:K| #[trigger] out.contains_key(k) implies exists|i:int| #[trigger] chosen(next,keep,i,k,out[k]) by {
        if has_key(entry,k) && (keep is Last || !before.contains_key(k)) {
            let i=offers.len() as int;
            if keep is First {
                assert forall|j:int| 0<=j<i implies !has_key(#[trigger] offers[j],k) by {
                    if let Some((actor,_))=offers[j] {assert(before.contains_key(actor));}
                }
            }
            assert(chosen(next,keep,i,k,out[k]));
        } else {
            assert(before.contains_key(k));
            let i=choose|i:int| #[trigger] chosen(offers,keep,i,k,before[k]);
            assert(out[k]==before[k]);
            assert(chosen(next,keep,i,k,out[k]));
        }
    }
}

pub(crate) proof fn record_binding<'m,T:'m,K,V,P:Fn(&'m T)->Option<(K,V)>>(
    prior:Seq<Option<(K,V)>>,changes:Seq<T>,project:P,j:int,keep:Keep,before:Map<K,V>,out:Map<K,V>,
)
    requires 0<=j<changes.len(),indexed(prior+row(changes,project).take(j),keep,before),
        out==match model(project,&changes[j]) {Some((k,v)) if keep is Last || !before.contains_key(k)=>before.insert(k,v),_=>before},
    ensures indexed(prior+row(changes,project).take(j+1),keep,out),
{
    let offers=prior+row(changes,project).take(j);
    let entry=model(project,&changes[j]);
    row(changes,project).lemma_take_succ_push(j);
    assert(offers.push(entry) =~= prior+row(changes,project).take(j+1));
    add_offer(offers,keep,before,out,entry);
}

} // verus!
