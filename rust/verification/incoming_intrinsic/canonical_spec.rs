use crate::canonical_shape::Shape;
use crate::lookup_spec::{model, modeled};
use vstd::prelude::*;

verus! {

pub(crate) open spec fn every_level<K,F:Fn(K)->usize>(members:Seq<K>,level:F)->bool {
    forall|i:int| 0<=i<members.len() ==> modeled(level,#[trigger] members[i])
}

pub(crate) open spec fn checkpoint_set<K,F:Fn(K)->usize>(members:Seq<K>,head:K,level:F,end:int,out:Seq<K>)->bool {
    out =~= members.take(end).filter(|k:K| k!=head && model(level,k)>0)
}

pub(crate) proof fn checkpoint_next<K,F:Fn(K)->usize>(members:Seq<K>,head:K,level:F,i:int,prior:Seq<K>,out:Seq<K>)
    requires 0<=i<members.len(), checkpoint_set(members,head,level,i,prior),
        out==if members[i]!=head && model(level,members[i])>0 { prior.push(members[i]) } else { prior },
    ensures checkpoint_set(members,head,level,i+1,out),
{
    members.lemma_take_succ_push(i);
    members.take(i).lemma_filter_push(members[i], |k:K| k!=head && model(level,k)>0);
}

pub(crate) open spec fn produced_boundary<K,L:Fn(K)->usize,D:FnOnce(K)->Vec<K>,B:FnOnce(Vec<K>,usize)->Vec<K>>(
    head:K,level:L,direct:D,boundary:B,cut:Vec<K>,
)->bool {
    exists|parents:Vec<K>| #[trigger] direct.ensures((head,),parents) && boundary.ensures((parents,model(level,head)),cut)
}

pub(crate) open spec fn can_assemble<K,L:Fn(K)->usize,D:FnOnce(K)->Vec<K>,B:FnOnce(Vec<K>,usize)->Vec<K>,M:FnOnce(&Vec<K>)->Vec<K>>(
    head:K,level:L,direct:D,boundary:B,members:M,
)->bool {
    &&& modeled(level,head)
    &&& direct.requires((head,))
    &&& forall|parents:Vec<K>| model(level,head)>0 && #[trigger] direct.ensures((head,),parents)
        ==> boundary.requires((parents,model(level,head)))
    &&& forall|cut:Vec<K>| model(level,head)>0 && #[trigger] produced_boundary(head,level,direct,boundary,cut)
        ==> members.requires((&cut,))
    &&& forall|cut:Vec<K>,content:Vec<K>| model(level,head)>0 && #[trigger] produced_boundary(head,level,direct,boundary,cut)
        && #[trigger] members.ensures((&cut,),content) ==> every_level(content@,level)
}

pub(crate) open spec fn descriptor<K,L:Fn(K)->usize,D:FnOnce(K)->Vec<K>,B:FnOnce(Vec<K>,usize)->Vec<K>,M:FnOnce(&Vec<K>)->Vec<K>>(
    head:K,level:L,direct:D,boundary:B,members:M,out:Shape<K>,
)->bool {
    &&& out.head==head && out.level==model(level,head)
    &&& if out.level==0 {
        &&& direct.ensures((head,),out.boundary)
        &&& out.members@==seq![head] && out.checkpoints@.len()==0
    } else {
        &&& produced_boundary(head,level,direct,boundary,out.boundary)
        &&& members.ensures((&out.boundary,),out.members)
        &&& checkpoint_set(out.members@,head,level,out.members@.len() as int,out.checkpoints@)
    }
}

} // verus!
