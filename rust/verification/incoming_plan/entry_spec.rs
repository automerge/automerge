use crate::candidate_origin::Candidate;
use crate::origin_spec::tagged;
use crate::pending::Origin;
use crate::prepare_entry::Prepared;
use vstd::prelude::*;

verus! {

pub(crate) open spec fn collected<L,G,E,O:FnOnce()->Vec<G>,B:FnOnce(Vec<L>)->Result<Vec<G>,E>>(
    raw:Vec<L>,pending:O,validate:B,old:Vec<G>,fresh:Vec<G>,
)->bool {
    &&& validate.ensures((raw,),Ok(fresh))
    &&& pending.ensures((),old)
}

pub(crate) open spec fn exact_candidates<L,G,E,O:FnOnce()->Vec<G>,B:FnOnce(Vec<L>)->Result<Vec<G>,E>>(
    raw:Vec<L>,pending:O,validate:B,old:Vec<G>,fresh:Vec<G>,candidates:Vec<Candidate<G>>,
)->bool {
    &&& collected(raw,pending,validate,old,fresh)
    &&& candidates@ == tagged(old@,Origin::ExistingPending)+tagged(fresh@,Origin::IncomingBatch)
}

pub(crate) open spec fn can_prepare<L,G,P,E,O:FnOnce()->Vec<G>,B:FnOnce(Vec<L>)->Result<Vec<G>,E>,C:FnOnce(&Vec<Candidate<G>>)->Result<P,E>>(
    raw:Vec<L>,pending:O,validate:B,plan:C,
)->bool {
    &&& validate.requires((raw,))
    &&& forall|fresh:Vec<G>| #[trigger] validate.ensures((raw,),Ok(fresh)) ==> pending.requires(())
    &&& forall|old:Vec<G>,fresh:Vec<G>| #[trigger] collected(raw,pending,validate,old,fresh) ==> old@.len()+fresh@.len()<=usize::MAX
    &&& forall|old:Vec<G>,fresh:Vec<G>,candidates:Vec<Candidate<G>>|
        #[trigger] exact_candidates(raw,pending,validate,old,fresh,candidates) ==> plan.requires((&candidates,))
}

pub(crate) open spec fn planned<L,G,P,E,O:FnOnce()->Vec<G>,B:FnOnce(Vec<L>)->Result<Vec<G>,E>,C:FnOnce(&Vec<Candidate<G>>)->Result<P,E>>(
    raw:Vec<L>,pending:O,validate:B,plan:C,candidates:Vec<Candidate<G>>,result:Result<P,E>,
)->bool {
    &&& exists|old:Vec<G>,fresh:Vec<G>| #[trigger] exact_candidates(raw,pending,validate,old,fresh,candidates)
    &&& plan.ensures((&candidates,),result)
}

#[verifier::opaque]
pub(crate) open spec fn preparation<L,G,P,E,O:FnOnce()->Vec<G>,B:FnOnce(Vec<L>)->Result<Vec<G>,E>,C:FnOnce(&Vec<Candidate<G>>)->Result<P,E>>(
    raw:Vec<L>,pending:O,validate:B,plan:C,out:Result<Prepared<G,P>,E>,
)->bool {
    match out {
        Ok(ready)=>planned(raw,pending,validate,plan,ready.candidates,Ok(ready.plan)),
        Err(error)=>validate.ensures((raw,),Err(error)) || failed(raw,pending,validate,plan,error),
    }
}

pub(crate) open spec fn failed<L,G,P,E,O:FnOnce()->Vec<G>,B:FnOnce(Vec<L>)->Result<Vec<G>,E>,C:FnOnce(&Vec<Candidate<G>>)->Result<P,E>>(
    raw:Vec<L>,pending:O,validate:B,plan:C,error:E,
)->bool {
    exists|candidates:Vec<Candidate<G>>| #[trigger] planned(raw,pending,validate,plan,candidates,Err(error))
}

pub(crate) proof fn ready_preparation<L,G,P,E,O:FnOnce()->Vec<G>,B:FnOnce(Vec<L>)->Result<Vec<G>,E>,C:FnOnce(&Vec<Candidate<G>>)->Result<P,E>>(
    raw:Vec<L>,pending:O,validate:B,plan:C,old:Vec<G>,fresh:Vec<G>,candidates:Vec<Candidate<G>>,value:P,
)
    requires exact_candidates(raw,pending,validate,old,fresh,candidates),plan.ensures((&candidates,),Ok(value)),
    ensures preparation(raw,pending,validate,plan,Ok(Prepared{candidates,plan:value})),
{ reveal(preparation); }

pub(crate) proof fn failed_plan<L,G,P,E,O:FnOnce()->Vec<G>,B:FnOnce(Vec<L>)->Result<Vec<G>,E>,C:FnOnce(&Vec<Candidate<G>>)->Result<P,E>>(
    raw:Vec<L>,pending:O,validate:B,plan:C,old:Vec<G>,fresh:Vec<G>,candidates:Vec<Candidate<G>>,error:E,
)
    requires exact_candidates(raw,pending,validate,old,fresh,candidates),plan.ensures((&candidates,),Err(error)),
    ensures preparation(raw,pending,validate,plan,Err(error)),
{
    reveal(preparation);
    assert(planned(raw,pending,validate,plan,candidates,Err(error)));
    assert(failed(raw,pending,validate,plan,error));
}

pub(crate) open spec fn assessed<'a,G:'a,A,V,E,R:FnOnce(&'a[G])->A,B:FnOnce(&'a[G],&A)->Result<V,E>>(
    candidates:&'a[G],resolve:R,evaluate:B,ancestry:A,value:V,
)->bool {
    resolve.ensures((candidates,),ancestry) && evaluate.ensures((candidates,&ancestry),Ok(value))
}

pub(crate) open spec fn can_analyze<'a,G:'a,A,V,P,E,R:FnOnce(&'a[G])->A,B:FnOnce(&'a[G],&A)->Result<V,E>,C:FnOnce(&'a[G],&A,V)->Result<P,E>>(
    candidates:&'a[G],resolve:R,evaluate:B,plan:C,
)->bool {
    &&& resolve.requires((candidates,))
    &&& forall|ancestry:A| #[trigger] resolve.ensures((candidates,),ancestry) ==> evaluate.requires((candidates,&ancestry))
    &&& forall|ancestry:A,value:V| #[trigger] assessed(candidates,resolve,evaluate,ancestry,value) ==>
        plan.requires((candidates,&ancestry,value))
}

pub(crate) open spec fn analysis<'a,G:'a,A,V,P,E,R:FnOnce(&'a[G])->A,B:FnOnce(&'a[G],&A)->Result<V,E>,C:FnOnce(&'a[G],&A,V)->Result<P,E>>(
    candidates:&'a[G],resolve:R,evaluate:B,plan:C,out:Result<P,E>,
)->bool {
    let compiled = exists|a:A,value:V| #[trigger] assessed(candidates,resolve,evaluate,a,value) && plan.ensures((candidates,&a,value),out);
    match out {
        Ok(_) => compiled,
        Err(error) => compiled || exists|a:A| #[trigger] resolve.ensures((candidates,),a) && evaluate.ensures((candidates,&a),Err(error)),
    }
}

} // verus!
