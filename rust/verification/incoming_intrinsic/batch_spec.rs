use crate::evaluation_batch::Outcome;
use vstd::prelude::*;

verus! {

broadcast use vstd::seq_lib::group_seq_properties;

pub(crate) open spec fn evaluated_prefix<G,S,V,E,F:Fn(S,&G)->(S,Result<Outcome<V>,E>)>(
    candidates:Seq<G>,initial:S,inspect:F,states:Seq<S>,outcomes:Seq<Outcome<V>>,end:int,
) -> bool {
    &&& 0<=end<=candidates.len()<=usize::MAX
    &&& outcomes.len()==end && states.len()==end+1 && states[0]==initial
    &&& forall|j:int| 0<=j<end ==> #[trigger] inspect.ensures((states[j],&candidates[j]),(states[j+1],Ok(outcomes[j])))
}

pub(crate) open spec fn can_inspect<G,S,V,E,F:Fn(S,&G)->(S,Result<Outcome<V>,E>)>(candidates:Seq<G>,inspect:F,states:Seq<S>,i:int) -> bool {
    inspect.requires((states[i],&candidates[i]))
}

pub(crate) open spec fn can_assess<G,S,V,E,F:Fn(S,&G)->(S,Result<Outcome<V>,E>)>(candidates:Seq<G>,initial:S,inspect:F) -> bool {
    &&& candidates.len()<=usize::MAX
    &&& forall|i:int, states:Seq<S>,outcomes:Seq<Outcome<V>>| i<candidates.len()
        && #[trigger] evaluated_prefix(candidates,initial,inspect,states,outcomes,i) ==> can_inspect(candidates,inspect,states,i)
}

// These are exact, ordered lists of candidate occurrences, not sets of heads.
// A dropped occurrence is neither waiting nor verified.
pub(crate) open spec fn partition<V>(outcomes:Seq<Outcome<V>>,waiting:Seq<usize>,verified:Seq<(usize,V)>) -> bool {
    &&& forall|j:int| 0<=j<waiting.len() ==> (#[trigger] waiting[j]) < outcomes.len() && outcomes[waiting[j] as int] == Outcome::Waiting
    &&& forall|j:int| 0<=j<verified.len() ==> (#[trigger] verified[j]).0 < outcomes.len()
        && outcomes[verified[j].0 as int] == Outcome::Verified(verified[j].1)
    &&& forall|i:int| 0<=i<outcomes.len() ==> match #[trigger] outcomes[i] {
        Outcome::Waiting => waiting.contains(i as usize),
        Outcome::Verified(v) => verified.contains((i as usize,v)),
        Outcome::Dropped => true,
    }
    &&& forall|j:int,k:int| 0<=j<k<waiting.len() ==> #[trigger] waiting[j] < #[trigger] waiting[k]
    &&& forall|j:int,k:int| 0<=j<k<verified.len() ==> (#[trigger] verified[j]).0 < (#[trigger] verified[k]).0
}

pub(crate) open spec fn failed<G,S,V,E,F:Fn(S,&G)->(S,Result<Outcome<V>,E>)>(
    candidates:Seq<G>,initial:S,inspect:F,states:Seq<S>,outcomes:Seq<Outcome<V>>,i:int,next:S,error:E,
) -> bool {
    &&& i<candidates.len()
    &&& evaluated_prefix(candidates,initial,inspect,states,outcomes,i)
    &&& inspect.ensures((states[i],&candidates[i]),(next,Err(error)))
}

pub(crate) open spec fn has_failure<G,S,V,E,F:Fn(S,&G)->(S,Result<Outcome<V>,E>)>(candidates:Seq<G>, initial:S, inspect:F, error:E) -> bool {
    exists|i:int,states:Seq<S>,outcomes:Seq<Outcome<V>>,next:S|
        #[trigger] failed(candidates,initial,inspect,states,outcomes,i,next,error)
}

#[verifier::opaque]
pub(crate) open spec fn assessed<G,S,V,E,F:Fn(S,&G)->(S,Result<Outcome<V>,E>)>(
    candidates:Seq<G>, initial:S, inspect:F, result:Result<(Vec<usize>,Vec<(usize,V)>), E>,
) -> bool {
    match result {
        Ok((waiting,verified)) => exists|states:Seq<S>,outcomes:Seq<Outcome<V>>|
            #[trigger] evaluated_prefix(candidates,initial,inspect,states,outcomes,candidates.len() as int)
            && partition(outcomes,waiting@,verified@),
        Err(error) => has_failure(candidates,initial,inspect,error),
    }
}

pub(crate) proof fn record_error<G,S,V,E,F:Fn(S,&G)->(S,Result<Outcome<V>,E>)>(
    candidates:Seq<G>,initial:S,inspect:F,states:Seq<S>,outcomes:Seq<Outcome<V>>,i:int,next:S,error:E,
)
    requires i<candidates.len(), evaluated_prefix(candidates,initial,inspect,states,outcomes,i),
        inspect.ensures((states[i],&candidates[i]),(next,Err(error))),
    ensures assessed(candidates,initial,inspect,Err(error)),
{
    assert(failed(candidates,initial,inspect,states,outcomes,i,next,error));
    assert(has_failure(candidates,initial,inspect,error));
    reveal(assessed);
}

pub(crate) proof fn record_success<G,S,V,E,F:Fn(S,&G)->(S,Result<Outcome<V>,E>)>(
    candidates:Seq<G>,initial:S,inspect:F,states:Seq<S>,outcomes:Seq<Outcome<V>>,i:int,next:S,outcome:Outcome<V>,
)
    requires i<candidates.len(),evaluated_prefix(candidates,initial,inspect,states,outcomes,i),
        inspect.ensures((states[i],&candidates[i]),(next,Ok(outcome))),
    ensures evaluated_prefix(candidates,initial,inspect,states.push(next),outcomes.push(outcome),i+1),
{
    let steps=states.push(next);
    let results=outcomes.push(outcome);
    assert forall|j:int| 0<=j<i+1 implies #[trigger] inspect.ensures((steps[j],&candidates[j]),(steps[j+1],Ok(results[j]))) by {
        if j<i { assert(inspect.ensures((states[j],&candidates[j]),(states[j+1],Ok(outcomes[j])))); }
    }
}

pub(crate) proof fn record_partition<V>(prior:Seq<Outcome<V>>,old_waiting:Seq<usize>,old_verified:Seq<(usize,V)>,
    outcome:Outcome<V>,waiting:Seq<usize>,verified:Seq<(usize,V)>,
)
    requires prior.len()<=usize::MAX, partition(prior,old_waiting,old_verified),
        waiting == if outcome is Waiting { old_waiting.push(prior.len() as usize) } else { old_waiting },
        verified == match outcome { Outcome::Verified(v) => old_verified.push((prior.len() as usize,v)), _ => old_verified },
    ensures partition(prior.push(outcome),waiting,verified),
{
    let extended=prior.push(outcome);
    assert forall|i:int| 0<=i<extended.len() implies match #[trigger] extended[i] {
        Outcome::Waiting => waiting.contains(i as usize),
        Outcome::Verified(v) => verified.contains((i as usize,v)),
        Outcome::Dropped => true,
    } by {
        if i<prior.len() { assert(extended[i]==prior[i]); }
    }
}

pub(crate) proof fn end_batch<G,S,V,E,F:Fn(S,&G)->(S,Result<Outcome<V>,E>)>(
    candidates:Seq<G>,initial:S,inspect:F,states:Seq<S>,outcomes:Seq<Outcome<V>>,waiting:Vec<usize>,verified:Vec<(usize,V)>,
)
    requires evaluated_prefix(candidates,initial,inspect,states,outcomes,candidates.len() as int), partition(outcomes,waiting@,verified@),
    ensures assessed(candidates,initial,inspect,Ok((waiting,verified))),
{
    reveal(assessed);
}

pub(crate) open spec fn validation_prefix<G,S,V,E,F:Fn(S,G)->(S,Result<V,E>)>(groups:Seq<G>,initial:S,validate:F,states:Seq<S>,out:Seq<V>)->bool {
    &&& out.len()<=groups.len() && states.len()==out.len()+1 && states[0]==initial
    &&& forall|i:int| 0<=i<out.len() ==> #[trigger] validate.ensures((states[i],groups[i]),(states[i+1],Ok(out[i])))
}

pub(crate) open spec fn validation_ready<G,S,V,E,F:Fn(S,G)->(S,Result<V,E>)>(groups:Seq<G>,initial:S,validate:F)->bool {
    forall|states:Seq<S>,out:Seq<V>| #[trigger] validation_prefix(groups,initial,validate,states,out) && out.len()<groups.len()
        ==> validate.requires((states.last(),groups[out.len() as int]))
}

pub(crate) open spec fn rejected<G,S,V,E,F:Fn(S,G)->(S,Result<V,E>)>(groups:Seq<G>,initial:S,validate:F,states:Seq<S>,done:Seq<V>,state:S,error:E)->bool {
    &&& validation_prefix(groups,initial,validate,states,done) && done.len()<groups.len()
    &&& validate.ensures((states.last(),groups[done.len() as int]),(state,Err(error)))
}

pub(crate) open spec fn validation_failed<G,S,V,E,F:Fn(S,G)->(S,Result<V,E>)>(groups:Seq<G>,initial:S,validate:F,error:E)->bool {
    exists|states:Seq<S>,done:Seq<V>,state:S| #[trigger] rejected(groups,initial,validate,states,done,state,error)
}

#[verifier::opaque]
pub(crate) open spec fn validation_result<G,S,V,E,F:Fn(S,G)->(S,Result<V,E>)>(groups:Seq<G>,initial:S,validate:F,result:Result<Vec<V>,E>)->bool {
    match result {
        Ok(out)=>out@.len()==groups.len() && exists|states:Seq<S>| #[trigger] validation_prefix(groups,initial,validate,states,out@),
        Err(error)=>validation_failed(groups,initial,validate,error),
    }
}

pub(crate) proof fn extend_validated<G,S,V,E,F:Fn(S,G)->(S,Result<V,E>)>(groups:Seq<G>,initial:S,validate:F,states:Seq<S>,done:Seq<V>,next:S,value:V)
    requires validation_prefix(groups,initial,validate,states,done), done.len()<groups.len(),
        validate.ensures((states.last(),groups[done.len() as int]),(next,Ok(value))),
    ensures validation_prefix(groups,initial,validate,states.push(next),done.push(value)),
{
    let path=states.push(next);
    let out=done.push(value);
    assert forall|j:int| 0<=j<out.len() implies #[trigger] validate.ensures((path[j],groups[j]),(path[j+1],Ok(out[j]))) by {
        if j<done.len() { assert(validate.ensures((states[j],groups[j]),(states[j+1],Ok(done[j])))); }
    }
}

pub(crate) proof fn validation_stopped<G,S,V,E,F:Fn(S,G)->(S,Result<V,E>)>(groups:Seq<G>,initial:S,validate:F,states:Seq<S>,done:Seq<V>,next:S,error:E)
    requires validation_prefix(groups,initial,validate,states,done), done.len()<groups.len(),
        validate.ensures((states.last(),groups[done.len() as int]),(next,Err(error))),
    ensures validation_result(groups,initial,validate,Err(error)),
{
    reveal(validation_result);
    assert(rejected(groups,initial,validate,states,done,next,error));
    assert(validation_failed(groups,initial,validate,error));
}

pub(crate) proof fn validation_finished<G,S,V,E,F:Fn(S,G)->(S,Result<V,E>)>(groups:Seq<G>,initial:S,validate:F,states:Seq<S>,done:Vec<V>)
    requires validation_prefix(groups,initial,validate,states,done@),done@.len()==groups.len(),
    ensures validation_result(groups,initial,validate,Ok(done)),
{ reveal(validation_result); }

} // verus!
