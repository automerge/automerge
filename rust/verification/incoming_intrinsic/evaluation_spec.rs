use crate::lookup_spec::model;
use vstd::prelude::*;

verus! {

pub(crate) open spec fn unbroken<T,E,F:Fn(&T)->Result<bool,E>>(members:Seq<T>, view:F, end:int) -> bool {
    forall|i:int| 0<=i<end ==> (#[trigger] model(view,&members[i])) is Ok
}

pub(crate) open spec fn all_ready<T,E,F:Fn(&T)->Result<bool,E>>(members:Seq<T>,view:F,end:int) -> bool {
    forall|i:int| 0<=i<end ==> #[trigger] model(view,&members[i]) == Ok(true)
}

pub(crate) open spec fn status_result<T,E,F:Fn(&T)->Result<bool,E>>(members:Seq<T>,view:F,result:Result<bool,E>) -> bool {
    match result {
        Ok(ready) => unbroken(members,view,members.len() as int) && ready == all_ready(members,view,members.len() as int),
        Err(e) => exists|i:int| 0<=i<members.len() && #[trigger] model(view,&members[i])==Err(e) && unbroken(members,view,i),
    }
}

pub(crate) proof fn report_broken<T,E,F:Fn(&T)->Result<bool,E>>(members:Seq<T>,view:F,i:int,error:E)
    requires 0<=i<members.len(), model(view,&members[i])==Err(error), unbroken(members,view,i),
    ensures status_result(members,view,Err(error)),
{}

pub(crate) open spec fn shape_ok<S,F,E,C:FnOnce(S)->(S,Result<F,E>)>(memo:S,canonical:C,state:S,fragment:F) -> bool {
    canonical.ensures((memo,), (state,Ok(fragment)))
}

pub(crate) open spec fn can_decide<S,F,P,E,Q:FnOnce()->Option<E>,C:FnOnce(S)->(S,Result<F,E>),B:FnOnce()->Result<P,E>>(
    status:Result<bool,E>, memo:S, equivocation:Q, canonical:C, proof:B,
) -> bool {
    status==Ok(true) ==> {
        &&& equivocation.requires(())
        &&& equivocation.ensures((),None) ==> canonical.requires((memo,))
        &&& forall|state:S, fragment:F| equivocation.ensures((),None) && #[trigger] shape_ok(memo,canonical,state,fragment)
            ==> proof.requires(())
    }
}

// Waiting is not evidence of invalidity. A ready group must pass each of the
// round's conflict, canonicality, and proof checks in order.
pub(crate) open spec fn decision<S,F,P,E,Q:FnOnce()->Option<E>,C:FnOnce(S)->(S,Result<F,E>),B:FnOnce()->Result<P,E>>(
    status:Result<bool,E>, memo:S, equivocation:Q, canonical:C, proof:B, result:(S,Result<Option<(F,P)>,E>),
) -> bool {
    match status {
        Err(e) => result==(memo,Err(e)),
        Ok(false) => result==(memo,Ok(None)),
        Ok(true) => {
            let (out,result) = result;
            match result {
                Ok(None) => false,
                Ok(Some((f,p))) => equivocation.ensures((),None) && shape_ok(memo,canonical,out,f) && proof.ensures((),Ok(p)),
                Err(e) => (out==memo && equivocation.ensures((),Some(e))) ||
                    (equivocation.ensures((),None) && (canonical.ensures((memo,),(out,Err(e)))
                        || (proof.ensures((),Err(e)) && exists|f:F| #[trigger] shape_ok(memo,canonical,out,f)))),
            }
        },
    }
}

} // verus!
