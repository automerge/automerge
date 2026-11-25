use crate::actor_chains::{AuthorSource, Candidate, Dependency, Resolution};
use vstd::prelude::*;

verus! {

// A dependency that is not in the applied graph refers to a strictly
// earlier resolved candidate. Unresolved candidates are not input rows.
pub(crate) open spec fn well_formed<A>(rows: Seq<Candidate<A>>) -> bool {
    &&& rows.len() <= usize::MAX
    &&& forall|i: int| 0 <= i < rows.len() ==> #[trigger] rows[i].seq > 0
    &&& forall|i: int, j: int| 0 <= i < rows.len() && 0 <= j < rows[i].deps@.len()
        ==> match #[trigger] rows[i].deps@[j] {
            Dependency::Candidate(p) => p < i,
            Dependency::Applied { seq, .. } => seq > 0,
        }
}

pub(crate) open spec fn dep_cause<A>(dep: Dependency<A>, earlier: Seq<Resolution>) -> Option<usize> {
    match dep {
        Dependency::Candidate(p) => match earlier[p as int] {
            Resolution::Broken(cause) => Some(cause),
            Resolution::Linked(_) => None,
        },
        Dependency::Applied { .. } => None,
    }
}

pub(crate) open spec fn no_broken<A>(deps: Seq<Dependency<A>>, earlier: Seq<Resolution>, end: int) -> bool {
    forall|j: int| 0 <= j < end ==> dep_cause(#[trigger] deps[j], earlier).is_none()
}

pub(crate) open spec fn binding<A>(
    rows: Seq<Candidate<A>>, earlier: Seq<Resolution>, i: int, dep: Dependency<A>,
) -> Option<AuthorSource> {
    let (actor, seq, source) = match dep {
        Dependency::Candidate(p) => (rows[p as int].actor, rows[p as int].seq, match earlier[p as int] {
            Resolution::Linked(s) => Some(s),
            Resolution::Broken(_) => None,
        }),
        Dependency::Applied { actor, seq, slot } => (actor, seq, Some(AuthorSource::Applied(slot))),
    };
    if actor == rows[i].actor && seq + 1 == rows[i].seq { source } else { None }
}

pub(crate) open spec fn no_binding<A>(rows: Seq<Candidate<A>>, earlier: Seq<Resolution>, i: int, end: int) -> bool {
    let deps = rows[i].deps@;
    forall|j: int| 0 <= j < end ==> binding(rows, earlier, i, #[trigger] deps[j]).is_none()
}

pub(crate) open spec fn first_binding<A>(
    rows: Seq<Candidate<A>>, earlier: Seq<Resolution>, i: int, end: int, source: Option<AuthorSource>,
) -> bool {
    let deps = rows[i].deps@;
    match source {
        None => no_binding(rows, earlier, i, end),
        Some(s) => exists|j: int| 0 <= j < end && binding(rows, earlier, i, #[trigger] deps[j]) == Some(s)
            && no_binding(rows, earlier, i, j),
    }
}

// A broken direct dependency takes precedence, independently of its actor.
// Otherwise seq=1 establishes a new source, and seq>1 inherits from a
// direct (same actor, seq-1) dependency or is itself the cause of failure.
pub(crate) open spec fn classification<A>(rows: Seq<Candidate<A>>, earlier: Seq<Resolution>, i: int, out: Resolution) -> bool {
    let deps = rows[i].deps@;
    if no_broken(deps, earlier, deps.len() as int) {
        if rows[i].seq == 1 {
            out == if rows[i].actor_already_applied { Resolution::Broken(i as usize) }
                else { Resolution::Linked(AuthorSource::Initial(i as usize)) }
        } else {
            match out {
                Resolution::Linked(s) => first_binding(rows, earlier, i, deps.len() as int, Some(s)),
                Resolution::Broken(cause) => cause == i && no_binding(rows, earlier, i, deps.len() as int),
            }
        }
    } else {
        match out {
            Resolution::Broken(cause) => exists|j: int| 0 <= j < deps.len()
                && dep_cause(#[trigger] deps[j], earlier) == Some(cause) && no_broken(deps, earlier, j),
            Resolution::Linked(_) => false,
        }
    }
}

pub(crate) open spec fn classified<A>(rows: Seq<Candidate<A>>, outcomes: Seq<Resolution>) -> bool {
    forall|i: int| 0 <= i < outcomes.len() ==> classification(rows, outcomes.take(i), i, #[trigger] outcomes[i])
}

} // verus!
