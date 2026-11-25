use crate::actor_chains::{AuthorSource, Candidate, Dependency, Resolution};
use crate::chain_spec::*;
use vstd::prelude::*;

verus! {

pub(crate) proof fn extend<A>(rows: Seq<Candidate<A>>, earlier: Seq<Resolution>, next: Resolution)
    requires classified(rows, earlier), classification(rows, earlier, earlier.len() as int, next),
    ensures classified(rows, earlier.push(next)),
{
    let updated = earlier.push(next);
    assert forall|i: int| 0 <= i < updated.len()
        implies classification(rows, updated.take(i), i, #[trigger] updated[i]) by {
        if i < earlier.len() {
            assert(updated.take(i) =~= earlier.take(i));
        } else {
            assert(updated.take(i) =~= earlier);
        }
    }
}

pub(crate) proof fn via_broken_dep<A>(rows: Seq<Candidate<A>>, earlier: Seq<Resolution>, i: int, j: int, cause: usize)
    requires 0 <= j < rows[i].deps@.len(), no_broken(rows[i].deps@, earlier, j),
        dep_cause(rows[i].deps@[j], earlier) == Some(cause),
    ensures classification(rows, earlier, i, Resolution::Broken(cause)),
{
    let deps = rows[i].deps@;
    assert(!no_broken(deps, earlier, deps.len() as int));
}

pub(crate) proof fn next_dep<A>(
    rows: Seq<Candidate<A>>, earlier: Seq<Resolution>, i: int, j: int,
    old: Option<AuthorSource>, next: Option<AuthorSource>,
)
    requires 0 <= j < rows[i].deps@.len(), no_broken(rows[i].deps@, earlier, j),
        dep_cause(rows[i].deps@[j], earlier).is_none(), first_binding(rows, earlier, i, j, old),
        next == if old.is_some() { old } else { binding(rows, earlier, i, rows[i].deps@[j]) },
    ensures no_broken(rows[i].deps@, earlier, j + 1), first_binding(rows, earlier, i, j + 1, next),
{}

// If the cause index isn't the node itself, it was copied from a broken
// parent. Repeatedly following such a parent must reach the original fault.
pub(crate) proof fn cause_is_local_fault<A>(rows: Seq<Candidate<A>>, out: Seq<Resolution>, i: int, cause: usize)
    requires well_formed(rows), classified(rows, out), out.len() == rows.len(),
        0 <= i < out.len(), out[i] == Resolution::Broken(cause),
    ensures cause <= i, out[cause as int] == Resolution::Broken(cause),
        no_broken(rows[cause as int].deps@, out.take(cause as int), rows[cause as int].deps@.len() as int),
    decreases i,
{
    let deps = rows[i].deps@;
    assert(classification(rows, out.take(i), i, out[i]));
    if no_broken(deps, out.take(i), deps.len() as int) {
        assert(cause == i);
    } else {
        let j = choose|j: int| 0 <= j < deps.len() && dep_cause(#[trigger] deps[j], out.take(i)) == Some(cause);
        assert(deps[j] is Candidate);
        let p = match deps[j] { Dependency::Candidate(p) => p, _ => { assert(false); 0usize } };
        assert(p < i);
        assert(out[p as int] == Resolution::Broken(cause));
        cause_is_local_fault(rows, out, p as int, cause);
    }
}

// A live candidate source can be followed down exact same-actor predecessor
// edges. Any initial source is that actor's sequence-one node; an applied
// source is introduced by a matching dependency, never by a different actor.
pub(crate) proof fn initial_source_belongs_to_actor<A>(
    rows: Seq<Candidate<A>>, out: Seq<Resolution>, i: int, initial: usize,
)
    requires well_formed(rows), classified(rows, out), out.len() == rows.len(),
        0 <= i < out.len(), out[i] == Resolution::Linked(AuthorSource::Initial(initial)),
    ensures initial <= i, rows[initial as int].actor == rows[i].actor,
        rows[initial as int].seq == 1, !rows[initial as int].actor_already_applied,
    decreases i,
{
    let deps = rows[i].deps@;
    assert(classification(rows, out.take(i), i, out[i]));
    assert(no_broken(deps, out.take(i), deps.len() as int));
    if rows[i].seq == 1 {
        assert(initial == i);
    } else {
        let j = choose|j: int| 0 <= j < deps.len()
            && binding(rows, out.take(i), i, #[trigger] deps[j]) == Some(AuthorSource::Initial(initial));
        assert(deps[j] is Candidate);
        let p = match deps[j] { Dependency::Candidate(p) => p, _ => { assert(false); 0usize } };
        assert(p < i);
        assert(out[p as int] == Resolution::Linked(AuthorSource::Initial(initial)));
        assert(rows[p as int].actor == rows[i].actor);
        initial_source_belongs_to_actor(rows, out, p as int, initial);
    }
}

} // verus!
