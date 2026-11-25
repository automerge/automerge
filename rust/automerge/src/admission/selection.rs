//! Exclude fragments that lack a live supplier for a required hash.

#![allow(unexpected_cfgs)]

#[cfg(automerge_verus)]
use crate::{selection_proof::*, selection_spec::*};
#[cfg(automerge_verus)]
use vstd::prelude::*;

#[cfg_attr(automerge_verus, verus_spec(ensures final(todo)@ == old(todo)@ + row@))]
fn enqueue(todo: &mut Vec<usize>, row: &[usize]) {
    #[cfg(automerge_verus)]
    proof_decl! { let ghost before = todo@; }
    todo.extend_from_slice(row);
    #[cfg(automerge_verus)]
    proof! {
        assert(vstd::pervasive::obeys_clone_eq::<usize>());
        assert(todo@ =~= before + row@);
    }
}

/// Return the exclusion mask over fragment occurrences. Already accepted
/// boundaries and self-supplied boundaries are omitted from `required_by`.
#[cfg_attr(automerge_verus, verus_spec(excluded =>
    requires well_formed(members@, required_by@, counts@),
    ensures biggest(members@, required_by@, excluded@),
))]
pub(super) fn exclude(
    members: &[Vec<usize>],
    required_by: &[Vec<usize>],
    mut counts: Vec<usize>,
) -> Vec<bool> {
    #[cfg(automerge_verus)]
    proof_decl! { let ghost initial = counts@; }
    let mut todo = Vec::new();
    let mut h = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant well_formed(members@, required_by@, initial), counts@ == initial,
            h <= counts@.len(), safe_queue(members@, required_by@, todo@),
            scheduled(required_by@, counts@, todo@, Seq::empty(), h as int),
        decreases counts@.len() - h,
    ))]
    while h < counts.len() {
        #[cfg(automerge_verus)]
        proof_decl! { let ghost before = todo@; }
        if counts[h] == 0 {
            enqueue(&mut todo, &required_by[h]);
        }
        #[cfg(automerge_verus)]
        proof! { seed(members@, required_by@, counts@, before, todo@, h); }
        h += 1;
    }

    let mut excluded = vec![false; members.len()];
    #[cfg(automerge_verus)]
    proof_decl! {
        let ghost mut done = Seq::<usize>::empty();
        init(members@, required_by@, counts@, excluded@);
    }
    #[cfg_attr(automerge_verus, verus_spec(
        invariant well_formed(members@, required_by@, initial), counts@.len() == initial.len(),
            state(members@, required_by@, counts@, todo@, excluded@, done, Seq::empty()),
        decreases members@.len() - done.len(), todo@.len(),
    ))]
    loop {
        #[cfg(automerge_verus)]
        proof_decl! { let ghost before = todo@; }
        let Some(fragment) = todo.pop() else {
            #[cfg(automerge_verus)]
            proof! { finish(members@, required_by@, counts@, todo@, excluded@, done); }
            return excluded;
        };
        #[cfg(automerge_verus)]
        proof! { dequeue(members@, required_by@, before, todo@, fragment); }
        if excluded[fragment] {
            #[cfg(automerge_verus)]
            proof! { already_done(required_by@, counts@, before, todo@, done, fragment); }
            continue;
        }

        let removed = &members[fragment];
        #[cfg(automerge_verus)]
        proof_decl! { let ghost old_mask = excluded@; }
        excluded[fragment] = true;
        #[cfg(automerge_verus)]
        proof! {
            start_exclusion(members@, required_by@, counts@, before, todo@, old_mask, excluded@, done, fragment);
            done = done.push(fragment);
            assert(removed@.skip(0) =~= removed@);
        }
        let mut j = 0;
        #[cfg_attr(automerge_verus, verus_spec(
            invariant well_formed(members@, required_by@, initial), counts@.len() == initial.len(),
                fragment < members@.len(), removed@ == members@[fragment as int]@,
                j <= removed@.len(), state(members@, required_by@, counts@, todo@, excluded@, done, removed@.skip(j as int)),
            decreases removed@.len() - j,
        ))]
        while j < removed.len() {
            let hash = removed[j];
            #[cfg(automerge_verus)]
            proof_decl! {
                let ghost previous = counts@;
                let ghost prior_queue = todo@;
                let ghost tail = removed@.skip(j as int);
                assert(hash < counts@.len());
                can_decrement(members@, counts@, done, tail, hash);
            }
            counts[hash] -= 1;
            if counts[hash] == 0 {
                enqueue(&mut todo, &required_by[hash]);
            }
            #[cfg(automerge_verus)]
            proof! {
                update(members@, required_by@, initial, previous, counts@, prior_queue, todo@, excluded@, done, tail, hash);
                assert(removed@.skip(j as int + 1) =~= tail.drop_first());
            }
            j += 1;
        }
        #[cfg(automerge_verus)]
        proof! { assert(removed@.skip(j as int) =~= Seq::<usize>::empty()); }
    }
}
