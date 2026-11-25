//! Build the reverse-edge input to the resolved-ancestry worklist.

#![allow(unexpected_cfgs)]

#[cfg(automerge_verus)]
use crate::{
    lookup_spec::{model, modeled},
    topology_input_spec::*,
};
#[cfg(automerge_verus)]
use vstd::prelude::*;

#[cfg_attr(automerge_verus, verus_verify)]
#[derive(Clone, Copy)]
pub(crate) enum Parent {
    Applied,
    Candidate(usize),
    Missing,
}

/// Count all unapplied parents, but create reverse edges only for candidate
/// parents. Every dependency occurrence contributes separately, even repeats.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires projected(candidates@, parents, classify),
    ensures counts(trace(candidates@,parents,classify,candidates@.len() as int), out.0@, out.1@, candidates@.len()),
))]
pub(super) fn build<'a, K: Copy + 'a>(
    candidates: &[K],
    parents: impl Fn(K) -> &'a [K],
    classify: impl Fn(K) -> Parent,
) -> (Vec<usize>, Vec<Vec<usize>>) {
    let mut waiting = vec![0; candidates.len()];
    let mut reverse = Vec::<Vec<usize>>::with_capacity(candidates.len());
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i<=candidates@.len(), reverse@.len()==i,
            forall|j:int| 0<=j<i ==> (#[trigger] reverse@[j])@.len()==0,
        decreases candidates@.len()-i,
    ))]
    while i < candidates.len() {
        reverse.push(Vec::new());
        i += 1;
    }
    let mut i = 0;
    #[cfg(automerge_verus)]
    proof! { reveal(trace); }
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i<=candidates@.len(),projected(candidates@,parents,classify),
            counts(trace(candidates@,parents,classify,i as int),waiting@,reverse@,candidates@.len()),
            untouched(waiting@,i as int),
        decreases candidates@.len()-i,
    ))]
    while i < candidates.len() {
        let candidate = candidates[i];
        #[cfg(automerge_verus)]
        proof! { assert(modeled(parents,candidate)); }
        let deps = parents(candidate);
        #[cfg(automerge_verus)]
        proof_decl! {
            assert(deps==model(parents,candidate));
            let ghost prefix=trace(candidates@,parents,classify,i as int);
            assert(row(i as int,deps@,classify).take(0) =~= Seq::<(usize,Parent)>::empty());
            assert(prefix+row(i as int,deps@,classify).take(0) =~= prefix);
        }
        let mut j = 0;
        #[cfg_attr(automerge_verus, verus_spec(
            invariant i<candidates@.len(), deps==model(parents,candidates@[i as int]), projected(candidates@,parents,classify),
                prefix==trace(candidates@,parents,classify,i as int),
                j<=deps@.len(), waiting@[i as int]<=j, untouched(waiting@,i as int+1),
                counts(prefix+row(i as int,deps@,classify).take(j as int),waiting@,reverse@,candidates@.len()),
            decreases deps@.len()-j,
        ))]
        while j < deps.len() {
            let dep = deps[j];
            #[cfg(automerge_verus)]
            proof_decl! { let ghost old_waiting=waiting@; let ghost old_reverse=reverse@; assert(modeled(classify,dep)); }
            let parent = classify(dep);
            #[cfg(automerge_verus)]
            proof! { assert(parent==model(classify,dep)); }
            match parent {
                Parent::Applied => {}
                Parent::Missing => waiting[i] += 1,
                Parent::Candidate(p) => {
                    waiting[i] += 1;
                    reverse[p].push(i);
                }
            }
            #[cfg(automerge_verus)]
            proof! { record_edge(prefix,i as int,deps@,classify,j as int,old_waiting,old_reverse,waiting@,reverse@); }
            j += 1;
        }
        #[cfg(automerge_verus)]
        proof! { assert(row(i as int,deps@,classify).take(j as int) =~= row(i as int,deps@,classify)); advance(candidates@,parents,classify,i as int); }
        i += 1;
    }
    (waiting, reverse)
}
