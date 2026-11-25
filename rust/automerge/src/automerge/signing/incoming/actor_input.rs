//! Keep hash, rank and dependency identities aligned during actor resolution.

#![allow(unexpected_cfgs)]

use super::actor_chains::{self, Candidate, Dependency, Resolution};

#[cfg(automerge_verus)]
use crate::{
    actor_input_spec::*,
    chain_spec::{classified, well_formed},
    lookup_spec::{model, modeled},
};
#[cfg(automerge_verus)]
use vstd::prelude::*;

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) struct Observation<'a, K, A> {
    pub actor: A,
    pub seq: u64,
    pub actor_already_applied: bool,
    pub parents: &'a [K],
}

/// Feed the exact resolved-rank view into the checked propagation pass.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires vstd::laws_eq::obeys_concrete_eq::<A>(), source_inputs(hashes@,read,label),
    ensures out@.len()==hashes@.len(),
        exists|rows:Seq<Candidate<A>>| #[trigger] exact_inputs(hashes@,read,label,rows)
            && well_formed(rows) && classified(rows,out@),
))]
pub(super) fn resolve<'a, K: Copy + 'a, A: Copy + Eq>(
    hashes: &[K],
    read: impl Fn(K) -> Observation<'a, K, A>,
    label: impl Fn(K) -> Dependency<A>,
) -> Vec<Resolution> {
    let rows = build(hashes, read, label);
    let outcomes = actor_chains::propagate(&rows);
    #[cfg(automerge_verus)]
    proof! { assert(exact_inputs(hashes@,read,label,rows@)); }
    outcomes
}

#[cfg_attr(automerge_verus, verus_spec(out =>
    requires source_inputs(hashes@,read,label),
    ensures exact_inputs(hashes@,read,label,out@), well_formed(out@),
))]
fn build<'a, K: Copy + 'a, A>(
    hashes: &[K],
    read: impl Fn(K) -> Observation<'a, K, A>,
    label: impl Fn(K) -> Dependency<A>,
) -> Vec<Candidate<A>> {
    let mut rows = Vec::with_capacity(hashes.len());
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i<=hashes@.len(), rows@.len()==i, source_inputs(hashes@,read,label),
            prefix_inputs(hashes@,read,label,rows@), well_formed(rows@),
        decreases hashes@.len()-i,
    ))]
    while i < hashes.len() {
        let hash = hashes[i];
        #[cfg(automerge_verus)]
        proof! { assert(modeled(read,hash)); }
        let Observation {
            actor,
            seq,
            actor_already_applied,
            parents,
        } = read(hash);
        #[cfg(automerge_verus)]
        proof! { assert(model(read,hash)==Observation{actor,seq,actor_already_applied,parents}); }
        let mut deps = Vec::with_capacity(parents.len());
        let mut j = 0;
        #[cfg_attr(automerge_verus, verus_spec(
            invariant j<=parents@.len(), deps@.len()==j, i<hashes@.len(), source_inputs(hashes@,read,label),
                parents==model(read,hashes@[i as int]).parents,
                forall|k:int| 0<=k<j ==> #[trigger] deps@[k]==model(label,parents@[k]),
            decreases parents@.len()-j,
        ))]
        while j < parents.len() {
            let parent = parents[j];
            #[cfg(automerge_verus)]
            proof! { assert(modeled(label,parent)); }
            let edge = label(parent);
            #[cfg(automerge_verus)]
            proof! { assert(edge==model(label,parent)); }
            deps.push(edge);
            j += 1;
        }
        let row = Candidate {
            actor,
            seq,
            actor_already_applied,
            deps,
        };
        #[cfg(automerge_verus)]
        proof! { extend(hashes@,read,label,i as int,rows@,row); }
        rows.push(row);
        i += 1;
    }
    rows
}
