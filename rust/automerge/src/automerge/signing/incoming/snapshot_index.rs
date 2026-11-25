//! Index exact values from a sequence of delivery member lists.

#![allow(unexpected_cfgs)]

use std::collections::HashMap;
use std::hash::Hash;

#[cfg(automerge_verus)]
use crate::{
    lookup_spec::{model, modeled},
    shape_spec::hash_laws,
    snapshot_spec::*,
};
#[cfg(automerge_verus)]
use vstd::{prelude::*, std_specs::hash::hash_map_inv};

#[cfg_attr(automerge_verus, verus_verify)]
#[derive(Clone, Copy)]
pub(crate) enum Keep {
    First,
    Last,
}

/// Index exactly the projected rows, retaining either the first or last
/// occurrence of a key. The payload is taken from that same row.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires hash_laws::<K>(), views(groups@, members, project),
    ensures indexed(flatten(groups@,members,project,groups@.len() as int), keep, out@),
))]
pub(super) fn build<'g, 'm, G: 'g, T: 'm, K: Eq + Hash, V>(
    groups: &'g [G],
    members: impl Fn(&'g G) -> &'m [T],
    project: impl Fn(&'m T) -> Option<(K, V)>,
    keep: Keep,
) -> HashMap<K, V> {
    let mut out = HashMap::new();
    let mut i = 0;
    #[cfg(automerge_verus)]
    proof! { reveal(flatten); }
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i<=groups@.len(), hash_laws::<K>(), hash_map_inv(&out), views(groups@,members,project),
            indexed(flatten(groups@,members,project,i as int),keep,out@),
        decreases groups@.len()-i,
    ))]
    while i < groups.len() {
        let group = &groups[i];
        #[cfg(automerge_verus)]
        proof! { assert(modeled(members,group)); }
        let changes = members(group);
        #[cfg(automerge_verus)]
        proof_decl! {
            assert(changes==model(members,group));
            let ghost prior=flatten(groups@,members,project,i as int);
            assert(row(changes@,project).take(0) =~= Seq::<Option<(K,V)>>::empty());
            assert(prior+row(changes@,project).take(0) =~= prior);
        }
        let mut j = 0;
        #[cfg_attr(automerge_verus, verus_spec(
            invariant i<groups@.len(), j<=changes@.len(), hash_laws::<K>(), hash_map_inv(&out),
                changes==model(members,&groups@[i as int]), views(groups@,members,project),
                prior==flatten(groups@,members,project,i as int),
                indexed(prior+row(changes@,project).take(j as int),keep,out@),
            decreases changes@.len()-j,
        ))]
        while j < changes.len() {
            let change = &changes[j];
            #[cfg(automerge_verus)]
            proof_decl! { let ghost before=out@; assert(modeled(project,change)); }
            let binding = project(change);
            #[cfg(automerge_verus)]
            proof! { assert(binding==model(project,change)); }
            if let Some((key, value)) = binding {
                if matches!(keep, Keep::Last) || !out.contains_key(&key) {
                    out.insert(key, value);
                }
            }
            #[cfg(automerge_verus)]
            proof! { record_binding(prior,changes@,project,j as int,keep,before,out@); }
            j += 1;
        }
        #[cfg(automerge_verus)]
        proof! {
            assert(row(changes@,project).take(j as int) =~= row(changes@,project));
            complete_group(groups@,members,project,i as int);
        }
        i += 1;
    }
    out
}
