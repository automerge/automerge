//! Execute and finish the one prepared incoming transaction.

#![allow(unexpected_cfgs)]

use super::{
    apply_payloads,
    candidate_origin::{self, Candidate},
    pending,
    prepare_entry::Prepared,
    prepare_plan::Plan,
    publication::{self, Publish},
};
use std::hash::Hash;

#[cfg(automerge_verus)]
use crate::transaction_spec::*;
#[cfg(automerge_verus)]
use vstd::prelude::*;

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) struct DeliveryRead<M, K> {
    pub members: M,
    pub key: K,
}

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) struct SigningEffects<P, V, D, I> {
    pub publish: P,
    pub viable: V,
    pub same_delivery: D,
    pub install_pending: I,
}

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) struct Effects<P, A, O, W> {
    pub prepare: P,
    pub apply: A,
    pub open_signing: O,
    pub signing: W,
}

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) enum Outcome<S, V, E> {
    Aborted(S, E),
    Committed(V),
}

/// Use the prepared cohort and plan exactly once. Opening the signing view
/// is deferred until apply succeeds, so no old slot survives actor shifts.
#[cfg_attr(automerge_verus, verus_spec(out=>
    requires can_run(initial,&read,&effects),
    ensures transaction(initial,&read,&effects,out),
))]
pub(crate) fn run<S, V, G, C, H: Copy + Eq + Hash + 'static, P: 'static, E>(
    initial: S,
    read: DeliveryRead<impl for<'a> Fn(&'a Candidate<G>) -> &'a [C], impl Fn(&C) -> H>,
    effects: Effects<
        impl FnOnce(S) -> (S, Result<Prepared<G, Plan<H, P>>, E>),
        impl FnOnce(S, Vec<&C>) -> (S, Result<(), E>),
        impl FnOnce(S) -> V,
        SigningEffects<
            impl for<'a> Fn(V, Publish<&'a P, &'a [H]>) -> V,
            impl Fn(&V, &Candidate<G>) -> bool,
            impl Fn(&Candidate<G>, &Candidate<G>) -> bool,
            impl FnOnce(V, Vec<G>) -> V,
        >,
    >,
) -> Outcome<S, V, E> {
    let (state, result) = (effects.prepare)(initial);
    let prepared = match result {
        Ok(prepared) => prepared,
        Err(error) => {
            #[cfg(automerge_verus)]
            proof! {reveal(transaction);}
            return Outcome::Aborted(state, error);
        }
    };
    #[cfg(automerge_verus)]
    proof! {assert(was_prepared(initial,effects.prepare,state,prepared));}
    let data = apply_payloads::materialize(
        &prepared.candidates,
        &prepared.plan.ordered,
        read.members,
        read.key,
    );
    #[cfg(automerge_verus)]
    proof_decl! {let ghost payload=data;}
    let applied = if data.is_empty() {
        #[cfg(automerge_verus)]
        proof! {assert(successful_apply(state,prepared,&read,effects.apply,state));}
        state
    } else {
        let (applied, result) = (effects.apply)(state, data);
        match result {
            Ok(()) => {
                #[cfg(automerge_verus)]
                proof! {record_apply(state,prepared,&read,effects.apply,payload,applied,result);}
                applied
            }
            Err(error) => {
                #[cfg(automerge_verus)]
                proof! {record_rejection(initial,state,prepared,&read,&effects,payload,applied,error);}
                return Outcome::Aborted(applied, error);
            }
        }
    };
    #[cfg(automerge_verus)]
    proof! {assert(reached_apply(initial,state,prepared,&read,&effects,applied));}
    let signing = (effects.open_signing)(applied);
    #[cfg(automerge_verus)]
    proof! {assert(opened_after_apply(initial,state,prepared,&read,&effects,applied,signing));}
    let done = finish(signing, prepared, effects.signing);
    #[cfg(automerge_verus)]
    proof! {record_commit(initial,state,prepared,&read,&effects,applied,signing,done);}
    Outcome::Committed(done)
}

#[cfg_attr(automerge_verus, verus_spec(out=>
    requires can_finish(initial,prepared,&effects),
    ensures completion(initial,prepared,&effects,out),
))]
fn finish<V, G, H: 'static, P: 'static>(
    initial: V,
    prepared: Prepared<G, Plan<H, P>>,
    effects: SigningEffects<
        impl for<'a> Fn(V, Publish<&'a P, &'a [H]>) -> V,
        impl Fn(&V, &Candidate<G>) -> bool,
        impl Fn(&Candidate<G>, &Candidate<G>) -> bool,
        impl FnOnce(V, Vec<G>) -> V,
    >,
) -> V {
    let Prepared { candidates, plan } = prepared;
    let members = plan.projection.members.as_slice();
    let published =
        publication::publish(initial, &plan.projection.proofs, members, effects.publish);
    #[cfg(automerge_verus)]
    proof! {assert(published_plan(initial,prepared,&effects,published));}
    let retained = pending::rebuild(
        candidates,
        &plan.projection.remaining,
        &published,
        effects.viable,
        effects.same_delivery,
    );
    #[cfg(automerge_verus)]
    proof_decl! {let ghost kept=retained@;}
    let groups = candidate_origin::into_groups(retained);
    #[cfg(automerge_verus)]
    proof! {assert(pruned_plan(initial,prepared,&effects,published,kept,groups));}
    let done = (effects.install_pending)(published, groups);
    #[cfg(automerge_verus)]
    proof! {record_completion(initial,prepared,&effects,published,kept,groups,done);}
    done
}

#[cfg(test)]
mod tests {
    use super::{run, DeliveryRead, Effects, Outcome, SigningEffects};
    use crate::automerge::signing::incoming::{
        candidate_origin,
        prepare_entry::Prepared,
        prepare_plan::{Plan, Projection},
        publication::Publish,
    };
    use std::collections::HashSet;

    #[derive(Debug, PartialEq, Eq)]
    struct Item(u8, &'static str);
    type Group = Vec<Item>;
    type Log = Vec<&'static str>;
    type Input = Prepared<Group, Plan<u8, &'static str>>;

    fn payloads(candidate: &candidate_origin::Candidate<Group>) -> &[Item] {
        &candidate.group
    }

    fn plan(ordered: Vec<u8>) -> Input {
        Prepared {
            candidates: candidate_origin::candidates(
                vec![vec![Item(90, "blocked")], vec![Item(10, "earlier copy")]],
                vec![vec![Item(20, "head"), Item(10, "last copy")]],
            ),
            plan: Plan {
                projection: Projection {
                    proofs: vec!["selected proof"],
                    members: vec![5, 20, 10],
                    remaining: HashSet::from([0]),
                },
                ordered,
            },
        }
    }

    fn publish(mut state: Log, event: Publish<&&'static str, &[u8]>) -> Log {
        state.push(match event {
            Publish::MergeProof(proof) => {
                assert_eq!(*proof, "selected proof");
                "merge"
            }
            Publish::IncludeMembers(members) => {
                assert_eq!(members, [5, 20, 10]);
                "include"
            }
            Publish::Reclaim => "reclaim",
            Publish::RemoveDrafts => "prune drafts",
        });
        state
    }

    fn drive(
        input: Result<Input, &'static str>,
        fail_apply: bool,
    ) -> Outcome<Log, Log, &'static str> {
        run(
            vec!["before"],
            DeliveryRead {
                members: payloads,
                key: |item: &Item| item.0,
            },
            Effects {
                prepare: |state: Log| (state, input),
                apply: |mut state: Log, changes: Vec<&Item>| {
                    if fail_apply {
                        return (state, Err("apply"));
                    }
                    assert_eq!(
                        changes.iter().map(|c| c.1).collect::<Vec<_>>(),
                        ["head", "last copy"]
                    );
                    state.push("apply");
                    (state, Ok(()))
                },
                open_signing: |mut state: Log| {
                    state.push("fresh view");
                    state
                },
                signing: SigningEffects {
                    publish,
                    viable: |state: &Log, candidate: &candidate_origin::Candidate<Group>| {
                        assert_eq!(state.last(), Some(&"prune drafts"));
                        assert_eq!(candidate.group, [Item(90, "blocked")]);
                        true
                    },
                    same_delivery:
                        |left: &candidate_origin::Candidate<Group>,
                         right: &candidate_origin::Candidate<Group>| {
                            left.group == right.group
                        },
                    install_pending: |mut state: Log, groups: Vec<Group>| {
                        assert_eq!(groups, [vec![Item(90, "blocked")]]);
                        state.push("pending");
                        state
                    },
                },
            },
        )
    }

    #[test]
    fn runs_exact_prepared_payloads_then_rebuilds_against_the_fresh_view() {
        let Outcome::Committed(log) = drive(Ok(plan(vec![20, 10])), false) else {
            panic!("transaction aborted");
        };
        assert_eq!(
            log,
            [
                "before",
                "apply",
                "fresh view",
                "merge",
                "include",
                "reclaim",
                "prune drafts",
                "pending"
            ]
        );
    }

    #[test]
    fn aborts_never_publish_and_an_empty_apply_still_completes() {
        assert!(
            matches!(drive(Err("prepare"), false), Outcome::Aborted(log, "prepare") if log == ["before"])
        );
        assert!(
            matches!(drive(Ok(plan(vec![20,10])), true), Outcome::Aborted(log, "apply") if log == ["before"])
        );
        let Outcome::Committed(log) = drive(Ok(plan(Vec::new())), true) else {
            panic!("empty schedule called apply");
        };
        assert_eq!(
            log,
            [
                "before",
                "fresh view",
                "merge",
                "include",
                "reclaim",
                "prune drafts",
                "pending"
            ]
        );
    }
}
