//! Pure, single-control mock interpretation. No mutable policy callbacks or CRDT reads.
use super::core::{ancestry, Revoke};
use crate::{ActorId, Change, ChangeHash};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Eligibility {
    Eligible,
    Excluded,
    Pending,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Authority {
    Pending,
    Authorized,
    Invalidated,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct PolicySnapshot {
    pub(super) authorized: BTreeSet<ChangeHash>,
    pub(super) invalidated: BTreeSet<ChangeHash>,
    pub(super) contexts: BTreeMap<ChangeHash, u64>,
    pub(super) grants: BTreeSet<u64>,
}

pub(super) struct HistoryFacts<'a> {
    pub(super) archive: &'a BTreeMap<ChangeHash, Change>,
    pub(super) authors: &'a BTreeMap<ActorId, Vec<u8>>,
    pub(super) controls: &'a BTreeMap<ChangeHash, Revoke>,
    pub(super) integrated: &'a BTreeSet<ChangeHash>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Reason {
    BaseAdmission,
    PendingAuthority(ChangeHash),
    ActiveControl(ChangeHash),
    InvalidatedControl(ChangeHash),
    MissingFrontier(ChangeHash),
    AwaitingControlValidation(ChangeHash),
    OutsideFrontier(ChangeHash),
    FreshGrant(u64),
    MissingGrant(u64),
}

pub(super) struct Interpretation {
    pub(super) eligibility: BTreeMap<ChangeHash, Eligibility>,
    pub(super) authority: BTreeMap<ChangeHash, Authority>,
    pub(super) reasons: BTreeMap<ChangeHash, Reason>,
}

pub(super) fn resolve_controls(
    history: HistoryFacts<'_>,
    policy: &PolicySnapshot,
) -> Interpretation {
    let authority: BTreeMap<_, _> = history
        .controls
        .keys()
        .map(|h| {
            (
                *h,
                if !policy.authorized.contains(h) {
                    Authority::Pending
                } else if policy.invalidated.contains(h) {
                    Authority::Invalidated
                } else {
                    Authority::Authorized
                },
            )
        })
        .collect();
    let mut reasons = BTreeMap::new();
    // Precompute each retained ancestry once, independent of the viewed heads.
    // Known retained hashes alone are insufficient: only an integrated control
    // has passed the session's dependency-ancestry validation. Credible queued
    // controls may make targets pending, but cannot establish final exclusion.
    let frontiers: BTreeMap<_, _> = history
        .controls
        .iter()
        .map(|(h, c)| {
            (
                *h,
                if c.retain.iter().any(|b| !history.integrated.contains(b)) {
                    Err(Reason::MissingFrontier(*h))
                } else if !history.integrated.contains(h) {
                    Err(Reason::AwaitingControlValidation(*h))
                } else {
                    Ok(ancestry(history.archive, &c.retain))
                },
            )
        })
        .collect();
    let eligibility = history
        .archive
        .iter()
        .map(|(h, c)| {
            let (mut status, mut reason) = if let Some(a) = authority.get(h) {
                match a {
                    Authority::Pending => (Eligibility::Pending, Reason::PendingAuthority(*h)),
                    Authority::Authorized => (Eligibility::Eligible, Reason::ActiveControl(*h)),
                    Authority::Invalidated => {
                        (Eligibility::Excluded, Reason::InvalidatedControl(*h))
                    }
                }
            } else if let Some(g) = policy.contexts.get(h) {
                if policy.grants.contains(g) {
                    (Eligibility::Eligible, Reason::FreshGrant(*g))
                } else {
                    (Eligibility::Pending, Reason::MissingGrant(*g))
                }
            } else {
                (Eligibility::Eligible, Reason::BaseAdmission)
            };
            for (r, control) in history.controls {
                if authority[r] != Authority::Authorized
                    || history.authors.get(c.actor_id()) != Some(&control.target)
                {
                    continue;
                }
                // A trusted fresh-context association has an independent admission decision.
                if policy.contexts.contains_key(h) {
                    continue;
                }
                match &frontiers[r] {
                    Err(unresolved) => {
                        status = Eligibility::Pending;
                        reason = unresolved.clone();
                    }
                    Ok(frontier) if !frontier.contains(h) => {
                        status = Eligibility::Excluded;
                        reason = Reason::OutsideFrontier(*r);
                    }
                    _ => {}
                }
            }
            reasons.insert(*h, reason);
            (*h, status)
        })
        .collect();
    Interpretation {
        eligibility,
        authority,
        reasons,
    }
}
