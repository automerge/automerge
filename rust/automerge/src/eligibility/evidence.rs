//! Pure evaluator for the toy authority/evidence model (prototype A).
//!
//! The evaluator folds a causally related set of evidence items into
//! per-change [`Decision`]s. Arrival order is never an input: the evidence
//! log is a set, and relationships between items (`after`) are explicit.

use std::collections::{BTreeMap, BTreeSet};

use crate::author::Author;
use crate::types::ChangeHash;

/// Identity of an evidence item (fixture-supplied).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId(pub u64);

/// An external authorization context a change was authored under (toy model
/// input; not a claim that anything proves it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AuthorizationContextId(pub u64);

/// How a bound context is to be evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ContextKind {
    /// An ordinary, already-established authoring context. It neither
    /// admits nor blocks; revocations apply normally.
    Established,
    /// A context linked to a fresh grant. Until a matching `Grant` is known
    /// the change is *pending*; once known, the change is admitted.
    FreshGrant,
}

/// Immutable association of a change with its authoring context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContextBinding {
    pub context: AuthorizationContextId,
    pub kind: ContextKind,
}

/// Toy evidence vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Evidence {
    /// A revocation statement excluding `target`'s changes outside the
    /// ancestry of `frontier`.
    Revocation {
        id: EventId,
        target: Author<'static>,
        frontier: Vec<ChangeHash>,
    },
    /// Evidence that establishes the authority of `event`.
    Authorizes { id: EventId, event: EventId },
    /// Evidence, causally after `after`, that invalidates the authority of
    /// `event`.
    Invalidates {
        id: EventId,
        event: EventId,
        after: Vec<EventId>,
    },
    /// A fresh grant that admits changes bound to `context` regardless of
    /// earlier revocations.
    Grant {
        id: EventId,
        context: AuthorizationContextId,
    },
}

impl Evidence {
    pub fn id(&self) -> EventId {
        match self {
            Evidence::Revocation { id, .. }
            | Evidence::Authorizes { id, .. }
            | Evidence::Invalidates { id, .. }
            | Evidence::Grant { id, .. } => *id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authority {
    /// Unresolved; lists evidence ids this item is still waiting for.
    Pending(Vec<EventId>),
    Authorized,
    Invalidated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Eligibility {
    Eligible,
    Excluded,
    Pending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reason {
    OutsideFrontier {
        event: EventId,
        frontier: Vec<ChangeHash>,
    },
    MissingBoundary {
        event: EventId,
        missing: Vec<ChangeHash>,
    },
    UnresolvedEvidence(EventId),
    AdmittedByContext(AuthorizationContextId),
    /// Bound to a fresh-grant context whose grant is not yet known.
    UnresolvedGrant(AuthorizationContextId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    pub eligibility: Eligibility,
    pub reasons: Vec<Reason>,
}

impl Decision {
    fn eligible() -> Self {
        Self {
            eligibility: Eligibility::Eligible,
            reasons: Vec::new(),
        }
    }
}

/// Set of evidence items (order-independent).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EvidenceLog {
    items: BTreeMap<EventId, Evidence>,
}

impl EvidenceLog {
    /// Insert; returns `false` if an identical item was already present.
    /// Conflicting reuse of an id is an error.
    pub fn insert(&mut self, item: Evidence) -> Result<bool, EvidenceError> {
        match self.items.get(&item.id()) {
            Some(existing) if existing == &item => Ok(false),
            Some(_) => Err(EvidenceError::ConflictingEvent(item.id())),
            None => {
                self.items.insert(item.id(), item);
                Ok(true)
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &Evidence> {
        self.items.values()
    }

    pub fn get(&self, id: EventId) -> Option<&Evidence> {
        self.items.get(&id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EvidenceError {
    #[error("evidence id {0:?} reused with different content")]
    ConflictingEvent(EventId),
}

/// The graph facts the evaluator needs about a change, supplied by the shell.
#[derive(Debug)]
pub struct ChangeFacts<'a> {
    pub hash: ChangeHash,
    pub author: Option<Author<'a>>,
    pub context: Option<ContextBinding>,
}

/// Structural questions the evaluator asks of the integrated graph.
pub trait GraphFacts {
    /// `None` if the frontier (or the change) is not integrated.
    fn in_ancestry(&self, frontier: &[ChangeHash], hash: &ChangeHash) -> Option<bool>;
    fn missing_from(&self, frontier: &[ChangeHash]) -> Vec<ChangeHash>;
}

/// Resolve the authority of every evidence item in the log.
pub fn authorities(log: &EvidenceLog) -> BTreeMap<EventId, Authority> {
    let mut out = BTreeMap::new();
    for item in log.iter() {
        let id = item.id();
        match item {
            Evidence::Revocation { .. } => {
                out.insert(id, revocation_authority(log, id));
            }
            Evidence::Grant { .. } => {
                // Grants are taken as authoritative mock evidence in this
                // experiment.
                out.insert(id, Authority::Authorized);
            }
            Evidence::Authorizes { .. } => {
                out.insert(id, Authority::Authorized);
            }
            Evidence::Invalidates { after, .. } => {
                let missing: Vec<EventId> = after
                    .iter()
                    .copied()
                    .filter(|a| log.get(*a).is_none())
                    .collect();
                if missing.is_empty() {
                    out.insert(id, Authority::Authorized);
                } else {
                    out.insert(id, Authority::Pending(missing));
                }
            }
        }
    }
    out
}

fn revocation_authority(log: &EvidenceLog, revocation: EventId) -> Authority {
    let mut authorized = false;
    let mut pending: BTreeSet<EventId> = BTreeSet::new();
    let mut invalidated = false;
    for item in log.iter() {
        match item {
            Evidence::Authorizes { event, .. } if *event == revocation => authorized = true,
            Evidence::Invalidates { event, after, .. } if *event == revocation => {
                // The invalidation is only effective once everything it is
                // causally after is known; otherwise it is unresolved and the
                // revocation stays pending on those items.
                let missing: Vec<EventId> = after
                    .iter()
                    .copied()
                    .filter(|a| log.get(*a).is_none())
                    .collect();
                if missing.is_empty() {
                    invalidated = true;
                } else {
                    pending.extend(missing);
                }
            }
            _ => {}
        }
    }
    if invalidated {
        Authority::Invalidated
    } else if !pending.is_empty() {
        Authority::Pending(pending.into_iter().collect())
    } else if authorized {
        Authority::Authorized
    } else {
        Authority::Pending(Vec::new())
    }
}

/// Evaluate one change against the whole evidence log.
pub fn evaluate(
    log: &EvidenceLog,
    authority: &BTreeMap<EventId, Authority>,
    facts: &dyn GraphFacts,
    change: &ChangeFacts<'_>,
) -> Decision {
    let mut decision = Decision::eligible();
    let mut excluded = false;
    let mut pending = false;
    if let Some(ContextBinding {
        context,
        kind: ContextKind::FreshGrant,
    }) = change.context
    {
        let granted = log
            .iter()
            .any(|e| matches!(e, Evidence::Grant { context: c, .. } if *c == context));
        if granted {
            decision.reasons.push(Reason::AdmittedByContext(context));
            return decision;
        }
        // Missing grant evidence is not allow-all: the change waits. Whether
        // an earlier revocation applies depends on this unresolved grant, so
        // the outcome is pending rather than excluded.
        decision.eligibility = Eligibility::Pending;
        decision.reasons.push(Reason::UnresolvedGrant(context));
        return decision;
    }
    for item in log.iter() {
        let Evidence::Revocation {
            id,
            target,
            frontier,
        } = item
        else {
            continue;
        };
        if change.author.as_ref() != Some(target) {
            continue;
        }
        match authority.get(id) {
            Some(Authority::Authorized) => {}
            // Pending or invalidated authority does not change the target's
            // eligibility (spec §3.5, EX-01).
            _ => continue,
        }
        match facts.in_ancestry(frontier, &change.hash) {
            Some(true) => {}
            Some(false) => {
                excluded = true;
                decision.reasons.push(Reason::OutsideFrontier {
                    event: *id,
                    frontier: frontier.clone(),
                });
            }
            None => {
                pending = true;
                decision.reasons.push(Reason::MissingBoundary {
                    event: *id,
                    missing: facts.missing_from(frontier),
                });
            }
        }
    }
    decision.eligibility = if excluded {
        Eligibility::Excluded
    } else if pending {
        Eligibility::Pending
    } else {
        Eligibility::Eligible
    };
    decision
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Facts;
    impl GraphFacts for Facts {
        fn in_ancestry(&self, _frontier: &[ChangeHash], _hash: &ChangeHash) -> Option<bool> {
            Some(false)
        }
        fn missing_from(&self, _frontier: &[ChangeHash]) -> Vec<ChangeHash> {
            vec![]
        }
    }

    fn alice() -> Author<'static> {
        Author::from(b"alice".to_vec())
    }

    fn r() -> Evidence {
        Evidence::Revocation {
            id: EventId(1),
            target: alice(),
            frontier: vec![],
        }
    }

    fn change() -> ChangeFacts<'static> {
        ChangeFacts {
            hash: ChangeHash([0; 32]),
            author: Some(alice()),
            context: None,
        }
    }

    #[test]
    fn late_e1_cannot_overwrite_e2() {
        // Same set, two insertion orders -> identical authority.
        let e1 = Evidence::Authorizes {
            id: EventId(2),
            event: EventId(1),
        };
        let e2 = Evidence::Invalidates {
            id: EventId(3),
            event: EventId(1),
            after: vec![EventId(2)],
        };
        let mut a = EvidenceLog::default();
        let mut b = EvidenceLog::default();
        for e in [r(), e1.clone(), e2.clone()] {
            a.insert(e).unwrap();
        }
        for e in [e2, r(), e1] {
            b.insert(e).unwrap();
        }
        assert_eq!(a, b);
        assert_eq!(authorities(&a)[&EventId(1)], Authority::Invalidated);
    }

    #[test]
    fn e2_without_e1_is_unresolved() {
        let mut log = EvidenceLog::default();
        log.insert(r()).unwrap();
        log.insert(Evidence::Invalidates {
            id: EventId(3),
            event: EventId(1),
            after: vec![EventId(2)],
        })
        .unwrap();
        let auth = authorities(&log);
        assert_eq!(auth[&EventId(1)], Authority::Pending(vec![EventId(2)]));
        assert_eq!(auth[&EventId(3)], Authority::Pending(vec![EventId(2)]));
        let d = evaluate(&log, &auth, &Facts, &change());
        assert_eq!(d.eligibility, Eligibility::Eligible);
    }

    #[test]
    fn pending_revocation_does_not_exclude() {
        let mut log = EvidenceLog::default();
        log.insert(r()).unwrap();
        let auth = authorities(&log);
        assert_eq!(auth[&EventId(1)], Authority::Pending(vec![]));
        let d = evaluate(&log, &auth, &Facts, &change());
        assert_eq!(d.eligibility, Eligibility::Eligible);
    }
}
