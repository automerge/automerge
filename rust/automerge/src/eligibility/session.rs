//! Imperative shell for prototype A: staged delivery groups over a real
//! `Automerge` document plus an external evidence log. Publishes captured
//! views and transitions; a failed group publishes nothing.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::evidence::{
    authorities, evaluate, Authority, AuthorizationContextId, ChangeFacts, Decision, Eligibility,
    EventId, Evidence, EvidenceError, EvidenceLog, GraphFacts,
};
use super::{Selection, ViewError, ViewSpec};
use crate::exid::ExId;
use crate::hydrate;
use crate::types::ChangeHash;
use crate::{Automerge, AutomergeError, Change, Patch, Prop, Value};

/// One harness delivery item.
#[derive(Debug, Clone)]
pub enum Input {
    Change(Change),
    Evidence(Evidence),
    /// Toy model input binding a change to the authorization context it was
    /// authored under (EX-03).
    Binding(ChangeHash, AuthorizationContextId),
}

/// Opaque index of a published capture within a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ViewId(usize);

/// Frozen inspection state at a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InspectionSnapshot {
    pub decisions: BTreeMap<ChangeHash, Decision>,
    pub authorities: BTreeMap<EventId, Authority>,
    /// Changes received but not yet structurally integrated, with the
    /// dependencies they are waiting for.
    pub waiting: BTreeMap<ChangeHash, BTreeSet<ChangeHash>>,
    pub bindings: BTreeMap<ChangeHash, AuthorizationContextId>,
}

/// A captured view: content heads, selection and inspection state.
#[derive(Debug, Clone)]
pub struct Capture {
    pub spec: ViewSpec,
    pub inspection: Arc<InspectionSnapshot>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatusDelta {
    pub eligibility_changes: BTreeMap<ChangeHash, (Eligibility, Eligibility)>,
    pub authority_changes: BTreeMap<EventId, (Option<Authority>, Authority)>,
    pub newly_integrated: BTreeSet<ChangeHash>,
}

impl StatusDelta {
    pub fn is_empty(&self) -> bool {
        self.eligibility_changes.is_empty()
            && self.authority_changes.is_empty()
            && self.newly_integrated.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct Transition {
    pub before: ViewId,
    pub after: ViewId,
    pub patches: Vec<Patch>,
    pub status: StatusDelta,
}

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error(transparent)]
    Automerge(#[from] AutomergeError),
    #[error(transparent)]
    Evidence(#[from] EvidenceError),
    #[error(transparent)]
    View(#[from] ViewError),
}

#[derive(Debug, Clone)]
struct Published {
    doc: Automerge,
    evidence: EvidenceLog,
    bindings: BTreeMap<ChangeHash, AuthorizationContextId>,
    capture: Capture,
}

#[derive(Debug)]
pub struct Session {
    published: Published,
    captures: Vec<Capture>,
}

struct DocFacts<'a>(&'a Automerge);

impl GraphFacts for DocFacts<'_> {
    fn in_ancestry(&self, frontier: &[ChangeHash], hash: &ChangeHash) -> Option<bool> {
        self.0.change_graph.in_ancestry(frontier, hash)
    }
    fn missing_from(&self, frontier: &[ChangeHash]) -> Vec<ChangeHash> {
        frontier
            .iter()
            .copied()
            .filter(|h| !self.0.change_graph.has_change(h))
            .collect()
    }
}

fn snapshot(
    doc: &Automerge,
    evidence: &EvidenceLog,
    bindings: &BTreeMap<ChangeHash, AuthorizationContextId>,
) -> (Selection, InspectionSnapshot) {
    let auth = authorities(evidence);
    let facts = DocFacts(doc);
    let mut decisions = BTreeMap::new();
    let mut selection = BTreeMap::new();
    for hash in doc.change_graph.iter_hashes() {
        let meta = doc
            .get_change_meta_by_hash(&hash)
            .expect("integrated change has metadata");
        let change = ChangeFacts {
            hash,
            author: meta.author.clone(),
            context: bindings.get(&hash).copied(),
        };
        let decision = evaluate(evidence, &auth, &facts, &change);
        selection.insert(hash, decision.eligibility);
        decisions.insert(hash, decision);
    }
    let mut waiting = BTreeMap::new();
    for queued in doc.queue.iter() {
        let missing: BTreeSet<ChangeHash> = queued
            .deps()
            .iter()
            .copied()
            .filter(|d| !doc.change_graph.has_change(d))
            .collect();
        waiting.insert(queued.hash(), missing);
    }
    (
        Selection::new(selection),
        InspectionSnapshot {
            decisions,
            authorities: auth,
            waiting,
            bindings: bindings.clone(),
        },
    )
}

impl Session {
    pub fn new(doc: Automerge) -> Self {
        let evidence = EvidenceLog::default();
        let bindings = BTreeMap::new();
        let (selection, inspection) = snapshot(&doc, &evidence, &bindings);
        let capture = Capture {
            spec: ViewSpec {
                heads: doc.get_heads(),
                selection,
            },
            inspection: Arc::new(inspection),
        };
        Self {
            published: Published {
                doc,
                evidence,
                bindings,
                capture: capture.clone(),
            },
            captures: vec![capture],
        }
    }

    /// Deliver one group. Either the whole group is integrated/evaluated and
    /// a transition is published, or nothing changes.
    pub fn deliver(&mut self, inputs: Vec<Input>) -> Result<Transition, SessionError> {
        let mut stage = self.published.clone();
        let mut changes = Vec::new();
        for input in inputs {
            match input {
                Input::Change(c) => changes.push(c),
                Input::Evidence(e) => {
                    stage.evidence.insert(e)?;
                }
                Input::Binding(hash, ctx) => {
                    stage.bindings.insert(hash, ctx);
                }
            }
        }
        stage.doc.apply_changes(changes)?;
        let (selection, inspection) = snapshot(&stage.doc, &stage.evidence, &stage.bindings);
        let after = Capture {
            spec: ViewSpec {
                heads: stage.doc.get_heads(),
                selection,
            },
            inspection: Arc::new(inspection),
        };
        let before = &self.published.capture;
        // Both endpoint scopes compile against the post-import graph.
        let patches = stage.doc.diff_view(&before.spec, &after.spec)?;
        let status = status_delta(&before.inspection, &after.inspection);
        let before_id = ViewId(self.captures.len() - 1);
        stage.capture = after.clone();
        self.published = stage;
        self.captures.push(after);
        Ok(Transition {
            before: before_id,
            after: ViewId(self.captures.len() - 1),
            patches,
            status,
        })
    }

    pub fn current(&self) -> ViewId {
        ViewId(self.captures.len() - 1)
    }

    pub fn capture(&self, id: ViewId) -> &Capture {
        &self.captures[id.0]
    }

    pub fn doc(&self) -> &Automerge {
        &self.published.doc
    }

    pub fn hydrate(&self, id: ViewId) -> hydrate::Value {
        self.published
            .doc
            .hydrate_view(&self.captures[id.0].spec)
            .expect("captured view compiles against the published graph")
    }

    pub fn get_all<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        id: ViewId,
        obj: O,
        prop: P,
    ) -> Vec<(Value<'_>, ExId)> {
        self.published
            .doc
            .get_all_view(&self.captures[id.0].spec, obj, prop)
            .expect("captured view compiles against the published graph")
    }

    pub fn decision(&self, id: ViewId, hash: &ChangeHash) -> Decision {
        self.captures[id.0]
            .inspection
            .decisions
            .get(hash)
            .cloned()
            .unwrap_or_else(|| panic!("change {hash} is not integrated in view {id:?}"))
    }

    pub fn authority(&self, id: ViewId, event: EventId) -> Authority {
        self.captures[id.0]
            .inspection
            .authorities
            .get(&event)
            .cloned()
            .unwrap_or_else(|| panic!("event {event:?} unknown in view {id:?}"))
    }

    pub fn waiting(&self, id: ViewId, hash: &ChangeHash) -> Option<BTreeSet<ChangeHash>> {
        self.captures[id.0].inspection.waiting.get(hash).cloned()
    }

    pub fn is_integrated(&self, id: ViewId, hash: &ChangeHash) -> bool {
        self.captures[id.0].inspection.decisions.contains_key(hash)
    }
}

fn status_delta(before: &InspectionSnapshot, after: &InspectionSnapshot) -> StatusDelta {
    let mut delta = StatusDelta::default();
    for (hash, d) in &after.decisions {
        match before.decisions.get(hash) {
            None => {
                delta.newly_integrated.insert(*hash);
            }
            Some(prev) if prev.eligibility != d.eligibility => {
                delta
                    .eligibility_changes
                    .insert(*hash, (prev.eligibility, d.eligibility));
            }
            Some(_) => {}
        }
    }
    for (event, a) in &after.authorities {
        let prev = before.authorities.get(event);
        if prev != Some(a) {
            delta
                .authority_changes
                .insert(*event, (prev.cloned(), a.clone()));
        }
    }
    delta
}
