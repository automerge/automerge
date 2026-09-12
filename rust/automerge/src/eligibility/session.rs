//! Imperative shell for prototype A: staged delivery groups over a real
//! `Automerge` document plus an external evidence log. Publishes captured
//! views and transitions; a failed group publishes nothing.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::evidence::{
    authorities, evaluate, Authority, AuthorizationContextId, ChangeFacts, Decision, Eligibility,
    EventId, Evidence, EvidenceError, EvidenceLog, GraphFacts, Reason,
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
    /// authored under (EX-03). Immutable once recorded: identical rebinding is
    /// idempotent, a conflicting one rejects the whole group.
    Binding(ChangeHash, AuthorizationContextId),
}

/// Per-process unique namespace for sessions so that a [`ViewId`] from one
/// session cannot select a capture of another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionId(u64);

static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);

impl SessionId {
    fn fresh() -> Self {
        Self(NEXT_SESSION.fetch_add(1, Ordering::Relaxed))
    }
}

/// Checked identity of a published capture: session namespace plus index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ViewId {
    session: SessionId,
    index: usize,
}

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

/// Complete inspection transition between two captures. Every field of
/// [`InspectionSnapshot`] has a corresponding change map so that
/// `is_empty()` is true iff the two snapshots are identical.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatusDelta {
    pub eligibility_changes: BTreeMap<ChangeHash, (Eligibility, Eligibility)>,
    /// Decisions whose reasons changed while eligibility did not.
    pub reason_changes: BTreeMap<ChangeHash, (Vec<Reason>, Vec<Reason>)>,
    pub authority_changes: BTreeMap<EventId, (Option<Authority>, Authority)>,
    pub newly_integrated: BTreeSet<ChangeHash>,
    /// Waiting inventory changes: `None` means absent (not received or
    /// already integrated).
    pub waiting_changes:
        BTreeMap<ChangeHash, (Option<BTreeSet<ChangeHash>>, Option<BTreeSet<ChangeHash>>)>,
    pub binding_changes:
        BTreeMap<ChangeHash, (Option<AuthorizationContextId>, AuthorizationContextId)>,
}

impl StatusDelta {
    pub fn is_empty(&self) -> bool {
        self.eligibility_changes.is_empty()
            && self.reason_changes.is_empty()
            && self.authority_changes.is_empty()
            && self.newly_integrated.is_empty()
            && self.waiting_changes.is_empty()
            && self.binding_changes.is_empty()
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
    #[error("change {hash} already bound to {existing:?}; refusing rebinding to {attempted:?}")]
    ConflictingBinding {
        hash: ChangeHash,
        existing: AuthorizationContextId,
        attempted: AuthorizationContextId,
    },
    #[error("view {0:?} does not belong to this session or is out of range")]
    ForeignView(ViewId),
    #[error("change {0} is not integrated in the requested view")]
    NotIntegrated(ChangeHash),
    #[error("event {0:?} is unknown in the requested view")]
    UnknownEvent(EventId),
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
    id: SessionId,
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

pub(crate) fn snapshot(
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

fn capture(
    doc: &Automerge,
    evidence: &EvidenceLog,
    bindings: &BTreeMap<ChangeHash, AuthorizationContextId>,
) -> Capture {
    let (selection, inspection) = snapshot(doc, evidence, bindings);
    Capture {
        spec: ViewSpec {
            heads: doc.get_heads(),
            selection,
        },
        inspection: Arc::new(inspection),
    }
}

impl Session {
    pub fn new(doc: Automerge) -> Self {
        let evidence = EvidenceLog::default();
        let bindings = BTreeMap::new();
        let capture = capture(&doc, &evidence, &bindings);
        Self {
            id: SessionId::fresh(),
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
                Input::Binding(hash, ctx) => match stage.bindings.get(&hash) {
                    Some(existing) if *existing == ctx => {}
                    Some(existing) => {
                        return Err(SessionError::ConflictingBinding {
                            hash,
                            existing: *existing,
                            attempted: ctx,
                        })
                    }
                    None => {
                        stage.bindings.insert(hash, ctx);
                    }
                },
            }
        }
        stage.doc.apply_changes(changes)?;
        let after = capture(&stage.doc, &stage.evidence, &stage.bindings);
        let before = &self.published.capture;
        // Both endpoint scopes compile against the post-import graph.
        let patches = stage.doc.diff_view(&before.spec, &after.spec)?;
        let status = status_delta(&before.inspection, &after.inspection);
        let before_id = self.current();
        stage.capture = after.clone();
        self.published = stage;
        self.captures.push(after);
        Ok(Transition {
            before: before_id,
            after: self.current(),
            patches,
            status,
        })
    }

    pub fn current(&self) -> ViewId {
        ViewId {
            session: self.id,
            index: self.captures.len() - 1,
        }
    }

    pub fn capture(&self, id: ViewId) -> Result<&Capture, SessionError> {
        if id.session != self.id {
            return Err(SessionError::ForeignView(id));
        }
        self.captures
            .get(id.index)
            .ok_or(SessionError::ForeignView(id))
    }

    pub fn doc(&self) -> &Automerge {
        &self.published.doc
    }

    pub fn hydrate(&self, id: ViewId) -> Result<hydrate::Value, SessionError> {
        let cap = self.capture(id)?;
        Ok(self.published.doc.hydrate_view(&cap.spec)?)
    }

    pub fn get_all<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        id: ViewId,
        obj: O,
        prop: P,
    ) -> Result<Vec<(Value<'_>, ExId)>, SessionError> {
        let cap = self.capture(id)?;
        Ok(self.published.doc.get_all_view(&cap.spec, obj, prop)?)
    }

    pub fn decision(&self, id: ViewId, hash: &ChangeHash) -> Result<Decision, SessionError> {
        self.capture(id)?
            .inspection
            .decisions
            .get(hash)
            .cloned()
            .ok_or(SessionError::NotIntegrated(*hash))
    }

    pub fn authority(&self, id: ViewId, event: EventId) -> Result<Authority, SessionError> {
        self.capture(id)?
            .inspection
            .authorities
            .get(&event)
            .cloned()
            .ok_or(SessionError::UnknownEvent(event))
    }

    pub fn waiting(
        &self,
        id: ViewId,
        hash: &ChangeHash,
    ) -> Result<Option<BTreeSet<ChangeHash>>, SessionError> {
        Ok(self.capture(id)?.inspection.waiting.get(hash).cloned())
    }

    pub fn is_integrated(&self, id: ViewId, hash: &ChangeHash) -> Result<bool, SessionError> {
        Ok(self.capture(id)?.inspection.decisions.contains_key(hash))
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
            Some(prev) if prev.reasons != d.reasons => {
                delta
                    .reason_changes
                    .insert(*hash, (prev.reasons.clone(), d.reasons.clone()));
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
    let waiting_keys: BTreeSet<&ChangeHash> =
        before.waiting.keys().chain(after.waiting.keys()).collect();
    for hash in waiting_keys {
        let prev = before.waiting.get(hash);
        let next = after.waiting.get(hash);
        if prev != next {
            delta
                .waiting_changes
                .insert(*hash, (prev.cloned(), next.cloned()));
        }
    }
    for (hash, ctx) in &after.bindings {
        let prev = before.bindings.get(hash).copied();
        if prev != Some(*ctx) {
            delta.binding_changes.insert(*hash, (prev, *ctx));
        }
    }
    delta
}
