use super::policy::{resolve_controls, HistoryFacts, Interpretation, PolicySnapshot, Reason};
pub(super) use super::policy::{Authority, Eligibility};
use crate::clock::{Clock, ClockRange};
use crate::iter::DiffIter;
use crate::legacy;
use crate::patches::PatchLog;
use crate::types::{ObjMeta, OpId};
use crate::{
    ActorId, Automerge, Change, ChangeHash, ObjId, Patch, ReadDoc, ScalarValue, Value, ROOT,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Revoke {
    pub(super) target: Vec<u8>,
    pub(super) retain: Vec<ChangeHash>,
}

pub(crate) fn decode_control(bytes: &[u8]) -> Result<Revoke, String> {
    // v1: version byte, u16 target length, target, u16 frontier length, 32-byte hashes.
    if bytes.len() < 5 || bytes[0] != 1 {
        return Err("control version/length".into());
    }
    let n = u16::from_le_bytes([bytes[1], bytes[2]]) as usize;
    if n == 0 || bytes.len() < 5 + n {
        return Err("control target/length".into());
    }
    let count = u16::from_le_bytes([bytes[3 + n], bytes[4 + n]]) as usize;
    if bytes.len() != 5 + n + 32 * count {
        return Err("control frontier length".into());
    }
    let retain: Vec<_> = bytes[5 + n..]
        .chunks_exact(32)
        .map(|b| ChangeHash(b.try_into().unwrap()))
        .collect();
    if !retain.windows(2).all(|w| w[0] < w[1]) {
        return Err("noncanonical frontier".into());
    }
    Ok(Revoke {
        target: bytes[3..3 + n].to_vec(),
        retain,
    })
}

fn encode_control(target: &[u8], mut retain: Vec<ChangeHash>) -> Result<Vec<u8>, String> {
    retain.sort();
    retain.dedup();
    let n = u16::try_from(target.len()).map_err(|_| "target too long")?;
    let count = u16::try_from(retain.len()).map_err(|_| "frontier too long")?;
    let mut out = vec![1];
    out.extend(n.to_le_bytes());
    out.extend(target);
    out.extend(count.to_le_bytes());
    for hash in retain {
        out.extend(hash.0);
    }
    decode_control(&out)?;
    Ok(out)
}

pub(super) fn record_control(
    doc: &mut Automerge,
    actor: ActorId,
    target: &[u8],
    retain: Vec<ChangeHash>,
) -> Result<ChangeHash, String> {
    // Fresh actor avoids accidental continuation of unobserved actor history.
    if doc.get_changes(&[]).iter().any(|c| c.actor_id() == &actor) {
        return Err("control constructor requires fresh actor".into());
    }
    let start = doc
        .get_changes(&[])
        .iter()
        .map(Change::max_op)
        .max()
        .unwrap_or(0)
        + 1;
    let archive: BTreeMap<_, _> = doc
        .get_changes(&[])
        .into_iter()
        .map(|c| (c.hash(), c))
        .collect();
    let ancestors = ancestry(&archive, &doc.get_heads());
    if retain.iter().any(|h| !ancestors.contains(h)) {
        return Err("unknown frontier".into());
    }
    let change: Change = legacy::Change {
        operations: vec![legacy::Op {
            action: legacy::OpType::Revoke(encode_control(target, retain)?),
            obj: legacy::ObjectId::Root,
            key: legacy::Key::Map("".into()),
            pred: Vec::new().into(),
            insert: false,
        }],
        actor_id: actor.clone(),
        hash: None,
        seq: 1,
        start_op: start.try_into().unwrap(),
        time: 0,
        message: None,
        deps: doc.get_heads(),
        extra_bytes: vec![],
        author: None,
    }
    .into();
    let hash = change.hash();
    doc.apply_changes([change]).map_err(|e| e.to_string())?;
    doc.set_actor(actor);
    Ok(hash)
}

#[derive(Clone, Debug)]
pub(super) enum Input {
    Content(Change, Vec<u8>),
    Authorize(ChangeHash),
    // Invalidation has a fixed causal dependency on Authorize(same control).
    Invalidate(ChangeHash),
    Context(ChangeHash, u64),
    Grant(u64),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Capture {
    identity: ActorId,
    encoding: crate::TextEncoding,
    pub(super) heads: Vec<ChangeHash>,
    pub(super) eligibility: BTreeMap<ChangeHash, Eligibility>,
    pub(super) authority: BTreeMap<ChangeHash, Authority>,
    pub(super) reasons: BTreeMap<ChangeHash, Reason>,
    pub(super) integrated: BTreeSet<ChangeHash>,
    pub(super) missing_evidence: BTreeSet<ChangeHash>,
    pub(super) missing_dependencies: BTreeMap<ChangeHash, BTreeSet<ChangeHash>>,
    pub(super) unresolved_frontiers: BTreeMap<ChangeHash, BTreeSet<ChangeHash>>,
    authors: BTreeMap<ActorId, Vec<u8>>,
    policy: PolicySnapshot,
}
pub(super) struct InspectionDelta {
    changes: Vec<ChangeHash>,
    policy_changed: bool,
}
impl InspectionDelta {
    pub(super) fn is_empty(&self) -> bool {
        self.changes.is_empty() && !self.policy_changed
    }
    pub(super) fn contains(&self, hash: &ChangeHash) -> bool {
        self.changes.contains(hash)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Observer {
    capture: Capture,
    pub(super) value: crate::hydrate::Value,
}
pub(super) struct Transition {
    encoding: crate::TextEncoding,
    pub(super) before: Capture,
    pub(super) after: Capture,
    pub(super) content: Vec<Patch>,
    pub(super) status: InspectionDelta,
}
impl Transition {
    pub(super) fn apply(&self, observer: &mut Observer) -> Result<(), String> {
        if observer.capture != self.before {
            return Err("foreign or stale patch endpoint".into());
        }
        let mut value = observer.value.clone();
        value
            // Hydrated values contain plain text only. Rich-text replay is checked
            // separately by the formatting-aware test observer; never send Mark to
            // hydrate::Text::apply, whose mark arm is deliberately unimplemented.
            .apply_patches(
                self.encoding,
                self.content
                    .iter()
                    .filter(|p| !matches!(p.action, crate::PatchAction::Mark { .. }))
                    .cloned(),
            )
            .map_err(|e| e.to_string())?;
        *observer = Observer {
            capture: self.after.clone(),
            value,
        };
        Ok(())
    }
}
/// In-memory transport package. Policy is explicitly separate from native document bytes.
#[derive(Clone)]
pub(super) struct Package {
    version: u8,
    capture: Capture,
    pub(super) content: Vec<(Vec<u8>, Vec<u8>)>,
}
#[derive(Clone)]
pub(super) struct Session {
    pub(super) doc: Automerge,
    identity: ActorId,
    archive: BTreeMap<ChangeHash, Change>,
    authors: BTreeMap<ActorId, Vec<u8>>,
    controls: BTreeMap<ChangeHash, Revoke>,
    authorized: BTreeSet<ChangeHash>,
    invalidated: BTreeSet<ChangeHash>,
    contexts: BTreeMap<ChangeHash, u64>,
    grants: BTreeSet<u64>,
}
pub(super) fn ancestry(
    archive: &BTreeMap<ChangeHash, Change>,
    heads: &[ChangeHash],
) -> BTreeSet<ChangeHash> {
    let mut todo = heads.to_vec();
    let mut result = BTreeSet::new();
    while let Some(h) = todo.pop() {
        if let Some(c) = archive.get(&h) {
            if result.insert(h) {
                todo.extend(c.deps());
            }
        }
    }
    result
}
impl Session {
    pub(super) fn new() -> Self {
        Self::with_encoding(Automerge::new().text_encoding())
    }
    pub(super) fn with_encoding(encoding: crate::TextEncoding) -> Self {
        Self {
            doc: Automerge::new_with_encoding(encoding),
            identity: ActorId::random(),
            archive: BTreeMap::new(),
            authors: BTreeMap::new(),
            controls: BTreeMap::new(),
            authorized: BTreeSet::new(),
            invalidated: BTreeSet::new(),
            contexts: BTreeMap::new(),
            grants: BTreeSet::new(),
        }
    }
    pub(super) fn receive(
        &mut self,
        version: u8,
        group: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<Transition, String> {
        if version != 1 {
            return Err("experimental native-control capability/version required".into());
        }
        let inputs = group
            .into_iter()
            .map(|(bytes, author)| {
                Change::from_bytes(bytes)
                    .map(|c| Input::Content(c, author))
                    .map_err(|e| e.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.deliver(inputs)
    }
    pub(super) fn deliver(&mut self, inputs: Vec<Input>) -> Result<Transition, String> {
        let before = self.capture();
        let mut staged = self.clone();
        for input in inputs {
            match input {
                Input::Content(change, author) => {
                    if author.is_empty() {
                        return Err("missing trusted provenance".into());
                    }
                    if staged
                        .authors
                        .get(change.actor_id())
                        .is_some_and(|a| a != &author)
                    {
                        return Err("contradictory actor author".into());
                    }
                    let decoded = change.decode();
                    for op in &decoded.operations {
                        if matches!(op.action, legacy::OpType::Make(crate::ObjType::Table)) {
                            return Err("table creation not yet validated".into());
                        }
                        if let legacy::OpType::Revoke(bytes) = &op.action {
                            if change.len() != 1
                                || op.obj != legacy::ObjectId::Root
                                || op.key != legacy::Key::Map("".into())
                                || op.insert
                                || !op.pred.is_empty()
                            {
                                return Err(
                                    "control must be a canonical control-only change".into()
                                );
                            }
                            if staged.controls.keys().any(|h| *h != change.hash()) {
                                return Err("B1 supports exactly one control; competing controls unimplemented".into());
                            }
                            if change
                                .iter_ops()
                                .any(|op| op.expand || op.mark_name.is_some())
                            {
                                return Err("noncanonical control metadata".into());
                            }
                            staged
                                .controls
                                .insert(change.hash(), decode_control(bytes)?);
                        }
                    }
                    staged.authors.insert(change.actor_id().clone(), author);
                    staged.archive.insert(change.hash(), change);
                }
                Input::Authorize(r) => {
                    staged.authorized.insert(r);
                }
                Input::Invalidate(r) => {
                    staged.invalidated.insert(r);
                }
                Input::Context(h, context) => {
                    if staged
                        .contexts
                        .insert(h, context)
                        .is_some_and(|old| old != context)
                    {
                        return Err("contradictory context".into());
                    }
                }
                Input::Grant(g) => {
                    staged.grants.insert(g);
                }
            }
        }
        // Session-only structural scheduling: validate every ready control before publishing.
        loop {
            let ready: Vec<_> = staged
                .archive
                .values()
                .filter(|c| {
                    staged.doc.get_change_by_hash(&c.hash()).is_none()
                        && c.deps()
                            .iter()
                            .all(|h| staged.doc.get_change_by_hash(h).is_some())
                })
                .cloned()
                .collect();
            if ready.is_empty() {
                break;
            }
            for change in ready {
                if let Some(control) = staged.controls.get(&change.hash()) {
                    let ancestors = ancestry(&staged.archive, change.deps());
                    if control.retain.iter().any(|h| !ancestors.contains(h)) {
                        return Err("frontier is not dependency ancestry".into());
                    }
                }
                staged
                    .doc
                    .apply_changes([change])
                    .map_err(|e| e.to_string())?;
            }
        }
        let after = staged.capture();
        let before_clock = staged.scope(&before)?;
        let after_clock = staged.scope(&after)?;
        let mut log = PatchLog::active();
        DiffIter::log(
            &staged.doc,
            ObjMeta::root(),
            ClockRange::Diff(before_clock, after_clock.clone()),
            &mut log,
            true,
        );
        let content = log.make_scoped_patches(&staged.doc, Some(after_clock));
        let hashes: BTreeSet<_> = before
            .eligibility
            .keys()
            .chain(after.eligibility.keys())
            .copied()
            .collect();
        let changes = hashes
            .into_iter()
            .filter(|h| {
                before.eligibility.get(h) != after.eligibility.get(h)
                    || before.reasons.get(h) != after.reasons.get(h)
                    || before.authority.get(h) != after.authority.get(h)
                    || before.integrated.contains(h) != after.integrated.contains(h)
                    || before.missing_evidence.contains(h) != after.missing_evidence.contains(h)
                    || before.missing_dependencies.get(h) != after.missing_dependencies.get(h)
                    || before.unresolved_frontiers.get(h) != after.unresolved_frontiers.get(h)
            })
            .collect();
        let status = InspectionDelta {
            changes,
            policy_changed: before.policy != after.policy,
        };
        *self = staged;
        Ok(Transition {
            before,
            after,
            content,
            status,
            encoding: self.doc.text_encoding(),
        })
    }
    pub(super) fn edit<F>(
        &mut self,
        capture: &Capture,
        actor: ActorId,
        author: &[u8],
        f: F,
    ) -> Result<(ChangeHash, Transition), String>
    where
        F: FnOnce(&mut crate::transaction::Transaction<'_>) -> Result<(), crate::AutomergeError>,
    {
        self.scope(capture)?;
        let mut staged = self.clone();
        staged.doc.set_actor(actor);
        // Allocate against structural heads first, retaining ordinary concurrent-actor
        // isolation. Only then compile the selected query scope in that actor table.
        let mut args = staged.doc.transaction_args(Some(&capture.heads));
        let mut scope = staged.scope(capture)?;
        scope.include_transaction(args.actor_index, args.start_op.get());
        args.scope = Some(scope);
        let mut tx =
            crate::transaction::Transaction::new(&mut staged.doc, args, PatchLog::inactive());
        f(&mut tx).map_err(|e| e.to_string())?;
        let hash = tx.commit().0.ok_or("empty edit")?;
        let change = staged.doc.get_change_by_hash(&hash).ok_or("missing edit")?;
        let transition = self.deliver(vec![Input::Content(change, author.to_vec())])?;
        Ok((hash, transition))
    }
    pub(super) fn export(&self) -> Package {
        Package {
            version: 1,
            capture: self.capture(),
            content: self
                .archive
                .values()
                .map(|c| (c.raw_bytes().to_vec(), self.authors[c.actor_id()].clone()))
                .collect(),
        }
    }
    pub(super) fn restore(package: Package) -> Result<Self, String> {
        let mut restored = Self::with_encoding(package.capture.encoding);
        restored.identity = package.capture.identity.clone();
        restored.receive(package.version, package.content)?;
        let policy = &package.capture.policy;
        let mut inputs = Vec::new();
        inputs.extend(policy.authorized.iter().copied().map(Input::Authorize));
        inputs.extend(policy.invalidated.iter().copied().map(Input::Invalidate));
        inputs.extend(policy.contexts.iter().map(|(h, g)| Input::Context(*h, *g)));
        inputs.extend(policy.grants.iter().copied().map(Input::Grant));
        restored.deliver(inputs)?;
        if restored.capture() != package.capture {
            return Err("incomplete or incompatible capture package".into());
        }
        Ok(restored)
    }
    pub(super) fn reconstruct(&self) -> Result<Self, String> {
        Self::restore(self.export())
    }
    pub(super) fn capture(&self) -> Capture {
        let integrated: BTreeSet<_> = self.doc.get_changes(&[]).iter().map(Change::hash).collect();
        let policy = PolicySnapshot {
            authorized: self.authorized.clone(),
            invalidated: self.invalidated.clone(),
            contexts: self.contexts.clone(),
            grants: self.grants.clone(),
        };
        let Interpretation {
            eligibility,
            authority,
            reasons,
        } = resolve_controls(
            HistoryFacts {
                archive: &self.archive,
                authors: &self.authors,
                controls: &self.controls,
                integrated: &integrated,
            },
            &policy,
        );
        let missing_dependencies = self
            .archive
            .iter()
            .filter_map(|(h, c)| {
                let missing: BTreeSet<_> = c
                    .deps()
                    .iter()
                    .filter(|d| !integrated.contains(d))
                    .copied()
                    .collect();
                (!missing.is_empty()).then_some((*h, missing))
            })
            .collect();
        let unresolved_frontiers = self
            .controls
            .iter()
            .filter_map(|(h, c)| {
                let missing: BTreeSet<_> = c
                    .retain
                    .iter()
                    .filter(|d| !integrated.contains(d))
                    .copied()
                    .collect();
                (!missing.is_empty()).then_some((*h, missing))
            })
            .collect();
        Capture {
            identity: self.identity.clone(),
            encoding: self.doc.text_encoding(),
            heads: self.doc.get_heads(),
            eligibility,
            authority,
            integrated,
            missing_dependencies,
            unresolved_frontiers,
            reasons,
            authors: self.authors.clone(),
            missing_evidence: self
                .invalidated
                .difference(&self.authorized)
                .copied()
                .collect(),
            policy,
        }
    }
    pub(super) fn scope(&self, capture: &Capture) -> Result<Clock, String> {
        if capture.encoding != self.doc.text_encoding() {
            return Err("incompatible capture text encoding".into());
        }
        if capture.identity != self.identity {
            return Err("foreign capture".into());
        }
        if capture
            .heads
            .iter()
            .any(|h| !capture.integrated.contains(h))
        {
            return Err("capture heads outside integrated manifest".into());
        }
        if capture
            .integrated
            .iter()
            .any(|h| self.doc.get_change_by_hash(h).is_none())
        {
            return Err("capture missing history".into());
        }
        let mut clock = self.doc.change_graph.clock_at(&capture.heads);
        let mut selected = BTreeSet::new();
        for (hash, status) in &capture.eligibility {
            if *status != Eligibility::Eligible || !capture.integrated.contains(hash) {
                continue;
            }
            let c = self.archive.get(hash).ok_or("missing archive")?;
            let actor = self
                .doc
                .ops
                .lookup_actor(c.actor_id())
                .ok_or("missing actor")?;
            for counter in c.start_op().get()..=c.max_op() {
                selected.insert(OpId::new(counter, actor));
            }
        }
        clock.select(selected);
        Ok(clock)
    }
    pub(super) fn observe(&self, capture: &Capture) -> Result<Observer, String> {
        self.scope(capture)?;
        Ok(Observer {
            capture: capture.clone(),
            value: self.hydrate(capture),
        })
    }
    pub(super) fn hydrate(&self, capture: &Capture) -> crate::hydrate::Value {
        self.doc.hydrate_map(
            &crate::types::ObjId::root(),
            Some(&self.scope(capture).unwrap()),
        )
    }
    pub(super) fn candidates(
        &self,
        capture: &Capture,
        obj: &ObjId,
        key: &str,
    ) -> Vec<(Value<'static>, ObjId)> {
        self.doc
            .get_all_for(obj, key, Some(self.scope(capture).unwrap()))
            .unwrap()
            .into_iter()
            .map(|(v, id)| (v.into_owned(), id))
            .collect()
    }
    pub(super) fn scalar(&self, capture: &Capture, key: &str) -> Option<String> {
        self.candidates(capture, &ROOT, key)
            .last()
            .and_then(|(v, _)| match v {
                Value::Scalar(s) => match s.as_ref() {
                    ScalarValue::Str(s) => Some(s.to_string()),
                    ScalarValue::Int(n) => Some(n.to_string()),
                    _ => None,
                },
                _ => None,
            })
    }
}
