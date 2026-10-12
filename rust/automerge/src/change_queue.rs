//! History on its way into the document.
//!
//! Everything a document has received but not applied lives here: the detached
//! signatures it holds, the mode which says what those signatures must
//! cover, and the chunks of history waiting for either a signature or a
//! dependency. The three travel together because the decisions they feed are
//! one decision — whether a piece of history may enter the document — which is
//! made in [`ChangeQueue::pop_ready`] and nowhere else.
//!
//! A queued chunk is released when it is causally ready and, if it requires
//! signatures, when those signatures verify. Those are the same wait in
//! different clothes. A chunk's signatures are verified under the authors of
//! the actors it contains, and an actor's author is declared by its first
//! change — which is in the causal history of everything that actor later
//! writes. A chunk whose key cannot be found yet is one whose dependencies have
//! not arrived; a chunk whose dependencies have all arrived always has its keys.
//! So there is one queue, with one readiness test, and the signature check is run
//! at the moment a chunk would otherwise be released.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use ed25519_dalek::VerifyingKey;

use crate::{
    change_graph::ChangeGraph,
    signatures::{self, Signature, SignatureMode, SignatureStore},
    ActorId, Author, AutomergeError, Change, ChangeHash,
};

mod actor_queue;
use actor_queue::ActorQueue;
mod doc_state;
pub(crate) use doc_state::DocState;
mod entry_id;
use entry_id::EntryId;

/// Whether the signatures a chunk requires verify.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Coverage {
    /// Every required hash carries a signature which verifies under the key of
    /// its actor's author.
    Verified,
    /// Some required hash carries no signature which verifies under a key this
    /// document can see.
    Failed,
    /// Some required hash's author cannot be found anywhere: its actor's first
    /// change is neither applied nor queued. Nothing can be said yet.
    AuthorUnknown,
}

/// What one call to [`ChangeQueue::pop_ready`] released.
#[derive(Debug, Default)]
pub(crate) struct Released {
    /// The released changes in an order which respects their dependencies.
    pub(crate) changes: Vec<Change>,
    /// How many of the released chunks required signatures.
    pub(crate) authenticated_chunks: usize,
}

/// What one call to [`crate::Automerge::apply_ready`] applied.
#[derive(Debug, Default)]
pub(crate) struct Applied {
    /// The applied changes, in the order they were applied.
    pub(crate) hashes: Vec<ChangeHash>,
    /// How many of the applied chunks required signatures.
    pub(crate) authenticated_chunks: usize,
}

/// One unit of queued history, released whole.
#[derive(Clone, Debug)]
pub(crate) struct QueuedChunk {
    changes: Vec<Change>,
    /// `Some` for a chunk which must be authenticated before it is released,
    /// `None` for history trusted on arrival, which is always one change.
    authentication: Option<Authentication>,
}

/// What a chunk needs before it may be released, and how it is written back.
#[derive(Clone, Debug)]
struct Authentication {
    /// The chunk exactly as it arrived.
    ///
    /// A save writes this back verbatim rather than re-encoding the changes,
    /// because the chunk is the unit of authentication: its signatures cover its
    /// per-actor maxima, so history regrouped into different chunks would need
    /// signatures this document does not have, and would be refused on reload.
    raw: Vec<u8>,
    /// The change hashes which each need one valid signature before this chunk
    /// may be released, with the actor whose author the signature must verify
    /// under: the greatest sequence each actor contributes.
    required: Vec<(ActorId, ChangeHash)>,
}

impl QueuedChunk {
    /// A change trusted on arrival, which waits only for its dependencies.
    pub(crate) fn trusted(change: Change) -> Self {
        Self {
            changes: vec![change],
            authentication: None,
        }
    }

    /// A chunk of history which must be authenticated before it is released,
    /// decoded from the bytes it arrived as.
    ///
    /// A chunk requires one valid signature for the greatest sequence each actor
    /// contributes to it. A signature's target hash commits to that change's
    /// whole causal history, so one signature at an actor's maximum covers every
    /// change that actor contributes to the chunk — provided each of those
    /// changes really is in the maximum's history. That is what this checks.
    ///
    /// # Errors
    ///
    /// Returns [`AutomergeError::DuplicateSeqNumber`] if one actor claims the
    /// same sequence twice, and [`AutomergeError::UncoveredChunk`] if some
    /// change could not be covered by its own actor's maximum. A real fragment
    /// always satisfies both: its members are causally closed on
    /// `(boundary, head]`, so any change causally between two members is itself
    /// a member, and an actor's sequence is monotonic in its own history.
    pub(crate) fn authenticated(
        raw: Vec<u8>,
        changes: Vec<Change>,
    ) -> Result<Self, AutomergeError> {
        let mut maxima: BTreeMap<&ActorId, (u64, ChangeHash)> = BTreeMap::new();
        for change in &changes {
            match maxima.get_mut(change.actor_id()) {
                Some((seq, _)) if *seq == change.seq() => {
                    return Err(AutomergeError::duplicate_seq(change))
                }
                Some((seq, hash)) if *seq < change.seq() => {
                    *seq = change.seq();
                    *hash = change.hash();
                }
                Some(_) => {}
                None => {
                    maxima.insert(change.actor_id(), (change.seq(), change.hash()));
                }
            }
        }
        Self::assert_covered(&changes, &maxima)?;
        let required = maxima
            .into_iter()
            .map(|(actor, (_, hash))| (actor.clone(), hash))
            .collect();
        Ok(Self {
            changes,
            authentication: Some(Authentication { raw, required }),
        })
    }

    /// Check that every change is in the causal history of its own actor's
    /// maximum, following dependencies within the chunk.
    ///
    /// Reachability must start from the *same actor's* maximum: reaching a change
    /// from another actor's maximum proves only that the other actor witnessed
    /// it, not that its author attested to it.
    ///
    /// Stopping at the chunk's edge is deliberate rather than approximate. A
    /// change below the boundary is already accepted, and therefore already
    /// authenticated by whichever chunk carried it; a change which is neither in
    /// this chunk nor accepted cannot lie on a path between two members of a
    /// causally closed fragment.
    fn assert_covered(
        changes: &[Change],
        maxima: &BTreeMap<&ActorId, (u64, ChangeHash)>,
    ) -> Result<(), AutomergeError> {
        let by_hash: HashMap<ChangeHash, &Change> = changes
            .iter()
            .map(|change| (change.hash(), change))
            .collect();
        for (actor, (_, maximum)) in maxima {
            let covered = Self::causal_history(&by_hash, *maximum);
            for change in changes.iter().filter(|c| c.actor_id() == *actor) {
                if !covered.contains(&change.hash()) {
                    return Err(AutomergeError::UncoveredChunk(
                        change.hash(),
                        change.actor_id().clone(),
                    ));
                }
            }
        }
        Ok(())
    }

    /// The causal history of `from`, including itself, confined to the chunk.
    fn causal_history(
        by_hash: &HashMap<ChangeHash, &Change>,
        from: ChangeHash,
    ) -> BTreeSet<ChangeHash> {
        let mut seen = BTreeSet::new();
        let mut stack = vec![from];
        while let Some(hash) = stack.pop() {
            let Some(change) = by_hash.get(&hash) else {
                continue;
            };
            if !seen.insert(hash) {
                continue;
            }
            stack.extend(change.deps());
        }
        seen
    }

    fn requires_signatures(&self) -> bool {
        self.authentication.is_some()
    }

    /// The bytes a save writes this chunk back as.
    pub(crate) fn raw_bytes(&self) -> &[u8] {
        match &self.authentication {
            Some(authentication) => &authentication.raw,
            // Trusted history is one change, and a change is its own chunk.
            None => self.changes[0].raw_bytes(),
        }
    }
}

/// The signatures a document holds and the history waiting to enter it.
///
/// See the module documentation for why these are one thing.
#[derive(Debug, Clone)]
pub(crate) struct ChangeQueue {
    mode: SignatureMode,
    signatures: SignatureStore,
    /// Queued chunks in arrival order, under ids which never change, so the
    /// indexes below survive a removal.
    chunks: BTreeMap<EntryId, QueuedChunk>,
    next_id: EntryId,
    /// Which chunk holds each queued change.
    by_hash: HashMap<ChangeHash, EntryId>,
    /// Which chunk holds each queued actor sequence. Also where an actor's
    /// author is found while its first change is still queued.
    by_actor: HashMap<ActorId, ActorQueue>,
}

impl ChangeQueue {
    pub(crate) fn new(mode: SignatureMode) -> Self {
        Self {
            mode,
            signatures: SignatureStore::default(),
            chunks: BTreeMap::new(),
            next_id: EntryId::zero(),
            by_hash: HashMap::new(),
            by_actor: HashMap::new(),
        }
    }

    pub(crate) fn with_signatures(
        mode: SignatureMode,
        signatures: impl IntoIterator<Item = Signature>,
    ) -> Self {
        let mut queue = Self::new(mode);
        queue.signatures.extend(signatures);
        queue
    }

    /// A queue with the same mode and signatures but nothing waiting, for a
    /// document forked from this one.
    pub(crate) fn fork(&self) -> Self {
        Self {
            mode: self.mode,
            signatures: self.signatures.clone(),
            ..Self::new(self.mode)
        }
    }

    pub(crate) fn mode(&self) -> SignatureMode {
        self.mode
    }

    pub(crate) fn signatures(&self) -> &SignatureStore {
        &self.signatures
    }

    /// Whether nothing is waiting. Signatures held are not history.
    pub(crate) fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Every queued change, in arrival order.
    pub(crate) fn changes(&self) -> impl Iterator<Item = &Change> {
        self.chunks.values().flat_map(|chunk| chunk.changes.iter())
    }

    /// Every queued chunk, in arrival order.
    pub(crate) fn chunks(&self) -> impl Iterator<Item = &QueuedChunk> {
        self.chunks.values()
    }

    /// How many queued chunks are waiting for signatures.
    pub(crate) fn authenticated_chunks(&self) -> usize {
        self.chunks
            .values()
            .filter(|chunk| chunk.requires_signatures())
            .count()
    }

    /// Retain `signatures` without checking them. They are checked when
    /// something asks whether they verify.
    pub(crate) fn extend_signatures(&mut self, signatures: impl IntoIterator<Item = Signature>) {
        self.signatures.extend(signatures);
    }

    /// Retain `signatures`, verifying any whose target is already applied.
    ///
    /// Verification is remembered, so every later question about the signature is
    /// answered without re-running the check. Signatures targeting queued or absent
    /// history are checked when that history is released.
    pub(crate) fn add_signatures(
        &mut self,
        signatures: impl IntoIterator<Item = Signature>,
        doc: &DocState<'_>,
    ) {
        let signatures: Vec<_> = signatures.into_iter().collect();
        let targets: Vec<_> = signatures
            .iter()
            .map(|signature| signature.change_hash)
            .collect();
        self.signatures.extend(signatures);
        for hash in targets {
            if let Some(key) = doc
                .actor_of(&hash)
                .and_then(|actor| self.key_of(actor, doc))
            {
                self.signatures.verify(hash, &key);
            }
        }
    }

    /// Queue `chunks`, having checked each change against the document and
    /// against what is already queued, and each chunk's signatures against what
    /// can be seen of its authors.
    ///
    /// Either everything is queued or nothing is: an error leaves the queue as
    /// it was, except that a change conflicting with the document's own actor
    /// sequence evicts the queued branch it conflicts with, as it always has.
    ///
    /// A change already applied or already queued is dropped from its chunk
    /// rather than refused, so re-receiving history is harmless. A chunk which
    /// requires signatures must carry a signature at each required hash, and where
    /// the author can already be seen the signature must verify now, so that a
    /// stripped or forged signature fails at import rather than waiting forever.
    pub(crate) fn push(
        &mut self,
        chunks: impl IntoIterator<Item = QueuedChunk>,
        doc: &DocState<'_>,
    ) -> Result<(), AutomergeError> {
        let mark = self.next_id;
        let result = self.try_push(chunks, doc);
        if result.is_err() {
            let unwound = self.chunks.split_off(&mark);
            for chunk in unwound.values() {
                self.unindex(chunk);
            }
        }
        result
    }

    fn try_push(
        &mut self,
        chunks: impl IntoIterator<Item = QueuedChunk>,
        doc: &DocState<'_>,
    ) -> Result<(), AutomergeError> {
        let mark = self.next_id;
        for mut chunk in chunks {
            chunk.changes.retain(|change| {
                !doc.graph.has_change(&change.hash()) && !self.by_hash.contains_key(&change.hash())
            });
            if chunk.changes.is_empty() {
                continue;
            }
            for change in &chunk.changes {
                self.check_change(change, doc)?;
            }
            self.insert(chunk);
        }
        let pushed: Vec<EntryId> = self.chunks.range(mark..).map(|(id, _)| *id).collect();
        for id in pushed {
            self.check_signatures_on_arrival(id, doc)?;
        }
        Ok(())
    }

    /// The checks one change must pass to be queued, which do not depend on
    /// the chunk it arrived in.
    fn check_change(
        &mut self,
        change: &Change,
        doc: &DocState<'_>,
    ) -> Result<(), AutomergeError> {
        if doc.has_actor_seq(change) {
            // The document already holds this actor sequence under another
            // hash. Any queued change at a later sequence belongs to the same
            // incompatible branch and can never apply.
            self.remove_actor_branch_from(change.actor_id(), change.seq().saturating_add(1));
            return Err(AutomergeError::duplicate_seq(change));
        }
        let queued = self.by_actor.get(change.actor_id());
        if queued.is_some_and(|seqs| seqs.at(change.seq()).is_some()) {
            return Err(AutomergeError::duplicate_seq(change));
        }
        if change.author().is_some() {
            let known = doc.author_of(change.actor_id()).is_some()
                || self.author_for_actor(change.actor_id()).is_some();
            if known {
                return Err(AutomergeError::duplicate_author(change));
            }
            if change.seq() != 1 {
                return Err(AutomergeError::author_on_non_initial_seq(change));
            }
        }
        Ok(())
    }

    /// Refuse a newly queued chunk whose signatures cannot authenticate it.
    ///
    /// A chunk only ever waits for an author to become reachable, never for a
    /// signature which may never arrive: every required hash must carry some
    /// signature, and where the author can be seen now the signature must verify now.
    fn check_signatures_on_arrival(
        &mut self,
        id: EntryId,
        doc: &DocState<'_>,
    ) -> Result<(), AutomergeError> {
        let Some(required) = self.required_of(id) else {
            return Ok(());
        };
        for (actor, hash) in &required {
            match self.key_of(actor, doc) {
                Some(key) if self.signatures.verify(*hash, &key) => {}
                Some(_) => return Err(AutomergeError::UnsignedChunk(*hash)),
                None if self.signatures.has_any(hash) => {}
                None => return Err(AutomergeError::UnsignedChunk(*hash)),
            }
        }
        Ok(())
    }

    fn insert(&mut self, chunk: QueuedChunk) {
        let id = self.next_id;
        self.next_id = id.next();
        for change in &chunk.changes {
            self.by_hash.insert(change.hash(), id);
            self.by_actor
                .entry(change.actor_id().clone())
                .or_default()
                .insert(change.seq(), id);
        }
        self.chunks.insert(id, chunk);
    }

    fn remove(&mut self, id: EntryId) -> Option<QueuedChunk> {
        let chunk = self.chunks.remove(&id)?;
        self.unindex(&chunk);
        Some(chunk)
    }

    fn unindex(&mut self, chunk: &QueuedChunk) {
        for change in &chunk.changes {
            self.by_hash.remove(&change.hash());
            if let Some(seqs) = self.by_actor.get_mut(change.actor_id()) {
                seqs.remove(change.seq());
                if seqs.is_empty() {
                    self.by_actor.remove(change.actor_id());
                }
            }
        }
    }

    /// What a queued chunk requires, or `None` if it is trusted.
    ///
    /// Owned, so that the signatures can be mutated while it is walked.
    fn required_of(&self, id: EntryId) -> Option<Vec<(ActorId, ChangeHash)>> {
        self.chunks[&id]
            .authentication
            .as_ref()
            .map(|authentication| authentication.required.clone())
    }

    /// The author `actor` assigns itself, if its first change is queued.
    ///
    /// An author assignment only ever appears on sequence one, and the
    /// signature over any later change in the chain commits to it
    /// transitively, so reading it here is as trustworthy as the signature
    /// which will authenticate the chunk.
    fn author_for_actor(&self, actor: &ActorId) -> Option<Author<'_>> {
        let id = self.by_actor.get(actor)?.first()?;
        self.chunks[&id]
            .changes
            .iter()
            .find(|change| change.actor_id() == actor && change.seq() == 1)
            .and_then(Change::author)
    }

    /// The Ed25519 key a signature on one of `actor`'s changes must verify
    /// under, from wherever the actor's first change is.
    fn key_of(&self, actor: &ActorId, doc: &DocState<'_>) -> Option<VerifyingKey> {
        let author = doc
            .author_of(actor)
            .or_else(|| self.author_for_actor(actor))?;
        signatures::ed25519_key(&author)
    }

    /// Whether the signatures a queued chunk requires verify.
    ///
    /// An author which is reachable but is not an Ed25519 key counts as a
    /// failure: no signature will ever verify under it.
    fn coverage(&mut self, id: EntryId, doc: &DocState<'_>) -> Coverage {
        let Some(required) = self.required_of(id) else {
            return Coverage::Verified;
        };
        let mut coverage = Coverage::Verified;
        for (actor, hash) in &required {
            let author = doc
                .author_of(actor)
                .or_else(|| self.author_for_actor(actor));
            let Some(author) = author else {
                coverage = Coverage::AuthorUnknown;
                continue;
            };
            let verified = signatures::ed25519_key(&author)
                .is_some_and(|key| self.signatures.verify(*hash, &key));
            if !verified {
                return Coverage::Failed;
            }
        }
        coverage
    }

    /// Remove queued changes at or after an incompatible actor sequence,
    /// together with changes which transitively depend on that branch.
    ///
    /// Removal is by chunk: a chunk is released whole, so it is evicted whole.
    pub(crate) fn remove_actor_branch_from(&mut self, actor: &ActorId, seq: u64) {
        let seeds: Vec<EntryId> = self
            .by_actor
            .get(actor)
            .map(|seqs| seqs.from_seq(seq).collect())
            .unwrap_or_default();
        if seeds.is_empty() {
            return;
        }
        let mut dependents: HashMap<ChangeHash, Vec<EntryId>> = HashMap::new();
        for (id, chunk) in &self.chunks {
            for change in &chunk.changes {
                for dep in change.deps() {
                    dependents.entry(*dep).or_default().push(*id);
                }
            }
        }
        let mut removed: BTreeSet<EntryId> = seeds.iter().copied().collect();
        let mut to_visit: VecDeque<EntryId> = seeds.into_iter().collect();
        while let Some(id) = to_visit.pop_front() {
            for change in &self.chunks[&id].changes {
                if let Some(children) = dependents.remove(&change.hash()) {
                    for child in children {
                        if removed.insert(child) {
                            to_visit.push_back(child);
                        }
                    }
                }
            }
        }
        for id in removed {
            self.remove(id);
        }
    }

    /// Release every chunk which is causally ready and, if it requires
    /// signatures, authenticated.
    ///
    /// A chunk is causally ready when every dependency of every change in it is
    /// applied, in the chunk itself, or in a chunk released before it. Chunks are
    /// visited in that order, so by the time one is examined the author of each
    /// actor it contains is reachable — applied, released earlier in this pass,
    /// or in the chunk itself — and its signatures can be checked. A chunk whose
    /// signatures do not verify is dropped: nothing will ever make them verify, and
    /// holding it would only hide that. Whatever depended on it stays queued,
    /// since the same history may yet arrive properly signed.
    pub(crate) fn pop_ready(&mut self, doc: &DocState<'_>) -> Released {
        let ids: Vec<EntryId> = self.chunks.keys().copied().collect();
        let mut unsatisfied = vec![0u32; ids.len()];
        let mut waiting_on: HashMap<ChangeHash, Vec<usize>> = HashMap::new();
        for (index, id) in ids.iter().enumerate() {
            let chunk = &self.chunks[id];
            let members: HashSet<ChangeHash> = chunk.changes.iter().map(Change::hash).collect();
            for change in &chunk.changes {
                for dep in change.deps() {
                    if members.contains(dep) || doc.graph.has_change(dep) {
                        continue;
                    }
                    unsatisfied[index] += 1;
                    waiting_on.entry(*dep).or_default().push(index);
                }
            }
        }

        let mut ready: VecDeque<usize> = (0..ids.len())
            .filter(|index| unsatisfied[*index] == 0)
            .collect();
        let mut released = Vec::new();
        let mut refused = Vec::new();
        while let Some(index) = ready.pop_front() {
            let id = ids[index];
            match self.coverage(id, doc) {
                Coverage::Verified => {}
                Coverage::Failed => {
                    refused.push(id);
                    continue;
                }
                Coverage::AuthorUnknown => continue,
            }
            released.push(id);
            for change in &self.chunks[&id].changes {
                let Some(dependents) = waiting_on.remove(&change.hash()) else {
                    continue;
                };
                for dependent in dependents {
                    unsatisfied[dependent] -= 1;
                    if unsatisfied[dependent] == 0 {
                        ready.push_back(dependent);
                    }
                }
            }
        }

        for id in refused {
            self.remove(id);
        }
        let mut result = Released::default();
        let mut changes = Vec::new();
        for id in released {
            let chunk = self.remove(id).expect("released chunk is queued");
            if chunk.requires_signatures() {
                result.authenticated_chunks += 1;
            }
            changes.extend(chunk.changes);
        }
        result.changes = topological_order(changes, doc.graph);
        result
    }
}

/// Order `changes`, every one of which is releasable, so that each follows
/// its dependencies.
///
/// Chunks were released in dependency order, but the changes within a chunk
/// come in whatever order the chunk encoded them.
fn topological_order(changes: Vec<Change>, graph: &ChangeGraph) -> Vec<Change> {
    let mut unsatisfied = vec![0u32; changes.len()];
    let mut waiting_on: HashMap<ChangeHash, Vec<usize>> = HashMap::new();
    for (index, change) in changes.iter().enumerate() {
        for dep in change.deps() {
            if !graph.has_change(dep) {
                unsatisfied[index] += 1;
                waiting_on.entry(*dep).or_default().push(index);
            }
        }
    }
    let mut ready: VecDeque<usize> = (0..changes.len())
        .filter(|index| unsatisfied[*index] == 0)
        .collect();
    let mut order = Vec::with_capacity(changes.len());
    while let Some(index) = ready.pop_front() {
        order.push(index);
        if let Some(dependents) = waiting_on.remove(&changes[index].hash()) {
            for dependent in dependents {
                unsatisfied[dependent] -= 1;
                if unsatisfied[dependent] == 0 {
                    ready.push_back(dependent);
                }
            }
        }
    }
    debug_assert_eq!(
        order.len(),
        changes.len(),
        "every released change is orderable"
    );
    let mut slots: Vec<Option<Change>> = changes.into_iter().map(Some).collect();
    order
        .into_iter()
        .map(|index| slots[index].take().expect("each index is ordered once"))
        .collect()
}
