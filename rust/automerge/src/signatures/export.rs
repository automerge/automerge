use std::collections::BTreeMap;

use ed25519_dalek::VerifyingKey;

use crate::{storage, Automerge, ChangeHash, Fragment, SigningRequest};

use super::{ed25519_key, SignatureRef};

impl Automerge {
    /// The Ed25519 signatures this document still needs.
    ///
    /// Two things are asked for. Changes above an actor's signed frontier, which
    /// is where a fresh local commit sits; and the actor maxima of every fragment
    /// in the index which carry no verifying signature of their own.
    ///
    /// The second is not implied by the first. One signature at an actor's greatest
    /// sequence authenticates that actor's whole prefix, which is all admission
    /// needs, so signing only the latest change lifts the frontier over
    /// everything below it. But a fragment travels alone, and its receiver can
    /// only check the signature on the exact hash it is given, so every fragment
    /// boundary needs its own signature. Asking for them here is what stops a
    /// signature at the tip from leaving the boundaries below it unexportable
    /// with nothing ever requested to fix it.
    ///
    /// Actors without an author, or with an author which is not an Ed25519 key,
    /// cannot be signed and are not reported.
    pub fn signing_requests(&self) -> Vec<SigningRequest> {
        let mut requests = BTreeMap::new();
        for hash in self.unsigned_changes() {
            if let Some(request) = self.signing_request_for(hash) {
                requests.entry(hash).or_insert(request);
            }
        }
        // The index's whole cover, not the exportable one: a fragment withheld
        // for want of a signature is exactly what needs requesting.
        for fragment in self
            .change_graph
            .fragments(&self.get_heads(), ..)
            .collect::<Vec<_>>()
        {
            for (hash, _) in self.actor_maxima(&fragment) {
                if self.has_verifying_signature(&hash) {
                    continue;
                }
                if let Some(request) = self.signing_request_for(hash) {
                    requests.entry(hash).or_insert(request);
                }
            }
        }
        requests.into_values().collect()
    }

    /// A request for `hash`, unless its actor has no author this document could
    /// ever produce an Ed25519 signature under.
    fn signing_request_for(&self, hash: ChangeHash) -> Option<SigningRequest> {
        let (actor, _) = self.change_graph.actor_seq_for_hash(&hash)?;
        let author = self.authors.get_author_for_actor(actor)?;
        ed25519_key(&author)?;
        Some(SigningRequest::new(hash, author.into_owned()))
    }

    /// Whether some signature on `hash` verifies under the author of its actor.
    pub(crate) fn has_verifying_signature(&self, hash: &ChangeHash) -> bool {
        self.materialized_ed25519_key(hash)
            .is_some_and(|key| self.signatures.is_valid(hash, &key))
    }

    /// A signature chunk authenticating `fragment`, or `None` if this document
    /// cannot authenticate it.
    ///
    /// A receiver requires a signature at each actor maximum of the chunk it is
    /// given, so a fragment missing one of those signatures is not exportable: it
    /// would be refused whole. Returning `None` withholds it instead of shipping
    /// bytes which cannot be used.
    pub(crate) fn fragment_signature_chunk(&self, fragment: &Fragment) -> Option<Vec<u8>> {
        let mut signatures = Vec::new();
        for (hash, _) in self.actor_maxima(fragment) {
            if !self.can_authenticate(&hash) {
                return None;
            }
            signatures.extend(
                self.signatures
                    .signatures_for(&hash)
                    .map(move |bytes| SignatureRef {
                        change_hash: hash,
                        bytes,
                    }),
            );
        }
        if signatures.is_empty() {
            return Some(Vec::new());
        }
        Some(storage::SignatureChunk::encode(signatures))
    }

    /// Whether this document could authenticate `hash` to a peer, which means
    /// holding a signature on that exact hash — not merely one which covers it.
    ///
    /// Vacuous in legacy mode, as coverage is: a legacy peer requires nothing, so
    /// there is nothing a legacy document cannot authenticate to it.
    fn can_authenticate(&self, hash: &ChangeHash) -> bool {
        !self.is_authenticated() || self.has_verifying_signature(hash)
    }

    /// The Ed25519 key a signature on a materialized `hash` must verify under.
    pub(crate) fn materialized_ed25519_key(&self, hash: &ChangeHash) -> Option<VerifyingKey> {
        let (actor, _) = self.change_graph.actor_seq_for_hash(hash)?;
        ed25519_key(&self.authors.get_author_for_actor(actor)?)
    }

    fn actor_maxima(&self, fragment: &Fragment) -> Vec<(ChangeHash, usize)> {
        self.change_graph
            .actor_maxima_for_hashes(fragment.members.iter().copied())
    }
}
