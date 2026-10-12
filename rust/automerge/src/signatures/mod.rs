//! Detached signatures over change hashes.
//!
//! A signature is an annotation on a change hash, stored beside the change
//! rather than inside it, so attaching one never alters the change or its
//! hash. Signatures are retained and relayed whether or not Automerge can
//! interpret them; only the Ed25519 scheme is ever verified.

mod export;
mod store;

pub(crate) use store::SignatureStore;

use ed25519_dalek::{Signature as Ed25519Signature, VerifyingKey};

use std::collections::{BTreeSet, HashMap};

use crate::{author::Authors, change_graph::ChangeGraph, Author, ChangeHash};

/// A detached signature, paired with the change hash it annotates.
///
/// The hash is not derived from the signature and the signature is not part of
/// the hash: this pairing is the whole of the relationship between a signature
/// and what it signs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Signature {
    pub(crate) change_hash: ChangeHash,
    pub(crate) bytes: Vec<u8>,
}

/// A [`Signature`] borrowed from the document holding it.
///
/// Ordered by change hash, then signature bytes, for canonical encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SignatureRef<'a> {
    pub(crate) change_hash: ChangeHash,
    pub(crate) bytes: &'a [u8],
}

impl SignatureRef<'_> {
    pub(crate) fn to_owned(self) -> Signature {
        Signature {
            change_hash: self.change_hash,
            bytes: self.bytes.to_vec(),
        }
    }
}

/// The first byte of every signature names its scheme.
const ED25519_TAG: u8 = 0;
const CUSTOM_TAG: u8 = 1;

/// Whether a document trusts the changes it receives or holds them until a
/// signature covers them.
///
/// The mode is fixed when a document is created or loaded, and forks inherit
/// it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SignatureMode {
    /// Apply received changes as Automerge always has. Signatures are
    /// retained and relayed but never consulted.
    #[default]
    Legacy,
    /// Hold received changes until a valid Ed25519 signature from their
    /// actor's author covers them. Local commits stay visible immediately,
    /// since signing them may need a key held outside Automerge.
    Authenticated,
}

/// A signature to attach to a change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignatureInput {
    change_hash: ChangeHash,
    bytes: Vec<u8>,
}

impl SignatureInput {
    /// An Ed25519 signature over the raw bytes of `change_hash`, made with the
    /// key whose public half is the author of the change's actor.
    pub fn ed25519(change_hash: ChangeHash, signature: [u8; 64]) -> Self {
        Self::tagged(change_hash, ED25519_TAG, &signature)
    }

    /// Application-defined bytes which Automerge retains and relays but never
    /// uses for authentication.
    pub fn custom(change_hash: ChangeHash, bytes: impl AsRef<[u8]>) -> Self {
        Self::tagged(change_hash, CUSTOM_TAG, bytes.as_ref())
    }

    fn tagged(change_hash: ChangeHash, tag: u8, payload: &[u8]) -> Self {
        let mut bytes = Vec::with_capacity(payload.len() + 1);
        bytes.push(tag);
        bytes.extend_from_slice(payload);
        Self { change_hash, bytes }
    }

    pub fn change_hash(&self) -> ChangeHash {
        self.change_hash
    }

    pub(crate) fn into_signature(self) -> Signature {
        Signature {
            change_hash: self.change_hash,
            bytes: self.bytes,
        }
    }
}

/// A change which needs an Ed25519 signature before the fragment it bounds
/// can be exported. See [`crate::Automerge::signing_requests`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SigningRequest {
    change_hash: ChangeHash,
    author: Author<'static>,
}

impl SigningRequest {
    pub(crate) fn new(change_hash: ChangeHash, author: Author<'static>) -> Self {
        Self {
            change_hash,
            author,
        }
    }

    pub fn change_hash(&self) -> ChangeHash {
        self.change_hash
    }

    /// The Ed25519 verifying key the signature must match.
    ///
    /// This is what selects the private half to sign with. The actor whose chain
    /// the change belongs to is deliberately absent: several actors may share an
    /// author, so it would not narrow the choice of key, and the signature is
    /// verified under the author alone.
    pub fn author(&self) -> &Author<'static> {
        &self.author
    }

    /// The exact bytes to sign: the raw change hash.
    pub fn bytes_to_sign(&self) -> &[u8] {
        self.change_hash.as_bytes()
    }
}

/// What one call to [`crate::Automerge::add_signatures`] admitted.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SignatureReport {
    admitted_chunks: usize,
    applied_changes: Vec<ChangeHash>,
    pending_chunks: usize,
}

impl SignatureReport {
    pub(crate) fn new(
        admitted_chunks: usize,
        applied_changes: Vec<ChangeHash>,
        pending_chunks: usize,
    ) -> Self {
        Self {
            admitted_chunks,
            applied_changes,
            pending_chunks,
        }
    }

    /// How many whole chunks crossed the authentication boundary.
    pub fn admitted_chunks(&self) -> usize {
        self.admitted_chunks
    }

    /// The changes those chunks contributed, in the order they were applied.
    pub fn applied_changes(&self) -> &[ChangeHash] {
        &self.applied_changes
    }

    /// How many chunks are still waiting for signatures.
    pub fn pending_chunks(&self) -> usize {
        self.pending_chunks
    }
}

/// What an authenticated load requires of a whole-document chunk.
///
/// Every actor's greatest sequence must carry a signature from that actor's
/// author: one signature there covers that actor's whole chain, and a document
/// carries its own authors, so this is decidable while the chunk is being
/// reconstructed, before any document is built from it.
///
/// `own_author` is the exception, and is `Some` only when the application has
/// asserted that these are its own bytes. History assigned that author is
/// admitted unsigned, which is what lets an application reload commits it has
/// not signed yet. Its fragments stay deferred until it signs them, so the only
/// history a document holds without a signature remains history it can still
/// produce one for.
pub(crate) struct Admission<'a> {
    signatures: &'a SignatureStore,
    own_author: Option<&'a Author<'a>>,
}

impl<'a> Admission<'a> {
    pub(crate) fn new(signatures: &'a SignatureStore, own_author: Option<&'a Author<'a>>) -> Self {
        Self {
            signatures,
            own_author,
        }
    }

    /// Build the fragment index the reconstructed history may have.
    ///
    /// Reconstruction forms a fragment for every level it meets, which is right
    /// for a document whose history is all signed and wrong for one carrying
    /// unsigned commits of its own: forming their fragments would supersede the
    /// lower-level fragments they absorb, withdrawing history which *was*
    /// exportable. Only the signatures decide, so the unsigned history `own_author`
    /// admits is deferred here exactly as a fresh local commit would be.
    pub(crate) fn index_fragments(&self, change_graph: &mut ChangeGraph, authors: &Authors) {
        let may_form = covered_fragments(change_graph, authors, self.signatures);
        change_graph.rebuild_fragment_index(&may_form);
    }

    /// Check every actor's greatest sequence, returning the first which is
    /// neither signed by its own author nor this load's own history, as its
    /// change hash and actor index.
    pub(crate) fn check(
        &self,
        change_graph: &ChangeGraph,
        authors: &Authors,
    ) -> Result<(), (ChangeHash, usize)> {
        for (actor, hash) in change_graph.actor_maxima() {
            let author = authors.get_author_for_actor(actor);
            let signed = author
                .as_ref()
                .and_then(ed25519_key)
                .is_some_and(|key| self.signatures.is_valid(&hash, &key));
            let is_own = match (&author, self.own_author) {
                (Some(author), Some(own)) => author.as_bytes() == own.as_bytes(),
                _ => false,
            };
            if !signed && !is_own {
                return Err((hash, actor));
            }
        }
        Ok(())
    }
}

/// The greatest sequence each actor has a verifying signature for.
///
/// A signature at an actor's sequence `n` covers that actor's whole prefix, so this
/// is the frontier below which history is authenticated. It is built from the
/// signatures held rather than from the changes, because there are far fewer of
/// them.
pub(crate) fn signed_frontier(
    signatures: &SignatureStore,
    change_graph: &ChangeGraph,
    authors: &Authors,
) -> HashMap<usize, u64> {
    let mut frontier: HashMap<usize, u64> = HashMap::new();
    for hash in signatures.targets() {
        // A signature may target history this document does not hold, which is
        // allowed and relayed. Skipping it is the conservative answer: it
        // advances no actor's frontier, so it authenticates nothing.
        let Some((actor, seq)) = change_graph.actor_seq_for_hash(&hash) else {
            continue;
        };
        let verifies = authors
            .get_author_for_actor(actor)
            .as_ref()
            .and_then(ed25519_key)
            .is_some_and(|key| signatures.is_valid(&hash, &key));
        if verifies {
            let entry = frontier.entry(actor).or_default();
            *entry = (*entry).max(seq);
        }
    }
    frontier
}

/// The fragment-forming hashes which `signatures` cover, and so the fragments the
/// index may hold.
///
/// History arrives, replays or reconstructs without regard to what has been
/// signed, so the index formed along the way treats every change as
/// authenticated. Selecting with this and handing the result to
/// [`ChangeGraph::rebuild_fragment_index`] puts it back: a fragment forms only
/// for history a signature covers, and the rest is deferred until one does, which
/// is what keeps every fragment in the index one the document can export.
///
/// The selection lives here rather than on the graph because what counts as
/// covered is a signature question — signatures, authors and keys — while the index
/// it feeds is the graph's own.
pub(crate) fn covered_fragments(
    change_graph: &ChangeGraph,
    authors: &Authors,
    signatures: &SignatureStore,
) -> BTreeSet<ChangeHash> {
    let frontier = signed_frontier(signatures, change_graph, authors);
    change_graph
        .fragment_forming_hashes()
        .filter(|hash| {
            change_graph
                .actor_seq_for_hash(hash)
                .is_some_and(|(actor, seq)| frontier.get(&actor).is_some_and(|max| *max >= seq))
        })
        .collect()
}

/// The author as an Ed25519 verifying key, if it is one.
pub(crate) fn ed25519_key(author: &Author<'_>) -> Option<VerifyingKey> {
    let bytes = <&[u8; 32]>::try_from(author.as_bytes()).ok()?;
    VerifyingKey::from_bytes(bytes)
        .ok()
        .filter(|key| !key.is_weak())
}

/// Whether `signature` is an Ed25519 signature over `hash` which verifies under `key`.
fn verifies(signature: &[u8], hash: &ChangeHash, key: &VerifyingKey) -> bool {
    let Some((&ED25519_TAG, payload)) = signature.split_first() else {
        return false;
    };
    let Ok(signature) = <&[u8; 64]>::try_from(payload) else {
        return false;
    };
    key.verify_strict(hash.as_bytes(), &Ed25519Signature::from_bytes(signature))
        .is_ok()
}
