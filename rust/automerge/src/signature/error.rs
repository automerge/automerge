//! Structural and admission errors for change signatures and signed fragments.

use crate::ChangeHash;

use super::ed25519;

/// A signature length exceeding the maximum of 1024 bytes.
#[derive(Debug, thiserror::Error)]
pub enum SignatureBytes {
    #[error("the signature size, '{actual}' bytes, exceeds the maximum")]
    ExceededSize {
        /// Length of the rejected signature bytes.
        actual: u64,
    },
}

/// A malformed domain-separated signing message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SigningMessage {
    #[error("signing message does not begin with the automerge-change-signature-v1 domain prefix")]
    InvalidPrefix,
    #[error("signing message hash suffix must be 32 bytes, found {actual}")]
    InvalidHashLength { actual: usize },
}

/// Structural canonicality or actor-chain failure for an incoming signed group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InvalidFragment {
    #[error("incoming group has no changes")]
    Empty,
    #[error("incoming group contains duplicate member {0}")]
    DuplicateMember(ChangeHash),
    #[error("incoming group does not have a single reachable head")]
    NotSingleHeaded,
    #[error("level-zero head {0} cannot have additional fragment members")]
    NonSingletonLoose(ChangeHash),
    #[error("member {member} does not belong below head {head} at its fragment level")]
    InvalidMemberLevel {
        head: ChangeHash,
        member: ChangeHash,
    },
    #[error("actor sequence chain is discontinuous or missing predecessor dependency at {0}")]
    BrokenActorChain(ChangeHash),
    #[error("incoming group for head {0} does not match canonical fragment membership")]
    NonCanonicalMembership(ChangeHash),
}

/// Failure to verify an incoming group's attached proof against established authors.
#[derive(Debug, thiserror::Error)]
pub enum ProofError {
    #[error("attached proof has no valid signature for required change {0}")]
    MissingSignature(ChangeHash),
    #[error("required change {0} has no established author")]
    MissingAuthor(ChangeHash),
    #[error("author of required change {hash} cannot supply an Ed25519 key: {source}")]
    InvalidAuthorKey {
        hash: ChangeHash,
        #[source]
        source: ed25519::error::VerifyingAuthor,
    },
}

/// Failure to import or apply history in required signing mode.
#[derive(Debug, thiserror::Error)]
pub enum SigningError {
    #[error("unauthenticated changes cannot be imported in signing mode")]
    UnauthenticatedChanges,
    #[error("fork_at is not supported in signing mode")]
    ForkAtNotSupported,
    #[error("merge requires both documents to be in the same signing mode")]
    MergeModeMismatch,
    #[error("the sync protocol is not supported in signing mode; transfer published history with save/load or merge")]
    SyncNotSupported,
    #[error("rescue cannot evaluate signed data; load it in signing mode instead")]
    RescueNotSupported,
    #[error("local changes are still unpublished; publish or keep the current author before forking with a different author")]
    UnpublishedLocalChanges,
    /// The requested local actor already has history recorded under another
    /// author, so the fixed local author cannot continue its chain.
    #[error(
        "actor {actor} belongs to a different author than this signing document's fixed author"
    )]
    ActorBoundToOtherAuthor { actor: crate::ActorId },
    #[error("incoming group is not a canonical fragment: {0}")]
    InvalidFragment(#[from] InvalidFragment),
    #[error("incoming group has an invalid or incomplete proof: {0}")]
    InvalidProof(#[from] ProofError),
}
