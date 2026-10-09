//! The next document API.
//!
//! [`next::Automerge`](Automerge) and [`next::AutoCommit`](AutoCommit) run on the
//! same engine as the default document types, but do not keep every change
//! hash in the document's history. History is addressed by
//! [`ChangeId`](crate::ChangeId) (an actor and sequence number) instead of by
//! hash, operations that may touch history return `Result`s, and documents
//! save in the compact change set format by default.
//!
//! The default types in the crate root keep every hash and the API they
//! have always had. Convert between the two with `into_next` and
//! `into_audit`.

pub use crate::autocommit::AutoCommit;
pub use crate::automerge::{
    Automerge, EnableAuditModeError, GcMode, LoadOptions, SaveFormat, SaveOptions,
};
pub use crate::read::ReadDoc;

#[doc(hidden)]
pub use crate::change_graph::Fragment;

/// Testing hook; use `into_audit` / `into_next` instead.
#[doc(hidden)]
pub use crate::automerge::AuditMode;

/// The [`Automerge`] that `enable_audit_mode` returns, which keeps every
/// change hash.
#[doc(hidden)]
pub type AuditedAutomerge = crate::automerge::Automerge<crate::hash_retention::Full>;

/// The [`AutoCommit`] that `enable_audit_mode` returns.
#[doc(hidden)]
pub type AuditedAutoCommit = crate::autocommit::AutoCommit<crate::hash_retention::Full>;

/// Transactions on [`next`](crate::next) documents.
pub mod transaction {
    pub use crate::tx::{
        BlockOrText, CommitOptions, Failure, OwnedTransaction, Result, Success, Transactable,
        Transaction,
    };
}
