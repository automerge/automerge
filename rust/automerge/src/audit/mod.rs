//! The audited document API re-exported from the crate root: every change
//! hash is kept and history is named by hash.

mod autocommit;
mod automerge;
mod read;
mod transactable;
pub(crate) mod transaction;
mod wrapper;

pub use self::autocommit::AutoCommit;
pub use self::automerge::{Automerge, Fragment, LoadOptions, SaveOptions};
pub use read::ReadDoc;
pub use transactable::Transactable;
pub(crate) use wrapper::AuditWrapper;

use crate::hash_retention::Full;
use crate::{ChangeHash, ChangeId};

/// Drops unknown hashes: audited reads use the subset of `heads` the document knows.
pub(crate) fn known_ids(
    doc: &crate::automerge::Automerge<Full>,
    heads: &[ChangeHash],
) -> Vec<ChangeId> {
    heads
        .iter()
        .filter_map(|h| doc.hash_to_change_id(h).ok().flatten())
        .collect()
}

pub(crate) fn strict_ids(
    doc: &crate::automerge::Automerge<Full>,
    heads: &[ChangeHash],
) -> ::std::result::Result<Vec<ChangeId>, crate::AutomergeError> {
    heads
        .iter()
        .map(|h| {
            doc.hash_to_change_id(h)
                .ok()
                .flatten()
                .ok_or(crate::AutomergeError::InvalidHash(*h))
        })
        .collect()
}

pub(crate) fn hashes_of(
    doc: &crate::automerge::Automerge<Full>,
    ids: &[ChangeId],
) -> Vec<ChangeHash> {
    ids.iter()
        .map(|id| doc.hash_of_id(id).expect("ids come from the same document"))
        .collect()
}
