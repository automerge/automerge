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
use crate::{ChangeHash, ChangeId, Viewpoint};

/// Every boundary as a hash, including hashes this document has not received
/// yet.
pub(crate) fn frontier_hashes(
    doc: &crate::automerge::Automerge<Full>,
) -> std::collections::HashMap<crate::Author<'static>, Vec<ChangeHash>> {
    use crate::write_frontier::Boundary;
    doc.write_frontier()
        .iter()
        .map(|(author, heads)| {
            let hashes = heads
                .iter()
                .filter_map(|boundary| match boundary {
                    Boundary::Hash(hash) => Some(*hash),
                    Boundary::Id(id) => doc.change_id_to_hash(id).ok().flatten(),
                })
                .collect();
            (author.clone(), hashes)
        })
        .collect()
}

/// Drops unknown hashes: audited reads use the subset of `heads` the document knows.
mod sealed {
    pub trait Sealed {}
}

/// Heads named by hash, read under the document's current write-frontier, or
/// a [`Viewpoint`], read under its own: either side of an audited `diff`.
/// Hashes the document does not have are ignored.
pub trait HeadsOrViewpoint: sealed::Sealed {
    #[doc(hidden)]
    fn into_viewpoint(self, doc: &crate::automerge::Automerge<Full>) -> Viewpoint;
}

impl sealed::Sealed for &[ChangeHash] {}
impl HeadsOrViewpoint for &[ChangeHash] {
    fn into_viewpoint(self, doc: &crate::automerge::Automerge<Full>) -> Viewpoint {
        Viewpoint::at_heads(known_ids(doc, self))
    }
}

impl sealed::Sealed for &Vec<ChangeHash> {}
impl HeadsOrViewpoint for &Vec<ChangeHash> {
    fn into_viewpoint(self, doc: &crate::automerge::Automerge<Full>) -> Viewpoint {
        self.as_slice().into_viewpoint(doc)
    }
}

impl<const N: usize> sealed::Sealed for &[ChangeHash; N] {}
impl<const N: usize> HeadsOrViewpoint for &[ChangeHash; N] {
    fn into_viewpoint(self, doc: &crate::automerge::Automerge<Full>) -> Viewpoint {
        self.as_slice().into_viewpoint(doc)
    }
}

impl sealed::Sealed for Viewpoint {}
impl HeadsOrViewpoint for Viewpoint {
    fn into_viewpoint(self, _doc: &crate::automerge::Automerge<Full>) -> Viewpoint {
        self
    }
}

impl sealed::Sealed for &Viewpoint {}
impl HeadsOrViewpoint for &Viewpoint {
    fn into_viewpoint(self, _doc: &crate::automerge::Automerge<Full>) -> Viewpoint {
        self.clone()
    }
}

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
