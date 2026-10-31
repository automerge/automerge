use std::cmp::Reverse;

use tracing::instrument;

use crate::{
    change::Change,
    change_graph::ChangeGraph,
    storage::{self, document::ReconstructError, parse, Bundle, Document},
    types::TextEncoding,
};

pub(crate) mod change_collector;

/// Whether loading checks that a document is well formed: that its stored
/// head hashes match its changes and its marks are in order.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum VerificationMode {
    Check,
    /// Load a corrupt document as far as possible: mismatched heads are
    /// accepted, and a change chunk with misordered marks ends the load,
    /// keeping the changes before it.
    DontCheck,
}

#[derive(Debug, thiserror::Error)]
#[allow(unreachable_pub)]
pub enum Error {
    #[error("unable to parse chunk: {0}")]
    Parse(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("invalid change columns: {0}")]
    InvalidChangeColumns(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("invalid ops columns: {0}")]
    InvalidOpsColumns(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("a chunk contained leftover data")]
    LeftoverData,
    #[error("invalid BundleV0: {0}")]
    InvalidBundleV0(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("error inflating document chunk ops: {0}")]
    InflateDocument(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("bad checksum")]
    BadChecksum,
}

/// Bundles are kept whole: expanding them into changes needs dep hashes
/// a hashless document may not have.
pub(crate) enum LoadedChunk {
    Change(Box<Change>),
    Bundle(Box<crate::storage::Bundle>),
}

pub(crate) enum LoadedChanges<'a> {
    /// All the data was succesfully loaded into a list of changes
    Complete(Vec<LoadedChunk>),
    /// We only managed to load _some_ changes.
    Partial {
        /// The succesfully loaded changes
        loaded: Vec<LoadedChunk>,
        /// The data which we were unable to parse
        #[allow(dead_code)]
        remaining: parse::Input<'a>,
        /// The error encountered whilst trying to parse `remaining`
        error: Error,
    },
}

/// Every chunk of a load, grouped so that the order they arrived in does
/// not matter.
#[derive(Debug)]
pub struct LoadData<'a> {
    /// The largest document, which reconstructs whole.
    pub(crate) doc: Option<Document<'a>>,
    /// Applied as changes unless `doc` already holds them, which is only
    /// known once it has reconstructed.
    pub(crate) other_docs: Vec<Document<'a>>,
    pub(crate) changes: Vec<Change>,
    /// Boxed: moving them by value costs a per-change load 7%.
    #[allow(clippy::vec_box)]
    pub(crate) bundles: Vec<Box<Bundle>>,
}

impl<'a> LoadData<'a> {
    /// Parses every chunk in `data`. With `allow_partial`, a chunk that
    /// fails ends the parse, keeping the chunks before it, unless it is
    /// the first.
    pub(crate) fn parse(data: &'a [u8], allow_partial: bool) -> Result<Self, Error> {
        let mut data = parse::Input::new(data);
        let mut docs = vec![];
        let mut changes = vec![];
        let mut bundles = vec![];
        let mut first = true;
        while !data.is_empty() {
            match parse_chunk(data, &mut docs, &mut changes, &mut bundles) {
                Ok(rest) => data = rest.reset(),
                Err(e) if first || !allow_partial => return Err(e),
                Err(e) => {
                    tracing::warn!(err=?e, "partial load");
                    break;
                }
            }
            first = false;
        }
        docs.sort_by_key(|d: &Document<'_>| Reverse(d.byte_len()));
        let mut docs = docs.into_iter();
        Ok(LoadData {
            doc: docs.next(),
            other_docs: docs.collect(),
            changes,
            bundles,
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.doc.is_none() && self.changes.is_empty() && self.bundles.is_empty()
    }
}

#[allow(clippy::vec_box)]
fn parse_chunk<'a>(
    data: parse::Input<'a>,
    docs: &mut Vec<Document<'a>>,
    changes: &mut Vec<Change>,
    bundles: &mut Vec<Box<Bundle>>,
) -> Result<parse::Input<'a>, Error> {
    let (remaining, chunk) = storage::Chunk::parse(data).map_err(|e| Error::Parse(Box::new(e)))?;
    if !chunk.checksum_valid() {
        return Err(Error::BadChecksum);
    }
    match chunk {
        storage::Chunk::Document(d) => docs.push(d),
        storage::Chunk::Change(change) => changes.push(
            Change::new_from_unverified(change.into_owned(), None)
                .map_err(|e| Error::InvalidChangeColumns(Box::new(e)))?,
        ),
        storage::Chunk::CompressedChange(change, compressed) => changes.push(
            Change::new_from_unverified(change.into_owned(), Some(compressed.into_owned()))
                .map_err(|e| Error::InvalidChangeColumns(Box::new(e)))?,
        ),
        storage::Chunk::BundleV0(bundle) => changes.extend(
            bundle
                .to_changes()
                .map_err(|e| Error::InvalidBundleV0(Box::new(e)))?,
        ),
        storage::Chunk::Bundle(bundle, _) => bundles.push(bundle),
    }
    Ok(remaining)
}

/// Attempt to Load all the chunks in `data`.
///
/// # Partial Loads
///
/// Automerge documents are encoded as one or more concatenated chunks. Each chunk containing one
/// or more changes. This means it is possible to partially load corrupted data if the first `n`
/// chunks are valid. This function returns a `LoadedChanges` which you can examine to determine if
/// this is the case.
#[instrument(skip(data))]
pub(crate) fn load_changes<'a, H: crate::hash_retention::HashRetention>(
    mut data: parse::Input<'a>,
    text_encoding: TextEncoding,
    current: &ChangeGraph<H>,
    verification: VerificationMode,
) -> LoadedChanges<'a> {
    let mut changes: Vec<LoadedChunk> = Vec::new();
    while !data.is_empty() {
        let remaining =
            match load_next_change(data, &mut changes, text_encoding, current, verification) {
                Ok(d) => d,
                Err(e) => {
                    return LoadedChanges::Partial {
                        loaded: changes,
                        remaining: data,
                        error: e,
                    };
                }
            };
        data = remaining.reset();
    }
    LoadedChanges::Complete(changes)
}

fn load_next_change<'a, H: crate::hash_retention::HashRetention>(
    data: parse::Input<'a>,
    changes: &mut Vec<LoadedChunk>,
    text_encoding: TextEncoding,
    current: &ChangeGraph<H>,
    verification: VerificationMode,
) -> Result<parse::Input<'a>, Error> {
    let (remaining, chunk) = storage::Chunk::parse(data).map_err(|e| Error::Parse(Box::new(e)))?;
    if !chunk.checksum_valid() {
        return Err(Error::BadChecksum);
    }
    match chunk {
        storage::Chunk::Document(d) => {
            tracing::trace!("loading document chunk");
            // an unknown answer reconstructs; the apply path deduplicates
            if !d
                .heads()
                .iter()
                .all(|h| current.has_change(h).unwrap_or(false))
            {
                let new_changes = match d.reconstruct_changes(text_encoding) {
                    Ok(c) => c,
                    Err(ReconstructError::InvalidMarkOrderChanges {
                        changes,
                        error_message: _,
                    }) if verification == VerificationMode::DontCheck => changes,
                    Err(e) => return Err(Error::InflateDocument(Box::new(e))),
                };
                changes.extend(
                    new_changes
                        .into_iter()
                        .map(|c| LoadedChunk::Change(Box::new(c))),
                );
            }
        }
        storage::Chunk::Change(change) => {
            tracing::trace!("loading change chunk");
            let change = Change::new_from_unverified(change.into_owned(), None)
                .map_err(|e| Error::InvalidChangeColumns(Box::new(e)))?;
            #[cfg(feature = "slow_path_assertions")]
            {
                let loaded_ops = change.iter_ops().collect::<Vec<_>>();
                tracing::trace!(actor=?change.actor_id(), num_ops=change.len(), ops=?loaded_ops, "loaded change");
            }
            #[cfg(not(feature = "slow_path_assertions"))]
            tracing::trace!(actor=?change.actor_id(), num_ops=change.len(), "loaded change");
            changes.push(LoadedChunk::Change(Box::new(change)));
        }
        storage::Chunk::BundleV0(bundle) => {
            tracing::trace!("loading a BundleV0 chunk");
            let bundle_changes = bundle
                .to_changes()
                .map_err(|e| Error::InvalidBundleV0(Box::new(e)))?;
            changes.extend(
                bundle_changes
                    .into_iter()
                    .map(|c| LoadedChunk::Change(Box::new(c))),
            );
        }
        storage::Chunk::Bundle(bundle, _) => {
            tracing::trace!("loading bundle columns chunk");
            changes.push(LoadedChunk::Bundle(bundle));
        }
        storage::Chunk::CompressedChange(change, compressed) => {
            tracing::trace!("loading compressed change chunk");
            let change =
                Change::new_from_unverified(change.into_owned(), Some(compressed.into_owned()))
                    .map_err(|e| Error::InvalidChangeColumns(Box::new(e)))?;
            changes.push(LoadedChunk::Change(Box::new(change)));
        }
    };
    Ok(remaining)
}
