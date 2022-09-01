use tracing::instrument;

use crate::{
    change::Change,
    storage::{self, parse},
};

mod change_collector;
mod reconstruct_document;
pub(crate) use reconstruct_document::{
    reconstruct_document, DocObserver, LoadedObject, Reconstructed,
};

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
    #[error("error inflating document chunk ops: {0}")]
    InflateDocument(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("bad checksum")]
    BadChecksum,
}

pub(crate) enum LoadedChanges<'a> {
    /// All the data was succesfully loaded into a list of changes
    Complete(Vec<Change>),
    /// We only managed to load _some_ changes.
    Partial {
        /// The succesfully loaded changes
        loaded: Vec<Change>,
        /// The data which we were unable to parse
        #[allow(dead_code)]
        remaining: parse::Input<'a>,
        /// The error encountered whilst trying to parse `remaining`
        error: Error,
    },
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
pub(crate) fn load_changes<'a>(mut data: parse::Input<'a>) -> LoadedChanges<'a> {
    let mut changes = Vec::new();
    while !data.is_empty() {
        let remaining = match load_next_change(data, &mut changes) {
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

fn load_next_change<'a>(
    data: parse::Input<'a>,
    changes: &mut Vec<Change>,
) -> Result<parse::Input<'a>, Error> {
    let (remaining, chunk) = storage::Chunk::parse(data).map_err(|e| Error::Parse(Box::new(e)))?;
    if !chunk.checksum_valid() {
        return Err(Error::BadChecksum);
    }
    match chunk {
        storage::Chunk::Document(d) => {
            tracing::trace!("loading document chunk");
            let Reconstructed {
                changes: new_changes,
                ..
            } = reconstruct_document(&d, NullObserver)
                .map_err(|e| Error::InflateDocument(Box::new(e)))?;
            changes.extend(new_changes);
        }
        storage::Chunk::Change(change) => {
            tracing::trace!("loading change chunk");
            let change = Change::new_from_unverified(change.into_owned(), None)
                .map_err(|e| Error::InvalidChangeColumns(Box::new(e)))?;
            #[cfg(debug_assertions)]
            {
                let loaded_ops = change.iter_ops().collect::<Vec<_>>();
                tracing::trace!(actor=?change.actor_id(), num_ops=change.len(), ops=?loaded_ops, "loaded change");
            }
            #[cfg(not(debug_assertions))]
            tracing::trace!(actor=?change.actor_id(), num_ops=change.len(), "loaded change");
            changes.push(change);
        }
        storage::Chunk::CompressedChange(change, compressed) => {
            tracing::trace!("loading compressed change chunk");
            let change =
                Change::new_from_unverified(change.into_owned(), Some(compressed.into_owned()))
                    .map_err(|e| Error::InvalidChangeColumns(Box::new(e)))?;
            changes.push(change);
        }
    };
    Ok(remaining)
}

struct NullObserver;
impl DocObserver for NullObserver {
    type Output = ();
    fn finish(self, _metadata: crate::op_tree::OpSetMetadata) -> Self::Output {}
    fn object_loaded(&mut self, _object: LoadedObject) {}
}
