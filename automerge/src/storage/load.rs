use tracing::instrument;

use crate::{storage, Change};

mod change_collector;
mod reconstruct_document;
pub(crate) use reconstruct_document::{
    reconstruct_document, DocObserver, LoadedObject, Reconstructed,
};

#[derive(Debug, thiserror::Error)]
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

/// Load all the chunks in `data` returning a vector of changes. Note that this will throw an error
/// if there is data left over.
#[instrument(skip(data), err)]
pub fn load_changes(data: &[u8]) -> Result<Vec<Change>, Error> {
    let mut changes = Vec::new();
    let mut data = data;
    while !data.is_empty() {
        let (remaining, chunk) =
            storage::Chunk::parse(data).map_err(|e| Error::Parse(Box::new(e)))?;
        if !chunk.checksum_valid() {
            return Err(Error::BadChecksum);
        }
        match chunk {
            storage::Chunk::Document(d) => {
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
                changes.push(change);
            }
            storage::Chunk::CompressedChange(change, compressed) => {
                tracing::trace!("loading compressed change chunk");
                let change =
                    Change::new_from_unverified(change.into_owned(), Some(compressed.into_owned()))
                        .map_err(|e| Error::InvalidChangeColumns(Box::new(e)))?;
                changes.push(change)
            }
        }
        data = remaining;
    }
    Ok(changes)
}

struct NullObserver;
impl DocObserver for NullObserver {
    type Output = ();
    fn finish(self, _metadata: crate::op_tree::OpSetMetadata) -> Self::Output {}
    fn object_loaded(&mut self, _object: LoadedObject) {}
}
