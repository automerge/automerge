use std::collections::HashMap;

use tracing::instrument;

use super::{rowblock, storage};
use crate::{op_set::OpSet, Change};

mod change_collector;
mod loading_document;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unable to parse chunk: {0}")]
    Parse(Box<dyn std::error::Error>),
    #[error("invalid change columns: {0}")]
    InvalidChangeColumns(Box<dyn std::error::Error>),
    #[error("invalid ops columns: {0}")]
    InvalidOpsColumns(Box<dyn std::error::Error>),
    #[error("a chunk contained leftover data")]
    LeftoverData,
    #[error("error inflating document chunk ops: {0}")]
    InflateDocument(Box<dyn std::error::Error>),
    #[error("bad checksum")]
    BadChecksum,
}

/// The result of `load_opset`. See the documentation for [`load_opset`] for details on why this is
/// necessary
pub(crate) enum LoadOpset {
    /// The data was a "document" chunk so we loaded an op_set
    Document {
        /// The opset we loaded
        op_set: OpSet,
        /// The changes
        history: Vec<Change>,
        /// An index from history index to hash
        history_index: HashMap<crate::types::ChangeHash, usize>,
        /// An index from actor index to seq to change index
        actor_to_history: HashMap<usize, Vec<usize>>,
    },
    /// The data was a change chunk so we just loaded the change
    Change(Change),
}

/// The binary storage format defines several different "chunk types". When we're loading a
/// document for the first time we wish to distinguish between "document" chunk types, and all the
/// others. The reason for this is that the "document" chunk type contains operations encoded in a
/// particular order which we can take advantage of to quickly load an OpSet. For all other chunk
/// types we must proceed as usual by loading changes in order.
///
/// The tuple returned by this function contains as it's first component any data which was not
/// consumed (i.e. data which could be more chunks) and as it's second component the [`LoadOpset`]
/// which represents the two possible alternatives described above.
#[instrument(level = "trace", skip(data))]
pub(crate) fn load_opset<'a>(data: &'a [u8]) -> Result<(&'a [u8], LoadOpset), Error> {
    let (remaining, chunk) = storage::Chunk::parse(data).map_err(|e| Error::Parse(Box::new(e)))?;
    if !chunk.checksum_valid() {
        return Err(Error::BadChecksum);
    }
    match chunk.typ() {
        storage::ChunkType::Document => {
            tracing::trace!("loading document chunk");
            let data = chunk.data();
            let (inner_remaining, doc) =
                storage::Document::parse(&data).map_err(|e| Error::Parse(Box::new(e)))?;
            if !inner_remaining.is_empty() {
                tracing::error!(
                    remaining = inner_remaining.len(),
                    "leftover data when parsing document chunk"
                );
                return Err(Error::LeftoverData);
            }
            let change_rowblock =
                rowblock::RowBlock::new(doc.change_metadata.iter(), doc.change_bytes)
                    .map_err(|e| Error::InvalidChangeColumns(Box::new(e)))?
                    .into_doc_change()
                    .map_err(|e| Error::InvalidChangeColumns(Box::new(e)))?;

            let ops_rowblock = rowblock::RowBlock::new(doc.op_metadata.iter(), doc.op_bytes)
                .map_err(|e| Error::InvalidOpsColumns(Box::new(e)))?
                .into_doc_ops()
                .map_err(|e| Error::InvalidOpsColumns(Box::new(e)))?;

            let loading_document::Loaded {
                op_set,
                history,
                history_index,
                actor_to_history,
                ..
            } = loading_document::load(
                doc.actors,
                doc.heads.into_iter().collect(),
                change_rowblock.into_iter(),
                ops_rowblock.into_iter(),
            )
            .map_err(|e| Error::InflateDocument(Box::new(e)))?;

            // TODO: remove this unwrap because we already materialized all the ops
            let history = history.into_iter().map(|h| h.try_into().unwrap()).collect();

            Ok((
                remaining,
                LoadOpset::Document {
                    op_set,
                    history,
                    history_index,
                    actor_to_history,
                },
            ))
        }
        storage::ChunkType::Change => {
            tracing::trace!("loading change chunk");
            let data = chunk.data();
            let (inner_remaining, change_chunk) =
                storage::Change::parse(&data).map_err(|e| Error::Parse(Box::new(e)))?;
            if !inner_remaining.is_empty() {
                tracing::error!(
                    remaining = inner_remaining.len(),
                    "leftover data when parsing document chunk"
                );
                return Err(Error::LeftoverData);
            }
            let change_rowblock =
                rowblock::RowBlock::new(change_chunk.ops_meta.iter(), change_chunk.ops_data.clone())
                    .map_err(|e| Error::InvalidOpsColumns(Box::new(e)))?
                    .into_change_ops()
                    .map_err(|e| Error::InvalidOpsColumns(Box::new(e)))?;
            let len = (&change_rowblock).into_iter().try_fold(0, |acc, c| {
                c.map_err(|e| Error::InvalidChangeColumns(Box::new(e)))?;
                Ok(acc + 1)
            })?;
            Ok((
                remaining,
                LoadOpset::Change(Change::new(change_chunk.into_owned(), chunk.hash(), len)),
            ))
        }
        storage::ChunkType::Compressed => panic!(),
    }
}

/// Load all the chunks in `data` returning a vector of changes. Note that this will throw an error
/// if there is data left over.
pub(crate) fn load(data: &[u8]) -> Result<Vec<Change>, Error> {
    let mut changes = Vec::new();
    let mut data = data;
    while data.len() > 0 {
        let (remaining, load_result) = load_opset(data)?;
        match load_result {
            LoadOpset::Change(c) => changes.push(c),
            LoadOpset::Document { history, .. } => {
                for stored_change in history {
                    changes.push(
                        Change::try_from(stored_change)
                            .map_err(|e| Error::InvalidOpsColumns(Box::new(e)))?,
                    );
                }
            }
        }
        data = remaining;
    }
    Ok(changes)
}
