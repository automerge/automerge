use tracing::instrument;

use crate::{
    change::Change,
    change_graph::ChangeGraph,
    signatures::Signature,
    storage::{self, document::ReconstructError, parse, Bundle},
    types::TextEncoding,
};

pub(crate) mod change_collector;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum VerificationMode {
    Check,
    DontCheck,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MarkOrderValidation {
    Validate,
    AllowInvalid,
}

impl MarkOrderValidation {
    pub(crate) fn allows_invalid(self) -> bool {
        matches!(self, Self::AllowInvalid)
    }
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
    #[error("a bundle contained an invalid column")]
    InvalidBundleColumn(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("a bundle contained an invalid change")]
    InvalidBundleChange(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("error inflating document chunk ops: {0}")]
    InflateDocument(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("bad checksum")]
    BadChecksum,
}

/// A chunk stream, read as far as it could be read.
pub(crate) struct ChunkStream {
    /// Every change the stream carried, in stream order.
    pub(crate) changes: Vec<Change>,
    /// Every detached signature the stream carried.
    pub(crate) signatures: Vec<Signature>,
    /// Why reading stopped, when it stopped before the end.
    ///
    /// A document is concatenated chunks, so corrupt or truncated data still
    /// leaves a prefix of whole chunks which is perfectly good. Each caller
    /// applies its own policy to this: an incremental load keeps the prefix and
    /// warns, while `load` consults [`OnPartialLoad`](crate::OnPartialLoad).
    pub(crate) error: Option<Error>,
}

/// Read `data` as a stream of chunks.
///
/// History chunks contribute their changes and signature chunks their signatures.
/// A document chunk whose heads are all present is skipped: re-deriving history
/// the document already holds is pure cost.
#[instrument(skip(data, current))]
pub(crate) fn read_chunk_stream(
    data: &[u8],
    text_encoding: TextEncoding,
    current: &ChangeGraph,
    mark_order: MarkOrderValidation,
) -> ChunkStream {
    let mut stream = ChunkStream {
        changes: Vec::new(),
        signatures: Vec::new(),
        error: None,
    };
    let mut input = parse::Input::new(data);
    while !input.is_empty() {
        match read_chunk(input, text_encoding, current, mark_order) {
            Ok((remaining, read)) => {
                match read {
                    ReadChunk::History(changes) => stream.changes.extend(changes),
                    ReadChunk::AlreadyHeld => {}
                    ReadChunk::Signatures(signatures) => stream.signatures.extend(signatures),
                }
                input = remaining.reset();
            }
            Err(e) => {
                stream.error = Some(e);
                break;
            }
        }
    }
    stream
}

/// What one chunk of a stream turned out to carry.
enum ReadChunk {
    History(Vec<Change>),
    Signatures(Vec<Signature>),
    /// A document chunk whose history the reading document already holds.
    AlreadyHeld,
}

fn read_chunk<'a>(
    data: parse::Input<'a>,
    text_encoding: TextEncoding,
    current: &ChangeGraph,
    mark_order: MarkOrderValidation,
) -> Result<(parse::Input<'a>, ReadChunk), Error> {
    let (remaining, chunk) = storage::Chunk::parse(data).map_err(|e| Error::Parse(Box::new(e)))?;
    if !chunk.checksum_valid() {
        return Err(Error::BadChecksum);
    }
    let read = match chunk {
        storage::Chunk::Document(d) => {
            tracing::trace!("reading document chunk");
            if d.heads().iter().all(|h| current.has_change(h)) {
                ReadChunk::AlreadyHeld
            } else {
                match d.reconstruct_changes(text_encoding) {
                    Ok(changes) => ReadChunk::History(changes),
                    Err(ReconstructError::InvalidMarkOrderChanges {
                        changes,
                        error_message: _,
                    }) if mark_order.allows_invalid() => ReadChunk::History(changes),
                    Err(e) => return Err(Error::InflateDocument(Box::new(e))),
                }
            }
        }
        storage::Chunk::Change(change) => {
            let change = Change::new_from_unverified(change.into_owned(), None)
                .map_err(|e| Error::InvalidChangeColumns(Box::new(e)))?;
            tracing::trace!(actor=?change.actor_id(), num_ops=change.len(), "read change");
            ReadChunk::History(vec![change])
        }
        storage::Chunk::CompressedChange(change, compressed) => {
            let change =
                Change::new_from_unverified(change.into_owned(), Some(compressed.into_owned()))
                    .map_err(|e| Error::InvalidChangeColumns(Box::new(e)))?;
            tracing::trace!(actor=?change.actor_id(), num_ops=change.len(), "read compressed change");
            ReadChunk::History(vec![change])
        }
        storage::Chunk::Bundle(bundle) => {
            tracing::trace!("reading bundle chunk");
            let bundle = Bundle::new_from_unverified(bundle.into_owned())
                .map_err(|e| Error::InvalidBundleColumn(Box::new(e)))?;
            ReadChunk::History(
                bundle
                    .to_changes()
                    .map_err(|e| Error::InvalidBundleChange(Box::new(e)))?,
            )
        }
        storage::Chunk::Signatures(chunk) => {
            tracing::trace!("reading detached signature chunk");
            ReadChunk::Signatures(chunk.into_signatures())
        }
    };
    Ok((remaining, read))
}
