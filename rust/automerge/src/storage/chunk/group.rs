//! Read content-bearing chunks together with their trailing signatures.
//!
//! A chunk group consists of one document, change, compressed change, or
//! bundle chunk, optionally followed by a signatures chunk.
//!
//! Groups are yielded only after their framing, checksums, and signature
//! payload structure have been validated. This does not verify signatures
//! cryptographically or establish that they attest to the group's contents.
//! An unclassified tail is reported separately from the preceding group.
//! Yielding a group does not establish that no trailing signatures were lost.

use std::ops::ControlFlow;

use crate::change::Change;
use crate::change_graph::ChangeGraph;
use crate::storage::chunk;
use crate::storage::document::ReconstructError;
use crate::storage::load::{Error as LoadError, LoadedChanges, MarkOrderValidation};
use crate::storage::parse;
use crate::storage::parse::Input;
use crate::storage::signatures::SignaturesChunk;
use crate::storage::{Bundle, Chunk, ChunkType, Document, Header};
use crate::types::TextEncoding;

type GroupResult<'a> = Result<Option<ReadChunkGroup<'a>>, Error<'a>>;
type Transition<'a> = ControlFlow<Output<'a>, AssemblyState<'a>>;

/// The content-bearing chunk of a chunk group.
///
/// Contains a change, compressed change, bundle, or document. Documents
/// remain in their storage representation so initial loading can reconstruct
/// a snapshot directly, while subsequent loading can extract its changes.
pub(crate) enum ChunkBody<'a> {
    Change(Change),
    CompressedChange(Change),
    Bundle(Bundle),
    Document(Document<'a>),
}

enum BodyOrSignatures<'a> {
    Body(Box<ChunkBody<'a>>),
    Signatures(SignaturesChunk<'a>),
}

impl<'a> BodyOrSignatures<'a> {
    fn from_chunk(chunk: Chunk<'a>) -> Result<Self, LoadError> {
        match chunk {
            Chunk::Document(document) => Ok(Self::Body(Box::new(ChunkBody::Document(document)))),
            Chunk::Change(change) => Change::new_from_unverified(change.into_owned(), None)
                .map(ChunkBody::Change)
                .map(Box::new)
                .map(Self::Body)
                .map_err(|e| LoadError::InvalidChangeColumns(e.into())),
            Chunk::Bundle(bundle) => Bundle::new_from_unverified(bundle.into_owned())
                .map(ChunkBody::Bundle)
                .map(Box::new)
                .map(Self::Body)
                .map_err(|e| LoadError::InvalidBundleColumn(e.into())),
            Chunk::CompressedChange(change, compressed) => {
                Change::new_from_unverified(change, Some(compressed.into_owned()))
                    .map(ChunkBody::CompressedChange)
                    .map(Box::new)
                    .map(Self::Body)
                    .map_err(|e| LoadError::InvalidChangeColumns(e.into()))
            }
            Chunk::Signatures(signatures) => Ok(Self::Signatures(signatures)),
        }
    }
}

/// A content-bearing chunk paired with its optional trailing signatures.
///
/// Borrows from the input stream. The signatures have been structurally
/// validated, but their cryptographic validity and relationship to the
/// body's contents have not been checked.
pub(crate) struct ChunkGroup<'a> {
    // Keep the large storage representation out of events and assembly states.
    // The same allocation moves from decoding through pairing to consumption.
    pub(crate) body: Box<ChunkBody<'a>>,
    pub(crate) signatures: Option<SignaturesChunk<'a>>,
}

impl<'a> ChunkGroup<'a> {
    fn new(body: Box<ChunkBody<'a>>) -> Self {
        Self {
            body,
            signatures: None,
        }
    }

    fn with_signatures(
        mut self,
        remaining: Input<'a>,
        signatures: SignaturesChunk<'a>,
    ) -> Result<Self, Error<'a>> {
        if self.signatures.is_some() {
            Err(Error::duplicate_signatures(remaining))
        } else {
            self.signatures = Some(signatures);
            Ok(self)
        }
    }
}

impl ChunkGroup<'_> {
    /// Expand the group into changes for the existing unsigned loading path.
    fn into_changes(
        self,
        text_encoding: TextEncoding,
        current: &ChangeGraph,
        mark_order: MarkOrderValidation,
    ) -> Result<Vec<Change>, LoadError> {
        match *self.body {
            ChunkBody::Change(change) | ChunkBody::CompressedChange(change) => Ok(vec![change]),
            ChunkBody::Bundle(bundle) => bundle
                .to_changes()
                .map_err(|e| LoadError::InvalidBundleChange(e.into())),
            ChunkBody::Document(document) => {
                if document.heads().iter().all(|head| current.has_change(head)) {
                    return Ok(Vec::new());
                }

                match document.reconstruct_changes(text_encoding) {
                    Ok(changes) => Ok(changes),
                    Err(ReconstructError::InvalidMarkOrderChanges {
                        changes,
                        error_message: _,
                    }) if mark_order.allows_invalid() => Ok(changes),
                    Err(error) => Err(LoadError::InflateDocument(error.into())),
                }
            }
        }
    }
}

/// The result of reading the first chunk group for document initialization.
pub(crate) enum InitialLoad<T> {
    /// A document reconstructed from a snapshot.
    Document(T),
    /// Changes to apply to a newly created document.
    Changes(Vec<Change>),
}

/// A complete chunk group together with its starting position in the input.
///
/// Retains the group's start so that a failure while converting its body
/// into changes can report the unprocessed input, even if the reader has
/// already read ahead into the next group.
pub(crate) struct ReadChunkGroup<'a> {
    start: Input<'a>,
    // An output can carry this completed group and a pending group together.
    // Indirection keeps that transition compact without boxing lifecycle states.
    group: Box<ChunkGroup<'a>>,
}

impl<'a> ReadChunkGroup<'a> {
    /// Initialize from the first chunk group, using `from_document` to
    /// reconstruct a document snapshot.
    ///
    /// For a document body, invokes the callback and returns
    /// [`InitialLoad::Document`]. For a change, compressed change, or bundle,
    /// returns [`InitialLoad::Changes`] without invoking the callback.
    ///
    /// Consumes the group and discards its paired signatures.
    ///
    /// # Errors
    ///
    /// Returns an error if snapshot reconstruction or bundle expansion fails.
    pub(crate) fn initialize_with<T, E>(
        self,
        from_document: impl FnOnce(Document<'a>) -> Result<T, E>,
    ) -> Result<InitialLoad<T>, E>
    where
        E: From<LoadError>,
    {
        match *self.group.body {
            ChunkBody::Change(change) | ChunkBody::CompressedChange(change) => {
                Ok(InitialLoad::Changes(vec![change]))
            }
            ChunkBody::Bundle(bundle) => {
                let changes = bundle
                    .to_changes()
                    .map_err(|e| LoadError::InvalidBundleChange(e.into()))?;
                Ok(InitialLoad::Changes(changes))
            }
            ChunkBody::Document(document) => from_document(document).map(InitialLoad::Document),
        }
    }

    fn into_changes(
        self,
        text_encoding: TextEncoding,
        current: &ChangeGraph,
        mark_order: MarkOrderValidation,
    ) -> Result<Vec<Change>, LoadError> {
        self.group.into_changes(text_encoding, current, mark_order)
    }

    /// The input beginning at this group's body, for conversion-error reporting.
    fn start(&self) -> Input<'a> {
        self.start
    }

    fn from_pending(PendingChunkGroup { start, group }: PendingChunkGroup<'a>) -> Self {
        Self {
            start,
            group: Box::new(group),
        }
    }
}

/// A loading error together with the input beginning at the failure boundary.
///
/// If trailing signatures invalidate a pending group, the boundary is the
/// start of that group's body. If the failure belongs to a subsequent group
/// or invalid trailing data, the boundary is the start of that input.
/// Groups yielded before the failure remain complete.
pub(crate) struct Error<'a> {
    pub(crate) remaining: Input<'a>,
    pub(crate) error: LoadError,
}

impl<'a> Error<'a> {
    fn orphan_signatures(remaining: Input<'a>) -> Self {
        Self {
            remaining,
            error: LoadError::OrphanSignatures,
        }
    }

    fn duplicate_signatures(remaining: Input<'a>) -> Self {
        Self {
            remaining,
            error: LoadError::DuplicateSignatures,
        }
    }
}

/// Iterate over structurally validated chunk groups.
///
/// Retains a pending group until the next body, end of input, or an
/// unclassified tail establishes its boundary. A malformed subsequent body
/// or a framing failure is reported after yielding the preceding group.
/// This preserves partial-load recovery for truncated or unknown-type tails.
///
/// Once a chunk is identified as signatures, truncation, malformed contents,
/// or an invalid checksum invalidates its associated group.
///
/// If the prefix ends before the chunk type can be identified, the preceding
/// group is yielded before the incomplete-input error is reported. Yielding
/// a group does not establish that no trailing signatures were lost.
///
/// Errors are yielded once. After an error or end of input, the iterator
/// remains exhausted.
pub(crate) struct ChunkGroupReader<'a> {
    chunks: ChunkReader<'a>,
    state: ReaderState<'a>,
}

impl<'a> ChunkGroupReader<'a> {
    pub(crate) fn new(input: Input<'a>) -> Self {
        Self {
            chunks: ChunkReader { remaining: input },
            state: ReaderState::Active(AssemblyState::Empty),
        }
    }

    /// Consume the remaining chunk groups, expanding their bodies into changes.
    ///
    /// Returns all collected changes on success. On failure, returns changes
    /// from earlier successfully converted groups, the error, and the input
    /// beginning at the failure boundary.
    ///
    /// If body conversion fails, the failure boundary is the group's start,
    /// even if the reader has already read ahead into a subsequent group.
    ///
    /// Discards paired signatures without cryptographic verification or retention.
    #[tracing::instrument(skip(self))]
    pub(crate) fn load_changes(
        self,
        text_encoding: TextEncoding,
        current: &ChangeGraph,
        mark_order: MarkOrderValidation,
    ) -> LoadedChanges<'a> {
        let mut changes = Vec::new();
        for result in self {
            match result {
                Ok(group) => {
                    let remaining = group.start();
                    match group.into_changes(text_encoding, current, mark_order) {
                        Ok(read) => changes.extend(read),
                        Err(error) => {
                            return LoadedChanges::Partial {
                                loaded: changes,
                                remaining,
                                error,
                            };
                        }
                    }
                }
                Err(error) => {
                    return LoadedChanges::Partial {
                        loaded: changes,
                        remaining: error.remaining,
                        error: error.error,
                    };
                }
            }
        }
        LoadedChanges::Complete(changes)
    }

    fn next_group(&mut self) -> GroupResult<'a> {
        loop {
            // Unless a transition supplies more work, EOF and errors are terminal.
            let state = std::mem::replace(&mut self.state, ReaderState::Finished);
            let assembly = match state {
                ReaderState::Active(assembly) => assembly,
                ReaderState::DeferredError(error) => return Err(error),
                ReaderState::Finished => return Ok(None),
            };

            match assembly.advance(self.chunks.next_event()) {
                ControlFlow::Continue(assembly) => {
                    self.state = ReaderState::Active(assembly);
                }
                ControlFlow::Break(Output::Group { group, next }) => {
                    self.state = match next {
                        AfterYield::Pending(pending) => {
                            ReaderState::Active(AssemblyState::Pending(pending))
                        }
                        AfterYield::DeferredError(error) => ReaderState::DeferredError(error),
                        AfterYield::Finished => ReaderState::Finished,
                    };
                    return Ok(Some(group));
                }
                ControlFlow::Break(Output::Error(error)) => return Err(error),
                ControlFlow::Break(Output::End) => return Ok(None),
            }
        }
    }
}

impl<'a> Iterator for ChunkGroupReader<'a> {
    type Item = Result<ReadChunkGroup<'a>, Error<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_group().transpose()
    }
}

impl std::iter::FusedIterator for ChunkGroupReader<'_> {}

struct PendingChunkGroup<'a> {
    start: Input<'a>,
    group: ChunkGroup<'a>,
}

/// Reader lifecycle. Only active assembly can consume a chunk event.
enum ReaderState<'a> {
    Active(AssemblyState<'a>),
    /// Report this on the next call, after emitting the preceding valid group.
    DeferredError(Error<'a>),
    Finished,
}

/// A group still being assembled, not yet safe to emit.
enum AssemblyState<'a> {
    Empty,
    Pending(PendingChunkGroup<'a>),
}

/// Work retained after returning a complete group.
enum AfterYield<'a> {
    Pending(PendingChunkGroup<'a>),
    DeferredError(Error<'a>),
    Finished,
}

/// Observable transition results. Errors and EOF always finish the reader.
enum Output<'a> {
    Group {
        group: ReadChunkGroup<'a>,
        next: AfterYield<'a>,
    },
    Error(Error<'a>),
    End,
}

impl<'a> AssemblyState<'a> {
    fn pending(start: Input<'a>, body: Box<ChunkBody<'a>>) -> Self {
        Self::Pending(PendingChunkGroup {
            start,
            group: ChunkGroup::new(body),
        })
    }

    fn advance(self, event: ChunkEvent<'a>) -> Transition<'a> {
        match (self, event) {
            (Self::Empty, ChunkEvent::Body { start, body }) => {
                ControlFlow::Continue(Self::pending(start, body))
            }
            (Self::Empty, ChunkEvent::Signatures { start, .. }) => {
                ControlFlow::Break(Output::Error(Error::orphan_signatures(start)))
            }
            (Self::Empty, ChunkEvent::End) => ControlFlow::Break(Output::End),
            (Self::Empty, ChunkEvent::BodyFailed(error))
            | (Self::Empty, ChunkEvent::MetadataFailed(error))
            | (Self::Empty, ChunkEvent::InvalidPrefix(error))
            | (Self::Empty, ChunkEvent::FramingFailed(error)) => {
                ControlFlow::Break(Output::Error(error))
            }
            (Self::Pending(pending), ChunkEvent::Body { start, body }) => {
                ControlFlow::Break(Output::Group {
                    group: ReadChunkGroup::from_pending(pending),
                    next: AfterYield::Pending(PendingChunkGroup {
                        start,
                        group: ChunkGroup::new(body),
                    }),
                })
            }
            (Self::Pending(pending), ChunkEvent::Signatures { signatures, .. }) => {
                // Keep the group pending: another metadata chunk may invalidate it.
                match pending.group.with_signatures(pending.start, signatures) {
                    Ok(group) => ControlFlow::Continue(Self::Pending(PendingChunkGroup {
                        start: pending.start,
                        group,
                    })),
                    Err(error) => ControlFlow::Break(Output::Error(error)),
                }
            }
            (Self::Pending(pending), ChunkEvent::BodyFailed(error))
            | (Self::Pending(pending), ChunkEvent::InvalidPrefix(error))
            | (Self::Pending(pending), ChunkEvent::FramingFailed(error)) => {
                ControlFlow::Break(Output::Group {
                    group: ReadChunkGroup::from_pending(pending),
                    next: AfterYield::DeferredError(error),
                })
            }
            (Self::Pending(pending), ChunkEvent::MetadataFailed(mut error)) => {
                error.remaining = pending.start;
                ControlFlow::Break(Output::Error(error))
            }
            (Self::Pending(pending), ChunkEvent::End) => ControlFlow::Break(Output::Group {
                group: ReadChunkGroup::from_pending(pending),
                next: AfterYield::Finished,
            }),
        }
    }
}

/// Results retain whether a failed chunk was identifiable as body or metadata.
/// Checksum and structural validation failures use the identified category too.
enum ChunkEvent<'a> {
    Body {
        start: Input<'a>,
        body: Box<ChunkBody<'a>>,
    },
    Signatures {
        start: Input<'a>,
        signatures: SignaturesChunk<'a>,
    },
    BodyFailed(Error<'a>),
    MetadataFailed(Error<'a>),
    /// Invalid magic cannot begin metadata belonging to the pending group.
    InvalidPrefix(Error<'a>),
    /// An incomplete or unknown-type prefix, reported after the preceding group.
    FramingFailed(Error<'a>),
    End,
}

struct ChunkReader<'a> {
    remaining: Input<'a>,
}

impl<'a> ChunkReader<'a> {
    fn next_event(&mut self) -> ChunkEvent<'a> {
        let start = self.remaining.reset();

        if start.is_empty() {
            return ChunkEvent::End;
        }

        let chunk_type = match parse::peek(Header::parse_prefix::<chunk::error::Header>)(start) {
            Ok((_, (_, chunk_type))) => chunk_type,
            Err(error) => {
                let invalid_magic = matches!(
                    &error,
                    parse::ParseError::Error(chunk::error::Header::InvalidMagicBytes)
                );
                let failure = Error {
                    remaining: start,
                    error: LoadError::Parse(error.into()),
                };
                // Preserve the diagnostic category. Neither kind identifies
                // signatures, so both leave the preceding group recoverable.
                return if invalid_magic {
                    ChunkEvent::InvalidPrefix(failure)
                } else {
                    ChunkEvent::FramingFailed(failure)
                };
            }
        };

        // The prefix identifies which group a later decoding/checksum failure
        // belongs to. Known metadata must never be treated as a new body.
        let failed = |error| {
            let error = Error {
                remaining: start,
                error,
            };
            match chunk_type {
                ChunkType::Document
                | ChunkType::Change
                | ChunkType::Compressed
                | ChunkType::Bundle => ChunkEvent::BodyFailed(error),
                ChunkType::Signatures => ChunkEvent::MetadataFailed(error),
            }
        };
        let (remaining, chunk) = match Chunk::parse(start) {
            Ok(parsed) => parsed,
            Err(error) => return failed(LoadError::Parse(error.into())),
        };
        if !chunk.checksum_valid() {
            return failed(LoadError::BadChecksum);
        }
        let content = match BodyOrSignatures::from_chunk(chunk) {
            Ok(content) => content,
            Err(error) => return failed(error),
        };

        self.remaining = remaining.reset();
        match content {
            BodyOrSignatures::Body(body) => ChunkEvent::Body { start, body },
            BodyOrSignatures::Signatures(signatures) => {
                ChunkEvent::Signatures { start, signatures }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{ChunkType, Header};
    use crate::{Automerge, ReadDoc};

    fn body() -> Box<ChunkBody<'static>> {
        let mut doc = Automerge::new();
        let hash = doc.empty_commit(Default::default());
        Box::new(ChunkBody::Change(doc.get_change_by_hash(&hash).unwrap()))
    }

    fn signature_bytes() -> Vec<u8> {
        let mut bytes = Vec::new();
        Header::new(ChunkType::Signatures, &[0]).write(&mut bytes);
        bytes.push(0);
        bytes
    }

    fn signatures(bytes: &[u8]) -> SignaturesChunk<'_> {
        let (_, Chunk::Signatures(signatures)) = Chunk::parse(Input::new(bytes)).unwrap() else {
            panic!("expected signatures");
        };
        signatures
    }

    fn continuing(transition: Transition<'_>) -> AssemblyState<'_> {
        match transition {
            ControlFlow::Continue(state) => state,
            ControlFlow::Break(_) => panic!("group is not ready to emit"),
        }
    }

    fn body_bytes() -> Vec<u8> {
        let ChunkBody::Change(change) = *body() else {
            unreachable!()
        };
        change.raw_bytes().to_vec()
    }

    fn framed(chunk_type: ChunkType, body: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        Header::new(chunk_type, body).write(&mut bytes);
        bytes.extend_from_slice(body);
        bytes
    }

    #[test]
    fn chunk_reader_advances_and_reports_original_starts() {
        let first = body_bytes();
        let sigs = signature_bytes();
        let second = body_bytes();
        let stream = [first.as_slice(), sigs.as_slice(), second.as_slice()].concat();
        let mut reader = ChunkReader {
            remaining: Input::new(&stream),
        };
        let ChunkEvent::Body { start, .. } = reader.next_event() else {
            panic!("expected body")
        };
        assert_eq!(start, Input::new(&stream));
        let ChunkEvent::Signatures { start, signatures } = reader.next_event() else {
            panic!("expected signatures")
        };
        assert_eq!(start, Input::new(&stream[first.len()..]));
        assert_eq!(signatures.bytes(), sigs.as_slice());
        let ChunkEvent::Body { start, .. } = reader.next_event() else {
            panic!("expected next body")
        };
        assert_eq!(start, Input::new(&second));
        assert!(matches!(reader.next_event(), ChunkEvent::End));
    }

    #[test]
    fn initial_changes_do_not_invoke_snapshot_callback() {
        let bytes = body_bytes();
        let Some(Ok(read)) = ChunkGroupReader::new(Input::new(&bytes)).next() else {
            panic!("expected group");
        };
        let initial = read.initialize_with::<(), LoadError>(|_| panic!("not a snapshot"));
        let Ok(InitialLoad::Changes(changes)) = initial else {
            panic!("expected pending changes");
        };
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].raw_bytes(), bytes.as_slice());
    }

    #[test]
    fn initial_snapshot_uses_callback_result_and_propagates_errors() {
        let bytes = Automerge::new().save();
        let Some(Ok(read)) = ChunkGroupReader::new(Input::new(&bytes)).next() else {
            panic!("expected snapshot group");
        };
        assert!(matches!(
            read.initialize_with::<_, LoadError>(|_| Ok(42)),
            Ok(InitialLoad::Document(42))
        ));
        let Some(Ok(read)) = ChunkGroupReader::new(Input::new(&bytes)).next() else {
            panic!("expected snapshot group");
        };
        assert!(matches!(
            read.initialize_with::<(), LoadError>(|_| Err(LoadError::BadChecksum)),
            Err(LoadError::BadChecksum)
        ));
    }

    #[test]
    fn iterator_is_fused_and_reports_deferred_error_once() {
        fn assert_fused<I: std::iter::FusedIterator>(_: &I) {}
        let body = body_bytes();
        let stream = [body.as_slice(), &[1, 2, 3, 4]].concat();
        let mut reader = ChunkGroupReader::new(Input::new(&stream));
        assert_fused(&reader);
        assert!(matches!(reader.next(), Some(Ok(_))));
        let Some(Err(error)) = reader.next() else {
            panic!("expected deferred error")
        };
        assert_eq!(error.remaining, Input::new(&stream[body.len()..]));
        assert!(reader.next().is_none());
        assert!(reader.next().is_none());
    }

    #[test]
    fn iterator_stays_exhausted_after_eof_or_immediate_error() {
        let mut empty = ChunkGroupReader::new(Input::empty());
        assert!(empty.next().is_none());
        assert!(empty.next().is_none());
        let mut invalid = ChunkGroupReader::new(Input::new(&[1, 2, 3, 4]));
        assert!(matches!(invalid.next(), Some(Err(_))));
        assert!(invalid.next().is_none());
        assert!(invalid.next().is_none());
    }

    #[test]
    fn iterator_can_be_partially_consumed_before_loading_remaining_changes() {
        let first = body_bytes();
        let second = body_bytes();
        let stream = [first.as_slice(), second.as_slice()].concat();
        let mut reader = ChunkGroupReader::new(Input::new(&stream));
        assert!(matches!(reader.next(), Some(Ok(_))));
        let actors = crate::actor::ActorTable::new();
        let graph = ChangeGraph::new(&actors);
        let crate::storage::load::LoadedChanges::Complete(changes) = reader.load_changes(
            TextEncoding::platform_default(),
            &graph,
            MarkOrderValidation::Validate,
        ) else {
            panic!("expected remaining change")
        };
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].raw_bytes(), second.as_slice());
    }

    #[test]
    fn group_reader_yields_a_body_once_then_finishes() {
        let bytes = body_bytes();
        let mut reader = ChunkGroupReader::new(Input::new(&bytes));
        assert!(matches!(reader.next_group(), Ok(Some(_))));
        assert!(matches!(reader.next_group(), Ok(None)));
        assert!(matches!(reader.next_group(), Ok(None)));
    }

    #[test]
    fn group_reader_pairs_signatures_and_retains_the_next_body() {
        let first = body_bytes();
        let second = body_bytes();
        let stream = [
            first.as_slice(),
            signature_bytes().as_slice(),
            second.as_slice(),
        ]
        .concat();
        let mut reader = ChunkGroupReader::new(Input::new(&stream));
        let Ok(Some(group)) = reader.next_group() else {
            panic!("expected signed group")
        };
        assert_eq!(group.start(), Input::new(&stream));
        assert!(group.group.signatures.is_some());
        let ChunkBody::Change(change) = *group.group.body else {
            panic!("expected change")
        };
        assert_eq!(change.raw_bytes(), first.as_slice());
        let Ok(Some(group)) = reader.next_group() else {
            panic!("expected second group")
        };
        let second_start = stream.len() - second.len();
        assert_eq!(group.start(), Input::new(&stream[second_start..]));
        assert!(group.group.signatures.is_none());
        let ChunkBody::Change(change) = *group.group.body else {
            panic!("expected change")
        };
        assert_eq!(change.raw_bytes(), second.as_slice());
        assert!(matches!(reader.next_group(), Ok(None)));
    }

    #[test]
    fn checksum_failure_preserves_the_failed_chunk_start() {
        for mut bytes in [body_bytes(), signature_bytes()] {
            bytes[4] ^= 1; // Change only the checksum, not the payload.
            let start = Input::new(&bytes);
            let mut reader = ChunkReader { remaining: start };
            let error = match reader.next_event() {
                ChunkEvent::BodyFailed(error) | ChunkEvent::MetadataFailed(error) => error,
                _ => panic!("expected classified checksum failure"),
            };
            assert_eq!(error.remaining, start);
            assert!(matches!(error.error, LoadError::BadChecksum));
        }
    }

    #[test]
    fn group_reader_defers_malformed_next_body_but_not_metadata() {
        let first = body_bytes();
        let bad_body = framed(ChunkType::Change, &[]);
        let stream = [first.as_slice(), bad_body.as_slice()].concat();
        let mut reader = ChunkGroupReader::new(Input::new(&stream));
        assert!(matches!(reader.next_group(), Ok(Some(_))));
        let Err(error) = reader.next_group() else {
            panic!("expected deferred error")
        };
        assert_eq!(error.remaining, Input::new(&stream[first.len()..]));
        assert!(matches!(error.error, LoadError::Parse(_)));
        assert!(matches!(reader.next_group(), Ok(None)));

        {
            let metadata = framed(ChunkType::Signatures, &[1]);
            let stream = [first.as_slice(), metadata.as_slice()].concat();
            let mut reader = ChunkGroupReader::new(Input::new(&stream));
            let Err(error) = reader.next_group() else {
                panic!("metadata must invalidate pending group")
            };
            assert_eq!(error.remaining, Input::new(&stream));
            assert!(matches!(error.error, LoadError::Parse(_)));
            assert!(matches!(reader.next_group(), Ok(None)));
        }
    }

    fn assert_invalid_magic(error: &LoadError) {
        let LoadError::Parse(error) = error else {
            panic!("expected parsing error")
        };
        assert!(matches!(
            error.downcast_ref::<parse::ParseError<chunk::error::Header>>(),
            Some(parse::ParseError::Error(
                chunk::error::Header::InvalidMagicBytes
            ))
        ));
    }

    #[test]
    fn invalid_magic_tail_yields_pending_group_then_reports_tail_error() {
        for signed in [false, true] {
            let body = body_bytes();
            let signatures = signature_bytes();
            let mut stream = body.clone();
            if signed {
                stream.extend_from_slice(&signatures);
            }
            let tail_start = stream.len();
            stream.extend_from_slice(&[1, 2, 3, 4]);
            let mut reader = ChunkGroupReader::new(Input::new(&stream));
            let Ok(Some(read)) = reader.next_group() else {
                panic!("invalid magic cannot be trailing metadata for the pending group");
            };
            assert_eq!(read.start(), Input::new(&stream));
            assert_eq!(read.group.signatures.is_some(), signed);
            let ChunkBody::Change(change) = *read.group.body else {
                panic!("expected change")
            };
            assert_eq!(change.raw_bytes(), body.as_slice());
            let Err(error) = reader.next_group() else {
                panic!("expected deferred tail error")
            };
            assert_eq!(error.remaining, Input::new(&stream[tail_start..]));
            assert_invalid_magic(&error.error);
            assert!(matches!(reader.next_group(), Ok(None)));
            assert!(matches!(reader.next_group(), Ok(None)));
        }
    }

    #[test]
    fn invalid_magic_without_pending_group_fails_immediately() {
        let bytes = [1, 2, 3, 4];
        let mut reader = ChunkGroupReader::new(Input::new(&bytes));
        let Err(error) = reader.next_group() else {
            panic!("expected immediate framing error")
        };
        assert_eq!(error.remaining, Input::new(&bytes));
        assert_invalid_magic(&error.error);
        assert!(matches!(reader.next_group(), Ok(None)));
    }

    #[test]
    fn group_reader_rejects_duplicate_metadata_but_defers_unclassified_tail() {
        let body = body_bytes();
        let sigs = signature_bytes();
        let duplicate = [body.as_slice(), sigs.as_slice(), sigs.as_slice()].concat();
        let mut reader = ChunkGroupReader::new(Input::new(&duplicate));
        let Err(error) = reader.next_group() else {
            panic!("expected duplicate error")
        };
        assert_eq!(error.remaining, Input::new(&duplicate));
        assert!(matches!(error.error, LoadError::DuplicateSignatures));
        assert!(matches!(reader.next_group(), Ok(None)));

        let truncated = [body.as_slice(), &[0x85]].concat();
        let mut reader = ChunkGroupReader::new(Input::new(&truncated));
        assert!(matches!(reader.next_group(), Ok(Some(_))));
        let Err(error) = reader.next_group() else {
            panic!("expected deferred framing error")
        };
        assert_eq!(error.remaining, Input::new(&truncated[body.len()..]));
        assert!(matches!(error.error, LoadError::Parse(_)));
        assert!(matches!(reader.next_group(), Ok(None)));
    }

    #[test]
    fn signatures_stay_pending_until_end() {
        let bytes = signature_bytes();
        let start = Input::new(&[1]);
        let state = continuing(AssemblyState::Empty.advance(ChunkEvent::Body {
            start,
            body: body(),
        }));
        let state = continuing(state.advance(ChunkEvent::Signatures {
            start: Input::new(&bytes),
            signatures: signatures(&bytes),
        }));
        let ControlFlow::Break(Output::Group {
            group,
            next: AfterYield::Finished,
        }) = state.advance(ChunkEvent::End)
        else {
            panic!("expected final group")
        };
        assert_eq!(group.start(), start);
        assert!(group.group.signatures.is_some());
    }

    #[test]
    fn next_body_yields_previous_group_and_retains_new_body() {
        let first = Input::new(&[1]);
        let second = Input::new(&[2]);
        let state = AssemblyState::pending(first, body());
        let ControlFlow::Break(Output::Group {
            group,
            next: AfterYield::Pending(pending),
        }) = state.advance(ChunkEvent::Body {
            start: second,
            body: body(),
        })
        else {
            panic!("expected yield with pending body")
        };
        assert_eq!(group.start(), first);
        assert!(group.group.signatures.is_none());
        assert_eq!(pending.start, second);
        assert!(matches!(
            AssemblyState::Pending(pending).advance(ChunkEvent::End),
            ControlFlow::Break(Output::Group {
                next: AfterYield::Finished,
                ..
            })
        ));
    }

    #[test]
    fn duplicate_signatures_fail_at_the_body_start() {
        let bytes = signature_bytes();
        let start = Input::new(&[1]);
        let state = continuing(AssemblyState::pending(start, body()).advance(
            ChunkEvent::Signatures {
                start: Input::new(&bytes),
                signatures: signatures(&bytes),
            },
        ));
        let ControlFlow::Break(Output::Error(error)) = state.advance(ChunkEvent::Signatures {
            start: Input::new(&bytes),
            signatures: signatures(&bytes),
        }) else {
            panic!("expected duplicate error")
        };
        assert!(matches!(error.error, LoadError::DuplicateSignatures));
        assert_eq!(error.remaining, start);
    }

    #[test]
    fn orphan_signatures_fail_at_their_own_start() {
        let bytes = signature_bytes();
        let start = Input::new(&bytes);
        let ControlFlow::Break(Output::Error(error)) =
            AssemblyState::Empty.advance(ChunkEvent::Signatures {
                start,
                signatures: signatures(&bytes),
            })
        else {
            panic!("expected orphan error")
        };
        assert!(matches!(error.error, LoadError::OrphanSignatures));
        assert_eq!(error.remaining, start);
    }

    #[test]
    fn body_failure_yields_pending_group_and_defers_error() {
        let failed_start = Input::new(&[2]);
        let ControlFlow::Break(Output::Group {
            next: AfterYield::DeferredError(error),
            ..
        }) = AssemblyState::pending(Input::new(&[1]), body()).advance(ChunkEvent::BodyFailed(
            Error {
                remaining: failed_start,
                error: LoadError::BadChecksum,
            },
        ))
        else {
            panic!("expected group before error")
        };
        assert_eq!(error.remaining, failed_start);
        assert!(matches!(error.error, LoadError::BadChecksum));
        let mut reader = ChunkGroupReader::new(Input::empty());
        reader.state = ReaderState::DeferredError(error);
        assert!(matches!(
            reader.next_group(),
            Err(Error {
                error: LoadError::BadChecksum,
                ..
            })
        ));
        assert!(matches!(reader.next_group(), Ok(None)));
        assert!(matches!(reader.next_group(), Ok(None)));
    }

    #[test]
    fn metadata_invalidates_pending_group_but_framing_error_is_deferred() {
        let start = Input::new(&[1]);
        for signed in [false, true] {
            for framing in [false, true] {
                let bytes = signature_bytes();
                let mut state = AssemblyState::pending(start, body());
                if signed {
                    state = continuing(state.advance(ChunkEvent::Signatures {
                        start: Input::new(&bytes),
                        signatures: signatures(&bytes),
                    }));
                }
                let error = Error {
                    remaining: Input::new(&[2]),
                    error: LoadError::BadChecksum,
                };
                let event = if framing {
                    ChunkEvent::FramingFailed(error)
                } else {
                    ChunkEvent::MetadataFailed(error)
                };
                match state.advance(event) {
                    ControlFlow::Break(Output::Group {
                        group,
                        next: AfterYield::DeferredError(error),
                    }) if framing => {
                        assert_eq!(group.start(), start);
                        assert_eq!(group.group.signatures.is_some(), signed);
                        assert_eq!(error.remaining, Input::new(&[2]));
                        assert!(matches!(error.error, LoadError::BadChecksum));
                    }
                    ControlFlow::Break(Output::Error(error)) if !framing => {
                        assert_eq!(error.remaining, start);
                        assert!(matches!(error.error, LoadError::BadChecksum));
                    }
                    _ => panic!("unexpected failure attribution"),
                }
            }
        }
    }

    #[test]
    fn failures_without_a_pending_group_preserve_their_start() {
        let start = Input::new(&[3]);
        for kind in 0..3 {
            let error = Error {
                remaining: start,
                error: LoadError::BadChecksum,
            };
            let event = match kind {
                0 => ChunkEvent::BodyFailed(error),
                1 => ChunkEvent::MetadataFailed(error),
                _ => ChunkEvent::FramingFailed(error),
            };
            let ControlFlow::Break(Output::Error(error)) = AssemblyState::Empty.advance(event)
            else {
                panic!("expected immediate error")
            };
            assert_eq!(error.remaining, start);
        }
    }

    #[test]
    fn empty_end_finishes_without_reading_more_events() {
        assert!(matches!(
            AssemblyState::Empty.advance(ChunkEvent::End),
            ControlFlow::Break(Output::End)
        ));
        let mut reader = ChunkGroupReader::new(Input::new(&[99]));
        reader.state = ReaderState::Finished;
        let remaining = reader.chunks.remaining;
        assert!(matches!(reader.next_group(), Ok(None)));
        assert!(matches!(reader.next_group(), Ok(None)));
        assert_eq!(reader.chunks.remaining, remaining);
    }
}
