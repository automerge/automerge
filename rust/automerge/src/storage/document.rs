use hexane::PackError;
use std::collections::BTreeSet;
use std::{borrow::Cow, ops::Range};

use super::{parse, shift_range, ChunkType, Header, RawColumns};

use crate::change_graph::ChangeGraph;
use crate::op_set2::change::{ChangeCollector, CollectedChanges, OutOfMemory};
use crate::op_set2::{OpSet, ReadOpError};
use crate::storage::columns::compression::Uncompressed;
use crate::storage::ColumnSpec;
use crate::{ActorId, Automerge, Change, ChangeHash, TextEncoding};

mod compression;

#[allow(dead_code)]
pub(crate) enum CompressConfig {
    None,
    Threshold(usize),
}

#[derive(Debug, Clone)]
pub(crate) struct Document<'a> {
    bytes: Cow<'a, [u8]>,
    #[allow(dead_code)]
    compressed_bytes: Option<Cow<'a, [u8]>>,
    header: Header,
    actors: Vec<ActorId>,
    heads: Vec<ChangeHash>,
    pub(crate) op_metadata: RawColumns<Uncompressed>,
    op_bytes: Range<usize>,
    change_metadata: RawColumns<Uncompressed>,
    change_bytes: Range<usize>,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum ParseError {
    #[error(transparent)]
    Leb128(#[from] parse::leb128::Error),
    #[error(transparent)]
    RawColumns(#[from] crate::storage::columns::raw_column::ParseError),
    #[error("bad column layout for {column_type}s: {error}")]
    BadColumnLayout {
        column_type: &'static str,
        error: super::columns::BadColumnLayout,
    },
}

impl<'a> Document<'a> {
    /// Parse a document chunk. Input must be the entire chunk including the header and magic
    /// bytes but the header must already have been parsed. That is to say, this is expected to be
    /// used like so:
    ///
    /// ```rust,ignore
    /// # use automerge::storage::{parse::{ParseResult, Input}, Document, Header};
    /// # fn main() -> ParseResult<(), ()> {
    /// let chunkbytes: &[u8] = todo!();
    /// let input = Input::new(chunkbytes);
    /// let (i, header) = Header::parse(input)?;
    /// let (i, doc) = Document::parse(i, header)?;
    /// # }
    /// ```
    pub(crate) fn parse(
        input: parse::Input<'a>,
        header: Header,
    ) -> parse::ParseResult<'a, Document<'a>, ParseError> {
        let i = input;

        // Because some columns in a document may be compressed we do some funky stuff when
        // parsing. As we're parsing the chunk we split the data into four parts:
        //
        // .----------------.
        // | Prefix         |
        // |.--------------.|
        // || Actors       ||
        // || Heads        ||
        // || Change Meta  ||
        // || Ops Meta     ||
        // |'--------------'|
        // +----------------+
        // | Change data    |
        // +----------------+
        // | Ops data       |
        // +----------------+
        // | Suffix         |
        // |.--------------.|
        // || Head indices ||
        // |'--------------'|
        // '----------------'
        //
        // We record the range of each of these sections using `parse::range_of`. Later, we check
        // if any of the column definitions in change meta or ops meta specify that their columns
        // are compressed. If there are compressed columns then we copy the uncompressed parts of the
        // input data to a new output vec, then decompress the compressed parts. Specifically we do
        // the following:
        //
        // * Copy everything in prefix to the output buffer
        // * If any of change columns are compressed, copy all of change data to the output buffer
        //   decompressing each compressed column
        // * Likewise if any of ops columns are compressed copy the data decompressing as required
        // * Finally copy the suffix
        //
        // The reason for all this work is that we end up keeping all of the data behind the
        // document chunk in a single Vec, which plays nicely with the cache and makes dumping the
        // document to disk or network straightforward.

        // parse everything in the prefix
        let (i, r) = parse::range_of(
            |i| -> parse::ParseResult<'_, _, ParseError> {
                let (i, actors) = parse::length_prefixed(parse::actor_id)(i)?;
                let (i, heads) = parse::length_prefixed(parse::change_hash)(i)?;
                let (i, change_meta) = RawColumns::parse::<ParseError>(i)?;
                let (i, ops_meta) = RawColumns::parse::<ParseError>(i)?;
                Ok((i, (actors, heads, change_meta, ops_meta)))
            },
            i,
        )?;
        let prefix = r.range.start;
        let (actors, heads, change_meta, ops_meta) = r.value;

        // parse the change data
        let change_len = change_meta.total_column_len();
        let (i, changes) = parse::range_only(|i| parse::take_n(change_len, i), i)?;

        // parse the ops data
        let ops_len = ops_meta.total_column_len();
        let (i, ops) = parse::range_only(|i| parse::take_n(ops_len, i), i)?;

        // parse the suffix, which may be empty if this document was produced by an older version
        // of the JS automerge implementation
        let (i, Range { start: suffix, .. }) = parse::range_only_unless_empty(
            |i| parse::apply_n(heads.len(), parse::leb128_u64::<ParseError>)(i),
            i,
        )?;

        let compression::Decompressed {
            change_bytes,
            op_bytes,
            uncompressed,
            compressed,
            changes,
            ops,
        } = compression::decompress(compression::Args {
            prefix,
            suffix,
            original: Cow::Borrowed(input.bytes()),
            changes: compression::Cols::new(changes, change_meta),
            ops: compression::Cols::new(ops, ops_meta),
            extra_args: (),
        })
        .map_err(|e| parse::ParseError::Error(ParseError::RawColumns(e)))?;

        let op_metadata = OpSet::validate(op_bytes.len(), &ops).map_err(|error| {
            parse::ParseError::Error(ParseError::BadColumnLayout {
                column_type: "ops",
                error,
            })
        })?;

        let change_metadata =
            ChangeGraph::validate(change_bytes.len(), &changes).map_err(|error| {
                parse::ParseError::Error(ParseError::BadColumnLayout {
                    column_type: "changes",
                    error,
                })
            })?;

        Ok((
            i,
            Document {
                bytes: uncompressed,
                compressed_bytes: compressed,
                header,
                actors,
                heads,
                op_metadata,
                op_bytes,
                change_metadata,
                change_bytes,
            },
        ))
    }

    pub(crate) fn change_meta(&self) -> &RawColumns<Uncompressed> {
        &self.change_metadata
    }

    pub(crate) fn change_bytes(&self) -> &[u8] {
        &self.bytes[self.change_bytes.clone()]
    }

    pub(crate) fn new(
        op_set: &OpSet,
        change_graph: &ChangeGraph,
        compress: CompressConfig,
    ) -> Document<'static> {
        let (op_metadata, ops_out_b) = op_set.export();

        let mut change_out = Vec::new();
        let change_metadata = change_graph.encode(&mut change_out);

        // actors already sorted
        let actors = op_set.actors.clone();

        let mut data = Vec::with_capacity(ops_out_b.len() + change_out.len());
        leb128::write::unsigned(&mut data, actors.len() as u64).unwrap();
        for actor in &actors {
            leb128::write::unsigned(&mut data, actor.to_bytes().len() as u64).unwrap();
            data.extend(actor.to_bytes());
        }

        let heads = change_graph.heads().collect::<Vec<_>>();
        let head_indices = change_graph.head_indexes().collect::<Vec<_>>();

        leb128::write::unsigned(&mut data, heads.len() as u64).unwrap();
        for head in &heads {
            data.extend(head.as_bytes());
        }
        let prefix_len = data.len();

        change_metadata.write(&mut data);
        op_metadata.write(&mut data);
        let change_start = data.len();
        let change_end = change_start + change_out.len();
        data.extend(change_out);
        let ops_start = data.len();
        let ops_end = ops_start + ops_out_b.len();
        data.extend(ops_out_b);
        let suffix_start = data.len();

        for index in &head_indices {
            leb128::write::unsigned(&mut data, *index).unwrap();
        }

        let header = Header::new(ChunkType::Document, &data);
        let mut bytes = Vec::with_capacity(data.len() + header.len());
        header.write(&mut bytes);
        let header_len = bytes.len();
        bytes.extend(&data);

        let op_bytes = shift_range(ops_start..ops_end, header.len());
        let change_bytes = shift_range(change_start..change_end, header.len());

        let compressed_bytes = if let CompressConfig::Threshold(threshold) = compress {
            let compressed = Cow::Owned(compression::compress(compression::Args {
                prefix: prefix_len + header.len(),
                suffix: suffix_start + header.len(),
                ops: compression::Cols {
                    raw_columns: op_metadata.clone(),
                    data: op_bytes.clone(),
                },
                changes: compression::Cols {
                    raw_columns: change_metadata.clone(),
                    data: change_bytes.clone(),
                },
                original: Cow::Borrowed(&bytes),
                extra_args: compression::CompressArgs {
                    threshold,
                    original_header_len: header_len,
                },
            }));
            Some(compressed)
        } else {
            None
        };

        Document {
            actors,
            bytes: Cow::Owned(bytes),
            compressed_bytes,
            header,
            heads,
            op_metadata,
            op_bytes,
            change_metadata,
            change_bytes,
        }
    }

    pub(crate) fn op_raw_bytes(&self) -> &[u8] {
        &self.bytes[self.op_bytes.clone()]
    }

    pub(crate) fn into_bytes(self) -> Vec<u8> {
        if let Some(compressed) = self.compressed_bytes {
            compressed.into_owned()
        } else {
            self.bytes.into_owned()
        }
    }

    pub(crate) fn checksum_valid(&self) -> bool {
        self.header.checksum_valid()
    }

    pub(crate) fn actors(&self) -> &[ActorId] {
        &self.actors
    }

    pub(crate) fn heads(&self) -> &[ChangeHash] {
        &self.heads
    }

    fn verify_changes(
        &self,
        cc: &CollectedChanges,
        mode: VerificationMode,
    ) -> Result<(), ReconstructError> {
        if mode == VerificationMode::Check && !self.heads().iter().eq(cc.heads.iter()) {
            let expected_heads: BTreeSet<_> = self.heads().iter().cloned().collect();
            tracing::error!(?expected_heads, ?cc.heads, "mismatching heads");
            Err(ReconstructError::MismatchingHeads(MismatchedHeads {
                changes: cc.changes.clone(),
                expected_heads,
                derived_heads: cc.heads.clone(),
            }))
        } else {
            Ok(())
        }
    }

    pub(crate) fn reconstruct(
        &self,
        mode: VerificationMode,
        text_encoding: TextEncoding,
    ) -> Result<Automerge, ReconstructError> {
        let mut op_set = OpSet::load(self, text_encoding)?;
        let mut change_graph = ChangeGraph::load(self)?;

        let mut index = op_set.index_builder();

        let change_collector = ChangeCollector::try_new(&change_graph, &op_set)?;
        let mut change_collector = change_collector.with_index(&mut index);

        change_collector.process_ops(&op_set)?;

        let changes = change_collector.build_changegraph(&op_set)?;

        self.verify_changes(&changes, mode)?;

        op_set.set_indexes(index);

        change_graph.set_hashes(&changes.changes);

        debug_assert_eq!(changes.changes.len(), change_graph.len());

        debug_assert!(op_set.validate_top_index());

        Ok(Automerge::from_parts(op_set, change_graph))
    }

    pub(crate) fn reconstruct_changes(
        &self,
        text_encoding: TextEncoding,
    ) -> Result<Vec<Change>, ReconstructError> {
        let op_set = OpSet::load(self, text_encoding)?;
        let change_graph = ChangeGraph::load(self)?;

        let mut change_collector = ChangeCollector::try_new(&change_graph, &op_set)?;

        change_collector.process_ops(&op_set)?;

        Ok(change_collector.build_changegraph(&op_set)?.changes)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReconstructError {
    // FIXME - I need to do this check
    //#[error("the document contained ops which were out of order")]
    //OpsOutOfOrder,
    #[error("invalid changes: {0}")]
    InvalidChanges(#[from] crate::storage::load::change_collector::Error),
    #[error("mismatching heads")]
    MismatchingHeads(MismatchedHeads),
    // FIXME - i need to do this check
    //#[error("succ out of order")]
    //SuccOutOfOrder,
    #[error(transparent)]
    InvalidOp(#[from] crate::error::InvalidOpType),
    #[error(transparent)]
    PackErr(#[from] PackError),
    #[error(transparent)]
    ReadOpErr(#[from] ReadOpError),
    #[error("invalid actor id {0}")]
    InvalidActorId(usize),
    #[error("invalid column length {0:?}")]
    InvalidColumnLength(ColumnSpec),
    #[error("max_op is lower than start_op")]
    InvalidMaxOp,
    #[error(transparent)]
    OutOfMemory(#[from] OutOfMemory),
}

pub(crate) struct MismatchedHeads {
    changes: Vec<Change>,
    expected_heads: BTreeSet<ChangeHash>,
    derived_heads: BTreeSet<ChangeHash>,
}

impl std::fmt::Debug for MismatchedHeads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MismatchedHeads")
            .field("changes", &self.changes.len())
            .field("expected_heads", &self.expected_heads)
            .field("derived_heads", &self.derived_heads)
            .finish()
    }
}

use super::load::VerificationMode;
