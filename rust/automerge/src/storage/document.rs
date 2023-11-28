use std::{borrow::Cow, ops::Range};

use super::{parse, shift_range, ChunkType, Columns, Header, RawColumns};

use crate::{convert, ActorId, ChangeHash};

mod doc_op_columns;
use doc_op_columns::DocOpColumns;
pub(crate) use doc_op_columns::{AsDocOp, DocOp, ReadDocOpError};
mod doc_change_columns;
use doc_change_columns::DocChangeColumns;
pub(crate) use doc_change_columns::{AsChangeMeta, ChangeMetadata, ReadChangeError};
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
    op_metadata: DocOpColumns,
    op_bytes: Range<usize>,
    change_metadata: DocChangeColumns,
    change_bytes: Range<usize>,
    #[allow(dead_code)]
    head_indices: Vec<u64>,
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
    #[error(transparent)]
    BadDocOps(#[from] doc_op_columns::Error),
    #[error(transparent)]
    BadDocChanges(#[from] doc_change_columns::ReadChangeError),
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
        let (
            i,
            parse::RangeOf {
                range: prefix,
                value: (actors, heads, change_meta, ops_meta),
            },
        ) = parse::range_of(
            |i| -> parse::ParseResult<'_, _, ParseError> {
                let (i, actors) = parse::length_prefixed(parse::actor_id)(i)?;
                let (i, heads) = parse::length_prefixed(parse::change_hash)(i)?;
                let (i, change_meta) = RawColumns::parse::<ParseError>(i)?;
                let (i, ops_meta) = RawColumns::parse::<ParseError>(i)?;
                Ok((i, (actors, heads, change_meta, ops_meta)))
            },
            i,
        )?;

        // parse the change data
        let (i, parse::RangeOf { range: changes, .. }) =
            parse::range_of(|i| parse::take_n(change_meta.total_column_len(), i), i)?;

        // parse the ops data
        let (i, parse::RangeOf { range: ops, .. }) =
            parse::range_of(|i| parse::take_n(ops_meta.total_column_len(), i), i)?;

        // parse the suffix, which may be empty if this document was produced by an older version
        // of the JS automerge implementation
        let (i, suffix, head_indices) = if i.is_empty() {
            (i, 0..0, Vec::new())
        } else {
            let (
                i,
                parse::RangeOf {
                    range: suffix,
                    value: head_indices,
                },
            ) = parse::range_of(
                |i| parse::apply_n(heads.len(), parse::leb128_u64::<ParseError>)(i),
                i,
            )?;
            (i, suffix, head_indices)
        };

        let compression::Decompressed {
            change_bytes,
            op_bytes,
            uncompressed,
            compressed,
            changes,
            ops,
        } = compression::decompress(compression::Args {
            prefix: prefix.start,
            suffix: suffix.start,
            original: Cow::Borrowed(input.bytes()),
            changes: compression::Cols {
                data: changes,
                raw_columns: change_meta,
            },
            ops: compression::Cols {
                data: ops,
                raw_columns: ops_meta,
            },
            extra_args: (),
        })
        .map_err(|e| parse::ParseError::Error(ParseError::RawColumns(e)))?;

        let ops_layout = Columns::parse(op_bytes.len(), ops.iter()).map_err(|e| {
            parse::ParseError::Error(ParseError::BadColumnLayout {
                column_type: "ops",
                error: e,
            })
        })?;
        let ops_cols =
            DocOpColumns::try_from(ops_layout).map_err(|e| parse::ParseError::Error(e.into()))?;

        let change_layout = Columns::parse(change_bytes.len(), changes.iter()).map_err(|e| {
            parse::ParseError::Error(ParseError::BadColumnLayout {
                column_type: "changes",
                error: e,
            })
        })?;
        let change_cols = DocChangeColumns::try_from(change_layout)
            .map_err(|e| parse::ParseError::Error(e.into()))?;

        Ok((
            i,
            Document {
                bytes: uncompressed,
                compressed_bytes: compressed,
                header,
                actors,
                heads,
                op_metadata: ops_cols,
                op_bytes,
                change_metadata: change_cols,
                change_bytes,
                head_indices,
            },
        ))
    }

    pub(crate) fn new<'b, I, C, IC, D, O>(
        mut actors: Vec<ActorId>,
        heads_with_indices: Vec<(ChangeHash, usize)>,
        ops: I,
        changes: IC,
        compress: CompressConfig,
    ) -> Document<'static>
    where
        I: Iterator<Item = D> + Clone + ExactSizeIterator,
        O: convert::OpId<usize>,
        D: AsDocOp<'b, OpId = O>,
        C: AsChangeMeta<'b>,
        IC: Iterator<Item = C> + Clone,
    {
        let mut ops_out = Vec::new();
        let ops_meta = DocOpColumns::encode(ops, &mut ops_out);

        let mut change_out = Vec::new();
        let change_meta = DocChangeColumns::encode(changes, &mut change_out);
        actors.sort_unstable();

        let mut data = Vec::with_capacity(ops_out.len() + change_out.len());
        leb128::write::unsigned(&mut data, actors.len() as u64).unwrap();
        for actor in &actors {
            leb128::write::unsigned(&mut data, actor.to_bytes().len() as u64).unwrap();
            data.extend(actor.to_bytes());
        }
        leb128::write::unsigned(&mut data, heads_with_indices.len() as u64).unwrap();
        for (head, _) in &heads_with_indices {
            data.extend(head.as_bytes());
        }
        let prefix_len = data.len();

        change_meta.raw_columns().write(&mut data);
        ops_meta.raw_columns().write(&mut data);
        let change_start = data.len();
        let change_end = change_start + change_out.len();
        data.extend(change_out);
        let ops_start = data.len();
        let ops_end = ops_start + ops_out.len();
        data.extend(ops_out);
        let suffix_start = data.len();

        let head_indices = heads_with_indices
            .iter()
            .map(|(_, i)| *i as u64)
            .collect::<Vec<_>>();
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
                    raw_columns: ops_meta.raw_columns(),
                    data: op_bytes.clone(),
                },
                changes: compression::Cols {
                    raw_columns: change_meta.raw_columns(),
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
            heads: heads_with_indices.into_iter().map(|(h, _)| h).collect(),
            op_metadata: ops_meta,
            op_bytes,
            change_metadata: change_meta,
            change_bytes,
            head_indices,
        }
    }

    pub(crate) fn iter_ops(
        &'a self,
    ) -> impl Iterator<Item = Result<DocOp, ReadDocOpError>> + Clone + 'a {
        self.op_metadata.iter(&self.bytes[self.op_bytes.clone()])
    }

    pub(crate) fn iter_changes(
        &'a self,
    ) -> impl Iterator<Item = Result<ChangeMetadata<'_>, ReadChangeError>> + Clone + 'a {
        self.change_metadata
            .iter(&self.bytes[self.change_bytes.clone()])
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
}
