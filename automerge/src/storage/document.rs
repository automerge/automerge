use std::{borrow::Cow, ops::Range};

use super::{parse, shift_range, ChunkType, ColumnLayout, Header, RawColumns};

use crate::{convert, ActorId, ChangeHash};

mod doc_op_columns;
use doc_op_columns::DocOpColumns;
pub(crate) use doc_op_columns::{AsDocOp, DocOp, ReadDocOpError};
mod doc_change_columns;
use doc_change_columns::DocChangeColumns;
pub(crate) use doc_change_columns::{AsChangeMeta, ChangeMetadata, ReadChangeError};
mod compression;

pub(crate) enum CompressConfig {
    None,
    Threshold(usize),
}

#[derive(Debug)]
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

impl<'a> Document<'a> {
    /// Parse a document chunk. Input must be the entire chunk including the header and magic
    /// bytes.
    pub(crate) fn parse(input: &'a [u8], header: Header) -> parse::ParseResult<'_, Document<'a>> {
        let i = &input[header.data_bytes()];
        let (i, actors) = parse::length_prefixed(parse::leb128_u64, parse::actor_id)(i)?;
        let (i, heads) = parse::length_prefixed(parse::leb128_u64, parse::change_hash)(i)?;
        let before_ops = input.len() - i.len();
        let (i, change_meta) = RawColumns::parse(i)?;
        let (i, ops_meta) = RawColumns::parse(i)?;

        let change_data_start = input.len() - i.len();
        let (i, _change_data) = parse::take_n(change_meta.total_column_len(), i)?;

        let ops_data_start = input.len() - i.len();
        let (i, _ops_data) = parse::take_n(ops_meta.total_column_len(), i)?;

        let head_start = input.len() - i.len();
        let (i, head_indices) = parse::apply_n(heads.len(), parse::leb128_u64)(i)?;
        let head_end = input.len() - i.len();

        tracing::trace!(change_bytes = ?&input[change_data_start..(change_data_start + change_meta.total_column_len())], "decompressing");

        let compression::Decompressed {
            change_bytes,
            op_bytes,
            uncompressed,
            compressed,
            changes,
            ops,
        } = compression::decompress(compression::Args {
            prefix: before_ops,
            suffix: head_start,
            original: Cow::Borrowed(&input[..head_end]),
            changes: compression::Cols {
                data: change_data_start..(change_data_start + change_meta.total_column_len()),
                raw_columns: change_meta,
            },
            ops: compression::Cols {
                data: ops_data_start..(ops_data_start + ops_meta.total_column_len()),
                raw_columns: ops_meta,
            },
            dir_args: (),
        });

        let ops_layout = ColumnLayout::parse(op_bytes.len(), ops.iter())
            .map_err(|e| parse::ParseError::parse_columns("doc ops", e))?;
        let ops_cols = DocOpColumns::try_from(ops_layout)
            .map_err(|e| parse::ParseError::parse_columns("doc ops", e))?;

        let change_layout = ColumnLayout::parse(change_bytes.len(), changes.iter())
            .map_err(|e| parse::ParseError::parse_columns("change ops", e))?;
        let change_cols = DocChangeColumns::try_from(change_layout)
            .map_err(|e| parse::ParseError::parse_columns("change ops", e))?;

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
        I: Iterator<Item = D> + Clone,
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
                dir_args: compression::CompressArgs {
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
