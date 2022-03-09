use std::ops::Range;

pub(crate) mod change;
mod chunk;
mod column_layout;
pub(crate) mod convert;
mod document;
pub(crate) mod load;
pub(crate) mod parse;
mod raw_column;
pub(crate) mod save;

pub(crate) use {
    change::{AsChangeOp, Change, ChangeOp, Compressed, ReadChangeOpError},
    chunk::{CheckSum, Chunk, ChunkType, Header},
    column_layout::{ColumnLayout, MismatchingColumn},
    document::{AsChangeMeta, AsDocOp, ChangeMetadata, CompressConfig, DocOp, Document},
    raw_column::{RawColumn, RawColumns},
};

fn shift_range(range: Range<usize>, by: usize) -> Range<usize> {
    range.start + by..range.end + by
}

pub(crate) const MAGIC_BYTES: [u8; 4] = [0x85, 0x6f, 0x4a, 0x83];
