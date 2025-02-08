use std::ops::Range;

pub(crate) mod change;
mod chunk;
pub(crate) mod columns;
pub(crate) mod document;
pub(crate) mod load;
pub(crate) mod parse;

pub use load::VerificationMode;
pub(crate) use {
    change::{AsChangeOp, Change, ChangeOp, Compressed, ReadChangeOpError},
    chunk::{CheckSum, Chunk, ChunkType, Header},
    columns::{ColumnSpec, Columns, MismatchingColumn, RawColumn, RawColumns},
    document::{ChangeMetadata, CompressConfig, DocChangeColumns, Document},
};

fn shift_range(range: Range<usize>, by: usize) -> Range<usize> {
    range.start + by..range.end + by
}

pub(crate) const MAGIC_BYTES: [u8; 4] = [0x85, 0x6f, 0x4a, 0x83];
