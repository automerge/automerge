use std::ops::Range;

pub(crate) mod change;
mod chunk;
mod columns;
pub(crate) mod convert;
mod document;
pub(crate) mod load;
pub(crate) mod parse;
pub(crate) mod save;

pub use load::VerificationMode;
pub(crate) use {
    change::{AsChangeOp, Change, ChangeOp, Compressed, ReadChangeOpError},
    chunk::{CheckSum, Chunk, ChunkType, Header},
    columns::{Columns, MismatchingColumn, RawColumn, RawColumns},
    document::{AsChangeMeta, AsDocOp, ChangeMetadata, CompressConfig, DocOp, Document},
};

fn shift_range(range: Range<usize>, by: usize) -> Range<usize> {
    range.start + by..range.end + by
}

pub(crate) const MAGIC_BYTES: [u8; 4] = [0x85, 0x6f, 0x4a, 0x83];
