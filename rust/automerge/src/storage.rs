use std::ops::Range;

mod bundle_v0;
pub(crate) mod change;
pub(crate) mod change_set;
mod chunk;
pub(crate) mod columns;
pub(crate) mod document;
pub(crate) mod load;
pub(crate) mod parse;

pub use change_set::{ChangeSet, ChangeSetChange, InvalidChangeSet};
pub use load::VerificationMode;

pub(crate) use {
    change::{AsChangeOp, Change, ChangeOp, Compressed, ReadChangeOpError},
    change_set::{ChangeSetChangeCols, ChangeSetMetadata, ChangeSetStorage, DepRef},
    chunk::{CheckSum, Chunk, ChunkType, Header},
    columns::{ColumnSpec, Columns, RawColumn, RawColumns},
    document::{CompressConfig, Document},
};

fn shift_range(range: Range<usize>, by: usize) -> Range<usize> {
    range.start + by..range.end + by
}

pub(crate) const MAGIC_BYTES: [u8; 4] = [0x85, 0x6f, 0x4a, 0x83];
