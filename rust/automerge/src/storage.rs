use std::ops::Range;

pub(crate) mod bundle;
mod bundle_v0;
pub(crate) mod change;
mod chunk;
pub(crate) mod columns;
pub(crate) mod document;
pub(crate) mod load;
pub(crate) mod parse;

pub use bundle::{AuditedMember, Bundle, BundleChange, InvalidBundle, MalformedBundle};
pub use load::VerificationMode;

pub(crate) use {
    bundle::{BundleChangeCols, BundleChangeWriter, BundleStorage, DeltaRunGrouper},
    change::{AsChangeOp, Change, ChangeOp, Compressed, ReadChangeOpError},
    chunk::{hash_parts, CheckSum, Chunk, ChunkType, Header},
    columns::{ColumnSpec, Columns, RawColumn, RawColumns},
    document::{CompressConfig, Document},
};

fn shift_range(range: Range<usize>, by: usize) -> Range<usize> {
    range.start + by..range.end + by
}

pub(crate) const MAGIC_BYTES: [u8; 4] = [0x85, 0x6f, 0x4a, 0x83];
