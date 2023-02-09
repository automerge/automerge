//! Types for reading data which is stored in a columnar storage format
//!
//! The details of how values are encoded in `encoding`, which exposes a set of "decoder" and
//! "encoder" types.
//!
//! The `column_range` module exposes a set of types - most of which are newtypes over
//! `Range<usize>` - which have useful instance methods such as `encode()` to create a new range and
//! `decoder()` to return an iterator of the correct type.
pub(crate) mod column_range;
pub(crate) use column_range::Key;
pub(crate) mod encoding;

mod splice_error;
pub(crate) use splice_error::SpliceError;
