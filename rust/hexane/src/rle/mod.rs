//! The verified core of an RLE column: the format's model (`model`),
//! reading (`read`, `dec`), writing (`enc`, `writer`, `state`), cutting
//! and merging slabs (`cut`, `merge`, `postfix`), and edits (`edit`).
//!
//! The encoding (shared with v0) is a sequence of runs:
//!
//!   Repeat run : signed_leb128( count > 0 )  packed_value
//!   Literal run: signed_leb128( -n      )    v0 v1 … v(n-1)
//!   Null run   : signed_leb128( 0       )    unsigned_leb128( count )

pub(crate) mod cut;
pub(crate) mod dec;
pub(crate) mod edit;
pub(crate) mod enc;
pub(crate) mod lift;
pub(crate) mod merge;
pub(crate) mod model;
pub(crate) mod postfix;
pub(crate) mod read;
pub(crate) mod size;
pub(crate) mod state;
pub(crate) mod unique;
pub(crate) mod writer;

use vstd::prelude::*;

/// A slab of an RLE column: its bytes, counts and tail.
pub(crate) type Slab = crate::column::Slab<RleTail>;

verus! {

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
#[verifier::allow(autoderive_clone_without_spec)]
pub struct RleTail {
    /// Total byte length of the last segment (header + values).
    /// Same meaning for repeat, literal, and null runs.
    ///
    /// `u32` by design: a single segment (one run) must stay under
    /// 4 GiB.  Large blobs belong in [`RawColumn`](crate::RawColumn),
    /// not an RLE column.
    pub(crate) bytes: u32,
    /// For literal runs: byte length of the last value only.
    /// 0 for repeat/null runs.
    pub(crate) lit_tail: u32,
}

} // verus!
