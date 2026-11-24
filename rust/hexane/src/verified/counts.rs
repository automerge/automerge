//! The run counts of a `bool` column, verified with Verus.
//!
//! A `bool` column is the lengths of its runs, written as numbers of the
//! column's codec. The bool core reaches the codec through a contract
//! ([`CountOps`](crate::bool_wire::CountOps)), which is *proved* here for
//! every codec ([`Codec`]).
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([`verified`](crate::verified#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`counts/imp.rs`), which has
//! its contract word for word and is proved there.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use super::codec::Codec;
#[cfg(verus_keep_ghost)]
use crate::bool_wire::{CountOps, CountsOf};
#[allow(unused_imports)]
use vstd::prelude::*;

mod imp;
#[cfg(verus_keep_ghost)]
#[allow(unused_imports)]
pub(crate) use imp::lemma_bool_size;
#[allow(unused_imports)]
use imp::*;
#[allow(unused_imports)]
pub(crate) use imp::{VCounts, VB};

verus! {} // verus!
