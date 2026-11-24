#![doc = include_str!("../README.md")]

#[doc(hidden)]
#[macro_export]
macro_rules! log {
     ( $( $t:tt )* ) => {
          {
            use $crate::__log;
            __log!( $( $t )* );
          }
     }
 }

#[cfg(all(feature = "wasm", target_family = "wasm"))]
#[doc(hidden)]
#[macro_export]
macro_rules! __log {
     ( $( $t:tt )* ) => {
         web_sys::console::log_1(&format!( $( $t )* ).into());
     }
 }

#[cfg(not(all(feature = "wasm", target_family = "wasm")))]
#[doc(hidden)]
#[macro_export]
macro_rules! __log {
     ( $( $t:tt )* ) => {
         println!( $( $t )* );
     }
 }

// The verified core of every column. Each module is verified with Verus;
// what they take on trust is in `trusted`.
pub(crate) mod attrib;
#[cfg(feature = "bijou64")]
mod bijou64;
pub(crate) mod bool_edit;
pub(crate) mod bool_enc;
pub(crate) mod bool_unique;
pub(crate) mod bool_wire;
pub(crate) mod btree;
pub(crate) mod codec;
pub(crate) mod column;
pub(crate) mod column_core;
pub(crate) mod copy;
pub(crate) mod cursor;
pub(crate) mod delta;
pub(crate) mod edit_core;
mod error;
pub(crate) mod fenwick;
pub(crate) mod index;
pub(crate) mod iter_core;
mod leb128_codec;
pub(crate) mod load_core;
mod load_opts;
pub(crate) mod merge_walk;
pub(crate) mod prefix_core;
pub(crate) mod prefix_search;
pub(crate) mod raw;
pub(crate) mod remap;
pub(crate) mod rle;
pub(crate) mod splices;
pub(crate) mod trusted;
pub(crate) mod weights;
pub(crate) mod window;

/// The API, with its specification: every public function of it has a
/// contract that Verus proves. Its items are the crate's: `hexane::Column`
/// is `hexane::verified::Column`. The module's documentation is the guide
/// to reviewing the specification.
pub mod verified;

pub use verified::*;

#[cfg(feature = "bijou64")]
pub use codec::Bijou64;
pub use codec::{Leb128, Leb128Error};
pub use column::Splice;
pub use delta::DeltaRun;
pub use error::PackError;
pub use load_opts::{Fill, LoadOpts, MaybeFill, NoFill};

/// Sealing for the plumbing traits of the core (`SlabWeight`), which no
/// other crate implements.
mod sealed {
    vstd::prelude::verus! {
    pub trait Sealed {}
    } // verus!
}

#[cfg(test)]
mod edit_fuzz;
#[cfg(test)]
mod splice_fuzz;
#[cfg(test)]
mod tests;

// ── Run ─────────────────────────────────────────────────────────────────────

/// A run of identical values from a column iterator.
///
/// For repeat runs, `count` is the number of remaining items (including the
/// current one) and `value` is the repeated value.  For null runs, `count`
/// is the remaining null count and `value` is the type's null representation.
/// For literal runs, `count` is always 1 since each item is distinct.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Run<V> {
    pub count: usize,
    pub value: V,
}
