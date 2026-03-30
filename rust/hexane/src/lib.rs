//! Hexane is a columnar compression library implementing the encoding described in the
//! [Automerge Binary Format](https://automerge.org/automerge-binary-format-spec/).
//!
//! Data is stored in [`ColumnData<C>`] where the cursor type `C` selects the encoding
//! (`RLE<T>`, delta, boolean, or raw). Values are batched into [`Slab`]s — `Arc`-wrapped byte
//! buffers — held in a [`SpanTree`] B-tree for O(log n) positional seek, insert, and splice.
//!
//! # Cursor Types
//!
//! | Type            | Item    | Encoding                        |
//! |-----------------|---------|---------------------------------|
//! | [`UIntCursor`]  | `u64`   | RLE + unsigned LEB128           |
//! | [`IntCursor`]   | `i64`   | RLE + signed LEB128             |
//! | [`StrCursor`]   | `str`   | RLE + length-prefixed UTF-8     |
//! | [`ByteCursor`]  | `[u8]`  | RLE + length-prefixed bytes     |
//! | [`BooleanCursor`]| `bool` | Boolean run-length encoding     |
//! | [`DeltaCursor`] | `i64`   | Delta-encoded integers          |
//! | [`RawCursor`]   | `[u8]`  | Uncompressed raw bytes          |
//!
//! # Quick Example
//!
//! ```rust
//! use hexane::{ColumnData, UIntCursor};
//!
//! let mut col: ColumnData<UIntCursor> = ColumnData::new();
//! col.splice(0, 0, [1u64, 2, 3, 4, 5]);
//! assert_eq!(col.to_vec(), vec![Some(1), Some(2), Some(3), Some(4), Some(5)]);
//!
//! let bytes = col.save();
//! let col2: ColumnData<UIntCursor> = ColumnData::load(&bytes).unwrap();
//! assert_eq!(col.to_vec(), col2.to_vec());
//! ```
//!
//! See the [README](https://github.com/automerge/automerge/tree/main/rust/hexane) for
//! comprehensive usage documentation.

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

pub(crate) mod aggregate;
pub(crate) mod boolean;
pub(crate) mod columndata;
pub(crate) mod cursor;
pub(crate) mod delta;
pub(crate) mod encoder;
pub(crate) mod leb128;
pub(crate) mod pack;
pub(crate) mod raw;
pub(crate) mod rle;
pub(crate) mod slab;

#[cfg(test)]
pub mod test;

pub mod v1;

pub use aggregate::{Acc, Agg};
pub use boolean::BooleanCursor;
pub use columndata::{
    ColAccIter, ColGroupItem, ColGroupIter, ColumnData, ColumnDataIter, ColumnDataIterState,
};
pub use cursor::{ColumnCursor, CursorIter, HasAcc, HasMinMax, HasPos, Run, RunIter, SpliceDel};
pub use delta::DeltaCursor;
pub use encoder::{Encoder, EncoderState};
pub use leb128::{lebsize, ulebsize};
pub use pack::{MaybePackable, PackError, Packable};
pub use raw::{RawCursor, RawCursorInternal, RawReader, ReadRawError};
pub use rle::{ByteCursor, IntCursor, RleCursor, StrCursor, UIntCursor};
pub use slab::{tree, Slab, SlabTree, SlabWeight, SlabWriter, SpanTree, SpanWeight, WriteOp};

pub(crate) use std::borrow::Cow;
