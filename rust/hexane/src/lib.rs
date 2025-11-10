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

pub use aggregate::{Acc, Agg};
pub use boolean::BooleanCursor;
pub use columndata::{
    ColAccIter, ColGroupItem, ColGroupIter, ColumnData, ColumnDataIter, ColumnDataIterState,
};
pub use cursor::{ColumnCursor, CursorIter, HasAcc, HasPos, Run, SpliceDel};
pub use delta::DeltaCursor;
pub use encoder::{Encoder, EncoderState};
pub use leb128::{lebsize, ulebsize};
pub use pack::{MaybePackable, PackError, Packable};
pub use raw::{RawCursor, RawReader, ReadRawError};
pub use rle::{ByteCursor, IntCursor, RleCursor, StrCursor, UIntCursor};
pub use slab::{tree, Slab, SlabTree, SlabWeight, SlabWriter, SpanTree, SpanWeight, WriteOp};

pub(crate) use std::borrow::Cow;
