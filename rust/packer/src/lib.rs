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
pub(crate) mod leb128;
pub(crate) mod pack;
pub(crate) mod raw;
pub(crate) mod rle;
pub(crate) mod slab;

#[cfg(test)]
pub(crate) mod test;

pub use aggregate::{Acc, Agg};
pub use boolean::BooleanCursor;
pub use columndata::{ColGroupItem, ColumnData, ColumnDataIter};
pub use cursor::{ColumnCursor, Run, ScanMeta};
pub use delta::DeltaCursor;
pub use leb128::{lebsize, ulebsize};
pub use pack::{MaybePackable, PackError, Packable};
pub use raw::{RawCursor, RawReader, ReadRawError};
pub use rle::{IntCursor, RleCursor, StrCursor};
pub use slab::{Slab, SlabTree, WriteOp};
