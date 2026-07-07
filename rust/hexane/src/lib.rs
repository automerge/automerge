//! Hexane is a columnar compression library implementing the encoding
//! described in the
//! [Automerge Binary Format](https://automerge.org/automerge-binary-format-spec/).
//!
//! The API lives in [`v1`]: typed columns (`Column<T>`, `PrefixColumn<T>`,
//! `DeltaColumn<T>`, `RawColumn`) over RLE/delta/boolean encodings, with
//! O(log n) random access and in-place splice.

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

mod error;
pub use error::PackError;

pub mod v1;

pub use v1::leb::{lebsize, ulebsize};
