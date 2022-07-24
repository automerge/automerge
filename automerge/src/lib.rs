#![doc(
    html_logo_url = "https://raw.githubusercontent.com/automerge/automerge-rs/main/img/brandmark.svg",
    html_favicon_url = "https:///raw.githubusercontent.com/automerge/automerge-rs/main/img/favicon.ico"
)]
#![warn(
    missing_debug_implementations,
    // missing_docs, // TODO: add documentation!
    rust_2018_idioms,
    unreachable_pub,
    bad_style,
    const_err,
    dead_code,
    improper_ctypes,
    non_shorthand_field_patterns,
    no_mangle_generic_items,
    overflowing_literals,
    path_statements,
    patterns_in_fns_without_body,
    private_in_public,
    unconditional_recursion,
    unused,
    unused_allocation,
    unused_comparisons,
    unused_parens,
    while_true
)]

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

mod autocommit;
mod automerge;
mod change;
#[cfg(feature = "storage-v2")]
mod change_v2;
mod clock;
mod columnar;
#[cfg(feature = "storage-v2")]
#[allow(dead_code)]
#[allow(unused_imports)]
mod columnar_2;
#[cfg(feature = "storage-v2")]
#[allow(dead_code)]
mod convert;
mod decoding;
mod encoding;
mod error;
mod exid;
mod indexed_cache;
mod keys;
mod keys_at;
mod legacy;
mod list_range;
mod list_range_at;
mod map_range;
mod map_range_at;
mod op_observer;
mod op_set;
mod op_tree;
mod options;
mod parents;
mod query;
#[cfg(feature = "storage-v2")]
#[allow(dead_code)]
#[allow(unused_imports)]
mod storage;
pub mod sync;
pub mod transaction;
mod types;
mod value;
mod values;
#[cfg(feature = "optree-visualisation")]
mod visualisation;

pub use crate::automerge::Automerge;
pub use autocommit::AutoCommit;
//#[cfg(not(feature = "storage-v2"))]
pub use change::Change;
//#[cfg(feature = "storage-v2")]
//pub use change_v2::{Change, LoadError as LoadChangeError};
pub use decoding::Error as DecodingError;
pub use decoding::InvalidChangeError;
pub use encoding::Error as EncodingError;
pub use error::AutomergeError;
pub use error::InvalidActorId;
pub use error::InvalidChangeHashSlice;
pub use exid::ExId as ObjId;
pub use keys::Keys;
pub use keys_at::KeysAt;
pub use legacy::Change as ExpandedChange;
pub use list_range::ListRange;
pub use list_range_at::ListRangeAt;
pub use map_range::MapRange;
pub use map_range_at::MapRangeAt;
pub use op_observer::OpObserver;
pub use op_observer::Patch;
pub use op_observer::VecOpObserver;
pub use options::ApplyOptions;
pub use parents::Parents;
pub use types::{ActorId, ChangeHash, ObjType, OpType, Prop};
pub use value::{ScalarValue, Value};
pub use values::Values;

pub const ROOT: ObjId = ObjId::Root;
