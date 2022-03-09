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
#[macro_export]
macro_rules! __log {
     ( $( $t:tt )* ) => {
         web_sys::console::log_1(&format!( $( $t )* ).into());
     }
 }

#[cfg(not(all(feature = "wasm", target_family = "wasm")))]
#[macro_export]
macro_rules! __log {
     ( $( $t:tt )* ) => {
         println!( $( $t )* );
     }
 }

mod autocommit;
mod automerge;
#[cfg(not(feature = "storage-v2"))]
mod change;
#[cfg(feature = "storage-v2")]
mod change_v2;
mod clock;
#[cfg(not(feature = "storage-v2"))]
mod columnar;
mod columnar_2;
mod decoding;
mod encoding;
mod error;
mod exid;
mod indexed_cache;
mod keys;
mod keys_at;
mod legacy;
mod op_set;
mod op_tree;
mod query;
pub mod sync;
pub mod transaction;
mod autoserde;
mod types;
mod value;
#[cfg(feature = "optree-visualisation")]
mod visualisation;

pub use crate::automerge::Automerge;
pub use autocommit::AutoCommit;
#[cfg(not(feature = "storage-v2"))]
pub use change::Change;
#[cfg(feature = "storage-v2")]
pub use change_v2::Change;
pub use error::AutomergeError;
pub use exid::ExId as ObjId;
pub use keys::Keys;
pub use keys_at::KeysAt;
pub use legacy::Change as ExpandedChange;
pub use types::{ActorId, ChangeHash, ObjType, OpType, Prop};
pub use value::{ScalarValue, Value};
pub use autoserde::AutoSerde;

pub const ROOT: ObjId = ObjId::Root;
