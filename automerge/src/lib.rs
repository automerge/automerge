#[macro_export]
macro_rules! log {
     ( $( $t:tt )* ) => {
          {
            use $crate::__log;
            __log!( $( $t )* );
          }
     }
 }

#[cfg(target_family = "wasm")]
#[macro_export]
macro_rules! __log {
     ( $( $t:tt )* ) => {
         web_sys::console::log_1(&format!( $( $t )* ).into());
     }
 }

#[cfg(not(target_family = "wasm"))]
#[macro_export]
macro_rules! __log {
     ( $( $t:tt )* ) => {
         println!( $( $t )* );
     }
 }

mod automerge;
mod change;
mod clock;
mod columnar;
mod decoding;
mod encoding;
mod error;
mod exid;
mod indexed_cache;
mod legacy;
mod op_set;
mod op_tree;
mod query;
mod sync;
mod types;
mod value;
#[cfg(feature = "optree-visualisation")]
mod visualisation;

pub use crate::automerge::Automerge;
pub use change::{decode_change, Change};
pub use error::AutomergeError;
pub use exid::ExId as ObjId;
pub use legacy::Change as ExpandedChange;
pub use sync::{BloomFilter, SyncHave, SyncMessage, SyncState};
pub use types::{ActorId, ChangeHash, ObjType, OpType, Prop};
pub use value::{ScalarValue, Value};

pub const ROOT: ObjId = ObjId::Root;
