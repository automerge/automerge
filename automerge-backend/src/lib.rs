extern crate im_rc;
extern crate rand;
extern crate web_sys;

#[allow(unused_macros)]
macro_rules! log {
    ( $( $t:tt )* ) => {
        web_sys::console::log_1(&format!( $( $t )* ).into());
    }
}

mod actor_states;
mod backend;
mod concurrent_operations;
mod error;
mod helper;
mod object_store;
mod op_handle;
mod op_set;
mod ordered_set;
mod patch;
mod protocol;
mod serialize;
mod time;

pub use crate::patch::{Diff, DiffEdit, DiffLink, DiffValue, Patch};
pub use crate::protocol::{
    ActorID, Change, ChangeRequest, ChangeRequestType, Clock, DataType, Key, ObjType, ObjectID,
    OpID, OpType, Operation, PrimitiveValue,
};
pub use backend::Backend;
pub use error::AutomergeError;
