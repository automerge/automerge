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
mod op_set;
mod patch;
mod protocol;
mod time;
mod value;

pub use crate::protocol::{
    ActorID, Change, ChangeRequest, ChangeRequestType, Clock, DataType, ElementID, Key, ObjType,
    ObjectID, OpHandle, OpID, OpType, Operation, PrimitiveValue,
};
pub use actor_states::ActorStates;
pub use backend::Backend;
pub use concurrent_operations::ConcurrentOperations;
pub use error::AutomergeError;
pub use object_store::ObjState;
pub use op_set::{OpSet, Version};
pub use patch::{
    Conflict, Diff, Diff2, DiffAction, DiffEdit, ElementValue, MapType, Patch, PendingDiff,
    SequenceType,
};
pub use value::Value;
