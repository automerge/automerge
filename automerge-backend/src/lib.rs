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
mod object_store;
mod op_set;
mod operation_with_metadata;
mod patch;
mod protocol;
mod value;

pub use crate::protocol::{
    ActorID, Change, ChangeRequest, ChangeRequestType, Clock, DataType, ElementID, Key, ObjType,
    OpID, Operation, PrimitiveValue,
};
pub use actor_states::ActorStates;
pub use backend::Backend;
pub use concurrent_operations::ConcurrentOperations;
pub use error::AutomergeError;
pub use object_store::ObjState;
pub use op_set::OpSet;
pub use operation_with_metadata::OperationWithMetadata;
pub use patch::{
    Conflict, Diff, Diff2, DiffAction, ElementValue, MapType, Patch, PendingDiff, SequenceType,
};
pub use value::Value;
