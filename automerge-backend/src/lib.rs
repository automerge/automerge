mod error;
mod value;
mod actor_histories;
mod concurrent_operations;
mod operation_with_metadata;
mod object_store;
mod op_set;
mod patch;
mod patch_serialization;
mod protocol;
mod backend;

pub use crate::protocol::{
    ActorID, Change, Clock, DataType, Key, ObjectID, Operation, PrimitiveValue, ElementID
};
pub use actor_histories::ActorHistories;
pub use object_store::{ObjectHistory, ObjectStore};
pub use concurrent_operations::ConcurrentOperations;
pub use value::Value;
pub use op_set::{OpSet, list_ops_in_order};
pub use error::AutomergeError;
pub use operation_with_metadata::OperationWithMetadata; pub use patch::{Patch, Diff, DiffAction, Conflict, MapType, SequenceType, ElementValue};
pub use backend::Backend;

