pub use automerge_backend::{AutomergeError as BackendError, Backend, Change};
pub use automerge_frontend::{
    value_ref, AutomergeFrontendError as FrontendError, Frontend, InvalidChangeRequest,
    InvalidPatch, LocalChange, MutableDocument, Path, Primitive, Value,
};
pub use automerge_protocol::{ChangeHash, MapType, ObjType, ScalarValue, SequenceType};

mod combined;

pub use combined::{Automerge, AutomergeError};
