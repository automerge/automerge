use crate::mutation::Path;
use crate::value::Value;
use automerge_protocol as amp;
use automerge_protocol::ObjectID;
use std::error::Error;
use std::fmt;
use thiserror::Error;

#[derive(Debug, PartialEq)]
pub enum AutomergeFrontendError {
    InvalidChangeRequest,
    MissingObjectError(ObjectID),
    NoSuchPathError(Path),
    PathIsNotCounter,
    CannotOverwriteCounter,
    MismatchedSequenceNumber,
    InvalidActorIDString(String),
}

impl fmt::Display for AutomergeFrontendError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<automerge_protocol::error::InvalidActorID> for AutomergeFrontendError {
    fn from(e: automerge_protocol::error::InvalidActorID) -> AutomergeFrontendError {
        AutomergeFrontendError::InvalidActorIDString(e.0)
    }
}

impl Error for AutomergeFrontendError {}

#[derive(Debug, PartialEq)]
pub enum InvalidInitialStateError {
    InitialStateMustBeMap,
}

impl fmt::Display for InvalidInitialStateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for InvalidInitialStateError {}

#[derive(Error, Debug, PartialEq)]
pub enum InvalidPatch {
    #[error("Mismatched sequence number, expected: {expected} but got {actual}")]
    MismatchedSequenceNumber { expected: u64, actual: u64 },
    #[error("Received a diff inserting a non text object in a text object. Target object id was {object_id}, diff was {diff:?}")]
    InsertNonTextInTextObject {
        object_id: ObjectID,
        diff: amp::Diff,
    },
    #[error("Received a diff which had multiple values for a key in a table. Table id was {table_id}, diff was {diff:?}")]
    ConflictsReceivedForTableKey { table_id: ObjectID, diff: amp::Diff },
    #[error("Patch contained a diff which expected object with ID {object_id} to be {expected_type:?} but we think it is {actual_type:?}")]
    MismatchingObjectType {
        object_id: ObjectID,
        expected_type: amp::ObjType,
        actual_type: Option<amp::ObjType>,
    },
}

#[derive(Error, Debug, PartialEq)]
pub enum InvalidChangeRequest {
    #[error("attempted to set the value of {path:?}, which is not allowed because that value is a counter")]
    CannotOverwriteCounter { path: Path },
    #[error("attempted an operation on a path that does not exist: {path:?}")]
    NoSuchPathError { path: Path },
    #[error("attempted to set a non map object {value:?} as the root")]
    CannotSetNonMapObjectAsRoot { value: Value },
    #[error("attempted to increment an object which is not a counter at {path:?}")]
    IncrementForNonCounterObject { path: Path },
    #[error("attempted to insert using a path which does not end in an index: {path:?}")]
    InsertWithNonSequencePath { path: Path },
    #[error("attempted to insert into an object which is not a sequence at {path:?}")]
    InsertForNonSequenceObject { path: Path },
    #[error("attempted to insert past the end of a sequence, path was {path:?}, max length of sequence is {sequence_length}")]
    InsertPastEndOfSequence { path: Path, sequence_length: u64 },
}
