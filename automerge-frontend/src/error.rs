use std::{error::Error, fmt, num::NonZeroU64};

use automerge_protocol as amp;
use automerge_protocol::ObjectId;
use thiserror::Error;

use crate::{value::Value, Path};

#[derive(Debug, PartialEq)]
pub enum AutomergeFrontendError {
    InvalidChangeRequest,
    MissingObjectError(ObjectId),
    NoSuchPathError(Path),
    PathIsNotCounter,
    CannotOverwriteCounter,
    MismatchedSequenceNumber,
    InvalidActorIdString(String),
}

impl fmt::Display for AutomergeFrontendError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<automerge_protocol::error::InvalidActorId> for AutomergeFrontendError {
    fn from(e: automerge_protocol::error::InvalidActorId) -> AutomergeFrontendError {
        AutomergeFrontendError::InvalidActorIdString(e.0)
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

//TODO Most of these errors should have paths associated with them to make it
//easier to understand where things are going wrong
#[derive(Error, Debug, PartialEq)]
pub enum InvalidPatch {
    #[error("Mismatched sequence number, expected: {expected} but got {actual}")]
    MismatchedSequenceNumber {
        expected: NonZeroU64,
        actual: NonZeroU64,
    },
    #[error("Received a diff inserting a non text object in a text object. Target object id was {object_id}, diff was {diff:?}")]
    InsertNonTextInTextObject {
        object_id: ObjectId,
        diff: amp::Diff,
    },
    #[error(
        "Received a diff for a character in a text object which created more than one character"
    )]
    InsertMultipleCharsInTextChar,
    #[error("Received a diff which had multiple values for a key in a table. Table id was {table_id}, diff was {diff:?}")]
    ConflictsReceivedForTableKey { table_id: ObjectId, diff: amp::Diff },
    #[error("Patch contained a diff which expected object with ID {object_id:?} to be {patch_expected_type:?} but we think it is {actual_type:?}")]
    MismatchingObjectType {
        object_id: ObjectId,
        patch_expected_type: Option<amp::ObjType>,
        actual_type: Option<amp::ObjType>,
    },
    #[error("Patch referenced an object id {patch_expected_id:?} at a path where we ecpected {actual_id:?}")]
    MismatchingObjectIDs {
        patch_expected_id: Option<ObjectId>,
        actual_id: ObjectId,
    },
    #[error("Patch attempted to reference an index which did not exist for object {object_id}")]
    InvalidIndex { object_id: ObjectId, index: usize },
    #[error("The patch tried to create an object but specified no value for the new object")]
    DiffCreatedObjectWithNoValue,
    #[error("The patch contained a diff with a list edit which referenced the '_head' of a list, rather than a specific element ID")]
    DiffEditWithHeadElemId,
    #[error("Value diff containing cursor")]
    ValueDiffContainedCursor,
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
    #[error("attempted to insert something into a text object which is not a character, object: {object:?}")]
    InsertNonTextInTextObject { path: Path, object: Value },
    #[error("attmpted to delete root object")]
    CannotDeleteRootObject,
    #[error("Attempted to access a missing index")]
    MissingIndexError {
        #[from]
        source: MissingIndexError,
    },
}

#[derive(Error, Debug, PartialEq)]
#[error("Attempted to access index {missing_index} in a collection with max index: {size_of_collection}")]
pub struct MissingIndexError {
    pub missing_index: usize,
    pub size_of_collection: usize,
}
