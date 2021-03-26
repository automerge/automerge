use automerge_protocol as amp;
//use std::error::Error;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum AutomergeError {
    #[error("Missing object ID")]
    MissingObjectError,
    #[error("Missing index in op {0}")]
    MissingIndex(amp::OpId),
    #[error("Missing element ID: {0}")]
    MissingElement(amp::ObjectId, amp::ElementId),
    #[error("No path to object: {0}")]
    NoPathToObject(amp::ObjectId),
    #[error("Cant extract object: {0}")]
    CantExtractObject(amp::ObjectId),
    #[error("Skiplist error: {0}")]
    SkipListError(String),
    #[error("Index out of bounds: {0}")]
    IndexOutOfBounds(usize),
    #[error("Invalid op id: {0}")]
    InvalidOpId(String),
    #[error("Invalid object ID: {0}")]
    InvalidObjectId(String),
    #[error("Missing value")]
    MissingValue,
    #[error("Unknown error: {0}")]
    GeneralError(String),
    #[error("Missing number value")]
    MissingNumberValue,
    #[error("Unknown version: {0}")]
    UnknownVersion(u64),
    #[error("Duplicate change {0}")]
    DuplicateChange(String),
    #[error("Diverged state {0}")]
    DivergedState(String),
    #[error("Change decompression error: {0}")]
    ChangeDecompressError(String),
    #[error("Invalid seq {0}")]
    InvalidSeq(u64),
    #[error("Map key in seq")]
    MapKeyInSeq,
    #[error("Head to opid")]
    HeadToOpId,
    #[error("Doc format not implemented yet")]
    DocFormatUnimplemented,
    #[error("Divergent change {0}")]
    DivergentChange(String),
    #[error("Encode failed")]
    EncodeFailed,
    #[error("Decode failed")]
    DecodeFailed,
    #[error("Invalid change")]
    InvalidChange {
        #[from]
        source: InvalidChangeError,
    },
    #[error("Change bad format: {source}")]
    ChangeBadFormat {
        #[source]
        source: amp::error::InvalidChangeHashSlice,
    },
    #[error("Encoding error")]
    EncodingError,
    #[error("Attempted to create a cursor for opid {opid} which was not an element in a sequence")]
    InvalidCursor { opid: amp::OpId },
    #[error("Found mismatching checksum values, calculated {calculated:?} but found {found:?}")]
    InvalidChecksum { found: [u8; 4], calculated: [u8; 4] },
    #[error("A compressed chunk could not be decompressed")]
    BadCompressedChunk,
    #[error("A change contained compressed columns, which is not supported")]
    ChangeContainedCompressedColumns,
}

#[derive(Error, Debug)]
#[error("Invalid element ID: {0}")]
pub struct InvalidElementId(pub String);

impl From<leb128::read::Error> for AutomergeError {
    fn from(_err: leb128::read::Error) -> Self {
        AutomergeError::EncodingError
    }
}

impl From<std::io::Error> for AutomergeError {
    fn from(_err: std::io::Error) -> Self {
        AutomergeError::EncodingError
    }
}

#[derive(Error, Debug, PartialEq)]
pub enum InvalidChangeError {
    #[error("Change contained an operation with action 'set' which did not have a 'value'")]
    SetOpWithoutValue,
    #[error("Received an inc operation which had an invalid value, value was: {op_value:?}")]
    IncOperationWithInvalidValue { op_value: Option<amp::ScalarValue> },
    #[error("Change contained an invalid object id: {}", source.0)]
    InvalidObjectId {
        #[from]
        source: amp::error::InvalidObjectId,
    },
    #[error("Change contained an invalid hash: {:?}", source.0)]
    InvalidChangeHash {
        #[from]
        source: amp::error::InvalidChangeHashSlice,
    },
}
