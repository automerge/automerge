use automerge_protocol::{ObjectID, Op, OpID};
use std::error::Error;
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum AutomergeError {
    MissingObjectError(ObjectID),
    MissingIndex(OpID),
    MissingChildID(String),
    MissingElement(ObjectID, OpID),
    NoPathToObject(ObjectID),
    CantExtractObject(ObjectID),
    LinkMissingChild(OpID),
    SkipListError(String),
    IndexOutOfBounds(usize),
    InvalidOpID(String),
    InvalidObjectID(String),
    NoRedo,
    NoUndo,
    MissingValue,
    GeneralError(String),
    MissingNumberValue(Op),
    UnknownVersion(u64),
    DuplicateChange(String),
    DivergedState(String),
    ChangeDecompressError(String),
    MapKeyInSeq,
    HeadToOpID,
    DocFormatUnimplemented,
    DivergentChange(String),
    EncodeFailed,
    DecodeFailed,
    InvalidChange,
    ChangeBadFormat,
    EncodingError,
}

impl From<automerge_protocol::error::InvalidChangeHashSlice> for AutomergeError {
    fn from(_: automerge_protocol::error::InvalidChangeHashSlice) -> AutomergeError {
        AutomergeError::ChangeBadFormat
    }
}

impl fmt::Display for AutomergeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for AutomergeError {}

#[derive(Debug)]
pub struct InvalidElementID(pub String);

impl fmt::Display for InvalidElementID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for InvalidElementID {}

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
