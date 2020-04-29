use crate::patch::DiffKey;
use crate::protocol::{ObjectID, OpID, OpRequest};
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
    GetChildFailed(ObjectID, DiffKey),
    IndexOutOfBounds(usize),
    InvalidOpID(String),
    InvalidObjectID(String),
    NoRedo,
    NoUndo,
    MissingValue,
    GeneralError(String),
    MissingNumberValue(OpRequest),
    UnknownVersion(u64),
    DuplicateChange(String),
    DivergedState(String),
    ChangeDecompressError(String),
    MapKeyInSeq,
    DiffKeyToOpID,
    HeadToOpID,
    DivergentChange(String),
    EncodeFailed,
    DecodeFailed,
    InvalidChange,
    ChangeBadFormat,
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
