//use std::error::Error;
use std::fmt::Debug;

use automerge_protocol as amp;
use thiserror::Error;

use crate::{decoding, encoding};

#[derive(Error, Debug)]
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
    #[error("Encoding error {0}")]
    EncodingError(#[from] encoding::Error),
    #[error("Decoding error {0}")]
    DecodingError(#[from] decoding::Error),
    #[error("Attempted to create a cursor for opid {opid} which was not an element in a sequence")]
    InvalidCursor { opid: amp::OpId },
    #[error("A compressed chunk could not be decompressed")]
    BadCompressedChunk,
    #[error("Overflow would have ocurred")]
    Overflow,
}

#[derive(Error, Debug)]
#[error("Invalid element ID: {0}")]
pub struct InvalidElementId(pub String);
