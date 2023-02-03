use crate::storage::load::Error as LoadError;
use crate::types::{ActorId, ScalarValue};
use crate::value::DataType;
use crate::{ChangeHash, ObjType};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AutomergeError {
    #[error(transparent)]
    ChangeGraph(#[from] crate::change_graph::MissingDep),
    #[error("failed to load compressed data: {0}")]
    Deflate(#[source] std::io::Error),
    #[error("duplicate seq {0} found for actor {1}")]
    DuplicateSeqNumber(u64, ActorId),
    #[error("key must not be an empty string")]
    EmptyStringKey,
    #[error("general failure")]
    Fail,
    #[error("invalid actor ID `{0}`")]
    InvalidActorId(String),
    #[error("invalid UTF-8 character at {0}")]
    InvalidCharacter(usize),
    #[error("invalid hash {0}")]
    InvalidHash(ChangeHash),
    #[error("index {0} is out of bounds")]
    InvalidIndex(usize),
    #[error("invalid obj id `{0}`")]
    InvalidObjId(String),
    #[error("invalid obj id format `{0}`")]
    InvalidObjIdFormat(String),
    #[error("invalid op for object of type `{0}`")]
    InvalidOp(ObjType),
    #[error("seq {0} is out of bounds")]
    InvalidSeq(u64),
    #[error("invalid type of value, expected `{expected}` but received `{unexpected}`")]
    InvalidValueType {
        expected: String,
        unexpected: String,
    },
    #[error(transparent)]
    Load(#[from] LoadError),
    #[error("increment operations must be against a counter value")]
    MissingCounter,
    #[error("hash {0} does not correspond to a change in this document")]
    MissingHash(ChangeHash),
    #[error("compressed chunk was not a change")]
    NonChangeCompressed,
    #[error("id was not an object id")]
    NotAnObject,
}

impl PartialEq for AutomergeError {
    fn eq(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

#[cfg(feature = "wasm")]
impl From<AutomergeError> for wasm_bindgen::JsValue {
    fn from(err: AutomergeError) -> Self {
        js_sys::Error::new(&std::format!("{}", err)).into()
    }
}

#[derive(Error, Debug)]
#[error("Invalid actor ID: {0}")]
pub struct InvalidActorId(pub String);

#[derive(Error, Debug, PartialEq)]
#[error("Invalid scalar value, expected {expected} but received {unexpected}")]
pub(crate) struct InvalidScalarValue {
    pub(crate) raw_value: ScalarValue,
    pub(crate) datatype: DataType,
    pub(crate) unexpected: String,
    pub(crate) expected: String,
}

#[derive(Error, Debug, Eq, PartialEq)]
#[error("Invalid change hash slice: {0:?}")]
pub struct InvalidChangeHashSlice(pub Vec<u8>);

#[derive(Error, Debug, Eq, PartialEq)]
#[error("Invalid object ID: {0}")]
pub struct InvalidObjectId(pub String);

#[derive(Error, Debug)]
#[error("Invalid element ID: {0}")]
pub struct InvalidElementId(pub String);

#[derive(Error, Debug)]
#[error("Invalid OpID: {0}")]
pub struct InvalidOpId(pub String);

#[derive(Error, Debug)]
pub(crate) enum InvalidOpType {
    #[error("unrecognized action index {0}")]
    UnknownAction(u64),
    #[error("non numeric argument for inc op")]
    NonNumericInc,
}
