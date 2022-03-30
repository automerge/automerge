use crate::types::{ActorId, ScalarValue};
use crate::value::DataType;
use crate::{decoding, encoding};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AutomergeError {
    #[error("invalid opid format `{0}`")]
    InvalidOpId(String),
    #[error("obj id not from this document `{0}`")]
    ForeignObjId(String),
    #[error("there was an encoding problem: {0}")]
    Encoding(#[from] encoding::Error),
    #[error("there was a decoding problem: {0}")]
    Decoding(#[from] decoding::Error),
    #[error("key must not be an empty string")]
    EmptyStringKey,
    #[error("invalid seq {0}")]
    InvalidSeq(u64),
    #[error("index {0} is out of bounds")]
    InvalidIndex(usize),
    #[error("duplicate seq {0} found for actor {1}")]
    DuplicateSeqNumber(u64, ActorId),
    #[error("generic automerge error")]
    Fail,
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
    pub raw_value: ScalarValue,
    pub datatype: DataType,
    pub unexpected: String,
    pub expected: String,
}

#[derive(Error, Debug, PartialEq)]
#[error("Invalid change hash slice: {0:?}")]
pub struct InvalidChangeHashSlice(pub Vec<u8>);

#[derive(Error, Debug, PartialEq)]
#[error("Invalid object ID: {0}")]
pub struct InvalidObjectId(pub String);

#[derive(Error, Debug)]
#[error("Invalid element ID: {0}")]
pub struct InvalidElementId(pub String);

#[derive(Error, Debug)]
#[error("Invalid OpID: {0}")]
pub struct InvalidOpId(pub String);
