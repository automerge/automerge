use thiserror::Error;

#[derive(Error, Debug)]
#[error("Invalid OpID: {0}")]
pub struct InvalidOpID(pub String);

#[derive(Error, Debug)]
#[error("Invalid object ID: {0}")]
pub struct InvalidObjectID(pub String);

