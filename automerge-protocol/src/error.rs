use thiserror::Error;

#[derive(Error, Debug)]
#[error("Invalid OpID: {0}")]
pub struct InvalidOpID(pub String);
