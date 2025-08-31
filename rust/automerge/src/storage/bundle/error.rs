use crate::op_set2::ReadOpError;
use crate::storage::columns::raw_column;
use crate::storage::{chunk, parse};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ParseError {
    #[error("invalid end of input")]
    Needed,
    #[error("compression error in change columns")]
    CompressedChangeCols,
    #[error("compression error in op columns")]
    CompressedOpCols,
    #[error("invalid change cols: {0}")]
    InvalidColumns(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("failed to parse header: {0}")]
    Header(#[from] chunk::error::Header),
    #[error("invalid change column")]
    InvalidChangeColumn(u32),
    #[error("invalid op column")]
    InvalidOpColumn(u32),
    #[error(transparent)]
    ParseColumns(#[from] raw_column::ParseError),
    #[error(transparent)]
    Leb128(#[from] parse::leb128::Error),
    #[error(transparent)]
    ReadOp(#[from] ReadOpError),
    #[error(transparent)]
    Pack(#[from] hexane::PackError),
    #[error(transparent)]
    Deflate(#[from] std::io::Error),
    #[error("failed to unbundle: {0}")]
    Unbundle(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl<E: Into<ParseError>> From<parse::ParseError<E>> for ParseError {
    fn from(e: parse::ParseError<E>) -> ParseError {
        match e {
            parse::ParseError::Error(e) => e.into(),
            parse::ParseError::Incomplete(_) => ParseError::Needed,
        }
    }
}
