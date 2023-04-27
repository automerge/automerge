pub(crate) mod raw;

pub(crate) use raw::{RawDecoder, RawEncoder};
mod rle;
pub(crate) use rle::{RleDecoder, RleEncoder};
mod boolean;
pub(crate) use boolean::{
    BooleanDecoder, BooleanEncoder, MaybeBooleanDecoder, MaybeBooleanEncoder,
};
mod delta;
pub(crate) use delta::{DeltaDecoder, DeltaEncoder};
pub(crate) mod leb128;

pub(crate) mod column_decoder;
pub(crate) use column_decoder::ColumnDecoder;

#[cfg(test)]
pub(crate) mod properties;

pub(crate) trait Sink {
    fn append(&mut self, bytes: &[u8]);
}

impl<'a> Sink for &'a mut Vec<u8> {
    fn append(&mut self, bytes: &[u8]) {
        self.extend(bytes)
    }
}

impl Sink for Vec<u8> {
    fn append(&mut self, bytes: &[u8]) {
        self.extend(bytes)
    }
}

pub(crate) trait Encodable {
    fn encode<S: Sink>(&self, out: &mut S) -> usize;
}

mod encodable_impls;
pub(crate) use encodable_impls::RawBytes;

#[derive(thiserror::Error, Debug)]
pub(crate) enum DecodeError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("invalid integer")]
    FromInt(#[from] std::num::TryFromIntError),
    #[error("bad leb128")]
    BadLeb(#[from] ::leb128::read::Error),
    #[error(transparent)]
    BadLeb128(#[from] crate::storage::parse::leb128::Error),
    #[error("attempted to allocate {attempted} which is larger than the maximum of {maximum}")]
    OverlargeAllocation { attempted: usize, maximum: usize },
    #[error("invalid string encoding")]
    BadString,
}

pub(crate) trait Decodable: Sized {
    fn decode<R>(bytes: &mut R) -> Result<Self, DecodeError>
    where
        R: std::io::Read;
}
mod decodable_impls;

pub(crate) mod col_error;
pub(crate) use col_error::DecodeColumnError;
