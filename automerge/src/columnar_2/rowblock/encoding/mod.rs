mod raw;
use std::borrow::Borrow;

pub(crate) use raw::{RawEncoder, RawDecoder};
mod rle;
pub(crate) use rle::{RleEncoder, RleDecoder};
mod boolean;
pub(crate) use boolean::{BooleanDecoder, BooleanEncoder};
mod delta;
pub(crate) use delta::{DeltaDecoder, DeltaEncoder};
mod value;
pub(crate) use value::ValueDecoder;
pub(crate) mod generic;
pub(crate) use generic::{GenericColDecoder, SimpleColDecoder};
mod opid;
pub(crate) use opid::OpIdDecoder;
mod opid_list;
pub(crate) use opid_list::OpIdListDecoder;
mod obj_id;
pub(crate) use obj_id::ObjDecoder;
mod key;
pub(crate) use key::{Key, KeyDecoder};

#[cfg(test)]
pub(crate) mod properties;



pub(crate) trait Encodable {
    fn encode(&self, out: &mut Vec<u8>) -> usize;
}
mod encodable_impls;
pub(crate) use encodable_impls::RawBytes;

pub(crate) trait Decodable: Sized {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: std::io::Read;
}
mod decodable_impls;


#[derive(Clone, thiserror::Error, Debug)]
pub(crate) enum DecodeColumnError {
    #[error("unexpected null decoding column {0}")]
    UnexpectedNull(String),
    #[error("invalid value in column {column}: {description}")]
    InvalidValue{
        column: String,
        description: String,
    },
}
