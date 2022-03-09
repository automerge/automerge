use std::{borrow::Cow, fmt::Debug};

use super::{Decodable, Encodable};

#[derive(Clone, Debug)]
pub(crate) struct RawDecoder<'a> {
    pub offset: usize,
    pub last_read: usize,
    data: Cow<'a, [u8]>,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
    #[error("no decoded value")]
    NoDecodedValue,
    #[error("buffer size did not change")]
    BufferSizeDidNotChange,
    #[error("trying to read past end")]
    TryingToReadPastEnd,
}

impl<'a> RawDecoder<'a> {
    pub fn new(data: Cow<'a, [u8]>) -> Self {
        RawDecoder {
            offset: 0,
            last_read: 0,
            data,
        }
    }

    pub fn read<T: Decodable + Debug>(&mut self) -> Result<T, Error> {
        let mut buf = &self.data[self.offset..];
        let init_len = buf.len();
        let val = T::decode::<&[u8]>(&mut buf).ok_or(Error::NoDecodedValue)?;
        let delta = init_len - buf.len();
        if delta == 0 {
            Err(Error::BufferSizeDidNotChange)
        } else {
            self.last_read = delta;
            self.offset += delta;
            Ok(val)
        }
    }

    pub fn read_bytes(&mut self, index: usize) -> Result<&[u8], Error> {
        if self.offset + index > self.data.len() {
            Err(Error::TryingToReadPastEnd)
        } else {
            let head = &self.data[self.offset..self.offset + index];
            self.last_read = index;
            self.offset += index;
            Ok(head)
        }
    }

    pub fn done(&self) -> bool {
        self.offset >= self.data.len()
    }
}

impl<'a> From<&'a [u8]> for RawDecoder<'a> {
    fn from(d: &'a [u8]) -> Self {
        Cow::Borrowed(d).into()
    }
}

impl<'a> From<Cow<'a, [u8]>> for RawDecoder<'a> {
    fn from(d: Cow<'a, [u8]>) -> Self {
        RawDecoder::new(d)
    }
}


pub(crate) struct RawEncoder<'a> {
    written: usize,
    output: &'a mut Vec<u8>,
}

impl<'a> RawEncoder<'a> {
    pub(crate) fn append<I: Encodable>(&mut self, value: &I) -> usize {
        let written = value.encode(&mut self.output);
        self.written += written;
        written
    }

    fn finish(self) -> usize {
        self.written
    }
}

impl<'a> From<&'a mut Vec<u8>> for RawEncoder<'a> {
    fn from(output: &'a mut Vec<u8>) -> Self {
        RawEncoder{ written: 0, output }
    }
}

