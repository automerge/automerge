use std::{
    borrow::{Borrow, Cow},
    fmt::Debug,
};

use super::{Decodable, DecodeError, Encodable, Sink};

#[derive(Clone, Debug)]
pub(crate) struct RawDecoder<'a> {
    offset: usize,
    last_read: usize,
    data: Cow<'a, [u8]>,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
    #[error("buffer size did not change")]
    BufferSizeDidNotChange,
    #[error("trying to read past end")]
    TryingToReadPastEnd,
    #[error(transparent)]
    Decode(#[from] DecodeError),
}

impl<'a> RawDecoder<'a> {
    pub(crate) fn new(data: Cow<'a, [u8]>) -> Self {
        RawDecoder {
            offset: 0,
            last_read: 0,
            data,
        }
    }

    pub(crate) fn read<T: Decodable + Debug>(&mut self) -> Result<T, Error> {
        let mut buf = &self.data[self.offset..];
        let init_len = buf.len();
        let val = T::decode::<&[u8]>(&mut buf)?;
        let delta = init_len - buf.len();
        if delta == 0 {
            Err(Error::BufferSizeDidNotChange)
        } else {
            self.last_read = delta;
            self.offset += delta;
            Ok(val)
        }
    }

    pub(crate) fn read_bytes(&mut self, index: usize) -> Result<&[u8], Error> {
        if self.offset + index > self.data.len() {
            Err(Error::TryingToReadPastEnd)
        } else {
            let head = &self.data[self.offset..self.offset + index];
            self.last_read = index;
            self.offset += index;
            Ok(head)
        }
    }

    pub(crate) fn done(&self) -> bool {
        self.offset >= self.data.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data.is_empty()
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

pub(crate) struct RawEncoder<S> {
    written: usize,
    output: S,
}

impl<S: Sink> RawEncoder<S> {
    pub(crate) fn append<B: Borrow<I>, I: Encodable>(&mut self, value: B) -> usize {
        let written = value.borrow().encode(&mut self.output);
        self.written += written;
        written
    }

    pub(crate) fn finish(self) -> (S, usize) {
        (self.output, self.written)
    }
}

impl<S: Sink> From<S> for RawEncoder<S> {
    fn from(output: S) -> Self {
        RawEncoder { written: 0, output }
    }
}
