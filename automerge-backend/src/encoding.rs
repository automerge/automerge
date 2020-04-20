use crate::error::AutomergeError;
use crate::protocol::ActorID;
use core::fmt::Debug;
use leb128;
use std::convert::TryFrom;
use std::str;

fn err(s: &str) -> AutomergeError {
    AutomergeError::ChangeDecompressError(s.to_string())
}

#[derive(Clone)]
pub(crate) struct Decoder<'a> {
    pub offset: usize,
    pub buf: &'a [u8],
}

impl<'a> Decoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Decoder { offset: 0, buf }
    }

    pub fn iter<T>(&self) -> T
    where
        T: From<&'a [u8]>,
    {
        T::from(&self.buf[self.offset..])
    }

    pub fn read<T: Decodable + Debug>(&mut self, name: &str) -> Result<T, AutomergeError> {
        let (val, offset) = T::decode(&self.buf[self.offset..]).ok_or_else(|| err(name))?;
        self.offset += offset;
        //log!("read {:?}={:?}", name, val);
        Ok(val)
    }

    pub fn read_bytes(&mut self, index: usize, name: &str) -> Result<&'a [u8], AutomergeError> {
        let buf = &self.buf[self.offset..];
        if buf.len() < index {
            Err(err(name))
        } else {
            let head = &buf[0..index];
            //log!("read_bytes {:?}={:?}", name, head);
            self.offset += index;
            Ok(head)
        }
    }

    pub fn done(&self) -> bool {
        self.buf.len() == self.offset
    }

    pub fn rest(self) -> &'a [u8] {
        &self.buf[self.offset..]
    }
}

pub(crate) struct BooleanDecoder<'a> {
    decoder: Decoder<'a>,
    last_value: bool,
    count: usize,
}

impl<'a> From<&'a [u8]> for BooleanDecoder<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        BooleanDecoder {
            decoder: Decoder::new(bytes),
            last_value: true,
            count: 0,
        }
    }
}

// this is an endless iterator that returns a bunch all falses after finishing the buffer

impl<'a> Iterator for BooleanDecoder<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<bool> {
        while self.count == 0 {
            if self.decoder.done() && self.count == 0 {
                return Some(false);
            }
            self.count = self.decoder.read("bool_count").unwrap_or_default();
            self.last_value = !self.last_value;
        }
        self.count -= 1;
        Some(self.last_value)
    }
}

pub(crate) struct RLEDecoder<'a, T> {
    pub decoder: Decoder<'a>,
    last_value: Option<T>,
    count: isize,
    literal: bool,
}

impl<'a, T> From<&'a [u8]> for RLEDecoder<'a, T> {
    fn from(bytes: &'a [u8]) -> Self {
        RLEDecoder {
            decoder: Decoder::new(bytes),
            last_value: None,
            count: 0,
            literal: false,
        }
    }
}

// this decoder needs to be able to send type T or 'null'
// it is an endless iterator that will return all 'null's
// once input is exhausted
impl<'a, T> Iterator for RLEDecoder<'a, T>
where
    T: Clone + Debug + Decodable,
{
    type Item = Option<T>;

    fn next(&mut self) -> Option<Option<T>> {
        while self.count == 0 {
            if self.decoder.done() {
                return Some(None);
            }
            match self.decoder.read("RLE_count") {
                Ok(count) if count > 0 => {
                    self.count = count;
                    self.last_value = self.decoder.read("RLE_val").ok();
                    self.literal = false;
                }
                Ok(count) if count < 0 => {
                    self.count = count.abs();
                    self.literal = true;
                }
                _ => {
                    self.count = self.decoder.read("RLE_count2").unwrap_or_default();
                    self.last_value = None;
                    self.literal = false;
                }
            }
        }
        self.count -= 1;
        if self.literal {
            Some(self.decoder.read("RLE_literal").ok())
        } else {
            Some(self.last_value.clone())
        }
    }
}

pub(crate) struct DeltaDecoder<'a> {
    rle: RLEDecoder<'a, i64>,
    absolute_val: u64,
}

impl<'a> From<&'a [u8]> for DeltaDecoder<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        DeltaDecoder {
            rle: RLEDecoder {
                decoder: Decoder::new(bytes),
                last_value: None,
                count: 0,
                literal: false,
            },
            absolute_val: 0,
        }
    }
}

impl<'a> Iterator for DeltaDecoder<'a> {
    type Item = Option<u64>;

    fn next(&mut self) -> Option<Option<u64>> {
        if let Some(delta) = self.rle.next()? {
            if delta < 0 {
                self.absolute_val -= delta.abs() as u64;
            } else {
                self.absolute_val += delta as u64;
            }
            Some(Some(self.absolute_val))
        } else {
            Some(None)
        }
    }
}

pub(crate) trait Decodable: Sized {
    fn decode(bytes: &[u8]) -> Option<(Self, usize)>;
}

impl Decodable for u8 {
    fn decode(bytes: &[u8]) -> Option<(Self, usize)> {
        bytes.first().map(|b| (*b, 1))
    }
}

impl Decodable for u32 {
    fn decode(bytes: &[u8]) -> Option<(Self, usize)> {
        let (val, size) = u64::decode(bytes)?;
        Some((Self::try_from(val).ok()?, size))
    }
}

impl Decodable for usize {
    fn decode(bytes: &[u8]) -> Option<(Self, usize)> {
        let (val, size) = u64::decode(bytes)?;
        Some((Self::try_from(val).ok()?, size))
    }
}

impl Decodable for isize {
    fn decode(bytes: &[u8]) -> Option<(Self, usize)> {
        let (val, size) = i64::decode(bytes)?;
        Some((Self::try_from(val).ok()?, size))
    }
}

impl Decodable for i32 {
    fn decode(bytes: &[u8]) -> Option<(Self, usize)> {
        let (val, size) = i64::decode(bytes)?;
        Some((Self::try_from(val).ok()?, size))
    }
}

impl Decodable for i64 {
    fn decode(bytes: &[u8]) -> Option<(Self, usize)> {
        let mut readable = &bytes[..];
        leb128::read::signed(&mut readable)
            .ok()
            .map(|val| (val, bytes.len() - readable.len()))
    }
}

impl Decodable for f64 {
    fn decode(bytes: &[u8]) -> Option<(Self, usize)> {
        match &bytes {
            [a0, a1, a2, a3, a4, a5, a6, a7, ..] => Some((
                Self::from_le_bytes([*a0, *a1, *a2, *a3, *a4, *a5, *a6, *a7]),
                8,
            )),
            _ => None,
        }
    }
}

impl Decodable for f32 {
    fn decode(bytes: &[u8]) -> Option<(Self, usize)> {
        match &bytes {
            [a0, a1, a2, a3, ..] => Some((Self::from_le_bytes([*a0, *a1, *a2, *a3]), 4)),
            _ => None,
        }
    }
}

impl Decodable for u64 {
    fn decode(bytes: &[u8]) -> Option<(Self, usize)> {
        let mut readable = &bytes[..];
        leb128::read::unsigned(&mut readable)
            .ok()
            .map(|val| (val, bytes.len() - readable.len()))
    }
}

impl Decodable for String {
    fn decode(bytes: &[u8]) -> Option<(String, usize)> {
        let (len, offset) = usize::decode(bytes)?;
        let size = offset + len;
        bytes
            .get(offset..size)
            .and_then(|data| str::from_utf8(&data).ok())
            .map(|s| (s.to_string(), size))
    }
}

impl Decodable for Option<String> {
    fn decode(bytes: &[u8]) -> Option<(Self, usize)> {
        let (len, offset) = usize::decode(bytes)?;
        if len == 0 {
            return Some((None, offset));
        }
        let size = offset + len;
        bytes
            .get(offset..size)
            .and_then(|data| str::from_utf8(&data).ok())
            .map(|s| (Some(s.to_string()), size))
    }
}

impl Decodable for ActorID {
    fn decode(bytes: &[u8]) -> Option<(Self, usize)> {
        let (s, offset) = String::decode(bytes)?;
        Some((ActorID(s), offset))
    }
}
