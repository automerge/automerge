use crate::error::AutomergeError;
use crate::protocol::ActorID;
use core::fmt::Debug;
use leb128;
use std::convert::TryFrom;
use std::io::Read;
use std::str;

fn err(s: &str) -> AutomergeError {
    AutomergeError::ChangeDecompressError(s.to_string())
}

#[derive(Clone)]
pub(crate) struct Decoder<'a> {
    pub offset: usize,
    buf: &'a [u8],
}

impl<'a> Decoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Decoder { offset: 0, buf }
    }

    pub fn iter<T>(&self) -> T
    where
        T: From<&'a [u8]>,
    {
        T::from(&self.buf[..])
    }

    pub fn read<T: Decodable + Debug>(&mut self, name: &str) -> Result<T, AutomergeError> {
        let mut new_buf = &self.buf[..];
        let val = T::decode::<&[u8]>(&mut new_buf).ok_or_else(|| err(name))?;
        let delta = self.buf.len() - new_buf.len();
        if delta == 0 {
            Err(err("buffer size didnt change..."))
        } else {
            self.buf = new_buf;
            self.offset += delta;
            Ok(val)
        }
    }

    pub fn read_bytes(&mut self, index: usize, name: &str) -> Result<&'a [u8], AutomergeError> {
        let buf = &self.buf[..];
        if buf.len() < index {
            Err(err(name))
        } else {
            let head = &buf[0..index];
            self.buf = &buf[index..];
            self.offset += index;
            Ok(head)
        }
    }

    pub fn done(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn rest(self) -> &'a [u8] {
        &self.buf[..]
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
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read;
}

impl Decodable for u8 {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        let mut buffer = [0; 1];
        bytes.read_exact(&mut buffer).ok()?;
        Some(buffer[0])
    }
}

impl Decodable for u32 {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        u64::decode::<R>(bytes).and_then(|val| Self::try_from(val).ok())
    }
}

impl Decodable for usize {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        u64::decode::<R>(bytes).and_then(|val| Self::try_from(val).ok())
    }
}

impl Decodable for isize {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        i64::decode::<R>(bytes).and_then(|val| Self::try_from(val).ok())
    }
}

impl Decodable for i32 {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        i64::decode::<R>(bytes).and_then(|val| Self::try_from(val).ok())
    }
}

impl Decodable for i64 {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        leb128::read::signed(bytes).ok()
    }
}

impl Decodable for f64 {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        let mut buffer = [0; 8];
        bytes.read_exact(&mut buffer).ok()?;
        Some(Self::from_le_bytes(buffer))
    }
}

impl Decodable for f32 {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        let mut buffer = [0; 4];
        bytes.read_exact(&mut buffer).ok()?;
        Some(Self::from_le_bytes(buffer))
    }
}

impl Decodable for u64 {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        leb128::read::unsigned(bytes).ok()
    }
}

impl Decodable for String {
    fn decode<R>(bytes: &mut R) -> Option<String>
    where
        R: Read,
    {
        let len = usize::decode::<R>(bytes)?;
        if len == 0 {
            return Some("".into());
        }
        let mut string = vec![0; len];
        bytes.read_exact(string.as_mut_slice()).ok()?;
        str::from_utf8(&string).map(|t| t.into()).ok()
    }
}

impl Decodable for Option<String> {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        let len = usize::decode::<R>(bytes)?;
        if len == 0 {
            return Some(None);
        }
        let mut string = vec![0; len];
        bytes.read_exact(string.as_mut_slice()).ok()?;
        Some(str::from_utf8(&string).map(|s| s.into()).ok())
    }
}

impl Decodable for ActorID {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        let s = String::decode::<R>(bytes)?;
        Some(ActorID(s))
    }
}
