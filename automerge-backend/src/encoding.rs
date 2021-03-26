use crate::error::AutomergeError;
use automerge_protocol as amp;
use core::fmt::Debug;
use std::convert::TryFrom;
use std::io;
use std::io::{Read, Write};
use std::mem;
use std::str;

fn err(_s: &str) -> AutomergeError {
    AutomergeError::EncodingError
}

#[derive(Clone, Debug)]
pub(crate) struct Decoder<'a> {
    pub offset: usize,
    pub last_read: usize,
    buf: &'a [u8],
}

impl<'a> Decoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Decoder {
            offset: 0,
            last_read: 0,
            buf,
        }
    }

    pub fn read<T: Decodable + Debug>(&mut self) -> Result<T, AutomergeError> {
        let mut new_buf = self.buf;
        let val = T::decode::<&[u8]>(&mut new_buf).ok_or(AutomergeError::EncodingError)?;
        let delta = self.buf.len() - new_buf.len();
        if delta == 0 {
            Err(err("buffer size didnt change..."))
        } else {
            self.buf = new_buf;
            self.last_read = delta;
            self.offset += delta;
            Ok(val)
        }
    }

    pub fn read_bytes(&mut self, index: usize) -> Result<&'a [u8], AutomergeError> {
        let buf = self.buf;
        if buf.len() < index {
            Err(AutomergeError::EncodingError)
        } else {
            let head = &buf[0..index];
            self.buf = &buf[index..];
            self.last_read = index;
            self.offset += index;
            Ok(head)
        }
    }

    pub fn done(&self) -> bool {
        self.buf.is_empty()
    }
}

pub(crate) struct BooleanEncoder {
    buf: Vec<u8>,
    last: bool,
    count: usize,
}

impl BooleanEncoder {
    pub fn new() -> BooleanEncoder {
        BooleanEncoder {
            buf: Vec::new(),
            last: false,
            count: 0,
        }
    }

    pub fn append(&mut self, value: bool) {
        if value == self.last {
            self.count += 1;
        } else {
            self.count.encode(&mut self.buf).ok();
            self.last = value;
            self.count = 1;
        }
    }

    pub fn finish(mut self, col: u32) -> ColData {
        if self.count > 0 {
            self.count.encode(&mut self.buf).ok();
        }
        ColData {
            col,
            data: self.buf,
        }
    }
}

pub(crate) struct BooleanDecoder<'a> {
    decoder: Decoder<'a>,
    last_value: bool,
    count: usize,
}

impl<'a> From<&'a [u8]> for Decoder<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        Decoder::new(bytes)
    }
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

// this is an endless iterator that returns false after input is exhausted
impl<'a> Iterator for BooleanDecoder<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<bool> {
        while self.count == 0 {
            if self.decoder.done() && self.count == 0 {
                return Some(false);
            }
            self.count = self.decoder.read().unwrap_or_default();
            self.last_value = !self.last_value;
        }
        self.count -= 1;
        Some(self.last_value)
    }
}

pub(crate) struct DeltaEncoder {
    rle: RleEncoder<i64>,
    absolute_value: u64,
}

impl DeltaEncoder {
    pub fn new() -> DeltaEncoder {
        DeltaEncoder {
            rle: RleEncoder::new(),
            absolute_value: 0,
        }
    }

    pub fn append_value(&mut self, value: u64) {
        self.rle
            .append_value(value as i64 - self.absolute_value as i64);
        self.absolute_value = value;
    }

    pub fn append_null(&mut self) {
        self.rle.append_null();
    }

    pub fn finish(self, col: u32) -> ColData {
        self.rle.finish(col)
    }
}

enum RleState<T> {
    Empty,
    NullRun(usize),
    LiteralRun(T, Vec<T>),
    LoneVal(T),
    Run(T, usize),
}

pub(crate) struct RleEncoder<T>
where
    T: Encodable + PartialEq + Clone,
{
    buf: Vec<u8>,
    state: RleState<T>,
}

impl<T> RleEncoder<T>
where
    T: Encodable + PartialEq + Clone,
{
    pub fn new() -> RleEncoder<T> {
        RleEncoder {
            buf: Vec::new(),
            state: RleState::Empty,
        }
    }

    pub fn finish(mut self, col: u32) -> ColData {
        match self.take_state() {
            // this coveres `only_nulls`
            RleState::NullRun(size) => {
                if !self.buf.is_empty() {
                    self.flush_null_run(size)
                }
            }
            RleState::LoneVal(value) => self.flush_lit_run(vec![value]),
            RleState::Run(value, len) => self.flush_run(value, len),
            RleState::LiteralRun(last, mut run) => {
                run.push(last);
                self.flush_lit_run(run);
            }
            RleState::Empty => {}
        }
        ColData {
            col,
            data: self.buf,
        }
    }

    fn flush_run(&mut self, val: T, len: usize) {
        self.encode(len as i64);
        self.encode(val);
    }

    fn flush_null_run(&mut self, len: usize) {
        self.encode::<i64>(0);
        self.encode(len);
    }

    fn flush_lit_run(&mut self, run: Vec<T>) {
        self.encode(-(run.len() as i64));
        for val in run {
            self.encode(val);
        }
    }

    fn take_state(&mut self) -> RleState<T> {
        let mut state = RleState::Empty;
        mem::swap(&mut self.state, &mut state);
        state
    }

    pub fn append_null(&mut self) {
        self.state = match self.take_state() {
            RleState::Empty => RleState::NullRun(1),
            RleState::NullRun(size) => RleState::NullRun(size + 1),
            RleState::LoneVal(other) => {
                self.flush_lit_run(vec![other]);
                RleState::NullRun(1)
            }
            RleState::Run(other, len) => {
                self.flush_run(other, len);
                RleState::NullRun(1)
            }
            RleState::LiteralRun(last, mut run) => {
                run.push(last);
                self.flush_lit_run(run);
                RleState::NullRun(1)
            }
        }
    }

    pub fn append_value(&mut self, value: T) {
        self.state = match self.take_state() {
            RleState::Empty => RleState::LoneVal(value),
            RleState::LoneVal(other) => {
                if other == value {
                    RleState::Run(value, 2)
                } else {
                    RleState::LiteralRun(value, vec![other])
                }
            }
            RleState::Run(other, len) => {
                if other == value {
                    RleState::Run(other, len + 1)
                } else {
                    self.flush_run(other, len);
                    RleState::LoneVal(value)
                }
            }
            RleState::LiteralRun(last, mut run) => {
                if last == value {
                    self.flush_lit_run(run);
                    RleState::Run(value, 2)
                } else {
                    run.push(last);
                    RleState::LiteralRun(value, run)
                }
            }
            RleState::NullRun(size) => {
                self.flush_null_run(size);
                RleState::LoneVal(value)
            }
        }
    }

    fn encode<V>(&mut self, val: V)
    where
        V: Encodable,
    {
        val.encode(&mut self.buf).ok();
    }
}

#[derive(Debug)]
pub(crate) struct RleDecoder<'a, T> {
    pub decoder: Decoder<'a>,
    last_value: Option<T>,
    count: isize,
    literal: bool,
}

impl<'a, T> From<&'a [u8]> for RleDecoder<'a, T> {
    fn from(bytes: &'a [u8]) -> Self {
        RleDecoder {
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
impl<'a, T> Iterator for RleDecoder<'a, T>
where
    T: Clone + Debug + Decodable,
{
    type Item = Option<T>;

    fn next(&mut self) -> Option<Option<T>> {
        while self.count == 0 {
            if self.decoder.done() {
                return Some(None);
            }
            match self.decoder.read() {
                Ok(count) if count > 0 => {
                    self.count = count;
                    self.last_value = self.decoder.read().ok();
                    self.literal = false;
                }
                Ok(count) if count < 0 => {
                    self.count = count.abs();
                    self.literal = true;
                }
                _ => {
                    // FIXME(jeffa5): handle usize > isize here somehow
                    self.count = self.decoder.read::<usize>().unwrap_or_default() as isize;
                    self.last_value = None;
                    self.literal = false;
                }
            }
        }
        self.count -= 1;
        if self.literal {
            Some(self.decoder.read().ok())
        } else {
            Some(self.last_value.clone())
        }
    }
}

pub(crate) struct DeltaDecoder<'a> {
    rle: RleDecoder<'a, i64>,
    absolute_val: u64,
}

impl<'a> From<&'a [u8]> for DeltaDecoder<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        DeltaDecoder {
            rle: RleDecoder {
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

impl Decodable for Vec<u8> {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        let len = usize::decode::<R>(bytes)?;
        if len == 0 {
            return Some(vec![]);
        }
        let mut buffer = vec![0; len];
        bytes.read_exact(buffer.as_mut_slice()).ok()?;
        Some(buffer)
    }
}
impl Decodable for String {
    fn decode<R>(bytes: &mut R) -> Option<String>
    where
        R: Read,
    {
        let buffer = Vec::decode(bytes)?;
        str::from_utf8(&buffer).map(|t| t.into()).ok()
    }
}

impl Decodable for Option<String> {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        let buffer = Vec::decode(bytes)?;
        if buffer.is_empty() {
            return Some(None);
        }
        Some(str::from_utf8(&buffer).map(|t| t.into()).ok())
    }
}

impl Decodable for amp::ActorId {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        let buffer = Vec::decode(bytes)?;
        Some(buffer.into())
    }
}

pub(crate) trait Encodable {
    fn encode_with_actors_to_vec(&self, actors: &mut Vec<amp::ActorId>) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.encode_with_actors(&mut buf, actors)?;
        Ok(buf)
    }

    fn encode_with_actors<R: Write>(
        &self,
        buf: &mut R,
        _actors: &mut Vec<amp::ActorId>,
    ) -> io::Result<usize> {
        self.encode(buf)
    }

    fn encode<R: Write>(&self, _buf: &mut R) -> io::Result<usize> {
        Ok(0)
    }
}

impl Encodable for String {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        let bytes = self.as_bytes();
        let head = bytes.len().encode(buf)?;
        buf.write_all(bytes)?;
        Ok(head + bytes.len())
    }
}

impl Encodable for Option<String> {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        if let Some(s) = self {
            s.encode(buf)
        } else {
            0.encode(buf)
        }
    }
}

impl Encodable for u64 {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        leb128::write::unsigned(buf, *self)
    }
}

impl Encodable for f64 {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        let bytes = self.to_le_bytes();
        buf.write_all(&bytes)?;
        Ok(bytes.len())
    }
}

impl Encodable for f32 {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        let bytes = self.to_le_bytes();
        buf.write_all(&bytes)?;
        Ok(bytes.len())
    }
}

impl Encodable for i64 {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        leb128::write::signed(buf, *self)
    }
}

impl Encodable for usize {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        (*self as u64).encode(buf)
    }
}

impl Encodable for u32 {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        (*self as u64).encode(buf)
    }
}

impl Encodable for i32 {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        (*self as i64).encode(buf)
    }
}

#[derive(Debug)]
pub(crate) struct ColData {
    pub col: u32,
    pub data: Vec<u8>,
}

impl ColData {
    pub fn encode_col_len<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        let mut len = 0;
        if !self.data.is_empty() {
            len += self.col.encode(buf)?;
            len += self.data.len().encode(buf)?;
        }
        Ok(len)
    }
}
