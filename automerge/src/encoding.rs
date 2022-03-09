use core::fmt::Debug;
use std::{
    io,
    io::{Read, Write},
    mem,
    num::NonZeroU64,
};

use flate2::{bufread::DeflateEncoder, Compression};
use smol_str::SmolStr;

#[cfg(not(feature = "storage-v2"))]
use crate::columnar::COLUMN_TYPE_DEFLATE;
use crate::ActorId;

pub(crate) const DEFLATE_MIN_SIZE: usize = 256;
#[cfg(feature = "storage-v2")]
const COLUMN_TYPE_DEFLATE: u32 = 8;

/// The error type for encoding operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl PartialEq<Error> for Error {
    fn eq(&self, other: &Error) -> bool {
        match (self, other) {
            (Self::Io(error1), Self::Io(error2)) => error1.kind() == error2.kind(),
        }
    }
}

/// Encodes booleans by storing the count of the same value.
///
/// The sequence of numbers describes the count of false values on even indices (0-indexed) and the
/// count of true values on odd indices (0-indexed).
///
/// Counts are encoded as usize.
pub(crate) struct BooleanEncoder {
    buf: Vec<u8>,
    last: bool,
    count: usize,
}

impl BooleanEncoder {
    pub(crate) fn new() -> BooleanEncoder {
        BooleanEncoder {
            buf: Vec::new(),
            last: false,
            count: 0,
        }
    }

    pub(crate) fn append(&mut self, value: bool) {
        if value == self.last {
            self.count += 1;
        } else {
            self.count.encode(&mut self.buf).ok();
            self.last = value;
            self.count = 1;
        }
    }

    pub(crate) fn finish(mut self, col: u32) -> ColData {
        if self.count > 0 {
            self.count.encode(&mut self.buf).ok();
        }
        ColData::new(col, self.buf)
    }
}

/// Encodes integers as the change since the previous value.
///
/// The initial value is 0 encoded as u64. Deltas are encoded as i64.
///
/// Run length encoding is then applied to the resulting sequence.
pub(crate) struct DeltaEncoder {
    rle: RleEncoder<i64>,
    absolute_value: u64,
}

impl DeltaEncoder {
    pub(crate) fn new() -> DeltaEncoder {
        DeltaEncoder {
            rle: RleEncoder::new(),
            absolute_value: 0,
        }
    }

    pub(crate) fn append_value(&mut self, value: u64) {
        self.rle
            .append_value(value as i64 - self.absolute_value as i64);
        self.absolute_value = value;
    }

    pub(crate) fn append_null(&mut self) {
        self.rle.append_null();
    }

    pub(crate) fn finish(self, col: u32) -> ColData {
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

/// Encodes data in run lengh encoding format. This is very efficient for long repeats of data
///
/// There are 3 types of 'run' in this encoder:
/// - a normal run (compresses repeated values)
/// - a null run (compresses repeated nulls)
/// - a literal run (no compression)
///
/// A normal run consists of the length of the run (encoded as an i64) followed by the encoded value that this run contains.
///
/// A null run consists of a zero value (encoded as an i64) followed by the length of the null run (encoded as a usize).
///
/// A literal run consists of the **negative** length of the run (encoded as an i64) followed by the values in the run.
///
/// Therefore all the types start with an encoded i64, the value of which determines the type of the following data.
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
    pub(crate) fn new() -> RleEncoder<T> {
        RleEncoder {
            buf: Vec::new(),
            state: RleState::Empty,
        }
    }

    pub(crate) fn finish(mut self, col: u32) -> ColData {
        match self.take_state() {
            // this covers `only_nulls`
            RleState::NullRun(size) => {
                if !self.buf.is_empty() {
                    self.flush_null_run(size);
                }
            }
            RleState::LoneVal(value) => self.flush_lit_run(vec![value]),
            RleState::Run(value, len) => self.flush_run(&value, len),
            RleState::LiteralRun(last, mut run) => {
                run.push(last);
                self.flush_lit_run(run);
            }
            RleState::Empty => {}
        }
        ColData::new(col, self.buf)
    }

    fn flush_run(&mut self, val: &T, len: usize) {
        self.encode(&(len as i64));
        self.encode(val);
    }

    fn flush_null_run(&mut self, len: usize) {
        self.encode::<i64>(&0);
        self.encode(&len);
    }

    fn flush_lit_run(&mut self, run: Vec<T>) {
        self.encode(&-(run.len() as i64));
        for val in run {
            self.encode(&val);
        }
    }

    fn take_state(&mut self) -> RleState<T> {
        let mut state = RleState::Empty;
        mem::swap(&mut self.state, &mut state);
        state
    }

    pub(crate) fn append_null(&mut self) {
        self.state = match self.take_state() {
            RleState::Empty => RleState::NullRun(1),
            RleState::NullRun(size) => RleState::NullRun(size + 1),
            RleState::LoneVal(other) => {
                self.flush_lit_run(vec![other]);
                RleState::NullRun(1)
            }
            RleState::Run(other, len) => {
                self.flush_run(&other, len);
                RleState::NullRun(1)
            }
            RleState::LiteralRun(last, mut run) => {
                run.push(last);
                self.flush_lit_run(run);
                RleState::NullRun(1)
            }
        }
    }

    pub(crate) fn append_value(&mut self, value: T) {
        self.state = match self.take_state() {
            RleState::Empty => RleState::LoneVal(value),
            RleState::LoneVal(other) => {
                if other == value {
                    RleState::Run(value, 2)
                } else {
                    let mut v = Vec::with_capacity(2);
                    v.push(other);
                    RleState::LiteralRun(value, v)
                }
            }
            RleState::Run(other, len) => {
                if other == value {
                    RleState::Run(other, len + 1)
                } else {
                    self.flush_run(&other, len);
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

    fn encode<V>(&mut self, val: &V)
    where
        V: Encodable,
    {
        val.encode(&mut self.buf).ok();
    }
}

pub(crate) trait Encodable {
    fn encode_with_actors_to_vec(&self, actors: &mut [ActorId]) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.encode_with_actors(&mut buf, actors)?;
        Ok(buf)
    }

    fn encode_with_actors<R: Write>(&self, buf: &mut R, _actors: &[ActorId]) -> io::Result<usize> {
        self.encode(buf)
    }

    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize>;

    fn encode_vec(&self, buf: &mut Vec<u8>) -> usize {
        self.encode(buf).unwrap()
    }
}

impl Encodable for SmolStr {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        let bytes = self.as_bytes();
        let head = bytes.len().encode(buf)?;
        buf.write_all(bytes)?;
        Ok(head + bytes.len())
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

impl Encodable for NonZeroU64 {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        leb128::write::unsigned(buf, self.get())
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
        u64::from(*self).encode(buf)
    }
}

impl Encodable for i32 {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        i64::from(*self).encode(buf)
    }
}

impl Encodable for [ActorId] {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        let mut len = self.len().encode(buf)?;
        for i in self {
            len += i.to_bytes().encode(buf)?;
        }
        Ok(len)
    }
}

fn actor_index(actor: &ActorId, actors: &[ActorId]) -> usize {
    actors.iter().position(|a| a == actor).unwrap()
}

impl Encodable for ActorId {
    fn encode_with_actors<R: Write>(&self, buf: &mut R, actors: &[ActorId]) -> io::Result<usize> {
        actor_index(self, actors).encode(buf)
    }

    fn encode<R: Write>(&self, _buf: &mut R) -> io::Result<usize> {
        // we instead encode actors as their position on a sequence
        Ok(0)
    }
}

impl Encodable for Vec<u8> {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        self.as_slice().encode(buf)
    }
}

impl Encodable for &[u8] {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        let head = self.len().encode(buf)?;
        buf.write_all(self)?;
        Ok(head + self.len())
    }
}

#[derive(Debug)]
pub(crate) struct ColData {
    pub(crate) col: u32,
    pub(crate) data: Vec<u8>,
    #[cfg(debug_assertions)]
    has_been_deflated: bool,
}

impl ColData {
    pub(crate) fn new(col_id: u32, data: Vec<u8>) -> ColData {
        ColData {
            col: col_id,
            data,
            #[cfg(debug_assertions)]
            has_been_deflated: false,
        }
    }

    pub(crate) fn encode_col_len<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        let mut len = 0;
        if !self.data.is_empty() {
            len += self.col.encode(buf)?;
            len += self.data.len().encode(buf)?;
        }
        Ok(len)
    }

    pub(crate) fn deflate(&mut self) {
        #[cfg(debug_assertions)]
        {
            debug_assert!(!self.has_been_deflated);
            self.has_been_deflated = true;
        }
        if self.data.len() > DEFLATE_MIN_SIZE {
            let mut deflated = Vec::new();
            let mut deflater = DeflateEncoder::new(&self.data[..], Compression::default());
            //This unwrap should be okay as we're reading and writing to in memory buffers
            deflater.read_to_end(&mut deflated).unwrap();
            self.col |= COLUMN_TYPE_DEFLATE;
            self.data = deflated;
        }
    }
}
