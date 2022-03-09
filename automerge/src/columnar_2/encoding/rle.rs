use std::{
    borrow::{Borrow, Cow},
    fmt::Debug,
};

use super::{Decodable, Encodable, RawDecoder};

pub(crate) struct RleEncoder<'a, T>
where
    T: Encodable + PartialEq + Clone,
{
    buf: &'a mut Vec<u8>,
    written: usize,
    state: RleState<T>,
}

impl<'a, T> RleEncoder<'a, T>
where
    T: Encodable + PartialEq + Clone,
{
    pub(crate) fn new(output_buf: &'a mut Vec<u8>) -> RleEncoder<'a, T> {
        RleEncoder {
            buf: output_buf,
            written: 0,
            state: RleState::Empty,
        }
    }

    /// Flush the encoded values and return the number of bytes writted
    pub(crate) fn finish(mut self) -> usize {
        match self.take_state() {
            RleState::InitialNullRun(_size) => {}
            RleState::NullRun(size) => {
                self.flush_null_run(size);
            }
            RleState::LoneVal(value) => self.flush_lit_run(vec![value]),
            RleState::Run(value, len) => self.flush_run(&value, len),
            RleState::LiteralRun(last, mut run) => {
                run.push(last);
                self.flush_lit_run(run);
            }
            RleState::Empty => {}
        }
        self.written
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
        std::mem::swap(&mut self.state, &mut state);
        state
    }

    pub(crate) fn append_null(&mut self) {
        self.state = match self.take_state() {
            RleState::Empty => RleState::InitialNullRun(1),
            RleState::InitialNullRun(size) => RleState::InitialNullRun(size + 1),
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

    pub(crate) fn append_value<BT: Borrow<T>>(&mut self, value: BT) {
        self.state = match self.take_state() {
            RleState::Empty => RleState::LoneVal(value.borrow().clone()),
            RleState::LoneVal(other) => {
                if &other == value.borrow() {
                    RleState::Run(value.borrow().clone(), 2)
                } else {
                    let mut v = Vec::with_capacity(2);
                    v.push(other);
                    RleState::LiteralRun(value.borrow().clone(), v)
                }
            }
            RleState::Run(other, len) => {
                if &other == value.borrow() {
                    RleState::Run(other, len + 1)
                } else {
                    self.flush_run(&other, len);
                    RleState::LoneVal(value.borrow().clone())
                }
            }
            RleState::LiteralRun(last, mut run) => {
                if &last == value.borrow() {
                    self.flush_lit_run(run);
                    RleState::Run(value.borrow().clone(), 2)
                } else {
                    run.push(last);
                    RleState::LiteralRun(value.borrow().clone(), run)
                }
            }
            RleState::NullRun(size) | RleState::InitialNullRun(size) => {
                self.flush_null_run(size);
                RleState::LoneVal(value.borrow().clone())
            }
        }
    }

    pub(crate) fn append<BT: Borrow<T>>(&mut self, value: Option<BT>) {
        match value {
            Some(t) => self.append_value(t),
            None => self.append_null(),
        }
    }

    fn encode<V>(&mut self, val: &V)
    where
        V: Encodable,
    {
        self.written += val.encode(self.buf);
    }
}

enum RleState<T> {
    Empty,
    InitialNullRun(usize),
    NullRun(usize),
    LiteralRun(T, Vec<T>),
    LoneVal(T),
    Run(T, usize),
}

impl<'a, T: Clone + PartialEq + Encodable> From<&'a mut Vec<u8>> for RleEncoder<'a, T> {
    fn from(output: &'a mut Vec<u8>) -> Self {
        Self::new(output)
    }
}

/// See discussion on [`RleEncoder`] for the format data is stored in.
#[derive(Clone, Debug)]
pub(crate) struct RleDecoder<'a, T> {
    decoder: RawDecoder<'a>,
    last_value: Option<T>,
    count: isize,
    literal: bool,
}

impl<'a, T> RleDecoder<'a, T> {
    pub(crate) fn done(&self) -> bool {
        self.decoder.done() && self.count == 0
    }
}

impl<'a, T> From<Cow<'a, [u8]>> for RleDecoder<'a, T> {
    fn from(bytes: Cow<'a, [u8]>) -> Self {
        RleDecoder {
            decoder: RawDecoder::from(bytes),
            last_value: None,
            count: 0,
            literal: false,
        }
    }
}

impl<'a, T> From<&'a [u8]> for RleDecoder<'a, T> {
    fn from(d: &'a [u8]) -> Self {
        Cow::Borrowed(d).into()
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
                return None;
            }
            match self.decoder.read::<i64>() {
                Ok(count) if count > 0 => {
                    // normal run
                    self.count = count as isize;
                    self.last_value = self.decoder.read().ok();
                    self.literal = false;
                }
                Ok(count) if count < 0 => {
                    // literal run
                    self.count = count.abs() as isize;
                    self.literal = true;
                }
                Ok(_) => {
                    // null run
                    // FIXME(jeffa5): handle usize > i64 here somehow
                    self.count = self.decoder.read::<usize>().unwrap() as isize;
                    self.last_value = None;
                    self.literal = false;
                }
                Err(e) => {
                    tracing::warn!(error=?e, "error during rle decoding");
                    return None;
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
