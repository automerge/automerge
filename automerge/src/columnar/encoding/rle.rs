use std::{
    borrow::{Borrow, Cow},
    fmt::Debug,
};

use super::{raw, Decodable, Encodable, RawDecoder, Sink};

pub(crate) struct RleEncoder<S, T>
where
    T: Encodable + PartialEq + Clone,
{
    buf: S,
    written: usize,
    state: RleState<T>,
}

impl<S, T> RleEncoder<S, T>
where
    S: Sink,
    T: Encodable + PartialEq + Clone,
{
    pub(crate) fn new(output_buf: S) -> RleEncoder<S, T> {
        RleEncoder {
            buf: output_buf,
            written: 0,
            state: RleState::Empty,
        }
    }

    /// Flush the encoded values and return the output buffer and the number of bytes written
    pub(crate) fn finish(mut self) -> (S, usize) {
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
        (self.buf, self.written)
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
        self.written += val.encode(&mut self.buf);
    }
}

enum RleState<T> {
    Empty,
    // Note that this is different to a `NullRun` because if every element of a column is null
    // (i.e. the state when we call `finish` is `InitialNullRun`) then we don't output anything at
    // all for the column
    InitialNullRun(usize),
    NullRun(usize),
    LiteralRun(T, Vec<T>),
    LoneVal(T),
    Run(T, usize),
}

impl<S: Sink, T: Clone + PartialEq + Encodable> From<S> for RleEncoder<S, T> {
    fn from(output: S) -> Self {
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

    fn try_next(&mut self) -> Result<Option<Option<T>>, raw::Error>
    where
        T: Decodable + Clone + Debug,
    {
        while self.count == 0 {
            if self.decoder.done() {
                return Ok(None);
            }
            match self.decoder.read::<i64>()? {
                count if count > 0 => {
                    // normal run
                    self.count = count as isize;
                    self.last_value = Some(self.decoder.read()?);
                    self.literal = false;
                }
                count if count < 0 => {
                    // literal run
                    self.count = count.abs() as isize;
                    self.literal = true;
                }
                _ => {
                    // null run
                    // FIXME(jeffa5): handle usize > i64 here somehow
                    self.count = self.decoder.read::<usize>()? as isize;
                    self.last_value = None;
                    self.literal = false;
                }
            }
        }
        self.count -= 1;
        if self.literal {
            Ok(Some(Some(self.decoder.read()?)))
        } else {
            Ok(Some(self.last_value.clone()))
        }
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

impl<'a, T> Iterator for RleDecoder<'a, T>
where
    T: Clone + Debug + Decodable,
{
    type Item = Result<Option<T>, raw::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}
