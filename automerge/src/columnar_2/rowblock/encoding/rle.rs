use std::{
    borrow::{Borrow, Cow},
    fmt::Debug,
    ops::Range,
};

use super::{Encodable, Decodable, RawDecoder};

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
    pub fn new(output_buf: &'a mut Vec<u8>) -> RleEncoder<'a, T> {
        RleEncoder {
            buf: output_buf,
            written: 0,
            state: RleState::Empty,
        }
    }

    pub fn finish(mut self) -> usize {
        match self.take_state() {
            // this covers `only_nulls`
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

    pub fn append_null(&mut self) {
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

    pub fn append_value(&mut self, value: &T) {
        self.state = match self.take_state() {
            RleState::Empty => RleState::LoneVal(value.clone()),
            RleState::LoneVal(other) => {
                if &other == value {
                    RleState::Run(value.clone(), 2)
                } else {
                    let mut v = Vec::with_capacity(2);
                    v.push(other);
                    RleState::LiteralRun(value.clone(), v)
                }
            }
            RleState::Run(other, len) => {
                if &other == value {
                    RleState::Run(other, len + 1)
                } else {
                    self.flush_run(&other, len);
                    RleState::LoneVal(value.clone())
                }
            }
            RleState::LiteralRun(last, mut run) => {
                if &last == value {
                    self.flush_lit_run(run);
                    RleState::Run(value.clone(), 2)
                } else {
                    run.push(last);
                    RleState::LiteralRun(value.clone(), run)
                }
            }
            RleState::NullRun(size) => {
                self.flush_null_run(size);
                RleState::LoneVal(value.clone())
            }
        }
    }

    pub fn append(&mut self, value: Option<&T>) {
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
    pub decoder: RawDecoder<'a>,
    last_value: Option<T>,
    count: isize,
    literal: bool,
}

impl<'a, T> RleDecoder<'a, T> {
    fn empty() -> Self {
        RleDecoder{
            decoder: RawDecoder::from(&[] as &[u8]),
            last_value: None,
            count: 0,
            literal: false,
        }
    }

    pub(crate) fn done(&self) -> bool {
        self.decoder.done() && self.count == 0
    }
}

impl<'a, T: Clone + Debug + Encodable + Decodable + Eq> RleDecoder<'a, T> {

    pub(crate) fn encode<I>(items: I, out: &'a mut Vec<u8>) -> Range<usize>
    where
        I: Iterator<Item=T>
    {
        let mut empty = RleDecoder::empty();
        let range = empty.splice(0..0, items.map(Some), out);
        range
    }

    pub(crate) fn splice<I: Iterator<Item=Option<TB>>, TB: Borrow<T>>(&mut self, replace: Range<usize>, mut replace_with: I, out: &mut Vec<u8>) -> Range<usize> {
        let start = out.len();
        let mut encoder = RleEncoder::new(out);
        let mut idx = 0;
        while idx < replace.start {
            match self.next() {
                Some(elem) => encoder.append(elem.as_ref()),
                None => panic!("out of bounds"),
            }
            idx += 1;
        }
        for _ in 0..replace.len() {
            self.next();
            if let Some(next) = replace_with.next() {
                encoder.append(next.as_ref().map(|n| n.borrow()));
            }
        }
        while let Some(next) = replace_with.next() {
            encoder.append(next.as_ref().map(|n| n.borrow()));
        }
        while let Some(next) = self.next() {
            encoder.append(next.as_ref());
        }
        start..(start + encoder.finish())
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

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use super::*;
    use proptest::prelude::*;
    use super::super::properties::splice_scenario;

    #[test]
    fn rle_int_round_trip() {
        let vals = [1,1,2,2,3,2,3,1,3];
        let mut buf = Vec::with_capacity(vals.len() * 3);
        let mut encoder: RleEncoder<'_, u64> = RleEncoder::new(&mut buf);
        for val in vals {
            encoder.append_value(&val)
        }
        let total_slice_len = encoder.finish();
        let mut decoder: RleDecoder<'_, u64> = RleDecoder::from(Cow::Borrowed(&buf[0..total_slice_len]));
        let mut result = Vec::new();
        while let Some(Some(val)) = decoder.next() {
            result.push(val);
        }
        assert_eq!(result, vals);
    }

    #[test]
    fn rle_int_insert() {
        let vals = [1,1,2,2,3,2,3,1,3];
        let mut buf = Vec::with_capacity(vals.len() * 3);
        let mut encoder: RleEncoder<'_, u64> = RleEncoder::new(&mut buf);
        for i in 0..4 {
            encoder.append_value(&vals[i])
        }
        encoder.append_value(&5);
        for i in 4..vals.len() {
            encoder.append_value(&vals[i]);
        }
        let total_slice_len = encoder.finish();
        let mut decoder: RleDecoder<'_, u64> = RleDecoder::from(Cow::Borrowed(&buf[0..total_slice_len]));
        let mut result = Vec::new();
        while let Some(Some(val)) = decoder.next() {
            result.push(val);
        }
        let expected = [1,1,2,2,5,3,2,3,1,3];
        assert_eq!(result, expected);
    }

    fn encode<T: Clone + Encodable + PartialEq>(vals: &[Option<T>]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(vals.len() * 3);
        let mut encoder: RleEncoder<'_, T> = RleEncoder::new(&mut buf);
        for val in vals {
            encoder.append(val.as_ref())
        }
        encoder.finish();
        buf
    }

    fn decode<T: Clone + Decodable + Debug>(buf: Vec<u8>) -> Vec<Option<T>> {
        let decoder = RleDecoder::<'_, T>::from(&buf[..]);
        decoder.collect()
    }

    proptest!{
        #[test]
        fn splice_ints(scenario in splice_scenario(any::<Option<i32>>())) {
            let buf = encode(&scenario.initial_values);
            let mut decoder = RleDecoder::<'_, i32>::from(&buf[..]);
            let mut out = Vec::new();
            decoder.splice(scenario.replace_range.clone(), scenario.replacements.iter().cloned(), &mut out);
            let result = decode::<i32>(out);
            scenario.check(result)
        }

        #[test]
        fn splice_strings(scenario in splice_scenario(any::<Option<String>>())) {
            let buf = encode(&scenario.initial_values);
            let mut decoder = RleDecoder::<'_, String>::from(&buf[..]);
            let mut out = Vec::new();
            decoder.splice(scenario.replace_range.clone(), scenario.replacements.iter().cloned(), &mut out);
            let result = decode::<String>(out);
            scenario.check(result)
        }
    }
}
