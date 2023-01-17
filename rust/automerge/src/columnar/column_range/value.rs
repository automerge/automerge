use std::{borrow::Cow, ops::Range};

use crate::{
    columnar::{
        encoding::{
            leb128::{lebsize, ulebsize},
            raw, DecodeColumnError, DecodeError, RawBytes, RawDecoder, RawEncoder, RleDecoder,
            RleEncoder, Sink,
        },
        SpliceError,
    },
    storage::parse::{
        leb128::{leb128_i64, leb128_u64},
        Input, ParseResult,
    },
    ScalarValue,
};

use super::{RawRange, RleRange};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ValueRange {
    meta: RleRange<u64>,
    raw: RawRange,
}

impl ValueRange {
    pub(crate) fn new(meta: RleRange<u64>, raw: RawRange) -> Self {
        Self { meta, raw }
    }

    pub(crate) fn range(&self) -> Range<usize> {
        // This is a hack, instead `raw` should be `Option<RawRange>`
        if self.raw.is_empty() {
            self.meta.clone().into()
        } else {
            self.meta.start()..self.raw.end()
        }
    }

    pub(crate) fn meta_range(&self) -> &RleRange<u64> {
        &self.meta
    }

    pub(crate) fn raw_range(&self) -> &RawRange {
        &self.raw
    }

    pub(crate) fn encode<'a, 'b, I>(items: I, out: &'b mut Vec<u8>) -> Self
    where
        I: Iterator<Item = Cow<'a, ScalarValue>> + Clone + 'a,
    {
        Self {
            meta: (0..0).into(),
            raw: (0..0).into(),
        }
        .splice(&[], 0..0, items, out)
    }

    pub(crate) fn iter<'a>(&self, data: &'a [u8]) -> ValueIter<'a> {
        ValueIter {
            meta: self.meta.decoder(data),
            raw: self.raw.decoder(data),
        }
    }

    pub(crate) fn splice<'b, I>(
        &self,
        data: &[u8],
        replace: Range<usize>,
        replace_with: I,
        out: &mut Vec<u8>,
    ) -> Self
    where
        I: Iterator<Item = Cow<'b, ScalarValue>> + Clone,
    {
        // SAFETY: try_splice fails if either the iterator of replacements fails, or the iterator
        //         of existing elements fails. But the replacement iterator is infallible and there
        //         are no existing elements
        self.try_splice::<_, ()>(data, replace, replace_with.map(Ok), out)
            .unwrap()
    }

    pub(crate) fn try_splice<'b, I, E>(
        &self,
        data: &[u8],
        replace: Range<usize>,
        mut replace_with: I,
        out: &mut Vec<u8>,
    ) -> Result<Self, SpliceError<raw::Error, E>>
    where
        I: Iterator<Item = Result<Cow<'b, ScalarValue>, E>> + Clone,
    {
        // Our semantics here are similar to those of Vec::splice. We can describe this
        // imperatively like this:
        //
        // * First copy everything up to the start of `replace` into the output
        // * For every index in `replace` skip that index from ourselves and if `replace_with`
        //   returns `Some` then copy that value to the output
        // * Once we have iterated past `replace.end` we continue to call `replace_with` until it
        //   returns None, copying the results to the output
        // * Finally we copy the remainder of our data into the output
        //
        // However, things are complicated by the fact that our data is stored in two columns. This
        // means that we do this in two passes. First we execute the above logic for the metadata
        // column. Then we do it all over again for the value column.

        // First pass - metadata
        //
        // Copy the metadata decoder so we can iterate over it again when we read the values in the
        // second pass
        let start = out.len();
        let mut meta_copy = self.meta.decoder(data);
        let mut meta_out = RleEncoder::<_, u64>::from(&mut *out);
        let mut idx = 0;
        // Copy everything up to replace.start to the output
        while idx < replace.start {
            let val = meta_copy
                .next()
                .transpose()
                .map_err(SpliceError::ReadExisting)?
                .unwrap_or(None);
            meta_out.append(val.as_ref());
            idx += 1;
        }
        // Now step through replace, skipping our data and inserting the replacement data (if there
        // is any)
        let mut meta_replace_with = replace_with.clone();
        for _ in 0..replace.len() {
            meta_copy.next();
            if let Some(val) = meta_replace_with.next() {
                let val = val.map_err(SpliceError::ReadReplace)?;
                // Note that we are just constructing metadata values here.
                let meta_val = &u64::from(ValueMeta::from(val.as_ref()));
                meta_out.append(Some(meta_val));
            }
            idx += 1;
        }
        // Copy any remaining input from the replacments to the output
        for val in meta_replace_with {
            let val = val.map_err(SpliceError::ReadReplace)?;
            let meta_val = &u64::from(ValueMeta::from(val.as_ref()));
            meta_out.append(Some(meta_val));
            idx += 1;
        }
        // Now copy any remaining data we have to the output
        while !meta_copy.done() {
            let val = meta_copy
                .next()
                .transpose()
                .map_err(SpliceError::ReadExisting)?
                .unwrap_or(None);
            meta_out.append(val.as_ref());
        }
        let (_, meta_len) = meta_out.finish();
        let meta_range = start..(start + meta_len);

        // Second pass, copying the values. For this pass we iterate over ourselves.
        //
        //
        let mut value_range_len = 0;
        let mut raw_encoder = RawEncoder::from(out);
        let mut iter = self.iter(data);
        idx = 0;
        // Copy everything up to replace.start to the output
        while idx < replace.start {
            let val = iter.next().unwrap().unwrap_or(ScalarValue::Null);
            value_range_len += encode_val(&mut raw_encoder, &val);
            idx += 1;
        }

        // Now step through replace, skipping our data and inserting the replacement data (if there
        // is any)
        for _ in 0..replace.len() {
            iter.next();
            if let Some(val) = replace_with.next() {
                let val = val.map_err(SpliceError::ReadReplace)?;
                value_range_len += encode_val(&mut raw_encoder, val.as_ref());
            }
            idx += 1;
        }
        // Copy any remaining input from the replacments to the output
        for val in replace_with {
            let val = val.map_err(SpliceError::ReadReplace)?;
            value_range_len += encode_val(&mut raw_encoder, val.as_ref());
            idx += 1;
        }
        // Now copy any remaining data we have to the output
        while !iter.done() {
            let val = iter.next().unwrap().unwrap_or(ScalarValue::Null);
            value_range_len += encode_val(&mut raw_encoder, &val);
        }

        let value_range = meta_range.end..(meta_range.end + value_range_len);

        Ok(Self {
            meta: meta_range.into(),
            raw: value_range.into(),
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ValueIter<'a> {
    meta: RleDecoder<'a, u64>,
    raw: RawDecoder<'a>,
}

impl<'a> Iterator for ValueIter<'a> {
    type Item = Result<ScalarValue, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = match self.meta.next().transpose() {
            Ok(n) => n,
            Err(e) => return Some(Err(DecodeColumnError::decode_raw("meta", e))),
        };
        match next {
            Some(Some(next)) => {
                let val_meta = ValueMeta::from(next);
                #[allow(clippy::redundant_slicing)]
                match val_meta.type_code() {
                    ValueType::Null => Some(Ok(ScalarValue::Null)),
                    ValueType::True => Some(Ok(ScalarValue::Boolean(true))),
                    ValueType::False => Some(Ok(ScalarValue::Boolean(false))),
                    ValueType::Uleb => self.parse_input(val_meta, leb128_u64),
                    ValueType::Leb => self.parse_input(val_meta, leb128_i64),
                    ValueType::String => self.parse_raw(val_meta, |bytes| {
                        let val = std::str::from_utf8(bytes)
                            .map_err(|e| DecodeColumnError::invalid_value("value", e.to_string()))?
                            .into();
                        Ok(ScalarValue::Str(val))
                    }),
                    ValueType::Float => self.parse_raw(val_meta, |bytes| {
                        if val_meta.length() != 8 {
                            return Err(DecodeColumnError::invalid_value(
                                "value",
                                format!("float should have length 8, had {0}", val_meta.length()),
                            ));
                        }
                        let raw: [u8; 8] = bytes
                            .try_into()
                            // SAFETY: parse_raw() calls read_bytes(val_meta.length()) and we have
                            //         checked that val_meta.length() == 8
                            .unwrap();
                        let val = f64::from_le_bytes(raw);
                        Ok(ScalarValue::F64(val))
                    }),
                    ValueType::Counter => self.parse_input(val_meta, |input| {
                        leb128_i64(input).map(|(i, n)| (i, ScalarValue::Counter(n.into())))
                    }),
                    ValueType::Timestamp => self.parse_input(val_meta, |input| {
                        leb128_i64(input).map(|(i, n)| (i, ScalarValue::Timestamp(n)))
                    }),
                    ValueType::Unknown(code) => self.parse_raw(val_meta, |bytes| {
                        Ok(ScalarValue::Unknown {
                            type_code: code,
                            bytes: bytes.to_vec(),
                        })
                    }),
                    ValueType::Bytes => match self.raw.read_bytes(val_meta.length()) {
                        Err(e) => Some(Err(DecodeColumnError::invalid_value(
                            "value",
                            e.to_string(),
                        ))),
                        Ok(bytes) => Some(Ok(ScalarValue::Bytes(bytes.to_vec()))),
                    },
                }
            }
            Some(None) => Some(Err(DecodeColumnError::unexpected_null("meta"))),
            None => None,
        }
    }
}

impl<'a> ValueIter<'a> {
    fn parse_raw<'b, R, F: Fn(&'b [u8]) -> Result<R, DecodeColumnError>>(
        &'b mut self,
        meta: ValueMeta,
        f: F,
    ) -> Option<Result<R, DecodeColumnError>> {
        let raw = match self.raw.read_bytes(meta.length()) {
            Err(e) => {
                return Some(Err(DecodeColumnError::invalid_value(
                    "value",
                    e.to_string(),
                )))
            }
            Ok(bytes) => bytes,
        };
        Some(f(raw))
    }

    fn parse_input<'b, R, F: Fn(Input<'b>) -> ParseResult<'b, R, DecodeError>>(
        &'b mut self,
        meta: ValueMeta,
        f: F,
    ) -> Option<Result<ScalarValue, DecodeColumnError>>
    where
        R: Into<ScalarValue>,
    {
        self.parse_raw(meta, |raw| match f(Input::new(raw)) {
            Err(e) => Err(DecodeColumnError::invalid_value("value", e.to_string())),
            Ok((i, _)) if !i.is_empty() => {
                Err(DecodeColumnError::invalid_value("value", "extra bytes"))
            }
            Ok((_, v)) => Ok(v.into()),
        })
    }

    pub(crate) fn done(&self) -> bool {
        self.meta.done()
    }
}

/// Appends values row-wise. That is to say, this struct manages two separate chunks of memory, one
/// for the value metadata and one for the raw values. To use it, create a new encoder using
/// `ValueEncoder::new`, sequentially append values using `ValueEncoder::append`, and finallly
/// concatenate the two columns and append them to a buffer returning the range within the output
/// buffer which contains the concatenated columns using `ValueEncoder::finish`.
pub(crate) struct ValueEncoder<S> {
    meta: RleEncoder<S, u64>,
    raw: RawEncoder<S>,
}

impl<S: Sink> ValueEncoder<S> {
    pub(crate) fn append(&mut self, value: &ScalarValue) {
        let meta_val = &u64::from(ValueMeta::from(value));
        self.meta.append_value(meta_val);
        encode_val(&mut self.raw, value);
    }
}

impl ValueEncoder<Vec<u8>> {
    pub(crate) fn new() -> Self {
        Self {
            meta: RleEncoder::new(Vec::new()),
            raw: RawEncoder::from(Vec::new()),
        }
    }
    pub(crate) fn finish(self, out: &mut Vec<u8>) -> ValueRange {
        let meta_start = out.len();
        let (meta, _) = self.meta.finish();
        out.extend(meta);
        let meta_end = out.len();

        let (val, _) = self.raw.finish();
        out.extend(val);
        let val_end = out.len();
        ValueRange {
            meta: (meta_start..meta_end).into(),
            raw: (meta_end..val_end).into(),
        }
    }
}

fn encode_val<S: Sink>(out: &mut RawEncoder<S>, val: &ScalarValue) -> usize {
    match val {
        ScalarValue::Uint(i) => out.append(*i),
        ScalarValue::Int(i) => out.append(*i),
        ScalarValue::Null => 0,
        ScalarValue::Boolean(_) => 0,
        ScalarValue::Timestamp(i) => out.append(*i),
        ScalarValue::F64(f) => out.append(*f),
        ScalarValue::Counter(i) => out.append(i.start),
        ScalarValue::Str(s) => out.append(RawBytes::from(s.as_bytes())),
        ScalarValue::Bytes(b) => out.append(RawBytes::from(&b[..])),
        ScalarValue::Unknown { bytes, .. } => out.append(RawBytes::from(&bytes[..])),
    }
}

#[derive(Debug)]
enum ValueType {
    Null,
    False,
    True,
    Uleb,
    Leb,
    Float,
    String,
    Bytes,
    Counter,
    Timestamp,
    Unknown(u8),
}

#[derive(Copy, Clone)]
struct ValueMeta(u64);

impl ValueMeta {
    fn type_code(&self) -> ValueType {
        let low_byte = (self.0 as u8) & 0b00001111;
        match low_byte {
            0 => ValueType::Null,
            1 => ValueType::False,
            2 => ValueType::True,
            3 => ValueType::Uleb,
            4 => ValueType::Leb,
            5 => ValueType::Float,
            6 => ValueType::String,
            7 => ValueType::Bytes,
            8 => ValueType::Counter,
            9 => ValueType::Timestamp,
            other => ValueType::Unknown(other),
        }
    }

    fn length(&self) -> usize {
        (self.0 >> 4) as usize
    }
}

impl From<&ScalarValue> for ValueMeta {
    fn from(p: &ScalarValue) -> Self {
        match p {
            ScalarValue::Uint(i) => Self((ulebsize(*i) << 4) | 3),
            ScalarValue::Int(i) => Self((lebsize(*i) << 4) | 4),
            ScalarValue::Null => Self(0),
            ScalarValue::Boolean(b) => Self(match b {
                false => 1,
                true => 2,
            }),
            ScalarValue::Timestamp(i) => Self((lebsize(*i) << 4) | 9),
            ScalarValue::F64(_) => Self((8 << 4) | 5),
            ScalarValue::Counter(i) => Self((lebsize(i.start) << 4) | 8),
            ScalarValue::Str(s) => Self(((s.as_bytes().len() as u64) << 4) | 6),
            ScalarValue::Bytes(b) => Self(((b.len() as u64) << 4) | 7),
            ScalarValue::Unknown { type_code, bytes } => {
                Self(((bytes.len() as u64) << 4) | (*type_code as u64))
            }
        }
    }
}

impl From<u64> for ValueMeta {
    fn from(raw: u64) -> Self {
        ValueMeta(raw)
    }
}

impl From<ValueMeta> for u64 {
    fn from(v: ValueMeta) -> Self {
        v.0
    }
}

impl From<&ScalarValue> for ValueType {
    fn from(p: &ScalarValue) -> Self {
        match p {
            ScalarValue::Uint(_) => ValueType::Uleb,
            ScalarValue::Int(_) => ValueType::Leb,
            ScalarValue::Null => ValueType::Null,
            ScalarValue::Boolean(b) => match b {
                true => ValueType::True,
                false => ValueType::False,
            },
            ScalarValue::Timestamp(_) => ValueType::Timestamp,
            ScalarValue::F64(_) => ValueType::Float,
            ScalarValue::Counter(_) => ValueType::Counter,
            ScalarValue::Str(_) => ValueType::String,
            ScalarValue::Bytes(_) => ValueType::Bytes,
            ScalarValue::Unknown { type_code, .. } => ValueType::Unknown(*type_code),
        }
    }
}

impl From<ValueType> for u64 {
    fn from(v: ValueType) -> Self {
        match v {
            ValueType::Null => 0,
            ValueType::False => 1,
            ValueType::True => 2,
            ValueType::Uleb => 3,
            ValueType::Leb => 4,
            ValueType::Float => 5,
            ValueType::String => 6,
            ValueType::Bytes => 7,
            ValueType::Counter => 8,
            ValueType::Timestamp => 9,
            ValueType::Unknown(other) => other as u64,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::encoding::properties::{scalar_value, splice_scenario};
    use proptest::prelude::*;
    use std::borrow::Cow;

    fn encode_values(vals: &[ScalarValue]) -> (Vec<u8>, ValueRange) {
        let mut out = Vec::new();
        let range = ValueRange::encode(vals.iter().cloned().map(Cow::Owned), &mut out);
        (out, range)
    }

    fn encode_rowwise(vals: &[ScalarValue]) -> (Vec<u8>, ValueRange) {
        let mut out = Vec::new();
        let mut encoder = ValueEncoder::new();
        for val in vals {
            encoder.append(val);
        }
        let range = encoder.finish(&mut out);
        (out, range)
    }

    proptest! {
        #[test]
        fn test_initialize_splice(values in proptest::collection::vec(scalar_value(), 0..100)) {
            let (out, range) = encode_values(&values[..]);
            let testvals = range.iter(&out).collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(values, testvals);
        }

        #[test]
        fn test_splice_values(scenario in splice_scenario(scalar_value())){
            let (out, range) = encode_values(&scenario.initial_values);
            let mut spliced = Vec::new();
            let new_range = range
                .splice(
                    &out,
                    scenario.replace_range.clone(),
                    scenario.replacements.clone().into_iter().map(Cow::Owned),
                    &mut spliced,
                );
            let result_values = new_range.iter(&spliced).collect::<Result<Vec<_>, _>>().unwrap();
            let mut expected: Vec<_> = scenario.initial_values.clone();
            expected.splice(scenario.replace_range, scenario.replacements);
            assert_eq!(result_values, expected);
        }

        #[test]
        fn encode_row_wise_and_columnwise_equal(values in proptest::collection::vec(scalar_value(), 0..50)) {
            let (colwise, col_range) = encode_values(&values[..]);
            let (rowwise, row_range) = encode_rowwise(&values[..]);
            assert_eq!(colwise, rowwise);
            assert_eq!(col_range, row_range);
        }
    }

    #[test]
    fn test_value_uleb() {
        let vals = [ScalarValue::Uint(127), ScalarValue::Uint(183)];
        let (out, range) = encode_values(&vals);
        let result = range.iter(&out).collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(result, vals);
    }
}
