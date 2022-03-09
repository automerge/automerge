use crate::columnar_2::rowblock::column_layout::ColumnSpliceError;
use std::{borrow::Cow, convert::TryInto, ops::Range};

use super::{DecodeColumnError, RawDecoder, RawEncoder, RleDecoder, RleEncoder, RawBytes};
use crate::columnar_2::rowblock::value::PrimVal;

#[derive(Clone)]
pub(crate) struct ValueDecoder<'a> {
    meta: RleDecoder<'a, u64>,
    raw: RawDecoder<'a>,
}

impl<'a> ValueDecoder<'a> {
    pub(crate) fn new(meta: RleDecoder<'a, u64>, raw: RawDecoder<'a>) -> ValueDecoder<'a> {
        ValueDecoder { meta, raw }
    }

    pub(crate) fn done(&self) -> bool {
        self.meta.done()
    }

    pub(crate) fn next(&mut self) -> Option<Result<PrimVal<'a>, DecodeColumnError>> {
        match self.meta.next() {
            Some(Some(next)) => {
                let val_meta = ValueMeta::from(next);
                #[allow(clippy::redundant_slicing)]
                match val_meta.type_code() {
                    ValueType::Null => Some(Ok(PrimVal::Null)),
                    ValueType::True => Some(Ok(PrimVal::Bool(true))),
                    ValueType::False => Some(Ok(PrimVal::Bool(false))),
                    ValueType::Uleb => self.parse_raw(val_meta, |mut bytes| {
                        let val = leb128::read::unsigned(&mut bytes).map_err(|e| {
                            DecodeColumnError::InvalidValue {
                                column: "value".to_string(),
                                description: e.to_string(),
                            }
                        })?;
                        Ok(PrimVal::Uint(val))
                    }),
                    ValueType::Leb => self.parse_raw(val_meta, |mut bytes| {
                        let val = leb128::read::signed(&mut bytes).map_err(|e| {
                            DecodeColumnError::InvalidValue {
                                column: "value".to_string(),
                                description: e.to_string(),
                            }
                        })?;
                        Ok(PrimVal::Int(val))
                    }),
                    ValueType::String => self.parse_raw(val_meta, |bytes| {
                        let val = Cow::Owned(
                            std::str::from_utf8(bytes)
                                .map_err(|e| DecodeColumnError::InvalidValue {
                                    column: "value".to_string(),
                                    description: e.to_string(),
                                })?
                                .into(),
                        );
                        Ok(PrimVal::String(val))
                    }),
                    ValueType::Float => self.parse_raw(val_meta, |bytes| {
                        if val_meta.length() != 8 {
                            return Err(DecodeColumnError::InvalidValue {
                                column: "value".to_string(),
                                description: format!(
                                    "float should have length 8, had {0}",
                                    val_meta.length()
                                ),
                            });
                        }
                        let raw: [u8; 8] = bytes
                            .try_into()
                            // SAFETY: parse_raw() calls read_bytes(val_meta.length()) and we have
                            //         checked that val_meta.length() == 8
                            .unwrap();
                        let val = f64::from_le_bytes(raw);
                        Ok(PrimVal::Float(val))
                    }),
                    ValueType::Counter => self.parse_raw(val_meta, |mut bytes| {
                        let val = leb128::read::unsigned(&mut bytes).map_err(|e| {
                            DecodeColumnError::InvalidValue {
                                column: "value".to_string(),
                                description: e.to_string(),
                            }
                        })?;
                        Ok(PrimVal::Counter(val))
                    }),
                    ValueType::Timestamp => self.parse_raw(val_meta, |mut bytes| {
                        let val = leb128::read::unsigned(&mut bytes).map_err(|e| {
                            DecodeColumnError::InvalidValue {
                                column: "value".to_string(),
                                description: e.to_string(),
                            }
                        })?;
                        Ok(PrimVal::Timestamp(val))
                    }),
                    ValueType::Unknown(code) => self.parse_raw(val_meta, |bytes| {
                        Ok(PrimVal::Unknown {
                            type_code: code,
                            data: bytes.to_vec(),
                        })
                    }),
                    ValueType::Bytes => match self.raw.read_bytes(val_meta.length()) {
                        Err(e) => Some(Err(DecodeColumnError::InvalidValue {
                            column: "value".to_string(),
                            description: e.to_string(),
                        })),
                        Ok(bytes) => Some(Ok(PrimVal::Bytes(Cow::Owned(bytes.to_vec())))),
                    },
                }
            }
            Some(None) => Some(Err(DecodeColumnError::UnexpectedNull("meta".to_string()))),
            None => None,
        }
    }

    pub(crate) fn splice<'b, I>(
        &'a mut self,
        replace: Range<usize>,
        replace_with: I,
        out: &mut Vec<u8>,
    ) -> (Range<usize>, Range<usize>)
    where
        I: Iterator<Item = PrimVal<'a>> + Clone,
    {
        // SAFETY: try_splice only fails if the iterator fails, and this iterator is infallible
        self.try_splice(replace, replace_with.map(|i| Ok(i)), out).unwrap()
    }

    pub(crate) fn try_splice<'b, I>(
        &'a mut self,
        replace: Range<usize>,
        mut replace_with: I,
        out: &mut Vec<u8>,
    ) -> Result<(Range<usize>, Range<usize>), ColumnSpliceError>
    where
        I: Iterator<Item = Result<PrimVal<'a>, ColumnSpliceError>> + Clone,
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
        let mut meta_copy = self.meta.clone();
        let mut meta_out = RleEncoder::from(&mut *out);
        let mut idx = 0;
        // Copy everything up to replace.start to the output
        while idx < replace.start {
            let val = meta_copy.next().unwrap_or(None);
            meta_out.append(val.as_ref());
            idx += 1;
        }
        // Now step through replace, skipping our data and inserting the replacement data (if there
        // is any)
        let mut meta_replace_with = replace_with.clone();
        for _ in 0..replace.len() {
            meta_copy.next();
            if let Some(val) = meta_replace_with.next() {
                let val = val?;
                // Note that we are just constructing metadata values here.
                let meta_val = &u64::from(ValueMeta::from(&val));
                meta_out.append(Some(meta_val));
            }
            idx += 1;
        }
        // Copy any remaining input from the replacments to the output
        while let Some(val) = meta_replace_with.next() {
            let val = val?;
            let meta_val = &u64::from(ValueMeta::from(&val));
            meta_out.append(Some(meta_val));
            idx += 1;
        }
        // Now copy any remaining data we have to the output
        while !meta_copy.done() {
            let val = meta_copy.next().unwrap_or(None);
            meta_out.append(val.as_ref());
        }
        let meta_len = meta_out.finish();
        let meta_range = start..(start + meta_len);

        // Second pass, copying the values. For this pass we iterate over ourselves.
        //
        //
        let mut value_range_len = 0;
        let mut raw_encoder = RawEncoder::from(out);
        idx = 0;
        // Copy everything up to replace.start to the output
        while idx < replace.start {
            let val = self.next().unwrap().unwrap_or(PrimVal::Null);
            value_range_len += encode_primval(&mut raw_encoder, &val);
            idx += 1;
        }

        // Now step through replace, skipping our data and inserting the replacement data (if there
        // is any)
        for _ in 0..replace.len() {
            self.next();
            if let Some(val) = replace_with.next() {
                let val = val?;
                value_range_len += encode_primval(&mut raw_encoder, &val);
            }
            idx += 1;
        }
        // Copy any remaining input from the replacments to the output
        while let Some(val) = replace_with.next() {
            let val = val?;
            value_range_len += encode_primval(&mut raw_encoder, &val);
            idx += 1;
        }
        // Now copy any remaining data we have to the output
        while !self.done() {
            let val = self.next().unwrap().unwrap_or(PrimVal::Null);
            value_range_len += encode_primval(&mut raw_encoder, &val);
        }

        let value_range = meta_range.end..(meta_range.end + value_range_len);

        Ok((meta_range, value_range))
    }

    fn parse_raw<R, F: Fn(&[u8]) -> Result<R, DecodeColumnError>>(
        &mut self,
        meta: ValueMeta,
        f: F,
    ) -> Option<Result<R, DecodeColumnError>> {
        let raw = match self.raw.read_bytes(meta.length()) {
            Err(e) => {
                return Some(Err(DecodeColumnError::InvalidValue {
                    column: "value".to_string(),
                    description: e.to_string(),
                }))
            }
            Ok(bytes) => bytes,
        };
        let val = match f(&mut &raw[..]) {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };
        Some(Ok(val))
    }
}

fn encode_primval(out: &mut RawEncoder, val: &PrimVal) -> usize {
    match val {
        PrimVal::Uint(i) => out.append(i),
        PrimVal::Int(i) => out.append(i),
        PrimVal::Null => 0,
        PrimVal::Bool(_) => 0,
        PrimVal::Timestamp(i) => out.append(i),
        PrimVal::Float(f) => out.append(f),
        PrimVal::Counter(i) => out.append(i),
        PrimVal::String(s) => out.append(&RawBytes::from(s.as_bytes())),
        PrimVal::Bytes(b) => out.append(&RawBytes::from(&b[..])),
        PrimVal::Unknown { data, .. } => out.append(&RawBytes::from(&data[..])),
    }
}

impl<'a> Iterator for ValueDecoder<'a> {
    type Item = Result<PrimVal<'a>, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        ValueDecoder::next(self)
    }
}

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
        let low_byte = (self.0 & 0b00001111) as u8;
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

impl<'a> From<&PrimVal<'a>> for ValueMeta {
    fn from(p: &PrimVal<'a>) -> Self {
        match p {
            PrimVal::Uint(i) => Self((ulebsize(*i) << 4) | 3),
            PrimVal::Int(i) => Self((lebsize(*i) << 4) | 4),
            PrimVal::Null => Self(0),
            PrimVal::Bool(b) => Self(match b {
                false => 1,
                true => 2,
            }),
            PrimVal::Timestamp(i) => Self((ulebsize(*i) << 4) | 9),
            PrimVal::Float(_) => Self((8 << 4) | 5),
            PrimVal::Counter(i) => Self((ulebsize(*i) << 4) | 8),
            PrimVal::String(s) => Self(((s.as_bytes().len() as u64) << 4) | 6),
            PrimVal::Bytes(b) => Self(((b.len() as u64) << 4) | 7),
            PrimVal::Unknown { type_code, data } => {
                Self(((data.len() as u64) << 4) | (*type_code as u64))
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

impl<'a> From<&PrimVal<'a>> for ValueType {
    fn from(p: &PrimVal) -> Self {
        match p {
            PrimVal::Uint(_) => ValueType::Uleb,
            PrimVal::Int(_) => ValueType::Leb,
            PrimVal::Null => ValueType::Null,
            PrimVal::Bool(b) => match b {
                true => ValueType::True,
                false => ValueType::False,
            },
            PrimVal::Timestamp(_) => ValueType::Timestamp,
            PrimVal::Float(_) => ValueType::Float,
            PrimVal::Counter(_) => ValueType::Counter,
            PrimVal::String(_) => ValueType::String,
            PrimVal::Bytes(_) => ValueType::Bytes,
            PrimVal::Unknown { type_code, .. } => ValueType::Unknown(*type_code),
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

fn lebsize(val: i64) -> u64 {
    if val == 0 {
        return 1;
    }
    let numbits = (val as f64).abs().log2().ceil() as u64;
    let mut numblocks = (numbits as f64 / 7.0).ceil() as u64;
    // Make room for the sign bit
    if numbits % 7 == 0 {
        numblocks += 1;
    }
    return numblocks;
}

fn ulebsize(val: u64) -> u64 {
    if val == 0 {
        return 1;
    }
    let numbits = (val as f64).log2().ceil() as u64;
    let numblocks = (numbits as f64 / 7.0).ceil() as u64;
    return numblocks;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar_2::rowblock::encoding::{
        properties::{splice_scenario, value}, RawDecoder, RleDecoder,
    };
    use proptest::prelude::*;

    fn encode_values(vals: &[PrimVal]) -> (Range<usize>, Range<usize>, Vec<u8>) {
        let mut decoder = ValueDecoder {
            meta: RleDecoder::from(&[] as &[u8]),
            raw: RawDecoder::from(&[] as &[u8]),
        };
        let mut out = Vec::new();
        let (meta_range, val_range) = decoder
            .try_splice(0..0, vals.iter().map(|v| Ok(v.clone())), &mut out)
            .unwrap();
        (meta_range, val_range, out)
    }

    proptest! {
        #[test]
        fn test_initialize_splice(values in proptest::collection::vec(value(), 0..100)) {
            let (meta_range, val_range, out) = encode_values(&values);
            let mut decoder = ValueDecoder{
                meta: RleDecoder::from(&out[meta_range]),
                raw: RawDecoder::from(&out[val_range]),
            };
            let mut testvals = Vec::new();
            while !decoder.done() {
                testvals.push(decoder.next().unwrap().unwrap());
            }
            assert_eq!(values, testvals);
        }

        #[test]
        fn test_splice_values(scenario in splice_scenario(value())){
            let (meta_range, val_range, out) = encode_values(&scenario.initial_values);
            let mut decoder = ValueDecoder{
                meta: RleDecoder::from(&out[meta_range]),
                raw: RawDecoder::from(&out[val_range]),
            };
            let mut spliced = Vec::new();
            let (spliced_meta, spliced_val) = decoder
                .try_splice(
                    scenario.replace_range.clone(),
                    scenario.replacements.clone().into_iter().map(|i| Ok(i)),
                    &mut spliced,
                ).unwrap();
            let mut spliced_decoder = ValueDecoder{
                meta: RleDecoder::from(&spliced[spliced_meta]),
                raw: RawDecoder::from(&spliced[spliced_val]),
            };
            let mut result_values = Vec::new();
            while !spliced_decoder.done() {
                result_values.push(spliced_decoder.next().unwrap().unwrap());
            }
            let mut expected: Vec<_> = scenario.initial_values.clone();
            expected.splice(scenario.replace_range, scenario.replacements);
            assert_eq!(result_values, expected);
        }
    }
}
