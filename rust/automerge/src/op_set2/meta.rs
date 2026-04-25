use super::types::ScalarValue;
use hexane::{lebsize, ulebsize, PackError};

#[derive(Debug)]
pub(crate) enum ValueType {
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

#[derive(Copy, Clone, Debug, Default, PartialEq, PartialOrd)]
pub(crate) struct ValueMeta(u64);

impl ValueMeta {
    pub(crate) fn type_code(&self) -> ValueType {
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

    pub(crate) fn length(&self) -> usize {
        (self.0 >> 4) as usize
    }
}

impl From<u64> for ValueMeta {
    fn from(raw: u64) -> Self {
        ValueMeta(raw)
    }
}

impl From<&crate::ScalarValue> for ValueMeta {
    fn from(p: &crate::ScalarValue) -> Self {
        match p {
            crate::ScalarValue::Uint(i) => Self((ulebsize(*i) << 4) | 3),
            crate::ScalarValue::Int(i) => Self((lebsize(*i) << 4) | 4),
            crate::ScalarValue::Null => Self(0),
            crate::ScalarValue::Boolean(b) => Self(match b {
                false => 1,
                true => 2,
            }),
            crate::ScalarValue::Timestamp(i) => Self((lebsize(*i) << 4) | 9),
            crate::ScalarValue::F64(_) => Self((8 << 4) | 5),
            crate::ScalarValue::Counter(i) => Self((lebsize(i.start) << 4) | 8),
            crate::ScalarValue::Str(s) => Self(((s.len() as u64) << 4) | 6),
            crate::ScalarValue::Bytes(b) => Self(((b.len() as u64) << 4) | 7),
            crate::ScalarValue::Unknown { type_code, bytes } => {
                Self(((bytes.len() as u64) << 4) | (*type_code as u64))
            }
        }
    }
}

impl<'a> From<&'a ScalarValue<'a>> for ValueMeta {
    fn from(p: &'a ScalarValue<'a>) -> Self {
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
            ScalarValue::Counter(i) => Self((lebsize(*i) << 4) | 8),
            ScalarValue::Str(s) => Self(((s.len() as u64) << 4) | 6),
            ScalarValue::Bytes(b) => Self(((b.len() as u64) << 4) | 7),
            ScalarValue::Unknown { type_code, bytes } => {
                Self(((bytes.len() as u64) << 4) | (*type_code as u64))
            }
        }
    }
}

impl From<&[u8]> for ValueMeta {
    fn from(b: &[u8]) -> Self {
        Self(((b.len() as u64) << 4) | 7)
    }
}

impl hexane::v1::ColumnValue for ValueMeta {
    type Encoding = hexane::v1::RleEncoding<ValueMeta>;
}

impl hexane::v1::RleValue for ValueMeta {
    fn try_unpack(data: &[u8]) -> Result<(usize, ValueMeta), PackError> {
        let mut buf = data;
        let start = buf.len();
        let v = leb128::read::unsigned(&mut buf)?;
        Ok((start - buf.len(), ValueMeta(v)))
    }
    fn pack(value: ValueMeta, out: &mut Vec<u8>) -> bool {
        leb128::write::unsigned(out, value.0).unwrap();
        true
    }
}

impl hexane::v1::PrefixValue for ValueMeta {
    type Prefix = u64;
    fn accumulate(target: &mut u64, val: ValueMeta) {
        *target += val.length() as u64;
    }
    fn accumulate_run(target: &mut u64, run: &hexane::v1::Run<ValueMeta>) {
        *target += run.value.length() as u64 * run.count as u64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hexane::v1::PrefixColumn;

    #[test]
    fn column_data_meta_group() {
        // ValueMeta packs (length << 4) | type_code; lengths sum to a running
        // byte offset into the value blob.  Types 0–3 (Null/False/True/Uleb)
        // have length 0; types 6+ encode an explicit length in the upper bits.
        let data = vec![
            ValueMeta(1),              // length 0
            ValueMeta(6 + (30 << 4)),  // length 30
            ValueMeta(6 + (10 << 4)),  // length 10
            ValueMeta(3),              // length 0
            ValueMeta(4),              // length 0
        ];
        let col = PrefixColumn::<ValueMeta>::from_values(data);

        // PrefixIter yields (inclusive_prefix, value) — running sum of length()
        // up to and including the current item.
        let mut iter = col.iter();

        let (acc, v) = iter.next().unwrap();
        assert_eq!(v, ValueMeta(1));
        assert_eq!(acc, 0);

        let (acc, v) = iter.next().unwrap();
        assert_eq!(v, ValueMeta(6 + (30 << 4)));
        assert_eq!(acc, 30);

        let (acc, v) = iter.next().unwrap();
        assert_eq!(v, ValueMeta(6 + (10 << 4)));
        assert_eq!(acc, 40);

        let (acc, v) = iter.next().unwrap();
        assert_eq!(v, ValueMeta(3));
        assert_eq!(acc, 40);

        // nth(3) jumps to index 3 (the fourth item)
        let (acc, v) = col.iter().nth(3).unwrap();
        assert_eq!(v, ValueMeta(3));
        assert_eq!(acc, 40);

        // iter_range(3..5) starts at index 3 with the cumulative prefix carried
        let mut iter = col.iter_range(3..5);
        let (acc, v) = iter.next().unwrap();
        assert_eq!(v, ValueMeta(3));
        assert_eq!(acc, 40);
    }
}
