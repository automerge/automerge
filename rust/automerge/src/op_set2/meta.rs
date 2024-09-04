use crate::columnar::encoding::leb128::{lebsize, ulebsize};

use super::{types::ScalarValue, MaybePackable, PackError, Packable, RleCursor, WriteOp};

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

#[derive(Copy, Clone, Debug, Default, PartialEq)]
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

impl<'a> Into<WriteOp<'a>> for ValueMeta {
    fn into(self) -> WriteOp<'static> {
        WriteOp::GroupUInt(self.0, self.length())
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
            crate::ScalarValue::Str(s) => Self(((s.as_bytes().len() as u64) << 4) | 6),
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
            ScalarValue::Str(s) => Self(((s.as_bytes().len() as u64) << 4) | 6),
            ScalarValue::Bytes(b) => Self(((b.len() as u64) << 4) | 7),
            ScalarValue::Unknown { type_code, bytes } => {
                Self(((bytes.len() as u64) << 4) | (*type_code as u64))
            }
        }
    }
}

impl Packable for ValueMeta {
    type Unpacked<'a> = ValueMeta;
    type Owned = ValueMeta;

    fn group(item: ValueMeta) -> usize {
        item.length()
    }

    fn width<'a>(item: ValueMeta) -> usize {
        u64::width(item.0)
    }

    fn own<'a>(item: ValueMeta) -> ValueMeta {
        item
    }

    fn unpack<'a>(mut buff: &'a [u8]) -> Result<(usize, Self::Unpacked<'a>), PackError> {
        let start_len = buff.len();
        let val = leb128::read::unsigned(&mut buff)?;
        Ok((start_len - buff.len(), ValueMeta(val)))
    }

    fn pack(buff: &mut Vec<u8>, element: &ValueMeta) -> Result<usize, PackError> {
        let len = leb128::write::unsigned(buff, element.0).unwrap();
        Ok(len)
    }
}

impl MaybePackable<ValueMeta> for ValueMeta {
    fn maybe_packable(&self) -> Option<ValueMeta> {
        Some(*self)
    }
}

impl MaybePackable<ValueMeta> for Option<ValueMeta> {
    fn maybe_packable(&self) -> Option<ValueMeta> {
        *self
    }
}

pub(crate) type MetaCursor = RleCursor<1024, ValueMeta>;

#[cfg(test)]
mod tests {
    use super::super::columns::ColumnData;
    use super::*;

    #[test]
    fn column_data_meta_group() {
        let mut data = vec![
            ValueMeta(1),
            ValueMeta(6 + (30 << 4)),
            ValueMeta(6 + (10 << 4)),
            ValueMeta(3),
            ValueMeta(4),
        ];
        let mut col = ColumnData::<MetaCursor>::new();
        col.splice(0, 0, data);

        let mut iter = col.iter();

        let value = iter.next();
        assert_eq!(value, Some(Some(ValueMeta(1))));
        assert_eq!(iter.group(), 0);

        let value = iter.next();
        assert_eq!(value, Some(Some(ValueMeta(6 + (30 << 4)))));
        assert_eq!(iter.group(), 0);

        let value = iter.next();
        assert_eq!(value, Some(Some(ValueMeta(6 + (10 << 4)))));
        assert_eq!(iter.group(), 30);

        let value = iter.next();
        assert_eq!(value, Some(Some(ValueMeta(3))));
        assert_eq!(iter.group(), 40);

        let mut iter = col.iter();
        iter.advance_by(3);

        let value = iter.next();
        assert_eq!(value, Some(Some(ValueMeta(3))));
        assert_eq!(iter.group(), 40);

        let mut iter = col.iter_range(&(3..5));

        let value = iter.next();
        assert_eq!(value, Some(Some(ValueMeta(3))));
        assert_eq!(iter.group(), 40);
    }
}
