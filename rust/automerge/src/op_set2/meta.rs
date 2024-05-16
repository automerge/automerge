use crate::columnar::encoding::leb128::{lebsize, ulebsize};

use super::{
    types::ScalarValue, ColExport, ColumnCursor, Encoder, MaybePackable, PackError, Packable,
    RleCursor, RleState, Run, Slab, WritableSlab,
};

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

#[derive(Copy, Clone, Debug, PartialEq)]
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

type SubCursor = RleCursor<{ usize::MAX }, ValueMeta>;

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct MetaCursor {
    sum: usize,
    rle: SubCursor,
}

impl ColumnCursor for MetaCursor {
    type Item = ValueMeta;
    type State<'a> = RleState<'a, ValueMeta>;
    type PostState<'a> = Option<Run<'a, ValueMeta>>;
    type Export = Option<ValueMeta>;

    fn finish<'a>(
        slab: &'a Slab,
        out: &mut WritableSlab,
        state: Self::State<'a>,
        post: Self::PostState<'a>,
        cursor: Self,
    ) {
        SubCursor::finish(slab, out, state, post, cursor.rle)
    }

    fn append<'a>(state: &mut Self::State<'a>, slab: &mut WritableSlab, item: Option<ValueMeta>) {
        SubCursor::append(state, slab, item)
    }

    fn encode<'a>(index: usize, slab: &'a Slab) -> Encoder<'a, Self> {
        let (run, cursor) = Self::seek(index, slab.as_ref());

        let last_run_count = run.as_ref().map(|r| r.count).unwrap_or(0);

        let (state, post) = SubCursor::encode_inner(&cursor.rle, run, index, slab);

        let current = cursor.rle.start_copy(slab, last_run_count);

        Encoder {
            slab,
            results: vec![],
            current,
            post,
            state,
            cursor,
        }
    }

    fn export_item(item: Option<ValueMeta>) -> Option<ValueMeta> {
        item
    }

    fn export(data: &[u8]) -> Vec<ColExport<ValueMeta>> {
        SubCursor::export(data)
    }

    fn try_next<'a>(
        &self,
        slab: &'a [u8],
    ) -> Result<Option<(Run<'a, ValueMeta>, Self)>, PackError> {
        match self.rle.try_next(slab)? {
            Some((
                Run {
                    count,
                    value: Some(value),
                },
                rle,
            )) => {
                let sum = self.sum + count * value.length();
                Ok(Some((
                    Run {
                        count,
                        value: Some(value),
                    },
                    Self { sum, rle },
                )))
            }
            Some((Run { count, value }, rle)) => {
                Ok(Some((Run { count, value }, Self { sum: self.sum, rle })))
            }
            _ => Ok(None),
        }
    }

    fn index(&self) -> usize {
        self.rle.index()
    }
}
