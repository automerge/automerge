use super::{
    ColExport, ColumnCursor, Encoder, PackError, Packable, RleCursor, RleState, Run, Slab,
    WritableSlab,
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

impl Packable for ValueMeta {
    type Unpacked<'a> = ValueMeta;
    type Owned = ValueMeta;

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

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct MetaCursor {
    sum: usize,
    rle: RleCursor<ValueMeta>,
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
        RleCursor::finish(slab, out, state, post, cursor.rle)
    }

    fn append<'a>(state: &mut Self::State<'a>, slab: &mut WritableSlab, item: Option<ValueMeta>) {
        RleCursor::append(state, slab, item)
    }

    fn encode<'a>(index: usize, slab: &'a Slab) -> Encoder<'a, Self> {
        let (run, cursor) = Self::seek(index, slab.as_ref());

        let last_run_count = run.as_ref().map(|r| r.count).unwrap_or(0);

        let (state, post) = RleCursor::encode_inner(&cursor.rle, run, index, slab);

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
        RleCursor::<ValueMeta>::export(data)
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
