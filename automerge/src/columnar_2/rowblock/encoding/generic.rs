use std::{
    borrow::Cow,
    ops::Range,
};

use crate::columnar_2::rowblock::{column_layout::ColumnSpliceError, value::CellValue};

use super::{
    BooleanDecoder, DecodeColumnError, DeltaDecoder, RleDecoder,
    ValueDecoder,
};

pub(crate) enum SimpleColDecoder<'a> {
    RleUint(RleDecoder<'a, u64>),
    RleString(RleDecoder<'a, smol_str::SmolStr>),
    Delta(DeltaDecoder<'a>),
    Bool(BooleanDecoder<'a>),
}

impl<'a> SimpleColDecoder<'a> {
    pub(crate) fn new_uint(d: RleDecoder<'a, u64>) -> Self {
        Self::RleUint(d)
    }

    pub(crate) fn new_string(d: RleDecoder<'a, smol_str::SmolStr>) -> Self {
        Self::RleString(d)
    }

    pub(crate) fn new_delta(d: DeltaDecoder<'a>) -> Self {
        Self::Delta(d)
    }

    pub(crate) fn new_bool(d: BooleanDecoder<'a>) -> Self {
        Self::Bool(d)
    }

    pub(crate) fn done(&self) -> bool {
        match self {
            Self::RleUint(d) => d.done(),
            Self::RleString(d) => d.done(),
            Self::Delta(d) => d.done(),
            Self::Bool(d) => d.done(),
        }
    }

    pub(crate) fn next(&mut self) -> Option<CellValue<'a>> {
        match self {
            Self::RleUint(d) => d.next().and_then(|i| i.map(CellValue::Uint)),
            Self::RleString(d) => d
                .next()
                .and_then(|s| s.map(|s| CellValue::String(Cow::Owned(s.into())))),
            Self::Delta(d) => d.next().and_then(|i| i.map(CellValue::Int)),
            Self::Bool(d) => d.next().map(CellValue::Bool),
        }
    }

    pub(crate) fn splice<'b, I>(
        &mut self,
        out: &mut Vec<u8>,
        replace: Range<usize>,
        replace_with: I,
    ) -> Result<usize, ColumnSpliceError> 
    where
        I: Iterator<Item=CellValue<'b>> + Clone
    {
        // Requires `try_splice` methods on all the basic decoders so that we can report an error
        // if the cellvalue types don't match up
        unimplemented!()
    }
}


pub(crate) enum SingleLogicalColDecoder<'a> {
    Simple(SimpleColDecoder<'a>),
    Value(ValueDecoder<'a>),
}

impl<'a> Iterator for SingleLogicalColDecoder<'a> {
    type Item = Result<CellValue<'a>, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Simple(s) => s.next().map(Ok),
            Self::Value(v) => v.next().map(|v| v.map(|v| CellValue::Value(v))),
        }
    }
}

pub(crate) enum GenericColDecoder<'a> {
    Simple(SimpleColDecoder<'a>),
    Value(ValueDecoder<'a>),
    Group(GroupDecoder<'a>),
}

impl<'a> GenericColDecoder<'a> {
    pub(crate) fn new_simple(s: SimpleColDecoder<'a>) -> Self {
        Self::Simple(s)
    }

    pub(crate) fn new_value(v: ValueDecoder<'a>) -> Self {
        Self::Value(v)
    }

    pub(crate) fn new_group(g: GroupDecoder<'a>) -> Self {
        Self::Group(g)
    }

    pub(crate) fn done(&self) -> bool {
        match self {
            Self::Simple(s) => s.done(),
            Self::Group(g) => g.done(),
            Self::Value(v) => v.done(),
        }
    }

    pub(crate) fn next(&mut self) -> Option<Result<CellValue<'a>, DecodeColumnError>> {
        match self {
            Self::Simple(s) => s.next().map(Ok),
            Self::Value(v) => v.next().map(|v| v.map(|v| CellValue::Value(v))),
            Self::Group(g) => g.next().map(|v| v.map(|v| CellValue::List(v))),
        }
    }
}

impl<'a> Iterator for GenericColDecoder<'a> {
    type Item = Result<CellValue<'a>, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        GenericColDecoder::next(self)
    }
}

pub(crate) struct GroupDecoder<'a> {
    num: RleDecoder<'a, u64>,
    values: Vec<SingleLogicalColDecoder<'a>>,
}

impl<'a> GroupDecoder<'a> {
    pub(crate) fn new(
        num: RleDecoder<'a, u64>,
        values: Vec<SingleLogicalColDecoder<'a>>,
    ) -> GroupDecoder<'a> {
        GroupDecoder { num, values }
    }

    fn next(&mut self) -> Option<Result<Vec<Vec<CellValue<'a>>>, DecodeColumnError>> {
        match self.num.next() {
            Some(Some(num_rows)) => {
                let mut result = Vec::with_capacity(num_rows as usize);
                for _ in 0..num_rows {
                    let mut row = Vec::with_capacity(self.values.len());
                    for (index, column) in self.values.iter_mut().enumerate() {
                        match column.next() {
                            Some(Ok(v)) => row.push(v),
                            Some(Err(e)) => {
                                return Some(Err(DecodeColumnError::InvalidValue {
                                    column: format!("group column {0}", index + 1),
                                    description: e.to_string(),
                                }))
                            }
                            None => {
                                return Some(Err(DecodeColumnError::UnexpectedNull(format!(
                                    "grouped column {0}",
                                    index + 1
                                ))))
                            }
                        }
                    }
                    result.push(row)
                }
                Some(Ok(result))
            }
            Some(None) => Some(Err(DecodeColumnError::UnexpectedNull("num".to_string()))),
            _ => None,
        }
    }

    fn done(&self) -> bool {
        self.num.done()
    }
}
