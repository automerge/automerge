use std::{borrow::Cow, ops::Range};

use super::{
    super::{
        encoding::{
            generic::{GenericColDecoder, GroupDecoder, SingleLogicalColDecoder},
            RawDecoder, RleDecoder, RleEncoder, SimpleColDecoder, ValueDecoder, BooleanDecoder, DeltaDecoder,
        },
        CellValue, ColumnId, ColumnSpec,
    },
    ColumnSpliceError,
};

use crate::columnar_2::column_specification::ColumnType;

#[derive(Debug)]
pub(crate) struct Column(ColumnInner);

impl Column {
    pub(crate) fn range(&self) -> Range<usize> {
        self.0.range()
    }

    pub(crate) fn ranges<'a>(&'a self) -> ColumnRanges<'a> {
        self.0.ranges()
    }

    pub(crate) fn decoder<'a>(&self, data: &'a [u8]) -> GenericColDecoder<'a> {
        self.0.decoder(data)
    }

    pub(crate) fn splice<'a, I>(
        &self,
        source: &[u8],
        output: &mut Vec<u8>,
        replace: Range<usize>,
        replace_with: I,
    ) -> Result<Self, ColumnSpliceError>
    where
        I: Iterator<Item=CellValue<'a>> + Clone
    {
        Ok(Self(self.0.splice(source, output, replace, replace_with)?))
    }

    pub(crate) fn col_type(&self) -> ColumnType {
        self.0.col_type()
    }

    pub fn id(&self) -> ColumnId {
        match self.0 {
            ColumnInner::Single(SingleColumn { spec: s, .. }) => s.id(),
            ColumnInner::Composite(CompositeColumn::Value(ValueColumn { spec, .. })) => spec.id(),
            ColumnInner::Composite(CompositeColumn::Group(GroupColumn { spec, .. })) => spec.id(),
        }
    }

    pub(crate) fn spec(&self) -> ColumnSpec {
        match &self.0 {
            ColumnInner::Single(s) => s.spec,
            ColumnInner::Composite(CompositeColumn::Value(v)) => v.spec,
            ColumnInner::Composite(CompositeColumn::Group(g)) => g.spec,
        }
    }
}

#[derive(Debug, Clone)]
enum ColumnInner {
    Single(SingleColumn),
    Composite(CompositeColumn),
}

pub(crate) enum ColumnRanges<'a> {
    Single(Range<usize>),
    Group{
        num: Range<usize>,
        cols: ColRangeIter<'a>,
    },
    Value {
        meta: Range<usize>,
        val: Range<usize>,
    }
}

pub(crate) enum GroupColRange {
    Single(Range<usize>),
    Value{
        meta: Range<usize>,
        val: Range<usize>,
    }
}

pub(crate) struct ColRangeIter<'a> {
    offset: usize,
    cols: &'a [GroupedColumn]
}

impl<'a> Iterator for ColRangeIter<'a> {
    type Item = GroupColRange;

    fn next(&mut self) -> Option<Self::Item> {
        match self.cols.get(self.offset) {
            None => None,
            Some(GroupedColumn::Single(SingleColumn{range, ..})) => {
                self.offset += 1;
                Some(GroupColRange::Single(range.clone()))
            },
            Some(GroupedColumn::Value(ValueColumn{meta, value, ..})) => {
                self.offset += 1;
                Some(GroupColRange::Value{meta: meta.clone(), val: value.clone()})
            }
        }
    }
}

impl<'a> ExactSizeIterator for ColRangeIter<'a> {
    fn len(&self) -> usize {
        self.cols.len()
    }
}

impl<'a> From<&'a [GroupedColumn]> for ColRangeIter<'a> {
    fn from(cols: &'a [GroupedColumn]) -> Self {
        ColRangeIter{
            cols,
            offset: 0,
        }
    }
}

impl ColumnInner {
    pub(crate) fn range(&self) -> Range<usize> {
        match self {
            Self::Single(SingleColumn { range: r, .. }) => r.clone(),
            Self::Composite(CompositeColumn::Value(ValueColumn { meta, value, .. })) => {
                meta.start..value.end
            }
            Self::Composite(CompositeColumn::Group(GroupColumn { num, values, .. })) => {
                num.start..values.last().map(|v| v.range().end).unwrap_or(num.end)
            }
        }
    }

    pub(crate) fn ranges<'a>(&'a self) -> ColumnRanges<'a> {
        match self {
            Self::Single(SingleColumn{range, ..}) => ColumnRanges::Single(range.clone()),
            Self::Composite(CompositeColumn::Value(ValueColumn{ meta, value, ..})) => ColumnRanges::Value {
                meta: meta.clone(),
                val: value.clone(),
            },
            Self::Composite(CompositeColumn::Group(GroupColumn{num, values, ..})) => ColumnRanges::Group {
                num: num.clone(),
                cols: (&values[..]).into(),
            }
        }
    }

    pub(crate) fn decoder<'a>(&self, data: &'a [u8]) -> GenericColDecoder<'a> {
        match self {
            Self::Single(SingleColumn {
                range, col_type, ..
            }) => {
                let simple = col_type.decoder(&data[range.clone()]);
                GenericColDecoder::new_simple(simple)
            },
            Self::Composite(CompositeColumn::Value(ValueColumn{meta, value,..})) => GenericColDecoder::new_value(
                ValueDecoder::new(
                    RleDecoder::from(Cow::Borrowed(&data[meta.clone()])),
                    RawDecoder::from(Cow::Borrowed(&data[value.clone()])),
                )
            ),
            Self::Composite(CompositeColumn::Group(GroupColumn{num, values, ..})) => {
                let num_coder = RleDecoder::from(Cow::from(&data[num.clone()]));
                let values = values
                    .iter()
                    .map(|gc| match gc {
                        GroupedColumn::Single(SingleColumn{col_type, range, ..}) => SingleLogicalColDecoder::Simple(
                            col_type.decoder(&data[range.clone()])
                        ),
                        GroupedColumn::Value(ValueColumn{ meta, value, .. }) => {
                            SingleLogicalColDecoder::Value(ValueDecoder::new(
                                RleDecoder::from(Cow::Borrowed(&data[meta.clone()])),
                                RawDecoder::from(Cow::Borrowed(&data[value.clone()])),
                            ))
                        }
                    })
                    .collect();
                GenericColDecoder::new_group(GroupDecoder::new(num_coder, values))
            }
        }
    }

    pub(crate) fn splice<'a, I>(
        &self,
        source: &[u8],
        output: &mut Vec<u8>,
        replace: Range<usize>,
        replace_with: I,
    ) -> Result<Self, ColumnSpliceError>
    where
        I: Iterator<Item=CellValue<'a>> + Clone
    {
        match self {
            Self::Single(s) => Ok(Self::Single(s.splice(
                source,
                output,
                replace,
                replace_with,
            )?)),
            Self::Composite(s) => Ok(Self::Composite(s.splice(
                source,
                output,
                replace,
                replace_with,
            )?)),
        }
    }

    pub(crate) fn col_type(&self) -> ColumnType {
        match self {
            Self::Single(SingleColumn{spec, ..}) => spec.col_type(),
            Self::Composite(CompositeColumn::Value(..)) => ColumnType::Value,
            Self::Composite(CompositeColumn::Group(..)) => ColumnType::Group,
        }
    }
}

#[derive(Clone, Debug)]
struct SingleColumn {
    pub(crate) spec: ColumnSpec,
    pub(crate) col_type: SimpleColType,
    pub(crate) range: Range<usize>,
}

impl SingleColumn {
    fn splice<'a, I>(
        &self,
        source: &[u8],
        output: &mut Vec<u8>,
        replace: Range<usize>,
        replace_with: I,
    ) -> Result<Self, ColumnSpliceError>
    where
        I: Iterator<Item=CellValue<'a>> + Clone
    {
        let output_start = output.len();
        let mut decoder = self.col_type.decoder(&source[self.range.clone()]);
        let end = decoder.splice(output, replace, replace_with)? + output_start;
        Ok(Self {
            spec: self.spec,
            col_type: self.col_type,
            range: (output_start..end).into(),
        })
    }
}

#[derive(Debug, Clone)]
enum CompositeColumn {
    Value(ValueColumn),
    Group(GroupColumn),
}

impl CompositeColumn {
    fn splice<'a, I>(
        &self,
        source: &[u8],
        output: &mut Vec<u8>,
        replace: Range<usize>,
        replace_with: I,
    ) -> Result<Self, ColumnSpliceError>
    where
        I: Iterator<Item=CellValue<'a>> + Clone
    {
        match self {
            Self::Value(value) => Ok(Self::Value(value.splice(
                source,
                replace,
                replace_with,
                output,
            )?)),
            Self::Group(group) => Ok(Self::Group(group.splice(
                source,
                output,
                replace,
                replace_with,
            )?)),
        }
    }
}

#[derive(Clone, Debug)]
struct ValueColumn {
    spec: ColumnSpec,
    meta: Range<usize>,
    value: Range<usize>,
}

impl ValueColumn {
    fn splice<'a, I>(
        &self,
        source: &[u8],
        replace: Range<usize>,
        replace_with: I,
        output: &mut Vec<u8>,
    ) -> Result<Self, ColumnSpliceError>
    where
        I: Iterator<Item=CellValue<'a>> + Clone
    {
        let mut decoder = ValueDecoder::new(
            RleDecoder::from(&source[self.meta.clone()]),
            RawDecoder::from(&source[self.value.clone()]),
        );
        let replacements = replace_with.enumerate().map(|(i, r)| match r {
            CellValue::Value(p) => Ok(p),
            _ => Err(ColumnSpliceError::InvalidValueForRow(i)),
        });
        let (new_meta, new_data) = decoder.try_splice(replace, replacements, output)?;
        Ok(ValueColumn {
            spec: self.spec,
            meta: new_meta.into(),
            value: new_data.into(),
        })
    }
}

#[derive(Debug, Clone)]
struct GroupColumn {
    spec: ColumnSpec,
    num: Range<usize>,
    values: Vec<GroupedColumn>,
}

#[derive(Eq, PartialEq, Clone, Copy, Debug)]
enum SimpleColType {
    Actor,
    Integer,
    DeltaInteger,
    Boolean,
    String,
}

impl SimpleColType {
    fn decoder<'a>(self, data: &'a [u8]) -> SimpleColDecoder<'a> {
        match self {
            SimpleColType::Actor => SimpleColDecoder::new_uint(RleDecoder::from(Cow::from(data))),
            SimpleColType::Integer => SimpleColDecoder::new_uint(RleDecoder::from(Cow::from(data))),
            SimpleColType::String => SimpleColDecoder::new_string(RleDecoder::from(Cow::from(data))),
            SimpleColType::Boolean => SimpleColDecoder::new_bool(BooleanDecoder::from(Cow::from(data))),
            SimpleColType::DeltaInteger => SimpleColDecoder::new_delta(DeltaDecoder::from(Cow::from(data))),
        }
    }
}

#[derive(Clone, Debug)]
enum GroupedColumn {
    Single(SingleColumn),
    Value(ValueColumn),
}

impl GroupedColumn {
    fn range(&self) -> Range<usize> {
        match self {
            Self::Single(SingleColumn{range, ..}) => range.clone(),
            Self::Value(ValueColumn { meta, value, .. }) => (meta.start..value.end),
        }
    }
}

impl GroupColumn {
    fn splice<'a, I>(
        &self,
        source: &[u8],
        output: &mut Vec<u8>,
        replace: Range<usize>,
        replace_with: I,
    ) -> Result<Self, ColumnSpliceError>
    where
        I: Iterator<Item=CellValue<'a>> + Clone
    {
        // This is a little like ValueDecoder::splice. First we want to read off the values from `num`
        // and insert them into the output - inserting replacements lengths as we go. Then we re-read
        // num and use it to also iterate over the grouped values, inserting those into the subsidiary
        // columns as we go.

        // First encode the lengths
        let output_start = output.len();
        let mut num_decoder =
            RleDecoder::<'_, u64>::from(Cow::from(&source[self.num.clone()]));
        let mut num_encoder = RleEncoder::from(output);
        let mut idx = 0;
        while idx < replace.start {
            match num_decoder.next() {
                Some(next_num) => {
                    num_encoder.append(next_num.as_ref());
                }
                None => {
                    panic!("out of bounds");
                }
            }
            idx += 1;
        }
        let mut num_replace_with = replace_with.clone();
        while let Some(replacement) = num_replace_with.next() {
            let rows = match &replacement {
                CellValue::List(rows) => rows,
                _ => return Err(ColumnSpliceError::InvalidValueForRow(idx)),
            };
            for row in rows {
                if row.len() != self.values.len() {
                    return Err(ColumnSpliceError::WrongNumberOfValues {
                        row: idx - replace.start,
                        expected: self.values.len(),
                        actual: row.len(),
                    });
                }
                num_encoder.append(Some(&(rows.len() as u64)));
            }
            idx += 1;
        }
        while let Some(num) = num_decoder.next() {
            num_encoder.append(num.as_ref());
            idx += 1;
        }
        let _num_range = output_start..num_encoder.finish();

        // Now encode the values
        let _num_decoder =
            RleDecoder::<'_, u64>::from(Cow::from(&source[self.num.clone()]));

        panic!()
    }
}

pub(crate) struct ColumnBuilder {
    
}

impl ColumnBuilder {
    pub(crate) fn build_actor(spec: ColumnSpec, range: Range<usize>) -> Column {
        Column(ColumnInner::Single(SingleColumn{spec, col_type: SimpleColType::Actor, range: range.into()}))
    }

    pub(crate) fn build_string(spec: ColumnSpec, range: Range<usize>) -> Column {
        Column(ColumnInner::Single(SingleColumn{spec, col_type: SimpleColType::String, range: range.into()}))
    }

    pub(crate) fn build_integer(spec: ColumnSpec, range: Range<usize>) -> Column {
        Column(ColumnInner::Single(SingleColumn{spec, col_type: SimpleColType::Integer, range: range.into()}))
    }

    pub(crate) fn build_delta_integer(spec: ColumnSpec, range: Range<usize>) -> Column {
        Column(ColumnInner::Single(SingleColumn{spec, col_type: SimpleColType::Integer, range: range.into()}))
    }

    pub(crate) fn build_boolean(spec: ColumnSpec, range: Range<usize>) -> Column {
        Column(ColumnInner::Single(SingleColumn{spec, col_type: SimpleColType::Boolean, range: range.into()}))
    }

    pub(crate) fn start_value(spec: ColumnSpec, meta: Range<usize>) -> AwaitingRawColumnValueBuilder {
        AwaitingRawColumnValueBuilder { spec, meta }
    }

    pub(crate) fn start_group(spec: ColumnSpec, num: Range<usize>) -> GroupBuilder {
        GroupBuilder{spec, num_range: num, columns: Vec::new()}
    }
}

pub(crate) struct AwaitingRawColumnValueBuilder {
    spec: ColumnSpec,
    meta: Range<usize>,
}

impl AwaitingRawColumnValueBuilder {
    pub(crate) fn id(&self) -> ColumnId {
        self.spec.id()
    }

    pub(crate) fn meta_range(&self) -> &Range<usize> {
        &self.meta
    }

    pub(crate) fn build(&mut self, raw: Range<usize>) -> Column {
        Column(ColumnInner::Composite(CompositeColumn::Value(ValueColumn{
            spec: self.spec,
            meta: self.meta.clone().into(),
            value: raw.into(),
        })))
    }
}

#[derive(Debug)]
pub(crate) struct GroupBuilder{
    spec: ColumnSpec,
    num_range: Range<usize>,
    columns: Vec<GroupedColumn>,
}

impl GroupBuilder {

    pub(crate) fn range(&self) -> Range<usize> {
        let start = self.num_range.start;
        let end = self.columns.last().map(|c| c.range().end).unwrap_or(self.num_range.end);
        start..end
    }

    pub(crate) fn add_actor(&mut self, spec: ColumnSpec, range: Range<usize>) {
        self.columns.push(GroupedColumn::Single(SingleColumn{
            col_type: SimpleColType::Actor,
            range: range.into(),
            spec,
        }));
    }

    pub(crate) fn add_string(&mut self, spec: ColumnSpec, range: Range<usize>) {
        self.columns.push(GroupedColumn::Single(SingleColumn{
            col_type: SimpleColType::String,
            range: range.into(),
            spec,
        }));
    }

    pub(crate) fn add_integer(&mut self, spec: ColumnSpec, range: Range<usize>) {
        self.columns.push(GroupedColumn::Single(SingleColumn{
            col_type: SimpleColType::Integer,
            range: range.into(),
            spec,
        }));
    }

    pub(crate) fn add_delta_integer(&mut self, spec: ColumnSpec, range: Range<usize>) {
        self.columns.push(GroupedColumn::Single(SingleColumn{
            col_type: SimpleColType::DeltaInteger,
            range: range.into(),
            spec,
        }));
    }

    pub(crate) fn add_boolean(&mut self, spec: ColumnSpec, range: Range<usize>) {
        self.columns.push(GroupedColumn::Single(SingleColumn{
            col_type: SimpleColType::Boolean,
            range: range.into(),
            spec,
        }));
    }

    pub(crate) fn start_value(&mut self, spec: ColumnSpec, meta: Range<usize>) -> GroupAwaitingValue {
        GroupAwaitingValue {
            spec,
            num_range: self.num_range.clone(),
            columns: std::mem::take(&mut self.columns),
            val_spec: spec,
            val_meta: meta,
        }
    }

    pub(crate) fn finish(&mut self) -> Column {
        Column(ColumnInner::Composite(CompositeColumn::Group(GroupColumn{
            spec: self.spec,
            num: self.num_range.clone(),
            values: std::mem::take(&mut self.columns),
        })))
    }
}

#[derive(Debug)]
pub(crate) struct GroupAwaitingValue {
    spec: ColumnSpec,
    num_range: Range<usize>,
    columns: Vec<GroupedColumn>,
    val_spec: ColumnSpec,
    val_meta: Range<usize>,
}

impl GroupAwaitingValue {
    pub(crate) fn finish_empty(&mut self) -> GroupBuilder {
        self.columns.push(GroupedColumn::Value(ValueColumn{
            meta: self.val_meta.clone(),
            value: 0..0,
            spec: self.val_spec,
        }));
        GroupBuilder {
            spec: self.spec,
            num_range: self.num_range.clone(),
            columns: std::mem::take(&mut self.columns),
        }
    }

    pub(crate) fn finish_value(&mut self, raw: Range<usize>) -> GroupBuilder {
        self.columns.push(GroupedColumn::Value(ValueColumn{
            spec: self.val_spec,
            value: raw.into(),
            meta: self.val_meta.clone(),
        }));
        GroupBuilder {
            spec: self.spec,
            num_range: self.num_range.clone(),
            columns: std::mem::take(&mut self.columns),
        }
    }

    pub(crate) fn range(&self) -> Range<usize> {
        self.num_range.start..self.val_meta.end
    }
}
