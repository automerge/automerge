use std::ops::Range;

use crate::columnar::column_range::{
    generic::{GenericColumnRange, GroupRange, GroupedColumnRange, SimpleColRange},
    BooleanRange, DeltaRange, RawRange, RleRange, ValueRange,
};

use super::{Column, ColumnId, ColumnSpec};

pub(crate) struct ColumnBuilder;

impl ColumnBuilder {
    pub(crate) fn build_actor(spec: ColumnSpec, range: RleRange<u64>) -> Column {
        Column::new(
            spec,
            GenericColumnRange::Simple(SimpleColRange::RleInt(range)),
        )
    }

    pub(crate) fn build_string(spec: ColumnSpec, range: RleRange<smol_str::SmolStr>) -> Column {
        Column::new(
            spec,
            GenericColumnRange::Simple(SimpleColRange::RleString(range)),
        )
    }

    pub(crate) fn build_integer(spec: ColumnSpec, range: RleRange<u64>) -> Column {
        Column::new(
            spec,
            GenericColumnRange::Simple(SimpleColRange::RleInt(range)),
        )
    }

    pub(crate) fn build_delta_integer(spec: ColumnSpec, range: DeltaRange) -> Column {
        Column::new(
            spec,
            GenericColumnRange::Simple(SimpleColRange::Delta(range)),
        )
    }

    pub(crate) fn build_boolean(spec: ColumnSpec, range: BooleanRange) -> Column {
        Column::new(
            spec,
            GenericColumnRange::Simple(SimpleColRange::Boolean(range)),
        )
    }

    pub(crate) fn start_value(
        spec: ColumnSpec,
        meta: RleRange<u64>,
    ) -> AwaitingRawColumnValueBuilder {
        AwaitingRawColumnValueBuilder { spec, meta }
    }

    pub(crate) fn start_group(spec: ColumnSpec, num: RleRange<u64>) -> GroupBuilder {
        GroupBuilder {
            spec,
            num_range: num,
            columns: Vec::new(),
        }
    }
}

pub(crate) struct AwaitingRawColumnValueBuilder {
    spec: ColumnSpec,
    meta: RleRange<u64>,
}

impl AwaitingRawColumnValueBuilder {
    pub(crate) fn id(&self) -> ColumnId {
        self.spec.id()
    }

    pub(crate) fn meta_range(&self) -> &RleRange<u64> {
        &self.meta
    }

    pub(crate) fn build(&mut self, raw: RawRange) -> Column {
        Column::new(
            self.spec,
            GenericColumnRange::Value(ValueRange::new(self.meta.clone(), raw)),
        )
    }
}

#[derive(Debug)]
pub(crate) struct GroupBuilder {
    spec: ColumnSpec,
    num_range: RleRange<u64>,
    columns: Vec<GroupedColumnRange>,
}

impl GroupBuilder {
    pub(crate) fn range(&self) -> Range<usize> {
        let start = self.num_range.start();
        let end = self
            .columns
            .last()
            .map(|c| c.range().end)
            .unwrap_or_else(|| self.num_range.end());
        start..end
    }

    pub(crate) fn add_actor(&mut self, _spec: ColumnSpec, range: Range<usize>) {
        self.columns
            .push(GroupedColumnRange::Simple(SimpleColRange::RleInt(
                range.into(),
            )));
    }

    pub(crate) fn add_string(&mut self, _spec: ColumnSpec, range: Range<usize>) {
        self.columns
            .push(GroupedColumnRange::Simple(SimpleColRange::RleString(
                range.into(),
            )));
    }

    pub(crate) fn add_integer(&mut self, _spec: ColumnSpec, range: Range<usize>) {
        self.columns
            .push(GroupedColumnRange::Simple(SimpleColRange::RleInt(
                range.into(),
            )));
    }

    pub(crate) fn add_delta_integer(&mut self, _spec: ColumnSpec, range: Range<usize>) {
        self.columns
            .push(GroupedColumnRange::Simple(SimpleColRange::Delta(
                range.into(),
            )));
    }

    pub(crate) fn add_boolean(&mut self, _spec: ColumnSpec, range: Range<usize>) {
        self.columns
            .push(GroupedColumnRange::Simple(SimpleColRange::Boolean(
                range.into(),
            )));
    }

    pub(crate) fn start_value(
        &mut self,
        _spec: ColumnSpec,
        meta: Range<usize>,
    ) -> GroupAwaitingValue {
        GroupAwaitingValue {
            spec: self.spec,
            num_range: self.num_range.clone(),
            columns: std::mem::take(&mut self.columns),
            val_meta: meta.into(),
        }
    }

    pub(crate) fn finish(&mut self) -> Column {
        Column::new(
            self.spec,
            GenericColumnRange::Group(GroupRange::new(
                self.num_range.clone(),
                std::mem::take(&mut self.columns),
            )),
        )
    }
}

#[derive(Debug)]
pub(crate) struct GroupAwaitingValue {
    spec: ColumnSpec,
    num_range: RleRange<u64>,
    columns: Vec<GroupedColumnRange>,
    val_meta: RleRange<u64>,
}

impl GroupAwaitingValue {
    pub(crate) fn finish_empty(&mut self) -> GroupBuilder {
        self.columns.push(GroupedColumnRange::Value(ValueRange::new(
            self.val_meta.clone(),
            (0..0).into(),
        )));
        GroupBuilder {
            spec: self.spec,
            num_range: self.num_range.clone(),
            columns: std::mem::take(&mut self.columns),
        }
    }

    pub(crate) fn finish_value(&mut self, raw: Range<usize>) -> GroupBuilder {
        self.columns.push(GroupedColumnRange::Value(ValueRange::new(
            self.val_meta.clone(),
            raw.into(),
        )));
        GroupBuilder {
            spec: self.spec,
            num_range: self.num_range.clone(),
            columns: std::mem::take(&mut self.columns),
        }
    }

    pub(crate) fn range(&self) -> Range<usize> {
        self.num_range.start()..self.val_meta.end()
    }
}
