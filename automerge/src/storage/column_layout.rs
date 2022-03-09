use std::ops::Range;

mod column_specification;
pub(crate) use column_specification::{ColumnId, ColumnSpec, ColumnType};
mod column;
pub(crate) use column::Column;
mod column_builder;
pub(crate) use column_builder::{
    AwaitingRawColumnValueBuilder, ColumnBuilder, GroupAwaitingValue, GroupBuilder,
};

use super::raw_column::RawColumn;

#[derive(Debug, thiserror::Error)]
#[error("mismatching column at {index}.")]
pub(crate) struct MismatchingColumn {
    pub(crate) index: usize,
}

pub(crate) mod compression {
    #[derive(Clone, Debug)]
    pub(crate) struct Unknown;
    #[derive(Clone, Debug)]
    pub(crate) struct Uncompressed;

    pub(crate) trait ColumnCompression {}
    impl ColumnCompression for Unknown {}
    impl ColumnCompression for Uncompressed {}
}

/// `ColumnLayout` represents a sequence of "logical" columns. "Logical" in this sense means that
/// each column produces one value, but may be composed of multiple `RawColumn`s. For example, in a
/// logical column containing values there are two `RawColumn`s, one for the metadata about the
/// values, and one for the values themselves.
#[derive(Clone, Debug)]
pub(crate) struct ColumnLayout {
    columns: Vec<Column>,
}

impl ColumnLayout {
    pub(crate) fn empty() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    pub(crate) fn append(&mut self, col: Column) {
        self.columns.push(col)
    }

    pub(crate) fn parse<'a, I: Iterator<Item = &'a RawColumn<compression::Uncompressed>>>(
        data_size: usize,
        cols: I,
    ) -> Result<ColumnLayout, BadColumnLayout> {
        let mut parser = ColumnLayoutParser::new(data_size, None);
        for raw_col in cols {
            parser.add_column(raw_col.spec(), raw_col.data())?;
        }
        parser.build()
    }
}

impl FromIterator<Column> for Result<ColumnLayout, BadColumnLayout> {
    fn from_iter<T: IntoIterator<Item = Column>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let mut result = Vec::with_capacity(iter.size_hint().1.unwrap_or(0));
        let mut last_column: Option<ColumnSpec> = None;
        for col in iter {
            if let Some(last_col) = last_column {
                if col.spec().normalize() < last_col.normalize() {
                    return Err(BadColumnLayout::OutOfOrder);
                }
            }
            last_column = Some(col.spec());
            result.push(col);
        }
        Ok(ColumnLayout { columns: result })
    }
}

impl IntoIterator for ColumnLayout {
    type Item = Column;
    type IntoIter = std::vec::IntoIter<Column>;

    fn into_iter(self) -> Self::IntoIter {
        self.columns.into_iter()
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BadColumnLayout {
    #[error("duplicate column specifications: {0}")]
    DuplicateColumnSpecs(u32),
    #[error("out of order columns")]
    OutOfOrder,
    #[error("nested group")]
    NestedGroup,
    #[error("raw value column without metadata column")]
    LoneRawValueColumn,
    #[error("value metadata followed by value column with different column ID")]
    MismatchingValueMetadataId,
    #[error("non contiguous columns")]
    NonContiguousColumns,
    #[error("data out of range")]
    DataOutOfRange,
}

struct ColumnLayoutParser {
    columns: Vec<Column>,
    last_spec: Option<ColumnSpec>,
    state: LayoutParserState,
    total_data_size: usize,
}

enum LayoutParserState {
    Ready,
    InValue(AwaitingRawColumnValueBuilder),
    InGroup(ColumnId, GroupParseState),
}

#[derive(Debug)]
enum GroupParseState {
    Ready(GroupBuilder),
    InValue(GroupAwaitingValue),
}

impl ColumnLayoutParser {
    fn new(data_size: usize, size_hint: Option<usize>) -> Self {
        ColumnLayoutParser {
            columns: Vec::with_capacity(size_hint.unwrap_or(0)),
            last_spec: None,
            state: LayoutParserState::Ready,
            total_data_size: data_size,
        }
    }

    fn build(mut self) -> Result<ColumnLayout, BadColumnLayout> {
        let columns = match self.state {
            LayoutParserState::Ready => self.columns,
            LayoutParserState::InValue(mut builder) => {
                self.columns.push(builder.build((0..0).into()));
                self.columns
            }
            LayoutParserState::InGroup(_, groupstate) => {
                match groupstate {
                    GroupParseState::InValue(mut builder) => {
                        self.columns.push(builder.finish_empty().finish());
                    }
                    GroupParseState::Ready(mut builder) => {
                        self.columns.push(builder.finish());
                    }
                };
                self.columns
            }
        };
        Ok(ColumnLayout { columns })
    }

    #[tracing::instrument(skip(self), err)]
    fn add_column(
        &mut self,
        column: ColumnSpec,
        range: Range<usize>,
    ) -> Result<(), BadColumnLayout> {
        self.check_contiguous(&range)?;
        self.check_bounds(&range)?;
        if let Some(last_spec) = self.last_spec {
            if last_spec.normalize() > column.normalize() {
                return Err(BadColumnLayout::OutOfOrder);
            } else if last_spec == column {
                return Err(BadColumnLayout::DuplicateColumnSpecs(column.into()));
            }
        }
        match &mut self.state {
            LayoutParserState::Ready => match column.col_type() {
                ColumnType::Group => {
                    self.state = LayoutParserState::InGroup(
                        column.id(),
                        GroupParseState::Ready(ColumnBuilder::start_group(column, range.into())),
                    );
                    Ok(())
                }
                ColumnType::ValueMetadata => {
                    self.state = LayoutParserState::InValue(ColumnBuilder::start_value(
                        column,
                        range.into(),
                    ));
                    Ok(())
                }
                ColumnType::Value => Err(BadColumnLayout::LoneRawValueColumn),
                ColumnType::Actor => {
                    self.columns
                        .push(ColumnBuilder::build_actor(column, range.into()));
                    Ok(())
                }
                ColumnType::String => {
                    self.columns
                        .push(ColumnBuilder::build_string(column, range.into()));
                    Ok(())
                }
                ColumnType::Integer => {
                    self.columns
                        .push(ColumnBuilder::build_integer(column, range.into()));
                    Ok(())
                }
                ColumnType::DeltaInteger => {
                    self.columns
                        .push(ColumnBuilder::build_delta_integer(column, range.into()));
                    Ok(())
                }
                ColumnType::Boolean => {
                    self.columns
                        .push(ColumnBuilder::build_boolean(column, range.into()));
                    Ok(())
                }
            },
            LayoutParserState::InValue(builder) => match column.col_type() {
                ColumnType::Value => {
                    if builder.id() != column.id() {
                        return Err(BadColumnLayout::MismatchingValueMetadataId);
                    }
                    self.columns.push(builder.build(range.into()));
                    self.state = LayoutParserState::Ready;
                    Ok(())
                }
                _ => {
                    self.columns.push(builder.build((0..0).into()));
                    self.state = LayoutParserState::Ready;
                    self.add_column(column, range)
                }
            },
            LayoutParserState::InGroup(id, group_state) => {
                if *id != column.id() {
                    match group_state {
                        GroupParseState::Ready(b) => self.columns.push(b.finish()),
                        GroupParseState::InValue(b) => self.columns.push(b.finish_empty().finish()),
                    };
                    std::mem::swap(&mut self.state, &mut LayoutParserState::Ready);
                    self.add_column(column, range)
                } else {
                    match group_state {
                        GroupParseState::Ready(builder) => match column.col_type() {
                            ColumnType::Group => Err(BadColumnLayout::NestedGroup),
                            ColumnType::Value => Err(BadColumnLayout::LoneRawValueColumn),
                            ColumnType::ValueMetadata => {
                                *group_state =
                                    GroupParseState::InValue(builder.start_value(column, range));
                                Ok(())
                            }
                            ColumnType::Actor => {
                                builder.add_actor(column, range);
                                Ok(())
                            }
                            ColumnType::Boolean => {
                                builder.add_boolean(column, range);
                                Ok(())
                            }
                            ColumnType::DeltaInteger => {
                                builder.add_delta_integer(column, range);
                                Ok(())
                            }
                            ColumnType::Integer => {
                                builder.add_integer(column, range);
                                Ok(())
                            }
                            ColumnType::String => {
                                builder.add_string(column, range);
                                Ok(())
                            }
                        },
                        GroupParseState::InValue(builder) => match column.col_type() {
                            ColumnType::Value => {
                                *group_state = GroupParseState::Ready(builder.finish_value(range));
                                Ok(())
                            }
                            _ => {
                                *group_state = GroupParseState::Ready(builder.finish_empty());
                                self.add_column(column, range)
                            }
                        },
                    }
                }
            }
        }
    }

    fn check_contiguous(&self, next_range: &Range<usize>) -> Result<(), BadColumnLayout> {
        match &self.state {
            LayoutParserState::Ready => {
                if let Some(prev) = self.columns.last() {
                    if prev.range().end != next_range.start {
                        tracing::error!(prev=?prev.range(), next=?next_range, "it's here");
                        Err(BadColumnLayout::NonContiguousColumns)
                    } else {
                        Ok(())
                    }
                } else {
                    Ok(())
                }
            }
            LayoutParserState::InValue(builder) => {
                if builder.meta_range().end() != next_range.start {
                    Err(BadColumnLayout::NonContiguousColumns)
                } else {
                    Ok(())
                }
            }
            LayoutParserState::InGroup(_, group_state) => {
                let end = match group_state {
                    GroupParseState::InValue(b) => b.range().end,
                    GroupParseState::Ready(b) => b.range().end,
                };
                if end != next_range.start {
                    Err(BadColumnLayout::NonContiguousColumns)
                } else {
                    Ok(())
                }
            }
        }
    }

    fn check_bounds(&self, next_range: &Range<usize>) -> Result<(), BadColumnLayout> {
        if next_range.end > self.total_data_size {
            Err(BadColumnLayout::DataOutOfRange)
        } else {
            Ok(())
        }
    }
}
