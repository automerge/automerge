use std::{borrow::Cow, convert::TryInto};

use self::column_layout::DocOpColumns;

use super::{ColumnId, ColumnSpec};

mod column_layout;
pub(crate) use column_layout::doc_change_columns;
pub(crate) use column_layout::doc_op_columns;
pub(crate) use column_layout::change_op_columns;
pub(crate) use column_layout::{BadColumnLayout, ColumnLayout};
mod column_range;
mod encoding;
pub(crate) use encoding::Key;
use encoding::{DecodeColumnError, GenericColDecoder};
mod value;
pub(crate) use value::{CellValue, PrimVal};

pub(crate) struct RowBlock<'a, C> {
    columns: C,
    data: Cow<'a, [u8]>,
}

impl<'a> RowBlock<'a, ColumnLayout> {
    pub(crate) fn new<I: Iterator<Item = (ColumnSpec, std::ops::Range<usize>)>>(
        cols: I,
        data: Cow<'a, [u8]>,
    ) -> Result<RowBlock<'a, ColumnLayout>, BadColumnLayout> {
        let layout = ColumnLayout::parse(data.len(), cols)?;
        Ok(RowBlock {
            columns: layout,
            data,
        })
    }

    pub(crate) fn into_doc_ops(
        self,
    ) -> Result<RowBlock<'a, column_layout::DocOpColumns>, column_layout::ParseDocColumnError> {
        let doc_cols: column_layout::DocOpColumns = self.columns.try_into()?;
        Ok(RowBlock {
            columns: doc_cols,
            data: self.data,
        })
    }

    pub(crate) fn into_doc_change(
        self,
    ) -> Result<
        RowBlock<'a, column_layout::doc_change_columns::DocChangeColumns>,
        column_layout::doc_change_columns::DecodeChangeError,
    > {
        let doc_cols: column_layout::doc_change_columns::DocChangeColumns =
            self.columns.try_into()?;
        Ok(RowBlock {
            columns: doc_cols,
            data: self.data,
        })
    }

    pub(crate) fn into_change_ops(
        self
    ) -> Result<RowBlock<'a, change_op_columns::ChangeOpsColumns>, change_op_columns::ParseChangeColumnsError> {
        let change_cols: change_op_columns::ChangeOpsColumns = self.columns.try_into()?;
        Ok(RowBlock {
            columns: change_cols,
            data: self.data,
        })
    }
}

impl<'a, 'b> IntoIterator for &'a RowBlock<'b, ColumnLayout> {
    type Item = Result<Vec<(usize, CellValue<'a>)>, DecodeColumnError>;
    type IntoIter = RowBlockIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        RowBlockIter {
            failed: false,
            decoders: self
                .columns
                .iter()
                .map(|c| (c.id(), c.decoder(&self.data)))
                .collect(),
        }
    }
}

pub(crate) struct RowBlockIter<'a> {
    failed: bool,
    decoders: Vec<(ColumnId, GenericColDecoder<'a>)>,
}

impl<'a> Iterator for RowBlockIter<'a> {
    type Item = Result<Vec<(usize, CellValue<'a>)>, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        if self.decoders.iter().all(|(_, d)| d.done()) {
            None
        } else {
            let mut result = Vec::with_capacity(self.decoders.len());
            for (col_index, (_, decoder)) in self.decoders.iter_mut().enumerate() {
                match decoder.next() {
                    Some(Ok(c)) => result.push((col_index, c)),
                    Some(Err(e)) => {
                        self.failed = true;
                        return Some(Err(e));
                    },
                    None => {},
                }
            }
            Some(Ok(result))
        }
    }
}

impl<'a> IntoIterator for &'a RowBlock<'a, DocOpColumns> {
    type Item = Result<doc_op_columns::DocOp<'a>, column_layout::doc_op_columns::DecodeOpError>;
    type IntoIter = column_layout::doc_op_columns::DocOpColumnIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.columns.iter(&self.data)
    }
}

impl<'a> IntoIterator for &'a RowBlock<'a, doc_change_columns::DocChangeColumns> {
    type Item = Result<doc_change_columns::ChangeMetadata<'a>, doc_change_columns::DecodeChangeError>;
    type IntoIter = doc_change_columns::DocChangeColumnIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.columns.iter(&self.data)
    }
}

impl<'a> IntoIterator for &'a RowBlock<'a, change_op_columns::ChangeOpsColumns> {
    type Item = Result<change_op_columns::ChangeOp<'a>, change_op_columns::ReadChangeOpError>;
    type IntoIter = change_op_columns::ChangeOpsIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.columns.iter(&self.data)
    }
}
