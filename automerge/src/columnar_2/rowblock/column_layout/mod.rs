use std::ops::Range;

pub(crate) mod column;
pub(crate) mod generic;
pub(crate) mod doc_op_columns;
pub(crate) mod doc_change_columns;
pub(crate) mod change_op_columns;

pub(crate) use generic::{BadColumnLayout, ColumnLayout};
pub(crate) use doc_op_columns::{DocOpColumns, Error as ParseDocColumnError};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ColumnSpliceError {
    #[error("invalid value for row {0}")]
    InvalidValueForRow(usize),
    #[error("wrong number of values for row {0}, expected {expected} but got {actual}")]
    WrongNumberOfValues {
        row: usize,
        expected: usize,
        actual: usize,
    }
}

#[derive(Debug, thiserror::Error)]
#[error("mismatching column at {index}.")]
struct MismatchingColumn {
    index: usize,
}

/// Given a `column::Column` assert that it is of the given `typ` and if so update `target` to be
/// `Some(range)`. Otherwise return a `MismatchingColumn{index}`
fn assert_col_type(
    index: usize,
    col: column::Column,
    typ: crate::columnar_2::column_specification::ColumnType,
    target: &mut Option<Range<usize>>,
) -> Result<(), MismatchingColumn> {
    if col.col_type() == typ {
        match col.ranges() {
            column::ColumnRanges::Single(range) => {
                *target = Some(range);
                Ok(())
            },
            _ => {
                tracing::error!("expected a single column range");
                return Err(MismatchingColumn{ index });
            }
        }
    } else {
        tracing::error!(index, expected=?typ, actual=?col.col_type(), "unexpected columnt type");
        Err(MismatchingColumn { index })
    }
}
