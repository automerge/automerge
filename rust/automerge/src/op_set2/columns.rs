use crate::storage::{ColumnSpec, ColumnType};
use crate::types::ActorId;

use super::meta::MetaCursor;
use super::packer::{
    BooleanCursor, ColumnData, DeltaCursor, IntCursor, PackError, RawCursor, ScanMeta, Slab,
    StrCursor,
};
use super::types::{ActionCursor, ActorCursor};

use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) enum Column {
    Actor(ColumnData<ActorCursor>),
    Str(ColumnData<StrCursor>),
    Integer(ColumnData<IntCursor>),
    Action(ColumnData<ActionCursor>),
    Delta(ColumnData<DeltaCursor>),
    Bool(ColumnData<BooleanCursor>),
    ValueMeta(ColumnData<MetaCursor>),
    Value(ColumnData<RawCursor>),
    Group(ColumnData<IntCursor>),
}

impl Column {
    // FIXME
    /*
        pub(crate) fn splice(&mut self, mut index: usize, op: &OpBuilder) {
            todo!()
            match self {
                Self::Actor(col) => col.write(out),
                Self::Str(col) => col.write(out),
                Self::Integer(col) => col.write(out),
                Self::Delta(col) => col.write(out),
                Self::Bool(col) => col.write(out),
                Self::ValueMeta(col) => col.write(out),
                Self::Value(col) => col.write(out),
                Self::Group(col) => col.write(out),
                Self::Action(col) => col.write(out),
            }
        }
    */

    pub(crate) fn write(&self, out: &mut Vec<u8>) -> Range<usize> {
        match self {
            Self::Actor(col) => col.write(out),
            Self::Str(col) => col.write(out),
            Self::Integer(col) => col.write(out),
            Self::Delta(col) => col.write(out),
            Self::Bool(col) => col.write(out),
            Self::ValueMeta(col) => col.write(out),
            Self::Value(col) => col.write(out),
            Self::Group(col) => col.write(out),
            Self::Action(col) => col.write(out),
        }
    }

    pub(crate) fn slabs(&self) -> &[Slab] {
        match self {
            Self::Actor(col) => col.slabs.as_slice(),
            Self::Str(col) => col.slabs.as_slice(),
            Self::Integer(col) => col.slabs.as_slice(),
            Self::Delta(col) => col.slabs.as_slice(),
            Self::Bool(col) => col.slabs.as_slice(),
            Self::ValueMeta(col) => col.slabs.as_slice(),
            Self::Value(col) => col.slabs.as_slice(),
            Self::Group(col) => col.slabs.as_slice(),
            Self::Action(col) => col.slabs.as_slice(),
        }
    }

    #[allow(unused)]
    pub(crate) fn dump(&self) {
        match self {
            Self::Actor(col) => col.dump(),
            Self::Str(col) => col.dump(),
            Self::Integer(col) => col.dump(),
            Self::Delta(col) => col.dump(),
            Self::Bool(col) => col.dump(),
            Self::ValueMeta(col) => col.dump(),
            Self::Value(col) => col.dump(),
            Self::Group(col) => col.dump(),
            Self::Action(col) => col.dump(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Actor(col) => col.is_empty(),
            Self::Str(col) => col.is_empty(),
            Self::Integer(col) => col.is_empty(),
            Self::Delta(col) => col.is_empty(),
            Self::Bool(col) => col.is_empty(),
            Self::ValueMeta(col) => col.is_empty(),
            Self::Value(col) => col.is_empty(),
            Self::Group(col) => col.is_empty(),
            Self::Action(col) => col.is_empty(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Actor(col) => col.len,
            Self::Str(col) => col.len,
            Self::Integer(col) => col.len,
            Self::Delta(col) => col.len,
            Self::Bool(col) => col.len,
            Self::ValueMeta(col) => col.len,
            Self::Value(col) => col.len,
            Self::Group(col) => col.len,
            Self::Action(col) => col.len,
        }
    }

    pub(crate) fn new(spec: ColumnSpec) -> Self {
        match spec.col_type() {
            ColumnType::Actor => Column::Actor(ColumnData::new()),
            ColumnType::String => Column::Str(ColumnData::new()),
            ColumnType::Integer => {
                if spec.id() == super::op_set::ACTION_COL_ID {
                    Column::Action(ColumnData::new())
                } else {
                    Column::Integer(ColumnData::new())
                }
            }
            ColumnType::DeltaInteger => Column::Delta(ColumnData::new()),
            ColumnType::Boolean => Column::Bool(ColumnData::new()),
            ColumnType::Group => Column::Group(ColumnData::new()),
            ColumnType::ValueMetadata => Column::ValueMeta(ColumnData::new()),
            ColumnType::Value => Column::Value(ColumnData::new()),
        }
    }

    pub(crate) fn external(
        spec: ColumnSpec,
        data: Arc<Vec<u8>>,
        range: Range<usize>,
        actors: &[ActorId],
    ) -> Result<Self, PackError> {
        let m = ScanMeta {
            actors: actors.len(),
        };
        match spec.col_type() {
            ColumnType::Actor => Ok(Column::Actor(ColumnData::external(data, range, &m)?)),
            ColumnType::String => Ok(Column::Str(ColumnData::external(data, range, &m)?)),
            ColumnType::Integer => {
                if spec.id() == super::op_set::ACTION_COL_ID {
                    Ok(Column::Action(ColumnData::external(data, range, &m)?))
                } else {
                    Ok(Column::Integer(ColumnData::external(data, range, &m)?))
                }
            }
            ColumnType::DeltaInteger => Ok(Column::Delta(ColumnData::external(data, range, &m)?)),
            ColumnType::Boolean => Ok(Column::Bool(ColumnData::external(data, range, &m)?)),
            ColumnType::Group => Ok(Column::Group(ColumnData::external(data, range, &m)?)),
            ColumnType::ValueMetadata => {
                Ok(Column::ValueMeta(ColumnData::external(data, range, &m)?))
            }
            ColumnType::Value => Ok(Column::Value(ColumnData::external(data, range, &m)?)),
        }
    }

    pub(crate) fn init_empty(spec: ColumnSpec, len: usize) -> Self {
        match spec.col_type() {
            ColumnType::Actor => Column::Actor(ColumnData::init_empty(len)),
            ColumnType::String => Column::Str(ColumnData::init_empty(len)),
            ColumnType::Integer => {
                if spec.id() == super::op_set::ACTION_COL_ID {
                    Column::Action(ColumnData::init_empty(len))
                } else {
                    Column::Integer(ColumnData::init_empty(len))
                }
            }
            ColumnType::DeltaInteger => Column::Delta(ColumnData::init_empty(len)),
            ColumnType::Boolean => Column::Bool(ColumnData::init_empty(len)),
            ColumnType::Group => Column::Group(ColumnData::init_empty(len)),
            ColumnType::ValueMetadata => Column::ValueMeta(ColumnData::init_empty(len)),
            ColumnType::Value => Column::Value(ColumnData::init_empty(len)),
        }
    }
}
