use std::convert::TryFrom;

use crate::{
    columnar::column_range::{
        generic::{GenericColumnRange, GroupRange, GroupedColumnRange, SimpleColRange},
        BooleanRange, DeltaRange, KeyRange, MaybeBooleanRange, ObjIdRange, OpIdListRange,
        OpIdRange, RleRange, ValueRange,
    },
    storage::{
        columns::{compression, ColumnId, ColumnSpec, ColumnType},
        Columns, MismatchingColumn, RawColumn, RawColumns,
    },
};

const OBJ_COL_ID: ColumnId = ColumnId::new(0);
const KEY_COL_ID: ColumnId = ColumnId::new(1);
const ID_COL_ID: ColumnId = ColumnId::new(2);
const INSERT_COL_ID: ColumnId = ColumnId::new(3);
const ACTION_COL_ID: ColumnId = ColumnId::new(4);
const VAL_COL_ID: ColumnId = ColumnId::new(5);
const SUCC_COL_ID: ColumnId = ColumnId::new(8);
const EXPAND_COL_ID: ColumnId = ColumnId::new(9);
const MARK_NAME_COL_ID: ColumnId = ColumnId::new(10);

#[derive(Debug, Clone)]
pub(crate) struct DocOpColumns {
    obj: Option<ObjIdRange>,
    key: KeyRange,
    id: OpIdRange,
    insert: BooleanRange,
    action: RleRange<u64>,
    val: ValueRange,
    succ: OpIdListRange,
    #[allow(dead_code)]
    other: Columns,
    expand: MaybeBooleanRange,
    mark_name: RleRange<smol_str::SmolStr>,
}

impl DocOpColumns {
    pub(crate) fn raw_columns(&self) -> RawColumns<compression::Uncompressed> {
        let mut cols = vec![
            RawColumn::new(
                ColumnSpec::new(OBJ_COL_ID, ColumnType::Actor, false),
                self.obj
                    .as_ref()
                    .map(|o| o.actor_range().clone().into())
                    .unwrap_or(0..0),
            ),
            RawColumn::new(
                ColumnSpec::new(OBJ_COL_ID, ColumnType::Integer, false),
                self.obj
                    .as_ref()
                    .map(|o| o.counter_range().clone().into())
                    .unwrap_or(0..0),
            ),
            RawColumn::new(
                ColumnSpec::new(KEY_COL_ID, ColumnType::Actor, false),
                self.key.actor_range().clone().into(),
            ),
            RawColumn::new(
                ColumnSpec::new(KEY_COL_ID, ColumnType::DeltaInteger, false),
                self.key.counter_range().clone().into(),
            ),
            RawColumn::new(
                ColumnSpec::new(KEY_COL_ID, ColumnType::String, false),
                self.key.string_range().clone().into(),
            ),
            RawColumn::new(
                ColumnSpec::new(ID_COL_ID, ColumnType::Actor, false),
                self.id.actor_range().clone().into(),
            ),
            RawColumn::new(
                ColumnSpec::new(ID_COL_ID, ColumnType::DeltaInteger, false),
                self.id.counter_range().clone().into(),
            ),
            RawColumn::new(
                ColumnSpec::new(INSERT_COL_ID, ColumnType::Boolean, false),
                self.insert.clone().into(),
            ),
            RawColumn::new(
                ColumnSpec::new(ACTION_COL_ID, ColumnType::Integer, false),
                self.action.clone().into(),
            ),
            RawColumn::new(
                ColumnSpec::new(VAL_COL_ID, ColumnType::ValueMetadata, false),
                self.val.meta_range().clone().into(),
            ),
        ];
        if !self.val.raw_range().is_empty() {
            cols.push(RawColumn::new(
                ColumnSpec::new(VAL_COL_ID, ColumnType::Value, false),
                self.val.raw_range().clone().into(),
            ));
        }
        cols.push(RawColumn::new(
            ColumnSpec::new(SUCC_COL_ID, ColumnType::Group, false),
            self.succ.group_range().clone().into(),
        ));
        if !self.succ.actor_range().is_empty() {
            cols.extend([
                RawColumn::new(
                    ColumnSpec::new(SUCC_COL_ID, ColumnType::Actor, false),
                    self.succ.actor_range().clone().into(),
                ),
                RawColumn::new(
                    ColumnSpec::new(SUCC_COL_ID, ColumnType::DeltaInteger, false),
                    self.succ.counter_range().clone().into(),
                ),
            ]);
        }
        if !self.expand.is_empty() {
            cols.push(RawColumn::new(
                ColumnSpec::new(EXPAND_COL_ID, ColumnType::Boolean, false),
                self.expand.clone().into(),
            ));
        }
        if !self.mark_name.is_empty() {
            cols.push(RawColumn::new(
                ColumnSpec::new(MARK_NAME_COL_ID, ColumnType::String, false),
                self.mark_name.clone().into(),
            ));
        }
        cols.into_iter().collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("mismatching column at {index}.")]
    MismatchingColumn { index: usize },
}

impl From<MismatchingColumn> for Error {
    fn from(m: MismatchingColumn) -> Self {
        Error::MismatchingColumn { index: m.index }
    }
}

impl TryFrom<Columns> for DocOpColumns {
    type Error = Error;

    fn try_from(columns: Columns) -> Result<Self, Self::Error> {
        let mut obj_actor: Option<RleRange<u64>> = None;
        let mut obj_ctr: Option<RleRange<u64>> = None;
        let mut key_actor: Option<RleRange<u64>> = None;
        let mut key_ctr: Option<DeltaRange> = None;
        let mut key_str: Option<RleRange<smol_str::SmolStr>> = None;
        let mut id_actor: Option<RleRange<u64>> = None;
        let mut id_ctr: Option<DeltaRange> = None;
        let mut insert: Option<BooleanRange> = None;
        let mut action: Option<RleRange<u64>> = None;
        let mut val: Option<ValueRange> = None;
        let mut succ_group: Option<RleRange<u64>> = None;
        let mut succ_actor: Option<RleRange<u64>> = None;
        let mut succ_ctr: Option<DeltaRange> = None;
        let mut expand: Option<MaybeBooleanRange> = None;
        let mut mark_name: Option<RleRange<smol_str::SmolStr>> = None;
        let mut other = Columns::empty(); // not doing anything with these here

        for (index, col) in columns.into_iter().enumerate() {
            match (col.id(), col.col_type()) {
                (ID_COL_ID, ColumnType::Actor) => id_actor = Some(col.range().into()),
                (ID_COL_ID, ColumnType::DeltaInteger) => id_ctr = Some(col.range().into()),
                (OBJ_COL_ID, ColumnType::Actor) => obj_actor = Some(col.range().into()),
                (OBJ_COL_ID, ColumnType::Integer) => obj_ctr = Some(col.range().into()),
                (KEY_COL_ID, ColumnType::Actor) => key_actor = Some(col.range().into()),
                (KEY_COL_ID, ColumnType::DeltaInteger) => key_ctr = Some(col.range().into()),
                (KEY_COL_ID, ColumnType::String) => key_str = Some(col.range().into()),
                (INSERT_COL_ID, ColumnType::Boolean) => insert = Some(col.range().into()),
                (ACTION_COL_ID, ColumnType::Integer) => action = Some(col.range().into()),
                (VAL_COL_ID, ColumnType::ValueMetadata) => match col.into_ranges() {
                    GenericColumnRange::Value(v) => val = Some(v),
                    _ => {
                        tracing::error!("col 9 should be a value column");
                        return Err(Error::MismatchingColumn { index });
                    }
                },
                (SUCC_COL_ID, ColumnType::Group) => match col.into_ranges() {
                    GenericColumnRange::Group(GroupRange { num, values }) => {
                        let mut cols = values.into_iter();
                        let first = cols.next();
                        let second = cols.next();
                        succ_group = Some(num);
                        match (first, second) {
                            (
                                Some(GroupedColumnRange::Simple(SimpleColRange::RleInt(
                                    actor_range,
                                ))),
                                Some(GroupedColumnRange::Simple(SimpleColRange::Delta(ctr_range))),
                            ) => {
                                succ_actor = Some(actor_range);
                                succ_ctr = Some(ctr_range);
                            }
                            (None, None) => {
                                succ_actor = Some((0..0).into());
                                succ_ctr = Some((0..0).into());
                            }
                            _ => {
                                tracing::error!(
                                    "expected a two column group of (actor, rle int) for index 10"
                                );
                                return Err(Error::MismatchingColumn { index });
                            }
                        };
                        if cols.next().is_some() {
                            return Err(Error::MismatchingColumn { index });
                        }
                    }
                    _ => return Err(Error::MismatchingColumn { index }),
                },
                (EXPAND_COL_ID, ColumnType::Boolean) => expand = Some(col.range().into()),
                (MARK_NAME_COL_ID, ColumnType::String) => mark_name = Some(col.range().into()),
                (other_col, other_type) => {
                    tracing::warn!(id=?other_col, typ=?other_type, "unknown column type");
                    other.append(col)
                }
            }
        }
        Ok(DocOpColumns {
            obj: ObjIdRange::new(
                obj_actor.unwrap_or_else(|| (0..0).into()),
                obj_ctr.unwrap_or_else(|| (0..0).into()),
            ),
            key: KeyRange::new(
                key_actor.unwrap_or_else(|| (0..0).into()),
                key_ctr.unwrap_or_else(|| (0..0).into()),
                key_str.unwrap_or_else(|| (0..0).into()),
            ),
            id: OpIdRange::new(
                id_actor.unwrap_or_else(|| (0..0).into()),
                id_ctr.unwrap_or_else(|| (0..0).into()),
            ),
            insert: insert.unwrap_or_else(|| (0..0).into()),
            action: action.unwrap_or_else(|| (0..0).into()),
            val: val.unwrap_or_else(|| ValueRange::new((0..0).into(), (0..0).into())),
            succ: OpIdListRange::new(
                succ_group.unwrap_or_else(|| (0..0).into()),
                succ_actor.unwrap_or_else(|| (0..0).into()),
                succ_ctr.unwrap_or_else(|| (0..0).into()),
            ),
            expand: expand.unwrap_or_else(|| (0..0).into()),
            mark_name: mark_name.unwrap_or_else(|| (0..0).into()),
            other,
        })
    }
}
