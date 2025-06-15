use std::{borrow::Cow, convert::TryFrom, ops::Range};

use hexane::{BooleanCursor, ColumnCursor, DeltaCursor, RawCursor, StrCursor, UIntCursor};

use crate::{
    change_graph::ChangeGraph,
    columnar::column_range::generic::{
        GenericColumnRange, GroupRange, GroupedColumnRange, SimpleColRange,
    },
    op_set2::{
        change::ActorMapper, op::OpLike, types::ActionCursor, ActorCursor, ActorIdx, MetaCursor,
        Op, OpSet,
    },
    storage::{
        columns::{compression, ColumnId, ColumnSpec, ColumnType},
        Columns, MismatchingColumn, RawColumn, RawColumns,
    },
};

use super::{reify_deletes::ReifiedDeletes, CommitRangeClocks};

const OBJ_COL_ID: ColumnId = ColumnId::new(0);
const KEY_COL_ID: ColumnId = ColumnId::new(1);
const ID_COL_ID: ColumnId = ColumnId::new(2);
const INSERT_COL_ID: ColumnId = ColumnId::new(3);
const ACTION_COL_ID: ColumnId = ColumnId::new(4);
const VAL_COL_ID: ColumnId = ColumnId::new(5);
const PRED_COL_ID: ColumnId = ColumnId::new(8);
const SUCC_COL_ID: ColumnId = ColumnId::new(9);
const EXPAND_COL_ID: ColumnId = ColumnId::new(10);
const MARK_NAME_COL_ID: ColumnId = ColumnId::new(11);

#[derive(Debug, Clone)]
pub(crate) struct BundleOpColumns {
    offset: usize,
    id_actor: Range<usize>,
    id_counter: Range<usize>,
    obj_actor: Range<usize>,
    obj_counter: Range<usize>,
    key_actor: Range<usize>,
    key_counter: Range<usize>,
    key_str: Range<usize>,
    insert: Range<usize>,
    action: Range<usize>,
    value_meta: Range<usize>,
    value_raw: Range<usize>,
    succ_count: Range<usize>,
    succ_actor: Range<usize>,
    succ_counter: Range<usize>,
    pred_count: Range<usize>,
    pred_actor: Range<usize>,
    pred_counter: Range<usize>,
    expand: Range<usize>,
    mark_name: Range<usize>,
    #[allow(dead_code)]
    other: Vec<Range<usize>>,
}

impl BundleOpColumns {
    pub(crate) fn new<'a, I: Iterator<Item = Op<'a>> + Clone>(
        data: &mut Vec<u8>,
        reified_deletes: &ReifiedDeletes<'_>,
        range_clocks: &[CommitRangeClocks],
    ) -> Self {
        let offset = data.len();

        let remap = move |actor: Option<Cow<'_, ActorIdx>>| {
            actor.map(|a| Cow::Owned(reified_deletes.map_actor(&a)))
        };

        let ops = reified_deletes.iter();

        let id_actor = ActorCursor::encode(
            data,
            ops.clone()
                .map(|o| Some(Cow::Owned(o.id.actoridx())))
                .map(&remap),
            false,
        );
        let id_counter = UIntCursor::encode(
            data,
            ops.clone().map(|o| Some(Cow::Owned(o.id.counter()))),
            false,
        );

        let obj_actor = ActorCursor::encode(
            data,
            ops.clone()
                .map(|o| o.obj.actor().map(Cow::Owned))
                .map(&remap),
            false,
        );
        let obj_counter = UIntCursor::encode(
            data,
            ops.clone().map(|o| o.obj.counter().map(Cow::Owned)),
            false,
        );
        let key_actor = ActorCursor::encode(
            data,
            ops.clone()
                .map(|o| o.key.actor().map(Cow::Owned))
                .map(&remap),
            false,
        );
        let key_counter = DeltaCursor::encode(
            data,
            ops.clone().map(|o| o.key.icounter().map(Cow::Owned)),
            false,
        );
        let key_str = StrCursor::encode(data, ops.clone().map(|o| o.key.key_str()), false);
        let insert =
            BooleanCursor::encode(data, ops.clone().map(|o| Some(Cow::Owned(o.insert))), true); // force
        let action =
            ActionCursor::encode(data, ops.clone().map(|o| Some(Cow::Owned(o.action))), false);
        let value_meta = MetaCursor::encode(
            data,
            ops.clone().map(|o| Some(Cow::Owned(o.meta_value()))),
            false,
        );
        let value_raw = RawCursor::encode(
            data,
            // This into_owned feels bad
            ops.clone()
                .map(|o| o.raw_value().map(|v| Cow::Owned(v.into_owned()))),
            false,
        );
        let pred_count = UIntCursor::encode(
            data,
            ops.clone().map(|o| {
                Some(Cow::Owned(
                    reified_deletes
                        .preds
                        .get(&o.id)
                        .map(|ps| ps.len() as u64)
                        .unwrap_or(0),
                ))
            }),
            false,
        );

        let pred_iter = ops
            .clone()
            .filter_map(|o| reified_deletes.preds.get(&o.id))
            .flat_map(|pred_ids| pred_ids.iter());
        let pred_actor_iter = pred_iter
            .clone()
            .map(|pred_id| Some(Cow::Owned(pred_id.actoridx())))
            .map(&remap);
        let pred_actor = ActorCursor::encode(data, pred_actor_iter, false);
        let pred_ctr_iter = pred_iter.map(|pred| Some(Cow::Owned(pred.counter() as i64)));
        let pred_counter = DeltaCursor::encode(data, pred_ctr_iter, false);

        let succ_count = UIntCursor::encode(
            data,
            ops.clone().map(|o| Some(Cow::Owned(o.succ().len() as u64))),
            false,
        );
        let succ_iter = ops.clone().flat_map(|o| {
            o.succ()
                .filter(|o| range_clocks.iter().any(|r| r.covers(o)))
        });
        let succ_actor_iter = succ_iter
            .clone()
            .map(|succ_id| Some(Cow::Owned(succ_id.actoridx())))
            .map(&remap);
        let succ_actor = ActorCursor::encode(data, succ_actor_iter, false);
        let succ_ctr_iter = succ_iter.map(|succ| Some(Cow::Owned(succ.counter() as i64)));
        let succ_counter = DeltaCursor::encode(data, succ_ctr_iter, false);

        let expand =
            BooleanCursor::encode(data, ops.clone().map(|o| Some(Cow::Owned(o.expand))), false);
        let mark_name = StrCursor::encode(data, ops.clone().map(|o| o.mark_name), false);

        Self {
            offset,
            id_actor,
            id_counter,
            obj_actor,
            obj_counter,
            key_actor,
            key_counter,
            key_str,
            insert,
            action,
            value_meta,
            value_raw,
            succ_count,
            succ_actor,
            succ_counter,
            pred_count,
            pred_actor,
            pred_counter,
            expand,
            mark_name,
            other: Vec::new(),
        }
    }

    pub(crate) fn raw_columns(&self) -> RawColumns<compression::Uncompressed> {
        let mut cols = vec![
            //id
            RawColumn::new(
                ColumnSpec::new(ID_COL_ID, ColumnType::Actor, false),
                self.id_actor.clone(),
            ),
            RawColumn::new(
                ColumnSpec::new(ID_COL_ID, ColumnType::DeltaInteger, false),
                self.id_counter.clone(),
            ),
            //obj
            RawColumn::new(
                ColumnSpec::new(OBJ_COL_ID, ColumnType::Actor, false),
                self.obj_actor.clone(),
            ),
            RawColumn::new(
                ColumnSpec::new(OBJ_COL_ID, ColumnType::Integer, false),
                self.obj_counter.clone(),
            ),
            //key
            RawColumn::new(
                ColumnSpec::new(KEY_COL_ID, ColumnType::Actor, false),
                self.key_actor.clone(),
            ),
            RawColumn::new(
                ColumnSpec::new(KEY_COL_ID, ColumnType::DeltaInteger, false),
                self.key_counter.clone(),
            ),
            RawColumn::new(
                ColumnSpec::new(KEY_COL_ID, ColumnType::String, false),
                self.key_str.clone(),
            ),
            //insert
            RawColumn::new(
                ColumnSpec::new(INSERT_COL_ID, ColumnType::Boolean, false),
                self.insert.clone(),
            ),
            RawColumn::new(
                ColumnSpec::new(ACTION_COL_ID, ColumnType::Integer, false),
                self.action.clone(),
            ),
            RawColumn::new(
                ColumnSpec::new(VAL_COL_ID, ColumnType::ValueMetadata, false),
                self.value_meta.clone(),
            ),
        ];
        if !self.value_raw.is_empty() {
            cols.push(RawColumn::new(
                ColumnSpec::new(VAL_COL_ID, ColumnType::Value, false),
                self.value_raw.clone(),
            ));
        }

        // succ
        cols.push(RawColumn::new(
            ColumnSpec::new(SUCC_COL_ID, ColumnType::Group, false),
            self.succ_count.clone(),
        ));
        if !self.succ_actor.is_empty() {
            cols.extend([
                RawColumn::new(
                    ColumnSpec::new(SUCC_COL_ID, ColumnType::Actor, false),
                    self.succ_actor.clone(),
                ),
                RawColumn::new(
                    ColumnSpec::new(SUCC_COL_ID, ColumnType::DeltaInteger, false),
                    self.succ_counter.clone(),
                ),
            ]);
        }

        // pred
        cols.push(RawColumn::new(
            ColumnSpec::new(PRED_COL_ID, ColumnType::Group, false),
            self.pred_count.clone(),
        ));
        if !self.succ_actor.is_empty() {
            cols.extend([
                RawColumn::new(
                    ColumnSpec::new(PRED_COL_ID, ColumnType::Actor, false),
                    self.pred_actor.clone(),
                ),
                RawColumn::new(
                    ColumnSpec::new(PRED_COL_ID, ColumnType::DeltaInteger, false),
                    self.pred_counter.clone(),
                ),
            ]);
        }

        if !self.expand.is_empty() {
            cols.push(RawColumn::new(
                ColumnSpec::new(EXPAND_COL_ID, ColumnType::Boolean, false),
                self.expand.clone(),
            ));
        }
        if !self.mark_name.is_empty() {
            cols.push(RawColumn::new(
                ColumnSpec::new(MARK_NAME_COL_ID, ColumnType::String, false),
                self.mark_name.clone(),
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

impl TryFrom<Columns> for BundleOpColumns {
    type Error = Error;

    fn try_from(columns: Columns) -> Result<Self, Self::Error> {
        let mut id_actor: Option<Range<usize>> = None;
        let mut id_counter: Option<Range<usize>> = None;
        let mut obj_actor: Option<Range<usize>> = None;
        let mut obj_counter: Option<Range<usize>> = None;
        let mut key_actor: Option<Range<usize>> = None;
        let mut key_counter: Option<Range<usize>> = None;
        let mut key_str: Option<Range<usize>> = None;
        let mut insert: Option<Range<usize>> = None;
        let mut action: Option<Range<usize>> = None;
        let mut value_meta: Option<Range<usize>> = None;
        let mut value_raw: Option<Range<usize>> = None;
        let mut succ_group: Option<Range<usize>> = None;
        let mut succ_actor: Option<Range<usize>> = None;
        let mut succ_counter: Option<Range<usize>> = None;
        let mut pred_group: Option<Range<usize>> = None;
        let mut pred_actor: Option<Range<usize>> = None;
        let mut pred_counter: Option<Range<usize>> = None;
        let mut expand: Option<Range<usize>> = None;
        let mut mark_name: Option<Range<usize>> = None;
        let mut other = Vec::new();

        for (index, col) in columns.into_iter().enumerate() {
            match (col.id(), col.col_type()) {
                (ID_COL_ID, ColumnType::Actor) => id_actor = Some(col.range().into()),
                (ID_COL_ID, ColumnType::DeltaInteger) => id_counter = Some(col.range().into()),
                (OBJ_COL_ID, ColumnType::Actor) => obj_actor = Some(col.range().into()),
                (OBJ_COL_ID, ColumnType::Integer) => obj_counter = Some(col.range().into()),
                (KEY_COL_ID, ColumnType::Actor) => key_actor = Some(col.range().into()),
                (KEY_COL_ID, ColumnType::DeltaInteger) => key_counter = Some(col.range().into()),
                (KEY_COL_ID, ColumnType::String) => key_str = Some(col.range().into()),
                (INSERT_COL_ID, ColumnType::Boolean) => insert = Some(col.range().into()),
                (ACTION_COL_ID, ColumnType::Integer) => action = Some(col.range().into()),
                (VAL_COL_ID, ColumnType::ValueMetadata) => match col.into_ranges() {
                    GenericColumnRange::Value(v) => {
                        value_meta = Some(v.meta_range().clone().into());
                        value_raw = Some(v.raw_range().clone().into());
                    }
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
                        succ_group = Some(num.into());
                        match (first, second) {
                            (
                                Some(GroupedColumnRange::Simple(SimpleColRange::RleInt(
                                    actor_range,
                                ))),
                                Some(GroupedColumnRange::Simple(SimpleColRange::Delta(ctr_range))),
                            ) => {
                                succ_actor = Some(actor_range.into());
                                succ_counter = Some(ctr_range.into());
                            }
                            (None, None) => {
                                succ_actor = Some((0..0).into());
                                succ_counter = Some((0..0).into());
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
                (PRED_COL_ID, ColumnType::Group) => match col.into_ranges() {
                    GenericColumnRange::Group(GroupRange { num, values }) => {
                        let mut cols = values.into_iter();
                        let first = cols.next();
                        let second = cols.next();
                        pred_group = Some(num.into());
                        match (first, second) {
                            (
                                Some(GroupedColumnRange::Simple(SimpleColRange::RleInt(
                                    actor_range,
                                ))),
                                Some(GroupedColumnRange::Simple(SimpleColRange::Delta(ctr_range))),
                            ) => {
                                pred_actor = Some(actor_range.into());
                                pred_counter = Some(ctr_range.into());
                            }
                            (None, None) => {
                                pred_actor = Some((0..0).into());
                                pred_counter = Some((0..0).into());
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
                    other.push(col.range())
                }
            }
        }
        Ok(BundleOpColumns {
            offset: 0, //really?
            id_actor: id_actor.unwrap_or_default(),
            id_counter: id_counter.unwrap_or_default(),
            obj_actor: obj_actor.unwrap_or_default(),
            obj_counter: obj_counter.unwrap_or_default(),
            key_actor: key_actor.unwrap_or_default(),
            key_counter: key_counter.unwrap_or_default(),
            key_str: key_str.unwrap_or_default(),
            insert: insert.unwrap_or_default(),
            action: action.unwrap_or_default(),
            value_meta: value_meta.unwrap_or_default(),
            value_raw: value_raw.unwrap_or_default(),
            succ_count: succ_group.unwrap_or_default(),
            succ_actor: succ_actor.unwrap_or_default(),
            succ_counter: succ_counter.unwrap_or_default(),
            pred_count: pred_group.unwrap_or_default(),
            pred_actor: pred_actor.unwrap_or_default(),
            pred_counter: pred_counter.unwrap_or_default(),
            expand: expand.unwrap_or_default(),
            mark_name: mark_name.unwrap_or_default(),
            other,
        })
    }
}
