use super::meta::{MetaCursor, ValueMeta};
use super::op::Op;
use super::packer::{BooleanCursor, DeltaCursor, Encoder, RawCursor, StrCursor, UIntCursor};
use super::types::{ActionCursor, ActorCursor, ActorIdx};
use crate::types::OpId;
use fxhash::FxBuildHasher;
use itertools::Itertools;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};

/*
const OBJ_COL_ID: ColumnId = ColumnId::new(0);
const KEY_COL_ID: ColumnId = ColumnId::new(1);
const INSERT_COL_ID: ColumnId = ColumnId::new(3);
const ACTION_COL_ID: ColumnId = ColumnId::new(4);
const VAL_COL_ID: ColumnId = ColumnId::new(5);
const PRED_COL_ID: ColumnId = ColumnId::new(7);
const EXPAND_COL_ID: ColumnId = ColumnId::new(9);
const MARK_NAME_COL_ID: ColumnId = ColumnId::new(10);
*/

/*
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ChangeOp {
    pub(crate) key: Key,
    pub(crate) insert: bool,
    pub(crate) val: ScalarValue,
    pub(crate) pred: Vec<OpId>,
    pub(crate) action: u64,
    pub(crate) obj: ObjId,
    pub(crate) expand: bool,
    pub(crate) mark_name: Option<smol_str::SmolStr>,
}
*/

#[derive(Debug, Clone)]
pub(crate) struct ChangeBuilder<'a> {
    start_op: u64,
    len: u64,
    pending_ops: BTreeSet<PendingOp<'a>>,
    writer: ChangeWriter<'a>,
}

#[derive(Debug, Clone)]
struct PendingOp<'a> {
    op: Op<'a>,
    pred: Vec<OpId>,
}

impl<'a> PartialEq for PendingOp<'a> {
    fn eq(&self, other: &PendingOp<'a>) -> bool {
        self.op.id == other.op.id
    }
}

impl<'a> Eq for PendingOp<'a> {}

impl<'a> PartialOrd for PendingOp<'a> {
    fn partial_cmp(&self, other: &PendingOp<'a>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for PendingOp<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.op.id.cmp(&other.op.id)
    }
}

impl<'a> ChangeBuilder<'a> {
    pub(crate) fn new(start_op: u64) -> Self {
        ChangeBuilder {
            start_op,
            len: 0,
            pending_ops: BTreeSet::default(),
            writer: ChangeWriter::default(),
        }
    }

    pub(crate) fn append(&mut self, op: Op<'a>, pred: Vec<OpId>) {
        if op.id.counter() == self.start_op + self.len {
            self.writer.append(op, pred);
            self.len += 1;
            while let Some(pending) = self.pending_ops.first() {
                if pending.op.id.counter() != self.start_op + self.len {
                    break;
                }
                let pending = self.pending_ops.pop_first().unwrap();
                self.writer.append(pending.op, pending.pred);
                self.len += 1;
            }
        } else {
            self.pending_ops.insert(PendingOp { op, pred });
        }
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct ChangeWriter<'a> {
    actors: HashSet<ActorIdx, FxBuildHasher>,
    obj_actor: Encoder<'a, ActorCursor>,
    obj_ctr: Encoder<'a, UIntCursor>,
    key_actor: Encoder<'a, ActorCursor>,
    key_ctr: Encoder<'a, DeltaCursor>,
    key_str: Encoder<'a, StrCursor>,
    insert: Encoder<'a, BooleanCursor>,
    action: Encoder<'a, ActionCursor>,
    value_meta: Encoder<'a, MetaCursor>,
    value: Encoder<'a, RawCursor>,
    pred_count: Encoder<'a, UIntCursor>,
    pred_actor: Encoder<'a, ActorCursor>,
    pred_ctr: Encoder<'a, DeltaCursor>,
    expand: Encoder<'a, BooleanCursor>,
    mark_name: Encoder<'a, StrCursor>,
}

impl<'a> ChangeWriter<'a> {
    fn _finish(self) {
        let _obj_actor = self.obj_actor.finish();
    }

    fn append(&mut self, op: Op<'a>, pred: Vec<OpId>) {
        self.record_actor(op.obj.actor());
        self.record_actor(op.key.actor());
        self.obj_actor.append(op.obj.actor());
        self.obj_ctr.append(op.obj.counter());
        self.key_actor.append(op.key.actor());
        self.key_ctr.append(op.key.icounter());
        self.key_str.append(op.key.key_str());
        self.insert.append(Some(op.insert));
        self.action.append(Some(op.action));
        self.value_meta.append(Some(ValueMeta::from(&op.value)));
        self.value.append_bytes(op.value.to_raw());
        self.expand.append(Some(op.expand));
        self.mark_name.append(op.mark_name);
        self.pred_count.append(Some(pred.len() as u64));
        for p in pred {
            self.record_actor(Some(p.actoridx()));
            self.pred_actor.append(Some(p.actoridx()));
            self.pred_ctr.append(Some(p.icounter()));
        }
    }

    #[allow(dead_code)]
    fn record_actor(&mut self, actor: Option<ActorIdx>) {
        if let Some(a) = actor {
            self.actors.insert(a);
        }
    }

    #[allow(dead_code)]
    fn rewrite_actors(&mut self, _map: HashMap<ActorIdx, ActorIdx, FxBuildHasher>) {
        /*
                 self.obj_actor.remap(|v| *map.get(&v?));
                 self.key_actor.remap(|v| *map.get(&v?));
                 self.pred_actor.remap(|v| *map.get(&v?));

                         let new_ids = col_data
                             .iter()
                             .map(|a| match a {
                                 Some(ActorIdx(id)) if id as usize >= idx => Some(ActorIdx(id + 1)),
                                 old => old,
                             })
                             .collect::<Vec<_>>();
                         let mut new_data = ColumnData::<ActorCursor>::new();
                         new_data.splice(0, 0, new_ids);
                         std::mem::swap(col_data, &mut new_data);
        */
    }

    #[allow(dead_code)]
    fn remap_actors(&mut self, actor: ActorIdx) -> HashMap<ActorIdx, ActorIdx, FxBuildHasher> {
        let mut actors = std::mem::take(&mut self.actors);
        let mut mapping = HashMap::default();
        let mut index = 0;

        actors.remove(&actor);
        mapping.insert(actor, ActorIdx(index));

        for actor in actors.into_iter().sorted() {
            index += 1;
            mapping.insert(actor, ActorIdx(index));
        }

        mapping
    }
}

/*
        for (index, col) in columns.into_iter().enumerate() {
            match (col.id(), col.col_type()) {
                (OBJ_COL_ID, ColumnType::Actor) => obj_actor = Some(col.range().into()),
                (OBJ_COL_ID, ColumnType::Integer) => obj_ctr = Some(col.range().into()),
                (KEY_COL_ID, ColumnType::Actor) => key_actor = Some(col.range().into()),
                (KEY_COL_ID, ColumnType::DeltaInteger) => key_ctr = Some(col.range().into()),
                (KEY_COL_ID, ColumnType::String) => key_str = Some(col.range().into()),
                (INSERT_COL_ID, ColumnType::Boolean) => insert = Some(col.range()),
                (ACTION_COL_ID, ColumnType::Integer) => action = Some(col.range()),
                (VAL_COL_ID, ColumnType::ValueMetadata) => match col.into_ranges() {
                    GenericColumnRange::Value(v) => {
                        val = Some(v);
                    }
                    _ => return Err(ParseChangeColumnsError::MismatchingColumn { index }),
                },
                (PRED_COL_ID, ColumnType::Group) => match col.into_ranges() {
                    GenericColumnRange::Group(GroupRange { num, values }) => {
                        let mut cols = values.into_iter();
                        pred_group = Some(num);
                        // If there was no data in the group at all then the columns won't be
                        // present
                        if cols.len() == 0 {
                            pred_actor = Some((0..0).into());
                            pred_ctr = Some((0..0).into());
                        } else {
                            let first = cols.next();
                            let second = cols.next();
                            match (first, second) {
                                (
                                    Some(GroupedColumnRange::Simple(SimpleColRange::RleInt(
                                        actor_range,
                                    ))),
                                    Some(GroupedColumnRange::Simple(SimpleColRange::Delta(
                                        ctr_range,
                                    ))),
                                ) => {
                                    pred_actor = Some(actor_range);
                                    pred_ctr = Some(ctr_range);
                                }
                                _ => {
                                    return Err(ParseChangeColumnsError::MismatchingColumn {
                                        index,
                                    })
                                }
                            }
                        }
                        if cols.next().is_some() {
                            return Err(ParseChangeColumnsError::MismatchingColumn { index });
                        }
                    }
                    _ => return Err(ParseChangeColumnsError::MismatchingColumn { index }),
                },
                (EXPAND_COL_ID, ColumnType::Boolean) => expand = Some(col.range().into()),
                (MARK_NAME_COL_ID, ColumnType::String) => mark_name = Some(col.range().into()),
                (other_type, other_col) => {
                    tracing::warn!(typ=?other_type, id=?other_col, "unknown column");
                    other.append(col);
                }

*/
