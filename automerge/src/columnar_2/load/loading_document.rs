use fxhash::FxBuildHasher;
use std::collections::{HashMap, BTreeSet};
use tracing::instrument;
use super::change_collector::ChangeCollector;

use crate::{
    columnar_2::{
        storage::Change as StoredChange,
        rowblock::{
            Key as DocOpKey,
            doc_change_columns::ChangeMetadata,
            doc_op_columns::DocOp,
            PrimVal,
        }
    },
    op_set::OpSet,
    op_tree::{OpSetMetadata, OpTree},
    types::{ActorId, ChangeHash, ElemId, Key, ObjId, ObjType, Op, OpId, OpType},
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("the document contained ops which were out of order")]
    OpsOutOfOrder,
    #[error("error reading operation: {0:?}")]
    ReadOp(Box<dyn std::error::Error>),
    #[error("an operation contained an invalid action")]
    InvalidAction,
    #[error("an operation referenced a missing actor id")]
    MissingActor,
    #[error("invalid changes: {0}")]
    InvalidChanges(#[from] super::change_collector::Error),
    #[error("mismatching heads")]
    MismatchingHeads,
}

struct LoadingObject {
    id: ObjId,
    ops: Vec<Op>,
    obj_type: ObjType,
    preds: HashMap<OpId, Vec<OpId>>,
}

impl LoadingObject {
    fn root() -> Self {
        LoadingObject {
            id: ObjId::root(),
            ops: Vec::new(),
            obj_type: ObjType::Map,
            preds: HashMap::new(),
        }
    }

    fn new(id: ObjId, obj_type: ObjType) -> Self {
        LoadingObject {
            id: id.into(),
            ops: Vec::new(),
            obj_type,
            preds: HashMap::new(),
        }
    }

    fn append_op(&mut self, op: Op) -> Result<(), Error> {
        if let Some(previous_op) = self.ops.last() {
            if op.key < previous_op.key {
                tracing::error!(
                    ?op,
                    ?previous_op,
                    "op key was smaller than key of previous op"
                );
                return Err(Error::OpsOutOfOrder);
            }
        }
        for succ in &op.succ {
            self.preds.entry(*succ).or_default().push(op.id);
        }
        self.ops.push(op);
        Ok(())
    }

    fn finish(mut self) -> (ObjId, ObjType, OpTree) {
        let mut op_tree = OpTree::new();
        for (index, mut op) in self.ops.into_iter().enumerate() {
            if let Some(preds) = self.preds.remove(&op.id) {
                op.pred = preds;
            }
            op_tree.insert(index, op);
        }
        (self.id, self.obj_type, op_tree)
    }
}

pub(crate) struct Loaded<'a> {
    pub(crate) op_set: OpSet,
    pub(crate) history: Vec<StoredChange<'a>>,
    pub(crate) history_index: HashMap<ChangeHash, usize>,
    pub(crate) actor_to_history: HashMap<usize, Vec<usize>>,
}

#[instrument(skip(actors, expected_heads, changes, ops))]
pub(crate) fn load<'a, I, C, OE, CE>(
    actors: Vec<ActorId>,
    expected_heads: BTreeSet<ChangeHash>,
    changes: C,
    ops: I,
) -> Result<Loaded<'static>, Error>
where
    OE: std::error::Error + 'static,
    CE: std::error::Error + 'static,
    I: Iterator<Item = Result<DocOp<'a>, OE>>,
    C: Iterator<Item = Result<ChangeMetadata<'a>, CE>>,
{
    let mut metadata = OpSetMetadata::from_actors(actors);
    let mut completed_objects = HashMap::<_, _, FxBuildHasher>::default();
    let mut current_object = LoadingObject::root();
    let mut collector = ChangeCollector::new(changes)?;
    let mut obj_types = HashMap::new();
    obj_types.insert(ObjId::root(), ObjType::Map);
    for op_res in ops {
        let doc_op = op_res.map_err(|e| Error::ReadOp(Box::new(e)))?;
        let obj = doc_op.object;
        let op = import_op(&mut metadata, doc_op)?;
        tracing::trace!(?op, "processing op");
        collector.collect(current_object.id, op.clone())?;

        // We have to record the object types of make operations so that when the object ID the
        // incoming operations refer to switches we can lookup the object type for the new object.
        // Ultimately we need this because the OpSet needs to know the object ID _and type_ for
        // each OpTree it tracks.
        if obj == current_object.id {
            match op.action {
                OpType::Make(obj_type) => {
                    obj_types.insert(op.id.into(), obj_type.clone());
                }
                _ => {}
            };
            current_object.append_op(op)?;
        } else {
            let new_obj_type = match obj_types.get(&obj) {
                Some(t) => Ok(t.clone()),
                None => {
                    tracing::error!(
                        ?op,
                        "operation referenced an object which we haven't seen a create op for yet"
                    );
                    Err(Error::OpsOutOfOrder)
                }
            }?;
            if obj < current_object.id {
                tracing::error!(?op, previous_obj=?current_object.id, "op referenced an object ID which was less than the previous object ID");
                return Err(Error::OpsOutOfOrder);
            } else {
                let (id, obj_type, op_tree) = current_object.finish();
                current_object = LoadingObject::new(obj, new_obj_type);
                current_object.append_op(op)?;
                completed_objects.insert(id, (obj_type, op_tree));
            }
        }
    }
    let super::change_collector::CollectedChanges{
        history,
        history_index,
        actor_to_history,
        heads,
    } = collector.finish(
        &metadata.actors,
        &metadata.props,
    )?;
    if expected_heads != heads {
        tracing::error!(?expected_heads, ?heads, "mismatching heads");
        return Err(Error::MismatchingHeads);
    }
    let (id, obj_type, op_tree) = current_object.finish();
    completed_objects.insert(id, (obj_type, op_tree));
    let op_set = OpSet::from_parts(completed_objects, metadata);

    Ok(Loaded {
        op_set,
        history,
        history_index,
        actor_to_history,
    })
}

#[instrument(skip(m))]
fn import_op<'a>(m: &mut OpSetMetadata, op: DocOp<'a>) -> Result<Op, Error> {
    let key = match op.key {
        DocOpKey::Prop(s) => Key::Map(m.import_prop(s)),
        DocOpKey::Elem(ElemId(op)) => Key::Seq(ElemId(check_opid(m, op)?)),
    };
    for opid in &op.succ {
        if m.actors.safe_get(opid.actor()).is_none() {
            tracing::error!(?opid, "missing actor");
            return Err(Error::MissingActor);
        }
    }
    Ok(Op {
        id: check_opid(m, op.id)?,
        action: parse_optype(op.action, op.value)?,
        key,
        succ: op.succ,
        pred: Vec::new(),
        insert: op.insert,
    })
}

/// We construct the OpSetMetadata directly from the vector of actors which are encoded in the
/// start of the document. Therefore we need to check for each opid in the docuemnt that the actor
/// ID which it references actually exists in the metadata.
#[tracing::instrument(skip(m))]
fn check_opid(m: &OpSetMetadata, opid: OpId) -> Result<OpId, Error> {
    match m.actors.safe_get(opid.actor()) {
        Some(_) => Ok(opid),
        None => {
            tracing::error!("missing actor");
            Err(Error::MissingActor)
        }
    }
}

fn parse_optype<'a>(action_index: usize, value: PrimVal<'a>) -> Result<OpType, Error> {
    match action_index {
        0 => Ok(OpType::Make(ObjType::Map)),
        1 => Ok(OpType::Set(value.into())),
        2 => Ok(OpType::Make(ObjType::List)),
        3 => Ok(OpType::Del),
        4 => Ok(OpType::Make(ObjType::Text)),
        5 => match value {
            PrimVal::Int(i) => Ok(OpType::Inc(i)),
            _ => {
                tracing::error!(?value, "invalid value for counter op");
                Err(Error::InvalidAction)
            }
        },
        6 => Ok(OpType::Make(ObjType::Table)),
        other => {
            tracing::error!(action = other, "unknown action type");
            Err(Error::InvalidAction)
        }
    }
}
