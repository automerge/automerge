use super::change_collector::ChangeCollector;
use std::collections::{BTreeSet, HashMap};

use crate::storage::document::ReadDocOpError;
use crate::{
    change::Change,
    columnar::Key as DocOpKey,
    op_set::{OpIdx, OpSet, OpSetData},
    storage::{change::Verified, Change as StoredChange, DocOp, Document},
    types::{ChangeHash, ElemId, Key, ObjId, OpBuilder, OpId, OpIds, OpType},
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("the document contained ops which were out of order")]
    OpsOutOfOrder,
    #[error("error reading operation: {0:?}")]
    ReadOp(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("an operation referenced a missing actor id")]
    MissingActor,
    #[error("invalid changes: {0}")]
    InvalidChanges(#[from] super::change_collector::Error),
    #[error("mismatching heads")]
    MismatchingHeads(MismatchedHeads),
    #[error("succ out of order")]
    SuccOutOfOrder,
    #[error(transparent)]
    InvalidOp(#[from] crate::error::InvalidOpType),
}

pub(crate) struct MismatchedHeads {
    changes: Vec<StoredChange<'static, Verified>>,
    expected_heads: BTreeSet<ChangeHash>,
    derived_heads: BTreeSet<ChangeHash>,
}

impl std::fmt::Debug for MismatchedHeads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MismatchedHeads")
            .field("changes", &self.changes.len())
            .field("expected_heads", &self.expected_heads)
            .field("derived_heads", &self.derived_heads)
            .finish()
    }
}

#[derive(Clone, Copy, Debug)]
pub enum VerificationMode {
    Check,
    DontCheck,
}

#[derive(Clone, Debug)]
struct NextDocOp {
    op: OpBuilder,
    succ: OpIds,
    key: Key,
    opid: OpId,
    obj: ObjId,
}

fn next_op<'a, I>(iter: &mut I, op_set: &mut OpSet) -> Result<Option<NextDocOp>, Error>
where
    I: Iterator<Item = Result<DocOp, ReadDocOpError>> + Clone + 'a,
{
    let op_res = iter.next();
    if let Some(op_res) = op_res {
        let doc_op = op_res.map_err(|e| Error::ReadOp(Box::new(e)))?;
        let obj = doc_op.object;
        check_opid(&op_set.osd, *obj.opid())?;
        let (op, succ) = import_op(&mut op_set.osd, doc_op)?;
        let opid = op.id;
        let key = op.elemid_or_key();
        Ok(Some(NextDocOp {
            op,
            succ,
            opid,
            key,
            obj,
        }))
    } else {
        Ok(None)
    }
}

struct ReconstructionState<'a> {
    op_set: OpSet,
    max_op: u64,
    last_obj: Option<ObjId>,
    last_key: Option<Key>,
    pred: HashMap<OpId, Vec<OpIdx>>,
    ops_collecter: Vec<OpIdx>,
    change_collector: ChangeCollector<'a>,
}

impl<'a> ReconstructionState<'a> {
    fn new(doc: &'a Document<'a>) -> Result<Self, Error> {
        Ok(Self {
            op_set: OpSet::from_actors(doc.actors().to_vec()),
            max_op: 0,
            last_obj: None,
            last_key: None,
            pred: HashMap::default(),
            ops_collecter: Vec::default(),
            change_collector: ChangeCollector::new(doc.iter_changes())?,
        })
    }
}

pub(crate) fn reconstruct_opset<'a>(
    doc: &'a Document<'a>,
    mode: VerificationMode,
) -> Result<ReconOpSet, Error> {
    let mut state = ReconstructionState::new(doc)?;
    let mut iter_ops = doc.iter_ops();
    let mut next = next_op(&mut iter_ops, &mut state.op_set)?;
    while let Some(NextDocOp {
        op,
        succ,
        key,
        opid,
        obj,
    }) = next
    {
        state.max_op = std::cmp::max(state.max_op, opid.counter());

        let idx = state.op_set.load(obj, op);

        for id in &succ {
            state
                .pred
                .entry(*id)
                .and_modify(|v| v.push(idx))
                .or_insert_with(|| vec![idx]);
        }

        if let Some(pred_idxs) = state.pred.get(&opid) {
            for p in pred_idxs {
                state.op_set.osd.add_pred(*p, idx);
            }
            state.pred.remove(&opid);
        }

        state.ops_collecter.push(idx);
        state.change_collector.collect(opid, idx)?;

        state.last_key = Some(key);
        state.last_obj = Some(obj);

        next = next_op(&mut iter_ops, &mut state.op_set)?;

        flush_ops(&obj, next.as_ref(), &mut state)?;
    }

    state.op_set.add_indexes();

    let op_set = state.op_set;
    let change_collector = state.change_collector;
    let max_op = state.max_op;

    let (changes, heads) = flush_changes(change_collector, doc, mode, &op_set.osd)?;

    Ok(ReconOpSet {
        changes,
        max_op,
        op_set,
        heads,
    })
}

// create all binary changes
// look for mismatched heads

fn flush_changes(
    change_collector: ChangeCollector<'_>,
    doc: &Document<'_>,
    mode: VerificationMode,
    osd: &OpSetData,
) -> Result<(Vec<Change>, BTreeSet<ChangeHash>), Error> {
    let super::change_collector::CollectedChanges { history, heads } =
        change_collector.finish(osd)?;
    if matches!(mode, VerificationMode::Check) {
        let expected_heads: BTreeSet<_> = doc.heads().iter().cloned().collect();
        if expected_heads != heads {
            tracing::error!(?expected_heads, ?heads, "mismatching heads");
            return Err(Error::MismatchingHeads(MismatchedHeads {
                changes: history,
                expected_heads,
                derived_heads: heads,
            }));
        }
    }
    let changes = history.into_iter().map(Change::new).collect();
    Ok((changes, heads))
}

// after we see all ops for a given obj/key we can detect delets (this is more complex with MOVE)
// also visibility for counters requires all ops to be observed before pushing them into op_tree because of visibility calculations

fn flush_ops(
    obj: &ObjId,
    next: Option<&NextDocOp>,
    state: &mut ReconstructionState<'_>,
) -> Result<(), Error> {
    let next_key = next.map(|n| n.key);
    let next_obj = next.map(|n| n.obj);

    if next_obj.is_some() && next_obj < state.last_obj {
        return Err(Error::OpsOutOfOrder);
    }

    if next.is_none() || next_key != state.last_key || next_obj != state.last_obj {
        for (opid, preds) in &state.pred {
            let del = OpBuilder {
                id: *opid,
                insert: false,
                key: state.last_key.unwrap(),
                action: OpType::Delete,
            };
            state.max_op = std::cmp::max(state.max_op, opid.counter());
            let del_idx = state.op_set.load(state.last_obj.unwrap(), del);
            for p in preds {
                state.op_set.osd.add_dep(*p, del_idx);
            }
            state.change_collector.collect(*opid, del_idx)?;
        }
        state.pred.clear();

        for idx in &state.ops_collecter {
            state
                .op_set
                .load_idx(obj, *idx)
                .map_err(|e| Error::ReadOp(Box::new(e)))?;
        }

        state.ops_collecter.truncate(0)
    }
    Ok(())
}

pub(crate) struct ReconOpSet {
    pub(crate) changes: Vec<Change>,
    pub(crate) max_op: u64,
    pub(crate) op_set: OpSet,
    pub(crate) heads: BTreeSet<ChangeHash>,
}

fn import_op(osd: &mut OpSetData, op: DocOp) -> Result<(OpBuilder, OpIds), Error> {
    let key = match op.key {
        DocOpKey::Prop(s) => Key::Map(osd.import_prop(s)),
        DocOpKey::Elem(ElemId(op)) => Key::Seq(ElemId(check_opid(osd, op)?)),
    };
    for opid in &op.succ {
        if osd.actors.safe_get(opid.actor()).is_none() {
            tracing::error!(?opid, "missing actor");
            return Err(Error::MissingActor);
        }
    }
    let action = OpType::from_action_and_value(op.action, op.value, op.mark_name, op.expand);
    let succ = osd.try_sorted_opids(op.succ).ok_or(Error::SuccOutOfOrder)?;
    Ok((
        OpBuilder {
            id: check_opid(osd, op.id)?,
            action,
            key,
            insert: op.insert,
        },
        succ,
    ))
}

/// We construct the OpSetData directly from the vector of actors which are encoded in the
/// start of the document. Therefore we need to check for each opid in the docuemnt that the actor
/// ID which it references actually exists in the op set data.
fn check_opid(osd: &OpSetData, opid: OpId) -> Result<OpId, Error> {
    match osd.actors.safe_get(opid.actor()) {
        Some(_) => Ok(opid),
        None => {
            tracing::error!("missing actor");
            Err(Error::MissingActor)
        }
    }
}
