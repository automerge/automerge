use super::change_collector::ChangeCollector;
use std::collections::{BTreeSet, HashMap};

use crate::{
    change::Change,
    op_set2::{KeyRef, OpBuilder2, OpSet, PackError, ReadOpError},
    storage::{change::Verified, Change as StoredChange, Document},
    types::{ChangeHash, ListEncoding, ObjId, OpId},
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    // FIXME - I need to do this check
    //#[error("the document contained ops which were out of order")]
    //OpsOutOfOrder,
    #[error("invalid changes: {0}")]
    InvalidChanges(#[from] super::change_collector::Error),
    #[error("mismatched max_op ops={0}, changes={0}")]
    MismatchingMaxOp(u64, u64),
    #[error("mismatching heads")]
    MismatchingHeads(MismatchedHeads),
    // FIXME - i need to do this check
    //#[error("succ out of order")]
    //SuccOutOfOrder,
    #[error(transparent)]
    InvalidOp(#[from] crate::error::InvalidOpType),
    #[error(transparent)]
    PackErr(#[from] PackError),
    #[error(transparent)]
    ReadOpErr(#[from] ReadOpError),
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

pub(crate) fn reconstruct_opset<'a>(
    doc: &'a Document<'a>,
    mode: VerificationMode,
) -> Result<ReconOpSet, Error> {
    let mut op_set = OpSet::new(doc)?;
    let mut change_collector = ChangeCollector::new(doc.iter_changes())?;
    let mut preds = HashMap::new();
    let mut counters = HashMap::new();
    let mut last = None;
    let mut iter = op_set.iter();
    let mut max_op = 0;
    let mut widths = Vec::with_capacity(op_set.len());
    let mut incs = Vec::with_capacity(op_set.sub_len());
    let mut marks = Vec::with_capacity(op_set.len());
    while let Some(op) = iter.try_next()? {
        marks.push(op.mark_index());
        if op.succ().len() == 0 {
            widths.push(op.width(ListEncoding::Text) as u64);
        } else {
            widths.push(0);
        }

        // opportunity to have a custom iterator that ignore some columns
        // read - op.obj(2) op.key(3) op.insert(1), op.id(2) op.succ(3)
        // not read - op.value(2) op.action(1), op.mark_name(1)
        let next = Some((op.obj, op.elemid_or_key()));
        if last != next {
            add_del_ops(&mut change_collector, &mut last, &mut preds)?;
            last = next;
        }
        for id in op.succ() {
            max_op = std::cmp::max(max_op, id.counter());
            preds.entry(id).or_default().push(op.id);
            if op.is_counter() {
                counters.entry(id).or_insert_with(Vec::new).push(incs.len());
            }
            incs.push(None); // will update later
        }

        max_op = std::cmp::max(max_op, op.id.counter());

        let pred = preds.remove(&op.id);
        let count = counters.remove(&op.id);

        if let Some(i) = op.get_increment_value() {
            for idx in count.iter().flatten() {
                incs[*idx] = Some(i);
            }
        }

        change_collector.collect(op.build(pred.iter().flatten().cloned()))?;
    }

    add_del_ops(&mut change_collector, &mut last, &mut preds)?;

    let (changes, heads, max_op2) = flush_changes(change_collector, doc, mode, &op_set)?;

    if max_op != max_op2 {
        return Err(Error::MismatchingMaxOp(max_op, max_op2));
    }

    op_set.set_text_index(widths);
    op_set.set_inc_index(incs);
    op_set.set_mark_index(marks);

    Ok(ReconOpSet {
        changes,
        max_op,
        op_set,
        heads,
    })
}

fn add_del_ops(
    change_collector: &mut ChangeCollector<'_>,
    last: &mut Option<(ObjId, KeyRef<'_>)>,
    preds: &mut HashMap<OpId, Vec<OpId>>,
) -> Result<(), Error> {
    if let Some((obj, key)) = last.take() {
        for (id, pred) in preds.drain() {
            let del = OpBuilder2::del(id, obj.into(), key.into_owned(), pred.iter().cloned());
            change_collector.collect(del)?;
        }
    }
    Ok(())
}

// create all binary changes
// look for mismatched heads

fn flush_changes(
    change_collector: ChangeCollector<'_>,
    doc: &Document<'_>,
    mode: VerificationMode,
    op_set: &OpSet,
) -> Result<(Vec<Change>, BTreeSet<ChangeHash>, u64), Error> {
    let super::change_collector::CollectedChanges { history, heads } =
        change_collector.finish(op_set)?;
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
    let changes = history.into_iter().map(Change::new).collect::<Vec<_>>();
    let max_op = changes.iter().map(|c| c.max_op()).max().unwrap_or(0);
    Ok((changes, heads, max_op))
}

pub(crate) struct ReconOpSet {
    pub(crate) changes: Vec<Change>,
    pub(crate) max_op: u64,
    pub(crate) op_set: OpSet,
    pub(crate) heads: BTreeSet<ChangeHash>,
}
