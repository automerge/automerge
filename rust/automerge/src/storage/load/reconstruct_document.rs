use super::change_collector::ChangeCollector;
use std::collections::{BTreeSet, HashMap};
use tracing::instrument;

use crate::{
    change::Change,
    columnar::Key as DocOpKey,
    op_tree::OpSetMetadata,
    storage::{change::Verified, Change as StoredChange, DocOp, Document},
    types::{ChangeHash, ElemId, Key, ObjId, ObjType, Op, OpId, OpIds, OpType},
    ScalarValue,
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
    #[error("missing operations")]
    MissingOps,
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

/// All the operations loaded from an object in the document format
pub(crate) struct LoadedObject {
    /// The id of the object
    pub(crate) id: ObjId,
    /// The id of the parent object, if any
    pub(crate) parent: Option<ObjId>,
    /// The operations for this object
    pub(crate) ops: Vec<crate::types::Op>,
    /// The type of the object
    pub(crate) obj_type: ObjType,
}

/// An observer which will be notified of each object as it completes and which can produce a
/// result once all the operations are loaded and the change graph is verified.
pub(crate) trait DocObserver {
    type Output;

    /// The operations for an object have been loaded
    fn object_loaded(&mut self, object: LoadedObject);
    /// The document has finished loading. The `metadata` is the `OpSetMetadata` which was used to
    /// create the indices in the operations which were passed to `object_loaded`
    fn finish(self, metadata: OpSetMetadata) -> Self::Output;
}

/// The result of reconstructing the change history from a document
pub(crate) struct Reconstructed<Output> {
    /// The maximum op counter that was found in the document
    pub(crate) max_op: u64,
    /// The changes in the document, in the order they were encoded in the document
    pub(crate) changes: Vec<Change>,
    /// The result produced by the `DocObserver` which was watching the reconstruction
    pub(crate) result: Output,
    /// The heads of the document
    pub(crate) heads: BTreeSet<ChangeHash>,
}

#[derive(Debug)]
pub enum VerificationMode {
    Check,
    DontCheck,
}

#[instrument(skip(doc, observer))]
pub(crate) fn reconstruct_document<'a, O: DocObserver>(
    doc: &'a Document<'a>,
    mode: VerificationMode,
    mut observer: O,
) -> Result<Reconstructed<O::Output>, Error> {
    // The document format does not contain the bytes of the changes which are encoded in it
    // directly. Instead the metadata about the changes (the actor, the start op, etc.) are all
    // encoded separately to all the ops in the document. We need to reconstruct the changes in
    // order to verify the heads of the document. To do this we iterate over the document
    // operations adding each operation to a `ChangeCollector`. Once we've collected all the
    // changes, the `ChangeCollector` knows how to group all the operations together to produce the
    // change graph.
    //
    // Some of the work involved in reconstructing the changes could in principle be quite costly.
    // For example, delete operations dont appear in the document at all, instead the delete
    // operations are recorded as `succ` operations on the operations which they delete. This means
    // that to reconstruct delete operations we have to first collect all the operations, then look
    // for succ operations which we have not seen a concrete operation for. Happily we can take
    // advantage of the fact that operations are encoded in the order of the object they apply to.
    // This is the purpose of `LoadingObject`.
    //
    // Finally, when constructing an OpSet from this data we want to process the operations in the
    // order they appear in the document, this allows us to create the OpSet more efficiently than
    // if we were directly applying the reconstructed change graph. This is the purpose of the
    // `DocObserver`, which we pass operations to as we complete the processing of each object.

    // The metadata which we create from the doc and which we will pass to the observer
    let mut metadata = OpSetMetadata::from_actors(doc.actors().to_vec());
    // The object we are currently loading, starts with the root
    let mut current_object = LoadingObject::root();
    // The changes we are collecting to later construct the change graph from
    let mut collector = ChangeCollector::new(doc.iter_changes())?;
    // A map where we record the create operations so that when the object ID the incoming
    // operations refer to switches we can lookup the object type for the new object. We also
    // need it so we can pass the parent object ID to the observer
    let mut create_ops = HashMap::new();
    // The max op we've seen
    let mut max_op = 0;
    // The objects we have finished loaded
    let mut objs_loaded = BTreeSet::new();

    for op_res in doc.iter_ops() {
        let doc_op = op_res.map_err(|e| Error::ReadOp(Box::new(e)))?;
        max_op = std::cmp::max(max_op, doc_op.id.counter());

        // Delete ops only appear as succ values in the document operations, so if a delete
        // operation is the max op we will only see it here. Therefore we step through the document
        // operations succs checking for max op
        for succ in &doc_op.succ {
            max_op = std::cmp::max(max_op, succ.counter());
        }

        let obj = doc_op.object;
        check_opid(&metadata, *obj.opid())?;
        let op = import_op(&mut metadata, doc_op)?;
        tracing::trace!(?op, ?obj, "loading document op");

        if let OpType::Make(obj_type) = op.action {
            create_ops.insert(
                ObjId::from(op.id),
                CreateOp {
                    obj_type,
                    parent_id: obj,
                },
            );
        };
        if obj == current_object.id {
            current_object.append_op(op.clone())?;
        } else {
            let create_op = match create_ops.get(&obj) {
                Some(t) => Ok(t),
                None => {
                    tracing::error!(
                        ?op,
                        "operation referenced an object which we haven't seen a create op for yet"
                    );
                    Err(Error::OpsOutOfOrder)
                }
            }?;
            if obj < current_object.id {
                tracing::error!(?op, previous_obj=?current_object.id, "op referenced an object ID which was smaller than the previous object ID");
                return Err(Error::OpsOutOfOrder);
            } else {
                let loaded = current_object.finish(&mut collector, &metadata)?;
                objs_loaded.insert(loaded.id);
                observer.object_loaded(loaded);
                current_object =
                    LoadingObject::new(obj, Some(create_op.parent_id), create_op.obj_type);
                current_object.append_op(op.clone())?;
            }
        }
    }
    let loaded = current_object.finish(&mut collector, &metadata)?;
    objs_loaded.insert(loaded.id);
    observer.object_loaded(loaded);

    // If an op created an object but no operation targeting that object was ever made then the
    // object will only exist in the create_ops map. We collect all such objects here.
    for (
        obj_id,
        CreateOp {
            parent_id,
            obj_type,
        },
    ) in create_ops.into_iter()
    {
        if !objs_loaded.contains(&obj_id) {
            observer.object_loaded(LoadedObject {
                parent: Some(parent_id),
                id: obj_id,
                ops: Vec::new(),
                obj_type,
            })
        }
    }

    let super::change_collector::CollectedChanges { history, heads } =
        collector.finish(&metadata)?;
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
    let result = observer.finish(metadata);

    Ok(Reconstructed {
        result,
        changes: history.into_iter().map(Change::new).collect(),
        heads,
        max_op,
    })
}

struct CreateOp {
    parent_id: ObjId,
    obj_type: ObjType,
}
struct LoadingObject {
    id: ObjId,
    parent_id: Option<ObjId>,
    ops: Vec<Op>,
    obj_type: ObjType,
    preds: HashMap<OpId, Vec<OpId>>,
    /// Operations which set a value, stored to later lookup keys when reconstructing delete events
    set_ops: HashMap<OpId, Key>,
    /// To correctly load the values of the `Counter` struct in the value of op IDs we need to
    /// lookup the various increment operations which have been applied by the succesors of the
    /// initial operation which creates the counter.
    inc_ops: HashMap<OpId, i64>,
}

impl LoadingObject {
    fn root() -> Self {
        Self::new(ObjId::root(), None, ObjType::Map)
    }

    fn new(id: ObjId, parent_id: Option<ObjId>, obj_type: ObjType) -> Self {
        LoadingObject {
            id,
            parent_id,
            ops: Vec::new(),
            obj_type,
            preds: HashMap::new(),
            set_ops: HashMap::new(),
            inc_ops: HashMap::new(),
        }
    }

    fn append_op(&mut self, op: Op) -> Result<(), Error> {
        // Collect set and make operations so we can find the keys which delete operations refer to
        // in `finish`
        if matches!(op.action, OpType::Put(_) | OpType::Make(_)) {
            match op.key {
                Key::Map(_) => {
                    self.set_ops.insert(op.id, op.key);
                }
                Key::Seq(ElemId(o)) => {
                    let elem_opid = if op.insert { op.id } else { o };
                    self.set_ops.insert(op.id, Key::Seq(ElemId(elem_opid)));
                }
            };
        }
        // Collect increment operations so we can reconstruct counters properly in `finish`
        if let OpType::Increment(inc) = op.action {
            self.inc_ops.insert(op.id, inc);
        }
        for succ in &op.succ {
            self.preds.entry(*succ).or_default().push(op.id);
        }
        self.ops.push(op);
        Ok(())
    }

    fn finish(
        mut self,
        collector: &mut ChangeCollector<'_>,
        meta: &OpSetMetadata,
    ) -> Result<LoadedObject, Error> {
        let mut ops = Vec::new();
        for mut op in self.ops.into_iter() {
            if let Some(preds) = self.preds.remove(&op.id) {
                op.pred = meta.sorted_opids(preds.into_iter());
            }
            if let OpType::Put(ScalarValue::Counter(c)) = &mut op.action {
                for inc in op.succ.iter().filter_map(|s| self.inc_ops.get(s)) {
                    c.increment(*inc);
                }
            }
            collector.collect(self.id, op.clone())?;
            ops.push(op)
        }
        // Any remaining pred ops must be delete operations
        // TODO (alex): Figure out what index these should be inserted at. Does it even matter?
        for (opid, preds) in self.preds.into_iter() {
            let key = self.set_ops.get(&preds[0]).ok_or_else(|| {
                tracing::error!(?opid, ?preds, "no delete operation found");
                Error::MissingOps
            })?;
            collector.collect(
                self.id,
                Op {
                    id: opid,
                    pred: meta.sorted_opids(preds.into_iter()),
                    insert: false,
                    succ: OpIds::empty(),
                    key: *key,
                    action: OpType::Delete,
                },
            )?;
        }
        Ok(LoadedObject {
            id: self.id,
            parent: self.parent_id,
            ops,
            obj_type: self.obj_type,
        })
    }
}

fn import_op(m: &mut OpSetMetadata, op: DocOp) -> Result<Op, Error> {
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
    let action = OpType::from_action_and_value(op.action, op.value, op.mark_name, op.expand);
    Ok(Op {
        id: check_opid(m, op.id)?,
        action,
        key,
        succ: m.try_sorted_opids(op.succ).ok_or(Error::SuccOutOfOrder)?,
        pred: OpIds::empty(),
        insert: op.insert,
    })
}

/// We construct the OpSetMetadata directly from the vector of actors which are encoded in the
/// start of the document. Therefore we need to check for each opid in the docuemnt that the actor
/// ID which it references actually exists in the metadata.
fn check_opid(m: &OpSetMetadata, opid: OpId) -> Result<OpId, Error> {
    match m.actors.safe_get(opid.actor()) {
        Some(_) => Ok(opid),
        None => {
            tracing::error!("missing actor");
            Err(Error::MissingActor)
        }
    }
}
