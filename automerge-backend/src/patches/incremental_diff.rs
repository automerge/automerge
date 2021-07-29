use std::collections::{HashMap, HashSet};

use automerge_protocol as amp;
use smol_str::SmolStr;

use super::{gen_value_diff::gen_value_diff, Edits, PatchWorkshop};
use crate::{
    actor_map::ActorMap,
    internal::{InternalOpType, Key, ObjectId, OpId},
    object_store::ObjState,
    op_handle::OpHandle,
};

/// Records a change that has happened as a result of an operation
#[derive(Debug, Clone, PartialEq)]
enum PendingDiff {
    // contains the op handle, the index to insert after and the new element's id
    SeqInsert(OpHandle, usize, OpId),
    // contains the op handle, the index to insert after and the new element's id
    SeqUpdate(OpHandle, usize, OpId),
    SeqRemove(OpHandle, usize),
    Set(OpHandle),
}

/// `IncrementalPatch` is used to build patches which are a result of applying a `Change`. As the
/// `OpSet` applies each op in the change it records the difference that will make in the
/// `IncrementalPatch` using the various `record_*` methods. At the end of the change process the
/// `IncrementalPatch::finalize` method is used to generate a `automerge_protocol::Diff` to send to
/// the frontend.
///
/// The reason this is called an "incremental" patch is because it impliciatly generates a diff
/// between the "current" state - represented by whatever was in the OpSet before the change was
/// received - and the new state after the change is applied. This is in contrast to when we are
/// generating a diff without any existing state, as in the case when we first load a saved
/// document.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct IncrementalPatch {
    maps: HashMap<ObjectId, HashSet<SmolStr>>,
    seqs: HashMap<ObjectId, Vec<PendingDiff>>,
}

impl IncrementalPatch {
    pub(crate) fn record_seq_set(&mut self, oid: &ObjectId, op: OpHandle) {
        self.append_seq_diff(oid, PendingDiff::Set(op));
    }

    pub(crate) fn record_map_set(&mut self, oid: &ObjectId, key: SmolStr) {
        self.append_map_diff(oid, key)
    }

    pub(crate) fn record_cursor_change(&mut self, oid: &ObjectId, key: SmolStr) {
        self.append_map_diff(oid, key);
    }

    pub(crate) fn record_seq_insert(
        &mut self,
        oid: &ObjectId,
        op: OpHandle,
        index: usize,
        opid: OpId,
    ) {
        self.append_seq_diff(oid, PendingDiff::SeqInsert(op, index, opid));
    }

    pub(crate) fn record_seq_updates<'b, 'c, I: Iterator<Item = &'c OpHandle>>(
        &'b mut self,
        oid: &ObjectId,
        object: &ObjState,
        index: usize,
        ops: I,
        actors: &ActorMap,
    ) {
        // TODO: Remove the actors argument and instead add a new case to the `PendingDiff`
        // enum to represent multiple seq updates, then sort by actor ID at the point at which we
        // finalize the diffs, when we have access to a `PatchWorkshop` to perform the sorting
        let diffs = self.seqs.entry(*oid).or_default();
        let mut new_diffs = Vec::new();
        'outer: for op in ops {
            let i = op
                .key
                .to_opid()
                .and_then(|opid| object.index_of(opid))
                .unwrap_or(0);
            if i == index {
                // go through existing diffs and find an insert
                for diff in diffs.iter_mut() {
                    match diff {
                        // if this insert was for the index we are now updating, and it is from the
                        // same actor,
                        // then change the insert to just insert our data instead
                        PendingDiff::SeqInsert(original_op, index, original_opid)
                            if i == *index && original_op.id.1 == op.id.1 =>
                        {
                            *diff = PendingDiff::SeqInsert(op.clone(), *index, *original_opid);
                            continue 'outer;
                        }
                        _ => {}
                    }
                }
                new_diffs.push(PendingDiff::SeqUpdate(op.clone(), index, op.id));
            }
        }
        new_diffs.sort_by_key(|d| {
            if let PendingDiff::SeqUpdate(op, _, _) = d {
                actors.export_actor(op.id.1)
            } else {
                // SAFETY: we only add SeqUpdates to this vec above.
                unreachable!()
            }
        });
        self.append_seq_diffs(oid, new_diffs);
    }

    pub(crate) fn record_seq_remove(&mut self, oid: &ObjectId, op: OpHandle, index: usize) {
        self.append_seq_diff(oid, PendingDiff::SeqRemove(op, index));
    }

    fn append_seq_diff(&mut self, oid: &ObjectId, diff: PendingDiff) {
        self.seqs.entry(*oid).or_default().push(diff);
    }

    fn append_map_diff(&mut self, oid: &ObjectId, key: SmolStr) {
        self.maps.entry(*oid).or_default().insert(key);
    }

    fn append_seq_diffs(&mut self, oid: &ObjectId, mut diffs: Vec<PendingDiff>) {
        self.seqs.entry(*oid).or_default().append(&mut diffs);
    }

    pub(crate) fn changed_object_ids(&self) -> impl Iterator<Item = &ObjectId> {
        self.seqs.keys().chain(self.maps.keys())
    }

    fn contains_key(&self, oid: &ObjectId) -> bool {
        self.maps.contains_key(oid) || self.seqs.contains_key(oid)
    }

    fn is_empty(&self) -> bool {
        self.seqs.is_empty() && self.maps.is_empty()
    }

    pub(crate) fn finalize(mut self, workshop: &dyn PatchWorkshop) -> amp::RootDiff {
        if self.is_empty() {
            return amp::RootDiff::default();
        }

        let mut objs: Vec<_> = self.changed_object_ids().copied().collect();
        while let Some(obj_id) = objs.pop() {
            if let Some(inbound) = workshop
                .get_obj(&obj_id)
                .and_then(|obj| obj.inbound.as_ref())
            {
                if !self.contains_key(&inbound.obj) {
                    // our parent was not changed - walk up the tree and try them too
                    objs.push(inbound.obj);
                }
                let inbound_object = workshop.get_obj(&inbound.obj).unwrap();
                match inbound_object.obj_type {
                    amp::ObjType::Map | amp::ObjType::Table => {
                        let key = inbound.operation_key().into_owned();
                        match key {
                            Key::Map(s) => self.record_map_set(&inbound.obj, s),
                            Key::Seq(_) => panic!("found seq key while finalizing for a map"),
                        }
                    }
                    amp::ObjType::List | amp::ObjType::Text => {
                        self.record_seq_set(&inbound.obj, inbound.clone())
                    }
                }
            }
        }

        if let Some((root, obj)) = self
            .maps
            .remove(&ObjectId::Root)
            .and_then(|root| workshop.get_obj(&ObjectId::Root).map(|obj| (root, obj)))
        {
            let props = root
                .into_iter()
                .map(|key| {
                    let opid_to_value = obj
                        .conflicts(&Key::Map(key.clone()))
                        .map(|op| {
                            let diff = match op.action {
                                InternalOpType::Set(ref value) => {
                                    gen_value_diff(op, value, workshop)
                                }
                                InternalOpType::Make(_) => {
                                    self.gen_obj_diff(&op.id.into(), workshop)
                                }
                                _ => panic!("del or inc found in field_operations"),
                            };
                            (workshop.make_external_opid(&op.id), diff)
                        })
                        .collect();
                    (key, opid_to_value)
                })
                .collect();
            amp::RootDiff { props }
        } else {
            amp::RootDiff {
                props: HashMap::new(),
            }
        }
    }

    fn gen_obj_diff(&mut self, obj_id: &ObjectId, workshop: &dyn PatchWorkshop) -> amp::Diff {
        // Safety: the pending diffs we are working with are all generated by
        // the OpSet, we should never have a missing object and if we do
        // there's nothing the user can do about that
        let obj = workshop
            .get_obj(obj_id)
            .expect("Missing object in internal diff");
        match obj.obj_type {
            amp::ObjType::Map => {
                if let Some(pending) = self.maps.remove(obj_id) {
                    amp::Diff::Map(self.gen_map_diff(obj_id, obj, pending, workshop))
                } else {
                    amp::Diff::Map(amp::MapDiff {
                        object_id: workshop.make_external_objid(obj_id),
                        props: HashMap::new(),
                    })
                }
            }
            amp::ObjType::Table => {
                if let Some(pending) = self.maps.remove(obj_id) {
                    amp::Diff::Table(self.gen_table_diff(obj_id, obj, pending, workshop))
                } else {
                    amp::Diff::Table(amp::TableDiff {
                        object_id: workshop.make_external_objid(obj_id),
                        props: HashMap::new(),
                    })
                }
            }
            amp::ObjType::List => {
                if let Some(pending) = self.seqs.remove(obj_id) {
                    amp::Diff::List(self.gen_list_diff(obj_id, obj, pending, workshop))
                } else {
                    amp::Diff::List(amp::ListDiff {
                        object_id: workshop.make_external_objid(obj_id),
                        edits: Vec::new(),
                    })
                }
            }
            amp::ObjType::Text => {
                if let Some(pending) = self.seqs.remove(obj_id) {
                    amp::Diff::Text(self.gen_text_diff(obj_id, obj, pending, workshop))
                } else {
                    amp::Diff::Text(amp::TextDiff {
                        object_id: workshop.make_external_objid(obj_id),
                        edits: Vec::new(),
                    })
                }
            }
        }
    }

    fn gen_list_diff(
        &mut self,
        obj_id: &ObjectId,
        obj: &ObjState,
        pending: Vec<PendingDiff>,
        workshop: &dyn PatchWorkshop,
    ) -> amp::ListDiff {
        let mut edits = Edits::new();
        // used to ensure we don't generate duplicate patches for some op ids (added to the pending
        // list to ensure we have a tree for deeper operations)
        let mut seen_op_ids = HashSet::new();
        for pending_edit in pending {
            match pending_edit {
                PendingDiff::SeqInsert(op, index, opid) => {
                    seen_op_ids.insert(op.id);
                    let value = match op.action {
                        InternalOpType::Set(ref value) => gen_value_diff(&op, value, workshop),
                        InternalOpType::Make(_) => self.gen_obj_diff(&op.id.into(), workshop),
                        _ => panic!("del or inc found in field operations"),
                    };
                    let op_id = workshop.make_external_opid(&opid);
                    edits.append_edit(amp::DiffEdit::SingleElementInsert {
                        index: index as u64,
                        elem_id: op_id.into(),
                        op_id: workshop.make_external_opid(&op.id),
                        value,
                    });
                }
                PendingDiff::SeqUpdate(op, index, opid) => {
                    seen_op_ids.insert(op.id);
                    let value = match op.action {
                        InternalOpType::Set(ref value) => gen_value_diff(&op, value, workshop),
                        InternalOpType::Make(_) => self.gen_obj_diff(&op.id.into(), workshop),
                        InternalOpType::Del | InternalOpType::Inc(..) => {
                            // do nothing
                            continue;
                        }
                    };
                    edits.append_edit(amp::DiffEdit::Update {
                        index: index as u64,
                        op_id: workshop.make_external_opid(&opid),
                        value,
                    });
                }
                PendingDiff::SeqRemove(op, index) => {
                    seen_op_ids.insert(op.id);

                    edits.append_edit(amp::DiffEdit::Remove {
                        index: index as u64,
                        count: 1,
                    });
                }
                PendingDiff::Set(op) => {
                    for op in obj.conflicts(&op.operation_key()) {
                        if !seen_op_ids.contains(&op.id) {
                            seen_op_ids.insert(op.id);
                            let value = match op.action {
                                InternalOpType::Set(ref value) => {
                                    gen_value_diff(op, value, workshop)
                                }
                                InternalOpType::Make(_) => {
                                    self.gen_obj_diff(&op.id.into(), workshop)
                                }
                                _ => panic!("del or inc found in field operations"),
                            };
                            edits.append_edit(amp::DiffEdit::Update {
                                index: obj.index_of(op.id).unwrap_or(0) as u64,
                                op_id: workshop.make_external_opid(&op.id),
                                value,
                            });
                        }
                    }
                }
            }
        }
        amp::ListDiff {
            object_id: workshop.make_external_objid(obj_id),
            edits: edits.into_vec(),
        }
    }

    fn gen_text_diff(
        &mut self,
        obj_id: &ObjectId,
        obj: &ObjState,
        pending: Vec<PendingDiff>,
        workshop: &dyn PatchWorkshop,
    ) -> amp::TextDiff {
        let mut edits = Edits::new();
        // used to ensure we don't generate duplicate patches for some op ids (added to the pending
        // list to ensure we have a tree for deeper operations)
        let mut seen_op_ids = HashSet::new();
        for pending_edit in pending {
            match pending_edit {
                PendingDiff::SeqInsert(op, index, opid) => {
                    seen_op_ids.insert(op.id);
                    let value = match op.action {
                        InternalOpType::Set(ref value) => gen_value_diff(&op, value, workshop),
                        InternalOpType::Make(_) => self.gen_obj_diff(&op.id.into(), workshop),
                        _ => panic!("del or inc found in field operations"),
                    };
                    let op_id = workshop.make_external_opid(&opid);
                    edits.append_edit(amp::DiffEdit::SingleElementInsert {
                        index: index as u64,
                        elem_id: op_id.into(),
                        op_id: workshop.make_external_opid(&op.id),
                        value,
                    });
                }
                PendingDiff::SeqUpdate(op, index, opid) => {
                    seen_op_ids.insert(op.id);
                    let value = match op.action {
                        InternalOpType::Set(ref value) => gen_value_diff(&op, value, workshop),
                        InternalOpType::Make(_) => self.gen_obj_diff(&op.id.into(), workshop),
                        InternalOpType::Del | InternalOpType::Inc(..) => {
                            // do nothing
                            continue;
                        }
                    };
                    edits.append_edit(amp::DiffEdit::Update {
                        index: index as u64,
                        op_id: workshop.make_external_opid(&opid),
                        value,
                    });
                }
                PendingDiff::SeqRemove(op, index) => {
                    seen_op_ids.insert(op.id);

                    edits.append_edit(amp::DiffEdit::Remove {
                        index: index as u64,
                        count: 1,
                    });
                }
                PendingDiff::Set(op) => {
                    for op in obj.conflicts(&op.operation_key()) {
                        if !seen_op_ids.contains(&op.id) {
                            seen_op_ids.insert(op.id);
                            let value = match op.action {
                                InternalOpType::Set(ref value) => {
                                    gen_value_diff(op, value, workshop)
                                }
                                InternalOpType::Make(_) => {
                                    self.gen_obj_diff(&op.id.into(), workshop)
                                }
                                _ => panic!("del or inc found in field operations"),
                            };
                            edits.append_edit(amp::DiffEdit::Update {
                                index: obj.index_of(op.id).unwrap_or(0) as u64,
                                op_id: workshop.make_external_opid(&op.id),
                                value,
                            });
                        }
                    }
                }
            }
        }
        amp::TextDiff {
            object_id: workshop.make_external_objid(obj_id),
            edits: edits.into_vec(),
        }
    }

    fn gen_map_diff(
        &mut self,
        obj_id: &ObjectId,
        obj: &ObjState,
        pending: HashSet<SmolStr>,
        workshop: &dyn PatchWorkshop,
    ) -> amp::MapDiff {
        let props = pending
            .into_iter()
            .map(|key| {
                let opid_to_value = obj
                    .conflicts(&Key::Map(key.clone()))
                    .map(|op| {
                        let value = match op.action {
                            InternalOpType::Set(ref value) => gen_value_diff(op, value, workshop),
                            InternalOpType::Make(_) => self.gen_obj_diff(&op.id.into(), workshop),
                            _ => panic!("del or inc found in field_operations"),
                        };
                        (workshop.make_external_opid(&op.id), value)
                    })
                    .collect();
                (key, opid_to_value)
            })
            .collect();
        amp::MapDiff {
            object_id: workshop.make_external_objid(obj_id),
            props,
        }
    }

    fn gen_table_diff(
        &mut self,
        obj_id: &ObjectId,
        obj: &ObjState,
        pending: HashSet<SmolStr>,
        workshop: &dyn PatchWorkshop,
    ) -> amp::TableDiff {
        let props = pending
            .into_iter()
            .map(|key| {
                let opid_to_value = obj
                    .conflicts(&Key::Map(key.clone()))
                    .map(|op| {
                        let link = match op.action {
                            InternalOpType::Set(ref value) => gen_value_diff(op, value, workshop),
                            InternalOpType::Make(_) => self.gen_obj_diff(&op.id.into(), workshop),
                            _ => panic!("del or inc found in field_operations"),
                        };
                        (workshop.make_external_opid(&op.id), link)
                    })
                    .collect();
                (key, opid_to_value)
            })
            .collect();
        amp::TableDiff {
            object_id: workshop.make_external_objid(obj_id),
            props,
        }
    }
}
