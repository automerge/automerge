use std::collections::{HashMap, HashSet};

use amp::{MapType, SequenceType};
use automerge_protocol as amp;

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
    CursorChange(Key),
}

impl PendingDiff {
    pub fn operation_key(&self) -> Key {
        match self {
            Self::SeqInsert(op, ..)
            | Self::SeqUpdate(op, ..)
            | Self::SeqRemove(op, ..)
            | Self::Set(op) => op.operation_key(),
            Self::CursorChange(k) => k.clone(),
        }
    }
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
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IncrementalPatch(HashMap<ObjectId, Vec<PendingDiff>>);

impl IncrementalPatch {
    pub(crate) fn new() -> IncrementalPatch {
        IncrementalPatch(HashMap::new())
    }

    pub(crate) fn record_set(&mut self, oid: &ObjectId, op: OpHandle) {
        self.append_diff(oid, PendingDiff::Set(op))
    }

    pub(crate) fn record_cursor_change(&mut self, oid: &ObjectId, key: Key) {
        self.append_diff(oid, PendingDiff::CursorChange(key));
    }

    pub(crate) fn record_seq_insert(
        &mut self,
        oid: &ObjectId,
        op: OpHandle,
        index: usize,
        opid: OpId,
    ) {
        self.append_diff(oid, PendingDiff::SeqInsert(op, index, opid));
    }

    pub(crate) fn record_seq_updates<'a, 'b, I: Iterator<Item = &'b OpHandle>>(
        &'a mut self,
        oid: &ObjectId,
        object: &ObjState,
        index: usize,
        ops: I,
        actors: &ActorMap,
    ) {
        // TODO: Remove the actors argument and instead add a new case to the `PendingDiff`
        // enum to represent multiple seq updates, then sort by actor ID at the point at which we
        // finalize the diffs, when we have access to a `PatchWorkshop` to perform the sorting
        let diffs = self.0.entry(*oid).or_default();
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
                new_diffs.push(PendingDiff::SeqUpdate(op.clone(), index, op.id))
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
        self.append_diffs(oid, new_diffs);
    }

    pub(crate) fn record_seq_remove(&mut self, oid: &ObjectId, op: OpHandle, index: usize) {
        self.append_diff(oid, PendingDiff::SeqRemove(op, index))
    }

    fn append_diff(&mut self, oid: &ObjectId, diff: PendingDiff) {
        self.0.entry(*oid).or_default().push(diff);
    }

    fn append_diffs(&mut self, oid: &ObjectId, mut diffs: Vec<PendingDiff>) {
        self.0.entry(*oid).or_default().append(&mut diffs)
    }

    pub(crate) fn changed_object_ids(&self) -> impl Iterator<Item = &ObjectId> {
        self.0.keys()
    }

    pub(crate) fn finalize(mut self, workshop: &dyn PatchWorkshop) -> amp::RootDiff {
        if self.0.is_empty() {
            return amp::RootDiff::default();
        }

        let mut objs: Vec<_> = self.changed_object_ids().copied().collect();
        while let Some(obj_id) = objs.pop() {
            if let Some(inbound) = workshop
                .get_obj(&obj_id)
                .and_then(|obj| obj.inbound.as_ref())
            {
                if !self.0.contains_key(&inbound.obj) {
                    // our parent was not changed - walk up the tree and try them too
                    objs.push(inbound.obj);
                }
                self.append_diff(&inbound.obj, PendingDiff::Set(inbound.clone()));
            }
        }

        if let Some(root) = self.0.remove(&ObjectId::Root) {
            let mut props = HashMap::new();
            // I may have duplicate keys - I do this to make sure I visit each one only once
            let keys: HashSet<_> = root.iter().map(PendingDiff::operation_key).collect();
            let obj = workshop.get_obj(&ObjectId::Root).expect("no root found");
            for key in &keys {
                let key_string = workshop.key_to_string(key);
                let mut opid_to_value = HashMap::new();
                for op in obj.conflicts(key) {
                    let link = match op.action {
                        InternalOpType::Set(ref value) => gen_value_diff(op, value, workshop),
                        InternalOpType::Make(_) => self.gen_obj_diff(&op.id.into(), workshop),
                        _ => panic!("del or inc found in field_operations"),
                    };
                    opid_to_value.insert(workshop.make_external_opid(&op.id), link);
                }
                props.insert(key_string, opid_to_value);
            }
            amp::RootDiff { props }
        } else {
            amp::RootDiff {
                props: HashMap::new(),
            }
        }
    }

    fn gen_obj_diff(&self, obj_id: &ObjectId, workshop: &dyn PatchWorkshop) -> amp::Diff {
        // Safety: the pending diffs we are working with are all generated by
        // the OpSet, we should never have a missing object and if we do
        // there's nothing the user can do about that
        let obj = workshop
            .get_obj(obj_id)
            .expect("Missing object in internal diff");
        if let Some(pending) = self.0.get(obj_id) {
            match obj.obj_type {
                amp::ObjType::List => {
                    amp::Diff::Seq(self.gen_list_diff(obj_id, obj, pending, workshop))
                }
                amp::ObjType::Text => {
                    amp::Diff::Seq(self.gen_text_diff(obj_id, obj, pending, workshop))
                }
                amp::ObjType::Map => {
                    amp::Diff::Map(self.gen_map_diff(obj_id, obj, pending, workshop))
                }
                amp::ObjType::Table => {
                    amp::Diff::Map(self.gen_table_diff(obj_id, obj, pending, workshop))
                }
            }
        } else {
            // no changes so just return empty edits or props
            match obj.obj_type {
                amp::ObjType::Map => amp::Diff::Map(amp::MapDiff {
                    object_id: workshop.make_external_objid(obj_id),
                    map_type: MapType::Map,
                    props: HashMap::new(),
                }),
                amp::ObjType::Table => amp::Diff::Map(amp::MapDiff {
                    object_id: workshop.make_external_objid(obj_id),
                    map_type: MapType::Table,
                    props: HashMap::new(),
                }),
                amp::ObjType::List => amp::Diff::Seq(amp::SeqDiff {
                    object_id: workshop.make_external_objid(obj_id),
                    seq_type: SequenceType::List,
                    edits: Vec::new(),
                }),
                amp::ObjType::Text => amp::Diff::Seq(amp::SeqDiff {
                    object_id: workshop.make_external_objid(obj_id),
                    seq_type: SequenceType::Text,
                    edits: Vec::new(),
                }),
            }
        }
    }

    fn gen_list_diff(
        &self,
        obj_id: &ObjectId,
        obj: &ObjState,
        pending: &[PendingDiff],
        workshop: &dyn PatchWorkshop,
    ) -> amp::SeqDiff {
        self.gen_seq_diff(obj_id, obj, pending, workshop, amp::SequenceType::List)
    }

    fn gen_text_diff(
        &self,
        obj_id: &ObjectId,
        obj: &ObjState,
        pending: &[PendingDiff],
        workshop: &dyn PatchWorkshop,
    ) -> amp::SeqDiff {
        self.gen_seq_diff(obj_id, obj, pending, workshop, amp::SequenceType::Text)
    }

    fn gen_seq_diff(
        &self,
        obj_id: &ObjectId,
        obj: &ObjState,
        pending: &[PendingDiff],
        workshop: &dyn PatchWorkshop,
        seq_type: amp::SequenceType,
    ) -> amp::SeqDiff {
        let mut edits = Edits::new();
        // used to ensure we don't generate duplicate patches for some op ids (added to the pending
        // list to ensure we have a tree for deeper operations)
        let mut seen_op_ids = HashSet::new();
        for pending_edit in pending.iter() {
            match pending_edit {
                PendingDiff::SeqInsert(op, index, opid) => {
                    seen_op_ids.insert(op.id);
                    let value = match op.action {
                        InternalOpType::Set(ref value) => gen_value_diff(op, value, workshop),
                        InternalOpType::Make(_) => self.gen_obj_diff(&op.id.into(), workshop),
                        _ => panic!("del or inc found in field operations"),
                    };
                    let op_id = workshop.make_external_opid(opid);
                    edits.append_edit(amp::DiffEdit::SingleElementInsert {
                        index: *index as u64,
                        elem_id: op_id.clone().into(),
                        op_id: workshop.make_external_opid(&op.id),
                        value,
                    });
                }
                PendingDiff::SeqUpdate(op, index, opid) => {
                    seen_op_ids.insert(op.id);
                    let value = match op.action {
                        InternalOpType::Set(ref value) => gen_value_diff(op, value, workshop),
                        InternalOpType::Make(_) => self.gen_obj_diff(&op.id.into(), workshop),
                        InternalOpType::Del | InternalOpType::Inc(..) => {
                            // do nothing
                            continue;
                        }
                    };
                    edits.append_edit(amp::DiffEdit::Update {
                        index: *index as u64,
                        op_id: workshop.make_external_opid(opid),
                        value,
                    });
                }
                PendingDiff::SeqRemove(op, index) => {
                    seen_op_ids.insert(op.id);

                    edits.append_edit(amp::DiffEdit::Remove {
                        index: (*index) as u64,
                        count: 1,
                    })
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
                            })
                        }
                    }
                }
                PendingDiff::CursorChange(_) => {
                    panic!("found cursor change pending diff while generating sequence diff")
                }
            }
        }
        amp::SeqDiff {
            object_id: workshop.make_external_objid(obj_id),
            seq_type,
            edits: edits.into_vec(),
        }
    }

    fn gen_map_diff(
        &self,
        obj_id: &ObjectId,
        obj: &ObjState,
        pending: &[PendingDiff],
        workshop: &dyn PatchWorkshop,
    ) -> amp::MapDiff {
        self.gen_map_or_table_diff(obj_id, obj, pending, workshop, amp::MapType::Map)
    }

    fn gen_table_diff(
        &self,
        obj_id: &ObjectId,
        obj: &ObjState,
        pending: &[PendingDiff],
        workshop: &dyn PatchWorkshop,
    ) -> amp::MapDiff {
        self.gen_map_or_table_diff(obj_id, obj, pending, workshop, amp::MapType::Table)
    }

    fn gen_map_or_table_diff(
        &self,
        obj_id: &ObjectId,
        obj: &ObjState,
        pending: &[PendingDiff],
        workshop: &dyn PatchWorkshop,
        map_type: amp::MapType,
    ) -> amp::MapDiff {
        let mut props = HashMap::new();
        // I may have duplicate keys - I do this to make sure I visit each one only once
        let keys: HashSet<_> = pending.iter().map(PendingDiff::operation_key).collect();
        for key in &keys {
            let key_string = workshop.key_to_string(key);
            let mut opid_to_value = HashMap::new();
            for op in obj.conflicts(key) {
                let link = match op.action {
                    InternalOpType::Set(ref value) => gen_value_diff(op, value, workshop),
                    InternalOpType::Make(_) => self.gen_obj_diff(&op.id.into(), workshop),
                    _ => panic!("del or inc found in field_operations"),
                };
                opid_to_value.insert(workshop.make_external_opid(&op.id), link);
            }
            props.insert(key_string, opid_to_value);
        }
        amp::MapDiff {
            object_id: workshop.make_external_objid(obj_id),
            map_type,
            props,
        }
    }
}
