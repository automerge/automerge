use core::cmp::max;
use std::collections::HashMap;

use automerge_protocol as amp;

use super::{gen_value_diff::gen_value_diff, Edits, PatchWorkshop};
use crate::{internal::ObjectId, object_store::ObjState};

/// Used to generate a diff when there is no previous state to diff against.
/// This works by starting at the root object and then recursively constructing
/// all the objects contained in it.
pub(crate) fn generate_from_scratch_diff(workshop: &dyn PatchWorkshop) -> amp::RootDiff {
    let mut props = HashMap::new();

    for (key, ops) in &workshop.get_obj(&ObjectId::Root).unwrap().props {
        if !ops.is_empty() {
            let mut opid_to_value = HashMap::new();
            for op in ops.iter() {
                let amp_opid = workshop.make_external_opid(&op.id);
                if let Some(child_id) = op.child() {
                    opid_to_value.insert(amp_opid, construct_object(&child_id, workshop));
                } else {
                    opid_to_value
                        .insert(amp_opid, gen_value_diff(op, &op.adjusted_value(), workshop));
                }
            }
            props.insert(workshop.key_to_string(key), opid_to_value);
        }
    }
    amp::RootDiff { props }
}

fn construct_map(
    object_id: &ObjectId,
    object: &ObjState,
    workshop: &dyn PatchWorkshop,
) -> amp::MapDiff {
    construct_map_or_table(object_id, object, amp::MapType::Map, workshop)
}

fn construct_table(
    object_id: &ObjectId,
    object: &ObjState,
    workshop: &dyn PatchWorkshop,
) -> amp::MapDiff {
    construct_map_or_table(object_id, object, amp::MapType::Table, workshop)
}

fn construct_map_or_table(
    object_id: &ObjectId,
    object: &ObjState,
    map_type: amp::MapType,
    workshop: &dyn PatchWorkshop,
) -> amp::MapDiff {
    let mut props = HashMap::new();

    for (key, ops) in &object.props {
        if !ops.is_empty() {
            let mut opid_to_value = HashMap::new();
            for op in ops.iter() {
                let amp_opid = workshop.make_external_opid(&op.id);
                if let Some(child_id) = op.child() {
                    opid_to_value.insert(amp_opid, construct_object(&child_id, workshop));
                } else {
                    opid_to_value
                        .insert(amp_opid, gen_value_diff(op, &op.adjusted_value(), workshop));
                }
            }
            props.insert(workshop.key_to_string(key), opid_to_value);
        }
    }
    amp::MapDiff {
        object_id: workshop.make_external_objid(object_id),
        map_type,
        props,
    }
}

fn construct_list(
    object_id: &ObjectId,
    object: &ObjState,
    workshop: &dyn PatchWorkshop,
) -> amp::SeqDiff {
    construct_list_or_text(object_id, object, amp::SequenceType::List, workshop)
}

fn construct_text(
    object_id: &ObjectId,
    object: &ObjState,
    workshop: &dyn PatchWorkshop,
) -> amp::SeqDiff {
    construct_list_or_text(object_id, object, amp::SequenceType::Text, workshop)
}

fn construct_list_or_text(
    object_id: &ObjectId,
    object: &ObjState,
    seq_type: amp::SequenceType,
    workshop: &dyn PatchWorkshop,
) -> amp::SeqDiff {
    let mut edits = Edits::new();
    let mut index = 0;
    let mut max_counter = 0;
    let mut seen_indices: std::collections::HashSet<u64> = std::collections::HashSet::new();

    for opid in &object.seq {
        max_counter = max(max_counter, opid.0);
        let key = (*opid).into(); // FIXME - something is wrong here
        if let Some(ops) = object.props.get(&key) {
            if !ops.is_empty() {
                for op in ops.iter() {
                    let value = if let Some(child_id) = op.child() {
                        construct_object(&child_id, workshop)
                    } else {
                        gen_value_diff(op, &op.adjusted_value(), workshop)
                    };
                    let amp_opid = workshop.make_external_opid(&op.id);
                    if seen_indices.contains(&index) {
                        edits.append_edit(amp::DiffEdit::Update {
                            index,
                            op_id: amp_opid,
                            value,
                        });
                    } else {
                        let key = workshop
                            .make_external_opid(&key.to_opid().unwrap_or(op.id))
                            .into();
                        edits.append_edit(amp::DiffEdit::SingleElementInsert {
                            index,
                            elem_id: key,
                            op_id: amp_opid,
                            value,
                        });
                    }
                    seen_indices.insert(index);
                }
                index += 1;
            }
        }
    }
    amp::SeqDiff {
        object_id: workshop.make_external_objid(object_id),
        seq_type,
        edits: edits.into_vec(),
    }
}

fn construct_object(object_id: &ObjectId, workshop: &dyn PatchWorkshop) -> amp::Diff {
    // Safety: if the object is missing when we're generating a diff from
    // scratch then the document is corrupt
    let object = workshop.get_obj(object_id).expect("missing object");
    match object.obj_type {
        amp::ObjType::Map => amp::Diff::Map(construct_map(object_id, object, workshop)),
        amp::ObjType::Table => amp::Diff::Map(construct_table(object_id, object, workshop)),
        amp::ObjType::List => amp::Diff::Seq(construct_list(object_id, object, workshop)),
        amp::ObjType::Text => amp::Diff::Seq(construct_text(object_id, object, workshop)),
    }
}
