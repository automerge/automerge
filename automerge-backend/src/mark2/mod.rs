#![allow(dead_code)]

use std::cmp::Ordering;

use automerge_protocol as amp;

use crate::{error::AutomergeError, internal::InternalOpType, patches::IncrementalPatch};

#[derive(Debug, PartialEq, Clone, Default)]
pub struct OpSet {
    actors: Vec<amp::ActorId>,
    changes: Vec<Change>,
    ops: Vec<Op>,
}
/*

#[derive(PartialEq, Debug, Clone)]
pub struct Change {
    bytes: ChangeBytes,
    pub hash: amp::ChangeHash,
    pub seq: u64,
    pub start_op: u64,
    pub time: i64,
    message: Range<usize>,
    actors: Vec<amp::ActorId>,
    pub deps: Vec<amp::ChangeHash>,
    ops: HashMap<u32, Range<usize>>,
    extra_bytes: Range<usize>,
}

 */

fn inc_visible(next: &Op, elem_visible: &mut bool, visible: &mut usize) {
    if next.insert {
        *elem_visible = false
    }
    if next.succ.is_empty() && !*elem_visible {
        *visible += 1;
        *elem_visible = true
    }
}

impl OpSet {
    fn index_for_actor(&self, actor: &amp::ActorId) -> Option<usize> {
        self.actors.iter().position(|n| n == actor)
    }

    fn import_key(&self, key: &amp::Key) -> Key {
        match key {
            amp::Key::Map(string) => Key::Map(string.clone()),
            amp::Key::Seq(amp::ElementId::Head) => Key::Seq(OpId(0, 0)),
            amp::Key::Seq(amp::ElementId::Id(id)) => Key::Seq(self.import_opid(id)),
        }
    }

    fn import_objectid(&self, obj: &amp::ObjectId) -> OpId {
        match obj {
            amp::ObjectId::Root => OpId(0, 0),
            amp::ObjectId::Id(id) => self.import_opid(id),
        }
    }

    fn import_opid(&self, opid: &amp::OpId) -> OpId {
        OpId(opid.0, self.index_for_actor(&opid.1).unwrap())
    }

    fn lamport_compare(&self, op1: &OpId, op2: &OpId) -> Ordering {
        match (op1, op2) {
            (OpId(0, 0), OpId(0, 0)) => Ordering::Equal,
            (OpId(0, 0), OpId(_, _)) => Ordering::Less,
            (OpId(_, _), OpId(0, 0)) => Ordering::Greater,
            (OpId(ctr1, actor1), OpId(ctr2, actor2)) => {
                if ctr1 == ctr2 {
                    let actor1 = &self.actors[*actor1];
                    let actor2 = &self.actors[*actor2];
                    actor1.cmp(actor2)
                } else {
                    op1.0.cmp(&op2.0)
                }
            }
        }
    }

    fn seek_to_obj(&self, obj: &OpId) -> usize {
        if self.ops.is_empty() {
            return 0;
        }
        let mut current_obj = None;
        for (i, next) in self.ops.iter().enumerate() {
            if current_obj == Some(&next.obj) {
                continue;
            }
            if &next.obj == obj || self.lamport_compare(&next.obj, obj) == Ordering::Greater {
                return i;
            }
            current_obj = Some(&next.obj);
        }
        self.ops.len()
    }

    fn seek_to_op(&self, op: &Op) -> Result<(usize, usize),AutomergeError> {
        let obj_start = self.seek_to_obj(&op.obj);
        let mut elem_visible = false;
        let mut visible = 0;

        match &op.key {
            Key::Map(_) => {
                for (i, next) in self.ops[obj_start..].iter().enumerate() {
                    if next.key >= op.key || next.obj != op.obj {
                        return Ok((obj_start + i, 0));
                    }
                }
                Ok((self.ops.len(), 0))
            }
            Key::Seq(_) => {
                //println!("seek to obj - {:?}", obj_start);
                if op.insert {
                    let mut insert_start = obj_start;
                    if !op.key.is_head() {
                        let mut found = false;
                        for (i, next) in self.ops[obj_start..].iter().enumerate() {
                            if next.obj != op.obj {
                                break;
                            }
                            if Key::Seq(next.id) == op.key {
                                found = true;
                                insert_start += i + 1;
                                //println!("step {:?}", insert_start);
                                inc_visible(next,&mut elem_visible, &mut visible);
                                break;
                            }
                            inc_visible(next,&mut elem_visible, &mut visible);
                        }
                        if !found {
                          return Err(AutomergeError::GeneralError("Cant find elemid to insert after".into()))
                        }
                        //println!("not head - seek to {:?}", insert_start);
                    }
                    for next in &self.ops[insert_start..] {
                        if next.obj != op.obj || (next.insert && self.lamport_compare(&next.id,&op.id) == Ordering::Less) {
                            //println!("less - break");
                            break
                        }
                        insert_start += 1;
                        //println!("step {:?}", insert_start);
                        inc_visible(next,&mut elem_visible, &mut visible);
                    }
                    Ok((insert_start, visible))
                } else {
                    for (i, next) in self.ops[obj_start..].iter().enumerate() {
                        if next.insert && next.key == op.key || next.obj != op.obj {
                            return Ok((obj_start + i, visible));
                        }
                        inc_visible(next,&mut elem_visible, &mut visible);
                    }
                    Err(AutomergeError::GeneralError("Cant find elemid to replace".into()))
                }
            }
        }
    }

    fn import_change(&mut self, change: crate::Change) -> Vec<Op> {
        for actor in &change.actors {
            if self.index_for_actor(actor).is_none() {
                self.actors.push(actor.clone());
            }
        }

        let actor = self.index_for_actor(change.actor_id()).unwrap(); // can unwrap b/c we added it above
        let extra_bytes = change.extra_bytes().to_vec();

        let change_id = self.changes.len();
        let ops: Vec<Op> = change
            .iter_ops()
            .enumerate()
            .map(|(i, expanded_op)| Op {
                change: change_id,
                id: OpId(change.start_op + i as u64, actor),
                action: expanded_op.action,
                insert: expanded_op.insert,
                key: self.import_key(&expanded_op.key),
                obj: self.import_objectid(&expanded_op.obj),
                pred: expanded_op
                    .pred
                    .iter()
                    .map(|id| self.import_opid(id))
                    .collect(),
                succ: vec![],
            })
            .collect();

        self.changes.push(Change {
            actor,
            hash: change.hash,
            seq: change.seq,
            max_op: change.max_op(),
            time: change.time,
            message: change.message(),
            deps: change.deps,
            extra_bytes,
        });

        ops
    }

    pub(crate) fn apply_changes(&mut self, changes: Vec<crate::Change>) -> Result<(), AutomergeError> {
        let mut patch = crate::patches::IncrementalPatch::new();
        for change in changes {
            self.apply_change(change, &mut patch)?;
        }
        Ok(())
    }

    pub(crate) fn apply_change(
        &mut self,
        change: crate::Change,
        _diffs: &mut IncrementalPatch,
    ) -> Result<(), AutomergeError> {

        let ops = self.import_change(change);

        for op in ops {
            // slow as balls
            // *** put them in the right place
            let (pos, _visible_count) = self.seek_to_op(&op)?;

            // 1. insert the ops into the list at the right place
            // 2. update the succ[] vecs - seek_to_op_id + update
            // 3. --> generate the diffs ***
            // 4. make it fast (b-tree)

            //println!("op {:?} {:?}",pos, op);
            if op.action != InternalOpType::Del {
                self.ops.insert(pos, op);
            }
        }

        Ok(())
        // update pred/succ properly
        // handle inc/del - they are special
        // generate diffs as we do it
        //
        // look at old code below and see what we might also need to do

        /*
        if self.history_index.contains_key(&change.hash) {
            return Ok(());
        }

        self.event_handlers.before_apply_change(&change);

        let change_index = self.update_history(change);

        // SAFETY: change_index is the index for the change we've just added so this can't (and
        // shouldn't) panic. This is to get around the borrow checker.
        let change = &self.history[change_index];

        let op_set = &mut self.op_set;

        let start_op = change.start_op;

        op_set.update_deps(change);

        let ops = OpHandle::extract(change, &mut self.actors);

        op_set.max_op = max(
            op_set.max_op,
            (start_op + (ops.len() as u64)).saturating_sub(1),
        );

        op_set.apply_ops(ops, diffs, &mut self.actors)?;

        self.event_handlers.after_apply_change(change);

        Ok(())
        */
    }

    fn opids(&self) -> Vec<amp::OpId> {
        self.ops.iter().map(|op| amp::OpId(op.id.0, self.actors[op.id.1].clone())).collect()
    }
}

#[derive(PartialEq, Debug, Copy, Clone)]
pub struct OpId(u64, usize);

#[derive(PartialEq, Debug, Clone)]
pub struct Change {
    pub actor: usize,
    pub hash: amp::ChangeHash,
    pub seq: u64,
    pub max_op: u64,
    pub time: i64,
    pub message: Option<String>,
    pub deps: Vec<amp::ChangeHash>,
    pub extra_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
struct Op {
    pub change: usize,
    pub id: OpId,
    pub action: InternalOpType,
    pub obj: OpId,
    pub key: Key,
    pub succ: Vec<OpId>,
    pub pred: Vec<OpId>,
    pub insert: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Key {
    Map(String),
    Seq(OpId),
}

impl Key {
    fn is_head(&self) -> bool {
        matches!(self, Key::Seq(OpId(0,_)))
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Key::Map(p1), Key::Map(p2)) => p1.partial_cmp(p2),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use automerge_protocol as amp;

    use automerge_protocol::{ ObjectId, OpType, ActorId };

    use base64;

    use crate::{ Change, Backend };

    use crate::change::{decode_document_opids};
    use super::*;

    fn compare_backends(save: Vec<u8>) {
//        let mut mark1 = crate::Backend::new();
        let mark1 = Backend::load(save).unwrap();
        let mut mark2 = OpSet::default();

        let changes = mark1.get_changes(&[]);
        mark2.apply_changes(changes.into_iter().cloned().collect()).unwrap();

        let saved = mark1.save().unwrap();
        let ops1 = decode_document_opids(&saved).unwrap();
        let ops2 = mark2.opids();
        //println!("--- OPS ---");
        //println!("OPS1 {:?}",ops1);
        //println!("OPS2 {:?}",ops2);
        //println!("--- OPS ---");
        assert_eq!(ops1,ops2);
    }

    #[test]
    fn test_save_vs_mark2() {
         let actor_a: ActorId = "aaaaaa".try_into().unwrap();
         let change_a1: Change = amp::Change {
            actor_id: actor_a.clone(),
            seq: 1,
            start_op: 1,
            time: 0,
            message: None,
            hash: None,
            deps: Vec::new(),
            operations: vec![
                amp::Op { obj: ObjectId::Root, action: OpType::Set("magpie".into()), key: "bird".into(), insert: false, pred: Vec::new() },
                amp::Op { obj: ObjectId::Root, action: OpType::Set("xxx".into()), key: "xxx".into(), insert: false, pred: Vec::new() },
                amp::Op { obj: ObjectId::Root, action: OpType::Set("aaa".into()), key: "aaa".into(), insert: false, pred: Vec::new() },
                amp::Op { obj: ObjectId::Root, action: OpType::Set("fff".into()), key: "fff".into(), insert: false, pred: Vec::new() }
            ],
            extra_bytes: Vec::new(),
        }
        .try_into()
        .unwrap();

        compare_backends(change_a1.raw_bytes().to_vec());

        let simple_list = base64::decode("hW9Kg4GAiacArQEBECmUwPz1kEZagKFl16rGqPYBmOik0/Zv7jELzKiUMyOFtA3XUZzGwxc8O12ssIsw0AsHAQIDAhMCIwY1EEACVgIMAQQCBBEEEwcVCCECIwI0AkIEVgRXBoABAn8AfwF/B3+doaqGBn8OSW5pdGlhbGl6YXRpb25/AH8HAAEGAAABBgEAAgUAAAF+AAIEAX8EbGlzdAAGBwAHAQEGfwIGAX8ABhMBAgMEBQYHAA==").unwrap();
        let test1a = base64::decode("hW9Kg0rKILsAnwIBEJu7Qtn7K0smqt1Q/BCcrvwBcDC+HoGmfNIk6Oqjq/BlGAwWZcvHHv3f5jkYU/koDpUHAQIDAhMCIwY1EEACVgIMAQQCDBEIEw4VMSECIws0BEIOVg9XHIABAn8AfwF/FX+Xr6qGBn8OSW5pdGlhbGl6YXRpb25/AH8HAAMSAAADBgICCQILAg0GDwAEBQAABwUAAAN+AAMEAQAGfnkQBAF9BWhlbGxvBGxpc3QEb2JqMQAGegNrZXkEb2JqMgNrZXkEb2JqMwNrZXkEbGlzdAAGFQACAX4HegUBfwILAQMGBgZ9AQIABwF7AAEAAQIGAX9WAgAGE3o2AEYARgAGE3dvcmxkAQIDBAUGdmFsdmFsMnZhbDMBAgMEBQYVAA==").unwrap();
        let test1b = base64::decode("hW9Kg7Cumy8AwgIBEJu7Qtn7K0smqt1Q/BCcrvwBXEUEIn/UuOAWRdn6Ezo5VWU7g8yFWWHfdNkrrLKnzDEIAQIDAhMDIwc1EUADQwJWAg4BBAIMEQgTERUxIQIjDjQGQg5WE1cdgAEGgQECgwECAgACAX4VAX6Xr6qGBgB+DkluaXRpYWxpemF0aW9uAH4AAX8AAgcAAxMAAAMHAgIJAgsCDQYPAAQGAAAHBQAAA34AAwMBfgABAAZ+eRAEAX0FaGVsbG8EbGlzdARvYmoxAAd6A2tleQRvYmoyA2tleQRvYmozA2tleQRsaXN0AAYWAAIBfgd6AwF8EHEBAgsBAwQBAgYGfQECAAgBewABAAECBgF/VgIABBN/FgITejYARgBGAAYTd29ybGQBAgMEYQUGdmFsdmFsMnZhbDMBAgMEBQYGAH8BDwB/AH8W").unwrap();
        let test1c = base64::decode("hW9Kg3IGRdUA0gIBEJu7Qtn7K0smqt1Q/BCcrvwB8yfjx27fDO1zrPWviQDKX5rwvZ/ekvhmnA6m+rterhQIAQIDAhMEIwg1EkAEQwNWAg4BBAIMEQgTFRUxIQIjEjQGQg5WE1cggAEGgQECgwECAwADAX0VAQN/l6+qhgYCAH8OSW5pdGlhbGl6YXRpb24CAH8AAgF+AAEDBwADFgAAAwoCAgkCCwINBg8ABAkAAAcFAAADfgADAwF/AAIBfg8BAAZ+aBAEAX0FaGVsbG8EbGlzdARvYmoxAAp6A2tleQRvYmoyA2tleQRvYmozA2tleQRsaXN0AAYZAAIBfgd6AwF8EHEBDwIBf3ELAQMEAQUGBn0BAgALAXsAAQABAgYBf1YCAAQTfxYFE3o2AEYARgAGE3dvcmxkAQIDBGEFBgQFBnZhbHZhbDJ2YWwzAQIDBAUGBgB/ARIAfwB/Fg==").unwrap();
        let test1d = base64::decode("hW9Kg+X6dsUA2wIBEJu7Qtn7K0smqt1Q/BCcrvwBoXwYdv30XZI1roqo/Ox7ilRrThYI3jBgTZzkAnOnqO4IAQIDAhMFIwg1EkAEQwRWAg4BBAIMEQgTFhUxIQIjFDQGQg5WFVcigAEGgQECgwECBAAEAXwVAQMCf5evqoYGAwB/DkluaXRpYWxpemF0aW9uAwB/AAMBfwACAQQHAAMYAAADDAICCQILAg0GDwAGCQAABwUAAAMDAH8DAwF/AAIBfg8BAAZ+aBAEAX0FaGVsbG8EbGlzdARvYmoxAAx6A2tleQRvYmoyA2tleQRvYmozA2tleQRsaXN0AAYbAAIBfAcSf2kDAXwQcQEPAgF/cQsBAwYBBQYGfQECAA0BewABAAECBgF/VgIAfxQFE38WBRN6NgBGAEYABhN3b3JsZH8AAQIDBGEFBgQFBnZhbHZhbDJ2YWwzAQIDBAUGCAB/ARIAfwB/Fg==").unwrap();
        let nested_objects_and_list_inserts2 = base64::decode("hW9Kg3VDsTMA2QIBEFi6HuSjyUELm4iTM1VtpFsBSaXXvVAz2BHS9i8jlWwqohfGPGPFqAkEG4BN0gWfgX8IAQIDAhMEIwg1EkAEQwNWAg4BBAIMEQgTFhUxIQIjFDQGQg5WFVcigAEGgQECgwECAwADAX8VAgN/gKiqhgYCAH8OSW5pdGlhbGl6YXRpb24CAH8AAgF+AAEDBwADGAAAAwwCAgkCCwINBg8ABgkAAAcFAAADAwB/AwMBfwACAX4OAQAGfmkQBAF9BWhlbGxvBGxpc3QEb2JqMQAMegNrZXkEb2JqMgNrZXkEb2JqMwNrZXkEbGlzdAAGGwACAXwHEn9pAwF8E24BDgIBf3ILAQMGAQUGBn0BAgANAXsAAQABAgYBf1YCAH8UBRN/FgUTejYARgBGAAYTd29ybGR/AAECAwRhBQYEBQZ2YWx2YWwydmFsMwECAwQFBggAfwESAH8Afxk=").unwrap();
        let nested_objects_with_deletes = base64::decode("hW9Kg1G4LGoAvQIBEJSubXV2MUdBlah2/lbGXfUBmozh93gHSQmP+3KPhwFEbEUyMf+or+B4o6kgIh1j0IcIAQIDAhMDIwc1EUADQwJWAg4BBAIMEQgTDhUxIQIjCzQEQg5WD1ccgAEMgQECgwEEAgACAX4VA36fqKqGBgB+DkluaXRpYWxpemF0aW9uAH4AAX8AAgcAAxIAAAMGAgIJAgsCDQYPAAQFAAAHBQAAA34AAwQBAAZ+eRAEAX0FaGVsbG8EbGlzdARvYmoxAAZ6A2tleQRvYmoyA2tleQRvYmozA2tleQRsaXN0AAYVAAIBfgd6BQF/AgsBAwYGBn0BAgAHAXsAAQABAgYBf1YCAAYTejYARgBGAAYTd29ybGQBAgMEBQZ2YWx2YWwydmFsMwECAwQFBn8BAgB/AQQAfwEMAAMAfRYCfw==").unwrap();

        let test2a = base64::decode("hW9KgzBGpZIAnQEBEI1ypR4ywkNNpp96l7zXTxUB6LDl3wzr1G2U8Iy19fzBzhdDdhnH/+FcM+KyKGuJPS8HAQIDAhMCIwY1EEACVgILAQQCBBMEFQghAiMCNAJCA1YDVwGAAQJ/AH8BfwJ/mMu4hgZ/DkluaXRpYWxpemF0aW9ufwB/BwABfwAAAX8BAAF/AH8EbGlzdAABAgACAQEBfgIBfgAUAQIA").unwrap();
        let test2b = base64::decode("hW9Kg5pvmycArwEBEI1ypR4ywkNNpp96l7zXTxUBeFm40CfXxlImmWuVOoPY8xAJeyUKW1oVoRZcamH3CjsIAQIDAhMDIwc1EUADQwJWAgwBBAIEEQQTBRUIIQIjAjQCQgRWBFcCgAECAgACAX4CAX6Yy7iGBgB+DkluaXRpYWxpemF0aW9uAH4AAX8AAgcAAQIAAAECAQACfwAAAX4AAn8EbGlzdAACAwADAQECfwICAX8AAhQBAgMA").unwrap();
        let test2c = base64::decode("hW9Kg8RtIrUAuQEBEI1ypR4ywkNNpp96l7zXTxUBnVTRJYAjAI4VFCHV/Sf8G/neum+KYZVmywMClPvNoZwIAQIDAhMEIwg1EkAEQwNWAgwBBAIEEQQTBhUIIQIjBTQCQgRWBFcDgAECAwADAX8CAgF/mMu4hgYCAH8OSW5pdGlhbGl6YXRpb24CAH8AAgF+AAEDBwABAwAAAQMBAAN/AAABAgB/An8EbGlzdAADBAB8AQN+AQEDfwIDAX8AAxQAAQIEAA==").unwrap();

        let test2d = base64::decode("hW9Kg9VXZL4A0AEBEMwPa/zJ3Ewzr0WmHsBddIoBUvT34IK8sCqecfLDuuAmVuij0/OK4NO60JYIajJ5VKgIAQIDAhMEIwg1EkAEQwRWAg4BBAIEEQQTBxUIIQIjBjQEQgRWB1cEgAEGgQECgwECBAAEAX8CAwF/wNG4hgYDAH8OSW5pdGlhbGl6YXRpb24DAH8AAwF/AAIBBAcAAQQAAAEEAQADAgAAAQIAfgIAfwRsaXN0AAQFAHsBA34DfgECAQF/AgQBfwACFH4WFAABYgICAH8BAgB/AH8F").unwrap();
        let test2e = base64::decode("hW9KgzVDLxEA4gEBEMwPa/zJ3Ewzr0WmHsBddIoBTQS9F2q3aVXqmepEcgRu7fYg7X4JLpT48JQO1as/GxcIAQIDAhMGIwg1EkAEQwRWAg4BBAIEEQgTChUIIQIjCjQEQgRWCVcHgAEGgQECgwECBQAFAX8CAwF/A3/A0biGBgQAfw5Jbml0aWFsaXphdGlvbgQAfwAEAX8AAwEFBwABBwAAAQcBAAIDAAABAgAAAXkABAIBeQIAfwRsaXN0AAcIAH0BAwICAX16A34BBQEBfwIHAX4AFAMWfRQWFAB4eXoBYgIFAH8BAgB/AH8F").unwrap();
        let test2f = base64::decode("hW9Kg8lkGKoA6wEBEMwPa/zJ3Ewzr0WmHsBddIoBOJQny6rv0rp5VuaCBRuBEGkSi9rKJ+BA/xkHhXtXD0AIAQIDAhMHIwg1EkAEQwRWAg4BBAIEEQgTCxUIIQIjDDQEQgRWDFcJgAEGgQECgwECBgAGAX8CAwF+AwJ/wNG4hgYFAH8OSW5pdGlhbGl6YXRpb24FAH8ABQF/AAQBBgcAAQkAAAEJAQAEAwAAAQIAAAEDAHoEAgF5AgB/BGxpc3QACQoAewEJf3sCAgF9egN+AQcBAX8CCQF/AAIWfxQDFn0UFhRycQB4eXoBYgIHAH8BAgB/AH8F").unwrap();
        let test2g = base64::decode("hW9Kg8+YqnIA9AEBEMwPa/zJ3Ewzr0WmHsBddIoB5tFFgKkrur3AcdnC+OV5y08zXGq1Eaa/L8JLu9JZtVoIAQIDAhMIIwg1EkAEQwRWAg4BBAIEEQgTDRUIIQIjDjQEQgRWDlcLgAEGgQECgwECBwAHAX8CAwF/AwICf8DRuIYGBgB/DkluaXRpYWxpemF0aW9uBgB/AAYBfwAFAQcHAAELAAABCwEABAMAAAEEAAABAwB4BAIBeQIAAQh/BGxpc3QACwwAewEJf3sCAgF7egN+CAEBBwEDfwILAX8AAhZ/FAMWfRQWFAIWcnEAeHl6AWICc3QHAH8BBAB/AH8F").unwrap();

        compare_backends(simple_list);
        compare_backends(test1a);
        compare_backends(test1b);
        compare_backends(test1c);
        compare_backends(test1d);
        compare_backends(test2a);
        compare_backends(test2b);
        compare_backends(test2c);
        compare_backends(test2d);
        compare_backends(test2e);
        compare_backends(test2f);
        compare_backends(test2g);
        compare_backends(nested_objects_and_list_inserts2);
        compare_backends(nested_objects_with_deletes);
    }
}
