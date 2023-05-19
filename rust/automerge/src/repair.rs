
use std::collections::HashMap;
//use std::collections::HashSet;
use std::str::FromStr;

use crate::error::RepairError;
use crate::transaction::Transactable;
use crate::history::History;
use crate::{ Value, ChangeHash, ObjId, ExpandedChange, Cursor, AutomergeError, Change, Prop, ReadDoc,  Automerge };
use crate::transaction::{ Transaction, UnObserved };
use crate::legacy::{ Key as ExpandedKey, OpId as ExpandedOpId, Op as ExpandedOp, OpType as ExpandedOpType };
use crate::storage;

pub fn repair(data: &[u8]) -> Result<Automerge, RepairError> {
    match Automerge::load(&data) {
      Ok(doc) => {
        println!("File loaded successfully");
        Ok(doc)
      },

      Err(AutomergeError::Load(storage::load::Error::InflateDocument(e))) => {
        println!("Error on load: {:?}", e);
        repair_mismatched_heads(data).map_err(|e| RepairError::RepairFailed(e))
      },
      Err(e) => Err(RepairError::UnknownError(e)),
    }
}

#[allow(dead_code)]
fn log_change(change: &ExpandedChange) {
    log!("change={}@{}", change.seq, change.actor_id.to_string());
    log!("start_op={:?}", change.start_op);
    for op in &change.operations {
      log!("  op={:?}", op);
    }
}

fn repair_mismatched_heads(data: &[u8]) -> Result<Automerge, AutomergeError> {
    let doc = Automerge::load_unverified_heads(&data)?;
    let changes = doc.get_changes(&[])?;

    println!("{} changes", changes.len());

    let mut repaired = Automerge::new();
    let mut hashes = HashMap::new();
    //let mut actors = HashSet::new();
    // safe to unwrap - cant have mismatched heads without at least one change
    let first_change = changes.get(0).cloned().cloned().unwrap();
    //log_change(&first_change.decode());
    //actors.insert(first_change.actor_id().clone());
    hashes.insert(first_change.hash(), first_change.hash());
    repaired.apply_changes(Some(first_change))?;

    let odd_hash = ChangeHash::from_str("1a3282b6672aeae8bf52582ac62f26bd83aa12160a0ca3d0c8704d53df1689a2").unwrap();

    let len = changes.len();

    for (i, change) in changes.into_iter().enumerate().skip(1) {
      if i % 100 == 0 { println!("change {}/{}",i,len); }
      let mut new_change = change.decode();
      //println!("change {}/{} {}@{}",i,len, new_change.start_op, new_change.actor_id);
      new_change.deps = new_change.deps.iter().map(|hash| hashes.get(hash).unwrap_or(hash).clone()).collect();

      let next_start_op = new_change.deps.iter().map(|hash| {
          let change = doc.get_change_by_hash(hash);
          if change.is_none() {
            println!(" cant find hash {}",hash);
            println!(" change={:?}", new_change);
            panic!("bla");
          }
          let change = change.unwrap();
          let start_op = change.start_op();
          let count = change.iter_ops().count();
          let next_start_op = u64::from(start_op)  + count as u64;
          next_start_op
      }).max().unwrap();

      if next_start_op != new_change.start_op.into() {
          println!("next start op {} vs {}", next_start_op, new_change.start_op);
      }

      let new_bin_change = tx_and_modify_change(&mut repaired, new_change)?;
/*
      let new_bin_change = if let Ok(new_change) = modify_change(&repaired, &mut new_change) {
          new_change
      } else { // that didnt work - try the super slow but more reliable version
          if new_change.deps == repaired.get_heads() {
            tx_and_modify_change(&mut repaired, &mut new_change)?
          } else {
            fork_and_modify_change(&repaired, &mut new_change)?
          }
      };
*/
/*
      if change.hash() != new_bin_change.hash() {
        println!("change hash mismatch {:?}", new_change);
      }
*/
      //actors.insert(new_bin_change.actor_id().clone());
      let before_heads = repaired.get_heads();
      repaired.apply_changes(Some(new_bin_change.clone()))?;
      if new_bin_change.hash() == odd_hash {
        println!("inserting odd hash");
        println!("** heads={:?}",repaired.get_heads());
        println!("** before_heads={:?}",before_heads);
      }
      hashes.insert(change.hash(), new_bin_change.hash());
    }
    println!("repaired document heads: {:?}", repaired.get_heads());
    Ok(repaired)
}

fn modify_change(repaired: &Automerge, new_change: &mut ExpandedChange) -> Result<Change,AutomergeError> {
      for op in &mut new_change.operations {
            let obj_str = format!("{}", op.obj);
            let (obj,_) = repaired.import(&obj_str)?;
            let prop = match &op.key {
              ExpandedKey::Map(s) => Prop::Map(s.as_str().into()),
              ExpandedKey::Seq(e) => {
                  let cursor : Option<Cursor> = e.into();
                  if let Some(cursor) = cursor {
                    let index = repaired.get_cursor_position(&obj, &cursor, Some(&new_change.deps))?;
                    Prop::Seq(index)
                  } else {
                    Prop::Seq(0)
                  }
              }
            };
            // this could be an error if the obj was created by this change
            // we assume the pred's are corrcet in this case
            let values = repaired.get_all_at(&obj, prop.clone(), &new_change.deps)?;
            op.pred = values.into_iter().map(|(_,id)| ExpandedOpId::try_from(&id).unwrap()).collect();
      }
      Ok(Change::from(new_change.clone()))
}

fn get_all<'a>(doc: &'a mut Automerge, obj: &ObjId, prop: Prop, heads: &[ChangeHash]) -> Result<Vec<(Value<'a>, ObjId)>,AutomergeError> {
        let obj = doc.exid_to_obj(obj.as_ref())?;
        let mut clock = doc.clock_at(heads);
        let actor_index = doc.get_actor_index();
        clock.maximize(actor_index);
        let clock = Some(clock);
        let values = doc
            .ops()
            .seek_ops_by_prop(&obj.id, prop, obj.encoding, clock.as_ref())
            .ops
            .into_iter()
            .map(|op| doc.export_value(op, clock.as_ref()))
            .collect::<Vec<_>>();
        Ok(values)
}
    fn get_cursor_position(
        doc: &mut Automerge,
        obj: &ObjId,
        cursor: &Cursor,
        at: &[ChangeHash],
    ) -> Result<usize, AutomergeError> {
        let obj = doc.exid_to_obj(obj.as_ref())?;
        let mut clock = doc.clock_at(at);
        let actor_index = doc.get_actor_index();
        clock.maximize(actor_index);
        let opid = doc.cursor_to_opid(cursor, Some(&clock))?;
        let found = doc
            .ops()
            .seek_opid(&obj.id, opid, Some(&clock))
            .ok_or_else(|| AutomergeError::InvalidCursor(cursor.clone()))?;
        Ok(found.index)
    }

fn tx_and_modify_change(repaired: &mut Automerge, mut new_change: ExpandedChange) -> Result<Change,AutomergeError> {
      //println!(" fall back to tx and modify...");
      repaired.set_actor(new_change.actor_id.clone());
      let mut args = repaired.transaction_args();
      args.start_op = new_change.start_op;
      let mut tx = Transaction::new(repaired, args, UnObserved, History::innactive());
      if new_change.operations.iter().all(|op| op.insert) {
        return Ok(Change::from(new_change))
      }
      for op in &mut new_change.operations {
            //println!("  op={:?}",op);
            let obj_str = format!("{}", op.obj);
            let obj = tx.doc().import_obj(&obj_str)?;
            let prop = match &op.key {
              ExpandedKey::Map(s) => Prop::Map(s.as_str().into()),
              ExpandedKey::Seq(e) => {
                  let cursor : Option<Cursor> = e.into();
                  if let Some(cursor) = cursor {
                    let index = get_cursor_position(tx.doc(), &obj, &cursor, &new_change.deps).unwrap();
                    Prop::Seq(index)
                  } else {
                    Prop::Seq(0)
                  }
              }
            };
            // this could be an error if the obj was created by this change
            // we assume the pred's are corrcet in this case
            if !op.insert {
              let values = get_all(tx.doc(), &obj, prop.clone(), &new_change.deps)?;
              op.pred = values.into_iter().map(|(_,id)| ExpandedOpId::try_from(&id).unwrap()).collect();
            }
            apply_op(&mut tx, &obj, prop, op)?;
      }
      Ok(Change::from(new_change))
}

fn fork_and_modify_change(repaired: &Automerge, new_change: &mut ExpandedChange) -> Result<Change,AutomergeError> {
      //println!(" fall back to fork and modify...");
      let mut tmp = repaired.fork_at(&new_change.deps)?.with_actor(new_change.actor_id.clone());
      let mut tx = tmp.transaction();
      for op in &mut new_change.operations {
            let obj_str = format!("{}", op.obj);
            let obj = tx.doc().import_obj(&obj_str)?;
            let prop = match &op.key {
              ExpandedKey::Map(s) => Prop::Map(s.as_str().into()),
              ExpandedKey::Seq(e) => {
                  let cursor : Option<Cursor> = e.into();
                  if let Some(cursor) = cursor {
                    let index = tx.get_cursor_position(&obj, &cursor, None).unwrap();
                    Prop::Seq(index)
                  } else {
                    Prop::Seq(0)
                  }
              }
            };
            // this could be an error if the obj was created by this change
            // we assume the pred's are corrcet in this case
            let values = tx.get_all(&obj, prop.clone()).unwrap();
            op.pred = values.into_iter().map(|(_,id)| ExpandedOpId::try_from(&id).unwrap()).collect();
            apply_op(&mut tx, &obj, prop, op)?;
      }
      Ok(Change::from(new_change.clone()))
}

fn apply_op(tx: &mut Transaction<'_, UnObserved>, obj: &ObjId, prop: Prop, op: &mut ExpandedOp) -> Result<(),AutomergeError> {
  match (&op.action, op.insert) {
      (ExpandedOpType::Make(obj_type), true) => {
          //println!("  insert_obj {}, {}={}", obj,prop,obj_type);
          tx.insert_object(obj, prop.to_index().unwrap(), *obj_type)?;
      }
      (ExpandedOpType::Make(obj_type), false) => {
          //println!("  put_obj {}, {}={}", obj,prop,obj_type);
          tx.put_object(obj, prop, *obj_type)?;
      }
      (ExpandedOpType::Delete,_) => {
          //println!("  delete {}, {}", obj,prop);
          tx.delete(obj, prop)?;
      }
      (ExpandedOpType::Increment(value),_) => {
          //println!("  increment {}, {}={}", obj,prop,*value);
          tx.increment(obj, prop, *value)?;
      }
      (ExpandedOpType::Put(value),true) => {
          //println!("  insert {}, {}={}", obj,prop,value);
          //let index = prop.to_index().unwrap();
          tx.insert(obj, prop.to_index().unwrap(), value.clone())?;
          //println!("  get:: {}, {}={:?}", obj, prop, tx.get_all(obj, index))
      }
      (ExpandedOpType::Put(value),false) => {
          //println!("  put {}, {}={}", obj,prop,value);
          tx.put(obj, prop, value.clone())?;
      }
      (ExpandedOpType::MarkBegin(_),_) => {},
      (ExpandedOpType::MarkEnd(_),_) => {},
  }
  Ok(())
}
